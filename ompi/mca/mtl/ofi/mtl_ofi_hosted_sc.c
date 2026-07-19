/*
 * Hosted Lithe multicontext: same-OS-process MTL/OFI short-circuit.
 *
 * Co-resident logical ranks (same vpid/RPH host) match via per-slot posted
 * and unexpected queues with memcpy — no cxi/OFI loopback. Cross-OS peers
 * still use OFI.
 *
 * Single-OS (num_procs == RPH): ANY_SOURCE is local-only (no fi_trecv).
 * Multi-OS: specific-source same-OS uses full SC queues; ANY_SOURCE never
 * dual-posts (SC + fi_trecv races with CQ cancel) — match local UE then
 * fall through to OFI for remote. RD Barrier/Allreduce use specific ranks.
 */

#include "ompi_config.h"

#if HAVE_LITHE

#include "mtl_ofi.h"
#include "mtl_ofi_hosted_sc.h"

#include "opal/class/opal_list.h"
#include "opal/mca/threads/mutex.h"
#include "opal/runtime/opal_progress.h"
#include "opal/util/output.h"
#include "opal/util/proc.h"

#include <lithe/fork_join_sched.h>
#include <lithe/lithe.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    opal_list_item_t super;
    ompi_mtl_ofi_request_t *req;
    void *buf; /* recv destination (user or pack buffer) */
    int tag;
    int src; /* MPI_ANY_SOURCE or specific comm rank */
    int cid;
    int ofi_dual;       /* may also use fi_trecv (ANY_SOURCE) */
    int ofi_posted;     /* fi_trecv successfully posted (safe to cancel) */
    /* Lithe context parked in try_park_pending for this posted recv. */
    lithe_context_t *waiter;
} ompi_mtl_ofi_hosted_sc_posted_t;

OBJ_CLASS_INSTANCE(ompi_mtl_ofi_hosted_sc_posted_t, opal_list_item_t, NULL, NULL);

typedef struct {
    opal_list_item_t super;
    void *buf; /* owned copy */
    size_t len;
    int tag;
    int src_rank;
    int cid;
    int is_sync;
    int send_is_isend;
    ompi_mtl_ofi_request_t *send_req;
    /* Sync-send Lithe context parked until matching recv takes this UE. */
    lithe_context_t *send_waiter;
} ompi_mtl_ofi_hosted_sc_ue_t;

OBJ_CLASS_INSTANCE(ompi_mtl_ofi_hosted_sc_ue_t, opal_list_item_t, NULL, NULL);

typedef struct {
    opal_mutex_t lock;
    opal_list_t posted;
    opal_list_t unexpected;
} ompi_mtl_ofi_hosted_sc_slot_t;

typedef struct {
    ompi_mtl_ofi_hosted_sc_slot_t *slot;
    ompi_mtl_ofi_hosted_sc_ue_t *ue;
    ompi_mtl_ofi_request_t *sreq;
} sc_ssend_park_arg_t;

static ompi_mtl_ofi_hosted_sc_slot_t *sc_slots = NULL;
static unsigned long sc_nslots = 0;
static int sc_enabled = 0;

/*
 * Clear waiter pointers matching w on this slot (lock held). Caller unlocks
 * then unblocks — never unblock under the SC slot lock.
 */
static void sc_clear_waiter_locked(ompi_mtl_ofi_hosted_sc_slot_t *slot,
                                   lithe_context_t *w)
{
    ompi_mtl_ofi_hosted_sc_posted_t *pr;
    ompi_mtl_ofi_hosted_sc_ue_t *ue;

    if (NULL == w) {
        return;
    }
    OPAL_LIST_FOREACH (pr, &slot->posted, ompi_mtl_ofi_hosted_sc_posted_t) {
        if (pr->waiter == w) {
            pr->waiter = NULL;
        }
    }
    OPAL_LIST_FOREACH (ue, &slot->unexpected, ompi_mtl_ofi_hosted_sc_ue_t) {
        if (ue->send_waiter == w) {
            ue->send_waiter = NULL;
        }
    }
}

static void sc_ssend_park_cb(lithe_context_t *context, void *arg)
{
    sc_ssend_park_arg_t *a = (sc_ssend_park_arg_t *) arg;

    if (NULL == a->sreq || a->sreq->completion_count <= 0) {
        opal_mutex_unlock(&a->slot->lock);
        lithe_context_unblock(context);
        return;
    }
    a->ue->send_waiter = context;
    opal_mutex_unlock(&a->slot->lock);
}

/* True when logical world spans more than one OS process (P>1). */
static int sc_multi_os_world(void)
{
    return (opal_process_info.num_procs > opal_lithe_env_cache_rph()) ? 1 : 0;
}

static int sc_tag_match(int posted_tag, int msg_tag)
{
    /* MPI_ANY_TAG matches user and collective (negative) tags. */
    if (MPI_ANY_TAG == posted_tag) {
        return 1;
    }
    return posted_tag == msg_tag;
}

static int sc_src_match(int posted_src, int msg_src)
{
    if (MPI_ANY_SOURCE == posted_src) {
        return 1;
    }
    return posted_src == msg_src;
}

static ompi_mtl_ofi_hosted_sc_slot_t *sc_slot_for_vpid(opal_vpid_t v)
{
    if (!sc_enabled || 0 == sc_nslots) {
        return NULL;
    }
    return &sc_slots[(unsigned long) v % sc_nslots];
}

static void sc_finish_recv(ompi_mtl_ofi_request_t *ofi_req, void *dst,
                           int src_rank, int tag, const void *src_buf,
                           size_t len)
{
    ompi_status_public_t *status;
    size_t copy_len;

    assert(ofi_req->super.ompi_req);
    status = &ofi_req->super.ompi_req->req_status;

    ofi_req->req_started = true;
    status->MPI_ERROR = MPI_SUCCESS;
    status->MPI_SOURCE = src_rank;
    status->MPI_TAG = tag;
    status->_ucount = len;

    if (len > ofi_req->length) {
        status->MPI_ERROR = MPI_ERR_TRUNCATE;
        copy_len = ofi_req->length;
    } else {
        copy_len = len;
    }

    if (NULL != dst && copy_len > 0 && NULL != src_buf) {
        memcpy(dst, src_buf, copy_len);
    }
    if (NULL != ofi_req->buffer && copy_len > 0) {
        (void) ompi_mtl_datatype_unpack(ofi_req->convertor, ofi_req->buffer,
                                        copy_len);
    }

    ompi_mtl_ofi_deregister_buffer(ofi_req);
    ofi_req->super.completion_callback(&ofi_req->super);
}

static void sc_finish_send(ompi_mtl_ofi_request_t *ofi_req, bool is_isend,
                           bool free_after, void *start)
{
    if (NULL == ofi_req) {
        return;
    }
    ofi_req->status.MPI_ERROR = OMPI_SUCCESS;
    if (is_isend) {
        if (ofi_req->completion_count <= 0) {
            ofi_req->completion_count = 1;
        }
        ofi_req->completion_count--;
        if (0 == ofi_req->completion_count) {
            ompi_mtl_ofi_deregister_and_free_buffer(ofi_req);
            ofi_req->super.ompi_req->req_status.MPI_ERROR =
                ofi_req->status.MPI_ERROR;
            ofi_req->super.completion_callback(&ofi_req->super);
        }
    } else {
        ofi_req->completion_count = 0;
        if (free_after && NULL != start) {
            free(start);
        }
    }
}

static lithe_context_t *sc_release_ssend(ompi_mtl_ofi_hosted_sc_ue_t *ue)
{
    ompi_mtl_ofi_request_t *sreq = ue->send_req;
    lithe_context_t *w = ue->send_waiter;

    ue->send_waiter = NULL;
    if (NULL == sreq) {
        return w;
    }
    if (ue->send_is_isend) {
        sc_finish_send(sreq, true, false, NULL);
    } else if (sreq->completion_count > 0) {
        sreq->completion_count--;
    }
    return w;
}

int ompi_mtl_ofi_hosted_sc_enabled(void)
{
    unsigned long rph;

    if (!sc_enabled) {
        return 0;
    }
    /*
     * Active for hosted RPH>=2 on both single-OS and multi-OS. Per-peer
     * same_os() gates memcpy vs OFI; ANY_SOURCE dual-post is never used
     * on multi-OS (see try_irecv).
     */
    rph = opal_lithe_env_cache_rph();
    if (rph < 2UL) {
        return 0;
    }
    return 1;
}

int ompi_mtl_ofi_hosted_sc_same_os(struct ompi_proc_t *peer)
{
    unsigned long rph;
    opal_vpid_t me, them;

    if (!sc_enabled || NULL == peer) {
        return 0;
    }
    rph = opal_lithe_env_cache_rph();
    if (rph < 2UL) {
        return 0;
    }
    me = opal_proc_local_get()->proc_name.vpid;
    them = peer->super.proc_name.vpid;
    return ((me / (opal_vpid_t) rph) == (them / (opal_vpid_t) rph));
}

int ompi_mtl_ofi_hosted_sc_init(void)
{
    const char *e;
    unsigned long rph, i;

    sc_enabled = 0;
    if (!opal_lithe_env_cache_active()) {
        return OMPI_SUCCESS;
    }
    e = getenv("LITHE_MTL_OFI_HOSTED_SC");
    if (NULL != e && '0' == e[0] && '\0' == e[1]) {
        return OMPI_SUCCESS;
    }
    rph = opal_lithe_env_cache_rph();
    if (rph < 2UL || rph > 4096UL) {
        return OMPI_SUCCESS;
    }

    sc_slots = calloc(rph, sizeof(*sc_slots));
    if (NULL == sc_slots) {
        return OMPI_ERR_OUT_OF_RESOURCE;
    }
    sc_nslots = rph;
    for (i = 0; i < rph; ++i) {
        OBJ_CONSTRUCT(&sc_slots[i].lock, opal_mutex_t);
        OBJ_CONSTRUCT(&sc_slots[i].posted, opal_list_t);
        OBJ_CONSTRUCT(&sc_slots[i].unexpected, opal_list_t);
    }
    sc_enabled = 1;
    /* wait_sync scarce path: park on posted recv instead of yield→steal. */
    opal_progress_set_sc_park_callback(ompi_mtl_ofi_hosted_sc_try_park_pending);
    opal_output_verbose(1, opal_common_ofi.output,
                        "mtl:ofi: hosted same-OS short-circuit enabled (slots=%lu multi_os=%d)",
                        rph, sc_multi_os_world());
    return OMPI_SUCCESS;
}

void ompi_mtl_ofi_hosted_sc_finalize(void)
{
    unsigned long i;

    opal_progress_set_sc_park_callback(NULL);
    if (NULL == sc_slots) {
        sc_enabled = 0;
        return;
    }
    for (i = 0; i < sc_nslots; ++i) {
        ompi_mtl_ofi_hosted_sc_ue_t *ue;
        ompi_mtl_ofi_hosted_sc_posted_t *pr;
        while (NULL
               != (ue = (ompi_mtl_ofi_hosted_sc_ue_t *) opal_list_remove_first(
                       &sc_slots[i].unexpected))) {
            free(ue->buf);
            OBJ_RELEASE(ue);
        }
        while (NULL
               != (pr = (ompi_mtl_ofi_hosted_sc_posted_t *) opal_list_remove_first(
                       &sc_slots[i].posted))) {
            OBJ_RELEASE(pr);
        }
        OBJ_DESTRUCT(&sc_slots[i].unexpected);
        OBJ_DESTRUCT(&sc_slots[i].posted);
        OBJ_DESTRUCT(&sc_slots[i].lock);
    }
    free(sc_slots);
    sc_slots = NULL;
    sc_nslots = 0;
    sc_enabled = 0;
}

int ompi_mtl_ofi_hosted_sc_try_send(struct ompi_communicator_t *comm, int dest,
                                    int tag, void *start, size_t length,
                                    bool free_after,
                                    mca_pml_base_send_mode_t mode,
                                    ompi_mtl_ofi_request_t *ofi_req,
                                    bool is_isend)
{
    ompi_proc_t *peer;
    ompi_mtl_ofi_hosted_sc_slot_t *slot;
    ompi_mtl_ofi_hosted_sc_posted_t *pr, *pr_next;
    ompi_mtl_ofi_hosted_sc_ue_t *ue;
    int src_rank, cid;
    int is_sync = (MCA_PML_BASE_SEND_SYNCHRONOUS == mode);
    void *payload;
    ompi_mtl_ofi_request_t *matched_recv = NULL;
    void *matched_dst = NULL;
    int need_ofi_cancel = 0;
    struct ompi_communicator_t *matched_comm = NULL;
    lithe_context_t *recv_waiter = NULL;

    if (!sc_enabled) {
        return 0;
    }
    peer = ompi_comm_peer_lookup(comm, dest);
    if (!ompi_mtl_ofi_hosted_sc_same_os(peer)) {
        return 0;
    }
    slot = sc_slot_for_vpid(peer->super.proc_name.vpid);
    if (NULL == slot) {
        return 0;
    }

    src_rank = ompi_mtl_ofi_comm_rank(comm);
    cid = (int) comm->c_index;

    opal_mutex_lock(&slot->lock);

    OPAL_LIST_FOREACH_SAFE (pr, pr_next, &slot->posted,
                            ompi_mtl_ofi_hosted_sc_posted_t) {
        if (pr->cid != cid || !sc_tag_match(pr->tag, tag)
            || !sc_src_match(pr->src, src_rank)) {
            continue;
        }
        if (pr->req->req_started) {
            opal_list_remove_item(&slot->posted, &pr->super);
            OBJ_RELEASE(pr);
            continue;
        }
        matched_recv = pr->req;
        matched_dst = pr->buf;
        matched_comm = pr->req->comm;
        recv_waiter = pr->waiter;
        pr->waiter = NULL;
        /* Only cancel if fi_trecv was actually posted (not merely planned). */
        need_ofi_cancel = (pr->ofi_dual && pr->ofi_posted) ? 1 : 0;
        sc_clear_waiter_locked(slot, recv_waiter);
        opal_list_remove_item(&slot->posted, &pr->super);
        OBJ_RELEASE(pr);
        break;
    }

    if (NULL != matched_recv) {
        opal_mutex_unlock(&slot->lock);
        /* Cancel OFI before completion_callback (may free the request). */
        if (need_ofi_cancel && NULL != matched_comm) {
            int ctxt_id = ompi_mtl_ofi_ctxt_index_for_comm(matched_comm);
            (void) fi_cancel((fid_t) ompi_mtl_ofi.ofi_ctxt[ctxt_id].rx_ep,
                             &matched_recv->ctx);
        }
        sc_finish_recv(matched_recv, matched_dst, src_rank, tag, start, length);
        sc_finish_send(ofi_req, is_isend, free_after, start);
        /* Directed wake: peer parked in try_park_pending on empty SC queue. */
        if (NULL != recv_waiter) {
            lithe_context_unblock(recv_waiter);
        }
        return 1;
    }

    /* Unexpected message. */
    payload = NULL;
    if (length > 0) {
        payload = malloc(length);
        if (NULL == payload) {
            opal_mutex_unlock(&slot->lock);
            ofi_req->status.MPI_ERROR = OMPI_ERR_OUT_OF_RESOURCE;
            return 1;
        }
        memcpy(payload, start, length);
    }
    ue = OBJ_NEW(ompi_mtl_ofi_hosted_sc_ue_t);
    if (NULL == ue) {
        free(payload);
        opal_mutex_unlock(&slot->lock);
        ofi_req->status.MPI_ERROR = OMPI_ERR_OUT_OF_RESOURCE;
        return 1;
    }
    ue->buf = payload;
    ue->len = length;
    ue->tag = tag;
    ue->src_rank = src_rank;
    ue->cid = cid;
    ue->is_sync = is_sync;
    ue->send_is_isend = is_isend ? 1 : 0;
    ue->send_req = NULL;
    ue->send_waiter = NULL;

    if (is_sync) {
        sc_ssend_park_arg_t parg;

        ofi_req->completion_count = 1;
        ofi_req->status.MPI_ERROR = OMPI_SUCCESS;
        ue->send_req = ofi_req;
        opal_list_append(&slot->unexpected, &ue->super);
        if (!is_isend) {
            /* Park until matching recv takes UE — not yield→empty steal. */
            while (ofi_req->completion_count > 0) {
                parg.slot = slot;
                parg.ue = ue;
                parg.sreq = ofi_req;
                lithe_context_block(sc_ssend_park_cb, &parg);
                /* Relock for next completion_count check / re-park. */
                opal_mutex_lock(&slot->lock);
            }
            opal_mutex_unlock(&slot->lock);
            if (free_after && NULL != start) {
                free(start);
            }
        } else {
            opal_mutex_unlock(&slot->lock);
        }
        return 1;
    }

    opal_list_append(&slot->unexpected, &ue->super);
    opal_mutex_unlock(&slot->lock);
    sc_finish_send(ofi_req, is_isend, free_after, start);
    return 1;
}

int ompi_mtl_ofi_hosted_sc_try_irecv(struct ompi_communicator_t *comm, int src,
                                     int tag, void *start, size_t length,
                                     ompi_mtl_ofi_request_t *ofi_req)
{
    ompi_mtl_ofi_hosted_sc_slot_t *slot;
    ompi_mtl_ofi_hosted_sc_ue_t *ue, *ue_next;
    ompi_mtl_ofi_hosted_sc_posted_t *pr;
    int cid;
    int any_src_ofi_only = 0;

    if (!sc_enabled) {
        return 0;
    }

    if (MPI_ANY_SOURCE != src) {
        ompi_proc_t *peer = ompi_comm_peer_lookup(comm, src);
        if (!ompi_mtl_ofi_hosted_sc_same_os(peer)) {
            return 0;
        }
    } else if (sc_multi_os_world()) {
        /*
         * Multi-OS ANY_SOURCE: never dual-post (SC + fi_trecv + cancel races).
         * Claim a local unexpected if present, else OFI-only for remote.
         */
        any_src_ofi_only = 1;
    }

    slot = sc_slot_for_vpid(opal_proc_local_get()->proc_name.vpid);
    if (NULL == slot) {
        return 0;
    }

    cid = (int) comm->c_index;
    ofi_req->length = length;
    ofi_req->comm = comm;

    opal_mutex_lock(&slot->lock);

    OPAL_LIST_FOREACH_SAFE (ue, ue_next, &slot->unexpected,
                            ompi_mtl_ofi_hosted_sc_ue_t) {
        lithe_context_t *sw = NULL;

        if (ue->cid != cid || !sc_tag_match(tag, ue->tag)
            || !sc_src_match(src, ue->src_rank)) {
            continue;
        }
        opal_list_remove_item(&slot->unexpected, &ue->super);
        if (ue->is_sync) {
            sw = sc_release_ssend(ue);
        }
        opal_mutex_unlock(&slot->lock);

        sc_finish_recv(ofi_req, start, ue->src_rank, ue->tag, ue->buf, ue->len);
        if (NULL != sw) {
            lithe_context_unblock(sw);
        }
        free(ue->buf);
        OBJ_RELEASE(ue);
        return 1;
    }

    if (any_src_ofi_only) {
        /* No local UE — wait on OFI only; do not leave an SC posted entry. */
        opal_mutex_unlock(&slot->lock);
        return 0;
    }

    pr = OBJ_NEW(ompi_mtl_ofi_hosted_sc_posted_t);
    if (NULL == pr) {
        opal_mutex_unlock(&slot->lock);
        return 0;
    }
    pr->req = ofi_req;
    pr->buf = start;
    pr->tag = tag;
    pr->src = src;
    pr->cid = cid;
    pr->ofi_dual = 0;
    pr->ofi_posted = 0;
    pr->waiter = NULL;
    opal_list_append(&slot->posted, &pr->super);
    opal_mutex_unlock(&slot->lock);

    return 1;
}

/* Mark that fi_trecv was posted for a dual-posted ANY_SOURCE recv. */
void ompi_mtl_ofi_hosted_sc_mark_ofi_posted(ompi_mtl_ofi_request_t *ofi_req)
{
    ompi_mtl_ofi_hosted_sc_slot_t *slot;
    ompi_mtl_ofi_hosted_sc_posted_t *pr, *pr_next;

    if (!sc_enabled || NULL == ofi_req) {
        return;
    }
    slot = sc_slot_for_vpid(opal_proc_local_get()->proc_name.vpid);
    if (NULL == slot) {
        return;
    }

    opal_mutex_lock(&slot->lock);
    if (ofi_req->req_started) {
        /* Matched via SC before/during fi_trecv — remove posted entry. */
        OPAL_LIST_FOREACH_SAFE (pr, pr_next, &slot->posted,
                                ompi_mtl_ofi_hosted_sc_posted_t) {
            if (pr->req == ofi_req) {
                opal_list_remove_item(&slot->posted, &pr->super);
                OBJ_RELEASE(pr);
                break;
            }
        }
        opal_mutex_unlock(&slot->lock);
        return;
    }
    OPAL_LIST_FOREACH_SAFE (pr, pr_next, &slot->posted,
                            ompi_mtl_ofi_hosted_sc_posted_t) {
        if (pr->req == ofi_req) {
            pr->ofi_posted = 1;
            break;
        }
    }
    opal_mutex_unlock(&slot->lock);
}

void ompi_mtl_ofi_hosted_sc_ofi_recv_claimed(ompi_mtl_ofi_request_t *ofi_req)
{
    ompi_mtl_ofi_hosted_sc_slot_t *slot;
    ompi_mtl_ofi_hosted_sc_posted_t *pr, *pr_next;

    if (!sc_enabled || NULL == ofi_req) {
        return;
    }
    slot = sc_slot_for_vpid(opal_proc_local_get()->proc_name.vpid);
    if (NULL == slot) {
        return;
    }

    opal_mutex_lock(&slot->lock);
    OPAL_LIST_FOREACH_SAFE (pr, pr_next, &slot->posted,
                            ompi_mtl_ofi_hosted_sc_posted_t) {
        if (pr->req == ofi_req) {
            opal_list_remove_item(&slot->posted, &pr->super);
            OBJ_RELEASE(pr);
            break;
        }
    }
    opal_mutex_unlock(&slot->lock);
}

int ompi_mtl_ofi_hosted_sc_cancel_recv(ompi_mtl_ofi_request_t *ofi_req)
{
    ompi_mtl_ofi_hosted_sc_slot_t *slot;
    ompi_mtl_ofi_hosted_sc_posted_t *pr, *pr_next;
    lithe_context_t *w = NULL;
    int found = 0;

    if (!sc_enabled || NULL == ofi_req || ofi_req->req_started) {
        return 0;
    }
    slot = sc_slot_for_vpid(opal_proc_local_get()->proc_name.vpid);
    if (NULL == slot) {
        return 0;
    }

    opal_mutex_lock(&slot->lock);
    OPAL_LIST_FOREACH_SAFE (pr, pr_next, &slot->posted,
                            ompi_mtl_ofi_hosted_sc_posted_t) {
        if (pr->req == ofi_req) {
            w = pr->waiter;
            pr->waiter = NULL;
            sc_clear_waiter_locked(slot, w);
            opal_list_remove_item(&slot->posted, &pr->super);
            OBJ_RELEASE(pr);
            found = 1;
            break;
        }
    }
    opal_mutex_unlock(&slot->lock);

    if (found) {
        ofi_req->super.ompi_req->req_status._cancelled = true;
        ofi_req->super.completion_callback(&ofi_req->super);
        if (NULL != w) {
            lithe_context_unblock(w);
        }
    }
    return found;
}

int ompi_mtl_ofi_hosted_sc_try_park_pending(void)
{
    ompi_mtl_ofi_hosted_sc_slot_t *slot;
    ompi_mtl_ofi_hosted_sc_posted_t *pr;
    int pending = 0;
    int ofi_needed = 0;
    int multi_os;
    unsigned int i, spins;

    if (!sc_enabled) {
        return 0;
    }
    multi_os = sc_multi_os_world();
    slot = sc_slot_for_vpid(opal_proc_local_get()->proc_name.vpid);
    if (NULL == slot) {
        if (multi_os) {
            /* Multi-OS without a slot: do not suppress CQ park. */
            return 0;
        }
        for (i = 0; i < 32u; i++) {
            cpu_relax();
        }
        return 1;
    }

    opal_mutex_lock(&slot->lock);
    OPAL_LIST_FOREACH (pr, &slot->posted, ompi_mtl_ofi_hosted_sc_posted_t) {
        if (!pr->req->req_started) {
            pending = 1;
            /* Dual / fi_trecv posted: completion may arrive on the CQ. */
            if (pr->ofi_dual || pr->ofi_posted) {
                ofi_needed = 1;
            }
        }
    }
    opal_mutex_unlock(&slot->lock);

    /*
     * Multi-OS: only suppress CQ park for *pure* same-OS SC waits.
     * Blanket return-1 with any pending SC hung P2K2 when an OFI recv was
     * also outstanding. Pure SC (ofi_dual=ofi_posted=0) spins so wait_sync
     * does not CQ-park on memcpy matches. P2K4 residual was structural RD
     * (fixed by hybrid coll); this path still avoids wasted CQ parks on
     * same-OS rounds. No pending SC → return 0 so cross-OS OFI can CQ-park.
     */
    if (multi_os) {
        if (!pending || ofi_needed) {
            return 0;
        }
        /*
         * Pure SC: spin-suppress CQ park. MTP=1: must occasionally yield so
         * the peer can post/match (no-yield hung P2K2 collectives after
         * pairwise). Throttle yield — every-spin yield taxed P2K2 median.
         */
        spins = 128u;
        for (i = 0; i < spins; i++) {
            cpu_relax();
            if (0 == (i & 63u) && lithe_fork_join_should_yield_to_runnable()) {
                lithe_context_yield();
            }
        }
        return 1;
    }

    /*
     * Single-OS: always spin-suppress yield/CQ-park in wait_sync. Gaps
     * between Sendrecv rounds used to return 0 → lithe_context_yield while
     * peers entered flat Barrier (P1K8/P1K16 pairwise flakes).
     */
    spins = pending ? 128u : 32u;
    for (i = 0; i < spins; i++) {
        cpu_relax();
    }
    return 1;
}

#endif /* HAVE_LITHE */
