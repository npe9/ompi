/*
 * Copyright (c) 2013-2020 Intel, Inc.  All rights reserved.
 * Copyright (c) 2021-2022 Triad National Security, LLC. All rights
 *                         reserved.
 *
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "mtl_ofi.h"

#include <string.h>

#if HAVE_LITHE
#include <lithe/mutex.h>
#include <lithe/condvar.h>
#include "opal/sys/atomic.h"
#endif

OMPI_DECLSPEC extern mca_mtl_ofi_component_t mca_mtl_ofi_component;

OBJ_CLASS_INSTANCE(mca_mtl_comm_t, opal_object_t, NULL, NULL);

mca_mtl_ofi_module_t ompi_mtl_ofi = {
    {
        (int)((1ULL << MTL_OFI_CID_BIT_COUNT_1) - 1), /* max cid */
        (int)((1ULL << (MTL_OFI_TAG_BIT_COUNT_1 - 1)) - 1) ,/* max tag value */
        0,           /* request reserve space */
        0,           /* flags */

        ompi_mtl_ofi_add_procs,
        ompi_mtl_ofi_del_procs,
        ompi_mtl_ofi_finalize,

         NULL,
         NULL,
         NULL,
         NULL,
         ompi_mtl_ofi_imrecv,
         NULL,

        ompi_mtl_ofi_cancel,
        ompi_mtl_ofi_add_comm,
        ompi_mtl_ofi_del_comm
    },
    0,
    0,
    NULL,
    NULL
};

#if HAVE_LITHE
/*
 * Shared no-SEP EP: exclusive CQ progress ownership.
 * Owner cookie is lithe_context_self(); nest depth allows CQ callbacks that
 * re-enter opal_progress while the same context holds the token.
 */
static opal_atomic_intptr_t mtl_ofi_excl_owner = 0;
static opal_atomic_int32_t mtl_ofi_excl_depth = 0;
static lithe_mutex_t mtl_ofi_excl_mtx;
static lithe_condvar_t mtl_ofi_excl_cv;
static volatile int mtl_ofi_excl_inited = 0;
static volatile unsigned long mtl_ofi_excl_acquires = 0;
static volatile unsigned long mtl_ofi_excl_wait_turns = 0;

void ompi_mtl_ofi_shared_ep_excl_init(void)
{
    if (mtl_ofi_excl_inited) {
        return;
    }
    lithe_mutex_init(&mtl_ofi_excl_mtx, NULL);
    lithe_condvar_init(&mtl_ofi_excl_cv);
    (void) opal_atomic_swap_ptr(&mtl_ofi_excl_owner, (intptr_t) 0);
    (void) opal_atomic_swap_32(&mtl_ofi_excl_depth, 0);
    opal_atomic_mb();
    mtl_ofi_excl_inited = 1;
}

int ompi_mtl_ofi_shared_ep_excl_active(void)
{
    return (ompi_mtl_ofi.shared_ep_excl_progress && mtl_ofi_excl_inited) ? 1 : 0;
}

int ompi_mtl_ofi_shared_ep_excl_busy(void)
{
    if (!ompi_mtl_ofi.shared_ep_excl_progress || !mtl_ofi_excl_inited) {
        return 0;
    }
    return (__atomic_load_n(&mtl_ofi_excl_owner, __ATOMIC_ACQUIRE) != 0) ? 1 : 0;
}

int ompi_mtl_ofi_shared_ep_excl_try_own(void)
{
    lithe_context_t *self;
    intptr_t self_i;
    intptr_t expected;

    if (!ompi_mtl_ofi.shared_ep_excl_progress || !mtl_ofi_excl_inited) {
        return 1; /* disabled ⇒ always "own" */
    }
    self = lithe_context_self();
    if (NULL == self) {
        return 1; /* pre-bootstrap: do not block */
    }
    self_i = (intptr_t) self;

    /* Nested re-enter: already owner → bump depth. */
    expected = self_i;
    if (opal_atomic_compare_exchange_strong_ptr(&mtl_ofi_excl_owner, &expected,
                                                self_i)) {
        (void) opal_atomic_add_fetch_32(&mtl_ofi_excl_depth, 1);
        return 1;
    }

    /* Free token? */
    expected = 0;
    if (opal_atomic_compare_exchange_strong_ptr(&mtl_ofi_excl_owner, &expected,
                                                self_i)) {
        (void) opal_atomic_swap_32(&mtl_ofi_excl_depth, 1);
        (void) __sync_fetch_and_add(&mtl_ofi_excl_acquires, 1UL);
        return 1;
    }
    return 0;
}

void ompi_mtl_ofi_shared_ep_excl_release(void)
{
    int32_t d;
    if (!ompi_mtl_ofi.shared_ep_excl_progress || !mtl_ofi_excl_inited) {
        return;
    }
    d = opal_atomic_add_fetch_32(&mtl_ofi_excl_depth, -1);
    if (d > 0) {
        return;
    }
    /* Clear owner before broadcast; waiters re-check under excl_mtx. */
    (void) opal_atomic_swap_ptr(&mtl_ofi_excl_owner, (intptr_t) 0);
    lithe_mutex_lock(&mtl_ofi_excl_mtx);
    lithe_condvar_broadcast(&mtl_ofi_excl_cv);
    lithe_mutex_unlock(&mtl_ofi_excl_mtx);
}

void ompi_mtl_ofi_shared_ep_excl_wait_turn(void)
{
    lithe_context_t *self;
    intptr_t self_i;
    intptr_t cur;

    if (!ompi_mtl_ofi.shared_ep_excl_progress || !mtl_ofi_excl_inited) {
        return;
    }
    self = lithe_context_self();
    self_i = (intptr_t) self;
    (void) __sync_fetch_and_add(&mtl_ofi_excl_wait_turns, 1UL);
    /*
     * Lock + while(busy): release broadcasts under the same mutex so we
     * cannot miss the wake (single-wait lost-wakeup hung P2K4).
     */
    lithe_mutex_lock(&mtl_ofi_excl_mtx);
    for (;;) {
        cur = (intptr_t) __atomic_load_n(&mtl_ofi_excl_owner, __ATOMIC_ACQUIRE);
        if (0 == cur || cur == self_i) {
            break;
        }
        lithe_condvar_wait(&mtl_ofi_excl_cv, &mtl_ofi_excl_mtx);
    }
    lithe_mutex_unlock(&mtl_ofi_excl_mtx);
}

void ompi_mtl_ofi_shared_ep_excl_wake(void)
{
    if (!ompi_mtl_ofi.shared_ep_excl_progress || !mtl_ofi_excl_inited) {
        return;
    }
    lithe_mutex_lock(&mtl_ofi_excl_mtx);
    lithe_condvar_broadcast(&mtl_ofi_excl_cv);
    lithe_mutex_unlock(&mtl_ofi_excl_mtx);
}

void ompi_mtl_ofi_shared_ep_excl_stats(unsigned long *acquires,
                                       unsigned long *wait_turns)
{
    if (NULL != acquires) {
        *acquires = mtl_ofi_excl_acquires;
    }
    if (NULL != wait_turns) {
        *wait_turns = mtl_ofi_excl_wait_turns;
    }
}

/* Per-ofi-ctxt CQ drain counters (composition: >1 ctxts_touched under MULTI_EP). */
enum { MTL_OFI_MULTI_EP_STAT_MAX = 64 };
static volatile unsigned long mtl_ofi_mep_drains[MTL_OFI_MULTI_EP_STAT_MAX];
static volatile unsigned long mtl_ofi_mep_events[MTL_OFI_MULTI_EP_STAT_MAX];

void ompi_mtl_ofi_multi_ep_note_cq_drain(int ctxt_id, int got)
{
    if (ctxt_id < 0 || ctxt_id >= MTL_OFI_MULTI_EP_STAT_MAX || got <= 0) {
        return;
    }
    (void) __sync_fetch_and_add(&mtl_ofi_mep_drains[ctxt_id], 1UL);
    (void) __sync_fetch_and_add(&mtl_ofi_mep_events[ctxt_id],
                                (unsigned long) got);
}

void ompi_mtl_ofi_multi_ep_stats(unsigned long *drains_out,
                                 unsigned long *events_out,
                                 unsigned long *ctxts_touched_out,
                                 int *num_ctxts_out,
                                 int *hosted_multi_ep_out)
{
    int n, i;
    unsigned long drains = 0, events = 0, touched = 0;

    n = ompi_mtl_ofi.num_ofi_contexts;
    if (n > MTL_OFI_MULTI_EP_STAT_MAX) {
        n = MTL_OFI_MULTI_EP_STAT_MAX;
    }
    for (i = 0; i < n; ++i) {
        unsigned long d = mtl_ofi_mep_drains[i];
        unsigned long e = mtl_ofi_mep_events[i];
        drains += d;
        events += e;
        if (d > 0 || e > 0) {
            touched++;
        }
    }
    if (NULL != drains_out) {
        *drains_out = drains;
    }
    if (NULL != events_out) {
        *events_out = events;
    }
    if (NULL != ctxts_touched_out) {
        *ctxts_touched_out = touched;
    }
    if (NULL != num_ctxts_out) {
        *num_ctxts_out = n;
    }
    if (NULL != hosted_multi_ep_out) {
        *hosted_multi_ep_out = ompi_mtl_ofi.hosted_multi_ep;
    }
}
#endif /* HAVE_LITHE */

static uint32_t ompi_mtl_ofi_lithe_ranks_per_host(void)
{
    unsigned long value = opal_lithe_env_cache_rph();
    if (value < 2UL || value > UINT32_MAX) {
        return 0;
    }
    return (uint32_t) value;
}

static void ompi_mtl_ofi_lithe_host_proc_name(opal_process_name_t *name)
{
    uint32_t ranks_per_host = ompi_mtl_ofi_lithe_ranks_per_host();

    if (ranks_per_host > 1 && name->vpid != OPAL_VPID_WILDCARD &&
        name->vpid != OPAL_VPID_INVALID) {
        name->vpid /= ranks_per_host;
    }
}

static int ompi_mtl_ofi_modex_recv_proc(ompi_proc_t *proc, void **ep_name, size_t *size)
{
    int ret;
    opal_process_name_t name = proc->super.proc_name;

    ompi_mtl_ofi_lithe_host_proc_name(&name);
    OPAL_MODEX_RECV(ret, &mca_mtl_ofi_component.super.mtl_version,
                    &name, ep_name, size);
    return ret;
}

static int ompi_mtl_ofi_init_contexts(struct mca_mtl_base_module_t *mtl,
                                      struct ompi_communicator_t *comm,
                                      mca_mtl_ofi_ep_type ep_type)
{
    int ret;
    int ctxt_id = ompi_mtl_ofi.total_ctxts_used;
    struct fi_cq_attr cq_attr = {0};
    ompi_mtl_ofi_init_cq_attr(&cq_attr);

    if (OFI_REGULAR_EP == ep_type) {
        /*
         * For regular endpoints, just create the Lock object and register
         * progress function.
         */
        goto init_regular_ep;
    }

    /*
     * We only create upto Max number of contexts asked for by the user.
     * If user enables thread grouping feature and creates more number of
     * communicators than available contexts, then we set the threshold
     * context_id so that new communicators created beyond the threshold
     * will be assigned to contexts in a round-robin fashion.
     */
    if (ompi_mtl_ofi.num_ofi_contexts <= ompi_mtl_ofi.total_ctxts_used) {
        ompi_mtl_ofi.comm_to_context[comm->c_index] = comm->c_index %
                                                          ompi_mtl_ofi.total_ctxts_used;
        if (!ompi_mtl_ofi.threshold_comm_context_id) {
            ompi_mtl_ofi.threshold_comm_context_id = comm->c_index;

            opal_show_help("help-mtl-ofi.txt", "SEP thread grouping ctxt limit", true, ctxt_id,
                           ompi_process_info.nodename, __FILE__, __LINE__);
        }

        return OMPI_SUCCESS;
    }

    /* Init context info for Scalable EPs */
    ret = fi_tx_context(ompi_mtl_ofi.sep, ctxt_id, NULL, &ompi_mtl_ofi.ofi_ctxt[ctxt_id].tx_ep, NULL);
    if (ret) {
        MTL_OFI_LOG_FI_ERR(ret, "fi_tx_context failed");
        goto init_error;
    }

    ret = fi_rx_context(ompi_mtl_ofi.sep, ctxt_id, NULL, &ompi_mtl_ofi.ofi_ctxt[ctxt_id].rx_ep, NULL);
    if (ret) {
        MTL_OFI_LOG_FI_ERR(ret, "fi_rx_context failed");
        goto init_error;
    }

    ret = ompi_mtl_ofi_open_cq(&cq_attr, &ompi_mtl_ofi.ofi_ctxt[ctxt_id].cq,
                               &ompi_mtl_ofi.ofi_ctxt[ctxt_id].cq_wait_fd);
    if (ret) {
        MTL_OFI_LOG_FI_ERR(ret, "fi_cq_open failed");
        goto init_error;
    }

    /* Bind CQ to TX/RX context object */
    ret = fi_ep_bind(ompi_mtl_ofi.ofi_ctxt[ctxt_id].tx_ep, (fid_t)ompi_mtl_ofi.ofi_ctxt[ctxt_id].cq,
                     FI_TRANSMIT | FI_SELECTIVE_COMPLETION);
    if (0 != ret) {
        MTL_OFI_LOG_FI_ERR(ret, "fi_bind CQ-EP (FI_TRANSMIT) failed");
        goto init_error;
    }

    ret = fi_ep_bind(ompi_mtl_ofi.ofi_ctxt[ctxt_id].rx_ep, (fid_t)ompi_mtl_ofi.ofi_ctxt[ctxt_id].cq,
                     FI_RECV | FI_SELECTIVE_COMPLETION);
    if (0 != ret) {
        MTL_OFI_LOG_FI_ERR(ret, "fi_bind CQ-EP (FI_RECV) failed");
        goto init_error;
    }

    /* Enable Endpoint for communication. This commits the bind operations */
    ret = fi_enable(ompi_mtl_ofi.ofi_ctxt[ctxt_id].tx_ep);
    if (0 != ret) {
        MTL_OFI_LOG_FI_ERR(ret, "fi_enable (send context) failed");
        goto init_error;
    }

    ret = fi_enable(ompi_mtl_ofi.ofi_ctxt[ctxt_id].rx_ep);
    if (0 != ret) {
        MTL_OFI_LOG_FI_ERR(ret, "fi_enable (recv context) failed");
        goto init_error;
    }

init_regular_ep:
    /* Initialize per-context lock */
    OBJ_CONSTRUCT(&ompi_mtl_ofi.ofi_ctxt[ctxt_id].context_lock, opal_mutex_t);

    if (!ompi_mtl_ofi.is_initialized) {
        ret = opal_progress_register(ompi_mtl_ofi_progress_no_inline);
        if (OMPI_SUCCESS != ret) {
            opal_output_verbose(1, opal_common_ofi.output,
                                "%s:%d: opal_progress_register failed: %d\n",
                                __FILE__, __LINE__, ret);
            goto init_error;
        }
#if HAVE_LITHE
        if (ompi_mtl_ofi.progress_block_enabled &&
            ompi_mtl_ofi.ofi_ctxt[ctxt_id].cq_wait_fd >= 0) {
            opal_progress_set_block_callback(ompi_mtl_ofi_progress_block_no_inline);
        }
        /* Same-OS SC park for wait_sync scarce path (avoid yield→steal). */
        if (ompi_mtl_ofi_hosted_sc_enabled()) {
            opal_progress_set_sc_park_callback(ompi_mtl_ofi_hosted_sc_try_park_pending);
        }
#endif
    }

    ompi_mtl_ofi.comm_to_context[comm->c_index] = ompi_mtl_ofi.total_ctxts_used;
    ompi_mtl_ofi.total_ctxts_used++;

    return OMPI_SUCCESS;

init_error:
    if (ompi_mtl_ofi.ofi_ctxt[ctxt_id].tx_ep) {
        (void) fi_close((fid_t)ompi_mtl_ofi.ofi_ctxt[ctxt_id].tx_ep);
    }

    if (ompi_mtl_ofi.ofi_ctxt[ctxt_id].rx_ep) {
        (void) fi_close((fid_t)ompi_mtl_ofi.ofi_ctxt[ctxt_id].rx_ep);
    }

    if (ompi_mtl_ofi.ofi_ctxt[ctxt_id].cq) {
        (void) fi_close((fid_t)ompi_mtl_ofi.ofi_ctxt[ctxt_id].cq);
    }

    return ret;
}

static int ompi_mtl_ofi_finalize_contexts(struct mca_mtl_base_module_t *mtl,
                                          struct ompi_communicator_t *comm,
                                          mca_mtl_ofi_ep_type ep_type)
{
    int ret = OMPI_SUCCESS, ctxt_id = 0;

    if (OFI_REGULAR_EP == ep_type) {
        /* For regular EPs, simply destruct Lock object and exit */
        goto finalize_regular_ep;
    }

    if (ompi_mtl_ofi.thread_grouping &&
        ompi_mtl_ofi.threshold_comm_context_id &&
        ((uint32_t) ompi_mtl_ofi.threshold_comm_context_id <= comm->c_index)) {
        return OMPI_SUCCESS;
    }

    ctxt_id = ompi_mtl_ofi.thread_grouping ?
           ompi_mtl_ofi.comm_to_context[comm->c_index] : 0;

    /*
     * For regular EPs, TX/RX contexts are aliased to SEP object which is
     * closed in ompi_mtl_ofi_finalize(). So, skip handling those here.
     */
    if ((ret = fi_close((fid_t)ompi_mtl_ofi.ofi_ctxt[ctxt_id].tx_ep))) {
        goto finalize_err;
    }

    if ((ret = fi_close((fid_t)ompi_mtl_ofi.ofi_ctxt[ctxt_id].rx_ep))) {
        goto finalize_err;
    }

    if ((ret = fi_close((fid_t)ompi_mtl_ofi.ofi_ctxt[ctxt_id].cq))) {
        goto finalize_err;
    }

finalize_regular_ep:
    /* Destroy context lock */
    OBJ_DESTRUCT(&ompi_mtl_ofi.ofi_ctxt[ctxt_id].context_lock);

    return OMPI_SUCCESS;

finalize_err:
    opal_show_help("help-mtl-ofi.txt", "OFI call fail", true,
                   "fi_close",
                   ompi_process_info.nodename, __FILE__, __LINE__,
                   fi_strerror(-ret), ret);

    return OMPI_ERROR;
}

int
ompi_mtl_ofi_add_procs(struct mca_mtl_base_module_t *mtl,
                       size_t nprocs,
                       struct ompi_proc_t** procs)
{
    int ret = OMPI_SUCCESS;
    size_t i;
    size_t size;
    size_t namelen;
    int count = 0;
    char *ep_name = NULL;
    fi_addr_t *fi_addrs = NULL;
    mca_mtl_ofi_endpoint_t *endpoint = NULL;
    int num_peers_limit = (1 << ompi_mtl_ofi.num_bits_source_rank) - 1;

    namelen = ompi_mtl_ofi.epnamelen;

    /* We cannot add more ranks than available tag bits */
    if ((false == ompi_mtl_ofi.fi_cq_data) &&
        OPAL_UNLIKELY(((int) (nprocs + ompi_mtl_ofi.num_peers) > num_peers_limit))) {
        opal_output(0, "%s:%d: OFI provider: %s does not have enough bits for source rank in its tag.\n"
                       "Adding more ranks will result in undefined behaviour. Please enable\n"
                       "FI_REMOTE_CQ_DATA feature in the provider. For more info refer fi_cq(3).\n",
                       __FILE__, __LINE__, ompi_mtl_ofi.provider_name);
        fflush(stderr);
        ret = OMPI_ERROR;
        goto bail;
    }

    /**
     * Create array of fi_addrs.
     */
    fi_addrs = malloc(nprocs * sizeof(fi_addr_t));
    if (NULL == fi_addrs) {
        ret = OMPI_ERROR;
        goto bail;
    }

    for (i = 0; i < nprocs; ++i) {
        const char *insert_name = NULL;
#if HAVE_LITHE
        uint32_t magic = 0, n_eps = 0, name_len32 = 0;
        uint32_t slot = 0;
#endif
        /**
         * Retrieve the processes' EP name from modex.
         */
        ret = ompi_mtl_ofi_modex_recv_proc(procs[i], (void **) &ep_name, &size);
        if (OMPI_SUCCESS != ret) {
            char *errhost = opal_get_proc_hostname(&procs[i]->super);
            opal_show_help("help-mtl-ofi.txt", "modex failed",
                           true, ompi_process_info.nodename,
			                     errhost, opal_strerror(ret), ret);
            free(errhost);
            goto bail;
        }

        insert_name = ep_name;
#if HAVE_LITHE
        /*
         * Hosted multi regular-EP modex blob: magic + n + namelen + n names.
         * Select the peer logical slot (vpid % RPH) so each Lithe context
         * addresses a distinct peer EP when SEP is unavailable.
         */
        if (size >= 3 * sizeof(uint32_t)) {
            memcpy(&magic, ep_name, sizeof(magic));
            if (0x4c4d4550u == magic) {
                memcpy(&n_eps, ep_name + sizeof(uint32_t), sizeof(n_eps));
                memcpy(&name_len32, ep_name + 2 * sizeof(uint32_t), sizeof(name_len32));
                if (n_eps >= 1 && name_len32 > 0 &&
                    size == (size_t) (3 * sizeof(uint32_t)) +
                            (size_t) n_eps * (size_t) name_len32) {
                    uint32_t rph = ompi_mtl_ofi_lithe_ranks_per_host();
                    opal_vpid_t v = procs[i]->super.proc_name.vpid;
                    if (rph > 1 && v != OPAL_VPID_INVALID && v != OPAL_VPID_WILDCARD) {
                        slot = (uint32_t) ((uint64_t) v % (uint64_t) rph);
                    }
                    if (slot >= n_eps) {
                        slot %= n_eps;
                    }
                    insert_name = ep_name + 3 * sizeof(uint32_t) +
                                  (size_t) slot * (size_t) name_len32;
                    namelen = (size_t) name_len32;
                    (void) namelen;
                }
            }
        }
#endif

        /**
         * Map the EP name to fi_addr.
         */
        count = fi_av_insert(ompi_mtl_ofi.av, insert_name, 1, &fi_addrs[i], 0, NULL);
        if ((count < 0) || (1 != (size_t)count)) {
            opal_output_verbose(1, opal_common_ofi.output,
                                "%s:%d: fi_av_insert failed for address %s: %d\n",
                                __FILE__, __LINE__, ep_name, count);
            ret = OMPI_ERROR;
            goto bail;
        }
#if HAVE_LITHE
        if (NULL != getenv("LITHE_MTL_OFI_DEBUG")) {
            unsigned long long h = 0;
            size_t bi;
            size_t nlen = (namelen > 0) ? namelen : 8;
            for (bi = 0; bi < nlen && bi < 16; ++bi) {
                h = (h * 131ull) + (unsigned char) insert_name[bi];
            }
            fprintf(stderr,
                    "mtl_ofi add_procs: i=%zu vpid=%u slot=%u magic=0x%x n_eps=%u "
                    "fi_addr=%llu name_hash=%llu size=%zu\n",
                    i, (unsigned) procs[i]->super.proc_name.vpid, (unsigned) slot,
                    (unsigned) magic, (unsigned) n_eps,
                    (unsigned long long) fi_addrs[i], h, size);
            fflush(stderr);
        }
#endif
    }

    /**
     * Store the fi_addrs within the endpoint objects.
     */
    for (i = 0; i < nprocs; ++i) {
        endpoint = OBJ_NEW(mca_mtl_ofi_endpoint_t);
        if (NULL == endpoint) {
            opal_output_verbose(1, opal_common_ofi.output,
                                "%s:%d: mtl/ofi: could not allocate endpoint"
                                " structure\n",
                                __FILE__, __LINE__);
            ret = OMPI_ERROR;
            goto bail;
        }

        endpoint->mtl_ofi_module = &ompi_mtl_ofi;
        endpoint->peer_fiaddr = fi_addrs[i];

        /* FIXME: What happens if this endpoint already exists? */
        procs[i]->proc_endpoints[OMPI_PROC_ENDPOINT_TAG_MTL] = endpoint;
    }

    /* Update global counter of number of procs added to this rank */
    ompi_mtl_ofi.num_peers += nprocs;

    ret = OMPI_SUCCESS;

bail:
    if (fi_addrs)
        free(fi_addrs);

    return ret;
}

int
ompi_mtl_ofi_del_procs(struct mca_mtl_base_module_t *mtl,
                       size_t nprocs,
                       struct ompi_proc_t** procs)
{
    int ret;
    size_t i;
    mca_mtl_ofi_endpoint_t *endpoint = NULL;

    for (i = 0 ; i < nprocs ; ++i) {
        if (NULL != procs[i] &&
            NULL != procs[i]->proc_endpoints[OMPI_PROC_ENDPOINT_TAG_MTL]) {
            endpoint = procs[i]->proc_endpoints[OMPI_PROC_ENDPOINT_TAG_MTL];
            ret = fi_av_remove(ompi_mtl_ofi.av, &endpoint->peer_fiaddr, 1, 0);
            if (ret) {
                opal_output_verbose(1, opal_common_ofi.output,
                        "%s:%d: fi_av_remove failed: %s\n", __FILE__, __LINE__, fi_strerror(errno));
                return ret;
            }
            procs[i]->proc_endpoints[OMPI_PROC_ENDPOINT_TAG_MTL] = NULL;
            OBJ_RELEASE(endpoint);
        }
    }

    return OMPI_SUCCESS;
}

int ompi_mtl_ofi_add_comm(struct mca_mtl_base_module_t *mtl,
                      struct ompi_communicator_t *comm)
{
    int ret = OMPI_SUCCESS;
    uint32_t comm_size;
    mca_mtl_comm_t* mtl_comm;

    mca_mtl_ofi_ep_type ep_type = (0 == ompi_mtl_ofi.enable_sep) ?
                                  OFI_REGULAR_EP : OFI_SCALABLE_EP;

    if (!OMPI_COMM_IS_GLOBAL_INDEX(comm)) {
        mtl_comm = OBJ_NEW(mca_mtl_comm_t);

        if (OMPI_COMM_IS_INTER(comm)) {
            comm_size = ompi_comm_remote_size(comm);
        } else {
            comm_size = ompi_comm_size(comm);
        }
        mtl_comm->c_index_vec = (c_index_vec_t *)malloc(sizeof(c_index_vec_t) * comm_size);
        if (NULL == mtl_comm->c_index_vec) {
            ret = OMPI_ERR_OUT_OF_RESOURCE;
            OBJ_RELEASE(mtl_comm);
            goto error;
        } else {
            for (uint32_t i=0; i < comm_size; i++) {
                mtl_comm->c_index_vec[i].c_index_state = MCA_MTL_OFI_CID_NOT_EXCHANGED;
            }
        }
        if (OMPI_COMM_IS_INTRA(comm)) {
            int my_rank = ompi_comm_rank(comm);
            mtl_comm->c_index_vec[my_rank].c_index = comm->c_index;
            mtl_comm->c_index_vec[my_rank].c_index_state = MCA_MTL_OFI_CID_EXCHANGED;
            if (comm == &ompi_mpi_comm_world.comm) {
                uint32_t ranks_per_host = ompi_mtl_ofi_lithe_ranks_per_host();
                if (ranks_per_host > 1) {
                    uint32_t base_rank = (uint32_t) my_rank / ranks_per_host * ranks_per_host;
                    uint32_t end_rank = base_rank + ranks_per_host;
                    if (end_rank > comm_size) {
                        end_rank = comm_size;
                    }
                    for (uint32_t rank = base_rank; rank < end_rank; ++rank) {
                        mtl_comm->c_index_vec[rank].c_index = comm->c_index;
                        mtl_comm->c_index_vec[rank].c_index_state = MCA_MTL_OFI_CID_EXCHANGED;
                    }
                }
            }
        }

        comm->c_mtl_comm = mtl_comm;

    } else  {

        comm->c_mtl_comm = NULL;

    }

    /*
     * If thread grouping enabled, add new OFI context for each communicator
     * other than MPI_COMM_SELF.
     */
    if ((ompi_mtl_ofi.thread_grouping && (MPI_COMM_SELF != comm)) ||
        /* If no thread grouping, add new OFI context only
         * for MPI_COMM_WORLD.
         */
        (!ompi_mtl_ofi.thread_grouping && (!ompi_mtl_ofi.is_initialized))) {
        int ctxts_to_init = 1;

#if HAVE_LITHE
        if (!ompi_mtl_ofi.thread_grouping && comm == &ompi_mpi_comm_world.comm) {
            uint32_t rph = ompi_mtl_ofi_lithe_ranks_per_host();
            if (rph > 1) {
                ctxts_to_init = (int) rph;
                if (ctxts_to_init > ompi_mtl_ofi.num_ofi_contexts) {
                    ctxts_to_init = ompi_mtl_ofi.num_ofi_contexts;
                }
            }
        }
#endif

        for (int c = 0; c < ctxts_to_init; c++) {
            ret = ompi_mtl_ofi_init_contexts(mtl, comm, ep_type);
            if (OMPI_SUCCESS != ret) {
                goto error;
            }
        }
        ompi_mtl_ofi.is_initialized = true;
#if HAVE_LITHE
        if (NULL != getenv("LITHE_MTL_OFI_DEBUG")) {
            fprintf(stderr,
                    "mtl_ofi hosted: rph=%u num_ofi_contexts=%d total_ctxts_used=%d "
                    "enable_sep=%d hosted_multi_ep=%d ctxts_to_init=%d\n",
                    (unsigned) ompi_mtl_ofi_lithe_ranks_per_host(),
                    ompi_mtl_ofi.num_ofi_contexts, ompi_mtl_ofi.total_ctxts_used,
                    ompi_mtl_ofi.enable_sep,
#if HAVE_LITHE
                    ompi_mtl_ofi.hosted_multi_ep,
#else
                    0,
#endif
                    ctxts_to_init);
            fflush(stderr);
        }
#endif
    }

error:
    return ret;
}

int ompi_mtl_ofi_del_comm(struct mca_mtl_base_module_t *mtl,
                          struct ompi_communicator_t *comm)
{
    int ret = OMPI_SUCCESS;
    mca_mtl_ofi_ep_type ep_type = (0 == ompi_mtl_ofi.enable_sep) ?
                                  OFI_REGULAR_EP : OFI_SCALABLE_EP;

    if(NULL != comm->c_mtl_comm) {
        free(comm->c_mtl_comm->c_index_vec);
        OBJ_RELEASE(comm->c_mtl_comm);
        comm->c_mtl_comm = NULL;
    }

    /*
     * Clean up OFI contexts information.
     */
    if ((ompi_mtl_ofi.thread_grouping && (MPI_COMM_SELF != comm)) ||
        (!ompi_mtl_ofi.thread_grouping && (MPI_COMM_WORLD == comm))) {

        ret = ompi_mtl_ofi_finalize_contexts(mtl, comm, ep_type);
    }

    return ret;
}

