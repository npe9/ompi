/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2026      LLNL.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

#include "opal/class/opal_object.h"
#include "opal/mca/threads/mutex.h"
#include "opal/runtime/opal_progress.h"
#include "opal/sys/atomic.h"
#include "opal/util/proc.h"

#include "ompi/communicator/communicator.h"
#include "ompi/datatype/ompi_datatype.h"
#include "ompi/mca/coll/base/coll_base_util.h"
#include "ompi/mca/coll/base/coll_tags.h"
#include "ompi/mca/pml/pml.h"
#include "ompi/op/op.h"
#include "ompi/proc/proc.h"
#include "ompi/request/request.h"
#include "ompi/runtime/ompi_lithe_world_coll.h"
#include "ompi/runtime/ompi_rte.h"

#if HAVE_LITHE
#include <lithe/fork_join_sched.h>
#include <parlib/arch.h> /* cpu_relax */
#endif

static int world_coll_mutex_ready = 0;
static opal_recursive_mutex_t world_coll_mutex;

static bool lithe_world_coll_wanted(void)
{
    const char *lithe_ctx = getenv("OMPI_LITHE_CONTEXT_LOCAL_PROC");
    const char *serialize = getenv("OMPI_LITHE_WORLD_COLL_SERIALIZE");

    if (NULL == lithe_ctx || '\0' == lithe_ctx[0]) {
        return false;
    }
    if (NULL == serialize || '\0' == serialize[0]) {
        return false;
    }
    if ('0' == serialize[0] && '\0' == serialize[1]) {
        return false;
    }
    return true;
}

static bool comm_is_world(MPI_Comm comm)
{
    return ((void *)comm) == ((void *)&ompi_mpi_comm_world);
}

void ompi_lithe_world_coll_mpi_init_hook(void)
{
    if (!lithe_world_coll_wanted()) {
        return;
    }

    OBJ_CONSTRUCT(&world_coll_mutex, opal_recursive_mutex_t);
    world_coll_mutex_ready = 1;
}

void ompi_lithe_world_coll_mpi_finalize_hook(void)
{
    if (!world_coll_mutex_ready) {
        return;
    }

    OBJ_DESTRUCT(&world_coll_mutex);
    world_coll_mutex_ready = 0;
}

void ompi_lithe_world_coll_lock(MPI_Comm comm)
{
    if (!world_coll_mutex_ready || !comm_is_world(comm)) {
        return;
    }

    opal_mutex_lock(&world_coll_mutex);
}

void ompi_lithe_world_coll_unlock(MPI_Comm comm)
{
    if (!world_coll_mutex_ready || !comm_is_world(comm)) {
        return;
    }

    opal_mutex_unlock(&world_coll_mutex);
}

/*
 * Hosted Lithe flat Barrier / Allreduce.
 *
 * Same-OS (P1K*): ticket barrier + local reduce — cuts RD Sendrecv/SC match
 * rounds (P1K4 ~41µs → ~7µs).
 *
 * Multi-OS (P2K*): hybrid — local ticket barrier/reduce among the RPH
 * co-resident ranks, then leader-only recursive-doubling Sendrecv across OS
 * processes (1 cross-OS round for P=2). Non-leaders spin on the next local
 * ticket (leader self-progresses PML). Regular packing only: size % RPH == 0
 * and contiguous vpids per OS.
 *
 * LITHE_HOSTED_SC_FLAT_COLL=0 disables (fallback RD).
 */
#ifndef OMPI_LITHE_SC_COLL_MAX_RANKS
#define OMPI_LITHE_SC_COLL_MAX_RANKS 64
#endif
#ifndef OMPI_LITHE_SC_COLL_MAX_BYTES
#define OMPI_LITHE_SC_COLL_MAX_BYTES 4096
#endif

typedef struct {
    /* Monotonic arrival tickets (never reset). Cohort goal = ceil(ticket/size)*size.
     * Avoids sense-reversing capture-before-arrive races that dropped P1K4 sums. */
    opal_atomic_int32_t arrived;
    unsigned char *slots[OMPI_LITHE_SC_COLL_MAX_RANKS];
    size_t slot_bytes;
    int ready;
} ompi_lithe_sc_coll_state_t;

/* Static arena: avoids first-touch malloc races when K contexts enter together. */
static unsigned char sc_coll_arena[OMPI_LITHE_SC_COLL_MAX_RANKS * OMPI_LITHE_SC_COLL_MAX_BYTES];
static ompi_lithe_sc_coll_state_t sc_coll;

static bool sc_flat_coll_enabled(void)
{
    const char *e = getenv("LITHE_HOSTED_SC_FLAT_COLL");
    if (NULL != e && '0' == e[0] && '\0' == e[1]) {
        return false;
    }
    return true;
}

typedef struct {
    unsigned long rph;
    int size;
    int rank;
    int local_slot;   /* vpid % rph */
    int leader_rank;  /* os_id * rph */
    int num_os;
    int is_leader;
    int single_os;    /* whole world on this OS */
} sc_coll_layout_t;

static bool sc_coll_layout(struct ompi_communicator_t *comm, sc_coll_layout_t *L)
{
    int r, os_counts[OMPI_LITHE_SC_COLL_MAX_RANKS];
    unsigned long rph;
    opal_vpid_t me, my_os, peer_os, expect;
    ompi_proc_t *peer;

    memset(L, 0, sizeof(*L));
    if (!ompi_rte_lithe_hosted_multicontext_active || !OMPI_COMM_IS_INTRA(comm)) {
        return false;
    }
    if (!opal_lithe_env_cache_active()) {
        return false;
    }
    rph = opal_lithe_env_cache_rph();
    if (rph < 2UL || rph > (unsigned long) OMPI_LITHE_SC_COLL_MAX_RANKS) {
        return false;
    }
    L->size = ompi_comm_size(comm);
    if (L->size < 2 || L->size > OMPI_LITHE_SC_COLL_MAX_RANKS) {
        return false;
    }
    if ((L->size % (int) rph) != 0) {
        return false;
    }
    L->rph = rph;
    L->num_os = L->size / (int) rph;
    L->rank = ompi_comm_rank(comm);
    me = opal_proc_local_get()->proc_name.vpid;
    my_os = me / (opal_vpid_t) rph;
    L->local_slot = (int) (me % (opal_vpid_t) rph);
    L->leader_rank = (int) (my_os * (opal_vpid_t) rph);
    L->is_leader = (L->local_slot == 0);
    L->single_os = (L->num_os == 1);

    /* Contiguous regular packing: rank r lives on OS floor(r/rph) with
     * vpid == r (hosted COMM_WORLD convention). */
    memset(os_counts, 0, sizeof(os_counts));
    for (r = 0; r < L->size; ++r) {
        peer = ompi_comm_peer_lookup(comm, r);
        if (NULL == peer) {
            return false;
        }
        expect = (opal_vpid_t) r;
        if (peer->super.proc_name.vpid != expect) {
            return false;
        }
        peer_os = expect / (opal_vpid_t) rph;
        if (peer_os >= (opal_vpid_t) L->num_os) {
            return false;
        }
        os_counts[peer_os]++;
    }
    for (r = 0; r < L->num_os; ++r) {
        if (os_counts[r] != (int) rph) {
            return false;
        }
    }
    /*
     * Multi-OS hybrid (local ticket + leader RD) is OPT-IN only.
     * Default keeps RD+SC/OFI: P2K2 ~15µs held; P2K4 ~170µs.
     * LITHE_HOSTED_SC_FLAT_MULTI_OS=1 enables hybrid — can occasionally hit
     * ~20µs on P2K8 but median regresses (P2K2 ~250µs, P2K4 ~200µs+) until
     * cross-OS leader progress/CQ park is sorted. Power-of-two OS count.
     */
    if (!L->single_os) {
        const char *mos = getenv("LITHE_HOSTED_SC_FLAT_MULTI_OS");
        if (NULL == mos || '1' != mos[0] || '\0' != mos[1]) {
            return false;
        }
        if (L->num_os < 2 || (L->num_os & (L->num_os - 1)) != 0) {
            return false;
        }
    }
    return true;
}

/* 0-byte pairwise sync for leader barrier (mirrors coll_base barrier). */
static int sc_coll_sendrecv_zero(struct ompi_communicator_t *comm, int peer, int tag)
{
    int rc;
    ompi_request_t *req = MPI_REQUEST_NULL;

    rc = MCA_PML_CALL(irecv(NULL, 0, MPI_BYTE, peer, tag, comm, &req));
    if (MPI_SUCCESS != rc) {
        return rc;
    }
    rc = MCA_PML_CALL(send(NULL, 0, MPI_BYTE, peer, tag, MCA_PML_BASE_SEND_STANDARD,
                           comm));
    if (MPI_SUCCESS != rc) {
        ompi_request_free(&req);
        return rc;
    }
    return ompi_request_wait(&req, MPI_STATUS_IGNORE);
}

static int sc_coll_leaders_barrier(struct ompi_communicator_t *comm,
                                   const sc_coll_layout_t *L)
{
    int dist, peer_os, peer_rank, rc;

    if (!L->is_leader) {
        return MPI_SUCCESS;
    }
    for (dist = 1; dist < L->num_os; dist <<= 1) {
        peer_os = (int) ((L->leader_rank / (int) L->rph) ^ dist);
        peer_rank = peer_os * (int) L->rph;
        rc = sc_coll_sendrecv_zero(comm, peer_rank, MCA_COLL_BASE_TAG_BARRIER);
        if (MPI_SUCCESS != rc) {
            return rc;
        }
    }
    return MPI_SUCCESS;
}

static int sc_coll_leaders_allreduce(void *buf, int count, MPI_Datatype datatype,
                                     MPI_Op op, struct ompi_communicator_t *comm,
                                     const sc_coll_layout_t *L)
{
    int dist, peer_os, peer_rank, rc, my_os;
    unsigned char tmp[OMPI_LITHE_SC_COLL_MAX_BYTES];
    ptrdiff_t span, gap = 0;

    if (!L->is_leader) {
        return MPI_SUCCESS;
    }
    span = opal_datatype_span(&datatype->super, count, &gap);
    if (span <= 0 || (size_t) span > OMPI_LITHE_SC_COLL_MAX_BYTES) {
        return OMPI_ERR_NOT_AVAILABLE;
    }
    (void) gap;
    my_os = L->leader_rank / (int) L->rph;
    for (dist = 1; dist < L->num_os; dist <<= 1) {
        peer_os = my_os ^ dist;
        peer_rank = peer_os * (int) L->rph;
        rc = ompi_coll_base_sendrecv_actual(buf, count, datatype, peer_rank,
                                            MCA_COLL_BASE_TAG_ALLREDUCE, tmp,
                                            count, datatype, peer_rank,
                                            MCA_COLL_BASE_TAG_ALLREDUCE, comm,
                                            MPI_STATUS_IGNORE);
        if (MPI_SUCCESS != rc) {
            return rc;
        }
        /* Keep reduce order stable: lower OS applies first (like RD). */
        if (my_os < peer_os) {
            ompi_op_reduce(op, buf, tmp, count, datatype);
            memcpy(buf, tmp, (size_t) span);
        } else {
            ompi_op_reduce(op, tmp, buf, count, datatype);
        }
    }
    return MPI_SUCCESS;
}

static int sc_coll_ensure_arena(size_t nbytes)
{
    int i;

    if (nbytes > OMPI_LITHE_SC_COLL_MAX_BYTES) {
        return OMPI_ERR_NOT_AVAILABLE;
    }
    if (sc_coll.ready) {
        return OMPI_SUCCESS;
    }
    sc_coll.slot_bytes = OMPI_LITHE_SC_COLL_MAX_BYTES;
    for (i = 0; i < OMPI_LITHE_SC_COLL_MAX_RANKS; ++i) {
        sc_coll.slots[i] = sc_coll_arena + (size_t) i * sc_coll.slot_bytes;
    }
    opal_atomic_swap_32(&sc_coll.arrived, 0);
    opal_atomic_wmb();
    sc_coll.ready = 1;
    return OMPI_SUCCESS;
}

static void sc_coll_ticket_barrier(int size, int with_progress)
{
    int32_t ticket, goal, cur;
    unsigned int spin = 0;

    ticket = opal_atomic_add_fetch_32(&sc_coll.arrived, 1);
    goal = ((ticket + (int32_t) size - 1) / (int32_t) size) * (int32_t) size;
    /* Must atomic-load each spin: a plain cast read can be cached in a
     * register and release early → peers reduce before all slots publish
     * (P1K4 rank0 sum=4, others sum=2/3). */
    do {
        cur = opal_atomic_add_fetch_32(&sc_coll.arrived, 0);
        if (cur >= goal) {
            break;
        }
        /*
         * Multi-OS: non-leaders wait here while the leader does cross-OS
         * Sendrecv; without opal_progress the shared CQ park starves (~10×
         * P2K2 regression). Throttle: every-spin opal_progress taxed P2K2.
         *
         * Single-OS: prefer cpu_relax, but if a peer is RUNNABLE (e.g. still
         * in pairwise RD-SUM after we arrived), donate the hart. With
         * HELPER soft_cap=RPH+1, should_yield stays false while owned<budget
         * so a yielded straggler never gets a hart unless barrier waiters
         * yield — P1K8/P1K16 pairwise flakes. Only yield after we have
         * arrived (ticket taken); unconditional yield-before-arrive hung
         * P1K4 under MTP=RPH.
         */
        if (with_progress && (0 == (spin & 63U))) {
            (void) opal_progress();
        }
#if HAVE_LITHE
        if (!with_progress && lithe_fork_join_should_yield_to_runnable()) {
            lithe_context_yield();
        } else if (!with_progress && lithe_fork_join_current_runnable_count() > 0) {
            /* Spare-hart soft_cap: should_yield false but peer needs a hart. */
            lithe_context_yield();
        } else {
            cpu_relax();
        }
        spin++;
#else
        (void) spin;
        break;
#endif
    } while (1);
    opal_atomic_rmb();
}

int ompi_lithe_hosted_sc_coll_barrier(MPI_Comm comm)
{
    sc_coll_layout_t L;
    int rc;

    if (!sc_flat_coll_enabled() || !sc_coll_layout(comm, &L)) {
        return OMPI_ERR_NOT_AVAILABLE;
    }
    if (OMPI_SUCCESS != sc_coll_ensure_arena(1)) {
        return OMPI_ERR_NOT_AVAILABLE;
    }
    if (L.single_os) {
        sc_coll_ticket_barrier(L.size, 0);
        return MPI_SUCCESS;
    }
    /* Multi-OS hybrid: local arrive → leader cross-OS sync → local release. */
    sc_coll_ticket_barrier((int) L.rph, 1);
    rc = sc_coll_leaders_barrier(comm, &L);
    if (MPI_SUCCESS != rc) {
        return rc;
    }
    sc_coll_ticket_barrier((int) L.rph, 1);
    return MPI_SUCCESS;
}

int ompi_lithe_hosted_sc_coll_allreduce(const void *sendbuf, void *recvbuf,
                                        int count, MPI_Datatype datatype,
                                        MPI_Op op, MPI_Comm comm)
{
    sc_coll_layout_t L;
    int r, ret, base;
    ptrdiff_t span, gap = 0;
    const void *src;
    void *acc;

    if (!sc_flat_coll_enabled() || !sc_coll_layout(comm, &L)) {
        return OMPI_ERR_NOT_AVAILABLE;
    }
    if (MPI_OP_NULL == op || NULL == datatype) {
        return OMPI_ERR_NOT_AVAILABLE;
    }
    span = opal_datatype_span(&datatype->super, count, &gap);
    if (span <= 0 || (size_t) span > OMPI_LITHE_SC_COLL_MAX_BYTES) {
        return OMPI_ERR_NOT_AVAILABLE;
    }
    ret = sc_coll_ensure_arena((size_t) span);
    if (OMPI_SUCCESS != ret) {
        return OMPI_ERR_NOT_AVAILABLE;
    }

    src = (MPI_IN_PLACE == sendbuf) ? recvbuf : sendbuf;
    (void) gap;
    ret = ompi_datatype_copy_content_same_ddt(datatype, count,
                                              (char *) sc_coll.slots[L.rank],
                                              (char *) src);
    if (ret < 0) {
        return OMPI_ERR_NOT_AVAILABLE;
    }
    opal_atomic_wmb();

    if (L.single_os) {
        sc_coll_ticket_barrier(L.size, 0);
        acc = recvbuf;
        ret = ompi_datatype_copy_content_same_ddt(datatype, count, (char *) acc,
                                                  (char *) sc_coll.slots[0]);
        if (ret < 0) {
            return OMPI_ERROR;
        }
        for (r = 1; r < L.size; ++r) {
            ompi_op_reduce(op, sc_coll.slots[r], acc, count, datatype);
        }
        sc_coll_ticket_barrier(L.size, 0);
        return MPI_SUCCESS;
    }

    /* Multi-OS: local reduce → leader RD → broadcast via leader slot. */
    sc_coll_ticket_barrier((int) L.rph, 1);
    if (L.is_leader) {
        base = L.leader_rank;
        acc = recvbuf;
        ret = ompi_datatype_copy_content_same_ddt(datatype, count, (char *) acc,
                                                  (char *) sc_coll.slots[base]);
        if (ret < 0) {
            return OMPI_ERROR;
        }
        for (r = 1; r < (int) L.rph; ++r) {
            ompi_op_reduce(op, sc_coll.slots[base + r], acc, count, datatype);
        }
        ret = sc_coll_leaders_allreduce(acc, count, datatype, op, comm, &L);
        if (MPI_SUCCESS != ret) {
            return ret;
        }
        ret = ompi_datatype_copy_content_same_ddt(datatype, count,
                                                  (char *) sc_coll.slots[base],
                                                  (char *) acc);
        if (ret < 0) {
            return OMPI_ERROR;
        }
        opal_atomic_wmb();
    }
    sc_coll_ticket_barrier((int) L.rph, 1);
    ret = ompi_datatype_copy_content_same_ddt(datatype, count, (char *) recvbuf,
                                              (char *) sc_coll.slots[L.leader_rank]);
    if (ret < 0) {
        return OMPI_ERROR;
    }
    sc_coll_ticket_barrier((int) L.rph, 1);
    return MPI_SUCCESS;
}
