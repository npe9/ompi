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
#include <lithe/condvar.h>
#include <lithe/fork_join_sched.h>
#include <lithe/mutex.h>
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
 * processes (1 cross-OS round for P=2). Non-leaders yield frequently so the
 * leader gets a hart under MTP=1; rare opal_progress assists CQ.
 * Auto-on when RPH>=4 (unset LITHE_HOSTED_SC_FLAT_MULTI_OS); RPH=2 stays RD
 * (P2K2 ~15–18µs). Force with =1 / disable with =0.
 *
 * LITHE_HOSTED_SC_FLAT_COLL=0 disables all flat/hybrid (fallback RD).
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

#if HAVE_LITHE
/*
 * Multi-OS hybrid gate: after local ticket arrive, non-leaders park (condvar)
 * while the leader runs cross-OS RD. Frees harts for leader/CQ under soft_cap
 * (target: P2N2K4 ≤2–3× van). Protocol: all snapshot done_gen after the
 * ticket barrier; leader bumps+broadcasts after RD; late non-leaders see
 * gen advanced and skip wait (no lost-wakeup).
 * Opt-in: LITHE_HOSTED_SC_COLL_GATE=1. Default OFF — condvar park added
 * ~30µs and a hang (P2N2K4 med ~129 vs ~93 yield-spin; 1/12 timeout).
 */
typedef struct {
    lithe_mutex_t mu;
    lithe_condvar_t cv;
    int32_t done_gen;
    int ready;
} sc_coll_gate_t;

static sc_coll_gate_t sc_gate;

/*
 * Gate (non-leaders condvar-park during leader RD):
 * Opt-in LITHE_HOSTED_SC_COLL_GATE=1 only. Default OFF — on P2N2K4 park
 * added ~30µs + hang risk; on P4N2K4 auto-on also inflated DOT (~0.035 vs
 * ~0.033) in early §1.3h samples. P>=4 hart/CQ starve is handled by
 * with_progress==2 (periodic yield + rare opal_progress) and by routing
 * PRE_INIT off world Allreduce in miniFE.
 */
static int sc_coll_gate_wanted(int num_os)
{
    static int v = -1;
    (void) num_os;
    if (v < 0) {
        const char *e = getenv("LITHE_HOSTED_SC_COLL_GATE");
        v = (e && e[0] == '1' && e[1] == '\0') ? 1 : 0;
    }
    return v;
}

static void sc_coll_gate_ensure(void)
{
    static opal_atomic_int32_t once = 0;

    if (sc_gate.ready) {
        return;
    }
    if (0 == opal_atomic_swap_32(&once, 1)) {
        lithe_mutex_init(&sc_gate.mu, NULL);
        lithe_condvar_init(&sc_gate.cv);
        sc_gate.done_gen = 0;
        opal_atomic_wmb();
        sc_gate.ready = 1;
    } else {
        while (!sc_gate.ready) {
            cpu_relax();
        }
    }
}

static void sc_coll_gate_wait(int32_t expect)
{
    sc_coll_gate_ensure();
    lithe_mutex_lock(&sc_gate.mu);
    while (sc_gate.done_gen == expect) {
        lithe_condvar_wait(&sc_gate.cv, &sc_gate.mu);
    }
    lithe_mutex_unlock(&sc_gate.mu);
}

static void sc_coll_gate_release(int32_t expect)
{
    sc_coll_gate_ensure();
    lithe_mutex_lock(&sc_gate.mu);
    if (sc_gate.done_gen == expect) {
        sc_gate.done_gen = expect + 1;
        lithe_condvar_broadcast(&sc_gate.cv);
    }
    lithe_mutex_unlock(&sc_gate.mu);
}

static int32_t sc_coll_gate_snapshot(void)
{
    sc_coll_gate_ensure();
    return sc_gate.done_gen;
}
#endif /* HAVE_LITHE */

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
     * Multi-OS hybrid (local ticket + leader RD):
     * - LITHE_HOSTED_SC_FLAT_MULTI_OS=1 → force on
     * - LITHE_HOSTED_SC_FLAT_MULTI_OS=0 → force off
     * - unset: auto-on when RPH>=4 (P2K4/P2K8); keep RD for P2K2 (RPH=2)
     *   where hybrid previously regressed median (~250µs vs ~15µs RD).
     * Power-of-two OS count required for leader RD.
     */
    if (!L->single_os) {
        const char *mos = getenv("LITHE_HOSTED_SC_FLAT_MULTI_OS");
        int enable = 0;
        if (NULL != mos) {
            if ('1' == mos[0] && '\0' == mos[1]) {
                enable = 1;
            } else if ('0' == mos[0] && '\0' == mos[1]) {
                enable = 0;
            }
        } else if (L->rph >= 4UL) {
            enable = 1;
        }
        if (!enable) {
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

/*
 * with_progress:
 *   0 — single-OS (yield only to RUNNABLE peers)
 *   1 — multi-OS P=2 (MTP-aware yield; no opal_progress — P2K4 floor)
 *   2 — multi-OS P>=4 (P4N2K4): also periodic yield when leader is in
 *       NO_CQ nanosleep (not RUNNABLE) so waiters do not hoard harts;
 *       rare opal_progress assists same-OS CQ without the every-64 tax.
 */
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
         * Multi-OS hybrid (with_progress): non-leaders wait while the leader
         * does cross-OS Sendrecv. MTP=1 puts the progress manager on one
         * context — if a non-leader holds the only hart and only cpu_relax'es,
         * the leader never runs (hang). Prefer frequent yield so the leader
         * gets a hart; rare opal_progress assists CQ without the every-64
         * progress tax that pushed P2K4 hybrid median to ~200µs+.
         *
         * Single-OS: prefer cpu_relax, but if a peer is RUNNABLE (e.g. still
         * in pairwise RD-SUM after we arrived), donate the hart. With
         * HELPER soft_cap=RPH+1, should_yield stays false while owned<budget
         * so a yielded straggler never gets a hart unless barrier waiters
         * yield — P1K8/P1K16 pairwise flakes. Only yield after we have
         * arrived (ticket taken); unconditional yield-before-arrive hung
         * P1K4 under MTP=RPH.
         */
#if HAVE_LITHE
        if (with_progress) {
            /*
             * Non-leaders wait while leader does cross-OS Sendrecv.
             * MTP=1: yield every 16 spins so the sole progress manager
             * (often the leader) gets a hart.
             * MTP>=2 (multi-node default): yield when should_yield OR a peer
             * is RUNNABLE — every-16 yield taxed P2N2K4 ~+30µs, but
             * should_yield-only stranded pairwise RD-SUM (7/8 OK, one
             * waiter never got a hart while ticket peers cpu_relax'd).
             * Do NOT call opal_progress on P=2 (P2K4 ~200–400µs floor).
             *
             * P>=4 (with_progress==2): leader often sits in NO_CQ
             * clock_nanosleep — not RUNNABLE — so runnable-only yield
             * lets waiters cpu_relax-hoard harts (DOT inflate + PRE_INIT
             * ec142). Yield every 64 spins; opal_progress every 256.
             */
            static int mtp_cached = -1;
            if (mtp_cached < 0) {
                const char *me = getenv("OMPI_MCA_opal_max_thread_in_progress");
                int m = (me && *me) ? atoi(me) : 1;
                mtp_cached = (m >= 2) ? 2 : 1;
            }
            if (with_progress >= 2) {
                if (lithe_fork_join_should_yield_to_runnable() ||
                    lithe_fork_join_current_runnable_count() > 0 ||
                    0 == (spin & 63U)) {
                    lithe_context_yield();
                } else if (0 == (spin & 255U)) {
                    (void) opal_progress();
                } else {
                    cpu_relax();
                }
            } else if (mtp_cached >= 2) {
                if (lithe_fork_join_should_yield_to_runnable() ||
                    lithe_fork_join_current_runnable_count() > 0) {
                    lithe_context_yield();
                } else {
                    cpu_relax();
                }
            } else if (0 == (spin & 15U)) {
                lithe_context_yield();
            } else {
                cpu_relax();
            }
        } else if (lithe_fork_join_should_yield_to_runnable()) {
            lithe_context_yield();
        } else if (lithe_fork_join_current_runnable_count() > 0) {
            /* Spare-hart soft_cap: should_yield false but peer needs a hart. */
            lithe_context_yield();
        } else {
            cpu_relax();
        }
        spin++;
#else
        if (with_progress && (0 == (spin & 63U))) {
            (void) opal_progress();
        }
        (void) spin;
        break;
#endif
    } while (1);
    opal_atomic_rmb();
}

/* Progress mode for multi-OS hybrid ticket waits (see sc_coll_ticket_barrier). */
static int sc_coll_multi_os_progress_mode(const sc_coll_layout_t *L)
{
    return (L->num_os >= 4) ? 2 : 1;
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
    {
        const int prog = sc_coll_multi_os_progress_mode(&L);
        sc_coll_ticket_barrier((int) L.rph, prog);
#if HAVE_LITHE
        if (sc_coll_gate_wanted(L.num_os)) {
            int32_t snap = sc_coll_gate_snapshot();
            if (L.is_leader) {
                rc = sc_coll_leaders_barrier(comm, &L);
                sc_coll_gate_release(snap);
            } else {
                sc_coll_gate_wait(snap);
                rc = MPI_SUCCESS;
            }
        } else {
            rc = sc_coll_leaders_barrier(comm, &L);
        }
#else
        rc = sc_coll_leaders_barrier(comm, &L);
#endif
        if (MPI_SUCCESS != rc) {
            return rc;
        }
        sc_coll_ticket_barrier((int) L.rph, prog);
    }
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
    {
        const int prog = sc_coll_multi_os_progress_mode(&L);
        sc_coll_ticket_barrier((int) L.rph, prog);
#if HAVE_LITHE
        {
            int32_t snap = 0;
            int use_gate = sc_coll_gate_wanted(L.num_os);
            if (use_gate) {
                snap = sc_coll_gate_snapshot();
            }
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
                    if (use_gate) {
                        sc_coll_gate_release(snap);
                    }
                    return ret;
                }
                ret = ompi_datatype_copy_content_same_ddt(datatype, count,
                                                          (char *) sc_coll.slots[base],
                                                          (char *) acc);
                if (ret < 0) {
                    if (use_gate) {
                        sc_coll_gate_release(snap);
                    }
                    return OMPI_ERROR;
                }
                opal_atomic_wmb();
                if (use_gate) {
                    sc_coll_gate_release(snap);
                }
            } else if (use_gate) {
                sc_coll_gate_wait(snap);
            }
        }
#else
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
#endif
        sc_coll_ticket_barrier((int) L.rph, prog);
        ret = ompi_datatype_copy_content_same_ddt(datatype, count, (char *) recvbuf,
                                                  (char *) sc_coll.slots[L.leader_rank]);
        if (ret < 0) {
            return OMPI_ERROR;
        }
        sc_coll_ticket_barrier((int) L.rph, prog);
    }
    return MPI_SUCCESS;
}
