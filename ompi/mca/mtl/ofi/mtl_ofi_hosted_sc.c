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
#include "opal/mca/threads/wait_sync.h"
#include <limits.h>

#include "opal/class/opal_list.h"
#include "opal/class/opal_lifo.h"
#include "opal/mca/threads/mutex.h"
#include "opal/runtime/opal_progress.h"
#include "opal/util/output.h"
#include "opal/util/proc.h"

#include <lithe/fork_join_sched.h>
#include <lithe/lithe.h>
#include <lithe/futex.h>
#include <lithe/sched_trace.h>
#include <time.h>
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
    /* LITHE_SC_RNDV: buf is the sender's own buffer (not owned, never freed). */
    int borrowed;
    /* LITHE_SC_PULL: the matched receive the receiver copies into (on slot->pulls). */
    ompi_mtl_ofi_request_t *rreq;
    void *rdst;
    ompi_mtl_ofi_request_t *send_req;
    /* Sync-send Lithe context parked until matching recv takes this UE. */
    lithe_context_t *send_waiter;
    /* LITHE_SC_FREELIST: payloads up to SC_UE_INLINE bytes live here (buf == inl). */
    char inl[];
} ompi_mtl_ofi_hosted_sc_ue_t;

OBJ_CLASS_INSTANCE(ompi_mtl_ofi_hosted_sc_ue_t, opal_list_item_t, NULL, NULL);

typedef struct {
    opal_mutex_t lock;
    opal_list_t posted;
    opal_list_t unexpected;
    /* LITHE_SC_FREELIST: recycled posted / unexpected entries of THIS slot's
     * receiver (the sender takes a UE from, and releases a posted entry to, the
     * destination slot; the receiver does the reverse), lock-free. */
    opal_lifo_t pr_free;
    opal_lifo_t ue_free;
    opal_atomic_lock_t spin; /* LITHE_SC_SPINLOCK: used instead of `lock` */
    /* LITHE_SC_PULL: matched large messages the receiver copies itself, and whether
     * the receiver is spinning in a direct wait on a hart right now. */
    opal_list_t pulls;
    opal_atomic_int32_t npull;
    volatile int32_t spinning;
    /* LITHE_PML_CM_NOSPIN_PROBE: blocked MPI_Probe / Mprobe of THIS slot's rank
     * sleep on probe_seq (a Lithe futex word); a sender that appends to
     * `unexpected` bumps it and wakes them when probe_waiters != 0. */
    volatile int32_t probe_seq;
    opal_atomic_int32_t probe_waiters;
} ompi_mtl_ofi_hosted_sc_slot_t;

/* LITHE_SC_SPINLOCK (2026-10-21, default ON, =0 restores the opal_mutex, a Lithe MCS
 * spin-then-block mutex): guard the slot queues with a plain spinlock. Every
 * critical section is short and never yields; the sync-send park releases the lock
 * from the block callback, which a spinlock allows too. A/B with real locks (4 reps,
 * 3 harts unless noted): halo 4 KiB 5.2-5.6 -> 4.9-5.0 us, 4 harts 3.6-4.0 -> 2.24-2.26,
 * 8 contexts 7.0-7.5 -> 6.3-6.8; allreduce, miniFE 32^3, MPI+OpenMP 16^3 unchanged;
 * ranks K=8 8.81 vs 8.75 ms/iter (8 reps, overlapping). */
static int sc_spin = 1;
/* 82930e332d's global replace made these wrappers call themselves; gcc deleted the
 * infinite recursion, so the slot queues ran UNLOCKED by default until 2026-10-21
 * afternoon. The first spinlock A/B's "mutex" arm was really "no lock" (it found no gain); it and
 * every hosted SC timing taken with that build are void. */
static inline void sc_lock(ompi_mtl_ofi_hosted_sc_slot_t *slot)
{
    if (sc_spin) {
        opal_atomic_lock(&slot->spin);
    } else {
        opal_mutex_lock(&slot->lock);
    }
}
static inline void sc_unlock(ompi_mtl_ofi_hosted_sc_slot_t *slot)
{
    if (sc_spin) {
        opal_atomic_unlock(&slot->spin);
    } else {
        opal_mutex_unlock(&slot->lock);
    }
}

typedef struct {
    ompi_mtl_ofi_hosted_sc_slot_t *slot;
    ompi_mtl_ofi_hosted_sc_ue_t *ue;
    ompi_mtl_ofi_request_t *sreq;
} sc_ssend_park_arg_t;

/* LITHE_SC_TRACE=1 (debug, default off): ring of every SC send/recv decision,
 * read with gdb after a hang (GAPS B33). op: 1 send->posted, 2 send->UE,
 * 3 recv->UE, 4 recv posted, 5 send dropped stale posted. */
typedef struct { int op, me, peer, tag, cid; void *req; } sc_trace_t;
#define SC_TRACE_N 65536
sc_trace_t ompi_mtl_ofi_sc_trace[SC_TRACE_N];
opal_atomic_int64_t ompi_mtl_ofi_sc_trace_i = 0;
static int sc_trace_on = -1;
static inline void sc_trace(int op, int me, int peer, int tag, int cid, void *req)
{
    if (sc_trace_on < 0) {
        const char *e = getenv("LITHE_SC_TRACE");
        sc_trace_on = (e && e[0] == '1');
    }
    if (sc_trace_on) {
        int64_t i = opal_atomic_fetch_add_64(&ompi_mtl_ofi_sc_trace_i, 1) & (SC_TRACE_N - 1);
        sc_trace_t t = {op, me, peer, tag, cid, req};
        ompi_mtl_ofi_sc_trace[i] = t;
    }
}

static ompi_mtl_ofi_hosted_sc_slot_t *sc_slots = NULL;
static unsigned long sc_nslots = 0;
static int sc_enabled = 0;

/* LITHE_SC_FREELIST (2026-10-21, default ON; =0 restores OBJ_NEW/malloc): recycle
 * posted and unexpected entries per slot instead of OBJ_NEW/OBJ_RELEASE per
 * message, and keep unexpected payloads <= SC_UE_INLINE bytes inside the entry
 * instead of a malloc'd copy (GAPS C17: the hosted path's remaining malloc-arena
 * waits). A/B, 4 reps: halo 256 KiB 51.4-52.9 -> 49.3-49.9 us, halo 4 KiB -1.5 %,
 * the rest within noise; stress ALL CLEAN at 4 and 8 contexts, integration OK. */
/* Atomic lifo ops unconditionally: co-resident ranks run on several harts at once. */
#define SC_UE_INLINE 4096
static int sc_freelist = 0;

/* LITHE_SC_RNDV=N (2026-10-21, default 4096 = ob1/sm's eager limit, 0 = off): an
 * unexpected message longer than N bytes is not
 * copied into a UE buffer; the UE borrows the sender's buffer and the send
 * completes only when a receive matches it (the existing synchronous-send
 * machinery), so a large unexpected message costs one copy instead of a malloc
 * and two. Vanilla ob1/sm does the same above its 4 KiB eager limit. MPI_Send
 * of a large message then blocks until matched, which the standard allows and
 * ob1 also does. Self-sends stay eager (a blocking large send to self before the
 * receive would deadlock). A/B (4 reps): 1 MiB reduce 285-302 -> 257-271 us, halo
 * 64 KiB at 4 harts 25.7-28.0 -> 18.0; the rest unchanged, ranks K=8 unchanged. */
static size_t sc_rndv = 4096;

/* LITHE_SC_PULL (2026-10-21, default ON, =0 restores sender copies): when a message longer than
 * sc_rndv matches a posted receive whose owner is spinning in a direct wait, the
 * sender queues it on the receiver's slot (borrowing its own buffer, completing as a
 * sync send) and the receiver copies it from its progress. Without this, whichever
 * rank arrived second did every copy: a 2-rank 64 KiB halo alternated exactly
 * between iterations with all four copies on one rank and none on the other
 * (posts >3 us in 2505/5000 iterations, <1 us in the rest), so four copies sat
 * serially on the critical path. Vanilla ob1/sm always copies on the receiver
 * (xpmem get). Lost-wakeup rule: the receiver clears `spinning`, fences, then drains
 * npull; the sender bumps npull, fences, then reads `spinning` and, if clear,
 * takes its entry back and copies itself. A/B: halo 64 KiB 2 ranks / 2 harts
 * 14.9 -> 4.7 us, 4 / 4 17.8 -> 16.6 (vanilla ob1/sm 12.9-16.9 in the same run); the
 * A/B suite (allreduce, halo 4 KiB, 1 MiB reduce, miniFE, MPI+OpenMP, ranks K=8)
 * unchanged. 0/1600 failures with every byte verified (2/2, 4/4, 8/3, 8/2). */
static int sc_pull = 1;
opal_atomic_int32_t ompi_mtl_ofi_sc_npull_total = 0;

static ompi_mtl_ofi_hosted_sc_posted_t *sc_pr_get(ompi_mtl_ofi_hosted_sc_slot_t *slot)
{
    if (sc_freelist) {
        opal_list_item_t *it = opal_lifo_pop_atomic(&slot->pr_free);
        if (NULL != it) {
            return (ompi_mtl_ofi_hosted_sc_posted_t *) it;
        }
    }
    return OBJ_NEW(ompi_mtl_ofi_hosted_sc_posted_t);
}

static void sc_pr_put(ompi_mtl_ofi_hosted_sc_slot_t *slot, ompi_mtl_ofi_hosted_sc_posted_t *pr)
{
    if (sc_freelist) {
        opal_lifo_push_atomic(&slot->pr_free, &pr->super);
    } else {
        OBJ_RELEASE(pr);
    }
}

static ompi_mtl_ofi_hosted_sc_ue_t *sc_ue_get(ompi_mtl_ofi_hosted_sc_slot_t *slot)
{
    ompi_mtl_ofi_hosted_sc_ue_t *ue;
    if (sc_freelist) {
        opal_list_item_t *it = opal_lifo_pop_atomic(&slot->ue_free);
        if (NULL != it) {
            return (ompi_mtl_ofi_hosted_sc_ue_t *) it;
        }
        /* OBJ_NEW sizes by the class; allocate the inline payload too. */
        ue = (ompi_mtl_ofi_hosted_sc_ue_t *) malloc(sizeof(*ue) + SC_UE_INLINE);
        if (NULL != ue) {
            OBJ_CONSTRUCT_INTERNAL(ue, OBJ_CLASS(ompi_mtl_ofi_hosted_sc_ue_t));
        }
        return ue;
    }
    return OBJ_NEW(ompi_mtl_ofi_hosted_sc_ue_t);
}

static void sc_ue_put(ompi_mtl_ofi_hosted_sc_slot_t *slot, ompi_mtl_ofi_hosted_sc_ue_t *ue)
{
    if (!ue->borrowed && ue->buf != ue->inl) {
        free(ue->buf);
    }
    ue->buf = NULL;
    if (sc_freelist) {
        opal_lifo_push_atomic(&slot->ue_free, &ue->super);
    } else {
        OBJ_RELEASE(ue);
    }
}

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
        sc_unlock(a->slot);
        lithe_context_unblock(context);
        return;
    }
    a->ue->send_waiter = context;
    sc_unlock(a->slot);
}

/* Progress calls per OFI CQ poll in a single-OS SC world (1 = every call). */
int ompi_mtl_ofi_sc_cq_every = 1;

/* True when logical world spans more than one OS process (P>1). */
static int sc_multi_os_world(void)
{
    return (opal_process_info.num_procs > opal_lithe_env_cache_rph()) ? 1 : 0;
}

/* LITHE_PML_CM_NOSPIN_PROBE (default 0): wake blocked probers of `slot` after a
 * sender appended to its unexpected list. Call WITHOUT the slot lock held (it
 * unblocks contexts). One load when the gate is off or nobody probes. */
static int sc_probe_wake = 0;
static int sc_probe_op(int op, int tok, unsigned us);
static inline void sc_probe_poke(ompi_mtl_ofi_hosted_sc_slot_t *slot)
{
    if (OPAL_LIKELY(!sc_probe_wake)) {
        return;
    }
    opal_atomic_mb(); /* the append before, the waiter count after (Dekker) */
    if (0 != slot->probe_waiters) {
        opal_atomic_add_fetch_32((opal_atomic_int32_t *) &slot->probe_seq, 1);
        (void) futex_wake_all((int *) &slot->probe_seq);
    }
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

/* LITHE_SC_PULL: copy every queued large message of SLOT into its matched receive.
 * Whoever takes the list under the slot lock owns the copies: the receiving context
 * (own slot, from MTL progress and when it stops spinning) or the progress context
 * (any slot, LITHE_SC_PROGRESS). */
static int sc_pull_slot(ompi_mtl_ofi_hosted_sc_slot_t *slot)
{
    ompi_mtl_ofi_hosted_sc_ue_t *ue;
    opal_list_t mine;
    int n = 0;

    if (NULL == slot || 0 == slot->npull) {
        return 0;
    }
    OBJ_CONSTRUCT(&mine, opal_list_t);
    sc_lock(slot);
    while (NULL != (ue = (ompi_mtl_ofi_hosted_sc_ue_t *) opal_list_remove_first(&slot->pulls))) {
        opal_list_append(&mine, &ue->super);
        ++n;
    }
    if (n > 0) {
        opal_atomic_add_fetch_32(&slot->npull, -n);
        opal_atomic_add_fetch_32(&ompi_mtl_ofi_sc_npull_total, -n);
    }
    sc_unlock(slot);
    while (NULL != (ue = (ompi_mtl_ofi_hosted_sc_ue_t *) opal_list_remove_first(&mine))) {
        lithe_context_t *sw;
        sc_finish_recv(ue->rreq, ue->rdst, ue->src_rank, ue->tag, ue->buf, ue->len);
        /* The sender's buffer is free only after the copy above. */
        sc_lock(slot);
        sw = sc_release_ssend(ue);
        sc_unlock(slot);
        if (NULL != sw) {
            lithe_context_unblock(sw);
        }
        sc_ue_put(slot, ue);
    }
    OBJ_DESTRUCT(&mine);
    return n;
}

static long sc_prog_nrecv_pull; /* copies taken by receivers (pull or queued) */

int ompi_mtl_ofi_hosted_sc_pull_progress(void)
{
    int got = sc_pull_slot(sc_slot_for_vpid(opal_proc_local_get()->proc_name.vpid));
    if (got > 0) {
        __atomic_add_fetch(&sc_prog_nrecv_pull, got, __ATOMIC_RELAXED);
    }
    return got;
}

/* LITHE_SC_PROGRESS=N (2026-10-24, .build_logs/progress/, default 0 = off): a Lithe
 * progress context for the same-OS path. Without it every large copy runs inside an MPI
 * call of one of the two ranks -- the sender's Isend (receive already posted) or the
 * receiver's Irecv (rendezvous message already queued) -- so Isend/Irecv + compute +
 * Wait never overlaps: 1 MiB with 200 us of compute on 2 ranks / 3 harts measured
 * comm 46-61 us, both 241-248 us, overlap 0.14-0.21. With the gate, an Isend or Irecv
 * of >= N bytes that would copy queues the copy on the receiver's slot instead (the
 * LITHE_SC_PULL list) and kicks the progress context. The copy is claimable: the
 * progress context takes it on a spare hart while both ranks compute, or the receiver
 * takes it in its own wait (MTL progress drains its slot), whichever locks the list
 * first. No spare hart costs nothing extra: the context stays runnable and the
 * receiver's wait does the copy, as before. The context blocks on a Lithe futex word
 * when no copies are queued; a queuer wakes it only if it sleeps.
 * MEASURED (.build_logs/progress/sweep1.txt, overlap.cpp: pack the whole send buffer,
 * unpack + check the whole receive buffer; 2 ranks / 3 harts, n=3 interleaved, us/iter):
 *   <= 1 MiB: neutral to LOSS (1 MiB 2000 us work 2073-2077 -> 2090-2102). The copy on a
 *     third core drags both buffers into its cache; the next pack/unpack pays it back
 *     (50 -> 65-108 us at 1 MiB). It is not wake latency: Perfetto shows the context on
 *     the spare hart, woken in ~10 us, ~22 us per copy.
 *   4 MiB: 2749 -> 2459 (2000 us work), 925-964 -> 856-871 (200 us); overlap 0 -> 0.39.
 *   16 MiB, 2000 us work: 4710 -> 3594 (overlap 0.02 -> 0.58; 1 of 3 reps did not overlap).
 *   No spare hart (4 ranks / 3 harts, 4 MiB): 5002-5046 -> 4892-4998, parity.
 *   Hosted stress ALL CLEAN at N=4 MiB (4 contexts) and N=1 (8 contexts, every copy queued).
 * Use N >= 4 MiB (beyond L2 the lines come from L3/DRAM anyway). Default stays 0 until
 * miniFE and integration_check run with it. */
static size_t sc_prog_min = 0;
static int sc_prog_word = 0;        /* futex: bumped on every kick */
static volatile int sc_prog_sleeping = 0;
static volatile int sc_prog_stop = 0;
static int sc_prog_done = 0;        /* futex: 1 once the loop has left */
static int sc_prog_live = 0;

static long sc_prog_nq, sc_prog_nself, sc_prog_nwake; /* LITHE_SC_PROGRESS_STATS */

static void sc_prog_kick(void)
{
    __atomic_add_fetch(&sc_prog_word, 1, __ATOMIC_SEQ_CST);
    if (__atomic_load_n(&sc_prog_sleeping, __ATOMIC_SEQ_CST)) {
        (void) futex_wake_one(&sc_prog_word);
    }
}

static void sc_prog_main(void *arg)
{
    (void) arg;
    lithe_sched_trace_label_self("mpi sc progress");
    while (!sc_prog_stop) {
        int seq = __atomic_load_n(&sc_prog_word, __ATOMIC_SEQ_CST);
        if (0 != ompi_mtl_ofi_sc_npull_total) {
            unsigned long i;
            for (i = 0; i < sc_nslots; ++i) {
                int got = sc_pull_slot(&sc_slots[i]);
                if (got > 0) {
                    __atomic_add_fetch(&sc_prog_nself, got, __ATOMIC_RELAXED);
                }
            }
            continue;
        }
        /* Dekker with sc_prog_kick: publish sleeping, then re-check the count the
         * queuer bumped before its kick; futex_wait also returns if seq moved. */
        __atomic_store_n(&sc_prog_sleeping, 1, __ATOMIC_SEQ_CST);
        if (0 == ompi_mtl_ofi_sc_npull_total && !sc_prog_stop) {
            (void) futex_wait(&sc_prog_word, seq);
            __atomic_add_fetch(&sc_prog_nwake, 1, __ATOMIC_RELAXED);
        }
        __atomic_store_n(&sc_prog_sleeping, 0, __ATOMIC_SEQ_CST);
    }
    __atomic_store_n(&sc_prog_done, 1, __ATOMIC_SEQ_CST);
    (void) futex_wake_all(&sc_prog_done);
}

static void sc_prog_start(void)
{
    const char *e = getenv("LITHE_SC_PROGRESS");
    long v = (NULL != e && *e) ? strtol(e, NULL, 10) : 0;

    if (v <= 0 || !sc_pull) {
        return;
    }
    sc_prog_min = (size_t) v;
    sc_prog_stop = 0;
    sc_prog_done = 0;
    if (NULL == lithe_fork_join_context_create_progress(262144, sc_prog_main, NULL)) {
        sc_prog_min = 0;
        return;
    }
    sc_prog_live = 1;
}

/* Stop and wait for the loop to leave; the 256 KiB context is not reclaimed. */
static void sc_prog_stop_wait(void)
{
    if (!sc_prog_live) {
        return;
    }
    sc_prog_min = 0; /* no new queueing; the receivers drain what is left */
    sc_prog_stop = 1;
    sc_prog_kick();
    (void) futex_wake_one(&sc_prog_word);
    if (lithe_can_block()) {
        while (0 == __atomic_load_n(&sc_prog_done, __ATOMIC_SEQ_CST)) {
            (void) futex_wait(&sc_prog_done, 0);
        }
    }
    sc_prog_live = 0;
    if (NULL != getenv("LITHE_SC_PROGRESS_STATS")) {
        fprintf(stderr, "[sc_progress] queued=%ld by_progress=%ld by_receiver=%ld wakes=%ld\n",
                sc_prog_nq, sc_prog_nself, sc_prog_nrecv_pull, sc_prog_nwake);
    }
}

/* Queue a matched large copy for the progress context or the receiver's wait. Called
 * with slot locked; unlocks it. */
static void sc_prog_queue_locked(ompi_mtl_ofi_hosted_sc_slot_t *slot,
                                 ompi_mtl_ofi_hosted_sc_ue_t *ue)
{
    opal_list_append(&slot->pulls, &ue->super);
    opal_atomic_add_fetch_32(&slot->npull, 1);
    opal_atomic_add_fetch_32(&ompi_mtl_ofi_sc_npull_total, 1);
    sc_unlock(slot);
    __atomic_add_fetch(&sc_prog_nq, 1, __ATOMIC_RELAXED);
    sc_prog_kick();
}

/* opal_lithe_wait_sync_spinning hook (base/wait_sync.c). */
static void sc_wait_spinning(int on)
{
    ompi_mtl_ofi_hosted_sc_slot_t *slot = sc_slot_for_vpid(opal_proc_local_get()->proc_name.vpid);

    if (NULL == slot) {
        return;
    }
    if (on) {
        slot->spinning = 1;
        return;
    }
    slot->spinning = 0;
    opal_atomic_mb(); /* pairs with the sender's fence after npull++ */
    if (0 != slot->npull) {
        (void) ompi_mtl_ofi_hosted_sc_pull_progress();
    }
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
        OBJ_CONSTRUCT(&sc_slots[i].pulls, opal_list_t);
        sc_slots[i].npull = 0;
        sc_slots[i].spinning = 0;
        sc_slots[i].probe_seq = 0;
        sc_slots[i].probe_waiters = 0;
        OBJ_CONSTRUCT(&sc_slots[i].pr_free, opal_lifo_t);
        OBJ_CONSTRUCT(&sc_slots[i].ue_free, opal_lifo_t);
        opal_atomic_lock_init(&sc_slots[i].spin, OPAL_ATOMIC_LOCK_UNLOCKED);
    }
    e = getenv("LITHE_SC_SPINLOCK");
    sc_spin = (NULL != e && '0' == e[0]) ? 0 : 1;
    e = getenv("LITHE_SC_FREELIST");
    sc_freelist = (NULL != e && '0' == e[0]) ? 0 : 1;
    e = getenv("LITHE_SC_RNDV");
    sc_rndv = (NULL != e) ? (size_t) strtoul(e, NULL, 10) : 4096;
    e = getenv("LITHE_SC_PULL");
    sc_pull = !(NULL != e && '0' == e[0]) && sc_rndv > 0 && !sc_multi_os_world();
    sc_enabled = 1;
    if (!sc_multi_os_world()) {
        /* Single-OS world: every peer goes through the memcpy short-circuit, so
         * (2026-10-20, login node, 4 ranks on 4 harts, 8-byte allreduce):
         *  - LITHE_SC_MULTI_PROGRESS=1 (opt-in): let every waiter drive progress.
         *    opal_max_thread_in_progress=1 sent all but one rank to sleep on a
         *    condvar, and each completion then woke one through the scheduler:
         *    22.3 -> 12.4 us/op alone, 15.2 -> 8.0 with LITHE_MUTEX_SPIN. NOT the
         *    default: with more ranks than harts (bench/alt ranks K=8 on 4 harts)
         *    the extra spinning waiters cost 9.1-9.6 -> 10.4-10.6 ms/iter, and
         *    miniFE (own intra-process allreduce) does not gain. An explicit
         *    OMPI_MCA_opal_max_thread_in_progress wins.
         *  - LITHE_MTL_OFI_SC_CQ_EVERY=N (default 64 since 2026-10-21): poll the OFI CQs on
         *    every Nth progress call only; they carry no point-to-point traffic
         *    here, but each call locked and read its own CQ and swept every peer's. */
        const char *mp = getenv("LITHE_SC_MULTI_PROGRESS");
        if (mp && mp[0] == '1' && NULL == getenv("OMPI_MCA_opal_max_thread_in_progress")) {
            opal_max_thread_in_progress = INT_MAX;
        }
        /* Default 64 since 2026-10-21 (with LITHE_WAIT_SYNC_DIRECT, wait_sync.c):
         * nothing arrives on these CQs in a single-OS world. =1 polls every call. */
        const char *ce = getenv("LITHE_MTL_OFI_SC_CQ_EVERY");
        int every = ce ? atoi(ce) : 64;
        ompi_mtl_ofi_sc_cq_every = every > 1 ? every : 1;
    }
    /* wait_sync scarce path: park on posted recv instead of yield→steal. */
    opal_progress_set_sc_park_callback(ompi_mtl_ofi_hosted_sc_try_park_pending);
    e = getenv("LITHE_PML_CM_NOSPIN_PROBE");
    sc_probe_wake = (NULL != e && '1' == e[0]);
    if (sc_probe_wake) {
        opal_progress_probe_wait_fn = sc_probe_op;
    }
    if (sc_pull) {
        opal_lithe_wait_sync_spinning = sc_wait_spinning;
    }
    sc_prog_start();
    opal_output_verbose(1, opal_common_ofi.output,
                        "mtl:ofi: hosted same-OS short-circuit enabled (slots=%lu multi_os=%d)",
                        rph, sc_multi_os_world());
    return OMPI_SUCCESS;
}

void ompi_mtl_ofi_hosted_sc_finalize(void)
{
    unsigned long i;

    sc_prog_stop_wait();
    opal_progress_set_sc_park_callback(NULL);
    opal_lithe_wait_sync_spinning = NULL;
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
            if (!ue->borrowed && ue->buf != ue->inl) {
                free(ue->buf);
            }
            OBJ_RELEASE(ue);
        }
        while (NULL
               != (pr = (ompi_mtl_ofi_hosted_sc_posted_t *) opal_list_remove_first(
                       &sc_slots[i].posted))) {
            OBJ_RELEASE(pr);
        }
        while (NULL != (ue = (ompi_mtl_ofi_hosted_sc_ue_t *) opal_lifo_pop_atomic(&sc_slots[i].ue_free))) {
            OBJ_RELEASE(ue);
        }
        while (NULL != (pr = (ompi_mtl_ofi_hosted_sc_posted_t *) opal_lifo_pop_atomic(&sc_slots[i].pr_free))) {
            OBJ_RELEASE(pr);
        }
        OBJ_DESTRUCT(&sc_slots[i].ue_free);
        OBJ_DESTRUCT(&sc_slots[i].pr_free);
        OBJ_DESTRUCT(&sc_slots[i].pulls);
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

    sc_lock(slot);

    OPAL_LIST_FOREACH_SAFE (pr, pr_next, &slot->posted,
                            ompi_mtl_ofi_hosted_sc_posted_t) {
        if (pr->cid != cid || !sc_tag_match(pr->tag, tag)
            || !sc_src_match(pr->src, src_rank)) {
            continue;
        }
        if (pr->req->req_started) {
            sc_trace(5, src_rank, (int) peer->super.proc_name.vpid, tag, cid, pr->req);
            opal_list_remove_item(&slot->posted, &pr->super);
            sc_pr_put(slot, pr);
            continue;
        }
        sc_trace(1, src_rank, (int) peer->super.proc_name.vpid, tag, cid, pr->req);
        matched_recv = pr->req;
        matched_dst = pr->buf;
        matched_comm = pr->req->comm;
        recv_waiter = pr->waiter;
        pr->waiter = NULL;
        /* Only cancel if fi_trecv was actually posted (not merely planned). */
        need_ofi_cancel = (pr->ofi_dual && pr->ofi_posted) ? 1 : 0;
        sc_clear_waiter_locked(slot, recv_waiter);
        opal_list_remove_item(&slot->posted, &pr->super);
        sc_pr_put(slot, pr);
        break;
    }

    if (NULL != matched_recv && sc_pull && length > sc_rndv && dest != src_rank
        && !need_ofi_cancel && slot->spinning
        && NULL != (ue = sc_ue_get(slot))) {
        /* LITHE_SC_PULL: leave the copy to the spinning receiver (see sc_pull). */
        int reclaimed = 0;

        matched_recv->req_started = true; /* matched: no SC cancel from here on */
        ue->buf = start;
        ue->len = length;
        ue->tag = tag;
        ue->src_rank = src_rank;
        ue->cid = cid;
        ue->is_sync = 1;
        ue->borrowed = 1;
        ue->send_is_isend = is_isend ? 1 : 0;
        ue->send_req = ofi_req;
        ue->send_waiter = NULL;
        ue->rreq = matched_recv;
        ue->rdst = matched_dst;
        ofi_req->completion_count = 1;
        ofi_req->status.MPI_ERROR = OMPI_SUCCESS;
        opal_list_append(&slot->pulls, &ue->super);
        opal_atomic_add_fetch_32(&slot->npull, 1);
        opal_atomic_add_fetch_32(&ompi_mtl_ofi_sc_npull_total, 1);
        sc_unlock(slot);
        opal_atomic_mb(); /* pairs with sc_wait_spinning(0)'s fence */
        if (!slot->spinning) {
            ompi_mtl_ofi_hosted_sc_ue_t *it;
            sc_lock(slot);
            OPAL_LIST_FOREACH (it, &slot->pulls, ompi_mtl_ofi_hosted_sc_ue_t) {
                if (it == ue) {
                    opal_list_remove_item(&slot->pulls, &ue->super);
                    opal_atomic_add_fetch_32(&slot->npull, -1);
                    opal_atomic_add_fetch_32(&ompi_mtl_ofi_sc_npull_total, -1);
                    reclaimed = 1;
                    break;
                }
            }
            sc_unlock(slot);
        }
        if (reclaimed) {
            /* The receiver stopped spinning before taking it: copy here as before. */
            sc_ue_put(slot, ue);
            ofi_req->completion_count = is_isend ? 1 : 0;
            sc_finish_recv(matched_recv, matched_dst, src_rank, tag, start, length);
            sc_finish_send(ofi_req, is_isend, free_after, start);
        } else if (!is_isend) {
            sc_ssend_park_arg_t parg;

            sc_lock(slot);
            while (ofi_req->completion_count > 0) {
                parg.slot = slot;
                parg.ue = ue;
                parg.sreq = ofi_req;
                lithe_context_block(sc_ssend_park_cb, &parg);
                sc_lock(slot);
            }
            sc_unlock(slot);
            if (free_after && NULL != start) {
                free(start);
            }
        }
        if (NULL != recv_waiter) {
            lithe_context_unblock(recv_waiter);
        }
        return 1;
    }

    if (NULL != matched_recv && is_isend && !free_after && sc_prog_min > 0
        && length >= sc_prog_min && dest != src_rank && !need_ofi_cancel
        && NULL != (ue = sc_ue_get(slot))) {
        /* LITHE_SC_PROGRESS: the receiver is not in a wait; leave the copy to the
         * progress context (or to the receiver's wait, whichever claims it first). */
        matched_recv->req_started = true;
        ue->buf = start;
        ue->len = length;
        ue->tag = tag;
        ue->src_rank = src_rank;
        ue->cid = cid;
        ue->is_sync = 1;
        ue->borrowed = 1;
        ue->send_is_isend = 1;
        ue->send_req = ofi_req;
        ue->send_waiter = NULL;
        ue->rreq = matched_recv;
        ue->rdst = matched_dst;
        ofi_req->completion_count = 1;
        ofi_req->status.MPI_ERROR = OMPI_SUCCESS;
        sc_prog_queue_locked(slot, ue);
        if (NULL != recv_waiter) {
            lithe_context_unblock(recv_waiter);
        }
        return 1;
    }

    if (NULL != matched_recv) {
        sc_unlock(slot);
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
    ue = sc_ue_get(slot);
    if (NULL == ue) {
        sc_unlock(slot);
        ofi_req->status.MPI_ERROR = OMPI_ERR_OUT_OF_RESOURCE;
        return 1;
    }
    payload = NULL;
    ue->borrowed = 0;
    if (sc_rndv > 0 && length > sc_rndv && dest != src_rank) {
        /* Rendezvous: borrow the sender's buffer; complete as a sync send. */
        ue->borrowed = 1;
        payload = start;
        is_sync = 1;
    } else if (length > 0) {
        payload = (sc_freelist && length <= SC_UE_INLINE) ? (void *) ue->inl : malloc(length);
        if (NULL == payload) {
            ue->buf = NULL;
            sc_ue_put(slot, ue);
            sc_unlock(slot);
            ofi_req->status.MPI_ERROR = OMPI_ERR_OUT_OF_RESOURCE;
            return 1;
        }
        memcpy(payload, start, length);
    }
    sc_trace(2, src_rank, (int) peer->super.proc_name.vpid, tag, cid, NULL);
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
            if (OPAL_UNLIKELY(sc_probe_wake)) {
                /* never unblock under the slot lock; the loop rechecks */
                sc_unlock(slot);
                sc_probe_poke(slot);
                sc_lock(slot);
            }
            /* Park until matching recv takes UE — not yield→empty steal. */
            while (ofi_req->completion_count > 0) {
                parg.slot = slot;
                parg.ue = ue;
                parg.sreq = ofi_req;
                lithe_context_block(sc_ssend_park_cb, &parg);
                /* Relock for next completion_count check / re-park. */
                sc_lock(slot);
            }
            sc_unlock(slot);
            if (free_after && NULL != start) {
                free(start);
            }
        } else {
            sc_unlock(slot);
            sc_probe_poke(slot);
        }
        return 1;
    }

    opal_list_append(&slot->unexpected, &ue->super);
    sc_unlock(slot);
    sc_probe_poke(slot);
    sc_finish_send(ofi_req, is_isend, free_after, start);
    return 1;
}

/*
 * MPI_Probe / Iprobe / Improbe on same-OS messages (2026-10-24).
 *
 * BUG FIXED HERE: the probe path only asked OFI (fi_trecvmsg FI_PEEK), but a same-OS
 * send goes through the short-circuit and never reaches OFI, so in a single-OS hosted
 * world MPI_Probe / MPI_Mprobe never matched and spun forever (p2p_despin probe /
 * mprobe: 0/3 complete, timeout, with LITHE_MTL_OFI_HOSTED_SC=1, the default; SC=0
 * passes probe at 39 us/op). Look at this slot's unexpected list first. Not gated:
 * it is a correctness fix (the old path cannot complete).
 *
 * MPI_ANY_TAG matches only non-negative (user) tags here, as in ob1: an unexpected
 * collective fragment must not satisfy a user probe.
 */
int ompi_mtl_ofi_hosted_sc_iprobe(struct ompi_communicator_t *comm, int src, int tag,
                                  int claim, int *flag, ompi_status_public_t *status,
                                  void **claimed)
{
    ompi_mtl_ofi_hosted_sc_slot_t *slot;
    ompi_mtl_ofi_hosted_sc_ue_t *ue, *found = NULL;
    int cid, any_remote = 0;

    if (!sc_enabled) {
        return 0;
    }
    if (MPI_ANY_SOURCE != src) {
        if (!ompi_mtl_ofi_hosted_sc_same_os(ompi_comm_peer_lookup(comm, src))) {
            return 0;
        }
    } else if (sc_multi_os_world()) {
        any_remote = 1; /* no local match: OFI must still peek for remote peers */
    }
    slot = sc_slot_for_vpid(opal_proc_local_get()->proc_name.vpid);
    if (NULL == slot) {
        return 0;
    }
    cid = (int) comm->c_index;

    sc_lock(slot);
    OPAL_LIST_FOREACH (ue, &slot->unexpected, ompi_mtl_ofi_hosted_sc_ue_t) {
        if (ue->cid != cid || !sc_src_match(src, ue->src_rank)) {
            continue;
        }
        if (MPI_ANY_TAG == tag ? ue->tag < 0 : ue->tag != tag) {
            continue;
        }
        found = ue;
        break;
    }
    if (NULL != found) {
        if (MPI_STATUS_IGNORE != status) {
            status->MPI_SOURCE = found->src_rank;
            status->MPI_TAG = found->tag;
            status->MPI_ERROR = MPI_SUCCESS;
            status->_ucount = found->len;
            status->_cancelled = 0;
        }
        if (claim) {
            opal_list_remove_item(&slot->unexpected, &found->super);
            *claimed = found;
        }
    }
    sc_unlock(slot);

    if (NULL != found) {
        *flag = 1;
        return 1;
    }
    if (any_remote) {
        return 0;
    }
    *flag = 0;
    return 1;
}

/* MPI_Mrecv of a UE claimed by ompi_mtl_ofi_hosted_sc_iprobe(claim=1): the same
 * delivery as try_irecv's unexpected match. rreq is set up like an irecv request. */
void ompi_mtl_ofi_hosted_sc_mrecv(void *claimed, ompi_mtl_ofi_request_t *rreq,
                                  void *start)
{
    ompi_mtl_ofi_hosted_sc_ue_t *ue = (ompi_mtl_ofi_hosted_sc_ue_t *) claimed;
    ompi_mtl_ofi_hosted_sc_slot_t *slot =
        sc_slot_for_vpid(opal_proc_local_get()->proc_name.vpid);
    lithe_context_t *sw = NULL;

    if (ue->is_sync && !ue->borrowed) {
        sc_lock(slot);
        sw = sc_release_ssend(ue);
        sc_unlock(slot);
    }
    sc_finish_recv(rreq, start, ue->src_rank, ue->tag, ue->buf, ue->len);
    if (ue->borrowed) {
        /* The sender's buffer is free only after the copy above. */
        sc_lock(slot);
        sw = sc_release_ssend(ue);
        sc_unlock(slot);
    }
    if (NULL != sw) {
        lithe_context_unblock(sw);
    }
    sc_ue_put(slot, ue);
}

/* opal_progress_probe_wait_fn (pml/cm blocking probe, LITHE_PML_CM_NOSPIN_PROBE):
 * op 0 arm (count this context as a prober of its slot), 1 snapshot (returns the
 * sequence to wait on; take it BEFORE the iprobe that may miss), 2 wait (sleep on
 * the snapshot for at most `us` microseconds: same-OS senders wake it at once, the
 * timeout covers OFI arrivals, which wake nothing), 3 disarm. */
static int sc_probe_op(int op, int tok, unsigned us)
{
    ompi_mtl_ofi_hosted_sc_slot_t *slot =
        sc_slot_for_vpid(opal_proc_local_get()->proc_name.vpid);
    struct timespec rel;

    if (NULL == slot) {
        if (2 == op) {
            int z = 0;
            rel.tv_sec = us / 1000000u;
            rel.tv_nsec = (long) (us % 1000000u) * 1000L;
            (void) futex_wait_timeout(&z, 0, &rel);
        }
        return 0;
    }
    switch (op) {
    case 0:
        opal_atomic_add_fetch_32(&slot->probe_waiters, 1); /* full barrier */
        return 0;
    case 1:
        return slot->probe_seq;
    case 2:
        rel.tv_sec = us / 1000000u;
        rel.tv_nsec = (long) (us % 1000000u) * 1000L;
        (void) futex_wait_timeout((int *) &slot->probe_seq, tok, &rel);
        return 0;
    default:
        opal_atomic_add_fetch_32(&slot->probe_waiters, -1);
        return 0;
    }
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

    sc_lock(slot);

    OPAL_LIST_FOREACH_SAFE (ue, ue_next, &slot->unexpected,
                            ompi_mtl_ofi_hosted_sc_ue_t) {
        lithe_context_t *sw = NULL;

        if (ue->cid != cid || !sc_tag_match(tag, ue->tag)
            || !sc_src_match(src, ue->src_rank)) {
            continue;
        }
        opal_list_remove_item(&slot->unexpected, &ue->super);
        sc_trace(3, (int) opal_proc_local_get()->proc_name.vpid, ue->src_rank, ue->tag, cid, ofi_req);
        if (ue->borrowed && sc_prog_min > 0 && ue->len >= sc_prog_min
            && NULL == ofi_req->buffer) {
            /* LITHE_SC_PROGRESS: the rendezvous copy goes to the progress context
             * instead of this Irecv (see sc_prog_min). */
            ofi_req->req_started = true;
            ue->rreq = ofi_req;
            ue->rdst = start;
            sc_prog_queue_locked(slot, ue);
            return 1;
        }
        if (ue->is_sync && !ue->borrowed) {
            sw = sc_release_ssend(ue);
        }
        sc_unlock(slot);

        sc_finish_recv(ofi_req, start, ue->src_rank, ue->tag, ue->buf, ue->len);
        if (ue->borrowed) {
            /* The sender's buffer is free only after the copy above. */
            sc_lock(slot);
            sw = sc_release_ssend(ue);
            sc_unlock(slot);
        }
        if (NULL != sw) {
            lithe_context_unblock(sw);
        }
        sc_ue_put(slot, ue);
        return 1;
    }

    if (any_src_ofi_only) {
        /* No local UE — wait on OFI only; do not leave an SC posted entry. */
        sc_unlock(slot);
        return 0;
    }

    pr = sc_pr_get(slot);
    if (NULL == pr) {
        sc_unlock(slot);
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
    sc_trace(4, (int) opal_proc_local_get()->proc_name.vpid, src, tag, cid, ofi_req);
    opal_list_append(&slot->posted, &pr->super);
    sc_unlock(slot);

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

    sc_lock(slot);
    if (ofi_req->req_started) {
        /* Matched via SC before/during fi_trecv — remove posted entry. */
        OPAL_LIST_FOREACH_SAFE (pr, pr_next, &slot->posted,
                                ompi_mtl_ofi_hosted_sc_posted_t) {
            if (pr->req == ofi_req) {
                opal_list_remove_item(&slot->posted, &pr->super);
                sc_pr_put(slot, pr);
                break;
            }
        }
        sc_unlock(slot);
        return;
    }
    OPAL_LIST_FOREACH_SAFE (pr, pr_next, &slot->posted,
                            ompi_mtl_ofi_hosted_sc_posted_t) {
        if (pr->req == ofi_req) {
            pr->ofi_posted = 1;
            break;
        }
    }
    sc_unlock(slot);
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

    sc_lock(slot);
    OPAL_LIST_FOREACH_SAFE (pr, pr_next, &slot->posted,
                            ompi_mtl_ofi_hosted_sc_posted_t) {
        if (pr->req == ofi_req) {
            opal_list_remove_item(&slot->posted, &pr->super);
            sc_pr_put(slot, pr);
            break;
        }
    }
    sc_unlock(slot);
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

    sc_lock(slot);
    OPAL_LIST_FOREACH_SAFE (pr, pr_next, &slot->posted,
                            ompi_mtl_ofi_hosted_sc_posted_t) {
        if (pr->req == ofi_req) {
            w = pr->waiter;
            pr->waiter = NULL;
            sc_clear_waiter_locked(slot, w);
            opal_list_remove_item(&slot->posted, &pr->super);
            sc_pr_put(slot, pr);
            found = 1;
            break;
        }
    }
    sc_unlock(slot);

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

    sc_lock(slot);
    OPAL_LIST_FOREACH (pr, &slot->posted, ompi_mtl_ofi_hosted_sc_posted_t) {
        if (!pr->req->req_started) {
            pending = 1;
            /* Dual / fi_trecv posted: completion may arrive on the CQ. */
            if (pr->ofi_dual || pr->ofi_posted) {
                ofi_needed = 1;
            }
        }
    }
    sc_unlock(slot);

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
         *
         * Also yield when runnable_count>0 (should_yield alone misses
         * soft_cap spare: owned<RPH with RUNNABLE peers). End with one
         * unconditional yield so all-RUNNING waiters rotate — P2N2K4
         * dist=1 same-OS SC pw=0 hangs when every rank SC-spins with r=0
         * and nobody posts (Flux EC=142).
         */
        spins = 128u;
        for (i = 0; i < spins; i++) {
            cpu_relax();
            if (0 == (i & 63u) &&
                (lithe_fork_join_should_yield_to_runnable() ||
                 lithe_fork_join_current_runnable_count() > 0)) {
                lithe_context_yield();
            }
        }
        lithe_context_yield();
        return 1;
    }

    /*
     * Single-OS: always spin-suppress yield/CQ-park in wait_sync. Gaps
     * between Sendrecv rounds used to return 0 → lithe_context_yield while
     * peers entered flat Barrier (P1K8/P1K16 pairwise flakes).
     */
    /* LITHE_SC_PARK_SPINS=n (2026-10-20, A/B; default 128 pending / 32): the
     * caller rechecks its request only between calls, so this spin is the
     * single-OS wait's polling period. 128 PAUSEs are several us here, about
     * half of a hosted 8-byte allreduce round (perf: 33 % of its samples). */
    {
        static int ps = -2;
        if (ps == -2) {
            const char *e = getenv("LITHE_SC_PARK_SPINS");
            ps = e ? atoi(e) : -1;
        }
        spins = ps >= 0 ? (unsigned) ps : (pending ? 128u : 32u);
    }
    for (i = 0; i < spins; i++) {
        cpu_relax();
    }
    return 1;
}

#endif /* HAVE_LITHE */
