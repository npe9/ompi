/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2014-2020 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2016      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2017-2022 IBM Corporation. All rights reserved.
 * Copyright (c) 2019      Sandia National Laboratories.  All rights reserved.
 * Copyright (c) 2021      Argonne National Laboratory.  All rights reserved.
 *
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal/mca/threads/wait_sync.h"
#if HAVE_LITHE
#include <lithe/sched_trace.h>
#endif

#include <stdlib.h>
#include <time.h>

#if HAVE_LITHE
#include <lithe/fork_join_sched.h>
#include <lithe/lithe.h>
#include <parlib/arch.h> /* cpu_relax */
#include <lithe/futex.h>
#include "opal/util/proc.h"
#endif

static opal_mutex_t wait_sync_lock = OPAL_MUTEX_STATIC_INIT;
ompi_wait_sync_t *opal_threads_base_wait_sync_list = NULL; /* not static for inline "wait_sync_st" */

void opal_threads_base_wait_sync_global_wakeup_st(int status)
{
    ompi_wait_sync_t *sync;
    for (sync = opal_threads_base_wait_sync_list; sync != NULL; sync = sync->next) {
        wait_sync_update(sync, 0, status);
    }
}

void opal_threads_base_wait_sync_global_wakeup_mt(int status)
{
    ompi_wait_sync_t *sync;
    opal_mutex_lock(&wait_sync_lock);
    for (sync = opal_threads_base_wait_sync_list; sync != NULL; sync = sync->next) {
        /* sync_update is going to  take the sync->lock from within
         * the wait_sync_lock. Thread lightly here: Idealy we should
         * find a way to not take a lock in a lock as this is deadlock prone,
         * but as of today we are the only place doing this so it is safe.
         */
        wait_sync_update(sync, 0, status);
        if (sync->next == opal_threads_base_wait_sync_list) {
            break; /* special case for rings */
        }
    }
    opal_mutex_unlock(&wait_sync_lock);
}

static opal_atomic_int32_t num_thread_in_progress = 0;

#if HAVE_LITHE
/* LITHE_WAIT_SYNC_DIRECT (2026-10-21, default ON in single-OS hosted worlds; =0
 * restores the condvar/elected-progress wait). Every same-process receive completes inside the matching sender's call
 * (mtl_ofi_hosted_sc.c), so a waiter needs no elected progress thread. The
 * default path parks every waiter but one on a condvar, and each completion then
 * takes the sync mutex, signals, and unblocks a context through the scheduler;
 * perf of the 4 KiB halo stress: ~3000 samples in that mutex/condvar, ~2300 in the
 * hart request behind the unblock, ~1000 polling the (idle) OFI CQs, against ~300
 * in the copy itself. Direct mode: spin on the count, calling opal_progress every
 * LITHE_WAIT_SYNC_DIRECT_PROGRESS spins (default 32) and yielding while another
 * context is runnable; after LITHE_WAIT_SYNC_DIRECT_SPIN spins (default 1024) sleep
 * in the Lithe futex on the count for at most LITHE_WAIT_SYNC_DIRECT_TIMEOUT_US
 * (default 100). The timeout keeps opal_progress running for anything only it
 * advances (libnbc schedules, generalized requests). The completer wakes the
 * futex (WAIT_SYNC_SIGNAL). Direct waiters are not on the wait-sync list, so a
 * global error wakeup reaches them only through the timeout's recheck. */
int opal_lithe_wait_sync_direct = -1;
int (*opal_lithe_wait_sync_keep_progress)(void) = NULL;
static unsigned lithe_wsd_spin = 256, lithe_wsd_progress = 32, lithe_wsd_timeout_us = 100;
/* LITHE_WAIT_SYNC_DIRECT_IDLE_SPIN (default 1; 0 = always sleep after the spin
 * budget). Within the budget a waiter yields to runnable peers as before; when the
 * budget runs out it sleeps only if a peer is runnable or the scheduler wants the
 * hart back, and otherwise keeps spinning: its hart has nothing else to run, and a
 * sleeper whose hart parked pays a hart wake per completion. (Sleeping at once
 * whenever a peer was runnable lost most of the latency gain: allreduce 3.0 ->
 * 12.7 us with 4 contexts on 3 harts.) bench/alt ranks K=8 (8 contexts / 4 harts, imbalanced) with 0:
 * 8.7-9.0 -> 9.5-9.8 ms/iter, ~10x the context wakeups, 15-30 us p50 / 250-350
 * us p90 unblock->dispatch, harts idle 43-154 -> 166-200 ms (Perfetto). */
static int lithe_wsd_idle_spin = 1;
/* LITHE_WAIT_SYNC_DIRECT_YIELDS (default 1, 0 = unlimited): after this many yields
 * to runnable peers in one wait, sleep instead. The trade-off (4 reps, with the
 * barrier budget below set the same): yields 8 -> halo 4 KiB 3.6 us but ranks K=8
 * 9.3-9.6 ms/iter; 2 -> 6.8 us / 8.9-9.4; 1 -> 7.7 us / 8.5-9.1 (old path 11.2 us /
 * 8.7-9.1). 1 is the setting that loses nowhere. */
static int lithe_wsd_yields = 1;

int opal_lithe_wait_sync_direct_resolve(void)
{
    const char *e = getenv("LITHE_WAIT_SYNC_DIRECT");
    if ((NULL != e && '0' == e[0]) || !opal_lithe_env_cache_active()) {
        opal_lithe_wait_sync_direct = 0;
        return 0;
    }
    /* Not known before the RTE fills num_procs; no MPI request waits before that. */
    if (0 == opal_process_info.num_procs) {
        return 0;
    }
    if (opal_process_info.num_procs > opal_lithe_env_cache_rph()) {
        opal_lithe_wait_sync_direct = 0; /* multi-OS: OFI completions need the CQ engine */
        return 0;
    }
    if ((e = getenv("LITHE_WAIT_SYNC_DIRECT_SPIN"))) lithe_wsd_spin = (unsigned) atoi(e);
    if ((e = getenv("LITHE_WAIT_SYNC_DIRECT_PROGRESS")) && atoi(e) > 0) lithe_wsd_progress = (unsigned) atoi(e);
    if ((e = getenv("LITHE_WAIT_SYNC_DIRECT_TIMEOUT_US")) && atoi(e) > 0) lithe_wsd_timeout_us = (unsigned) atoi(e);
    if ((e = getenv("LITHE_WAIT_SYNC_DIRECT_IDLE_SPIN"))) lithe_wsd_idle_spin = ('0' != e[0]);
    if ((e = getenv("LITHE_WAIT_SYNC_DIRECT_YIELDS"))) lithe_wsd_yields = atoi(e);
    opal_lithe_wait_sync_direct = 1;
    return 1;
}

static void lithe_sync_wait_direct_sleep(ompi_wait_sync_t *sync)
{
    int32_t c = sync->count;
    if (NULL != opal_lithe_wait_sync_keep_progress && opal_lithe_wait_sync_keep_progress()) {
        /* Our libnbc schedule advances only when we call opal_progress, and its
         * internal receives do not wake this sync: sleeping cost a 100 us timeout
         * per round (Iallreduce 300-450 us/op, Ibarrier 8 ranks / 3 harts hung). */
        if (lithe_fork_join_current_runnable_count() > 0) {
            lithe_context_yield();
        } else {
            cpu_relax();
        }
        return;
    }
    if (c > 0 && lithe_can_block()) {
        struct timespec rel = {0, (long) lithe_wsd_timeout_us * 1000L};
        (void) futex_wait_timeout((int *) &sync->count, (int) c, &rel);
    }
}

static int lithe_sync_wait_direct(ompi_wait_sync_t *sync)
{
    unsigned i = 0;
    int yields = 0;
    while (sync->count > 0) {
        if (0 == (i % lithe_wsd_progress)) {
            (void) opal_progress();
            if (sync->count <= 0) {
                break;
            }
            if (lithe_fork_join_current_runnable_count() > 0) {
                if (lithe_wsd_yields > 0 && ++yields > lithe_wsd_yields) {
                    lithe_sync_wait_direct_sleep(sync);
                } else {
                    lithe_context_yield();
                }
            }
        }
        if (++i < lithe_wsd_spin) {
            cpu_relax();
            continue;
        }
        i = 0;
        if (lithe_wsd_idle_spin && 0 == lithe_fork_join_current_runnable_count() &&
            !lithe_fork_join_should_yield_to_runnable()) {
            continue; /* nobody wants this hart: keep spinning, never park it */
        }
        lithe_sync_wait_direct_sleep(sync);
    }
    return (0 == sync->status) ? OPAL_SUCCESS : OPAL_ERROR;
}
#endif

#define WAIT_SYNC_PASS_OWNERSHIP(who)                        \
    do {                                                     \
        opal_thread_internal_mutex_lock(&(who)->lock);       \
        opal_thread_internal_cond_signal(&(who)->condition); \
        opal_thread_internal_mutex_unlock(&(who)->lock);     \
    } while (0)

int ompi_sync_wait_mt(ompi_wait_sync_t *sync)
{
#if HAVE_LITHE
    uint32_t _lt_key = 0; int _lt_on = lithe_sched_trace_enabled();
    if (_lt_on) { _lt_key = lithe_sched_trace_ctx_key();
        lithe_sched_trace_emit(LITHE_ST_WAIT_SYNC_BLOCK, NULL, _lt_key, 0, (int32_t)(((uintptr_t) sync >> 4) & 0x7fffffff)); }
#endif
    /* Don't stop if the waiting synchronization is completed. We avoid the
     * race condition around the release of the synchronization using the
     * signaling field.
     */
    if (sync->count <= 0) {
        {
#if HAVE_LITHE
        if (_lt_on) lithe_sched_trace_emit(LITHE_ST_WAIT_SYNC_WAKE, NULL, _lt_key, 0, 0);
#endif
        return (0 == sync->status) ? OPAL_SUCCESS : OPAL_ERROR; }
    }

#if HAVE_LITHE
    if (opal_lithe_wait_sync_direct_on()) {
        int rc = lithe_sync_wait_direct(sync);
        if (_lt_on) lithe_sched_trace_emit(LITHE_ST_WAIT_SYNC_WAKE, NULL, _lt_key, 0, 0);
        return rc;
    }
#endif
    /* lock so nobody can signal us during the list updating */
    opal_thread_internal_mutex_lock(&sync->lock);

    /* Now that we hold the lock make sure another thread has not already
     * call cond_signal.
     */
    if (sync->count <= 0) {
        opal_thread_internal_mutex_unlock(&sync->lock);
        {
#if HAVE_LITHE
        if (_lt_on) lithe_sched_trace_emit(LITHE_ST_WAIT_SYNC_WAKE, NULL, _lt_key, 0, 0);
#endif
        return (0 == sync->status) ? OPAL_SUCCESS : OPAL_ERROR; }
    }

    /* Insert sync on the list of pending synchronization constructs */
    OPAL_THREAD_LOCK(&wait_sync_lock);
    if (NULL == opal_threads_base_wait_sync_list) {
        sync->next = sync->prev = sync;
        opal_threads_base_wait_sync_list = sync;
    } else {
        sync->prev = opal_threads_base_wait_sync_list->prev;
        sync->prev->next = sync;
        sync->next = opal_threads_base_wait_sync_list;
        opal_threads_base_wait_sync_list->prev = sync;
    }
    OPAL_THREAD_UNLOCK(&wait_sync_lock);

    /**
     * If we are not responsible for progressing, go silent until something
     * worth noticing happen:
     *  - this thread has been promoted to take care of the progress
     *  - our sync has been triggered.
     */
check_status:
    if (sync != opal_threads_base_wait_sync_list && num_thread_in_progress >= opal_max_thread_in_progress
#if HAVE_LITHE
        /* hosted: libnbc progresses only the owner's requests, so an owner must not
         * sleep here waiting for the elected progress thread (GAPS B33) */
        && !(NULL != opal_lithe_wait_sync_keep_progress && opal_lithe_wait_sync_keep_progress())
#endif
        ) {
        opal_thread_internal_cond_wait(&sync->condition, &sync->lock);

        /**
         * At this point either the sync was completed in which case
         * we should remove it from the wait list, or/and I was
         * promoted as the progress manager.
         */

        if (sync->count <= 0) { /* Completed? */
            opal_thread_internal_mutex_unlock(&sync->lock);
            goto i_am_done;
        }
        /* either promoted, or spurious wakeup ! */
        goto check_status;
    }
    opal_thread_internal_mutex_unlock(&sync->lock);

    OPAL_THREAD_ADD_FETCH32(&num_thread_in_progress, 1);
#if HAVE_LITHE
    /*
     * Per-waiter nopump idle cadence (see no_cq_park branch). Must NOT be a
     * function-static: hosted multi-context shares one OS process, so a
     * static counter is raced/reset across peers and starves cxi nanosleep.
     */
    unsigned lithe_nocq_idle_iters = 0;
    unsigned lithe_fullcore_yield_iters = 0;
    static int lithe_ws_single_os = -1;
    static int lithe_ws_no_cq_park = -1;
    if (lithe_ws_single_os < 0) {
        const char *e = getenv("LITHE_MTL_OFI_SINGLE_OS");
        lithe_ws_single_os = (e && e[0] == '1' && e[1] == '\0') ? 1 : 0;
    }
    if (lithe_ws_no_cq_park < 0) {
        /*
         * Opt-in LITHE_MTL_OFI_NO_CQ_PARK=1 only (launcher sets this for
         * multi-node nopump). Do NOT key off LITHE_HOSTED_PUMP=0 — that
         * coupling floored single-node P2K4 at ~70µs when the env leaked.
         */
        const char *n = getenv("LITHE_MTL_OFI_NO_CQ_PARK");
        lithe_ws_no_cq_park = (n && n[0] == '1' && n[1] == '\0') ? 1 : 0;
    }
#endif
    while (sync->count > 0) { /* progress till completion */
        /* don't progress with the sync lock locked or you'll deadlock */
        int events = opal_progress();
#if HAVE_LITHE
        /*
         * MPI_THREAD_MULTIPLE / hosted ranks: the progress-manager uthread
         * used to busy-spin opal_progress() with no yield. That hoards the
         * vcore while co-resident Lithe contexts sit RUNNABLE (e.g. peer
         * hosted ranks that must post/complete for same-process Allreduce)
         * — classic hart-starvation. Mirror libomp's inner-barrier cure:
         * if peers are waiting for a hart, donate via lithe_context_yield;
         * else park on OFI CQ readiness (opal_progress_block) instead of
         * burning the core.
         */
        if (sync->count > 0 && events <= 0) {
            /*
             * Prefer same-OS SC spin first (MTL callback). Nesting it under
             * should_yield skipped SC when soft_cap had spare harts — waiters
             * fell into CQ progress_block while a peer matched via memcpy, then
             * a later SC park backoff could strand one rank (P1K4 pairwise
             * ~1/10 timeout: three ranks OK, one never finishes RD-SUM).
             *
             * Single-OS (LITHE_MTL_OFI_SINGLE_OS=1): yield ONLY when the
             * current fork-join sched has queued RUNNABLE contexts. That is
             * strictly productive multiplexing: yield re-enqueues this
             * waiter and the same hart immediately dequeues from its own
             * queue (peer or self) — no parent hart grant involved, so the
             * old "yield strands the waiter" hazard (speculative
             * should_yield with runnable==0, P1K8/P1K16) cannot happen.
             * Without this, a spinning waiter hoards the last busy hart
             * forever while the peer that must post its Sendrecv sits
             * RUNNABLE with a drifted harts_needed ledger (see
             * fork_join_sched.c context_block NOTE) — P1K4 nx=80
             * make_local/CG stall, eu-stack: one hart in sc_try_park_pending,
             * every other vcore parked at vcore_entry_gate.
             *
             * Multi-OS: SC park returning 1 must NOT skip yield-to-runnable.
             * P2N2K4 dist=1 is same-OS SC; waiters that SC-spin while a peer
             * is still RUNNABLE (has not posted Sendrecv) never donate a hart
             * → pw=0 Flux EC=142.
             */
            if (opal_progress_sc_park()) {
                if (lithe_ws_single_os) {
                    if (lithe_fork_join_current_runnable_count() > 0) {
                        lithe_context_yield();
                    }
                } else if (lithe_fork_join_should_yield_to_runnable() ||
                           lithe_fork_join_current_runnable_count() > 0) {
                    lithe_context_yield();
                    lithe_nocq_idle_iters = 0;
                    lithe_fullcore_yield_iters = 0;
                }
            } else if (lithe_ws_single_os) {
                if (lithe_fork_join_current_runnable_count() > 0) {
                    lithe_context_yield();
                } else {
                    cpu_relax();
                }
            } else if (lithe_ws_no_cq_park) {
                /*
                 * Nopump multi-node: pure userspace spin can starve cxi
                 * CQ delivery. Yield when should_yield or runnable_count>0
                 * (P1K8 spare-hart cure). Do NOT unconditional-yield
                 * every 256 — ab9b that raised hang rate 2/20→5/20.
                 *
                 * When soft_cap==VCORE==RPH (P24K4 fullcore), runnable_count
                 * stays 0 once non-leaders park. A 5µs nanosleep / progress_block
                 * every 2048 was still too rare for CXI CQ wake on P24 Bruck;
                 * park every 32 idle iters. Always-park (NO_CQ=0) serializes
                 * P2K2 CG — keep the cadence gated.
                 */
                if (lithe_fork_join_should_yield_to_runnable() ||
                    lithe_fork_join_current_runnable_count() > 0) {
                    lithe_context_yield();
                    lithe_nocq_idle_iters = 0;
                } else if ((++lithe_nocq_idle_iters & 31U) == 0U) {
                    (void) opal_progress_block();
                } else {
                    cpu_relax();
                }
            } else if (!lithe_ws_single_os) {
                /*
                 * Multi-OS + CQ park (NO_CQ=0, P24K4 fullcore): co-resident
                 * non-leaders may stay RUNNABLE and drive opal_progress (safe
                 * shared-EP help — no FI_PEEK). If we always yield when
                 * runnable_count>0 we never enter progress_block and CXI CQ
                 * wake starves (pause-spin livelock). Yield a few times to
                 * multiplex helpers, then CQ-park; helpers' CQ drains call
                 * wake_cq_waiters so this waiter resumes.
                 */
                if ((lithe_fork_join_should_yield_to_runnable() ||
                     lithe_fork_join_current_runnable_count() > 0) &&
                    (++lithe_fullcore_yield_iters & 7U) != 0U) {
                    lithe_context_yield();
                } else {
                    lithe_fullcore_yield_iters = 0;
                    (void) opal_progress_block();
                }
            } else if (lithe_fork_join_should_yield_to_runnable()) {
                lithe_context_yield();
            } else {
                (void) opal_progress_block();
            }
        }
#else
        (void) events;
#endif
    }
    OPAL_THREAD_ADD_FETCH32(&num_thread_in_progress, -1);

i_am_done:
    /* My sync is now complete. Trim the list: remove self, wake next */
    OPAL_THREAD_LOCK(&wait_sync_lock);
    sync->prev->next = sync->next;
    sync->next->prev = sync->prev;
    /* In case I am the progress manager, pass the duties on */
    if (sync == opal_threads_base_wait_sync_list) {
        opal_threads_base_wait_sync_list = (sync == sync->next) ? NULL : sync->next;
        if (NULL != opal_threads_base_wait_sync_list) {
            WAIT_SYNC_PASS_OWNERSHIP(opal_threads_base_wait_sync_list);
        }
    }
    OPAL_THREAD_UNLOCK(&wait_sync_lock);

    {
#if HAVE_LITHE
        if (_lt_on) lithe_sched_trace_emit(LITHE_ST_WAIT_SYNC_WAKE, NULL, _lt_key, 0, 0);
#endif
        return (0 == sync->status) ? OPAL_SUCCESS : OPAL_ERROR; }
}
