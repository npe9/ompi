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

#include <stdlib.h>
#include <time.h>

#if HAVE_LITHE
#include <lithe/fork_join_sched.h>
#include <lithe/lithe.h>
#include <parlib/arch.h> /* cpu_relax */
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

#define WAIT_SYNC_PASS_OWNERSHIP(who)                        \
    do {                                                     \
        opal_thread_internal_mutex_lock(&(who)->lock);       \
        opal_thread_internal_cond_signal(&(who)->condition); \
        opal_thread_internal_mutex_unlock(&(who)->lock);     \
    } while (0)

int ompi_sync_wait_mt(ompi_wait_sync_t *sync)
{
    /* Don't stop if the waiting synchronization is completed. We avoid the
     * race condition around the release of the synchronization using the
     * signaling field.
     */
    if (sync->count <= 0) {
        return (0 == sync->status) ? OPAL_SUCCESS : OPAL_ERROR;
    }

    /* lock so nobody can signal us during the list updating */
    opal_thread_internal_mutex_lock(&sync->lock);

    /* Now that we hold the lock make sure another thread has not already
     * call cond_signal.
     */
    if (sync->count <= 0) {
        opal_thread_internal_mutex_unlock(&sync->lock);
        return (0 == sync->status) ? OPAL_SUCCESS : OPAL_ERROR;
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
    if (sync != opal_threads_base_wait_sync_list && num_thread_in_progress >= opal_max_thread_in_progress) {
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
             * Single-OS (LITHE_MTL_OFI_SINGLE_OS=1): never yield — peers may
             * already be in flat Barrier spin; yielding strands this waiter
             * when soft_cap spare harts do not show up (P1K8/P1K16).
             */
            if (opal_progress_sc_park()) {
                /* SC spin handled this iter; do not yield/CQ-park. */
            } else {
                static int single_os = -1;
                static int no_cq_park = -1;
                if (single_os < 0) {
                    const char *e = getenv("LITHE_MTL_OFI_SINGLE_OS");
                    single_os = (e && e[0] == '1' && e[1] == '\0') ? 1 : 0;
                }
                if (no_cq_park < 0) {
                    /*
                     * Multi-node nopump (LITHE_HOSTED_PUMP=0) or explicit
                     * NO_CQ_PARK: never opal_progress_block — all waiters
                     * parked with MTP=1 never wake (pre-pairwise hang).
                     * Yield-to-runnable then cpu_relax instead.
                     * SINGLE_OS stays cpu_relax-only (never yield) — see
                     * P1K8/P1K16 stranding note above.
                     */
                    const char *n = getenv("LITHE_MTL_OFI_NO_CQ_PARK");
                    const char *p = getenv("LITHE_HOSTED_PUMP");
                    no_cq_park =
                        ((n && n[0] == '1' && n[1] == '\0') ||
                         (p && p[0] == '0' && p[1] == '\0'))
                            ? 1
                            : 0;
                }
                if (single_os) {
                    cpu_relax();
                } else if (no_cq_park) {
                    /*
                     * Nopump multi-node: pure userspace spin can starve cxi
                     * CQ delivery. Occasional 5µs clock_nanosleep gives
                     * kernel entry without the 50µs host pump (med
                     * ~130–800µs). Yield when should_yield or runnable_count>0
                     * (P1K8 spare-hart cure). Do NOT unconditional-yield
                     * every 256 — ab9b that raised hang rate 2/20→5/20.
                     */
                    static unsigned idle_iters = 0;
                    if (lithe_fork_join_should_yield_to_runnable() ||
                        lithe_fork_join_current_runnable_count() > 0) {
                        lithe_context_yield();
                        idle_iters = 0;
                    } else if ((++idle_iters & 2047U) == 0U) {
                        struct timespec req = {.tv_sec = 0, .tv_nsec = 5000L};
                        (void) clock_nanosleep(CLOCK_MONOTONIC, 0, &req, NULL);
                    } else {
                        cpu_relax();
                    }
                } else if (lithe_fork_join_should_yield_to_runnable()) {
                    lithe_context_yield();
                } else {
                    (void) opal_progress_block();
                }
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

    return (0 == sync->status) ? OPAL_SUCCESS : OPAL_ERROR;
}
