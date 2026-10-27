/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2020 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007-2018 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2016      Mellanox Technologies. All rights reserved.
 * Copyright (c) 2015-2016 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
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

#ifndef OPAL_MCA_THREADS_WAIT_SYNC_H
#define OPAL_MCA_THREADS_WAIT_SYNC_H

#include "opal/mca/threads/condition.h"
#include "opal/mca/threads/mutex.h"
#include "opal/mca/threads/threads.h"
#include "opal/runtime/opal_progress.h"
#include "opal/sys/atomic.h"

#if HAVE_LITHE
#include <stdlib.h>
#include <lithe/fork_join_sched.h>
#include <lithe/lithe.h>
#include <lithe/sched_trace.h> /* Perfetto trace points, no-op unless LITHE_SCHED_TRACE=1 */
#include <parlib/arch.h> /* cpu_relax */
#include <lithe/futex.h>
#endif

BEGIN_C_DECLS

extern int opal_max_thread_in_progress;

typedef struct ompi_wait_sync_t {
    opal_atomic_int32_t count;
    int32_t status;
    opal_thread_internal_cond_t condition;
    opal_thread_internal_mutex_t lock;
    struct ompi_wait_sync_t *next;
    struct ompi_wait_sync_t *prev;
    volatile bool signaling;
#if HAVE_LITHE
    /* CQ wait fd a direct waiter is parked on (reactor), else -1: the completer
     * cancels that park as well as waking the futex (LITHE_WAIT_SYNC_CQ_PARK). */
    volatile int park_fd;
#endif
} ompi_wait_sync_t;

#define SYNC_WAIT(sync) (opal_using_threads() ? ompi_sync_wait_mt(sync) : sync_wait_st(sync))

/* The loop in release handles a race condition between the signaling
 * thread and the destruction of the condition variable. The signaling
 * member will be set to false after the final signaling thread has
 * finished operating on the sync object. This is done to avoid
 * extra atomics in the signalling function and keep it as fast
 * as possible. Note that the race window is small so spinning here
 * is more optimal than sleeping since this macro is called in
 * the critical path. */
#if HAVE_LITHE
/* LITHE_WAIT_SYNC_RELEASE_YIELD (2026-10-16). Default ON since the A/B below;
 * =0 restores the upstream spin for comparison.
 *
 * The spin on (sync)->signaling assumes the signaler is another OS thread
 * that the kernel will run. With hosted ranks it is a co-resident Lithe
 * context: WAIT_SYNC_SIGNAL takes (sync)->lock, a BLOCKING lithe mutex, and
 * if the waiter still held it the signaler blocked and was re-queued RUNNABLE
 * on the vcore it was running on -- which is the waiter's own vcore. The
 * waiter then spins here on that very hart, never yields, and the signaler
 * never runs: permanent hang with sync->count == 0.
 *
 * Caught on the login node, hosted miniFE RPH4 on 3 harts, NX80 make_local,
 * 5/9 runs of the production binary (.build_logs/login_smoke/makelocal_hang/
 * stale0_r8.inspect.txt): rank 3 spinning at request.h WAIT_SYNC_RELEASE
 * inside MPI_Allreduce; rank 1 queued RUNNABLE on rank 3's vcore with the
 * stack  lithe_mutex_lock <- wait_sync_update <- ompi_request_complete
 * <- ompi_mtl_ofi_hosted_sc_try_send (same-process delivery of the message
 * rank 3 was waiting for); two other ranks already past the Allreduce in the
 * next barrier. Same signature as the P24K4 "contexts frozen in sync_os"
 * campaign wedge (ROOT_CAUSE_HART_OVERSUBSCRIPTION.md Evidence 1).
 *
 * Yielding here is legal (uthread context, inside an MPI call) and cheap:
 * the window is normally sub-microsecond, and a yield with nothing else
 * runnable returns immediately.
 *
 * A/B, login node, tests/hosted_stress/allreduce_stress.cpp (4 contexts on 3
 * harts, 20000 Allreduces): gate off hung 8/8 before iteration 5000; gate on
 * finished 8/8 in 0.46 s (23 us/op). .build_logs/login_smoke/makelocal_hang/
 * wsr*_r*.log, docs/GAPS.md B16. */
static inline int lithe_wait_sync_release_yield_enabled(void)
{
    static int cached = -1;
    if (OPAL_UNLIKELY(cached < 0)) {
        const char *e = getenv("LITHE_WAIT_SYNC_RELEASE_YIELD");
        cached = (e && e[0] == '0' && e[1] == '\0') ? 0 : 1;
    }
    return cached;
}
#define WAIT_SYNC_RELEASE(sync)                                \
    if (opal_using_threads()) {                                \
        while ((sync)->signaling) {                            \
            if (lithe_wait_sync_release_yield_enabled()) {     \
                lithe_context_yield();                         \
            } else if (opal_progress_yield_when_idle) {        \
                opal_thread_yield();                           \
            }                                                  \
            continue;                                          \
        }                                                      \
        opal_thread_internal_cond_destroy(&(sync)->condition); \
        opal_thread_internal_mutex_destroy(&(sync)->lock);     \
    }
#else
#define WAIT_SYNC_RELEASE(sync)                                \
    if (opal_using_threads()) {                                \
        while ((sync)->signaling) {                            \
            if (opal_progress_yield_when_idle) {               \
                opal_thread_yield();                           \
            }                                                  \
            continue;                                          \
        }                                                      \
        opal_thread_internal_cond_destroy(&(sync)->condition); \
        opal_thread_internal_mutex_destroy(&(sync)->lock);     \
    }
#endif /* HAVE_LITHE */

#define WAIT_SYNC_RELEASE_NOWAIT(sync)                         \
    if (opal_using_threads()) {                                \
        opal_thread_internal_cond_destroy(&(sync)->condition); \
        opal_thread_internal_mutex_destroy(&(sync)->lock);     \
    }

#if HAVE_LITHE
/* LITHE_WAIT_SYNC_DIRECT (base/wait_sync.c): single-OS hosted worlds wait on the
 * count with a Lithe futex instead of the condvar; the completer wakes it there.
 * signaling=false stays last: the waiter's (stack) sync may vanish after it. */
OPAL_DECLSPEC int opal_lithe_wait_sync_direct_resolve(void);
/* Set by components whose requests advance only in opal_progress (coll/libnbc):
 * nonzero while the CALLING Lithe context owns such a request, so its waits keep
 * polling instead of sleeping (GAPS B33). */
OPAL_DECLSPEC extern int (*opal_lithe_wait_sync_keep_progress)(void);
/* Set by mtl/ofi's hosted SC path: 1 while the calling context spins in a direct
 * wait, 0 before it yields, sleeps or returns (base/wait_sync.c). */
OPAL_DECLSPEC extern void (*opal_lithe_wait_sync_spinning)(int on);
/* Set by mtl/ofi: park the calling context on its own CQ wait fd while sync's count
 * is still c, publishing the fd in sync->park_fd. Returns 0 if it cannot park
 * (caller falls back to the futex sleep), 1 otherwise (base/wait_sync.c). */
OPAL_DECLSPEC extern int (*opal_lithe_wait_sync_cq_park)(struct ompi_wait_sync_t *sync, int32_t c);
OPAL_DECLSPEC extern void (*opal_lithe_wait_sync_cq_unpark)(int fd);
/* Waiters (direct wait_sync, flat barrier) currently queued after a yield. */
OPAL_DECLSPEC extern opal_atomic_int32_t opal_lithe_wait_queued;
OPAL_DECLSPEC extern int opal_lithe_wait_sync_direct;
static inline int opal_lithe_wait_sync_direct_on(void)
{
    int v = opal_lithe_wait_sync_direct;
    return OPAL_LIKELY(v >= 0) ? v : opal_lithe_wait_sync_direct_resolve();
}
#define WAIT_SYNC_SIGNAL(sync)                                    \
    if (opal_using_threads()) {                                   \
        if (opal_lithe_wait_sync_direct_on()) {                   \
            futex_wake_all((int *) &(sync)->count);               \
            int _pfd = __atomic_load_n(&(sync)->park_fd,          \
                                       __ATOMIC_SEQ_CST);         \
            if (_pfd >= 0 && NULL != opal_lithe_wait_sync_cq_unpark) \
                opal_lithe_wait_sync_cq_unpark(_pfd);             \
        } else {                                                  \
            opal_thread_internal_mutex_lock(&(sync)->lock);       \
            opal_thread_internal_cond_signal(&(sync)->condition); \
            opal_thread_internal_mutex_unlock(&(sync)->lock);     \
        }                                                         \
        opal_atomic_wmb();                                        \
        (sync)->signaling = false;                                \
    }
#else
#define WAIT_SYNC_SIGNAL(sync)                                \
    if (opal_using_threads()) {                               \
        opal_thread_internal_mutex_lock(&(sync)->lock);       \
        opal_thread_internal_cond_signal(&(sync)->condition); \
        opal_thread_internal_mutex_unlock(&(sync)->lock);     \
        (sync)->signaling = false;                            \
    }
#endif

#define WAIT_SYNC_SIGNALLED(sync)  \
    {                              \
        (sync)->signaling = false; \
    }

/* not static for inline "wait_sync_st" */
OPAL_DECLSPEC extern ompi_wait_sync_t *opal_threads_base_wait_sync_list;

OPAL_DECLSPEC int ompi_sync_wait_mt(ompi_wait_sync_t *sync);
static inline int sync_wait_st(ompi_wait_sync_t *sync)
{
#if HAVE_LITHE
    uint32_t _lt_key = 0; int _lt_on = lithe_sched_trace_enabled();
    if (_lt_on) { _lt_key = lithe_sched_trace_ctx_key();
        lithe_sched_trace_emit(LITHE_ST_WAIT_SYNC_BLOCK, NULL, _lt_key, 0, (int32_t)(((uintptr_t) sync >> 4) & 0x7fffffff)); }
#endif
    assert(NULL == opal_threads_base_wait_sync_list);
    assert(NULL == sync->next);
    opal_threads_base_wait_sync_list = sync;

    while (sync->count > 0) {
        int events = opal_progress();
#if HAVE_LITHE
        if (sync->count > 0 && events <= 0) {
            /* Prefer SC spin; see ompi_sync_wait_mt for rationale. */
            if (opal_progress_sc_park()) {
                /* handled */
            } else {
                static int single_os = -1;
                if (single_os < 0) {
                    const char *e = getenv("LITHE_MTL_OFI_SINGLE_OS");
                    single_os = (e && e[0] == '1' && e[1] == '\0') ? 1 : 0;
                }
                if (single_os) {
                    /* see ompi_sync_wait_mt: never yield on single-OS SC */
#if HAVE_LITHE
                    cpu_relax();
#else
                    (void) 0;
#endif
                } else if (lithe_fork_join_should_yield_to_runnable()) {
                    lithe_context_yield();
                } else {
                    (void) opal_progress_block();
                }
            }
        }
#endif
    }
    opal_threads_base_wait_sync_list = NULL;

    {
#if HAVE_LITHE
        if (_lt_on) lithe_sched_trace_emit(LITHE_ST_WAIT_SYNC_WAKE, NULL, _lt_key, 0, 0);
#endif
        return sync->status; }
}

#if HAVE_LITHE
#define WAIT_SYNC_INIT_PARK_FD(sync) ((sync)->park_fd = -1)
#else
#define WAIT_SYNC_INIT_PARK_FD(sync) ((void) 0)
#endif
#define WAIT_SYNC_INIT(sync, c)                                    \
    do {                                                           \
        (sync)->count = (c);                                       \
        (sync)->next = NULL;                                       \
        (sync)->prev = NULL;                                       \
        (sync)->status = 0;                                        \
        (sync)->signaling = (0 != (c));                            \
        WAIT_SYNC_INIT_PARK_FD(sync);                              \
        if (opal_using_threads()) {                                \
            opal_thread_internal_cond_init(&(sync)->condition);    \
            opal_thread_internal_mutex_init(&(sync)->lock, false); \
        }                                                          \
    } while (0)

/**
 * Wake up all syncs with a particular status. If status is OMPI_SUCCESS this
 * operation is a NO-OP. Otherwise it will trigger the "error condition" from
 * all registered sync.
 */
OPAL_DECLSPEC void opal_threads_base_wait_sync_global_wakeup_st(int status);
OPAL_DECLSPEC void opal_threads_base_wait_sync_global_wakeup_mt(int status);
#define wait_sync_global_wakeup(st) \
    (opal_using_threads() ? opal_threads_base_wait_sync_global_wakeup_mt(st) : \
    		                opal_threads_base_wait_sync_global_wakeup_st(st))

/**
 * Update the status of the synchronization primitive. If an error is
 * reported the synchronization is completed and the signal
 * triggered. The status of the synchronization will be reported to
 * the waiting threads.
 */
static inline void wait_sync_update(ompi_wait_sync_t *sync, int updates, int status)
{
    if (OPAL_LIKELY(OPAL_SUCCESS == status)) {
        if (0 != (OPAL_THREAD_ADD_FETCH32(&sync->count, -updates))) {
            return;
        }
    } else {
        /* this is an error path so just use the atomic */
        sync->status = status;
        opal_atomic_wmb();
        opal_atomic_swap_32(&sync->count, 0);
    }
    WAIT_SYNC_SIGNAL(sync);
}

END_C_DECLS

#endif /* OPAL_MCA_THREADS_WAIT_SYNC_H */
