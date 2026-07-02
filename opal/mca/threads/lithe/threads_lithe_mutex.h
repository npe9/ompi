/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2010      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2014-2015 Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef OPAL_MCA_THREADS_LITHE_THREADS_LITHE_MUTEX_H
#define OPAL_MCA_THREADS_LITHE_THREADS_LITHE_MUTEX_H

#include "opal_config.h"
#include "opal/class/opal_object.h"
#include "opal/constants.h"
#include "opal/prefetch.h"
#include <stdio.h>
#include <stdlib.h>

/* lithe headers pull parlib; static inlines there trigger -Wunused-function under OPAL's -Wall. */
#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-function"
#endif
#include "lithe/lithe.h"
#include "lithe/mutex.h"
#include "lithe/condvar.h"
#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC diagnostic pop
#endif

/* Avoid macro collisions with parlib */
#ifdef CHECK_FLAG
#undef CHECK_FLAG
#endif

/* Avoid atomic macro collisions with parlib */
#ifdef atomic_add
#undef atomic_add
#endif
#ifdef atomic_sub
#undef atomic_sub
#endif
#ifdef atomic_and
#undef atomic_and
#endif
#ifdef atomic_or
#undef atomic_or
#endif
#ifdef atomic_xor
#undef atomic_xor
#endif

BEGIN_C_DECLS

#include <pthread.h>

struct uthread;
extern __thread struct uthread *current_uthread;

/** Abort if not on a Lithe-managed uthread (stray pthread detection).
 *  in_vcore_context() is only true in raw vcore scheduling callbacks, not
 *  in uthreads. current_uthread != NULL is the correct check: it's non-NULL
 *  for any thread managed by parlib/Lithe and NULL for raw pthreads. */
#define LITHE_ASSERT_VCORE() do { \
    if (current_uthread == NULL) { \
        fprintf(stderr, "[LITHE] FATAL: Lithe component entered from non-vcore context " \
                "(stray pthread? current_uthread=NULL). Aborting.\n"); \
        abort(); \
    } \
} while (0)

#define LITHE_MUTEX_ENSURE_VCORE()                       \
    do {                                                 \
        if (OPAL_UNLIKELY(current_uthread == NULL)) {    \
            lithe_ensure_main_on_vcore0();               \
        }                                                \
    } while (0)

/* Lithe-only mutual exclusion. No pthread fields: lithified runtimes must not
 * use Linux mutual exclusion. See .cursor/rules/lithified-runtimes-no-linux-primitives.mdc */
typedef struct {
    lithe_mutex_t lithe_lock;
} opal_thread_internal_mutex_t;

typedef struct {
    lithe_condvar_t lithe_cond;
} opal_thread_internal_cond_t;

#define OPAL_THREAD_INTERNAL_MUTEX_INITIALIZER { \
    .lithe_lock = { .attr = {0}, .queue = {NULL, NULL}, .lock = {0}, .qnode = NULL, .locked = 0, .owner = NULL } \
}
#define OPAL_THREAD_INTERNAL_RECURSIVE_MUTEX_INITIALIZER { \
    .lithe_lock = { .attr = {LITHE_MUTEX_RECURSIVE}, .queue = {NULL, NULL}, .lock = {0}, .qnode = NULL, .locked = 0, .owner = NULL } \
}
#define OPAL_THREAD_INTERNAL_COND_INITIALIZER { \
    .lithe_cond = { .lock = {0}, .waiting_qnode = NULL, .waiting_mutex = NULL, .queue = {NULL, NULL} } \
}

static inline int opal_thread_internal_mutex_init(opal_thread_internal_mutex_t *p_mutex, bool recursive)
{
    lithe_mutexattr_t attr;
    lithe_mutexattr_init(&attr);
    if (recursive) {
        lithe_mutexattr_settype(&attr, LITHE_MUTEX_RECURSIVE);
    }
    lithe_mutex_init(&p_mutex->lithe_lock, &attr);
    return OPAL_SUCCESS;
}

static inline int opal_thread_internal_mutex_init_recursive(opal_thread_internal_mutex_t *p_mutex)
{
    return opal_thread_internal_mutex_init(p_mutex, true);
}

static inline int opal_thread_internal_mutex_destroy(opal_thread_internal_mutex_t *p_mutex)
{
    /* Lithe mutexes need no explicit destruction. */
    (void) p_mutex;
    return OPAL_SUCCESS;
}

/* Forward declaration - defined in threads_lithe_module.c */
OPAL_DECLSPEC void ensure_main_fj_context(void);

static inline int opal_thread_internal_mutex_lock(opal_thread_internal_mutex_t *p_mutex)
{
    LITHE_MUTEX_ENSURE_VCORE();
    LITHE_ASSERT_VCORE();
    ensure_main_fj_context();
    return 0 == lithe_mutex_lock(&p_mutex->lithe_lock) ? OPAL_SUCCESS : OPAL_ERR_IN_ERRNO;
}

static inline int opal_thread_internal_mutex_trylock(opal_thread_internal_mutex_t *p_mutex)
{
    LITHE_MUTEX_ENSURE_VCORE();
    LITHE_ASSERT_VCORE();
    return 0 == lithe_mutex_trylock(&p_mutex->lithe_lock) ? OPAL_SUCCESS : OPAL_ERR_IN_ERRNO;
}

static inline int opal_thread_internal_mutex_unlock(opal_thread_internal_mutex_t *p_mutex)
{
    LITHE_MUTEX_ENSURE_VCORE();
    LITHE_ASSERT_VCORE();
    return 0 == lithe_mutex_unlock(&p_mutex->lithe_lock) ? OPAL_SUCCESS : OPAL_ERR_IN_ERRNO;
}

static inline int opal_thread_internal_cond_init(opal_thread_internal_cond_t *p_cond)
{
    lithe_condvar_init(&p_cond->lithe_cond);
    return OPAL_SUCCESS;
}

static inline int opal_thread_internal_cond_destroy(opal_thread_internal_cond_t *p_cond)
{
    /* Lithe condvars need no explicit destruction. */
    (void) p_cond;
    return OPAL_SUCCESS;
}

static inline int opal_thread_internal_cond_wait(opal_thread_internal_cond_t *p_cond, opal_thread_internal_mutex_t *p_mutex)
{
    LITHE_MUTEX_ENSURE_VCORE();
    LITHE_ASSERT_VCORE();
    ensure_main_fj_context();
    return 0 == lithe_condvar_wait(&p_cond->lithe_cond, &p_mutex->lithe_lock)
               ? OPAL_SUCCESS
               : OPAL_ERR_IN_ERRNO;
}

static inline int opal_thread_internal_cond_signal(opal_thread_internal_cond_t *p_cond)
{
    LITHE_MUTEX_ENSURE_VCORE();
    LITHE_ASSERT_VCORE();
    lithe_condvar_signal(&p_cond->lithe_cond);
    return OPAL_SUCCESS;
}

static inline int opal_thread_internal_cond_broadcast(opal_thread_internal_cond_t *p_cond)
{
    LITHE_MUTEX_ENSURE_VCORE();
    LITHE_ASSERT_VCORE();
    lithe_condvar_broadcast(&p_cond->lithe_cond);
    return OPAL_SUCCESS;
}

END_C_DECLS

#endif /* OPAL_MCA_THREADS_LITHE_THREADS_LITHE_MUTEX_H */

