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
#include "lithe/lithe.h"
#include "lithe/mutex.h"
#include "lithe/condvar.h"

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

/* Hybrid mutex: uses Lithe when in Lithe context, pthread otherwise */
typedef struct {
    lithe_mutex_t lithe_lock;
    pthread_mutex_t pthread_lock;
    int use_pthread;  /* 1 if initialized for pthread fallback */
} opal_thread_internal_mutex_t;

typedef struct {
    lithe_condvar_t lithe_cond;
    pthread_cond_t pthread_cond;
} opal_thread_internal_cond_t;

#define OPAL_THREAD_INTERNAL_MUTEX_INITIALIZER { \
    .lithe_lock = { .attr = {0}, .queue = {NULL, NULL}, .lock = {0}, .qnode = NULL, .locked = 0, .owner = NULL }, \
    .pthread_lock = PTHREAD_MUTEX_INITIALIZER, \
    .use_pthread = 0 \
}
#define OPAL_THREAD_INTERNAL_RECURSIVE_MUTEX_INITIALIZER { \
    .lithe_lock = { .attr = {LITHE_MUTEX_RECURSIVE}, .queue = {NULL, NULL}, .lock = {0}, .qnode = NULL, .locked = 0, .owner = NULL }, \
    .pthread_lock = PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP, \
    .use_pthread = 0 \
}
#define OPAL_THREAD_INTERNAL_COND_INITIALIZER { \
    .lithe_cond = { .lock = {0}, .waiting_qnode = NULL, .waiting_mutex = NULL, .queue = {NULL, NULL} }, \
    .pthread_cond = PTHREAD_COND_INITIALIZER \
}

/* Check if we're in a Lithe context */
static inline int in_lithe_context(void) {
    extern lithe_context_t *lithe_context_self(void);
    return lithe_context_self() != NULL;
}

static inline int opal_thread_internal_mutex_init(opal_thread_internal_mutex_t *p_mutex, bool recursive)
{
    lithe_mutexattr_t attr;
    lithe_mutexattr_init(&attr);
    if (recursive) {
        lithe_mutexattr_settype(&attr, LITHE_MUTEX_RECURSIVE);
    }
    lithe_mutex_init(&p_mutex->lithe_lock, &attr);
    
    pthread_mutexattr_t pattr;
    pthread_mutexattr_init(&pattr);
    if (recursive) {
        pthread_mutexattr_settype(&pattr, PTHREAD_MUTEX_RECURSIVE);
    }
    pthread_mutex_init(&p_mutex->pthread_lock, &pattr);
    pthread_mutexattr_destroy(&pattr);
    p_mutex->use_pthread = 0;
    return OPAL_SUCCESS;
}

static inline int opal_thread_internal_mutex_init_recursive(opal_thread_internal_mutex_t *p_mutex)
{
    return opal_thread_internal_mutex_init(p_mutex, true);
}

static inline int opal_thread_internal_mutex_destroy(opal_thread_internal_mutex_t *p_mutex)
{
    pthread_mutex_destroy(&p_mutex->pthread_lock);
    return OPAL_SUCCESS;
}

/* Forward declaration - defined in threads_lithe_module.c */
OPAL_DECLSPEC void ensure_main_fj_context(void);

static inline int opal_thread_internal_mutex_lock(opal_thread_internal_mutex_t *p_mutex)
{
    if (in_lithe_context()) {
        ensure_main_fj_context();
        return 0 == lithe_mutex_lock(&p_mutex->lithe_lock) ? OPAL_SUCCESS : OPAL_ERR_IN_ERRNO;
    } else {
        /* Not in Lithe context (e.g., PMIx pthread) - use pthread */
        return 0 == pthread_mutex_lock(&p_mutex->pthread_lock) ? OPAL_SUCCESS : OPAL_ERR_IN_ERRNO;
    }
}

static inline int opal_thread_internal_mutex_trylock(opal_thread_internal_mutex_t *p_mutex)
{
    if (in_lithe_context()) {
        return 0 == lithe_mutex_trylock(&p_mutex->lithe_lock) ? OPAL_SUCCESS : OPAL_ERR_IN_ERRNO;
    } else {
        return 0 == pthread_mutex_trylock(&p_mutex->pthread_lock) ? OPAL_SUCCESS : OPAL_ERR_IN_ERRNO;
    }
}

static inline int opal_thread_internal_mutex_unlock(opal_thread_internal_mutex_t *p_mutex)
{
    if (in_lithe_context()) {
        return 0 == lithe_mutex_unlock(&p_mutex->lithe_lock) ? OPAL_SUCCESS : OPAL_ERR_IN_ERRNO;
    } else {
        return 0 == pthread_mutex_unlock(&p_mutex->pthread_lock) ? OPAL_SUCCESS : OPAL_ERR_IN_ERRNO;
    }
}

static inline int opal_thread_internal_cond_init(opal_thread_internal_cond_t *p_cond)
{
    lithe_condvar_init(&p_cond->lithe_cond);
    pthread_cond_init(&p_cond->pthread_cond, NULL);
    return OPAL_SUCCESS;
}

static inline int opal_thread_internal_cond_destroy(opal_thread_internal_cond_t *p_cond)
{
    pthread_cond_destroy(&p_cond->pthread_cond);
    return OPAL_SUCCESS;
}

static inline int opal_thread_internal_cond_wait(opal_thread_internal_cond_t *p_cond, opal_thread_internal_mutex_t *p_mutex)
{
    if (in_lithe_context()) {
        ensure_main_fj_context();
        return 0 == lithe_condvar_wait(&p_cond->lithe_cond, &p_mutex->lithe_lock) ? OPAL_SUCCESS : OPAL_ERR_IN_ERRNO;
    } else {
        return 0 == pthread_cond_wait(&p_cond->pthread_cond, &p_mutex->pthread_lock) ? OPAL_SUCCESS : OPAL_ERR_IN_ERRNO;
    }
}

static inline int opal_thread_internal_cond_signal(opal_thread_internal_cond_t *p_cond)
{
    /* Signal both - one will be no-op */
    lithe_condvar_signal(&p_cond->lithe_cond);
    pthread_cond_signal(&p_cond->pthread_cond);
    return OPAL_SUCCESS;
}

static inline int opal_thread_internal_cond_broadcast(opal_thread_internal_cond_t *p_cond)
{
    /* Broadcast both - one will be no-op */
    lithe_condvar_broadcast(&p_cond->lithe_cond);
    pthread_cond_broadcast(&p_cond->pthread_cond);
    return OPAL_SUCCESS;
}

END_C_DECLS

#endif /* OPAL_MCA_THREADS_LITHE_THREADS_LITHE_MUTEX_H */

