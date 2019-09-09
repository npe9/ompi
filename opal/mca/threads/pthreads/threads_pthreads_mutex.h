/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007-2018 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2015-2016 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2019      Sandia National Laboratories.  All rights reserved.
 *
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef  OPAL_MCA_THREADS_PTHREADS_THREADS_PTHREADS_MUTEX_H
#define  OPAL_MCA_THREADS_PTHREADS_THREADS_PTHREADS_MUTEX_H 1

/**
 * @file:
 *
 * Mutual exclusion functions: Unix implementation.
 *
 * Functions for locking of critical sections.
 *
 * On unix, use pthreads or our own atomic operations as
 * available.
 */

#include "opal_config.h"

#include <pthread.h>
#include <errno.h>
#include <stdio.h>

#include "opal/class/opal_object.h"
#include "opal/sys/atomic.h"

BEGIN_C_DECLS

struct opal_mutex_t {
    opal_object_t super;

    pthread_mutex_t m_lock_pthread;

#if OPAL_ENABLE_DEBUG
    int m_lock_debug;
    const char *m_lock_file;
    int m_lock_line;
#endif

    opal_atomic_lock_t m_lock_atomic;
};
OPAL_DECLSPEC OBJ_CLASS_DECLARATION(opal_mutex_t);
OPAL_DECLSPEC OBJ_CLASS_DECLARATION(opal_recursive_mutex_t);

#if defined(PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP)
#define OPAL_PTHREAD_RECURSIVE_MUTEX_INITIALIZER PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP
#elif defined(PTHREAD_RECURSIVE_MUTEX_INITIALIZER)
#define OPAL_PTHREAD_RECURSIVE_MUTEX_INITIALIZER PTHREAD_RECURSIVE_MUTEX_INITIALIZER
#endif

#if OPAL_ENABLE_DEBUG
#define OPAL_MUTEX_STATIC_INIT                                          \
    {                                                                   \
        .super = OPAL_OBJ_STATIC_INIT(opal_mutex_t),                    \
        .m_lock_pthread = PTHREAD_MUTEX_INITIALIZER,                    \
        .m_lock_debug = 0,                                              \
        .m_lock_file = NULL,                                            \
        .m_lock_line = 0,                                               \
        .m_lock_atomic = OPAL_ATOMIC_LOCK_INIT,                         \
    }
#else
#define OPAL_MUTEX_STATIC_INIT                                          \
    {                                                                   \
        .super = OPAL_OBJ_STATIC_INIT(opal_mutex_t),                    \
        .m_lock_pthread = PTHREAD_MUTEX_INITIALIZER,                    \
        .m_lock_atomic = OPAL_ATOMIC_LOCK_INIT,                         \
    }
#endif

#if defined(OPAL_PTHREAD_RECURSIVE_MUTEX_INITIALIZER)

#if OPAL_ENABLE_DEBUG
#define OPAL_RECURSIVE_MUTEX_STATIC_INIT                                \
    {                                                                   \
        .super = OPAL_OBJ_STATIC_INIT(opal_mutex_t),                    \
        .m_lock_pthread = OPAL_PTHREAD_RECURSIVE_MUTEX_INITIALIZER,     \
        .m_lock_debug = 0,                                              \
        .m_lock_file = NULL,                                            \
        .m_lock_line = 0,                                               \
        .m_lock_atomic = OPAL_ATOMIC_LOCK_INIT,                         \
    }
#else
#define OPAL_RECURSIVE_MUTEX_STATIC_INIT                                \
    {                                                                   \
        .super = OPAL_OBJ_STATIC_INIT(opal_mutex_t),                    \
        .m_lock_pthread = OPAL_PTHREAD_RECURSIVE_MUTEX_INITIALIZER,     \
        .m_lock_atomic = OPAL_ATOMIC_LOCK_INIT,                         \
    }
#endif

#endif


typedef pthread_cond_t opal_cond_t;
#define OPAL_CONDITION_STATIC_INIT PTHREAD_COND_INITIALIZER
#define opal_cond_init(a)          pthread_cond_init(a, NULL)
#define opal_cond_wait(a,b)        pthread_cond_wait(a, &(b)->m_lock_pthread)
#define opal_cond_broadcast(a)     pthread_cond_broadcast(a)
#define opal_cond_signal(a)        pthread_cond_signal(a)
#define opal_cond_destroy(a)       pthread_cond_destroy(a)

END_C_DECLS

#endif           /* OPAL_MCA_THREADS_PTHREADS_THREADS_PTHREADS_MUTEX_H */

 int opal_mutex_trylock(opal_mutex_t *m);

 void opal_mutex_lock(opal_mutex_t *m);

 void opal_mutex_unlock(opal_mutex_t *m);


#if OPAL_HAVE_ATOMIC_SPINLOCKS

 int opal_mutex_atomic_trylock(opal_mutex_t *m);


 void opal_mutex_atomic_lock(opal_mutex_t *m);


 void opal_mutex_atomic_unlock(opal_mutex_t *m);

#else

 int opal_mutex_atomic_trylock(opal_mutex_t *m);

 void opal_mutex_atomic_lock(opal_mutex_t *m);


 void opal_mutex_atomic_unlock(opal_mutex_t *m);


#endif