/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
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
 * Copyright (c) 2007-2016 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"

#include <pthread.h>

#include "opal/mca/threads/mutex.h"
//#include "opal/mca/threads/pthreads/mutex_unix.h"


/*
 * Wait and see if some upper layer wants to use threads, if support
 * exists.
 */
bool opal_uses_threads = false;

struct opal_pthread_mutex_t {
    opal_object_t super;

    pthread_mutex_t m_lock_pthread;

#if OPAL_ENABLE_DEBUG
    int m_lock_debug;
    const char *m_lock_file;
    int m_lock_line;
#endif

    opal_atomic_lock_t m_lock_atomic;
};
typedef struct opal_pthread_mutex_t opal_pthread_mutex_t;
typedef struct opal_pthread_mutex_t opal_pthread_recursive_mutex_t;

#if OPAL_ENABLE_DEBUG
opal_mutex_t *opal_init_mutex(void) {                                   
    opal_pthread_mutex_t mutex = {                                                                   
        .super = OPAL_OBJ_STATIC_INIT(opal_mutex_t),                    
        .m_lock_pthread = PTHREAD_MUTEX_INITIALIZER,                    
        .m_lock_debug = 0,                                              
        .m_lock_file = NULL,                                            
        .m_lock_line = 0,                                               
        .m_lock_atomic = { .u = { .lock = OPAL_ATOMIC_UNLOCKED } },     
    };
	return &mutex.super;
}
#else
opal_mutex_t *opal_init_mutex(void) {
	 opal_pthread_mutex_t mutex = {                                                                  
        .super = OPAL_OBJ_STATIC_INIT(opal_mutex_t),                    
        .m_lock_pthread = PTHREAD_MUTEX_INITIALIZER,                    
        .m_lock_atomic = { .u = { .lock = OPAL_ATOMIC_UNLOCKED } },     
    };
	return &mutex.super;
}
#endif

void mca_threads_pthreads_mutex_construct(opal_pthread_mutex_t *m)
{
#if OPAL_ENABLE_DEBUG
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);

    /* set type to ERRORCHECK so that we catch recursive locks */
#if OPAL_HAVE_PTHREAD_MUTEX_ERRORCHECK_NP
    pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_ERRORCHECK_NP);
#elif OPAL_HAVE_PTHREAD_MUTEX_ERRORCHECK
    pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_ERRORCHECK);
#endif /* OPAL_HAVE_PTHREAD_MUTEX_ERRORCHECK_NP */

    pthread_mutex_init(&m->m_lock_pthread, &attr);
    pthread_mutexattr_destroy(&attr);

    m->m_lock_debug = 0;
    m->m_lock_file = NULL;
    m->m_lock_line = 0;
#else

    /* Without debugging, choose the fastest available mutexes */
    pthread_mutex_init(&m->m_lock_pthread, NULL);

#endif /* OPAL_ENABLE_DEBUG */

#if OPAL_HAVE_ATOMIC_SPINLOCKS
    opal_atomic_init( &m->m_lock_atomic, OPAL_ATOMIC_UNLOCKED );
#endif
}

void mca_threads_pthreads_mutex_destruct(opal_pthread_mutex_t *m)
{
    pthread_mutex_destroy(&m->m_lock_pthread);
}

OBJ_CLASS_INSTANCE(opal_pthread_mutex_t,
                   opal_mutex_t,
                   mca_threads_pthreads_mutex_construct,
                   mca_threads_pthreads_mutex_destruct);

void mca_threads_pthreads_recursive_mutex_construct(opal_pthread_recursive_mutex_t *m)
{
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);

#if OPAL_ENABLE_DEBUG
    m->m_lock_debug = 0;
    m->m_lock_file = NULL;
    m->m_lock_line = 0;
#endif

    pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);

    pthread_mutex_init(&m->m_lock_pthread, &attr);
    pthread_mutexattr_destroy(&attr);

#if OPAL_HAVE_ATOMIC_SPINLOCKS
    opal_atomic_init( &m->m_lock_atomic, OPAL_ATOMIC_UNLOCKED );
#endif
}

OBJ_CLASS_INSTANCE(opal_pthread_recursive_mutex_t,
                   opal_mutex_t,
                   mca_threads_pthreads_recursive_mutex_construct,
                   mca_threads_pthreads_mutex_destruct);