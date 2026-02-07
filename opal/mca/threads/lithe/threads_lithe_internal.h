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
 * Copyright (c) 2014      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef OPAL_MCA_THREADS_LITHE_INTERNAL_H
#define OPAL_MCA_THREADS_LITHE_INTERNAL_H

#include "opal_config.h"
#include "opal/mca/threads/threads.h"
#include "lithe/lithe.h"
#include "lithe/mutex.h"

/* Forward declarations */
struct opal_threads_base_module_t;
typedef struct opal_threads_base_module_t opal_threads_base_module_t;
#include "lithe/condvar.h"
#include <pthread.h>

BEGIN_C_DECLS

/* Define the internal types that OpenMPI expects */
typedef lithe_mutex_t opal_thread_internal_mutex_t;
typedef lithe_condvar_t opal_thread_internal_cond_t;
typedef pthread_key_t opal_tsd_key_t;

/* Define the mutex initializer macro */
/* LITHE_MUTEX_INITIALIZER is a macro that takes an argument, so we need to expand it */
/* For static initialization, we'll use a zero-initialized structure */
/* The mutex will need to be properly initialized with lithe_mutex_init() before use */
#define OPAL_THREAD_INTERNAL_MUTEX_INITIALIZER {0}

/* Define the condvar initializer macro */
/* LITHE_CONDVAR_INITIALIZER is a macro that takes an argument, so we need to expand it */
/* For static initialization, we'll use a zero-initialized structure */
/* The condvar will need to be properly initialized before use */
#define OPAL_THREAD_INTERNAL_COND_INITIALIZER {0}

/* Define the internal mutex functions that OpenMPI expects */
static inline int opal_thread_internal_mutex_init(opal_thread_internal_mutex_t *mutex)
{
    return lithe_mutex_init(mutex, NULL);
}

static inline int opal_thread_internal_mutex_destroy(opal_thread_internal_mutex_t *mutex)
{
    /* lithe mutexes don't need explicit destruction */
    return 0;
}

static inline int opal_thread_internal_mutex_lock(opal_thread_internal_mutex_t *mutex)
{
    return lithe_mutex_lock(mutex);
}

static inline int opal_thread_internal_mutex_unlock(opal_thread_internal_mutex_t *mutex)
{
    return lithe_mutex_unlock(mutex);
}

static inline int opal_thread_internal_mutex_trylock(opal_thread_internal_mutex_t *mutex)
{
    return lithe_mutex_trylock(mutex);
}

/* Define the TSD functions that OpenMPI expects using pthread keys */
static inline int opal_tsd_key_create(opal_tsd_key_t *key, void (*destructor)(void*))
{
    pthread_key_t *pkey = (pthread_key_t*)key;
    return pthread_key_create(pkey, destructor);
}

static inline int opal_tsd_key_delete(opal_tsd_key_t key)
{
    pthread_key_t pkey = (pthread_key_t)key;
    return pthread_key_delete(pkey);
}

static inline int opal_tsd_setspecific(opal_tsd_key_t key, void *value)
{
    pthread_key_t pkey = (pthread_key_t)key;
    return pthread_setspecific(pkey, value);
}

static inline int opal_tsd_get(opal_tsd_key_t key, void **valuep)
{
    pthread_key_t pkey = (pthread_key_t)key;
    void *value = pthread_getspecific(pkey);
    if (valuep != NULL) {
        *valuep = value;
    }
    return 0;
}

/* Define the thread yield function that OpenMPI expects */

/* Thread module structure */
extern opal_threads_base_module_t opal_threads_lithe_module;

/* Basic lithe thread functions */
int opal_threads_lithe_init(void);
int opal_threads_lithe_finalize(void);

END_C_DECLS

#endif /* OPAL_MCA_THREADS_LITHE_INTERNAL_H */
