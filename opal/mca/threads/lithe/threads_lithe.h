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

#ifndef OPAL_MCA_THREADS_LITHE_THREADS_LITHE_H
#define OPAL_MCA_THREADS_LITHE_THREADS_LITHE_H

#include "opal_config.h"
#include "opal/mca/threads/threads.h"
#include "opal/mca/threads/lithe/threads_lithe_threads.h"
#include <sched.h>
#if defined(__GNUC__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-function"
#endif
#include "lithe/lithe.h"
#if defined(__GNUC__)
#pragma GCC diagnostic pop
#endif

BEGIN_C_DECLS

/* LITHE_ASSERT_VCORE and in_vcore_context declaration: threads_lithe_mutex.h (via threads.h → mutex.h). */

/* Forward declarations */
struct opal_thread_t;
typedef struct opal_thread_t opal_thread_t;

/* Function pointer types */
typedef int (*opal_threads_init_fn_t)(void);
typedef int (*opal_threads_finalize_fn_t)(void);
typedef int (*opal_thread_create_fn_t)(opal_thread_fn_t func, void *arg, opal_thread_t *t, int *priority);
typedef int (*opal_thread_join_fn_t)(opal_thread_t *t, void **exit_status);
typedef int (*opal_thread_self_fn_t)(opal_thread_t *t);
typedef int (*opal_thread_equal_fn_t)(opal_thread_t t1, opal_thread_t t2);
typedef int (*opal_thread_yield_fn_t)(void);
typedef int (*opal_thread_set_affinity_fn_t)(opal_thread_t *t, void *topo, int bitmap_index);
typedef int (*opal_thread_get_affinity_fn_t)(opal_thread_t *t, void *topo, int bitmap_index);

/* Base module structure */
struct opal_threads_base_module_t {
    opal_threads_init_fn_t threads_init;
    opal_threads_finalize_fn_t threads_finalize;
    opal_thread_create_fn_t thread_create;
    opal_thread_join_fn_t thread_join;
    opal_thread_self_fn_t thread_self;
    opal_thread_equal_fn_t thread_equal;
    opal_thread_yield_fn_t thread_yield;
    opal_thread_set_affinity_fn_t thread_set_affinity;
    opal_thread_get_affinity_fn_t thread_get_affinity;
};

typedef struct opal_threads_base_module_t opal_threads_base_module_t;

/* Thread module structure */
extern opal_threads_base_module_t opal_threads_lithe_module;

/* Alias for base module access - allows base code to use opal_threads_base_module */
#define opal_threads_base_module opal_threads_lithe_module

/**
 * Initialize the lithe threading component
 */
OPAL_DECLSPEC int opal_threads_lithe_init(void);

/**
 * Finalize the lithe threading component
 */
OPAL_DECLSPEC int opal_threads_lithe_finalize(void);

/**
 * Create a new thread
 */
OPAL_DECLSPEC int opal_threads_lithe_thread_create(opal_thread_fn_t func, void *arg);

/**
 * Join a thread
 */
OPAL_DECLSPEC int opal_threads_lithe_thread_join(opal_thread_t *thread);

/**
 * Yield the current thread
 */
OPAL_DECLSPEC int opal_threads_lithe_yield(void);

/**
 * Get next pending helper context (called by lithe library)
 */
lithe_context_t *opal_get_next_helper_context(void);

/**
 * Pre-initialize lithe threading component (must be called before opal_init_util)
 */
OPAL_DECLSPEC void opal_threads_lithe_preinit(void);

/**
 * Create opal fork-join scheduler once, register it with PMIx, and enter it when
 * a parent Lithe scheduler exists (lithe_sched_current() != NULL).
 */
OPAL_DECLSPEC void opal_threads_lithe_ensure_opal_fork_join_sched(void);

/**
 * Ensure lithe threads component is initialized
 */
static inline void opal_threads_lithe_ensure_init(void)
{
    static bool initialized = false;
    if (!initialized) {
        if (opal_threads_lithe_module.threads_init != NULL) {
            opal_threads_lithe_module.threads_init();
        }
        initialized = true;
    }
}

/**
 * Yield the current thread using Lithe
 */
static inline void opal_thread_yield(void)
{
    LITHE_ASSERT_VCORE();
    lithe_context_yield();
    sched_yield();
}

END_C_DECLS

#endif /* OPAL_MCA_THREADS_LITHE_THREADS_LITHE_H */
