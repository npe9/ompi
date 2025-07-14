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
 * Copyright (c) 2020      Triad National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2021      Argonne National Laboratory.  All rights reserved.
 *
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef OPAL_MCA_THREADS_LITHE_THREADS_LITHE_MUTEX_H
#define OPAL_MCA_THREADS_LITHE_THREADS_LITHE_MUTEX_H

/**
 * @file:
 *
 * Mutual exclusion functions: Lithe implementation.
 *
 * Functions for locking of critical sections.
 *
 * Uses Lithe's mutex and condition variable implementations.
 */

#include "opal_config.h"

#include <errno.h>
#include <stdio.h>

#include "opal/class/opal_object.h"
#include "opal/constants.h"
#include "opal/util/show_help.h"

/* Complete structure definitions for parlib types */
typedef struct mcs_lock_qnode {
    volatile struct mcs_lock_qnode* volatile next;
    volatile int locked;
} mcs_lock_qnode_t;

typedef struct mcs_pdr_lock {
    mcs_lock_qnode_t* lock;
} mcs_pdr_lock_t;

typedef struct lithe_context_queue {
    /* This is a TAILQ_HEAD macro, so we need to provide the structure */
    struct lithe_context *tqh_first;
    struct lithe_context **tqh_last;
} lithe_context_queue_t;

/* Complete structure definitions for lithe types */
typedef struct lithe_mutexattr {
    int type;
} lithe_mutexattr_t;

typedef struct lithe_mutex {
    lithe_mutexattr_t attr;
    lithe_context_queue_t queue;
    mcs_pdr_lock_t lock;
    mcs_lock_qnode_t *qnode;
    int locked;
} lithe_mutex_t;

typedef struct lithe_condvar {
    mcs_pdr_lock_t lock;
    mcs_lock_qnode_t *waiting_qnode;
    lithe_mutex_t *waiting_mutex;
    lithe_context_queue_t queue;
} lithe_condvar_t;

BEGIN_C_DECLS

typedef lithe_mutex_t opal_thread_internal_mutex_t;

#define OPAL_THREAD_INTERNAL_MUTEX_INITIALIZER {0}
#define OPAL_THREAD_INTERNAL_RECURSIVE_MUTEX_INITIALIZER {0}

int opal_thread_internal_mutex_init_recursive(opal_thread_internal_mutex_t *p_mutex);

int opal_thread_internal_mutex_init(opal_thread_internal_mutex_t *p_mutex, bool recursive);
void opal_thread_internal_mutex_lock(opal_thread_internal_mutex_t *p_mutex);
int opal_thread_internal_mutex_trylock(opal_thread_internal_mutex_t *p_mutex);
void opal_thread_internal_mutex_unlock(opal_thread_internal_mutex_t *p_mutex);
void opal_thread_internal_mutex_destroy(opal_thread_internal_mutex_t *p_mutex);

typedef lithe_condvar_t opal_thread_internal_cond_t;

#define OPAL_THREAD_INTERNAL_COND_INITIALIZER {0}

int opal_thread_internal_cond_init(opal_thread_internal_cond_t *p_cond);
void opal_thread_internal_cond_wait(opal_thread_internal_cond_t *p_cond, opal_thread_internal_mutex_t *p_mutex);
void opal_thread_internal_cond_broadcast(opal_thread_internal_cond_t *p_cond);
void opal_thread_internal_cond_signal(opal_thread_internal_cond_t *p_cond);
void opal_thread_internal_cond_destroy(opal_thread_internal_cond_t *p_cond);

END_C_DECLS

#endif /* OPAL_MCA_THREADS_LITHE_THREADS_LITHE_MUTEX_H */ 