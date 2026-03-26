/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2021 The University of Tennessee and The University
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

#include "opal_config.h"
#include <unistd.h>
#include <pthread.h>

#include "opal/constants.h"
#include "opal/mca/threads/threads.h"
#include "opal/mca/threads/tsd.h"
#include "opal/prefetch.h"
#include "opal/util/output.h"
#include "opal/util/sys_limits.h"

#include MCA_threads_base_include_HEADER

/*
 * Constructor
 */
static void opal_thread_construct(opal_thread_t *t)
{
    t->t_run = 0;
    t->t_handle = (pthread_t) -1;
}

OBJ_CLASS_INSTANCE(opal_thread_t, opal_object_t, opal_thread_construct, NULL);

int opal_thread_start(opal_thread_t *t)
{
    if (OPAL_ENABLE_DEBUG) {
        if (NULL == t->t_run || (pthread_t) -1 != t->t_handle) {
            return OPAL_ERR_BAD_PARAM;
        }
    }

#ifdef HAVE_LITHE
    return opal_threads_base_module.thread_create(
        (opal_thread_fn_t)t->t_run, t, t, NULL);
#else
    int rc = pthread_create(&t->t_handle, NULL, (void *(*) (void *) ) t->t_run, t);
    return 0 == rc ? OPAL_SUCCESS : OPAL_ERR_IN_ERRNO;
#endif
}

int opal_thread_join(opal_thread_t *t, void **thr_return)
{
#ifdef HAVE_LITHE
    return opal_threads_base_module.thread_join(t, thr_return);
#else
    int rc = pthread_join(t->t_handle, thr_return);
    t->t_handle = (pthread_t) -1;
    return 0 == rc ? OPAL_SUCCESS : OPAL_ERR_IN_ERRNO;
#endif
}

bool opal_thread_self_compare(opal_thread_t *t)
{
    return pthread_self() == t->t_handle;
}

opal_thread_t *opal_thread_get_self(void)
{
    opal_thread_t *t = OBJ_NEW(opal_thread_t);
    t->t_handle = pthread_self();
    return t;
}

void opal_thread_set_main(void)
{
}
