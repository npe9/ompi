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

#ifndef OPAL_MCA_THREADS_LITHE_THREADS_LITHE_THREADS_H
#define OPAL_MCA_THREADS_LITHE_THREADS_LITHE_THREADS_H

#include "opal_config.h"
#include "opal/mca/threads/threads.h"
#include "opal/mca/threads/mutex.h"
#include "opal/mca/threads/tsd.h"

/* Lithe is cooperatively scheduled so yield when idle */
#define OPAL_THREAD_YIELD_WHEN_IDLE_DEFAULT true

BEGIN_C_DECLS

/**
 * Initialize the lithe threading component
 */
OPAL_DECLSPEC int opal_threads_lithe_init(void);

/**
 * Finalize the lithe threading component
 */
OPAL_DECLSPEC int opal_threads_lithe_finalize(void);

/* opal_thread_yield: defined in opal/mca/threads/base/threads_base_frame.c
 * which calls opal_threads_lithe_module.thread_yield() -> lithe_context_yield().
 * Do not define here: threads.h declares it as extern when OPAL_THREADS_LITHE. */

END_C_DECLS

#endif /* OPAL_MCA_THREADS_LITHE_THREADS_LITHE_THREADS_H */
