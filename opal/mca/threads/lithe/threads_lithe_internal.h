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

BEGIN_C_DECLS

/* Thread module structure */
extern opal_threads_base_module_t opal_threads_lithe_module;

/* Basic lithe thread functions */
int opal_threads_lithe_init(void);
int opal_threads_lithe_finalize(void);

END_C_DECLS

#endif /* OPAL_MCA_THREADS_LITHE_INTERNAL_H */
