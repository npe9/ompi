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
 * Copyright (c) 2010-2011 IBM Corporation.  All rights reserved.
 * Copyright (c) 2011-2013 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2014-2015 Intel, Inc. All rights reserved.
 * Copyright (c) 2014-2015 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2016      IBM Corporation.  All rights reserved.
 * Copyright (c) 2016-2017 Intel, Inc. All rights reserved.
 * Copyright (c) 2017      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2017-2018 IBM Corporation.  All rights reserved.
 * Copyright (c) 2018      Intel, Inc. All rights reserved.
 * Copyright (c) 2024      NVIDIA Corporation.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef OPAL_MCA_THREADS_LITHE_THREADS_H
#define OPAL_MCA_THREADS_LITHE_THREADS_H

#include "opal_config.h"

#include "opal/mca/threads/threads.h"
#include "opal/mca/threads/thread.h"

/* Forward declarations to avoid including lithe/parlib headers in the public interface */
typedef struct opal_threads_base_component_t opal_threads_base_component_t;
typedef struct opal_threads_base_module_t opal_threads_base_module_t;
typedef struct lithe_context lithe_context_t;

BEGIN_C_DECLS

OPAL_DECLSPEC extern const mca_base_component_t mca_threads_lithe_component;

struct mca_threads_lithe_module_t {
    opal_threads_base_module_t *super;
    lithe_context_t *context;
};

typedef struct mca_threads_lithe_module_t mca_threads_lithe_module_t;

END_C_DECLS

#endif /* OPAL_MCA_THREADS_LITHE_THREADS_H */ 