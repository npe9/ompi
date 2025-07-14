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

#include "opal_config.h"

#include "opal/mca/threads/lithe/lithe_threads.h"
#include "opal/mca/threads/threads.h"
#include "opal/mca/threads/base/base.h"
#include "opal/util/output.h"

#include <lithe/lithe.h>
#include <lithe/mutex.h>
#include <lithe/condvar.h>

/*
 * Local functions
 */
static int lithe_threads_init(void);
static int lithe_threads_finalize(void);
static int lithe_threads_yield(void);
static int lithe_threads_join(opal_thread_t *thread, void **exit_status);
static int lithe_threads_create(opal_thread_t *thread, opal_thread_fn_t func, void *arg);
static int lithe_threads_set_affinity(opal_thread_t *thread, int cpu);

/*
 * Component structure
 */
const mca_base_component_t mca_threads_lithe_component = {
    .mca_component_name = "lithe",
    MCA_BASE_MAKE_VERSION(component, OPAL_MAJOR_VERSION, OPAL_MINOR_VERSION,
                          OPAL_RELEASE_VERSION),
    .mca_open_component = NULL,
    .mca_close_component = NULL,
    .mca_register_component_params = NULL,
};

/*
 * Module structure
 */
mca_threads_lithe_module_t mca_threads_lithe_module = {
    .super = NULL,
    .context = NULL
};

static int lithe_threads_init(void)
{
    /* lithe_lib_init is called automatically via constructor attribute */
    return OPAL_SUCCESS;
}

static int lithe_threads_finalize(void)
{
    if (mca_threads_lithe_module.context != NULL) {
        lithe_context_cleanup(mca_threads_lithe_module.context);
        mca_threads_lithe_module.context = NULL;
    }

    return OPAL_SUCCESS;
}

static int lithe_threads_yield(void)
{
    lithe_context_yield();
    return OPAL_SUCCESS;
}

static int lithe_threads_join(opal_thread_t *thread, void **exit_status)
{
    /* lithe doesn't support thread joining */
    return OPAL_ERR_NOT_SUPPORTED;
}

static int lithe_threads_create(opal_thread_t *thread, opal_thread_fn_t func, void *arg)
{
    /* Initialize the context */
    lithe_context_init(mca_threads_lithe_module.context, (void (*)(void *))func, arg);
    return OPAL_SUCCESS;
}

static int lithe_threads_set_affinity(opal_thread_t *thread, int cpu)
{
    /* lithe doesn't support setting thread affinity */
    return OPAL_ERR_NOT_SUPPORTED;
} 