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
#include "opal/mca/threads/base/threads_base_frame.h"
#include "opal/util/output.h"

#include <lithe.h>

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
opal_threads_base_component_t mca_threads_lithe_component = {
    .threads_version = {
        OPAL_THREADS_BASE_VERSION_2_0_0,
        .mca_component_name = "lithe",
        MCA_BASE_MAKE_VERSION(component, OPAL_MAJOR_VERSION, OPAL_MINOR_VERSION,
                              OPAL_RELEASE_VERSION),
        .mca_open_component = NULL,
        .mca_close_component = NULL,
        .mca_register_component_params = NULL,
    },
    .threads_data = {
        .parameter_name = "lithe",
        .priority = 10,
        .want_default = true,
    },
};

/*
 * Module structure
 */
opal_threads_lithe_module_t opal_threads_lithe_module = {
    .super = {
        .threads_init = lithe_threads_init,
        .threads_finalize = lithe_threads_finalize,
        .threads_yield = lithe_threads_yield,
        .threads_join = lithe_threads_join,
        .threads_create = lithe_threads_create,
        .threads_set_affinity = lithe_threads_set_affinity,
    },
    .context = NULL
};

static int lithe_threads_init(void)
{
    int ret;

    ret = lithe_init();
    if (OPAL_UNLIKELY(ret != 0)) {
        opal_output(0, "lithe_threads_init: lithe_init() failed with error %d\n", ret);
        return OPAL_ERROR;
    }

    return OPAL_SUCCESS;
}

static int lithe_threads_finalize(void)
{
    if (opal_threads_lithe_module.context != NULL) {
        lithe_context_destroy(opal_threads_lithe_module.context);
        opal_threads_lithe_module.context = NULL;
    }

    return OPAL_SUCCESS;
}

static int lithe_threads_yield(void)
{
    lithe_yield();
    return OPAL_SUCCESS;
}

static int lithe_threads_join(opal_thread_t *thread, void **exit_status)
{
    /* lithe doesn't support thread joining */
    return OPAL_ERR_NOT_SUPPORTED;
}

static int lithe_threads_create(opal_thread_t *thread, opal_thread_fn_t func, void *arg)
{
    int ret;

    ret = lithe_context_create(&opal_threads_lithe_module.context, func, arg);
    if (OPAL_UNLIKELY(ret != 0)) {
        opal_output(0, "lithe_threads_create: lithe_context_create() failed with error %d\n", ret);
        return OPAL_ERROR;
    }

    return OPAL_SUCCESS;
}

static int lithe_threads_set_affinity(opal_thread_t *thread, int cpu)
{
    /* lithe doesn't support setting thread affinity */
    return OPAL_ERR_NOT_SUPPORTED;
} 