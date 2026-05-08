/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2014 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2020 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007-2015 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2019      Sandia National Laboratories.  All rights reserved.
 *
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"

#include "opal/version.h"
#include "opal/constants.h"
#include "opal/mca/base/base.h"
#include "threads_lithe.h"
#include "opal/mca/threads/thread.h"
#include "opal/mca/threads/threads.h"
#include "opal/util/show_help.h"

#include <lithe/fork_join_sched.h>
#include <stdlib.h>

/* Scheduler state - must match threads_lithe_module.c */
extern lithe_fork_join_sched_t *opal_sched;
extern bool opal_sched_entered;
extern volatile int opal_lithe_vcore_ready;

static int opal_threads_lithe_open(void);
static int opal_threads_lithe_register(void);

int opal_threads_lithe_register(void)
{
    return OPAL_SUCCESS;
}

static int lithe_component_debug(void) { return getenv("LITHE_DEBUG") != NULL; }

int opal_threads_lithe_open(void)
{
    if (lithe_component_debug())
        fprintf(stderr, "[LITHE-COMPONENT] opal_threads_lithe_open() called!\n");
    /* PMIx_Log_nb uses blocking PMIX_PTL_SEND_RECV to the Flux PMIx server; with Lithe
     * vcores that can stall multinode MPI_Init after topology (show_help may run there).
     * Direct stderr preserves help text without the server round-trip. Override:
     * OPAL_SHOW_HELP_FORCE_PMIX_LOG=1 */
    if (NULL == getenv("OPAL_SHOW_HELP_FORCE_PMIX_LOG")) {
        opal_show_help_set_use_pmix_log(0);
    }
    /* Deferred lithe constructor: uthread_lib_init was skipped at .so load.
     * threads_init() and OPAL mutex paths require a parlib uthread; migrate main
     * when OPAL adopts Lithe (including opal_wrapper / mpicxx). */
    lithe_ensure_main_on_vcore0();
    opal_threads_lithe_ensure_init();
    if (lithe_component_debug())
        fprintf(stderr, "[LITHE-COMPONENT] ensure_init() completed\n");
    
    /* Single path: create (if needed), register with PMIx, enter when parent sched exists. */
    opal_threads_lithe_ensure_opal_fork_join_sched();
    if (lithe_component_debug()) {
        fprintf(stderr, "[LITHE-COMPONENT] opal_sched=%p entered=%d\n", (void *)opal_sched,
                (int)opal_sched_entered);
    }
    /* Allow non-zero vcores to run uthreads (e.g. PMIx progress thread). Without this,
     * vcore 1+ spin in vcore_entry and the progress thread never runs -> PMIx init deadlock. */
    opal_lithe_vcore_ready = 1;

    if (lithe_component_debug())
        fprintf(stderr, "[LITHE-COMPONENT] About to return OPAL_SUCCESS from opal_threads_lithe_open()\n");
    return OPAL_SUCCESS;
}

const opal_threads_base_component_1_0_0_t mca_threads_lithe_component = {
    /* First, the mca_component_t struct containing meta information
     * about the component itself */
    .threadsc_version = {OPAL_THREADS_BASE_VERSION_1_0_0,

                         /* Component name and version */
                         .mca_component_name = "lithe",
                         MCA_BASE_MAKE_VERSION(component, OPAL_MAJOR_VERSION, OPAL_MINOR_VERSION,
                                               OPAL_RELEASE_VERSION),

                         .mca_open_component = opal_threads_lithe_open,
                         .mca_register_component_params = opal_threads_lithe_register},
    .threadsc_data =
        {/* The component is checkpoint ready */
         MCA_BASE_METADATA_PARAM_CHECKPOINT},
};
