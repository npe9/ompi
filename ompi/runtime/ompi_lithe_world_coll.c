/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2026      LLNL.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include <stdbool.h>
#include <stdlib.h>

#include "opal/class/opal_object.h"
#include "opal/mca/threads/mutex.h"

#include "ompi/communicator/communicator.h"
#include "ompi/runtime/ompi_lithe_world_coll.h"

static int world_coll_mutex_ready = 0;
static opal_recursive_mutex_t world_coll_mutex;

static bool lithe_world_coll_wanted(void)
{
    const char *lithe_ctx = getenv("OMPI_LITHE_CONTEXT_LOCAL_PROC");
    const char *serialize = getenv("OMPI_LITHE_WORLD_COLL_SERIALIZE");

    if (NULL == lithe_ctx || '\0' == lithe_ctx[0]) {
        return false;
    }
    if (NULL == serialize || '\0' == serialize[0]) {
        return false;
    }
    if ('0' == serialize[0] && '\0' == serialize[1]) {
        return false;
    }
    return true;
}

static bool comm_is_world(MPI_Comm comm)
{
    return ((void *)comm) == ((void *)&ompi_mpi_comm_world);
}

void ompi_lithe_world_coll_mpi_init_hook(void)
{
    if (!lithe_world_coll_wanted()) {
        return;
    }

    OBJ_CONSTRUCT(&world_coll_mutex, opal_recursive_mutex_t);
    world_coll_mutex_ready = 1;
}

void ompi_lithe_world_coll_mpi_finalize_hook(void)
{
    if (!world_coll_mutex_ready) {
        return;
    }

    OBJ_DESTRUCT(&world_coll_mutex);
    world_coll_mutex_ready = 0;
}

void ompi_lithe_world_coll_lock(MPI_Comm comm)
{
    if (!world_coll_mutex_ready || !comm_is_world(comm)) {
        return;
    }

    opal_mutex_lock(&world_coll_mutex);
}

void ompi_lithe_world_coll_unlock(MPI_Comm comm)
{
    if (!world_coll_mutex_ready || !comm_is_world(comm)) {
        return;
    }

    opal_mutex_unlock(&world_coll_mutex);
}
