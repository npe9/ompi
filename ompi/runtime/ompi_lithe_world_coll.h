/*
 * Copyright (c) 2026      LLNL.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/**
 * @file
 *
 * Optional serialization of MPI_COMM_WORLD collective entry points when multiple
 * Lithe-backed logical ranks share one OS process (OMPI_LITHE_CONTEXT_LOCAL_PROC).
 * Currently: MPI_Barrier, MPI_Allreduce, MPI_Reduce (extend as hosted apps need).
 *
 * Also: same-OS flat Barrier/Allreduce for hosted Lithe (see
 * ompi_lithe_hosted_sc_coll_*); disable with LITHE_HOSTED_SC_FLAT_COLL=0.
 */

#ifndef OMPI_RUNTIME_OMPI_LITHE_WORLD_COLL_H
#define OMPI_RUNTIME_OMPI_LITHE_WORLD_COLL_H

#include "ompi_config.h"

#include "mpi.h"

BEGIN_C_DECLS

void ompi_lithe_world_coll_mpi_init_hook(void);
void ompi_lithe_world_coll_mpi_finalize_hook(void);
void ompi_lithe_world_coll_lock(MPI_Comm comm);
void ompi_lithe_world_coll_unlock(MPI_Comm comm);

/** Same-OS flat barrier; OMPI_ERR_NOT_AVAILABLE → use RD. */
int ompi_lithe_hosted_sc_coll_barrier(MPI_Comm comm);
/** Same-OS flat allreduce; OMPI_ERR_NOT_AVAILABLE → use RD. */
int ompi_lithe_hosted_sc_coll_allreduce(const void *sendbuf, void *recvbuf,
                                        int count, MPI_Datatype datatype,
                                        MPI_Op op, MPI_Comm comm);

END_C_DECLS

#endif /* OMPI_RUNTIME_OMPI_LITHE_WORLD_COLL_H */
