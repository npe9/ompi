/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2016 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2006 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2017      Intel, Inc. All rights reserved
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef PML_CM_REQUEST_H
#define PML_CM_REQUEST_H

#include "ompi/mca/pml/base/pml_base_sendreq.h"
#include "ompi/mca/pml/base/pml_base_bsend.h"
#include "ompi/mca/pml/pml.h"
#include "ompi/mca/mtl/mtl.h"
#include "ompi/communicator/communicator.h"
#include "opal/class/opal_free_list.h"

/*
 * LITHE_PML_CM_FAST (2026-10-21, bitmask read once at component open, default 3,
 * =0 restores; both bits are safe in any process: intrinsic communicators outlive
 * every request, and the stash is per thread / per Lithe context). Co-resident ranks share every per-process object, so per-request
 * atomics that are private in a process-per-rank run bounce one cache line between
 * harts here:
 *  bit 1: skip the request's refcount on intrinsic communicators (world/self/null,
 *         freed only at finalize). Every Isend/Irecv retained and released
 *         MPI_COMM_WORLD's refcount (predefined datatypes already skip theirs).
 *  bit 2: a per-context (__thread is per Lithe context) stash of free requests in front
 *         of the shared send/recv free lists, whose 128-bit CAS head all ranks contend.
 * A/B (5 reps, idle cpus): halo 4 KiB 4 ranks / 4 harts 2.15-2.45 -> 1.56-1.66 us (bit 1
 * alone 2.0, bit 2 alone 1.7), 3 harts 4.7-4.9 -> 4.5-4.7; suite (4 reps): 8 contexts /
 * 3 harts 6.2-6.5 -> 5.6-5.7, ranks K=8 9.04 -> 8.53 ms/iter mean, the rest unchanged.
 */
OMPI_DECLSPEC extern int mca_pml_cm_lithe_fast;
#define PML_CM_COMM_RETAIN(c)                                              \
    do {                                                                   \
        if (!((mca_pml_cm_lithe_fast & 1) && OMPI_COMM_IS_INTRINSIC(c))) { \
            OBJ_RETAIN(c);                                                 \
        }                                                                  \
    } while (0)
#define PML_CM_COMM_RELEASE(c)                                             \
    do {                                                                   \
        if (!((mca_pml_cm_lithe_fast & 1) && OMPI_COMM_IS_INTRINSIC(c))) { \
            OBJ_RELEASE(c);                                                \
        }                                                                  \
    } while (0)

#define PML_CM_STASH_N 16
typedef struct {
    int n;
    opal_free_list_item_t *it[PML_CM_STASH_N];
} mca_pml_cm_stash_t;
OMPI_DECLSPEC extern __thread mca_pml_cm_stash_t mca_pml_cm_stash_send, mca_pml_cm_stash_recv;

static inline opal_free_list_item_t *
mca_pml_cm_fl_get(opal_free_list_t *fl, mca_pml_cm_stash_t *st, int wait)
{
    if ((mca_pml_cm_lithe_fast & 2) && st->n > 0) {
        return st->it[--st->n];
    }
    return wait ? opal_free_list_wait(fl) : opal_free_list_get(fl);
}

static inline void
mca_pml_cm_fl_return(opal_free_list_t *fl, mca_pml_cm_stash_t *st, opal_free_list_item_t *it)
{
    if ((mca_pml_cm_lithe_fast & 2) && st->n < PML_CM_STASH_N) {
        st->it[st->n++] = it;
        return;
    }
    opal_free_list_return(fl, it);
}

/**
 * Type of request.
 */
typedef enum {
    MCA_PML_CM_REQUEST_SEND_HEAVY,
    MCA_PML_CM_REQUEST_SEND_THIN,
    MCA_PML_CM_REQUEST_RECV_HEAVY,
    MCA_PML_CM_REQUEST_RECV_THIN,
    MCA_PML_CM_REQUEST_NULL
} mca_pml_cm_request_type_t;

/**
 *  Base type for PML CM P2P requests
 */
struct mca_pml_cm_request_t {

/* START: These fields have to match the definition of the mca_pml_base_request_t */
    ompi_request_t req_ompi;              /**< base request */
    volatile int32_t req_pml_complete;    /**< flag indicating if the pt-2-pt layer is done with this request */
    volatile int32_t req_free_called;     /**< flag indicating if the user has freed this request */
    mca_pml_cm_request_type_t req_pml_type;
    struct ompi_communicator_t *req_comm; /**< communicator pointer */
    struct ompi_datatype_t *req_datatype; /**< pointer to data type */
    opal_convertor_t req_convertor;       /**< convertor that describes the memory layout */
/* END: These fields have to match the definition of the mca_pml_base_request_t */
};
typedef struct mca_pml_cm_request_t mca_pml_cm_request_t;
OBJ_CLASS_DECLARATION(mca_pml_cm_request_t);

/*
 * Avoid accelerator convertor inits only for contiguous memory and if indicated by
 * the MTL. For non-contiguous memory, do not skip accelerator convertor init phases.
 */
#define MCA_PML_CM_SWITCH_ACCELERATOR_CONVERTOR_OFF(flags, datatype, count)            \
    {                                                                           \
        if (opal_datatype_is_contiguous_memory_layout(&datatype->super, count)  \
            && (ompi_mtl->mtl_flags & MCA_MTL_BASE_FLAG_ACCELERATOR_INIT_DISABLE)) {   \
            flags |= CONVERTOR_SKIP_ACCELERATOR_INIT;                                  \
        }                                                                       \
    }

#endif
