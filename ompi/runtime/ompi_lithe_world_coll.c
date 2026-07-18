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
#include <string.h>

#include "opal/class/opal_object.h"
#include "opal/mca/threads/mutex.h"
#include "opal/sys/atomic.h"
#include "opal/util/proc.h"

#include "ompi/communicator/communicator.h"
#include "ompi/datatype/ompi_datatype.h"
#include "ompi/op/op.h"
#include "ompi/proc/proc.h"
#include "ompi/runtime/ompi_lithe_world_coll.h"
#include "ompi/runtime/ompi_rte.h"

#if HAVE_LITHE
#include <parlib/arch.h> /* cpu_relax */
#endif

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

/*
 * Same-OS flat Barrier / Allreduce for hosted Lithe multicontext.
 *
 * RD Allreduce on P1K4 is 2 Sendrecv rounds (+ Barrier's 2) through MTL SC
 * match/wait (~41µs floor post-SC-spin). One sense-reversing arrival barrier
 * plus a local reduce replaces those match rounds when every peer is in this
 * OS process. Multi-OS (P2K*) keeps RD.
 *
 * LITHE_HOSTED_SC_FLAT_COLL=0 disables (fallback RD).
 */
#ifndef OMPI_LITHE_SC_COLL_MAX_RANKS
#define OMPI_LITHE_SC_COLL_MAX_RANKS 64
#endif
#ifndef OMPI_LITHE_SC_COLL_MAX_BYTES
#define OMPI_LITHE_SC_COLL_MAX_BYTES 4096
#endif

typedef struct {
    /* Monotonic arrival tickets (never reset). Cohort goal = ceil(ticket/size)*size.
     * Avoids sense-reversing capture-before-arrive races that dropped P1K4 sums. */
    opal_atomic_int32_t arrived;
    unsigned char *slots[OMPI_LITHE_SC_COLL_MAX_RANKS];
    size_t slot_bytes;
    int ready;
} ompi_lithe_sc_coll_state_t;

/* Static arena: avoids first-touch malloc races when K contexts enter together. */
static unsigned char sc_coll_arena[OMPI_LITHE_SC_COLL_MAX_RANKS * OMPI_LITHE_SC_COLL_MAX_BYTES];
static ompi_lithe_sc_coll_state_t sc_coll;

static bool sc_flat_coll_enabled(void)
{
    const char *e = getenv("LITHE_HOSTED_SC_FLAT_COLL");
    if (NULL != e && '0' == e[0] && '\0' == e[1]) {
        return false;
    }
    return true;
}

static bool sc_coll_single_os_intra(struct ompi_communicator_t *comm)
{
    int size, rank, r;
    unsigned long rph;
    opal_vpid_t me, my_os;
    ompi_proc_t *peer;

    if (!ompi_rte_lithe_hosted_multicontext_active || !OMPI_COMM_IS_INTRA(comm)) {
        return false;
    }
    if (!opal_lithe_env_cache_active()) {
        return false;
    }
    rph = opal_lithe_env_cache_rph();
    if (rph < 2UL) {
        return false;
    }
    size = ompi_comm_size(comm);
    if (size < 2 || size > OMPI_LITHE_SC_COLL_MAX_RANKS) {
        return false;
    }
    rank = ompi_comm_rank(comm);
    me = opal_proc_local_get()->proc_name.vpid;
    my_os = me / (opal_vpid_t) rph;
    for (r = 0; r < size; ++r) {
        if (r == rank) {
            continue;
        }
        peer = ompi_comm_peer_lookup(comm, r);
        if (NULL == peer) {
            return false;
        }
        if ((peer->super.proc_name.vpid / (opal_vpid_t) rph) != my_os) {
            return false;
        }
    }
    return true;
}

static int sc_coll_ensure_arena(size_t nbytes)
{
    int i;

    if (nbytes > OMPI_LITHE_SC_COLL_MAX_BYTES) {
        return OMPI_ERR_NOT_AVAILABLE;
    }
    if (sc_coll.ready) {
        return OMPI_SUCCESS;
    }
    sc_coll.slot_bytes = OMPI_LITHE_SC_COLL_MAX_BYTES;
    for (i = 0; i < OMPI_LITHE_SC_COLL_MAX_RANKS; ++i) {
        sc_coll.slots[i] = sc_coll_arena + (size_t) i * sc_coll.slot_bytes;
    }
    opal_atomic_swap_32(&sc_coll.arrived, 0);
    opal_atomic_wmb();
    sc_coll.ready = 1;
    return OMPI_SUCCESS;
}

static void sc_coll_ticket_barrier(int size)
{
    int32_t ticket, goal, cur;
    unsigned int spin = 0;

    ticket = opal_atomic_add_fetch_32(&sc_coll.arrived, 1);
    goal = ((ticket + (int32_t) size - 1) / (int32_t) size) * (int32_t) size;
    /* Must atomic-load each spin: a plain cast read can be cached in a
     * register and release early → peers reduce before all slots publish
     * (P1K4 rank0 sum=4, others sum=2/3). */
    do {
        cur = opal_atomic_add_fetch_32(&sc_coll.arrived, 0);
        if (cur >= goal) {
            break;
        }
        /*
         * Spin only (no yield). soft_cap≥RPH keeps K harts online for
         * single-OS worlds; yielding here reintroduces empty-steal noise and
         * rare pairwise→coll handoff hangs under HOSTED_FULLCORE.
         */
#if HAVE_LITHE
        cpu_relax();
        (void) spin;
#else
        (void) spin;
        break;
#endif
    } while (1);
    opal_atomic_rmb();
}

int ompi_lithe_hosted_sc_coll_barrier(MPI_Comm comm)
{
    int size;

    if (!sc_flat_coll_enabled() || !sc_coll_single_os_intra(comm)) {
        return OMPI_ERR_NOT_AVAILABLE;
    }
    size = ompi_comm_size(comm);
    if (OMPI_SUCCESS != sc_coll_ensure_arena(1)) {
        return OMPI_ERR_NOT_AVAILABLE;
    }
    sc_coll_ticket_barrier(size);
    return MPI_SUCCESS;
}

int ompi_lithe_hosted_sc_coll_allreduce(const void *sendbuf, void *recvbuf,
                                        int count, MPI_Datatype datatype,
                                        MPI_Op op, MPI_Comm comm)
{
    int size, rank, r, ret;
    ptrdiff_t span, gap = 0;
    const void *src;
    void *acc;

    if (!sc_flat_coll_enabled() || !sc_coll_single_os_intra(comm)) {
        return OMPI_ERR_NOT_AVAILABLE;
    }
    if (MPI_OP_NULL == op || NULL == datatype) {
        return OMPI_ERR_NOT_AVAILABLE;
    }
    size = ompi_comm_size(comm);
    rank = ompi_comm_rank(comm);
    span = opal_datatype_span(&datatype->super, count, &gap);
    if (span <= 0 || (size_t) span > OMPI_LITHE_SC_COLL_MAX_BYTES) {
        return OMPI_ERR_NOT_AVAILABLE;
    }
    ret = sc_coll_ensure_arena((size_t) span);
    if (OMPI_SUCCESS != ret) {
        return OMPI_ERR_NOT_AVAILABLE;
    }

    src = (MPI_IN_PLACE == sendbuf) ? recvbuf : sendbuf;
    (void) gap;
    ret = ompi_datatype_copy_content_same_ddt(datatype, count,
                                              (char *) sc_coll.slots[rank],
                                              (char *) src);
    if (ret < 0) {
        return OMPI_ERR_NOT_AVAILABLE;
    }
    opal_atomic_wmb();
    sc_coll_ticket_barrier(size);

    /* Local reduce of all published slots. */
    acc = recvbuf;
    ret = ompi_datatype_copy_content_same_ddt(datatype, count, (char *) acc,
                                              (char *) sc_coll.slots[0]);
    if (ret < 0) {
        return OMPI_ERROR;
    }
    for (r = 1; r < size; ++r) {
        ompi_op_reduce(op, sc_coll.slots[r], acc, count, datatype);
    }
    /* Epilogue: do not let a fast rank overwrite slots before peers finish. */
    sc_coll_ticket_barrier(size);
    return MPI_SUCCESS;
}
