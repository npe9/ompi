/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2016 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2013      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2014      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2017      IBM Corporation. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */


#include "ompi_config.h"
#include <stdio.h>
#include <stdlib.h>

#include "ompi/constants.h"
#include "ompi/mca/mca.h"
#include "opal/util/output.h"
#include "opal/mca/base/base.h"


#include "ompi/mca/coll/coll.h"
#include "ompi/mca/coll/base/base.h"
#include "ompi/mca/coll/base/coll_base_functions.h"
#include "ompi/proc/proc.h"
#include "ompi/communicator/communicator.h"
#include "opal/sys/atomic.h"
#include "ompi/mca/coll/base/coll_tags.h"
#include <string.h>

#if defined(__has_include)
#    if __has_include(<parlib/dtls.h>)
#        include <parlib/dtls.h>
#        define OMPI_COLL_HAVE_PARLIB_DTLS 1
#    endif
#endif

/*
 * The following file was created by configure.  It contains extern
 * statements and the definition of an array of pointers to each
 * component's public mca_base_component_t struct.
 */
#include "ompi/mca/coll/base/static-components.h"

#if OMPI_COLL_HAVE_PARLIB_DTLS
/*
 * Hosted Lithe ranks share one communicator / coll module but run concurrent
 * collectives. The shared mcct_reqs pool races (wrong Allreduce sums). Keep a
 * private req array per Lithe context (parlib DTLS).
 */
typedef struct {
    ompi_request_t **reqs;
    int num_reqs;
    /* Per-context topology caches (GAPS B21): the cached trees describe the
     * CALLING rank's parent/children, so a tree built by one hosted rank and
     * reused by another sends to the wrong peers (MPI_Reduce wrong or hung
     * for every tree algorithm; only linear, which caches nothing, worked).
     * Keyed by the shared coll data and the communicator. */
    struct {
        mca_coll_base_comm_t *key;
        struct ompi_communicator_t *comm;
        mca_coll_base_comm_t *shadow;
    } topo[8];
    int ntopo;
    /* Per-context nonblocking-collective tag counters, indexed by local cid
     * (GAPS B33): see ompi_coll_base_lithe_nbc_tag_slot. 0 = not yet used. */
    int32_t *nbc_tag;
    uint32_t nbc_tag_n;
} ompi_coll_lithe_reqs_t;

static dtls_key_t ompi_coll_lithe_reqs_dtls_key;
static volatile int ompi_coll_lithe_reqs_dtls_ready = 0;

static void ompi_coll_lithe_reqs_dtor(void *value)
{
    ompi_coll_lithe_reqs_t *slot = (ompi_coll_lithe_reqs_t *) value;
    if (NULL == slot) {
        return;
    }
    free(slot->reqs);
    free(slot->nbc_tag);
    free(slot);
}

/* Once-only (2026-10-21, GAPS B33). This was check-then-create: two hosted ranks
 * reaching their first collective together each created a key, the second store
 * replaced the first, and a rank that had already set its slot under the first
 * key found NULL under the second -> a fresh slot. Its per-context nonblocking
 * tag counter restarted (its 2nd MPI_Iallreduce reused tag -33 while the peers
 * sent -34: hung ~4 % of runs, 4 ranks / 3 harts; an SC match trace showed the
 * repeated round-0 post), and the B21 topology caches could be lost the same way. */
static void ompi_coll_lithe_reqs_dtls_init(void)
{
    static opal_atomic_int32_t state = 0; /* 0 none, 1 creating */
    int32_t expect = 0;
    dtls_key_t key;

    if (ompi_coll_lithe_reqs_dtls_ready) {
        return;
    }
    if (!opal_atomic_compare_exchange_strong_32(&state, &expect, 1)) {
        while (0 == ompi_coll_lithe_reqs_dtls_ready) {
            opal_atomic_rmb();
        }
        return;
    }
    key = dtls_key_create(ompi_coll_lithe_reqs_dtor);
    ompi_coll_lithe_reqs_dtls_key = key;
    opal_atomic_wmb();
    ompi_coll_lithe_reqs_dtls_ready = (NULL == key) ? -1 : 1;
}
#endif /* OMPI_COLL_HAVE_PARLIB_DTLS */

/*
 * Ensure all function pointers are NULL'ed out to start with
 */
static void coll_base_module_construct(mca_coll_base_module_t *m)
{
    /* zero out all functions */
    memset ((char *) m + sizeof (m->super), 0, sizeof (*m) - sizeof (m->super));
    m->coll_module_disable = NULL;
    m->base_data = NULL;
}

static void
coll_base_module_destruct(mca_coll_base_module_t *module)
{
    if (NULL != module->base_data) {
        OBJ_RELEASE(module->base_data);
    }
}

OBJ_CLASS_INSTANCE(mca_coll_base_module_t, opal_object_t,
                   coll_base_module_construct, coll_base_module_destruct);


static void
coll_base_comm_construct(mca_coll_base_comm_t *data)
{
    memset ((char *) data + sizeof (data->super), 0, sizeof (*data) - sizeof (data->super));
}

static void
coll_base_comm_destruct(mca_coll_base_comm_t *data)
{
    if( NULL != data->mcct_reqs ) {
        ompi_coll_base_free_reqs( data->mcct_reqs, data->mcct_num_reqs );
        free(data->mcct_reqs);
        data->mcct_reqs = NULL;
        data->mcct_num_reqs = 0;
    }
    assert(0 == data->mcct_num_reqs);

    /* free any cached information that has been allocated */
    if (data->cached_ntree) { /* destroy general tree if defined */
        ompi_coll_base_topo_destroy_tree (&data->cached_ntree);
    }
    if (data->cached_bintree) { /* destroy bintree if defined */
        ompi_coll_base_topo_destroy_tree (&data->cached_bintree);
    }
    if (data->cached_bmtree) { /* destroy bmtree if defined */
        ompi_coll_base_topo_destroy_tree (&data->cached_bmtree);
    }
    if (data->cached_in_order_bmtree) { /* destroy bmtree if defined */
        ompi_coll_base_topo_destroy_tree (&data->cached_in_order_bmtree);
    }
    if (data->cached_kmtree) { /* destroy kmtree if defined */
        ompi_coll_base_topo_destroy_tree (&data->cached_kmtree);
    }
    if (data->cached_chain) { /* destroy general chain if defined */
        ompi_coll_base_topo_destroy_tree (&data->cached_chain);
    }
    if (data->cached_pipeline) { /* destroy pipeline if defined */
        ompi_coll_base_topo_destroy_tree (&data->cached_pipeline);
    }
    if (data->cached_in_order_bintree) { /* destroy in order bintree if defined */
        ompi_coll_base_topo_destroy_tree (&data->cached_in_order_bintree);
    }
}

OBJ_CLASS_INSTANCE(mca_coll_base_comm_t, opal_object_t,
                   coll_base_comm_construct, coll_base_comm_destruct);

#if OMPI_COLL_HAVE_PARLIB_DTLS
static ompi_coll_lithe_reqs_t *ompi_coll_lithe_slot(void)
{
    ompi_coll_lithe_reqs_t *slot;

    ompi_coll_lithe_reqs_dtls_init();
    if (ompi_coll_lithe_reqs_dtls_ready <= 0) {
        return NULL;
    }
    slot = (ompi_coll_lithe_reqs_t *) get_dtls(ompi_coll_lithe_reqs_dtls_key);
    if (NULL == slot) {
        slot = (ompi_coll_lithe_reqs_t *) calloc(1, sizeof(*slot));
        if (NULL == slot) {
            return NULL;
        }
        set_dtls(ompi_coll_lithe_reqs_dtls_key, slot);
    }
    return slot;
}
#endif

/* GAPS B33 (2026-10-21): comm->c_nbc_tag is per communicator, and hosted Lithe
 * ranks share one communicator struct, so every co-resident rank's
 * MPI_Iallreduce/MPI_Ibarrier/... took the NEXT tag from one counter: 4 ranks got
 * 4 different tags and nothing matched (hung at the first call, 3/3). MPI orders
 * collectives per communicator, so a counter per rank per communicator stays in
 * step across ranks. Returns NULL (use the shared counter) when one rank owns the
 * process or there is no Lithe context. */
int32_t *ompi_coll_base_lithe_nbc_tag_slot(struct ompi_communicator_t *comm)
{
#if OMPI_COLL_HAVE_PARLIB_DTLS
    ompi_coll_lithe_reqs_t *slot;
    uint32_t cid;

    if (!ompi_rte_lithe_hosted_multicontext_active) {
        return NULL;
    }
    slot = ompi_coll_lithe_slot();
    if (NULL == slot) {
        return NULL;
    }
    cid = ompi_comm_get_local_cid(comm);
    if (cid >= slot->nbc_tag_n) {
        uint32_t n = slot->nbc_tag_n ? slot->nbc_tag_n : 16;
        int32_t *t;
        while (n <= cid) {
            n *= 2;
        }
        t = (int32_t *) realloc(slot->nbc_tag, n * sizeof(*t));
        if (NULL == t) {
            return NULL;
        }
        memset(t + slot->nbc_tag_n, 0, (n - slot->nbc_tag_n) * sizeof(*t));
        slot->nbc_tag = t;
        slot->nbc_tag_n = n;
    }
    if (0 == slot->nbc_tag[cid]) {
        slot->nbc_tag[cid] = MCA_COLL_BASE_TAG_NONBLOCKING_BASE;
    }
    return &slot->nbc_tag[cid];
#else
    (void) comm;
    return NULL;
#endif
}

/* The coll data whose cached_* topology fields the calling rank may use.
 * Normally the module's own; for hosted Lithe ranks sharing one communicator,
 * a private per-context copy that holds only the topology caches (its request
 * pool is never used: ompi_coll_base_comm_get_reqs is per-context already). */
mca_coll_base_comm_t *ompi_coll_base_comm_topo(struct ompi_communicator_t *comm,
                                               mca_coll_base_module_t *module)
{
    mca_coll_base_comm_t *data = module->base_data;
#if OMPI_COLL_HAVE_PARLIB_DTLS
    /* OMPI_LITHE_COLL_TOPO_PRIVATE=0 restores the shared cache (ablation). */
    static int topo_private = -1;
    if (topo_private < 0) {
        const char *e = getenv("OMPI_LITHE_COLL_TOPO_PRIVATE");
        topo_private = (NULL == e || '0' != *e);
    }
    if (ompi_rte_lithe_hosted_multicontext_active && topo_private) {
        ompi_coll_lithe_reqs_t *slot = ompi_coll_lithe_slot();
        if (NULL != slot) {
            int i, victim;
            for (i = 0; i < slot->ntopo; i++) {
                if (slot->topo[i].key == data && slot->topo[i].comm == comm) {
                    return slot->topo[i].shadow;
                }
            }
            victim = slot->ntopo < 8 ? slot->ntopo++ : 7;
            if (NULL != slot->topo[victim].shadow) {
                /* evict: its trees go, the coll data itself is not ours */
                coll_base_comm_destruct(slot->topo[victim].shadow);
                free(slot->topo[victim].shadow);
            }
            slot->topo[victim].shadow =
                (mca_coll_base_comm_t *) calloc(1, sizeof(mca_coll_base_comm_t));
            if (NULL == slot->topo[victim].shadow) {
                slot->ntopo = victim;
                return data;
            }
            slot->topo[victim].key = data;
            slot->topo[victim].comm = comm;
            return slot->topo[victim].shadow;
        }
    }
#endif
    (void) comm;
    return data;
}

ompi_request_t** ompi_coll_base_comm_get_reqs(mca_coll_base_comm_t* data, int nreqs)
{
    if( 0 == nreqs ) return NULL;

#if OMPI_COLL_HAVE_PARLIB_DTLS
    if (ompi_rte_lithe_hosted_multicontext_active) {
        ompi_coll_lithe_reqs_t *slot = ompi_coll_lithe_slot();

        if (NULL != slot) {
            if (slot->num_reqs < nreqs) {
                ompi_request_t **nr =
                    (ompi_request_t **) realloc(slot->reqs,
                                                sizeof(ompi_request_t *) * (size_t) nreqs);
                if (NULL == nr) {
                    return NULL;
                }
                for (int i = slot->num_reqs; i < nreqs; i++) {
                    nr[i] = MPI_REQUEST_NULL;
                }
                slot->reqs = nr;
                slot->num_reqs = nreqs;
            }
            return slot->reqs;
        }
    }
#endif /* OMPI_COLL_HAVE_PARLIB_DTLS */

    if( data->mcct_num_reqs < nreqs ) {
        data->mcct_reqs = (ompi_request_t**)realloc(data->mcct_reqs, sizeof(ompi_request_t*) * nreqs);

        if( NULL != data->mcct_reqs ) {
            for( int i = data->mcct_num_reqs; i < nreqs; i++ )
                data->mcct_reqs[i] = MPI_REQUEST_NULL;
            data->mcct_num_reqs = nreqs;
        } else
            data->mcct_num_reqs = 0;  /* nothing to return */
    }
    return data->mcct_reqs;
}

MCA_BASE_FRAMEWORK_DECLARE(ompi, coll, "Collectives", NULL, NULL, NULL,
                           mca_coll_base_static_components, 0);
