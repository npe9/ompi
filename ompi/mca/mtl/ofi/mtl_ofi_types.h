/*
 * Copyright (c) 2013-2018 Intel, Inc. All rights reserved
 *
 * Copyright (c) 2015 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2022-2023 Triad National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef MTL_OFI_TYPES_H_HAS_BEEN_INCLUDED
#define MTL_OFI_TYPES_H_HAS_BEEN_INCLUDED

#include <stdint.h>

#include "mtl_ofi.h"

BEGIN_C_DECLS

/**
 * MTL Module Interface
 */

typedef struct mca_mtl_ofi_context_t {
    /* Transmit and receive contexts */
    struct fid_ep *tx_ep;
    struct fid_ep *rx_ep;

    /* Completion queue */
    struct fid_cq *cq;
    int cq_wait_fd;

    /* Thread locking */
    opal_mutex_t context_lock;
} mca_mtl_ofi_context_t;

typedef struct mca_mtl_ofi_module_t {
    mca_mtl_base_module_t base;

    /** Fabric Domain handle */
    struct fid_fabric *fabric;

    /** Access Domain handle */
    struct fid_domain *domain;

    /** Address vector handle */
    struct fid_av *av;

    /* Multi-threaded Application flag */
    bool mpi_thread_multiple;

    /* Scalable Endpoint attributes */
    struct fid_ep *sep;                 /* Endpoint object */
    mca_mtl_ofi_context_t *ofi_ctxt;    /* OFI contexts */
    int threshold_comm_context_id;      /* Set threshold communicator ID */
    int *comm_to_context;               /* Map communicator ID to context */
    int rx_ctx_bits;                    /* Bits used for RX context */
    int total_ctxts_used;               /* Total number of contexts used */
    int enable_sep;                     /* MCA to enable/disable SEP feature */
    int thread_grouping;                /* MCA for thread grouping feature */
    int num_ofi_contexts;               /* MCA for number of contexts to use */
    bool disable_hmem;                   /* MCA to enable/disable request for FI_HMEM support from provider */

    /** Endpoint name length */
    size_t epnamelen;

    /** "Any source" address */
    fi_addr_t any_addr;

    /** OFI provider name */
    char *provider_name;

    /** Maximum inject size */
    size_t max_inject_size;

    /** Largest message that can be sent in a single send. */
    size_t max_msg_size;

    /** Maximum number of CQ events to read in OFI Progress */
    int ofi_progress_event_count;

    /** Use FI_REMOTE_CQ_DATA*/
    bool fi_cq_data;

    /** Info used to create the OFI tag **/
    unsigned long long source_rank_tag_mask;
    int num_bits_source_rank;
    unsigned long long source_rank_mask;
    unsigned long long mpi_tag_mask;
    int num_bits_mpi_tag;
    int num_peers;

    /** Synchronous protocol tag bits */
    unsigned long long sync_send;
    unsigned long long sync_send_ack;
    unsigned long long sync_proto_mask;

    /** Optimized function Symbol Tables **/
    struct ompi_mtl_ofi_symtable sym_table;

    bool is_initialized;
    bool progress_block_enabled;
    bool has_posted_initial_buffer;
    bool hmem_needs_reg;

#if HAVE_LITHE
    /* Hosted RPH>=2 on providers without SEP (e.g. cxi max_ep_*_ctx=1):
     * one regular EP+CQ per logical slot instead of a single shared EP. */
    int hosted_multi_ep;
    /*
     * Hosted RPH>=2 + no SEP + FI_REMOTE_CQ_DATA: embed *dest and source*
     * co-resident slots (rank % RPH) in the CQD match tag.
     *
     * One shared EP: all co-resident ranks share one fi_addr, so
     * FI_DIRECTED_RECV cannot isolate sources. Dest-slot alone demuxes
     * concurrent receivers, but recursive-doubling Allreduce reuses one
     * MPI tag across rounds — an early round-N send can match a still-posted
     * round-(N-1) recv on the same dest slot (FI_ADDR_UNSPEC). Packing both
     * dest and source slots prevents that. Recvs use FI_ADDR_UNSPEC;
     * MPI_SOURCE still comes from FI_REMOTE_CQ_DATA.
     *
     * Bits are placed only where provider mem_tag_format is 1 (usable).
     */
    int hosted_dst_in_cqd_tag;
    int hosted_cqd_src_bits;   /* ceil(log2(RPH)) bits each for dest and src */
    int hosted_cqd_cid_bits;   /* remaining usable upper bits for cid */
    /* Bit positions: [0..src_bits) dest, [src_bits..2*src_bits) source, then cid. */
    uint8_t hosted_cqd_slot_bitpos[16];
    uint8_t hosted_cqd_cid_bitpos[24];
#endif

} mca_mtl_ofi_module_t;

extern mca_mtl_ofi_module_t ompi_mtl_ofi;

typedef struct mca_mtl_ofi_component_t {
    /** Base MTL component */
    mca_mtl_base_component_2_0_0_t super;
} mca_mtl_ofi_component_t;

typedef enum {
    OFI_REGULAR_EP  = 0,
    OFI_SCALABLE_EP,
} mca_mtl_ofi_ep_type;

/*
 * Define upper limit for number of events read from a CQ.
 * Setting this to 100 as this was deemed optimal from empirical data.
 * If one wants to read lesser number of events from the CQ, the MCA
 * variable can be used.
 */
#define MTL_OFI_MAX_PROG_EVENT_COUNT    100

/*OFI TAG:
 * Define 3 different OFI tag distributions:
 * 1) Support FI_REMOTE_CQ_DATA: No need for source rank in the tag
 * 2) ofi_tag_1: fallback when no FI_REMOTE_CQ_DATA is supported
 * 3) ofi_tag_2: Alternative tag when no FI_REMOTE_CQ_DATA is supported
 *    with more bits for the communicator ID.
 * More details of the tags are in the README file (mtl_ofi_tag_mode).
*/

#define MTL_OFI_MINIMUM_CID_BITS        (8)

/* Support FI_REMOTE_CQ_DATA, send the source rank in the CQ data (4 Bytes is the minimum)
 *  01234567 01234567 01234567 012345  67  01234567 01234567 01234567 01234567
 *                                   |    |
 *           context_id              |prot|          message tag
 */
#define MTL_OFI_PROTO_BIT_COUNT         (2)
#define MTL_OFI_HIGHEST_TAG_BIT         (0x8000000000000000ULL)

#define MTL_OFI_CID_MASK_DATA           (0xFFFFFFFC00000000ULL)
#define MTL_OFI_CID_BIT_COUNT_DATA      (30)
#define MTL_OFI_TAG_MASK_DATA           (0x00000000FFFFFFFFULL)
#define MTL_OFI_TAG_BIT_COUNT_DATA      (32)
#define MTL_OFI_PROTO_MASK_DATA         (0x0000000300000000ULL)
#define MTL_OFI_SYNC_SEND_DATA          (0x0000000100000000ULL)
#define MTL_OFI_SYNC_SEND_ACK_DATA      (0x0000000200000000ULL)

/* Bits below this are MPI tag (32) + proto (2) in the FULL/CQD layout. */
#define MTL_OFI_HOSTED_CQD_PROTO_TAG_SHIFT \
    (MTL_OFI_TAG_BIT_COUNT_DATA + MTL_OFI_PROTO_BIT_COUNT)

#if HAVE_LITHE
/* Co-resident slot for hosted no-SEP CQD match isolation (rank % RPH). */
__opal_attribute_always_inline__ static inline uint64_t
mtl_ofi_hosted_cqd_slot(int rank)
{
    unsigned long rph = opal_lithe_env_cache_rph();
    if (rph < 2UL || rank < 0) {
        return (uint64_t) (unsigned) rank;
    }
    return (uint64_t) ((unsigned) rank % (unsigned) rph);
}

/* Pack cid + dest_slot + src_slot into provider-usable bit positions. */
__opal_attribute_always_inline__ static inline uint64_t
mtl_ofi_hosted_cqd_pack_upper(uint64_t cid, uint64_t dest_slot,
                              uint64_t src_slot)
{
    uint64_t bits = 0;
    int i;
    int sb = ompi_mtl_ofi.hosted_cqd_src_bits;
    int cid_bits = ompi_mtl_ofi.hosted_cqd_cid_bits;

    for (i = 0; i < sb; ++i) {
        if (dest_slot & (1ULL << i)) {
            bits |= (1ULL << ompi_mtl_ofi.hosted_cqd_slot_bitpos[i]);
        }
    }
    for (i = 0; i < sb; ++i) {
        if (src_slot & (1ULL << i)) {
            bits |= (1ULL << ompi_mtl_ofi.hosted_cqd_slot_bitpos[sb + i]);
        }
    }
    for (i = 0; i < cid_bits; ++i) {
        if (cid & (1ULL << i)) {
            bits |= (1ULL << ompi_mtl_ofi.hosted_cqd_cid_bitpos[i]);
        }
    }
    return bits;
}

__opal_attribute_always_inline__ static inline uint64_t
mtl_ofi_hosted_cqd_src_slot_mask_bits(void)
{
    uint64_t mask = 0;
    int i;
    int sb = ompi_mtl_ofi.hosted_cqd_src_bits;
    for (i = 0; i < sb; ++i) {
        mask |= (1ULL << ompi_mtl_ofi.hosted_cqd_slot_bitpos[sb + i]);
    }
    return mask;
}
#endif

/* Send tag with CQ_DATA.
 * Hosted no-SEP: embed dest slot and local (source) slot. */
__opal_attribute_always_inline__ static inline uint64_t
mtl_ofi_create_send_tag_CQD(int comm_id, int tag, int dest)
{
    uint64_t  match_bits = (uint64_t) comm_id;
#if HAVE_LITHE
    if (ompi_mtl_ofi.hosted_dst_in_cqd_tag &&
        ompi_mtl_ofi.hosted_cqd_src_bits > 0) {
        int cid_bits = ompi_mtl_ofi.hosted_cqd_cid_bits;
        int sb = ompi_mtl_ofi.hosted_cqd_src_bits;
        uint64_t cid_mask = (cid_bits >= 63) ? ~0ULL : ((1ULL << cid_bits) - 1ULL);
        uint64_t slot_mask = (sb >= 63) ? ~0ULL : ((1ULL << sb) - 1ULL);
        opal_vpid_t self_v = opal_proc_local_get()->proc_name.vpid;
        uint64_t dslot = mtl_ofi_hosted_cqd_slot(dest) & slot_mask;
        uint64_t sslot = mtl_ofi_hosted_cqd_slot((int) self_v) & slot_mask;
        uint64_t cid = (uint64_t) comm_id & cid_mask;
        match_bits = mtl_ofi_hosted_cqd_pack_upper(cid, dslot, sslot);
        match_bits |= (tag & MTL_OFI_TAG_MASK_DATA);
        return match_bits;
    }
#else
    (void) dest;
#endif
    match_bits = (match_bits << (MTL_OFI_TAG_BIT_COUNT_DATA
                                + MTL_OFI_PROTO_BIT_COUNT));
    match_bits |= (tag & MTL_OFI_TAG_MASK_DATA);
    return match_bits;
}

/* Receive tag with CQ_DATA.
 * Hosted no-SEP: match local dest slot + expected source slot. ANY_SOURCE
 * ignores source-slot bits; dest slot is always required. */
__opal_attribute_always_inline__ static inline void
mtl_ofi_create_recv_tag_CQD(uint64_t *match_bits, uint64_t *mask_bits,
                            int comm_id, int tag, int source)
{
    *mask_bits  = ompi_mtl_ofi.sync_send;
    *match_bits = (uint64_t) comm_id;
#if HAVE_LITHE
    if (ompi_mtl_ofi.hosted_dst_in_cqd_tag &&
        ompi_mtl_ofi.hosted_cqd_src_bits > 0) {
        int cid_bits = ompi_mtl_ofi.hosted_cqd_cid_bits;
        int sb = ompi_mtl_ofi.hosted_cqd_src_bits;
        uint64_t cid_mask = (cid_bits >= 63) ? ~0ULL : ((1ULL << cid_bits) - 1ULL);
        uint64_t slot_mask = (sb >= 63) ? ~0ULL : ((1ULL << sb) - 1ULL);
        uint64_t cid = (uint64_t) comm_id & cid_mask;
        opal_vpid_t self_v = opal_proc_local_get()->proc_name.vpid;
        uint64_t dslot = mtl_ofi_hosted_cqd_slot((int) self_v) & slot_mask;
        uint64_t sslot = 0;

        if (MPI_ANY_SOURCE != source) {
            sslot = mtl_ofi_hosted_cqd_slot(source) & slot_mask;
        }
        *match_bits = mtl_ofi_hosted_cqd_pack_upper(cid, dslot, sslot);
        if (MPI_ANY_SOURCE == source) {
            *mask_bits |= mtl_ofi_hosted_cqd_src_slot_mask_bits();
        }
        if (MPI_ANY_TAG == tag) {
            *mask_bits |= (ompi_mtl_ofi.mpi_tag_mask >> 1);
        } else {
            *match_bits |= (ompi_mtl_ofi.mpi_tag_mask & tag);
        }
        return;
    }
#else
    (void) source;
#endif
    *match_bits = (*match_bits << (MTL_OFI_PROTO_BIT_COUNT
                                +  MTL_OFI_TAG_BIT_COUNT_DATA));
    if (MPI_ANY_TAG == tag) {
        /* Special negative tags are used for collective operations.
         * MPI_ANY_TAG should not match these special tags.
         * See ompi/mca/coll/base/coll_tags.h
         */
        *mask_bits  |= (ompi_mtl_ofi.mpi_tag_mask>>1);
    } else {
        *match_bits |= (ompi_mtl_ofi.mpi_tag_mask & tag);
    }
}

/*
* ofi_tag_1: fallback when no FI_REMOTE_CQ_DATA is supported
*
*  01234567 0123 4567 01234567 012345   67   01234567 01234567 01234567 01234567
*               |                     |    |
*    Comm id    |     source          |prot|           message tag
*/

#define MTL_OFI_CID_BIT_COUNT_1         (12)
#define MTL_OFI_SOURCE_TAG_MASK_1       (0x000FFFFC00000000ULL)
#define MTL_OFI_SOURCE_BIT_COUNT_1      (18)
#define MTL_OFI_SOURCE_MASK_1           (0x000000000003FFFFULL)
#define MTL_OFI_TAG_MASK_1              (0x00000000FFFFFFFFULL)
#define MTL_OFI_TAG_BIT_COUNT_1         (32)
#define MTL_OFI_PROTO_MASK_1            (0x0000000300000000ULL)
#define MTL_OFI_SYNC_SEND_1             (0x0000000100000000ULL)
#define MTL_OFI_SYNC_SEND_ACK_1         (0x0000000200000000ULL)

/*
* ofi_tag_2: Alternative tag when no FI_REMOTE_CQ_DATA is supported
*
*  01234567 01234567 01234567 01234567 01234567 01  23   4567 01234567 01234567
*                            |                    |    |
*                Comm id     |     source         |prot|     message tag
*/

#define MTL_OFI_CID_BIT_COUNT_2         (24)
#define MTL_OFI_SOURCE_TAG_MASK_2       (0x000000FFFFC00000ULL)
#define MTL_OFI_SOURCE_BIT_COUNT_2      (18)
#define MTL_OFI_SOURCE_MASK_2           (0x000000000003FFFFULL)
#define MTL_OFI_TAG_MASK_2              (0x00000000000FFFFFULL)
#define MTL_OFI_TAG_BIT_COUNT_2         (20)
#define MTL_OFI_PROTO_MASK_2            (0x0000000000300000ULL)
#define MTL_OFI_SYNC_SEND_2             (0x0000000000100000ULL)
#define MTL_OFI_SYNC_SEND_ACK_2         (0x0000000000200000ULL)

/* Send tag */
__opal_attribute_always_inline__ static inline uint64_t
mtl_ofi_create_send_tag(int comm_id, int source, int tag)
{
    uint64_t  match_bits = comm_id;
    match_bits = (match_bits << ompi_mtl_ofi.num_bits_source_rank);
    match_bits |= (uint64_t)(source & ompi_mtl_ofi.source_rank_mask);
    match_bits = (match_bits << (ompi_mtl_ofi.num_bits_mpi_tag
                                 + MTL_OFI_PROTO_BIT_COUNT));
    match_bits |= (tag & ompi_mtl_ofi.mpi_tag_mask);
    return match_bits;
}

/* Receive tag*/
__opal_attribute_always_inline__ static inline void
mtl_ofi_create_recv_tag(uint64_t *match_bits, uint64_t *mask_bits,
                            int comm_id, int source, int tag)
{
    *mask_bits  = ompi_mtl_ofi.sync_send;
    *match_bits = comm_id;
    *match_bits = (*match_bits << ompi_mtl_ofi.num_bits_source_rank);

    if (MPI_ANY_SOURCE == source) {
        *match_bits = (*match_bits << (ompi_mtl_ofi.num_bits_mpi_tag
                                    + MTL_OFI_PROTO_BIT_COUNT));
        *mask_bits |= ompi_mtl_ofi.source_rank_tag_mask;
    } else {
        *match_bits |= (uint64_t)(source & ompi_mtl_ofi.source_rank_mask);
        *match_bits = (*match_bits << (ompi_mtl_ofi.num_bits_mpi_tag
                                 + MTL_OFI_PROTO_BIT_COUNT));
    }

    if (MPI_ANY_TAG == tag) {
        /* Special negative tags are used for collective operations.
         * MPI_ANY_TAG should not match these special tags.
         * See ompi/mca/coll/base/coll_tags.h
         */
          *mask_bits  |= (ompi_mtl_ofi.mpi_tag_mask>>1);
    } else {
        *match_bits |= (ompi_mtl_ofi.mpi_tag_mask & tag);
    }
}

#define MTL_OFI_SET_SYNC_SEND(match_bits)          \
        match_bits |= ompi_mtl_ofi.sync_send

#define MTL_OFI_IS_SYNC_SEND(match_bits)           \
    (ompi_mtl_ofi.sync_send == (ompi_mtl_ofi.sync_proto_mask & match_bits))

#define MTL_OFI_IS_SYNC_SEND_ACK(match_bits)       \
    (ompi_mtl_ofi.sync_send_ack == (ompi_mtl_ofi.sync_proto_mask & match_bits))

#define MTL_OFI_GET_TAG(match_bits)                \
    ((int)(match_bits & ompi_mtl_ofi.mpi_tag_mask))

__opal_attribute_always_inline__ static inline int
mtl_ofi_get_source(struct fi_cq_tagged_entry *wc)
{
    int src;
    if (ompi_mtl_ofi.fi_cq_data) {
        src = (int) wc->data;
    }
    else {
        src = (int)((wc->tag >> (MTL_OFI_PROTO_BIT_COUNT +
                    ompi_mtl_ofi.num_bits_mpi_tag)) & ompi_mtl_ofi.source_rank_mask);
    }

    return src;
}
END_C_DECLS

#endif /* MTL_OFI_TYPES_H_HAS_BEEN_INCLUDED */
