/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2013-2018 Intel, Inc. All rights reserved
 *
 * Copyright (c) 2014-2021 Cisco Systems, Inc.  All rights reserved
 * Copyright (c) 2015-2016 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2018-2022 Amazon.com, Inc. or its affiliates.  All Rights reserved.
 * Copyright (c) 2020-2023 Triad National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"
#include "mtl_ofi.h"
#include "opal/util/argv.h"
#include "opal/util/printf.h"
#include "opal/mca/common/ofi/common_ofi.h"
#include "opal/util/proc.h"

#include <inttypes.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#if defined(__has_include)
#    if __has_include(<parlib/dtls.h>)
#        include <parlib/dtls.h>
#        define OMPI_MTL_OFI_HAVE_PARLIB_DTLS 1
#    endif
#endif

static int ompi_mtl_ofi_component_open(void);
static int ompi_mtl_ofi_component_query(mca_base_module_t **module, int *priority);
static int ompi_mtl_ofi_component_close(void);
static int ompi_mtl_ofi_component_register(void);

static mca_mtl_base_module_t*
ompi_mtl_ofi_component_init(bool enable_progress_threads,
                            bool enable_mpi_threads,
                            bool *accelerator_support);

static int param_priority;
static int control_progress;
static int data_progress;
static int av_type;
static int ofi_tag_mode;

#if OPAL_HAVE_THREAD_LOCAL
static opal_thread_local int ompi_mtl_ofi_per_thread_ctx_fallback;
#endif

#if OMPI_MTL_OFI_HAVE_PARLIB_DTLS
static dtls_key_t ompi_mtl_ofi_ctxt_dtls_key;
static volatile int ompi_mtl_ofi_ctxt_dtls_ready;
#endif

static void ompi_mtl_ofi_ctxt_storage_lazy_init(void)
{
#if OMPI_MTL_OFI_HAVE_PARLIB_DTLS
    if (ompi_mtl_ofi_ctxt_dtls_ready != 0) {
        return;
    }
    if (NULL == getenv("OMPI_LITHE_CONTEXT_LOCAL_PROC")) {
        ompi_mtl_ofi_ctxt_dtls_ready = -1;
        return;
    }
    ompi_mtl_ofi_ctxt_dtls_key = dtls_key_create(NULL);
    ompi_mtl_ofi_ctxt_dtls_ready = (NULL == ompi_mtl_ofi_ctxt_dtls_key) ? -1 : 1;
#else
    (void) 0;
#endif
}

void ompi_mtl_ofi_thread_ctxt_set(int ctxt_id)
{
    ompi_mtl_ofi_ctxt_storage_lazy_init();
#if OMPI_MTL_OFI_HAVE_PARLIB_DTLS
    if (ompi_mtl_ofi_ctxt_dtls_ready > 0) {
        set_dtls(ompi_mtl_ofi_ctxt_dtls_key, (void *) (intptr_t) ctxt_id);
        return;
    }
#endif
#if OPAL_HAVE_THREAD_LOCAL
    ompi_mtl_ofi_per_thread_ctx_fallback = ctxt_id;
#endif
}

int ompi_mtl_ofi_thread_ctxt_get(void)
{
    ompi_mtl_ofi_ctxt_storage_lazy_init();
#if OMPI_MTL_OFI_HAVE_PARLIB_DTLS
    if (ompi_mtl_ofi_ctxt_dtls_ready > 0) {
        void *v = get_dtls(ompi_mtl_ofi_ctxt_dtls_key);
        return (int) (intptr_t) v;
    }
#endif
#if OPAL_HAVE_THREAD_LOCAL
    return ompi_mtl_ofi_per_thread_ctx_fallback;
#else
    return 0;
#endif
}

#if HAVE_LITHE && OMPI_MTL_OFI_HAVE_PARLIB_DTLS

static dtls_key_t ompi_mtl_ofi_mc_prog_nest_key;
static volatile int ompi_mtl_ofi_mc_prog_nest_ready;

/* Track outermost Lithe-context opal_progress so we sweep every SEP CQ once,
 * while nested CQ callback progress only drains the invoking ctxt's CQ. */

static void ompi_mtl_ofi_mc_prog_nest_lazy_init(void)
{
    if (0 != ompi_mtl_ofi_mc_prog_nest_ready) {
        return;
    }
    if (NULL == getenv("OMPI_LITHE_CONTEXT_LOCAL_PROC")) {
        ompi_mtl_ofi_mc_prog_nest_ready = -1;
        return;
    }
    ompi_mtl_ofi_mc_prog_nest_key = dtls_key_create(NULL);
    ompi_mtl_ofi_mc_prog_nest_ready = (NULL == ompi_mtl_ofi_mc_prog_nest_key) ? -1 : 1;
}

int ompi_mtl_ofi_litheme_mc_outer_progress_enter(void)
{
    intptr_t depth;

    if (!ompi_mtl_ofi_lithe_multicontext_active()) {
        return 1;
    }
    ompi_mtl_ofi_mc_prog_nest_lazy_init();
    if (ompi_mtl_ofi_mc_prog_nest_ready <= 0) {
        return 1;
    }

    depth = (intptr_t) get_dtls(ompi_mtl_ofi_mc_prog_nest_key);
    depth++;
    set_dtls(ompi_mtl_ofi_mc_prog_nest_key, (void *) depth);
    return (depth == 1);
}

void ompi_mtl_ofi_litheme_mc_outer_progress_leave(void)
{
    intptr_t depth;

    if (!ompi_mtl_ofi_lithe_multicontext_active()) {
        return;
    }
    if (ompi_mtl_ofi_mc_prog_nest_ready <= 0) {
        return;
    }

    depth = (intptr_t) get_dtls(ompi_mtl_ofi_mc_prog_nest_key);
    if (depth <= 1) {
        set_dtls(ompi_mtl_ofi_mc_prog_nest_key, NULL);
    } else {
        set_dtls(ompi_mtl_ofi_mc_prog_nest_key, (void *) (depth - 1));
    }
}
#elif HAVE_LITHE

int ompi_mtl_ofi_litheme_mc_outer_progress_enter(void)
{
    return 1;
}

void ompi_mtl_ofi_litheme_mc_outer_progress_leave(void)
{
}

#endif /* HAVE_LITHE with/without PARLIB DTLS progress nesting */

#if HAVE_LITHE
static opal_proc_local_changed_fn_t ompi_mtl_ofi_prev_proc_local_hook;
static int ompi_mtl_ofi_proc_hook_installed;

static void ompi_mtl_ofi_proc_local_changed_cb(void)
{
    if (NULL != ompi_mtl_ofi_prev_proc_local_hook) {
        ompi_mtl_ofi_prev_proc_local_hook();
    }
    if (!ompi_mtl_ofi_lithe_multicontext_active()) {
        return;
    }
    if (ompi_comm_invalid(&ompi_mpi_comm_world.comm)) {
        ompi_mtl_ofi_thread_ctxt_set(0);
        return;
    }
    {
        int ctxt = ompi_mtl_ofi_ctxt_index_for_comm(&ompi_mpi_comm_world.comm);

        ompi_mtl_ofi_thread_ctxt_set(ctxt);
    }
}
#endif /* HAVE_LITHE */

/*
 * Enumerators
 */

enum {
    MTL_OFI_PROG_AUTO=1,
    MTL_OFI_PROG_MANUAL,
    MTL_OFI_PROG_UNSPEC,
};

mca_base_var_enum_value_t control_prog_type[] = {
    {MTL_OFI_PROG_AUTO, "auto"},
    {MTL_OFI_PROG_MANUAL, "manual"},
    {MTL_OFI_PROG_UNSPEC, "unspec"},
    {0, NULL}
};

mca_base_var_enum_value_t data_prog_type[] = {
    {MTL_OFI_PROG_AUTO, "auto"},
    {MTL_OFI_PROG_MANUAL, "manual"},
    {MTL_OFI_PROG_UNSPEC, "unspec"},
    {0, NULL}
};

enum {
    MTL_OFI_AV_MAP=1,
    MTL_OFI_AV_TABLE,
    MTL_OFI_AV_UNKNOWN,
};

mca_base_var_enum_value_t av_table_type[] = {
    {MTL_OFI_AV_MAP, "map"},
    {MTL_OFI_AV_TABLE, "table"},
    {0, NULL}
};

enum {
    MTL_OFI_TAG_AUTO=1,
    MTL_OFI_TAG_1,
    MTL_OFI_TAG_2,
    MTL_OFI_TAG_FULL,
};

mca_base_var_enum_value_t ofi_tag_mode_type[] = {
    {MTL_OFI_TAG_AUTO, "auto"},
    {MTL_OFI_TAG_1, "ofi_tag_1"},
    {MTL_OFI_TAG_2, "ofi_tag_2"},
    {MTL_OFI_TAG_FULL, "ofi_tag_full"},
    {0, NULL}
};

mca_mtl_ofi_component_t mca_mtl_ofi_component = {
    {

        /* First, the mca_base_component_t struct containing meta
         * information about the component itself */

        .mtl_version = {
            MCA_MTL_BASE_VERSION_2_0_0,

            .mca_component_name = "ofi",
            OFI_COMPAT_MCA_VERSION,
            .mca_open_component = ompi_mtl_ofi_component_open,
            .mca_close_component = ompi_mtl_ofi_component_close,
            .mca_query_component = ompi_mtl_ofi_component_query,
            .mca_register_component_params = ompi_mtl_ofi_component_register,
        },
        .mtl_data = {
            /* The component is not checkpoint ready */
            MCA_BASE_METADATA_PARAM_NONE
        },

        .mtl_init = ompi_mtl_ofi_component_init,
    }
};

static int
ompi_mtl_ofi_component_register(void)
{
    int ret;
    mca_base_var_enum_t *new_enum = NULL;
    char *desc;

    param_priority = 25;   /* for now give a lower priority than the psm mtl */
    mca_base_component_var_register(&mca_mtl_ofi_component.super.mtl_version,
                                    "priority", "Priority of the OFI MTL component",
                                    MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                    OPAL_INFO_LVL_9,
                                    MCA_BASE_VAR_SCOPE_READONLY,
                                    &param_priority);

    ompi_mtl_ofi.ofi_progress_event_count = MTL_OFI_MAX_PROG_EVENT_COUNT;
    opal_asprintf(&desc, "Max number of events to read each call to OFI progress (default: %d events will be read per OFI progress call)", ompi_mtl_ofi.ofi_progress_event_count);
    mca_base_component_var_register(&mca_mtl_ofi_component.super.mtl_version,
                                    "progress_event_cnt",
                                    desc,
                                    MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                    OPAL_INFO_LVL_6,
                                    MCA_BASE_VAR_SCOPE_READONLY,
                                    &ompi_mtl_ofi.ofi_progress_event_count);

    free(desc);

    ret = mca_base_var_enum_create ("ofi_tag_mode_type", ofi_tag_mode_type , &new_enum);
    if (OPAL_SUCCESS != ret) {
        return ret;
    }

    ofi_tag_mode = MTL_OFI_TAG_AUTO;
    opal_asprintf(&desc, "Mode specifying how many bits to use for various MPI values in OFI/Libfabric"
            " communications. Some Libfabric provider network types can support most of Open MPI"
            " needs; others can only supply a limited number of bits, which then must be split"
            " across the MPI communicator ID, MPI source rank, and MPI tag. Three different"
            " splitting schemes are available: ofi_tag_full (%d bits for the communicator, %d bits"
            " for the source rank, and %d bits for the tag), ofi_tag_1 (%d bits for the communicator"
            ", %d bits source rank, %d bits tag), ofi_tag_2 (%d bits for the communicator"
            ", %d bits source rank, %d bits tag). By default, this MCA variable is set to \"auto\","
            " which will first try to use ofi_tag_full, and if that fails, fall back to ofi_tag_1.",
            MTL_OFI_CID_BIT_COUNT_DATA, 32, MTL_OFI_TAG_BIT_COUNT_DATA,
            MTL_OFI_CID_BIT_COUNT_1, MTL_OFI_SOURCE_BIT_COUNT_1, MTL_OFI_TAG_BIT_COUNT_1,
            MTL_OFI_CID_BIT_COUNT_2, MTL_OFI_SOURCE_BIT_COUNT_2, MTL_OFI_TAG_BIT_COUNT_2);

    mca_base_component_var_register (&mca_mtl_ofi_component.super.mtl_version,
                                    "tag_mode",
                                     desc,
                                     MCA_BASE_VAR_TYPE_INT, new_enum, 0, 0,
                                     OPAL_INFO_LVL_6,
                                     MCA_BASE_VAR_SCOPE_READONLY,
                                     &ofi_tag_mode);

    free(desc);
    OBJ_RELEASE(new_enum);

    ret = mca_base_var_enum_create ("control_prog_type", control_prog_type, &new_enum);
    if (OPAL_SUCCESS != ret) {
        return ret;
    }

    control_progress = MTL_OFI_PROG_UNSPEC;
    mca_base_component_var_register (&mca_mtl_ofi_component.super.mtl_version,
                                     "control_progress",
                                     "Specify control progress model (default: unspecified, use provider's default). Set to auto or manual for auto or manual progress respectively.",
                                     MCA_BASE_VAR_TYPE_INT, new_enum, 0, 0,
                                     OPAL_INFO_LVL_3,
                                     MCA_BASE_VAR_SCOPE_READONLY,
                                     &control_progress);
    OBJ_RELEASE(new_enum);

    ret = mca_base_var_enum_create ("data_prog_type", data_prog_type, &new_enum);
    if (OPAL_SUCCESS != ret) {
        return ret;
    }

    data_progress = MTL_OFI_PROG_UNSPEC;
    mca_base_component_var_register(&mca_mtl_ofi_component.super.mtl_version,
                                    "data_progress",
                                    "Specify data progress model (default: unspecified, use provider's default). Set to auto or manual for auto or manual progress respectively.",
                                    MCA_BASE_VAR_TYPE_INT, new_enum, 0, 0,
                                    OPAL_INFO_LVL_3,
                                    MCA_BASE_VAR_SCOPE_READONLY,
                                    &data_progress);
    OBJ_RELEASE(new_enum);

    ret = mca_base_var_enum_create ("av_type", av_table_type, &new_enum);
    if (OPAL_SUCCESS != ret) {
        return ret;
    }

    av_type = MTL_OFI_AV_MAP;
    mca_base_component_var_register (&mca_mtl_ofi_component.super.mtl_version,
                                     "av",
                                     "Specify AV type to use (default: map). Set to table for FI_AV_TABLE AV type.",
                                     MCA_BASE_VAR_TYPE_INT, new_enum, 0, 0,
                                     OPAL_INFO_LVL_3,
                                     MCA_BASE_VAR_SCOPE_READONLY,
                                     &av_type);
    OBJ_RELEASE(new_enum);

    ompi_mtl_ofi.enable_sep = 0;
    mca_base_component_var_register(&mca_mtl_ofi_component.super.mtl_version,
                                    "enable_sep",
                                    "Enable SEP feature",
                                    MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                    OPAL_INFO_LVL_3,
                                    MCA_BASE_VAR_SCOPE_READONLY,
                                    &ompi_mtl_ofi.enable_sep);

    ompi_mtl_ofi.thread_grouping = 0;
    mca_base_component_var_register(&mca_mtl_ofi_component.super.mtl_version,
                                    "thread_grouping",
                                    "Enable/Disable Thread Grouping feature",
                                    MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                    OPAL_INFO_LVL_3,
                                    MCA_BASE_VAR_SCOPE_READONLY,
                                    &ompi_mtl_ofi.thread_grouping);

    /*
     * Default Policy: Create 1 context and let user ask for more for
     * multi-threaded workloads. User needs to ask for as many contexts as the
     * number of threads that are anticipated to make MPI calls.
     */
    ompi_mtl_ofi.num_ofi_contexts = 1;
    mca_base_component_var_register(&mca_mtl_ofi_component.super.mtl_version,
                                    "num_ctxts",
                                    "Specify number of OFI contexts to create",
                                    MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                    OPAL_INFO_LVL_4,
                                    MCA_BASE_VAR_SCOPE_READONLY,
                                    &ompi_mtl_ofi.num_ofi_contexts);

    ompi_mtl_ofi.disable_hmem = false;
    mca_base_component_var_register(&mca_mtl_ofi_component.super.mtl_version,
                                    "disable_hmem",
                                    "Disable HMEM usage",
                                    MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                    OPAL_INFO_LVL_3,
                                    MCA_BASE_VAR_SCOPE_READONLY,
                                    &ompi_mtl_ofi.disable_hmem);

    return opal_common_ofi_mca_register(&mca_mtl_ofi_component.super.mtl_version);
}



static int
ompi_mtl_ofi_component_open(void)
{
    ompi_mtl_ofi.base.mtl_request_size =
        sizeof(ompi_mtl_ofi_request_t) - sizeof(struct mca_mtl_request_t);

    ompi_mtl_ofi.domain =  NULL;
    ompi_mtl_ofi.av     =  NULL;
    ompi_mtl_ofi.sep     =  NULL;

    /**
     * Sanity check: provider_include and provider_exclude must be mutually
     * exclusive
     */
    if (OMPI_SUCCESS !=
        mca_base_var_check_exclusive("ompi",
            mca_mtl_ofi_component.super.mtl_version.mca_type_name,
            mca_mtl_ofi_component.super.mtl_version.mca_component_name,
            "provider_include",
            mca_mtl_ofi_component.super.mtl_version.mca_type_name,
            mca_mtl_ofi_component.super.mtl_version.mca_component_name,
            "provider_exclude")) {
        return OMPI_ERR_NOT_AVAILABLE;
    }
    {
        int ret = opal_common_ofi_open();
        if (OMPI_SUCCESS != ret) {
            return ret;
        }
    }
    return OMPI_SUCCESS;
}

static int
ompi_mtl_ofi_component_query(mca_base_module_t **module, int *priority)
{
    *priority = param_priority;
    *module = (mca_base_module_t *)&ompi_mtl_ofi.base;
    return OMPI_SUCCESS;
}

static int
ompi_mtl_ofi_component_close(void)
{
#if HAVE_LITHE
    if (ompi_mtl_ofi_proc_hook_installed) {
        if (opal_proc_local_changed_hook == ompi_mtl_ofi_proc_local_changed_cb) {
            opal_proc_local_changed_hook = ompi_mtl_ofi_prev_proc_local_hook;
        }
        ompi_mtl_ofi_prev_proc_local_hook = NULL;
        ompi_mtl_ofi_proc_hook_installed = 0;
    }
#endif
    return opal_common_ofi_close();
}

int
ompi_mtl_ofi_progress_no_inline(void)
{
	return ompi_mtl_ofi_progress();
}

int
ompi_mtl_ofi_progress_block_no_inline(void)
{
	return ompi_mtl_ofi_progress_block();
}

static struct fi_info*
select_ofi_provider(struct fi_info *providers,
                    char **include_list, char **exclude_list)
{
    struct fi_info *prov = providers;

    if (NULL != include_list) {
        while ((NULL != prov) &&
               (!opal_common_ofi_is_in_list(include_list, prov->fabric_attr->prov_name))) {
            opal_output_verbose(1, opal_common_ofi.output,
                                "%s:%d: mtl:ofi: \"%s\" not in include list\n",
                                __FILE__, __LINE__,
                                prov->fabric_attr->prov_name);
            prov = prov->next;
        }
    } else if (NULL != exclude_list) {
        while ((NULL != prov) &&
               (opal_common_ofi_is_in_list(exclude_list, prov->fabric_attr->prov_name))) {
            opal_output_verbose(1, opal_common_ofi.output,
                                "%s:%d: mtl:ofi: \"%s\" in exclude list\n",
                                __FILE__, __LINE__,
                                prov->fabric_attr->prov_name);
            prov = prov->next;
        }
    }

    opal_output_verbose(1, opal_common_ofi.output,
                        "%s:%d: mtl:ofi:provider: %s\n",
                        __FILE__, __LINE__,
                        (prov ? prov->fabric_attr->prov_name : "none"));

    /** The initial provider selection will return a list of providers
      * available for this process. once a provider is selected from the
      * list, we will cycle through the remaining list to identify NICs
      * serviced by this provider, and try to pick one on the same NUMA
      * node as this process. If there are no NICs on the same NUMA node,
      * we pick one in a manner which allows all ranks to make balanced
      * use of available NICs on the system.
      *
      * Most providers give a separate fi_info object for each NIC,
      * however some may have multiple info objects with different
      * attributes for the same NIC. The initial provider attributes
      * are used to ensure that all NICs we return provide the same
      * capabilities as the initial one.
      *
      * We use package rank to select between NICs of equal distance
      * if we cannot calculate a package_rank, we fall back to using the
      * process id.
      */
    if (NULL != prov) {
        prov = opal_common_ofi_select_provider(prov, &ompi_process_info);
        opal_output_verbose(1, opal_common_ofi.output,
                            "%s:%d: mtl:ofi:provider:domain: %s\n",
                            __FILE__, __LINE__,
                            (prov ? prov->domain_attr->name : "none"));
    }

    return prov;
}

static void
ompi_mtl_ofi_define_tag_mode(int ofi_tag_mode_arg, int *bits_for_cid) {
    switch (ofi_tag_mode_arg) {
        case MTL_OFI_TAG_1:
            *bits_for_cid = (int) MTL_OFI_CID_BIT_COUNT_1;
            ompi_mtl_ofi.base.mtl_max_tag = (int)((1ULL << (MTL_OFI_TAG_BIT_COUNT_1 - 1)) - 1);

            ompi_mtl_ofi.source_rank_tag_mask = MTL_OFI_SOURCE_TAG_MASK_1;
            ompi_mtl_ofi.num_bits_source_rank = MTL_OFI_SOURCE_BIT_COUNT_1;
            ompi_mtl_ofi.source_rank_mask = MTL_OFI_SOURCE_MASK_1;

            ompi_mtl_ofi.mpi_tag_mask = MTL_OFI_TAG_MASK_1;
            ompi_mtl_ofi.num_bits_mpi_tag = MTL_OFI_TAG_BIT_COUNT_1;

            ompi_mtl_ofi.sync_send = MTL_OFI_SYNC_SEND_1;
            ompi_mtl_ofi.sync_send_ack = MTL_OFI_SYNC_SEND_ACK_1;
            ompi_mtl_ofi.sync_proto_mask = MTL_OFI_PROTO_MASK_1;
        break;
        case MTL_OFI_TAG_2:
            *bits_for_cid = (int) MTL_OFI_CID_BIT_COUNT_2;
            ompi_mtl_ofi.base.mtl_max_tag = (int)((1ULL << (MTL_OFI_TAG_BIT_COUNT_2 - 1)) - 1);

            ompi_mtl_ofi.source_rank_tag_mask = MTL_OFI_SOURCE_TAG_MASK_2;
            ompi_mtl_ofi.num_bits_source_rank = MTL_OFI_SOURCE_BIT_COUNT_2;
            ompi_mtl_ofi.source_rank_mask = MTL_OFI_SOURCE_MASK_2;

            ompi_mtl_ofi.mpi_tag_mask = MTL_OFI_TAG_MASK_2;
            ompi_mtl_ofi.num_bits_mpi_tag = MTL_OFI_TAG_BIT_COUNT_2;

            ompi_mtl_ofi.sync_send = MTL_OFI_SYNC_SEND_2;
            ompi_mtl_ofi.sync_send_ack = MTL_OFI_SYNC_SEND_ACK_2;
            ompi_mtl_ofi.sync_proto_mask = MTL_OFI_PROTO_MASK_2;
        break;
        default: /* use FI_REMOTE_CQ_DATA */
            *bits_for_cid = (int) MTL_OFI_CID_BIT_COUNT_DATA;
            ompi_mtl_ofi.base.mtl_max_tag = (int)((1ULL << (MTL_OFI_TAG_BIT_COUNT_DATA - 1)) - 1);

            ompi_mtl_ofi.mpi_tag_mask = MTL_OFI_TAG_MASK_DATA;

            ompi_mtl_ofi.sync_send = MTL_OFI_SYNC_SEND_DATA;
            ompi_mtl_ofi.sync_send_ack = MTL_OFI_SYNC_SEND_ACK_DATA;
            ompi_mtl_ofi.sync_proto_mask = MTL_OFI_PROTO_MASK_DATA;
    }
}

#define MTL_OFI_ALLOC_COMM_TO_CONTEXT(arr_size)                                         \
    do {                                                                                \
        ompi_mtl_ofi.comm_to_context = calloc(arr_size, sizeof(int));                   \
        if (OPAL_UNLIKELY(!ompi_mtl_ofi.comm_to_context)) {                             \
            opal_output_verbose(1, opal_common_ofi.output,            \
                                   "%s:%d: alloc of comm_to_context array failed: %s\n",\
                                   __FILE__, __LINE__, strerror(errno));                \
            return ret;                                                                 \
        }                                                                               \
    } while (0);

#define MTL_OFI_ALLOC_OFI_CTXTS()                                                           \
    do {                                                                                    \
        ompi_mtl_ofi.ofi_ctxt = (mca_mtl_ofi_context_t *) malloc(ompi_mtl_ofi.num_ofi_contexts * \
                                                          sizeof(mca_mtl_ofi_context_t));   \
        if (OPAL_UNLIKELY(!ompi_mtl_ofi.ofi_ctxt)) {                                        \
            opal_output_verbose(1, opal_common_ofi.output,                \
                                   "%s:%d: alloc of ofi_ctxt array failed: %s\n",           \
                                   __FILE__, __LINE__, strerror(errno));                    \
            return ret;                                                                     \
        }                                                                                   \
    } while(0);

static int ompi_mtl_ofi_init_sep(struct fi_info *prov, int universe_size)
{
    int ret = OMPI_SUCCESS, num_ofi_ctxts;
    struct fi_av_attr av_attr = {0};

    prov->ep_attr->tx_ctx_cnt = prov->ep_attr->rx_ctx_cnt =
                                ompi_mtl_ofi.num_ofi_contexts;

    ret = fi_scalable_ep(ompi_mtl_ofi.domain, prov, &ompi_mtl_ofi.sep, NULL);
    if (0 != ret) {
        opal_show_help("help-mtl-ofi.txt", "OFI call fail", true,
                       "fi_scalable_ep",
                       ompi_process_info.nodename, __FILE__, __LINE__,
                       fi_strerror(-ret), -ret);
        return ret;
    }

    ompi_mtl_ofi.rx_ctx_bits = 0;
    while (ompi_mtl_ofi.num_ofi_contexts >> ++ompi_mtl_ofi.rx_ctx_bits);

    av_attr.type = (MTL_OFI_AV_TABLE == av_type) ? FI_AV_TABLE: FI_AV_MAP;
    av_attr.rx_ctx_bits = ompi_mtl_ofi.rx_ctx_bits;
    av_attr.count = ompi_mtl_ofi.num_ofi_contexts * universe_size;
    ret = fi_av_open(ompi_mtl_ofi.domain, &av_attr, &ompi_mtl_ofi.av, NULL);

    if (0 != ret) {
        MTL_OFI_LOG_FI_ERR(ret, "fi_av_open failed");
        return ret;
    }

    ret = fi_scalable_ep_bind(ompi_mtl_ofi.sep, (fid_t)ompi_mtl_ofi.av, 0);
    if (0 != ret) {
        MTL_OFI_LOG_FI_ERR(ret, "fi_bind AV-EP failed");
        return ret;
    }

    /*
     * If SEP supported and Thread Grouping feature enabled, use
     * num_ofi_contexts + 2. Extra 2 items is to accommodate Open MPI contextid
     * numbering- COMM_WORLD is 0, COMM_SELF is 1. Other user created
     * Comm contextid values are assigned sequentially starting with 3.
     */
    num_ofi_ctxts = ompi_mtl_ofi.thread_grouping ?
                ompi_mtl_ofi.num_ofi_contexts + 2 : 1;
    MTL_OFI_ALLOC_COMM_TO_CONTEXT(num_ofi_ctxts);

    ompi_mtl_ofi.total_ctxts_used = 0;
    ompi_mtl_ofi.threshold_comm_context_id = 0;

    /* Allocate memory for OFI contexts */
    MTL_OFI_ALLOC_OFI_CTXTS();

    return ret;
}

static int ompi_mtl_ofi_init_regular_ep(struct fi_info * prov, int universe_size)
{
    int ret = OMPI_SUCCESS;
    struct fi_av_attr av_attr = {0};
    struct fi_cq_attr cq_attr = {0};
    ompi_mtl_ofi_init_cq_attr(&cq_attr);

    /* Override any user defined setting */
    ompi_mtl_ofi.num_ofi_contexts = 1;
    ret = fi_endpoint(ompi_mtl_ofi.domain, /* In:  Domain object   */
                      prov,                /* In:  Provider        */
                      &ompi_mtl_ofi.sep,    /* Out: Endpoint object */
                      NULL);               /* Optional context     */
    if (0 != ret) {
        opal_show_help("help-mtl-ofi.txt", "OFI call fail", true,
                       "fi_endpoint",
                       ompi_process_info.nodename, __FILE__, __LINE__,
                       fi_strerror(-ret), -ret);
        return ret;
    }

    /**
     * Create the objects that will be bound to the endpoint.
     * The objects include:
     *     - address vector and completion queues
     */
    av_attr.type = (MTL_OFI_AV_TABLE == av_type) ? FI_AV_TABLE: FI_AV_MAP;
    av_attr.count = universe_size;
    ret = fi_av_open(ompi_mtl_ofi.domain, &av_attr, &ompi_mtl_ofi.av, NULL);
    if (ret) {
        MTL_OFI_LOG_FI_ERR(ret, "fi_av_open failed");
        return ret;
    }

    ret = fi_ep_bind(ompi_mtl_ofi.sep,
                     (fid_t)ompi_mtl_ofi.av,
                     0);
    if (0 != ret) {
        MTL_OFI_LOG_FI_ERR(ret, "fi_bind AV-EP failed");
        return ret;
    }

    MTL_OFI_ALLOC_COMM_TO_CONTEXT(1);

    /* Allocate memory for OFI contexts */
    MTL_OFI_ALLOC_OFI_CTXTS();

    ompi_mtl_ofi.ofi_ctxt[0].tx_ep = ompi_mtl_ofi.sep;
    ompi_mtl_ofi.ofi_ctxt[0].rx_ep = ompi_mtl_ofi.sep;

    ret = ompi_mtl_ofi_open_cq(&cq_attr, &ompi_mtl_ofi.ofi_ctxt[0].cq,
                               &ompi_mtl_ofi.ofi_ctxt[0].cq_wait_fd);
    if (ret) {
        MTL_OFI_LOG_FI_ERR(ret, "fi_cq_open failed");
        return ret;
    }

    /* Bind CQ to endpoint object */
    ret = fi_ep_bind(ompi_mtl_ofi.sep, (fid_t)ompi_mtl_ofi.ofi_ctxt[0].cq,
                     FI_TRANSMIT | FI_RECV | FI_SELECTIVE_COMPLETION);
    if (0 != ret) {
        MTL_OFI_LOG_FI_ERR(ret, "fi_bind CQ-EP failed");
        return ret;
    }

    return ret;
}

#if HAVE_LITHE
/*
 * Hosted multicontext without provider SEP (cxi: max_ep_*_ctx=1, ep_cnt>>1):
 * open one regular EP+CQ per logical slot so co-resident ranks do not share a
 * single matching/CQ domain (wrong Allreduce sums at RPH>=4).
 */
static int ompi_mtl_ofi_init_multi_regular_ep(struct fi_info *prov, int universe_size,
                                              int num_eps)
{
    int ret = OMPI_SUCCESS, i;
    struct fi_av_attr av_attr = {0};
    struct fi_cq_attr cq_attr = {0};

    if (num_eps < 2) {
        return ompi_mtl_ofi_init_regular_ep(prov, universe_size);
    }

    ompi_mtl_ofi_init_cq_attr(&cq_attr);
    ompi_mtl_ofi.num_ofi_contexts = num_eps;
    ompi_mtl_ofi.rx_ctx_bits = 0;
    ompi_mtl_ofi.enable_sep = 0;
    ompi_mtl_ofi.hosted_multi_ep = 1;

    av_attr.type = (MTL_OFI_AV_TABLE == av_type) ? FI_AV_TABLE : FI_AV_MAP;
    av_attr.count = (size_t) num_eps * (size_t) universe_size;
    ret = fi_av_open(ompi_mtl_ofi.domain, &av_attr, &ompi_mtl_ofi.av, NULL);
    if (ret) {
        MTL_OFI_LOG_FI_ERR(ret, "fi_av_open failed");
        return ret;
    }

    MTL_OFI_ALLOC_COMM_TO_CONTEXT(num_eps);
    MTL_OFI_ALLOC_OFI_CTXTS();
    memset(ompi_mtl_ofi.ofi_ctxt, 0, (size_t) num_eps * sizeof(mca_mtl_ofi_context_t));

    for (i = 0; i < num_eps; ++i) {
        struct fid_ep *ep = NULL;

        ret = fi_endpoint(ompi_mtl_ofi.domain, prov, &ep, NULL);
        if (0 != ret) {
            opal_show_help("help-mtl-ofi.txt", "OFI call fail", true,
                           "fi_endpoint",
                           ompi_process_info.nodename, __FILE__, __LINE__,
                           fi_strerror(-ret), -ret);
            return ret;
        }

        ompi_mtl_ofi.ofi_ctxt[i].tx_ep = ep;
        ompi_mtl_ofi.ofi_ctxt[i].rx_ep = ep;

        ret = fi_ep_bind(ep, (fid_t) ompi_mtl_ofi.av, 0);
        if (0 != ret) {
            MTL_OFI_LOG_FI_ERR(ret, "fi_bind AV-EP failed");
            return ret;
        }

        ret = ompi_mtl_ofi_open_cq(&cq_attr, &ompi_mtl_ofi.ofi_ctxt[i].cq,
                                   &ompi_mtl_ofi.ofi_ctxt[i].cq_wait_fd);
        if (ret) {
            MTL_OFI_LOG_FI_ERR(ret, "fi_cq_open failed");
            return ret;
        }

        ret = fi_ep_bind(ep, (fid_t) ompi_mtl_ofi.ofi_ctxt[i].cq,
                         FI_TRANSMIT | FI_RECV | FI_SELECTIVE_COMPLETION);
        if (0 != ret) {
            MTL_OFI_LOG_FI_ERR(ret, "fi_bind CQ-EP failed");
            return ret;
        }
    }

    ompi_mtl_ofi.sep = ompi_mtl_ofi.ofi_ctxt[0].tx_ep;
    opal_output_verbose(1, opal_common_ofi.output,
                        "%s:%d: Lithe hosted multi regular EP: num_eps=%d (no SEP)\n",
                        __FILE__, __LINE__, num_eps);
    return OMPI_SUCCESS;
}
#endif /* HAVE_LITHE */

static mca_mtl_base_module_t*
ompi_mtl_ofi_component_init(bool enable_progress_threads,
                            bool enable_mpi_threads,
                            bool *accelerator_support)
{
    int ret, fi_primary_version, fi_alternate_version;
    int num_local_ranks, sep_support_in_provider, max_ofi_ctxts;
    int ofi_tag_leading_zeros, ofi_tag_bits_for_cid;
    char **include_list = NULL;
    char **exclude_list = NULL;
    struct fi_info *hints, *hints_dup = NULL;
    struct fi_info *providers = NULL;
    struct fi_info *prov = NULL;
    struct fi_info *prov_cq_data = NULL;
    void *ep_name = NULL;
    size_t namelen = 0;
    int universe_size;
    char *univ_size_str;

    opal_output_verbose(1, opal_common_ofi.output,
                        "%s:%d: mtl:ofi:provider_include = \"%s\"\n",
                        __FILE__, __LINE__, *opal_common_ofi.prov_include);
    opal_output_verbose(1, opal_common_ofi.output,
                        "%s:%d: mtl:ofi:provider_exclude = \"%s\"\n",
                        __FILE__, __LINE__, *opal_common_ofi.prov_exclude);
#if HAVE_LITHE
    ompi_mtl_ofi.hosted_multi_ep = 0;
    ompi_mtl_ofi.shared_ep_excl_progress = 0;
    ompi_mtl_ofi.hosted_dst_in_cqd_tag = 0;
    ompi_mtl_ofi.hosted_cqd_src_bits = 0;
    ompi_mtl_ofi.hosted_cqd_cid_bits = 0;
    memset(ompi_mtl_ofi.hosted_cqd_slot_bitpos, 0,
           sizeof(ompi_mtl_ofi.hosted_cqd_slot_bitpos));
    memset(ompi_mtl_ofi.hosted_cqd_cid_bitpos, 0,
           sizeof(ompi_mtl_ofi.hosted_cqd_cid_bitpos));
#endif

    if (NULL != *opal_common_ofi.prov_include) {
        include_list = opal_argv_split(*opal_common_ofi.prov_include, ',');
    } else if (NULL != *opal_common_ofi.prov_exclude) {
        exclude_list = opal_argv_split(*opal_common_ofi.prov_exclude, ',');
    }

    /**
     * Note: API version 1.5 is the first version that supports
     * FI_LOCAL_COMM / FI_REMOTE_COMM checking (and we definitely need
     * that checking -- e.g., the shared memory provider supports
     * intranode communication (FI_LOCAL_COMM), but not internode
     * (FI_REMOTE_COMM), which is insufficient for MTL selection.
     *
     * Note: API version 1.9 is the first version that supports FI_HMEM
     *
     * Note: API version 1.18 is the first version that clearly define
     * provider's behavior in making CUDA API calls that all provider
     * by default is permitted to make CUDA calls if application uses >= 1.18 API.
     *
     * If application is using < 1.18 API, some provider will not claim support
     * of FI_HMEM (even if they are capable of) because it does not know
     * whether application permits it to make CUDA calls.
     */
    fi_primary_version = FI_VERSION(1, 18);
    fi_alternate_version = FI_VERSION(1, 9);

    /**
     * Hints to filter providers
     * See man fi_getinfo for a list of all filters
     * mode:  Select capabilities MTL is prepared to support.
     *        In this case, MTL will pass in context into communication calls
     * ep_type:  reliable datagram operation
     * caps:     Capabilities required from the provider.
     *           Tag matching is specified to implement MPI semantics.
     * msg_order: Guarantee that messages with same tag are ordered.
     */
    hints = fi_allocinfo();
    if (!hints) {
        opal_output_verbose(1, opal_common_ofi.output,
                            "%s:%d: Could not allocate fi_info\n",
                            __FILE__, __LINE__);
        goto error;
    }

    /* Request device transfer capabilities */
#if defined(FI_HMEM)
    if (false == ompi_mtl_ofi.disable_hmem) {
        hints->caps |= FI_HMEM;
        hints->domain_attr->mr_mode |= FI_MR_HMEM | FI_MR_ALLOCATED;
    }
#endif

no_hmem:

    /* Make sure to get a RDM provider that can do the tagged matching
       interface and local communication and remote communication. */
    hints->mode               = FI_CONTEXT | FI_CONTEXT2;
    hints->ep_attr->type      = FI_EP_RDM;
    hints->caps               |= FI_MSG | FI_TAGGED | FI_LOCAL_COMM | FI_REMOTE_COMM | FI_DIRECTED_RECV;
    hints->tx_attr->msg_order = FI_ORDER_SAS;
    hints->rx_attr->msg_order = FI_ORDER_SAS;
    hints->rx_attr->op_flags  = FI_COMPLETION;
    hints->tx_attr->op_flags  = FI_COMPLETION;

    if (enable_mpi_threads) {
        ompi_mtl_ofi.mpi_thread_multiple = true;
        hints->domain_attr->threading = FI_THREAD_SAFE;
    } else {
        ompi_mtl_ofi.mpi_thread_multiple = false;
        hints->domain_attr->threading = FI_THREAD_DOMAIN;
    }

    if ((MTL_OFI_TAG_AUTO == ofi_tag_mode) || (MTL_OFI_TAG_FULL == ofi_tag_mode)) {
        hints->domain_attr->cq_data_size = sizeof(int);
    }

    switch (control_progress) {
    case MTL_OFI_PROG_AUTO:
	hints->domain_attr->control_progress = FI_PROGRESS_AUTO;
	break;
    case MTL_OFI_PROG_MANUAL:
        hints->domain_attr->control_progress = FI_PROGRESS_MANUAL;
	break;
    default:
        hints->domain_attr->control_progress = FI_PROGRESS_UNSPEC;
    }

    switch (data_progress) {
    case MTL_OFI_PROG_AUTO:
	hints->domain_attr->data_progress = FI_PROGRESS_AUTO;
	break;
    case MTL_OFI_PROG_MANUAL:
        hints->domain_attr->data_progress = FI_PROGRESS_MANUAL;
	break;
    default:
        hints->domain_attr->data_progress = FI_PROGRESS_UNSPEC;
    }

    if (MTL_OFI_AV_TABLE == av_type) {
        hints->domain_attr->av_type          = FI_AV_TABLE;
    } else {
        hints->domain_attr->av_type          = FI_AV_MAP;
    }

    hints->domain_attr->resource_mgmt    = FI_RM_ENABLED;

    /**
     * The EFA provider in Libfabric versions prior to 1.10 contains a bug
     * where the FI_LOCAL_COMM and FI_REMOTE_COMM capabilities are not
     * advertised.  However, we know that this provider supports both local and
     * remote communication. We must exclude these capability bits in order to
     * select EFA when we are using a version of Libfabric with this bug.
     *
     * Call fi_getinfo() without those capabilities and specifically ask for
     * the EFA provider. This is safe to do as EFA is only supported on Amazon
     * EC2 and EC2 only supports EFA and TCP-based networks. We'll also skip
     * this logic if the user specifies an include list without EFA or adds EFA
     * to the exclude list.
     */
    if ((include_list && opal_common_ofi_is_in_list(include_list, "efa")) ||
        (exclude_list && !opal_common_ofi_is_in_list(exclude_list, "efa"))) {
        hints_dup = fi_dupinfo(hints);
        hints_dup->caps &= ~(FI_LOCAL_COMM | FI_REMOTE_COMM);
        hints_dup->fabric_attr->prov_name = strdup("efa");

        ret = fi_getinfo(fi_primary_version, NULL, NULL, 0ULL, hints_dup, &providers);
        if (FI_ENOSYS == -ret) {
            /* libfabric is not new enough, fallback to use older version of API */
           ret = fi_getinfo(fi_alternate_version, NULL, NULL, 0ULL, hints_dup, &providers);
	}

        opal_output_verbose(1, opal_common_ofi.output,
                            "%s:%d: EFA specific fi_getinfo(): %s\n",
                            __FILE__, __LINE__, fi_strerror(-ret));

        if (FI_ENODATA == -ret) {
            /**
             * EFA is not available so fall through to call fi_getinfo() again
             * with the local/remote capabilities set.
             */
            fi_freeinfo(hints_dup);
            hints_dup = NULL;
        } else if (0 != ret) {
            opal_show_help("help-mtl-ofi.txt", "OFI call fail", true,
                           "fi_getinfo",
                           ompi_process_info.nodename, __FILE__, __LINE__,
                           fi_strerror(-ret), -ret);
            goto error;
        } else {
            fi_freeinfo(hints);
            hints = hints_dup;
            hints_dup = NULL;
            goto select_prov;
        }
    }

    /**
     * fi_getinfo:  returns information about fabric  services for reaching a
     * remote node or service.  this does not necessarily allocate resources.
     * Pass NULL for name/service because we want a list of providers supported.
     */
    ret = fi_getinfo(fi_primary_version,    /* OFI version requested            */
                     NULL,          /* Optional name or fabric to resolve       */
                     NULL,          /* Optional service name or port to request */
                     0ULL,          /* Optional flag                            */
                     hints,         /* In: Hints to filter providers            */
                     &providers);   /* Out: List of matching providers          */
    if (FI_ENOSYS == -ret) {
        ret = fi_getinfo(fi_alternate_version, NULL, NULL, 0ULL, hints, &providers);
    }

    opal_output_verbose(1, opal_common_ofi.output,
                        "%s:%d: fi_getinfo(): %s\n",
                        __FILE__, __LINE__, fi_strerror(-ret));

    if (FI_ENODATA == -ret) {
#if defined(FI_HMEM)
        /* Attempt selecting a provider without FI_HMEM hints */
        if (hints->caps & FI_HMEM) {
            hints->caps &= ~FI_HMEM;
            hints->domain_attr->mr_mode &= ~FI_MR_HMEM;
            goto no_hmem;
        }
#endif
        /* It is not an error if no information is returned. */
        goto error;
    } else if (0 != ret) {
        opal_show_help("help-mtl-ofi.txt", "OFI call fail", true,
                       "fi_getinfo",
                       ompi_process_info.nodename, __FILE__, __LINE__,
                       fi_strerror(-ret), -ret);
        goto error;
    }

select_prov:
    /**
     * Select a provider from the list returned by fi_getinfo().
     */
    prov = select_ofi_provider(providers, include_list, exclude_list);
    if (!prov) {
        opal_output_verbose(1, opal_common_ofi.output,
                            "%s:%d: select_ofi_provider: no provider found\n",
                            __FILE__, __LINE__);
        goto error;
    }

    opal_argv_free(include_list);
    include_list = NULL;
    opal_argv_free(exclude_list);
    exclude_list = NULL;

    *accelerator_support = false;
#if defined(FI_HMEM)
    if (!(prov->caps & FI_HMEM) || (true == ompi_mtl_ofi.disable_hmem)) {
        if (!(prov->caps & FI_HMEM) && (false == ompi_mtl_ofi.disable_hmem)) {
            opal_output_verbose(50, opal_common_ofi.output,
                                "%s:%d: Libfabric provider does not support device buffers. Continuing with device to host copies.\n",
                               __FILE__, __LINE__);
        }
        if (true == ompi_mtl_ofi.disable_hmem) {
            opal_output_verbose(50, opal_common_ofi.output,
                                "%s:%d: Support for device buffers disabled by MCA parameter. Continuing with device to host copies.\n",
                               __FILE__, __LINE__);
        }
    } else {
        *accelerator_support = true;
        ompi_mtl_ofi.hmem_needs_reg = true;
        /*
         * Workaround for the fact that the CXI provider actually doesn't need for accelerator memory to be registered
         * for local buffers, but if one does do so using fi_mr_regattr, one actually needs to manage the
         * requested_key field in the fi_mr_attr attr argument, and the OFI MTL doesn't track which requested_keys
         * have already been registered. So just set a flag to disable local registration.  Note the OFI BTL doesn't
         * have a problem here since it uses fi_mr_regattr only within the context of an rcache, and manages the
         * requested_key field in this way.
         */
         if (!strncasecmp(prov->fabric_attr->prov_name, "cxi", 3)) {
             ompi_mtl_ofi.hmem_needs_reg = false;
         }

    }
#else
    opal_output_verbose(50, opal_common_ofi.output,
                        "%s:%d: Libfabric provider does not support device buffers. Continuing with device to host copies.\n",
                        __FILE__, __LINE__);
#endif

    /**
     * Select the format of the OFI tag
     */
    if ((MTL_OFI_TAG_AUTO == ofi_tag_mode) ||
        (MTL_OFI_TAG_FULL == ofi_tag_mode)) {
            if (prov->domain_attr->cq_data_size >= sizeof(int) &&
                (prov->caps & FI_DIRECTED_RECV)) {
                /* Use FI_REMOTE_CQ_DATA (FULL tag layout — required for cxi
                 * mem_tag_format; ofi_tag_1 does not fit). */
                ompi_mtl_ofi.fi_cq_data = true;
                ompi_mtl_ofi_define_tag_mode(MTL_OFI_TAG_FULL, &ofi_tag_bits_for_cid);
#if HAVE_LITHE
                /*
                 * Hosted ranks (RPH>=2) on providers without SEP share one
                 * regular EP address, so FI_DIRECTED_RECV cannot isolate
                 * co-resident peers. Embed dest+src slots in the CQD match
                 * tag (see mtl_ofi_create_*_tag_CQD).
                 */
                {
                    int sep_ok = (prov->domain_attr->max_ep_tx_ctx > 1) ||
                                 (prov->domain_attr->max_ep_rx_ctx > 1);
                    unsigned long lith_rph = opal_lithe_env_cache_rph();
                    if (!sep_ok && lith_rph >= 2UL) {
                        /* Flag only here; shrink cid after mem_tag_format so
                         * the provider high-bit check still uses FULL width. */
                        ompi_mtl_ofi.hosted_dst_in_cqd_tag = 1;
                        opal_output_verbose(
                            1, opal_common_ofi.output,
                            "%s:%d: Lithe hosted no-SEP: will embed dest+src "
                            "slots in CQD tag; provider=%s rph=%lu\n",
                            __FILE__, __LINE__,
                            prov->fabric_attr->prov_name, lith_rph);
                    }
                }
#endif
            } else {
                /* No support for FI_REMTOTE_CQ_DATA */
                ompi_mtl_ofi.fi_cq_data = false;
                if (MTL_OFI_TAG_AUTO == ofi_tag_mode) {
                   /* Fallback to MTL_OFI_TAG_1 */
                   ompi_mtl_ofi_define_tag_mode(MTL_OFI_TAG_1, &ofi_tag_bits_for_cid);
                } else { /* MTL_OFI_TAG_FULL */
                   opal_output_verbose(1, opal_common_ofi.output,
                            "%s:%d: OFI provider %s does not support FI_REMOTE_CQ_DATA\n",
                            __FILE__, __LINE__, prov->fabric_attr->prov_name);
                    goto error;
                }
            }
    } else { /* MTL_OFI_TAG_1 or MTL_OFI_TAG_2 */
        ompi_mtl_ofi.fi_cq_data = false;
        ompi_mtl_ofi_define_tag_mode(ofi_tag_mode, &ofi_tag_bits_for_cid);
    }

    /**
     * Initialize the MTL OFI Symbol Tables & function pointers
     * for specialized functions.
     */

    ompi_mtl_ofi_send_symtable_init(&ompi_mtl_ofi.sym_table);
    ompi_mtl_ofi.base.mtl_send =
        ompi_mtl_ofi.sym_table.ompi_mtl_ofi_send[ompi_mtl_ofi.fi_cq_data];

    ompi_mtl_ofi_isend_symtable_init(&ompi_mtl_ofi.sym_table);
    ompi_mtl_ofi.base.mtl_isend =
        ompi_mtl_ofi.sym_table.ompi_mtl_ofi_isend[ompi_mtl_ofi.fi_cq_data];

    ompi_mtl_ofi_irecv_symtable_init(&ompi_mtl_ofi.sym_table);
    ompi_mtl_ofi.base.mtl_irecv =
        ompi_mtl_ofi.sym_table.ompi_mtl_ofi_irecv[ompi_mtl_ofi.fi_cq_data];

    ompi_mtl_ofi_iprobe_symtable_init(&ompi_mtl_ofi.sym_table);
    ompi_mtl_ofi.base.mtl_iprobe =
        ompi_mtl_ofi.sym_table.ompi_mtl_ofi_iprobe[ompi_mtl_ofi.fi_cq_data];

    ompi_mtl_ofi_improbe_symtable_init(&ompi_mtl_ofi.sym_table);
    ompi_mtl_ofi.base.mtl_improbe =
        ompi_mtl_ofi.sym_table.ompi_mtl_ofi_improbe[ompi_mtl_ofi.fi_cq_data];

    /**
     * Check for potential bits in the OFI tag that providers may be reserving
     * for internal usage (see mem_tag_format in fi_endpoint man page).
     */

    ofi_tag_leading_zeros = 0;
    while (!((prov->ep_attr->mem_tag_format << ofi_tag_leading_zeros++) &
           (uint64_t) MTL_OFI_HIGHEST_TAG_BIT) &&
           /* Do not keep looping if the provider does not support enough bits */
           (ofi_tag_bits_for_cid >= MTL_OFI_MINIMUM_CID_BITS)){
       ofi_tag_bits_for_cid--;
    }

    if (ofi_tag_bits_for_cid < MTL_OFI_MINIMUM_CID_BITS) {
        opal_show_help("help-mtl-ofi.txt", "Not enough bits for CID", true,
                       prov->fabric_attr->prov_name,
                       prov->fabric_attr->prov_name,
                       ompi_process_info.nodename, __FILE__, __LINE__);
        goto error;
    }

#if HAVE_LITHE
    /*
     * After provider bit reservation: carve co-resident *slot* bits
     * (ceil(log2(RPH))) from provider-*usable* upper tag bits only.
     * Contiguous <<34 packing is wrong on cxi (mem_tag_format
     * 0x0000aaaaaaaaaaaa — odd bits only); low slot bits land on ignored
     * even positions and collapse slots. Cross-node peers still differ by
     * FI_DIRECTED_RECV addresses.
     */
    if (ompi_mtl_ofi.hosted_dst_in_cqd_tag) {
        unsigned long lith_rph = opal_lithe_env_cache_rph();
        int need_src = 0;
        unsigned long r;
        uint64_t fmt = prov->ep_attr->mem_tag_format;
        uint8_t usable[32];
        int n_usable = 0;
        int b, i;

        if (lith_rph < 2UL) {
            ompi_mtl_ofi.hosted_dst_in_cqd_tag = 0;
        } else {
            r = lith_rph;
            while ((1UL << need_src) < r) {
                need_src++;
            }
            /* Upper field starts at PROTO_TAG_SHIFT (tag32+proto2). */
            for (b = MTL_OFI_HOSTED_CQD_PROTO_TAG_SHIFT; b < 64; ++b) {
                if (fmt & (1ULL << b)) {
                    if (n_usable < (int) sizeof(usable)) {
                        usable[n_usable++] = (uint8_t) b;
                    }
                }
            }
            /* Need 2*slot_bits (dest+src) + at least 1 cid bit. cxi has ~7
             * usable upper bits: RPH<=8 fits (3+3+1); RPH=16 needs multi-EP. */
            {
                int need_slots = need_src * 2;
                if (need_src < 1 || need_src > 8 || n_usable < need_slots + 1) {
                    opal_output_verbose(
                        1, opal_common_ofi.output,
                        "%s:%d: Lithe hosted no-SEP: only %d usable upper tag "
                        "bits (mem_tag_format=0x%016" PRIx64 "); need %d "
                        "(dest+src) slot + cid for rph=%lu. Leaving CQD-only.\n",
                        __FILE__, __LINE__, n_usable, (uint64_t) fmt, need_slots,
                        lith_rph);
                    ompi_mtl_ofi.hosted_dst_in_cqd_tag = 0;
                    ompi_mtl_ofi.hosted_cqd_src_bits = 0;
                    ompi_mtl_ofi.hosted_cqd_cid_bits = ofi_tag_bits_for_cid;
                } else {
                    int cid_bits = n_usable - need_slots;
                    if (cid_bits > 24) {
                        cid_bits = 24;
                    }
                    ompi_mtl_ofi.hosted_cqd_src_bits = need_src;
                    ompi_mtl_ofi.hosted_cqd_cid_bits = cid_bits;
                    for (i = 0; i < need_slots; ++i) {
                        ompi_mtl_ofi.hosted_cqd_slot_bitpos[i] = usable[i];
                    }
                    for (i = 0; i < cid_bits; ++i) {
                        ompi_mtl_ofi.hosted_cqd_cid_bitpos[i] =
                            usable[need_slots + i];
                    }
                    ofi_tag_bits_for_cid = cid_bits;
                    opal_output_verbose(
                        1, opal_common_ofi.output,
                        "%s:%d: Lithe hosted no-SEP: embed dest+src slots in "
                        "CQD tag (usable_upper=%d cid_bits=%d slot_bits=%d "
                        "dest_bitpos=[%u,%u] src_bitpos=[%u,%u] rph=%lu "
                        "fmt=0x%016" PRIx64 ")\n",
                        __FILE__, __LINE__, n_usable, cid_bits, need_src,
                        (unsigned) ompi_mtl_ofi.hosted_cqd_slot_bitpos[0],
                        need_src > 1
                            ? (unsigned) ompi_mtl_ofi.hosted_cqd_slot_bitpos[1]
                            : 0u,
                        (unsigned) ompi_mtl_ofi.hosted_cqd_slot_bitpos[need_src],
                        need_src > 1
                            ? (unsigned) ompi_mtl_ofi
                                  .hosted_cqd_slot_bitpos[need_src + 1]
                            : 0u,
                        lith_rph, (uint64_t) fmt);
                }
            }
        }
    }
#endif

    /* Update the maximum supported Communicator ID */
    ompi_mtl_ofi.base.mtl_max_contextid = (int)((1ULL << ofi_tag_bits_for_cid) - 1);
    ompi_mtl_ofi.num_peers = 0;

    /* Check if Scalable Endpoints can be enabled for the provider */
    sep_support_in_provider = 0;
    if ((prov->domain_attr->max_ep_tx_ctx > 1) ||
        (prov->domain_attr->max_ep_rx_ctx > 1)) {
        sep_support_in_provider = 1;
    }

#if HAVE_LITHE
    /*
     * Hosted multicontext (RPH>=2): prefer SEP (one rx/tx ctxt per logical
     * slot). MCA default enable_sep=0. Auto-enable only when the provider
     * advertises max_ep_*_ctx>1 — cxi on this site reports 1 (no SEP); forcing
     * SEP then aborts. Without SEP, keep K small per OS process (see HOSTED_RANKS).
     */
    {
        unsigned long lith_rph = opal_lithe_env_cache_rph();
        if (lith_rph >= 2UL && 0 == ompi_mtl_ofi.enable_sep &&
            0 != sep_support_in_provider) {
            ompi_mtl_ofi.enable_sep = 1;
            opal_output_verbose(1, opal_common_ofi.output,
                                "%s:%d: Lithe hosted RPH=%lu: enabling OFI SEP\n",
                                __FILE__, __LINE__, lith_rph);
        }
    }
#endif

    if (1 == ompi_mtl_ofi.enable_sep) {
        if (0 == sep_support_in_provider) {
            opal_show_help("help-mtl-ofi.txt", "SEP unavailable", true,
                           prov->fabric_attr->prov_name,
                           ompi_process_info.nodename, __FILE__, __LINE__);
            goto error;
        } else if (1 == sep_support_in_provider) {
            opal_output_verbose(1, opal_common_ofi.output,
                                "%s:%d: Scalable EP supported in %s provider. Enabling in MTL.\n",
                                __FILE__, __LINE__, prov->fabric_attr->prov_name);
        }
    } else {
        /*
         * Scalable Endpoints is required for Thread Grouping feature
         */
        if (1 == ompi_mtl_ofi.thread_grouping) {
            opal_show_help("help-mtl-ofi.txt", "SEP required", true,
                           ompi_process_info.nodename, __FILE__, __LINE__);
            goto error;
        }
    }

    /* this must be called during single threaded part of the code and
     * before Libfabric configures its memory monitors.  Easiest to do
     * that before domain open.  Silently ignore not-supported errors,
     * as they are not critical to program correctness, but only
     * indicate that LIbfabric will have to pick a different, possibly
     * less optimal, monitor. */
    ret = opal_common_ofi_export_memory_monitor();
    if (0 != ret && -FI_ENOSYS != ret) {
        opal_output_verbose(1, opal_common_ofi.output,
                            "Failed to inject Libfabric memory monitor: %s",
                             fi_strerror(-ret));
    }


    /**
     * Open fabric
     * The getinfo struct returns a fabric attribute struct that can be used to
     * instantiate the virtual or physical network. This opens a "fabric
     * provider". See man fi_fabric for details.
     */
    ret = fi_fabric(prov->fabric_attr,    /* In:  Fabric attributes             */
                    &ompi_mtl_ofi.fabric, /* Out: Fabric handle                 */
                    NULL);                /* Optional context for fabric events */
    if (0 != ret) {
        opal_show_help("help-mtl-ofi.txt", "OFI call fail", true,
                       "fi_fabric",
                       ompi_process_info.nodename, __FILE__, __LINE__,
                       fi_strerror(-ret), -ret);
        goto error;
    }

    /**
     * Unfortunately the attempt to implement FI_MR_SCALABLE in the GNI provider
     * doesn't work, at least not well.  Since we're asking for the 1.5 libfabric
     * API now, we have to tell GNI we want to use Mr. Basic.  Using FI_MR_BASIC
     * rather than FI_MR_VIRT_ADDR | FI_MR_ALLOCATED | FI_MR_PROV_KEY to stay
     * compatible with older libfabrics.
     */
    if (!strncmp(prov->fabric_attr->prov_name,"gni",3)) {
         prov->domain_attr->mr_mode = FI_MR_BASIC;
    }

    /**
     * Create the access domain, which is the physical or virtual network or
     * hardware port/collection of ports.  Returns a domain object that can be
     * used to create endpoints.  See man fi_domain for details.
     */
    ret = fi_domain(ompi_mtl_ofi.fabric,  /* In:  Fabric object                 */
                    prov,                 /* In:  Provider                      */
                    &ompi_mtl_ofi.domain, /* Out: Domain object                 */
                    NULL);                /* Optional context for domain events */
    if (0 != ret) {
        opal_show_help("help-mtl-ofi.txt", "OFI call fail", true,
                       "fi_domain",
                       ompi_process_info.nodename, __FILE__, __LINE__,
                       fi_strerror(-ret), -ret);
        goto error;
    }

    /**
     * Save the maximum sizes.
     */
    ompi_mtl_ofi.max_inject_size = prov->tx_attr->inject_size;
    ompi_mtl_ofi.max_msg_size = prov->ep_attr->max_msg_size;

    /**
     * The user is not allowed to exceed MTL_OFI_MAX_PROG_EVENT_COUNT.
     * The reason is because progress entries array is now a TLS variable
     * as opposed to being allocated on the heap for thread-safety purposes.
     */
    if (ompi_mtl_ofi.ofi_progress_event_count > MTL_OFI_MAX_PROG_EVENT_COUNT) {
        ompi_mtl_ofi.ofi_progress_event_count = MTL_OFI_MAX_PROG_EVENT_COUNT;
     }

    /**
     * Create a transport level communication endpoint.  To use the endpoint,
     * it must be bound to the resources consumed by it such as address
     * vectors, completion counters or event queues etc, and enabled.
     * See man fi_endpoint for more details.
     */

    /* use the universe size as a rough guess on the address vector
     * size hint that should be passed to fi_av_open().  For regular
     * endpoints, the count will be the universe size.  For scalable
     * endpoints, the count will be the universe size multiplied by
     * the number of contexts.  In either case, if the universe grows
     * (via dynamic processes), the count is a hint, not a hard limit,
     * so libfabric will just be slightly less efficient.
     */
    univ_size_str = getenv("OMPI_UNIVERSE_SIZE");
    if (NULL == univ_size_str ||
        (universe_size = strtol(univ_size_str, NULL, 0)) <= 0) {
        universe_size = ompi_proc_world_size();
    }

    if (1 == ompi_mtl_ofi.enable_sep) {
        max_ofi_ctxts = (prov->domain_attr->max_ep_tx_ctx <
                         prov->domain_attr->max_ep_rx_ctx) ?
                         prov->domain_attr->max_ep_tx_ctx :
                         prov->domain_attr->max_ep_rx_ctx;

        num_local_ranks = 1 + ompi_process_info.num_local_peers;
#if HAVE_LITHE
        /*
         * Hosted mode expands LOCAL_PEERS to logical vpids (P*K). SEP budget must
         * divide provider ctxts by OS processes on the node, not logical ranks —
         * otherwise max_ofi_ctxts collapses (e.g. 128/16=8) and vpid%RPH aliases
         * for K>8 (wrong Allreduce sums / hangs).
         */
        {
            unsigned long lith_rph = opal_lithe_env_cache_rph();
            int sep_budget_peers = num_local_ranks;
            if (lith_rph >= 2UL) {
                sep_budget_peers = (num_local_ranks + (int) lith_rph - 1) / (int) lith_rph;
                if (sep_budget_peers < 1) {
                    sep_budget_peers = 1;
                }
            }
            if (max_ofi_ctxts <= sep_budget_peers) {
                opal_show_help("help-mtl-ofi.txt", "Local ranks exceed ofi contexts",
                               true, prov->fabric_attr->prov_name,
                               ompi_process_info.nodename, __FILE__, __LINE__);
                goto error;
            }
            max_ofi_ctxts /= sep_budget_peers;
        }
#else
        if (max_ofi_ctxts <= num_local_ranks) {
            opal_show_help("help-mtl-ofi.txt", "Local ranks exceed ofi contexts",
                           true, prov->fabric_attr->prov_name,
                           ompi_process_info.nodename, __FILE__, __LINE__);
            goto error;
        }

        /* Provision enough contexts to service all ranks in a node */
        max_ofi_ctxts /= num_local_ranks;
#endif

        /*
         *  If num ctxts user specified is more than max allowed, limit to max
         *  and start round-robining. Print warning to user.
         */
        if (max_ofi_ctxts < ompi_mtl_ofi.num_ofi_contexts) {
            opal_show_help("help-mtl-ofi.txt", "Ctxts exceeded available",
                           true, max_ofi_ctxts,
                           ompi_process_info.nodename, __FILE__, __LINE__);
            ompi_mtl_ofi.num_ofi_contexts = max_ofi_ctxts;
        }

#if HAVE_LITHE
        /*
         * Hosted multicontext (LITHE_CONTEXT_RANKS_PER_HOST>=2): one scalable-endpoint
         * rx/tx pair per logical rank slot (vpid % RPH). Default num_ctxts=1 leaves
         * only ctxt 0 and breaks Irecv/tag matching for higher slots.
         */
        {
            unsigned long lith_rph = opal_lithe_env_cache_rph();
            if (lith_rph >= 2UL) {
                int want = (int) lith_rph;
                if (want > max_ofi_ctxts) {
                    want = max_ofi_ctxts;
                }
                if (want > ompi_mtl_ofi.num_ofi_contexts) {
                    ompi_mtl_ofi.num_ofi_contexts = want;
                }
            }
        }
#endif

#if HAVE_LITHE
        ompi_mtl_ofi.progress_block_enabled = true;
#else
        ompi_mtl_ofi.progress_block_enabled = false;
#endif

        ret = ompi_mtl_ofi_init_sep(prov, universe_size);
    } else {
#if HAVE_LITHE
        ompi_mtl_ofi.progress_block_enabled = true;
        {
            unsigned long lith_rph = opal_lithe_env_cache_rph();
            /* Multi regular-EP remains opt-in (LITHE_MTL_OFI_MULTI_EP=1).
             * Site cxi: no SEP (max_ep_*_ctx=1) but ep_cnt>>1 — one regular
             * EP+CQ per slot. Vanilla cxi same-process EP loopback works;
             * lithified stack still hangs residual OFI same-OS paths at P1K*
             * even with SC (pairwise SC OK, world Barrier/Allreduce hangs).
             * Skip MULTI_EP when LITHE_MTL_OFI_SINGLE_OS=1 (world==RPH): SC
             * alone is the composition path. MULTI_EP unlocks multi-OS: each
             * of K contexts progresses a distinct CQ toward remote peers. */
            const char *multi_env = getenv("LITHE_MTL_OFI_MULTI_EP");
            const char *single_os_env = getenv("LITHE_MTL_OFI_SINGLE_OS");
            int single_os = (NULL != single_os_env && single_os_env[0] == '1' &&
                             single_os_env[1] == '\0');
            int want_multi = (NULL != multi_env && multi_env[0] != '\0' &&
                              multi_env[0] != '0' && !single_os);
            if (want_multi && lith_rph >= 2UL && 0 == sep_support_in_provider) {
                int want = (int) lith_rph;
                size_t ep_cnt = prov->domain_attr->ep_cnt;
                if (ep_cnt > 0 && want > (int) ep_cnt) {
                    want = (int) ep_cnt;
                }
                ret = ompi_mtl_ofi_init_multi_regular_ep(prov, universe_size, want);
                /* Multi-EP: each slot has its own CQ wait_fd. Keep
                 * progress_block enabled so wait_sync parks on the *local*
                 * CQ fd (peer completions wake the peer context). Previously
                 * disabling block caused busy-spin / hart starvation hangs.
                 * Slot-in-tag is unnecessary once addresses differ — clear it. */
                if (OMPI_SUCCESS == ret) {
                    ompi_mtl_ofi.progress_block_enabled = true;
                    ompi_mtl_ofi.hosted_dst_in_cqd_tag = 0;
                    ompi_mtl_ofi.hosted_cqd_src_bits = 0;
                }
            } else {
                ompi_mtl_ofi.hosted_multi_ep = 0;
                ret = ompi_mtl_ofi_init_regular_ep(prov, universe_size);
            }
            /*
             * Shared regular EP (cxi no SEP): enable exclusive CQ progress
             * ownership when RPH>=2. Opt-out: LITHE_MTL_OFI_EXCL_PROGRESS=0.
             * MULTI_EP has per-slot CQs — ownership not required.
             */
            if (OMPI_SUCCESS == ret && 0 == ompi_mtl_ofi.hosted_multi_ep &&
                lith_rph >= 2UL) {
                const char *ex = getenv("LITHE_MTL_OFI_EXCL_PROGRESS");
                int want = 1;
                if (NULL != ex && '\0' != ex[0]) {
                    want = (ex[0] != '0');
                }
                ompi_mtl_ofi.shared_ep_excl_progress = want ? 1 : 0;
                if (want) {
                    ompi_mtl_ofi_shared_ep_excl_init();
                }
            }
        }
#else
        ompi_mtl_ofi.progress_block_enabled = false;
        ret = ompi_mtl_ofi_init_regular_ep(prov, universe_size);
#endif
    }

    if (OMPI_SUCCESS != ret) {
        goto error;
    }

    ompi_mtl_ofi.total_ctxts_used = 0;
    ompi_mtl_ofi.threshold_comm_context_id = 0;

    /* Enable Endpoint(s) for communication */
#if HAVE_LITHE
    if (ompi_mtl_ofi.hosted_multi_ep) {
        int epi;
        for (epi = 0; epi < ompi_mtl_ofi.num_ofi_contexts; ++epi) {
            ret = fi_enable(ompi_mtl_ofi.ofi_ctxt[epi].tx_ep);
            if (0 != ret) {
                MTL_OFI_LOG_FI_ERR(ret, "fi_enable failed");
                goto error;
            }
        }
    } else
#endif
    {
        ret = fi_enable(ompi_mtl_ofi.sep);
        if (0 != ret) {
            MTL_OFI_LOG_FI_ERR(ret, "fi_enable failed");
            goto error;
        }
    }

    ompi_mtl_ofi.provider_name = strdup(prov->fabric_attr->prov_name);

    /**
     * Free providers info since it's not needed anymore.
     */
    fi_freeinfo(hints);
    hints = NULL;
    fi_freeinfo(providers);
    providers = NULL;

#if HAVE_LITHE
    if (ompi_mtl_ofi.hosted_multi_ep) {
        /* Pack K EP names: magic(u32) + n(u32) + namelen(u32) + n*namelen bytes */
        uint32_t n_eps = (uint32_t) ompi_mtl_ofi.num_ofi_contexts;
        uint32_t name_len32 = 0;
        size_t blob_size, off;
        char *blob = NULL;
        void **names = calloc(n_eps, sizeof(void *));
        size_t *name_lens = calloc(n_eps, sizeof(size_t));
        uint32_t epi;

        if (NULL == names || NULL == name_lens) {
            free(names);
            free(name_lens);
            ret = OMPI_ERR_OUT_OF_RESOURCE;
            goto error;
        }
        for (epi = 0; epi < n_eps; ++epi) {
            ret = opal_common_ofi_fi_getname((fid_t) ompi_mtl_ofi.ofi_ctxt[epi].tx_ep,
                                             &names[epi], &name_lens[epi]);
            if (OMPI_SUCCESS != ret) {
                MTL_OFI_LOG_FI_ERR(ret, "opal_common_ofi_fi_getname failed");
                for (uint32_t j = 0; j < epi; ++j) {
                    free(names[j]);
                }
                free(names);
                free(name_lens);
                goto error;
            }
            if (0 == epi) {
                name_len32 = (uint32_t) name_lens[epi];
            } else if (name_lens[epi] != (size_t) name_len32) {
                opal_output(0, "%s:%d: hosted multi-EP name length mismatch\n",
                            __FILE__, __LINE__);
                for (uint32_t j = 0; j <= epi; ++j) {
                    free(names[j]);
                }
                free(names);
                free(name_lens);
                ret = OMPI_ERROR;
                goto error;
            }
        }
        blob_size = (size_t) (3 * sizeof(uint32_t)) + (size_t) n_eps * (size_t) name_len32;
        blob = malloc(blob_size);
        if (NULL == blob) {
            for (epi = 0; epi < n_eps; ++epi) {
                free(names[epi]);
            }
            free(names);
            free(name_lens);
            ret = OMPI_ERR_OUT_OF_RESOURCE;
            goto error;
        }
        off = 0;
        {
            uint32_t magic = 0x4c4d4550u; /* 'LMEP' */
            memcpy(blob + off, &magic, sizeof(magic));
            off += sizeof(magic);
            memcpy(blob + off, &n_eps, sizeof(n_eps));
            off += sizeof(n_eps);
            memcpy(blob + off, &name_len32, sizeof(name_len32));
            off += sizeof(name_len32);
        }
        for (epi = 0; epi < n_eps; ++epi) {
            memcpy(blob + off, names[epi], name_len32);
            off += name_len32;
            free(names[epi]);
        }
        free(names);
        free(name_lens);
        ep_name = blob;
        namelen = blob_size;
        ompi_mtl_ofi.epnamelen = (size_t) name_len32;
    } else
#endif
    {
        ret = opal_common_ofi_fi_getname((fid_t)ompi_mtl_ofi.sep,
                                         &ep_name,
                                         &namelen);
        if (OMPI_SUCCESS != ret) {
            MTL_OFI_LOG_FI_ERR(ret, "opal_common_ofi_fi_getname failed");
            goto error;
        }
        ompi_mtl_ofi.epnamelen = namelen;
    }

    OFI_COMPAT_MODEX_SEND(ret,
                          &mca_mtl_ofi_component.super.mtl_version,
                          &ep_name,
                          namelen);
    if (OMPI_SUCCESS != ret) {
        opal_output_verbose(1, opal_common_ofi.output,
                            "%s:%d: modex_send failed: %d\n",
                            __FILE__, __LINE__, ret);
        goto error;
    }

    free(ep_name);
    ep_name = NULL;

    /**
     * Set the ANY_SRC address.
     */
    ompi_mtl_ofi.any_addr = FI_ADDR_UNSPEC;
    ompi_mtl_ofi.is_initialized = false;
    ompi_mtl_ofi.has_posted_initial_buffer = false;
    
    ompi_mtl_ofi.base.mtl_flags |= MCA_MTL_BASE_FLAG_SUPPORTS_EXT_CID;

#if HAVE_LITHE
    ompi_mtl_ofi_prev_proc_local_hook = opal_proc_local_changed_hook;
    opal_proc_local_changed_hook = ompi_mtl_ofi_proc_local_changed_cb;
    ompi_mtl_ofi_proc_hook_installed = 1;
    (void) ompi_mtl_ofi_hosted_sc_init();
#endif

    return &ompi_mtl_ofi.base;

error:
    if (include_list) {
        opal_argv_free(include_list);
    }
    if (exclude_list) {
        opal_argv_free(exclude_list);
    }
    if (providers) {
        (void) fi_freeinfo(providers);
    }
    if (prov_cq_data) {
        (void) fi_freeinfo(prov_cq_data);
    }
    if (hints) {
        (void) fi_freeinfo(hints);
    }
    if (hints_dup) {
        (void) fi_freeinfo(hints_dup);
    }
    if (ompi_mtl_ofi.sep) {
        (void) fi_close((fid_t)ompi_mtl_ofi.sep);
    }
    if (ompi_mtl_ofi.av) {
        (void) fi_close((fid_t)ompi_mtl_ofi.av);
    }
    if ((0 == ompi_mtl_ofi.enable_sep) &&
        ompi_mtl_ofi.ofi_ctxt != NULL &&
         ompi_mtl_ofi.ofi_ctxt[0].cq) {
        /* Check if CQ[0] was created for non-SEP case and close if needed */
        (void) fi_close((fid_t)ompi_mtl_ofi.ofi_ctxt[0].cq);
    }
    if (ompi_mtl_ofi.domain) {
        (void) fi_close((fid_t)ompi_mtl_ofi.domain);
    }
    if (ompi_mtl_ofi.fabric) {
        (void) fi_close((fid_t)ompi_mtl_ofi.fabric);
    }
    if (ompi_mtl_ofi.comm_to_context) {
        free(ompi_mtl_ofi.comm_to_context);
    }
    if (ompi_mtl_ofi.ofi_ctxt) {
        free(ompi_mtl_ofi.ofi_ctxt);
    }
    if (ep_name) {
        free(ep_name);
    }

    return NULL;
}

int
ompi_mtl_ofi_finalize(struct mca_mtl_base_module_t *mtl)
{
    ssize_t ret;

    opal_progress_unregister(ompi_mtl_ofi_progress_no_inline);
#if HAVE_LITHE
    opal_progress_set_block_callback(NULL);
    opal_progress_set_sc_park_callback(NULL);
    ompi_mtl_ofi_hosted_sc_finalize();
#endif

#if HAVE_LITHE
    if (ompi_mtl_ofi.hosted_multi_ep) {
        int i;
        /* Close each regular EP then its CQ (EPs share the AV). */
        for (i = 0; i < ompi_mtl_ofi.num_ofi_contexts; ++i) {
            if (NULL != ompi_mtl_ofi.ofi_ctxt[i].tx_ep) {
                if ((ret = fi_close((fid_t) ompi_mtl_ofi.ofi_ctxt[i].tx_ep))) {
                    goto finalize_err;
                }
                ompi_mtl_ofi.ofi_ctxt[i].tx_ep = NULL;
                ompi_mtl_ofi.ofi_ctxt[i].rx_ep = NULL;
            }
            if (NULL != ompi_mtl_ofi.ofi_ctxt[i].cq) {
                if ((ret = fi_close((fid_t) ompi_mtl_ofi.ofi_ctxt[i].cq))) {
                    goto finalize_err;
                }
                ompi_mtl_ofi.ofi_ctxt[i].cq = NULL;
            }
        }
        ompi_mtl_ofi.sep = NULL;
    } else
#endif
    {
        /* Close all the OFI objects */
        if ((ret = fi_close((fid_t)ompi_mtl_ofi.sep))) {
            goto finalize_err;
        }

        if (0 == ompi_mtl_ofi.enable_sep) {
            /*
             * CQ[0] is bound to the EP when SEP is not supported by a
             * provider. OFI spec requires that we close the Endpoint that is bound
             * to the CQ before closing the CQ itself. So, for the non-SEP case, we
             * handle the closing of CQ[0] here.
             */
            if ((ret = fi_close((fid_t)ompi_mtl_ofi.ofi_ctxt[0].cq))) {
                goto finalize_err;
            }
        }
    }

    if ((ret = fi_close((fid_t)ompi_mtl_ofi.av))) {
        goto finalize_err;
    }

    if ((ret = fi_close((fid_t)ompi_mtl_ofi.domain))) {
        goto finalize_err;
    }

    if ((ret = fi_close((fid_t)ompi_mtl_ofi.fabric))) {
        goto finalize_err;
    }

    /* Free memory allocated for TX/RX contexts */
    free(ompi_mtl_ofi.comm_to_context);
    free(ompi_mtl_ofi.ofi_ctxt);

    return OMPI_SUCCESS;

finalize_err:
    opal_show_help("help-mtl-ofi.txt", "OFI call fail", true,
                   "fi_close",
                   ompi_process_info.nodename, __FILE__, __LINE__,
                   fi_strerror(-ret), -ret);

    return OMPI_ERROR;
}

/* Storage for the CQ holder diagnostic declared in mtl_ofi.h. */
mtl_ofi_cq_owner_t mtl_ofi_cq_owner_dbg[MTL_OFI_CQ_OWNER_MAX];
