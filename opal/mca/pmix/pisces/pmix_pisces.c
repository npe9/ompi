/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2016      Intel, Inc.  All rights reserved.
 * Copyright (c) 2011-2015 Los Alamos National Security, LLC. All
 *                         rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"
#include "opal/constants.h"
#include "opal/types.h"

#ifdef HAVE_STRING_H
#include <string.h>
#endif
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

#include "opal/dss/dss.h"
#include "opal/mca/event/event.h"
#include "opal/mca/hwloc/base/base.h"
#include "opal/runtime/opal.h"
#include "opal/runtime/opal_progress_threads.h"
#include "opal/util/argv.h"
#include "opal/util/error.h"
#include "opal/util/output.h"
#include "opal/util/proc.h"
#include "opal/util/show_help.h"

#include "pmix_pisces.h"
#include "opal/mca/pmix/base/base.h"
#include "opal/mca/pmix/base/pmix_base_hash.h"


static int pisces_init(void);
static int pisces_fini(void);
static int pisces_initialized(void);
static int pisces_abort(int flat, const char *msg,
                      opal_list_t *procs);
static int pisces_spawn(opal_list_t *jobinfo, opal_list_t *apps, opal_jobid_t *jobid);
static int pisces_spawn_nb(opal_list_t *jobinfo, opal_list_t *apps,
                         opal_pmix_spawn_cbfunc_t cbfunc,
                         void *cbdata);
static int pisces_job_connect(opal_list_t *procs);
static int pisces_job_disconnect(opal_list_t *procs);
static int pisces_job_disconnect_nb(opal_list_t *procs,
                                  opal_pmix_op_cbfunc_t cbfunc,
                                  void *cbdata);
static int pisces_resolve_peers(const char *nodename,
                              opal_jobid_t jobid,
                              opal_list_t *procs);
static int pisces_resolve_nodes(opal_jobid_t jobid, char **nodelist);
static int pisces_put(opal_pmix_scope_t scope, opal_value_t *kv);
static int pisces_fence(opal_list_t *procs, int collect_data);
static int pisces_fence_nb(opal_list_t *procs, int collect_data,
                         opal_pmix_op_cbfunc_t cbfunc, void *cbdata);
static int pisces_commit(void);
static int pisces_get(const opal_process_name_t *id,
                    const char *key, opal_list_t *info,
                    opal_value_t **kv);
static int pisces_get_nb(const opal_process_name_t *id, const char *key,
                       opal_list_t *info,
                       opal_pmix_value_cbfunc_t cbfunc, void *cbdata);
static int pisces_publish(opal_list_t *info);
static int pisces_publish_nb(opal_list_t *info,
                           opal_pmix_op_cbfunc_t cbfunc, void *cbdata);
static int pisces_lookup(opal_list_t *data, opal_list_t *info);
static int pisces_lookup_nb(char **keys, opal_list_t *info,
                          opal_pmix_lookup_cbfunc_t cbfunc, void *cbdata);
static int pisces_unpublish(char **keys, opal_list_t *info);
static int pisces_unpublish_nb(char **keys, opal_list_t *info,
                            opal_pmix_op_cbfunc_t cbfunc, void *cbdata);
static const char *pisces_get_version(void);
static int pisces_store_local(const opal_process_name_t *proc,
                          opal_value_t *val);
static const char *pisces_get_nspace(opal_jobid_t jobid);
static void pisces_register_jobid(opal_jobid_t jobid, const char *nspace);

const opal_pmix_base_module_t opal_pmix_pisces_module = {
    .init = pisces_init,
    .finalize = pisces_fini,
    .initialized = pisces_initialized,
    .abort = pisces_abort,
    .commit = pisces_commit,
    .fence = pisces_fence,
    .fence_nb = pisces_fence_nb,
    .put = pisces_put,
    .get = pisces_get,
    .get_nb = pisces_get_nb,
    .publish = pisces_publish,
    .publish_nb = pisces_publish_nb,
    .lookup = pisces_lookup,
    .lookup_nb = pisces_lookup_nb,
    .unpublish = pisces_unpublish,
    .unpublish_nb = pisces_unpublish_nb,
    .spawn = pisces_spawn,
    .spawn_nb = pisces_spawn_nb,
    .connect = pisces_job_connect,
    .disconnect = pisces_job_disconnect,
    .disconnect_nb = pisces_job_disconnect_nb,
    .resolve_peers = pisces_resolve_peers,
    .resolve_nodes = pisces_resolve_nodes,
    .get_version = pisces_get_version,
    .register_errhandler = opal_pmix_base_register_handler,
    .deregister_errhandler = opal_pmix_base_deregister_handler,
    .store_local = pisces_store_local,
    .get_nspace = pisces_get_nspace,
    .register_jobid = pisces_register_jobid
};

static int pisces_init_count = 0;
static opal_process_name_t pisces_pname;

static int pisces_init(void)
{
    int rc;
    opal_value_t kv;

    ++pisces_init_count;

    /* store our name in the opal_proc_t so that
     * debug messages will make sense - an upper
     * layer will eventually overwrite it, but that
     * won't do any harm */
    pisces_pname.jobid = 1;
    pisces_pname.vpid = 0;
    opal_proc_set_name(&pisces_pname);
    opal_output_verbose(10, opal_pmix_base_framework.framework_output,
                        "%s pmix:pisces: assigned tmp name %d %d",
                        OPAL_NAME_PRINT(pisces_pname),pisces_pname.jobid,pisces_pname.vpid);

    // setup hash table
    opal_pmix_base_hash_init();

    /* save the job size */
    OBJ_CONSTRUCT(&kv, opal_value_t);
    kv.key = strdup(OPAL_PMIX_JOB_SIZE);
    kv.type = OPAL_UINT32;
    kv.data.uint32 = 1;
    if (OPAL_SUCCESS != (rc = opal_pmix_base_store(&OPAL_PROC_MY_NAME, &kv))) {
        OPAL_ERROR_LOG(rc);
        OBJ_DESTRUCT(&kv);
        goto err_exit;
    }
    OBJ_DESTRUCT(&kv);

    /* save the appnum */
    OBJ_CONSTRUCT(&kv, opal_value_t);
    kv.key = strdup(OPAL_PMIX_APPNUM);
    kv.type = OPAL_UINT32;
    kv.data.uint32 = 0;
    if (OPAL_SUCCESS != (rc = opal_pmix_base_store(&OPAL_PROC_MY_NAME, &kv))) {
        OPAL_ERROR_LOG(rc);
        OBJ_DESTRUCT(&kv);
        goto err_exit;
    }
    OBJ_DESTRUCT(&kv);

    OBJ_CONSTRUCT(&kv, opal_value_t);
    kv.key = strdup(OPAL_PMIX_UNIV_SIZE);
    kv.type = OPAL_UINT32;
    kv.data.uint32 = 1;
    if (OPAL_SUCCESS != (rc = opal_pmix_base_store(&OPAL_PROC_MY_NAME, &kv))) {
        OPAL_ERROR_LOG(rc);
        OBJ_DESTRUCT(&kv);
        goto err_exit;
    }
    OBJ_DESTRUCT(&kv);

    OBJ_CONSTRUCT(&kv, opal_value_t);
    kv.key = strdup(OPAL_PMIX_JOBID);
    kv.type = OPAL_UINT32;
    kv.data.uint32 = 1;
    if (OPAL_SUCCESS != (rc = opal_pmix_base_store(&OPAL_PROC_MY_NAME, &kv))) {
        OPAL_ERROR_LOG(rc);
        OBJ_DESTRUCT(&kv);
        goto err_exit;
    }
    OBJ_DESTRUCT(&kv);

    /* save the local size */
    OBJ_CONSTRUCT(&kv, opal_value_t);
    kv.key = strdup(OPAL_PMIX_LOCAL_SIZE);
    kv.type = OPAL_UINT32;
    kv.data.uint32 = 1;
    if (OPAL_SUCCESS != (rc = opal_pmix_base_store(&OPAL_PROC_MY_NAME, &kv))) {
        OPAL_ERROR_LOG(rc);
        OBJ_DESTRUCT(&kv);
        goto err_exit;
    }
    OBJ_DESTRUCT(&kv);

    OBJ_CONSTRUCT(&kv, opal_value_t);
    kv.key = strdup(OPAL_PMIX_LOCAL_PEERS);
    kv.type = OPAL_STRING;
    kv.data.string = strdup("0");
    if (OPAL_SUCCESS != (rc = opal_pmix_base_store(&OPAL_PROC_MY_NAME, &kv))) {
        OPAL_ERROR_LOG(rc);
        OBJ_DESTRUCT(&kv);
        goto err_exit;
    }
    OBJ_DESTRUCT(&kv);

    /* save the local leader */
    OBJ_CONSTRUCT(&kv, opal_value_t);
    kv.key = strdup(OPAL_PMIX_LOCALLDR);
    kv.type = OPAL_UINT64;
    kv.data.uint64 = 0;
    if (OPAL_SUCCESS != (rc = opal_pmix_base_store(&OPAL_PROC_MY_NAME, &kv))) {
        OPAL_ERROR_LOG(rc);
        OBJ_DESTRUCT(&kv);
        goto err_exit;
    }

    /* save our local rank */
    OBJ_CONSTRUCT(&kv, opal_value_t);
    kv.key = strdup(OPAL_PMIX_LOCAL_RANK);
    kv.type = OPAL_UINT16;
    kv.data.uint16 = 0;
    if (OPAL_SUCCESS != (rc = opal_pmix_base_store(&OPAL_PROC_MY_NAME, &kv))) {
        OPAL_ERROR_LOG(rc);
        OBJ_DESTRUCT(&kv);
        goto err_exit;
    }

    /* and our node rank */
    OBJ_CONSTRUCT(&kv, opal_value_t);
    kv.key = strdup(OPAL_PMIX_NODE_RANK);
    kv.type = OPAL_UINT16;
    kv.data.uint16 = 0;
    if (OPAL_SUCCESS != (rc = opal_pmix_base_store(&OPAL_PROC_MY_NAME, &kv))) {
        OPAL_ERROR_LOG(rc);
        OBJ_DESTRUCT(&kv);
        goto err_exit;
    }
    OBJ_DESTRUCT(&kv);

    return OPAL_SUCCESS;

err_exit:
    return rc;
}

static int pisces_fini(void)
{
    if (0 == pisces_init_count) {
        return OPAL_SUCCESS;
    }

    if (0 != --pisces_init_count) {
        return OPAL_SUCCESS;
    }
    opal_pmix_base_hash_finalize();
    return OPAL_SUCCESS;
}

static int pisces_initialized(void)
{
    if (0 < pisces_init_count) {
        return 1;
    }
    return 0;
}

static int pisces_abort(int flag, const char *msg,
                      opal_list_t *procs)
{
    return OPAL_SUCCESS;
}

static int pisces_spawn(opal_list_t *jobinfo, opal_list_t *apps, opal_jobid_t *jobid)
{
    return OPAL_ERR_NOT_SUPPORTED;
}

static int pisces_spawn_nb(opal_list_t *jobinfo, opal_list_t *apps,
                         opal_pmix_spawn_cbfunc_t cbfunc,
                         void *cbdata)
{
    return OPAL_ERR_NOT_SUPPORTED;
}

static int pisces_job_connect(opal_list_t *procs)
{
    return OPAL_ERR_NOT_SUPPORTED;
}

static int pisces_job_disconnect(opal_list_t *procs)
{
    return OPAL_ERR_NOT_SUPPORTED;
}

static int pisces_job_disconnect_nb(opal_list_t *procs,
                                  opal_pmix_op_cbfunc_t cbfunc,
                                  void *cbdata)
{
    return OPAL_ERR_NOT_SUPPORTED;
}

static int pisces_resolve_peers(const char *nodename,
                              opal_jobid_t jobid,
                              opal_list_t *procs)
{
    return OPAL_ERR_NOT_IMPLEMENTED;
}

static int pisces_resolve_nodes(opal_jobid_t jobid, char **nodelist)
{
    return OPAL_ERR_NOT_IMPLEMENTED;
}

static int pisces_put(opal_pmix_scope_t scope,
                  opal_value_t *kv)
{
    int rc;

    opal_output_verbose(10, opal_pmix_base_framework.framework_output,
                        "%s pmix:pisces pisces_put key %s scope %d\n",
                         OPAL_NAME_PRINT(OPAL_PROC_MY_NAME), kv->key, scope);

    if (!pisces_init_count) {
        return OPAL_ERROR;
    }

    rc = opal_pmix_base_store(&pisces_pname, kv);

    return rc;
}

static int pisces_commit(void)
{
    return OPAL_SUCCESS;
}

static int pisces_fence(opal_list_t *procs, int collect_data)
{
    return OPAL_SUCCESS;
}

static int pisces_fence_nb(opal_list_t *procs, int collect_data,
                         opal_pmix_op_cbfunc_t cbfunc, void *cbdata)
{
    return OPAL_ERR_NOT_IMPLEMENTED;
}

static int pisces_get(const opal_process_name_t *id,
                    const char *key, opal_list_t *info,
                    opal_value_t **kv)
{
    int rc;
    opal_list_t vals;

    opal_output_verbose(2, opal_pmix_base_framework.framework_output,
                        "%s pmix:pisces getting value for proc %s key %s",
                        OPAL_NAME_PRINT(OPAL_PROC_MY_NAME),
                        OPAL_NAME_PRINT(*id), key);

    OBJ_CONSTRUCT(&vals, opal_list_t);
    rc = opal_pmix_base_fetch(id, key, &vals);
    if (OPAL_SUCCESS == rc) {
        *kv = (opal_value_t*)opal_list_remove_first(&vals);
        return OPAL_SUCCESS;
    } else {
        opal_output_verbose(2, opal_pmix_base_framework.framework_output,
                "%s pmix:pisces fetch from dstore failed: %d",
                OPAL_NAME_PRINT(OPAL_PROC_MY_NAME), rc);
    }
    OPAL_LIST_DESTRUCT(&vals);

    return rc;
}
static int pisces_get_nb(const opal_process_name_t *id, const char *key,
                       opal_list_t *info, opal_pmix_value_cbfunc_t cbfunc, void *cbdata)
{
    return OPAL_ERR_NOT_IMPLEMENTED;
}

static int pisces_publish(opal_list_t *info)
{
    return OPAL_ERR_NOT_SUPPORTED;
}

static int pisces_publish_nb(opal_list_t *info,
                           opal_pmix_op_cbfunc_t cbfunc, void *cbdata)
{
    return OPAL_ERR_NOT_SUPPORTED;
}

static int pisces_lookup(opal_list_t *data, opal_list_t *info)
{
    return OPAL_ERR_NOT_SUPPORTED;
}

static int pisces_lookup_nb(char **keys, opal_list_t *info,
                          opal_pmix_lookup_cbfunc_t cbfunc, void *cbdata)
{
    return OPAL_ERR_NOT_SUPPORTED;
}

static int pisces_unpublish(char **keys, opal_list_t *info)
{
    return OPAL_ERR_NOT_SUPPORTED;
}

static int pisces_unpublish_nb(char **keys, opal_list_t *info,
                            opal_pmix_op_cbfunc_t cbfunc, void *cbdata)
{
    return OPAL_ERR_NOT_SUPPORTED;
}

static const char *pisces_get_version(void)
{
    return "N/A";
}

static int pisces_store_local(const opal_process_name_t *proc,
                          opal_value_t *val)
{
    opal_pmix_base_store(proc, val);

    return OPAL_SUCCESS;
}

static const char *pisces_get_nspace(opal_jobid_t jobid)
{
    return "N/A";
}

static void pisces_register_jobid(opal_jobid_t jobid, const char *nspace)
{
    return;
}

