/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */ /*
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

#include "pmix_xpmem.h"
#include "pmix_xpmem_hash.h"
#include "opal/mca/pmix/base/base.h"


static int xpmem_init(void);
static int xpmem_fini(void);
static int xpmem_initialized(void);
static int xpmem_abort(int flat, const char *msg,
                      opal_list_t *procs);
static int xpmem_spawn(opal_list_t *jobinfo, opal_list_t *apps, opal_jobid_t *jobid);
static int xpmem_spawn_nb(opal_list_t *jobinfo, opal_list_t *apps,
                         opal_pmix_spawn_cbfunc_t cbfunc,
                         void *cbdata);
static int xpmem_job_connect(opal_list_t *procs);
static int xpmem_job_disconnect(opal_list_t *procs);
static int xpmem_job_disconnect_nb(opal_list_t *procs,
                                  opal_pmix_op_cbfunc_t cbfunc,
                                  void *cbdata);
static int xpmem_resolve_peers(const char *nodename,
                              opal_jobid_t jobid,
                              opal_list_t *procs);
static int xpmem_resolve_nodes(opal_jobid_t jobid, char **nodelist);
static int xpmem_put(opal_pmix_scope_t scope, opal_value_t *kv);
static int xpmem_fence(opal_list_t *procs, int collect_data);
static int xpmem_fence_nb(opal_list_t *procs, int collect_data,
                         opal_pmix_op_cbfunc_t cbfunc, void *cbdata);
static int xpmem_commit(void);
static int xpmem_get(const opal_process_name_t *id,
                    const char *key, opal_list_t *info,
                    opal_value_t **kv);
static int xpmem_get_nb(const opal_process_name_t *id, const char *key,
                       opal_list_t *info,
                       opal_pmix_value_cbfunc_t cbfunc, void *cbdata);
static int xpmem_publish(opal_list_t *info);
static int xpmem_publish_nb(opal_list_t *info,
                           opal_pmix_op_cbfunc_t cbfunc, void *cbdata);
static int xpmem_lookup(opal_list_t *data, opal_list_t *info);
static int xpmem_lookup_nb(char **keys, opal_list_t *info,
                          opal_pmix_lookup_cbfunc_t cbfunc, void *cbdata);
static int xpmem_unpublish(char **keys, opal_list_t *info);
static int xpmem_unpublish_nb(char **keys, opal_list_t *info,
                            opal_pmix_op_cbfunc_t cbfunc, void *cbdata);
static const char *xpmem_get_version(void);
static int xpmem_store_local(const opal_process_name_t *proc,
                          opal_value_t *val);
static const char *xpmem_get_nspace(opal_jobid_t jobid);
static void xpmem_register_jobid(opal_jobid_t jobid, const char *nspace);

const opal_pmix_base_module_t opal_pmix_xpmem_module = {
    .init = xpmem_init,
    .finalize = xpmem_fini,
    .initialized = xpmem_initialized,
    .abort = xpmem_abort,
    .commit = xpmem_commit,
    .fence = xpmem_fence,
    .fence_nb = xpmem_fence_nb,
    .put = xpmem_put,
    .get = xpmem_get,
    .get_nb = xpmem_get_nb,
    .publish = xpmem_publish,
    .publish_nb = xpmem_publish_nb,
    .lookup = xpmem_lookup,
    .lookup_nb = xpmem_lookup_nb,
    .unpublish = xpmem_unpublish,
    .unpublish_nb = xpmem_unpublish_nb,
    .spawn = xpmem_spawn,
    .spawn_nb = xpmem_spawn_nb,
    .connect = xpmem_job_connect,
    .disconnect = xpmem_job_disconnect,
    .disconnect_nb = xpmem_job_disconnect_nb,
    .resolve_peers = xpmem_resolve_peers,
    .resolve_nodes = xpmem_resolve_nodes,
    .get_version = xpmem_get_version,
    .register_errhandler = opal_pmix_base_register_handler,
    .deregister_errhandler = opal_pmix_base_deregister_handler,
    .store_local = xpmem_store_local,
    .get_nspace = xpmem_get_nspace,
    .register_jobid = xpmem_register_jobid
};

static int xpmem_init_count = 0;
static opal_process_name_t xpmem_pname;
static opal_proc_table_t *ptable;
static opal_proc_table_t ptable_loc;
static opal_condition_t *mca_pmix_xpmem_condition;
static opal_condition_t *mca_pmix_xpmem_condition2;
static opal_condition_t mca_pmix_xpmem_condition_loc;
static opal_condition_t mca_pmix_xpmem_condition_loc2;
static int *var;
static int var_loc;
static int *var2;
static int var2_loc;

static opal_mutex_t *mutex;
static opal_mutex_t mutex_loc;

// need to set up hash table
// init needs to set up the hash table 
static int xpmem_init(void)
{
    int rc, spawned, rank;
    opal_value_t kv;
    char *rankstr;

    ++xpmem_init_count;

    printf("%s: initing\n", __func__);
    /*
      we don't have ranks yet.
      so that is interesting.
      well yes we do thanks to kitten.
      So what we can do here is load the kitten rank.
      This defeats the puporse of PMI though.
      That is fine. We already know where this must be.

     */
    /* so I don't initialize unless I'm rank 0 */

    /* store our name in the opal_proc_t so that
     * debug messages will make sense - an upper
     * layer will eventually overwrite it, but that
     * won't do any harm */
    xpmem_pname.jobid = 1;
    xpmem_pname.vpid = 0;
    opal_proc_set_name(&xpmem_pname);
    opal_output_verbose(10, opal_pmix_base_framework.framework_output,
                        "%s pmix:xpmem: assigned tmp name %d %d",
                        OPAL_NAME_PRINT(xpmem_pname),xpmem_pname.jobid,xpmem_pname.vpid);

    // setup hash table, not that this differs from the isolated initialization because we have a shared memory hash across address spaces.
    rankstr = getenv("PMI_RANK");
    if(rankstr == NULL)
        goto err_exit;
    ptable = (opal_proc_table_t *)(0x8000000000UL + (uint64_t)&ptable_loc);
    mutex = (opal_mutex_t *)(0x8000000000UL + (uint64_t)&mutex_loc);
    mca_pmix_xpmem_condition = (opal_condition_t *)(0x8000000000UL + (uint64_t)&mca_pmix_xpmem_condition_loc);
    mca_pmix_xpmem_condition2 = (opal_condition_t *)(0x8000000000UL + (uint64_t)&mca_pmix_xpmem_condition_loc2);
    var = (opal_proc_table_t *)(0x8000000000UL + (uint64_t)&var_loc);
    var2 = (opal_proc_table_t *)(0x8000000000UL + (uint64_t)&var2_loc);

    rank = atoi(rankstr);
    if(rank == 0){
        //ptable = &ptable_loc;
        //mutex = &mutex_loc;
        OBJ_CONSTRUCT(mutex, opal_mutex_t);
        //mca_pmix_xpmem_condition = &mca_pmix_xpmem_condition_loc;
        OBJ_CONSTRUCT(mca_pmix_xpmem_condition, opal_condition_t);
        //mca_pmix_xpmem_condition2 = &mca_pmix_xpmem_condition_loc2;
        OBJ_CONSTRUCT(mca_pmix_xpmem_condition2, opal_condition_t);
        //var = &var_loc;
        *var = 0xdeadbeef;
        //var2 = &var2_loc;
        OPAL_THREAD_LOCK(mutex);
        while(!var2 || *var2 == 0)
            *var = 0xdeadbeef;
        printf("%s: *var %x *var2 %x\n", __func__, *var, *var2);
        printf("%s: %d: signaling on condition variable %p\n", __func__, rank, mca_pmix_xpmem_condition2);
        opal_condition_signal(mca_pmix_xpmem_condition2);
        printf("%s: %d: waiting on condition variable %p\n", __func__, rank, mca_pmix_xpmem_condition);
        //opal_condition_wait(mca_pmix_xpmem_condition, mutex);
        printf("%s: %d: waited on condition variable %p\n", __func__, rank, mca_pmix_xpmem_condition);
        OPAL_THREAD_UNLOCK(mutex);
    }else{
        // in kitten the first smartmap location is: 0x8000000000UL
        /*
        ptable = (opal_proc_table_t *)(0x8000000000UL + (uint64_t)&ptable_loc);
        mutex = (opal_mutex_t *)(0x8000000000UL + (uint64_t)&mutex_loc);
        mca_pmix_xpmem_condition = (opal_condition_t *)(0x8000000000UL + (uint64_t)&mca_pmix_xpmem_condition_loc);
        mca_pmix_xpmem_condition2 = (opal_condition_t *)(0x8000000000UL + (uint64_t)&mca_pmix_xpmem_condition_loc2);
        var = (opal_proc_table_t *)(0x8000000000UL + (uint64_t)&var_loc);
        var2 = (opal_proc_table_t *)(0x8000000000UL + (uint64_t)&var2_loc);
        */
        *var2 = 0xba5eba77;
        printf("%s: %d: locking\n", __func__, rank);
        OPAL_THREAD_LOCK(mutex);
        while(!var || *var == 0)
            *var2 = 0xba5eba77;
        printf("%s: *var %x *var2 %x\n", __func__, *var, *var2);
        printf("%s: %d: waiting on condition variable %p\n", __func__, rank, mca_pmix_xpmem_condition2);
        //opal_condition_wait(mca_pmix_xpmem_condition2, mutex);
        printf("%s: %d: waited on condition variable %p\n", __func__, rank, mca_pmix_xpmem_condition2);
        printf("%s: %d: signaling on condition variable %p\n", __func__, rank, mca_pmix_xpmem_condition);
        opal_condition_signal(mca_pmix_xpmem_condition);

        OPAL_THREAD_UNLOCK(mutex);
    }

    pmix_xpmem_hash_init(ptable);

    printf("%s: finished initializing table\n", __func__);
    /* save the job size */
    OBJ_CONSTRUCT(&kv, opal_value_t);
    kv.key = strdup(OPAL_PMIX_JOB_SIZE);
    kv.type = OPAL_UINT32;
    kv.data.uint32 = 1;
    if (OPAL_SUCCESS != (rc = pmix_xpmem_store(ptable, &OPAL_PROC_MY_NAME, &kv))) {
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
    if (OPAL_SUCCESS != (rc = pmix_xpmem_store(ptable, &OPAL_PROC_MY_NAME, &kv))) {
        OPAL_ERROR_LOG(rc);
        OBJ_DESTRUCT(&kv);
        goto err_exit;
    }
    OBJ_DESTRUCT(&kv);

    OBJ_CONSTRUCT(&kv, opal_value_t);
    kv.key = strdup(OPAL_PMIX_UNIV_SIZE);
    kv.type = OPAL_UINT32;
    kv.data.uint32 = 1;
    if (OPAL_SUCCESS != (rc = pmix_xpmem_store(ptable, &OPAL_PROC_MY_NAME, &kv))) {
        OPAL_ERROR_LOG(rc);
        OBJ_DESTRUCT(&kv);
        goto err_exit;
    }
    OBJ_DESTRUCT(&kv);

    OBJ_CONSTRUCT(&kv, opal_value_t);
    kv.key = strdup(OPAL_PMIX_JOBID);
    kv.type = OPAL_UINT32;
    kv.data.uint32 = 1;
    if (OPAL_SUCCESS != (rc = pmix_xpmem_store(ptable, &OPAL_PROC_MY_NAME, &kv))) {
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
    if (OPAL_SUCCESS != (rc = pmix_xpmem_store(ptable, &OPAL_PROC_MY_NAME, &kv))) {
        OPAL_ERROR_LOG(rc);
        OBJ_DESTRUCT(&kv);
        goto err_exit;
    }
    OBJ_DESTRUCT(&kv);

    OBJ_CONSTRUCT(&kv, opal_value_t);
    kv.key = strdup(OPAL_PMIX_LOCAL_PEERS);
    kv.type = OPAL_STRING;
    kv.data.string = strdup("0");
    if (OPAL_SUCCESS != (rc = pmix_xpmem_store(ptable, &OPAL_PROC_MY_NAME, &kv))) {
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
    if (OPAL_SUCCESS != (rc = pmix_xpmem_store(ptable, &OPAL_PROC_MY_NAME, &kv))) {
        OPAL_ERROR_LOG(rc);
        OBJ_DESTRUCT(&kv);
        goto err_exit;
    }

    /* save our local rank */
    OBJ_CONSTRUCT(&kv, opal_value_t);
    kv.key = strdup(OPAL_PMIX_LOCAL_RANK);
    kv.type = OPAL_UINT16;
    kv.data.uint16 = 0;
    if (OPAL_SUCCESS != (rc = pmix_xpmem_store(ptable, &OPAL_PROC_MY_NAME, &kv))) {
        OPAL_ERROR_LOG(rc);
        OBJ_DESTRUCT(&kv);
        goto err_exit;
    }

    /* and our node rank */
    OBJ_CONSTRUCT(&kv, opal_value_t);
    kv.key = strdup(OPAL_PMIX_NODE_RANK);
    kv.type = OPAL_UINT16;
    kv.data.uint16 = 0;
    if (OPAL_SUCCESS != (rc = pmix_xpmem_store(ptable, &OPAL_PROC_MY_NAME, &kv))) {
        OPAL_ERROR_LOG(rc);
        OBJ_DESTRUCT(&kv);
        goto err_exit;
    }
    OBJ_DESTRUCT(&kv);
    printf("%s: succeeded continuing\n", __func__);
    return OPAL_SUCCESS;

err_exit:
    return rc;
}

static int xpmem_fini(void)
{
    printf("%s: finalizing\n", __func__);
    if (0 == xpmem_init_count) {
        return OPAL_SUCCESS;
    }

    if (0 != --xpmem_init_count) {
        return OPAL_SUCCESS;
    }
    pmix_xpmem_hash_finalize(ptable);
    free(ptable);
    return OPAL_SUCCESS;
}

static int xpmem_initialized(void)
{
    printf("%s: \n", __func__);
    if (0 < xpmem_init_count) {
        return 1;
    }
    return 0;
}

static int xpmem_abort(int flag, const char *msg,
                      opal_list_t *procs)
{
    printf("%s: aborting\n", __func__);
    return OPAL_SUCCESS;
}

static int xpmem_spawn(opal_list_t *jobinfo, opal_list_t *apps, opal_jobid_t *jobid)
{
    printf("%s: spawning\n", __func__);
    return OPAL_ERR_NOT_SUPPORTED;
}

static int xpmem_spawn_nb(opal_list_t *jobinfo, opal_list_t *apps,
                         opal_pmix_spawn_cbfunc_t cbfunc,
                         void *cbdata)
{
    printf("%s: spawn_nbing\n", __func__);
    return OPAL_ERR_NOT_SUPPORTED;
}

static int xpmem_job_connect(opal_list_t *procs)
{
    printf("%s: job connecting\n", __func__);
    return OPAL_ERR_NOT_SUPPORTED;
}

static int xpmem_job_disconnect(opal_list_t *procs)
{
    printf("%s: disconnecting\n", __func__);
    return OPAL_ERR_NOT_SUPPORTED;
}

static int xpmem_job_disconnect_nb(opal_list_t *procs,
                                  opal_pmix_op_cbfunc_t cbfunc,
                                  void *cbdata)
{
    printf("%s: disconnecting\n", __func__);

    return OPAL_ERR_NOT_SUPPORTED;
}

static int xpmem_resolve_peers(const char *nodename,
                              opal_jobid_t jobid,
                              opal_list_t *procs)
{
    printf("%s: \n", __func__);
    return OPAL_ERR_NOT_IMPLEMENTED;
}

static int xpmem_resolve_nodes(opal_jobid_t jobid, char **nodelist)
{
    printf("%s: resolving \n", __func__);
    return OPAL_ERR_NOT_IMPLEMENTED;
}

static int xpmem_put(opal_pmix_scope_t scope,
                  opal_value_t *kv)
{
    int rc;

    printf("%s: putting\n", __func__);
    opal_output_verbose(10, opal_pmix_base_framework.framework_output,
                        "%s pmix:xpmem xpmem_put key %s scope %d\n",
                         OPAL_NAME_PRINT(OPAL_PROC_MY_NAME), kv->key, scope);

    if (!xpmem_init_count) {
        printf("%s: init_count = 0\n", __func__);
        return OPAL_ERROR;
    }
    //if(PMI_KVS_Put(&xpmem_pname, kv->key, kv->val) == PMI_FAIL)
    //    return OPAL_FAIL;
    rc = pmix_xpmem_store(ptable, &xpmem_pname, kv);

    return rc;
}

static int xpmem_commit(void)
{
    printf("%s: commiting\n", __func__);
    return OPAL_SUCCESS;
}

static int xpmem_fence(opal_list_t *procs, int collect_data)
{
    printf("%s: fencing\n", __func__);
    sleep(1);
    return OPAL_SUCCESS;
}

static int xpmem_fence_nb(opal_list_t *procs, int collect_data,
                         opal_pmix_op_cbfunc_t cbfunc, void *cbdata)
{
    printf("%s: fencing non block\n", __func__);

    sleep(5);
    return OPAL_ERR_NOT_IMPLEMENTED;
}

static int xpmem_get(const opal_process_name_t *id,
                    const char *key, opal_list_t *info,
                    opal_value_t **kv)
{
    int rc;
    opal_list_t vals;

    printf("%s: %p getting %s\n", __func__, id, key);
    //printf("%s pmix:xpmem getting value for proc %s key %s",
    //                    OPAL_NAME_PRINT(OPAL_PROC_MY_NAME),
    //                    OPAL_NAME_PRINT(*id), key);


    opal_output_verbose(2, opal_pmix_base_framework.framework_output,
                        "%s pmix:xpmem getting value for proc %s key %s",
                        OPAL_NAME_PRINT(OPAL_PROC_MY_NAME),
                        OPAL_NAME_PRINT(*id), key);

    OBJ_CONSTRUCT(&vals, opal_list_t);
    printf("%s: fetching from %p\n", __func__, ptable);
    OPAL_THREAD_LOCK(mutex);
    rc = pmix_xpmem_fetch(ptable, id, key, &vals);
    OPAL_THREAD_UNLOCK(mutex);
    printf("%s: fetch results %d\n", __func__, rc);
    if (OPAL_SUCCESS == rc) {
        *kv = (opal_value_t*)opal_list_remove_first(&vals);
        return OPAL_SUCCESS;
    } else {
        opal_output_verbose(2, opal_pmix_base_framework.framework_output,
                "%s pmix:xpmem fetch from dstore failed: %d",
                OPAL_NAME_PRINT(OPAL_PROC_MY_NAME), rc);
    }
    OPAL_LIST_DESTRUCT(&vals);

    return rc;
}
static int xpmem_get_nb(const opal_process_name_t *id, const char *key,
                       opal_list_t *info, opal_pmix_value_cbfunc_t cbfunc, void *cbdata)
{
    printf("%s: getting notblock\n", __func__);
    return OPAL_ERR_NOT_IMPLEMENTED;
}

static int xpmem_publish(opal_list_t *info)
{
    printf("%s: putting\n", __func__);
 
    return OPAL_ERR_NOT_SUPPORTED;
}

static int xpmem_publish_nb(opal_list_t *info,
                           opal_pmix_op_cbfunc_t cbfunc, void *cbdata)
{
    printf("%s: publishing nonblocking\n", __func__);

    return OPAL_ERR_NOT_SUPPORTED;
}

static int xpmem_lookup(opal_list_t *data, opal_list_t *info)
{
    printf("%s: looking up\n", __func__);
    return OPAL_ERR_NOT_SUPPORTED;
}

static int xpmem_lookup_nb(char **keys, opal_list_t *info,
                          opal_pmix_lookup_cbfunc_t cbfunc, void *cbdata)
{
    printf("%s: lookup nonblocking\n", __func__);

    return OPAL_ERR_NOT_SUPPORTED;
}

static int xpmem_unpublish(char **keys, opal_list_t *info)
{
    printf("%s: unpublishing\n", __func__);

    return OPAL_ERR_NOT_SUPPORTED;
}

static int xpmem_unpublish_nb(char **keys, opal_list_t *info,
                            opal_pmix_op_cbfunc_t cbfunc, void *cbdata)
{
    printf("%s: publishing nonblocking\n", __func__);

    return OPAL_ERR_NOT_SUPPORTED;
}

static const char *xpmem_get_version(void)
{
    printf("%s: getting version\n", __func__);
    return "N/A";
}

static int xpmem_store_local(const opal_process_name_t *proc,
                          opal_value_t *val)
{
    printf("%s: storing local\n", __func__);
    OPAL_THREAD_LOCK(mutex);
    pmix_xpmem_store(ptable, proc, val);
    OPAL_THREAD_UNLOCK(mutex);

    return OPAL_SUCCESS;
}

static const char *xpmem_get_nspace(opal_jobid_t jobid)
{
    printf("%s: getting nspace\n", __func__);

    return "N/A";
}

static void xpmem_register_jobid(opal_jobid_t jobid, const char *nspace)
{
    printf("%s: registering jobid\n", __func__);

    return;
}

