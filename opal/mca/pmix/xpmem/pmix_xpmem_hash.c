/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2010      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2004-2011 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2011-2014 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2014-2015 Intel, Inc. All rights reserved.
 * Copyright (c) 2014      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 */

#include "opal_config.h"
#include "opal/constants.h"

#include <time.h>
#include <string.h>

#include "opal_stdint.h"
#include "opal/class/opal_hash_table.h"
#include "opal/class/opal_pointer_array.h"
#include "opal/dss/dss_types.h"
#include "opal/util/error.h"
#include "opal/util/output.h"
#include "opal/util/proc.h"
#include "opal/util/show_help.h"

#include "opal/mca/pmix/base/base.h"
#include "opal/mca/pmix/xpmem/pmix_xpmem_hash.h"

static void proc_data_construct(pmix_xpmem_proc_data_t *ptr)
{
    ptr->loaded = false;
    OBJ_CONSTRUCT(&ptr->data, opal_list_t);
}

static void proc_data_destruct(pmix_xpmem_proc_data_t *ptr)
{
    OPAL_LIST_DESTRUCT(&ptr->data);
}
OBJ_CLASS_INSTANCE(pmix_xpmem_proc_data_t,
                   opal_list_item_t,
                   proc_data_construct,
                   proc_data_destruct);

/**
 * Find data for a given key in a given proc_data_t
 * container.
 */
static opal_value_t* lookup_keyval(pmix_xpmem_proc_data_t *proc_data,
                                   const char *key)
{
    opal_value_t *kv;

    OPAL_LIST_FOREACH(kv, &proc_data->data, opal_value_t) {
        if (0 == strcmp(key, kv->key)) {
            return kv;
        }
    }
    return NULL;
}

/**
 * Find proc_data_t container associated with given
 * opal_process_name_t.
 */
static pmix_xpmem_proc_data_t* lookup_proc(opal_proc_table_t *pt,
                                          opal_process_name_t id, bool create)
{
    pmix_xpmem_proc_data_t *proc_data = NULL;

    opal_proc_table_get_value(pt, id, (void**)&proc_data);
    if (NULL == proc_data && create) {
        proc_data = OBJ_NEW(pmix_xpmem_proc_data_t);
        if (NULL == proc_data) {
            opal_output(0, "pmix:hash:lookup_proc: unable to allocate proc_data_t\n");
            return NULL;
        }
        opal_proc_table_set_value(pt, id, proc_data);
    }

    return proc_data;
}


//static opal_proc_table_t ptable;

/* Initialize our hash table */
void pmix_xpmem_hash_init(opal_proc_table_t *pt)
{
    OBJ_CONSTRUCT(pt, opal_proc_table_t);
    opal_proc_table_init(&pt, 16, 256);
}

void pmix_xpmem_hash_finalize(opal_proc_table_t *pt)
{
    pmix_xpmem_proc_data_t *proc_data;
    opal_process_name_t key;
    void *node1, *node2;

    /* to assist in getting a clean valgrind, cycle thru the hash table
     * and release all data stored in it
     */
    if (OPAL_SUCCESS == opal_proc_table_get_first_key(pt, &key,
                                                      (void**)&proc_data,
                                                      &node1, &node2)) {
        if (NULL != proc_data) {
            OBJ_RELEASE(proc_data);
        }
        while (OPAL_SUCCESS == opal_proc_table_get_next_key(pt, &key,
                                                            (void**)&proc_data,
                                                            node1, &node1,
                                                            node2, &node2)) {
            if (NULL != proc_data) {
                OBJ_RELEASE(proc_data);
            }
        }
    }
    OBJ_DESTRUCT(pt);
}



int pmix_xpmem_store(opal_proc_table_t *pt, const opal_process_name_t *id,
                         opal_value_t *val)
{
    pmix_xpmem_proc_data_t *proc_data;
    opal_value_t *kv;
    int rc;

    opal_output_verbose(1, opal_pmix_base_framework.framework_output,
                        "%s pmix:hash:store storing data for proc %s",
                        OPAL_NAME_PRINT(OPAL_PROC_MY_NAME), OPAL_NAME_PRINT(*id));

    /* lookup the proc data object for this proc */
    if (NULL == (proc_data = lookup_proc(pt, *id, true))) {
        /* unrecoverable error */
        OPAL_OUTPUT_VERBOSE((5, opal_pmix_base_framework.framework_output,
                             "%s pmix:hash:store: storing data for proc %s unrecoverably failed",
                             OPAL_NAME_PRINT(OPAL_PROC_MY_NAME), OPAL_NAME_PRINT(*id)));
        return OPAL_ERR_OUT_OF_RESOURCE;
    }

    /* see if we already have this key in the data - means we are updating
     * a pre-existing value
     */
    kv = lookup_keyval(proc_data, val->key);
#if OPAL_ENABLE_DEBUG
    char *_data_type = opal_dss.lookup_data_type(val->type);
    OPAL_OUTPUT_VERBOSE((5, opal_pmix_base_framework.framework_output,
                         "%s pmix:hash:store: %s key %s[%s] for proc %s",
                         OPAL_NAME_PRINT(OPAL_PROC_MY_NAME),
                         (NULL == kv ? "storing" : "updating"),
                         val->key, _data_type, OPAL_NAME_PRINT(*id)));
    free (_data_type);
#endif

    if (NULL != kv) {
        opal_list_remove_item(&proc_data->data, &kv->super);
        OBJ_RELEASE(kv);
    }
    /* create the copy */
    if (OPAL_SUCCESS != (rc = opal_dss.copy((void**)&kv, val, OPAL_VALUE))) {
        OPAL_ERROR_LOG(rc);
        return rc;
    }
    opal_list_append(&proc_data->data, &kv->super);

    return OPAL_SUCCESS;
}

int pmix_xpmem_fetch(opal_proc_table_t *pt, const opal_process_name_t *id,
                         const char *key, opal_list_t *kvs)
{
    pmix_xpmem_proc_data_t *proc_data;
    opal_value_t *kv, *knew;
    int rc;

    OPAL_OUTPUT_VERBOSE((5, opal_pmix_base_framework.framework_output,
                         "%s pmix:hash:fetch: searching for key %s on proc %s",
                         OPAL_NAME_PRINT(OPAL_PROC_MY_NAME),
                         (NULL == key) ? "NULL" : key, OPAL_NAME_PRINT(*id)));

    /* lookup the proc data object for this proc */
    if (NULL == (proc_data = lookup_proc(pt, *id, true))) {
        OPAL_OUTPUT_VERBOSE((5, opal_pmix_base_framework.framework_output,
                             "%s pmix_hash:fetch data for proc %s not found",
                             OPAL_NAME_PRINT(OPAL_PROC_MY_NAME),
                             OPAL_NAME_PRINT(*id)));
        return OPAL_ERR_NOT_FOUND;
    }

    /* if the key is NULL, that we want everything */
    if (NULL == key) {
        /* must provide an output list or this makes no sense */
        if (NULL == kvs) {
            OPAL_ERROR_LOG(OPAL_ERR_BAD_PARAM);
            return OPAL_ERR_BAD_PARAM;
        }
        OPAL_LIST_FOREACH(kv, &proc_data->data, opal_value_t) {
            /* copy the value */
            if (OPAL_SUCCESS != (rc = opal_dss.copy((void**)&knew, kv, OPAL_VALUE))) {
                OPAL_ERROR_LOG(rc);
                return rc;
            }
            OPAL_OUTPUT_VERBOSE((5, opal_pmix_base_framework.framework_output,
                                 "%s pmix:hash:fetch: adding data for key %s on proc %s",
                                 OPAL_NAME_PRINT(OPAL_PROC_MY_NAME),
                                 (NULL == kv->key) ? "NULL" : kv->key,
                                 OPAL_NAME_PRINT(*id)));

            /* add it to the output list */
            opal_list_append(kvs, &knew->super);
        }
        return OPAL_SUCCESS;
    }

    /* find the value */
    if (NULL == (kv = lookup_keyval(proc_data, key))) {
        OPAL_OUTPUT_VERBOSE((5, opal_pmix_base_framework.framework_output,
                             "%s pmix_hash:fetch key %s for proc %s not found",
                             OPAL_NAME_PRINT(OPAL_PROC_MY_NAME),
                             (NULL == key) ? "NULL" : key,
                             OPAL_NAME_PRINT(*id)));
        return OPAL_ERR_NOT_FOUND;
    }

    /* if the user provided a NULL list object, then they
     * just wanted to know if the key was present */
    if (NULL == kvs) {
        return OPAL_SUCCESS;
    }

    /* create the copy */
    if (OPAL_SUCCESS != (rc = opal_dss.copy((void**)&knew, kv, OPAL_VALUE))) {
        OPAL_ERROR_LOG(rc);
        return rc;
    }
    /* add it to the output list */
    opal_list_append(kvs, &knew->super);

    return OPAL_SUCCESS;
}

int pmix_xpmem_remove(opal_proc_table_t *pt, const opal_process_name_t *id, const char *key)
{
    pmix_xpmem_proc_data_t *proc_data;
    opal_value_t *kv;

    /* lookup the specified proc */
    if (NULL == (proc_data = lookup_proc(pt, *id, false))) {
        /* no data for this proc */
        return OPAL_SUCCESS;
    }

    /* if key is NULL, remove all data for this proc */
    if (NULL == key) {
        while (NULL != (kv = (opal_value_t *) opal_list_remove_first(&proc_data->data))) {
            OBJ_RELEASE(kv);
        }
        /* remove the proc_data object itself from the jtable */
        opal_proc_table_remove_value(pt, *id);
        /* cleanup */
        OBJ_RELEASE(proc_data);
        return OPAL_SUCCESS;
    }

    /* remove this item */
    OPAL_LIST_FOREACH(kv, &proc_data->data, opal_value_t) {
        if (0 == strcmp(key, kv->key)) {
            opal_list_remove_item(&proc_data->data, &kv->super);
            OBJ_RELEASE(kv);
            break;
        }
    }

    return OPAL_SUCCESS;
}

