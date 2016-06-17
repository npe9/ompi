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

#include "opal/class/opal_hash_table.h"
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

// parallels the opal version, needs to be in sync
struct supplemental_hash_element_t {
    int         valid;          /* whether this element is valid */
    union {                     /* the key, in its various forms */
        uint32_t        u32;
        uint64_t        u64;
        struct {
            const void * key;
            size_t      key_size;
        }       ptr;
    }           key;
    void *      value;          /* the value */
};
typedef struct supplemental_hash_element_t supplemental_hash_element_t;

static size_t
supplemental_hash_round_capacity_up(size_t capacity)
{
    /* round up to (1 mod 30) */
    return ((capacity+29)/30*30 + 1);
}

int *ntab;
int ntab_loc = 0;
supplemental_hash_element_t *supp_tab;
supplemental_hash_element_t supp_tab_loc[32][1024];

/* this could be the new init if people wanted a more general API */
/* (that's why it isn't static) */
int                             /* OPAL_ return code */
supplemental_hash_table_init2(opal_hash_table_t* ht, size_t estimated_max_size,
                      int density_numer, int density_denom,
                      int growth_numer, int growth_denom)
{
    size_t est_capacity = estimated_max_size * density_denom / density_numer;
    size_t capacity = supplemental_hash_round_capacity_up(est_capacity);
    printf("%s: capacity %d *ntab %d supp_tab %p &supp_tab[(*ntab)] %p\n", __func__, capacity, *ntab, supp_tab, &supp_tab[(*ntab)]);
    //ht->ht_table = (supplemental_hash_element_t*) calloc(capacity, sizeof(supplemental_hash_element_t));
    ht->ht_table = (supplemental_hash_element_t*) &supp_tab[(*ntab)++];
    if (NULL == ht->ht_table) {
        return OPAL_ERR_OUT_OF_RESOURCE;
    }
    ht->ht_capacity       = capacity;
    ht->ht_density_numer  = density_numer;
    ht->ht_density_denom  = density_denom;
    ht->ht_growth_numer   = growth_numer;
    ht->ht_growth_denom   = growth_denom;
    ht->ht_growth_trigger = capacity * density_numer / density_denom;
    ht->ht_type_methods   = NULL;
    printf("%s: finished\n", __func__);
    return OPAL_SUCCESS;
}

int                             /* OPAL_ return code */
supplemental_hash_table_init(opal_hash_table_t* ht, size_t table_size)
{
    /* default to density of 1/2 and growth of 2/1 */
    return supplemental_hash_table_init2(ht, table_size, 1, 2, 2, 1);
}


int *nvpids;
int nvpids_loc;
struct opal_hash_table_t vpidstab[32];

int xpmem_proc_table_set_value(opal_proc_table_t* pt, opal_process_name_t key, void* value) {
    int rc;
    opal_hash_table_t * vpids;
    rc = opal_hash_table_get_value_uint32(&pt->super, key.jobid, (void **)&vpids);
    if (rc != OPAL_SUCCESS) {
        //vpids = OBJ_NEW(opal_hash_table_t);
        vpids = &vpidstab[(*nvpids)++];
        vpids = (opal_hash_table_t *)(0x8000000000UL + (uint64_t)vpids);
        printf("%s: set vpids %p &pt->super %p\n", __func__, vpids, &pt->super);
        if (NULL == vpids) {
            return OPAL_ERR_OUT_OF_RESOURCE;
        }
        if (OPAL_SUCCESS != (rc=supplemental_hash_table_init(vpids, pt->vpids_size))) {
            OBJ_RELEASE(vpids);
            return rc;
        }
        if (OPAL_SUCCESS != (rc=opal_hash_table_set_value_uint32(&pt->super, key.jobid, vpids))) {
            OBJ_RELEASE(vpids);
            return rc;
        }
    }
    rc = opal_hash_table_set_value_uint32(vpids, key.vpid, value);
    return rc;
}


/**
 * Find data for a given key in a given proc_data_t
 * container.
 */
static opal_value_t* lookup_keyval(pmix_xpmem_proc_data_t *proc_data,
                                   const char *key)
{
    opal_value_t *kv;

    printf("%s: looking for %s in %p at %p\n", __func__, key, proc_data, &proc_data->data);
    OPAL_LIST_FOREACH(kv, &proc_data->data, opal_value_t) {
        printf("%s: comparing key %s to kv %p in %p\n", __func__, key, kv, proc_data);
        if (0 == strcmp(key, kv->key)) {
            return kv;
        }
    }
    return NULL;
}

int *nproc_data;
int nproc_data_loc;
pmix_xpmem_proc_data_t proc_data_tab[32];

/**
 * Find proc_data_t container associated with given
 * opal_process_name_t.
 */
static pmix_xpmem_proc_data_t* lookup_proc(opal_proc_table_t *pt,
                                          opal_process_name_t id, bool create)
{
    pmix_xpmem_proc_data_t *proc_data = NULL;

    printf("%s: looking up proc_data %p in pt %p should create? %d\n", __func__, proc_data, pt, create);
    opal_proc_table_get_value(pt, id, (void**)&proc_data);
    printf("%s: got data\n", __func__);
    if (NULL == proc_data && create) {
        printf("%s: not found creating\n", __func__);
        //proc_data = OBJ_NEW(pmix_xpmem_proc_data_t);
        proc_data = &proc_data_tab[(*nproc_data)++];
        if (NULL == proc_data) {
            printf("%s: unable to allocate\n", __func__);
            opal_output(0, "pmix:hash:lookup_proc: unable to allocate proc_data_t\n");
            return NULL;
        }
        proc_data = (pmix_xpmem_proc_data_t *)(0x8000000000UL + (uint64_t)proc_data);
        printf("%s: setting value pt %p &id %p create %d\n", __func__, pt, &id, create);
        xpmem_proc_table_set_value(pt, id, proc_data);
    }

    printf("%s: returning proc_data %p\n", __func__, proc_data);
    return proc_data;
}


//static opal_proc_table_t ptable;

int xpmem_proc_table_init(opal_proc_table_t* pt, size_t jobids, size_t vpids) {
    int rc;
    printf("%s: doing supplemental hash init\n", __func__);
    if (OPAL_SUCCESS != (rc=supplemental_hash_table_init(&pt->super, jobids))) {
        return rc;
    }
    pt->vpids_size = vpids;
    printf("%s: did proc table init\n", __func__);
    return OPAL_SUCCESS;
}


/* Initialize our hash table */
void pmix_xpmem_hash_init(opal_proc_table_t *pt)
{
    printf("%s: setting up table offsets\n", __func__);
    nproc_data = (void*)(0x8000000000UL + (uint64_t)&nproc_data_loc);
    nvpids = (void*)(0x8000000000UL + (uint64_t)&nvpids_loc);
    
    supp_tab = (void*)(0x8000000000UL + (uint64_t)&supp_tab_loc);
    ntab = (void*)(0x8000000000UL + (uint64_t)&ntab_loc);

    OBJ_CONSTRUCT(pt, opal_proc_table_t);
    xpmem_proc_table_init(pt, 16, 256);
    printf("%s: finished proc table init\n", __func__);
    pt->super.ht_table = (void*)(0x8000000000UL + (uint64_t)pt->super.ht_table);
    printf("%s: pt %p\n", __func__, pt);
    printf("%s: pt->super %p\n", __func__, pt->super);
    printf("%s: pt->super.ht_table %p\n", __func__, pt->super.ht_table);
    //mutex = (void*)(0x8000000000UL + (uint64_t)&mutex_loc);
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
    printf("%s: would have printed name here\n", __func__);
    //printf("%s: %s pmix:hash:store storing data for proc %s", __func__,
    //                    OPAL_NAME_PRINT(OPAL_PROC_MY_NAME), OPAL_NAME_PRINT(*id));

    /* lookup the proc data object for this proc */
    if (NULL == (proc_data = lookup_proc(pt, *id, true))) {
        /* unrecoverable error */
        OPAL_OUTPUT_VERBOSE((5, opal_pmix_base_framework.framework_output,
                             "%s pmix:hash:store: storing data for proc %s unrecoverably failed",
                             OPAL_NAME_PRINT(OPAL_PROC_MY_NAME), OPAL_NAME_PRINT(*id)));
        return OPAL_ERR_OUT_OF_RESOURCE;
    }

    printf("%s: found proc %p want val->key %s\n", __func__, proc_data, val->key);
    /* see if we already have this key in the data - means we are updating
     * a pre-existing value
     */
    kv = lookup_keyval(proc_data, val->key);
#if OPAL_ENABLE_DEBUG
    char *_data_type = opal_dss.lookup_data_type(val->type);
    printf("%s: %s pmix:hash:store: %s key %s[%s] for proc %s",
           __func__,
           OPAL_NAME_PRINT(OPAL_PROC_MY_NAME),
           (NULL == kv ? "storing" : "updating"),
           val->key, _data_type, OPAL_NAME_PRINT(*id));
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
    printf("%s: storing to proc_data->data %p &kv->super %p\n", __func__, proc_data->data, &kv->super);
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
        printf("%s: looking up proc data object\n", __func__);
        printf("%s: proc %s : looking for proc %s but not found\n", __func__, OPAL_NAME_PRINT(OPAL_PROC_MY_NAME), OPAL_NAME_PRINT(*id));
        OPAL_OUTPUT_VERBOSE((5, opal_pmix_base_framework.framework_output,
                             "%s pmix_hash:fetch data for proc %s not found",
                             OPAL_NAME_PRINT(OPAL_PROC_MY_NAME),
                             OPAL_NAME_PRINT(*id)));
        return OPAL_ERR_NOT_FOUND;
    }
    printf("%s: got past proc lookup\n", __func__);
    if(id == NULL)
        printf("%s: *id is null\n", __func__);
    //printf("%s: proc %s : looking in proc %s for key %p\n", __func__, OPAL_NAME_PRINT(OPAL_PROC_MY_NAME), OPAL_NAME_PRINT(*id), key);
    /* if the key is NULL, that we want everything */
    if (NULL == key) {
        printf("%s: key is null\n", __func__);
        /* must provide an output list or this makes no sense */
        if (NULL == kvs) {
            OPAL_ERROR_LOG(OPAL_ERR_BAD_PARAM);
            printf("%s: bad param\n", __func__);
            return OPAL_ERR_BAD_PARAM;
        }
        OPAL_LIST_FOREACH(kv, &proc_data->data, opal_value_t) {
            /* copy the value */
            if (OPAL_SUCCESS != (rc = opal_dss.copy((void**)&knew, kv, OPAL_VALUE))) {
                OPAL_ERROR_LOG(rc);
                printf("%s: couldn't copy\n", __func__);
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

    printf("%s: find the value\n", __func__);
//    printf("%s: proc %s : looking in proc %s for value of key %p\n", __func__, OPAL_NAME_PRINT(OPAL_PROC_MY_NAME), OPAL_NAME_PRINT(*id), &key);
    /* find the value */
    if (NULL == (kv = lookup_keyval(proc_data, key))) {
        OPAL_OUTPUT_VERBOSE((5, opal_pmix_base_framework.framework_output,
                             "%s pmix_hash:fetch key %s for proc %s not found",
                             OPAL_NAME_PRINT(OPAL_PROC_MY_NAME),
                             (NULL == key) ? "NULL" : key,
                             OPAL_NAME_PRINT(*id)));
        printf("%s: key not found\n", __func__);
        return OPAL_ERR_NOT_FOUND;
    }

    printf("%s: found kv %p\n", __func__, kv);
    /* if the user provided a NULL list object, then they
     * just wanted to know if the key was present */
    if (NULL == kvs) {
        return OPAL_SUCCESS;
    }

    printf("%s: making copy of %p\n", __func__, kv);
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

