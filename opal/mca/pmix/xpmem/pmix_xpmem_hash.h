/*
 * Copyright (c) 2010      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2012      Los Alamos National Security, Inc. All rights reserved.
 * Copyright (c) 2014-2015 Intel, Inc. All rights reserved.
 * Copyright (c) 2014      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef OPAL_PMIX_HASH_H
#define OPAL_PMIX_HASH_H

#include "opal/class/opal_list.h"
#include "opal/class/opal_hash_table.h"
#include "opal/dss/dss.h"
#include "opal/util/proc.h"

BEGIN_C_DECLS

/**
 * Data for a particular opal process
 * The name association is maintained in the
 * proc_data hash table.
 */
typedef struct {
	/** Structure can be put on lists (including in hash tables) */
	opal_list_item_t super;
	bool loaded;
	/* List of opal_value_t structures containing all data
	   received from this process, sorted by key. */
	opal_list_t data;
} pmix_xpmem_proc_data_t;


OPAL_DECLSPEC void pmix_xpmem_hash_init(opal_proc_table_t*);
OPAL_DECLSPEC void pmix_xpmem_hash_finalize(opal_proc_table_t*);

OPAL_DECLSPEC int pmix_xpmem_store(opal_proc_table_t *pt, const opal_process_name_t *id,
                                       opal_value_t *val);

OPAL_DECLSPEC int pmix_xpmem_fetch(opal_proc_table_t *pt, const opal_process_name_t *id,
                                       const char *key, opal_list_t *kvs);

OPAL_DECLSPEC int pmix_xpmem_remove(opal_proc_table_t *pt, const opal_process_name_t *id, const char *key);

END_C_DECLS

#endif /* OPAL_DSTORE_HASH_H */
