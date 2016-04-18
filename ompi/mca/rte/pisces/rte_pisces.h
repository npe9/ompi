/*
 * Copyright (c) 2012-2013 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2013-2015 Intel, Inc. All rights reserved
 * Copyright (c) 2014      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2014-2016 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2015      Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 * When this component is used, this file is included in the rest of
 * the OPAL/ORTE/OMPI code base via ompi/mca/rte/rte.h.  As such,
 * this header represents the public interface to this static component.
 */

#ifndef MCA_OMPI_RTE_PISCES_H
#define MCA_OMPI_RTE_PISCES_H

#include "ompi_config.h"
#include "ompi/constants.h"
#include "opal/threads/condition.h"

typedef struct
{
	int vpid;
	int jobid;
} ompi_process_name_t;

extern ompi_process_name_t cur;

#include "ompi/mca/rte/rte.h"

typedef struct {
	char *job_session_dir;
	char *proc_session_dir;
	char *nodename;
	int my_local_rank;
	int cpuset;
	int app_num;
	int num_procs;
	int pid;
} ompi_process_info_t;

extern ompi_process_info_t ompi_process_info;

typedef struct {
	ompi_rte_component_t super;
	opal_mutex_t lock;
	opal_list_t modx_reqs;
} ompi_rte_pisces_component_t;

typedef struct {
	opal_list_item_t super;
	opal_mutex_t lock;
	opal_condition_t cond;
	bool active;
	ompi_process_name_t peer;
} ompi_pisces_tracker_t;

typedef int ompi_rte_cmp_bitmask_t;
typedef int ompi_jobid_t;
typedef int ompi_vpid_t;
#define OMPI_NAME 1

int OMPI_LOCAL_JOBID(int);
int OMPI_JOB_FAMILY(int);
int OMPI_CONSTRUCT_JOBID(int, int);
int OMPI_PROCESS_NAME_HTON(ompi_process_name_t);
int OMPI_PROCESS_NAME_NTOH(ompi_process_name_t);

int ompi_rte_proc_is_bound;
ompi_process_name_t *orte_proc_applied_binding;
void ompi_rte_abort(int, char*, ...);

#define OMPI_PROC_MY_NAME ((ompi_process_name_t*)&cur)
int OMPI_NAME_PRINT(ompi_process_name_t*);
int ompi_rte_compare_name_fields(ompi_rte_cmp_bitmask_t, const ompi_process_name_t*, const ompi_process_name_t*);
int ompi_rte_abort_peers(ompi_process_name_t*, int, int);
int ompi_rte_convert_string_to_process_name(ompi_process_name_t*, const char*);
int ompi_rte_convert_process_name_to_string(char**,  const ompi_process_name_t*);
int ompi_rte_init(int *, char ***);
int ompi_rte_finalize(void);
void ompi_rte_wait_for_debugger(void);


int OMPI_ERROR_LOG(int);

#define OMPI_RTE_CMP_ALL 1
#define OMPI_RTE_CMP_JOBID 2
#define OMPI_RTE_CMP_VPID 3


#define OMPI_ERR_NO_MATCH_YET -2

#define OMPI_VPID_WILDCARD -1
#define OMPI_JOBID_WILDCARD -1

#define OMPI_VPID_INVALID OPAL_VPID_INVALID

#define OMPI_CAST_RTE_NAME(a) ((ompi_process_name_t*)(a))

#endif
