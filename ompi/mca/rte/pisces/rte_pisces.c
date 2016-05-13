/*  I bet this eeds to go  */
#define _GNU_SOURCE
#include <stdio.h>
#include "rte_pisces.h"

#include "opal/mca/pmix/base/base.h"
#include "opal/mca/pmix/pmix.h"
#include "opal/mca/pmix/pisces/pmix_pisces.h"
#include "opal/mca/btl/base/base.h"
#include "opal/mca/btl/vader/btl_vader.h"
#include "opal/util/proc.h"
#include "opal/runtime/opal_progress_threads.h"
#include "ompi/communicator/communicator.h"

ompi_process_name_t cur;
ompi_process_info_t ompi_process_info;
opal_pmix_base_module_t opal_pmix;
opal_event_base_t *pisces_event_base;



int ompi_rte_compare_name_fields(ompi_rte_cmp_bitmask_t mask, const ompi_process_name_t *name1, const ompi_process_name_t *name2) {
	int ret;
	printf("%s: %x name1 vpid %d jobid %d name2 vpid %d jobid %d\n", __func__, mask, name1->vpid, name1->jobid, name2->vpid, name2->jobid);
	if(name1->jobid > name2->jobid)
		return 1;
	if(name1->jobid < name2->jobid)
		return -1;
	if(name1->jobid == name2->jobid && mask == OMPI_RTE_CMP_JOBID)
		return 0;
	if(name1->vpid > name2->vpid)
		return 1;
	if(name1->vpid < name2->vpid)
		return -1;
	if(name1->vpid == name2->vpid)
		return 0;
	return 0;

}

int OMPI_NAME_PRINT(ompi_process_name_t *name) {
	printf("%s: vpid %d jobid %d\n", __func__, name->vpid, name->jobid);
	return 1;
}

int OMPI_CONSTRUCT_JOBID(int family, int job)
{
	printf("%s: %d %d\n", __func__, family, job);
	return family&(0xffff0000)|job&(0xffff);
}

int OMPI_ERROR_LOG(int err)
{
	printf("%s: %d\n", __func__, err);
}

int ompi_rte_abort_peers(ompi_process_name_t *proc, int nprocs, int err)
{
	printf("%s: vpid %d jobid %d %d %d\n", __func__, proc->vpid, proc->jobid, nprocs, err);
	return 1;
}

int OMPI_LOCAL_JOBID(int job)
{
	printf("%s: job %d jobid %d\n", __func__, job, job & 0xffff);

	return job & 0xffff;
}

int OMPI_JOB_FAMILY(int job)
{
	printf("%s: job %d family %d\n", __func__, job, job & 0xffff0000);
	return 1;
}

int ompi_rte_convert_string_to_process_name(ompi_process_name_t *name, const char *str)
{
	printf("%s: vpid %d jobid %d string %s\n", __func__, name->vpid, name->jobid, str);
	name->vpid = 1;
	name->jobid = 1;
	printf("%s: vpid %d jobid %d string %s\n", __func__, name->vpid, name->jobid, str);
	return 1;
}

int ompi_rte_convert_process_name_to_string(char **str,  const ompi_process_name_t *name)
{
	printf("%s: vpid %d jobid %d %p\n", __func__, name->vpid, name->jobid, *str);
	return asprintf(*str, "%d.%d", __func__, name->vpid, name->jobid, str);
}

int ompi_rte_init(int *argc, char ***argv)
{
	int i, rank, size, ret;
	char *rankstr, *sizestr;
	opal_value_t *kv;
	opal_process_name_t name;

	rankstr = getenv("PMI_RANK");
	rank = atoi(rankstr);
	if(rankstr == NULL){
		printf("couldn't find rank\n");
		rank = 0;
	}
	sizestr = getenv("PMI_SIZE");
	size = atoi(sizestr);
	if(sizestr == NULL){
		printf("couldn't find size\n");
		size = 1;
	}
	mca_btl_base_include = strdup("self,vader");
	opal_process_info.my_local_rank = rank;
	ompi_process_info.my_local_rank = rank;
	ompi_process_info.num_procs = size;
	ompi_process_info.pid = getpid();
	ompi_process_info.my_local_rank = rank;
	ompi_process_info.app_num = 0;
	ompi_process_info.nodename = "kitten!";
	OMPI_PROC_MY_NAME->vpid = rank;
	OMPI_PROC_MY_NAME->jobid = 0;
	opal_process_info.num_local_peers = size;

	printf("%s: %p rank %d rankstr %s PMI_RANK %s PMI_SIZE %s ",__func__, argc, rank, rankstr, getenv("PMI_RANK"), getenv("PMI_SIZE"));
	if(argc){
		printf(" *argc %d", *argc);
		for(i = 0; i < *argc; i++) {
			printf(" %s", *argv[i]);
		}
	}
	//ompi_mpi_comm_world.comm.c_my_rank = rank;
//	ompi_mpi_comm_world.comm.c_local_group->grp_my_rank = rank;
	printf("\n");
	if (OPAL_SUCCESS != opal_init(&argc, &argv)) {
		exit(1);
	}
	pisces_event_base = opal_progress_thread_init(NULL);

	/*  open and setup pmix */
	if (OPAL_SUCCESS != mca_base_framework_open(&opal_pmix_base_framework, 0)) {
		printf("%s: error opening pmix\n",__func__);
		return 1;
	}
	ompi_process_info.nodename = "pisces-null";
	printf("%s: setting pmix\n",__func__);
	if (OPAL_SUCCESS != opal_pmix_base_select())
		return 1;
	printf("%s: pmix set: %p\n",__func__, opal_pmix);
	opal_pmix_base_set_evbase(pisces_event_base);
	if (!opal_pmix.initialized() && (OPAL_SUCCESS != (ret = opal_pmix.init()))) {
		printf("%s: couldn't initialize pmix\n", __func__);
		return ret;
	}
	for(i = 0; i < size; i++){
		union fake_modex_t {
			struct vader_modex_xpmem_t {
				uint64_t seg_id;
				void *segment_base;
			} xpmem;
		} faker;
		int slot, source_pid = getpid();
		if (source_pid == 1)
			slot = 0;
		else
			//slot = source_pid - 0x1000 + 1;
			slot = source_pid-2;
#define SMARTMAP_SHIFT 39

		printf("%s: making smartmap addr in pid %d at %lx addr is: %lx\n", __func__, source_pid, &mca_btl_vader_component.my_seg_id, (((slot + 1) << SMARTMAP_SHIFT) | ((unsigned long)&mca_btl_vader_component.my_seg_id)));
		uint64_t ind[3] = { 8589934635, 12884901932, 17179869229};
		uint64_t base[3] = { 0x0000008000000000, 0x0000010000000000, 0x0000018000000000};
		//uint64_t seg_base[3] = { 0x8007950000, 0x10007950000, 0x18007950000};
		uint64_t seg_base[3] = { 0x8007951000, 0x1000f951000, 0x1800f951000};
		faker.xpmem.seg_id = ind[i];
		//faker.xpmem.seg_id = *(uint64_t*)(((slot + 1) << SMARTMAP_SHIFT) | ((unsigned long)&mca_btl_vader_component.my_seg_id));
		printf("%s: set seg_id to %ld from addr %lx\n", __func__, faker.xpmem.seg_id, (uint64_t*)((((slot + 1) << SMARTMAP_SHIFT) | ((unsigned long)&mca_btl_vader_component.my_seg_id))));
		//faker.xpmem.segment_base = 0xbf7900;
		faker.xpmem.segment_base = seg_base[i];
		printf("%s: set segment_base to %lx\n", __func__, faker.xpmem.segment_base);

		kv = OBJ_NEW(opal_value_t);
		kv->key = strdup(OPAL_PMIX_LOCALITY);
		kv->type = OPAL_UINT16;
		name.jobid = 0;
		name.vpid = i;
		kv->data.uint16 = OPAL_PROC_ALL_LOCAL;
		opal_pmix.store_local(&name, kv);

		kv = OBJ_NEW(opal_value_t);
		kv->key = strdup(OPAL_PMIX_HOSTNAME);
		kv->type = OPAL_STRING;
		kv->data.string = strdup("enclave-1");
		opal_pmix.store_local(&name, kv);

		kv = OBJ_NEW(opal_value_t);
		kv->key = strdup("btl.vader.3.0");
		kv->type = OPAL_BYTE_OBJECT;
		kv->data.bo.bytes = &faker;
		kv->data.bo.size = sizeof(faker);
		opal_pmix.store_local(&name, kv);
	}
	return 0;
}

int ompi_rte_finalize(void)
{
	printf("%s:\n", __func__);
	return 1;
}
