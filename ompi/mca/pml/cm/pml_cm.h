/*
 * Copyright (c) 2004-2006 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2004-2021 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2017      Intel, Inc. All rights reserved
 * Copyright (c) 2022      IBM Corporation. All rights reserved
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef PML_CM_H
#define PML_CM_H

#ifdef HAVE_ALLOCA_H
#include <alloca.h>
#endif

#include "ompi_config.h"
#include "ompi/request/request.h"
#include "ompi/mca/pml/pml.h"
#include "ompi/mca/pml/base/base.h"
#include "ompi/datatype/ompi_datatype.h"
#include "ompi/communicator/communicator.h"
#include "ompi/request/request.h"
#include "ompi/mca/mtl/mtl.h"


#include "pml_cm_request.h"
#include "ompi/mca/pml/base/pml_base_recvreq.h"
#include "ompi/mca/mtl/mtl.h"
#include "pml_cm_recvreq.h"
#include "pml_cm_sendreq.h"
#include "ompi/message/message.h"
#if HAVE_LITHE
#include "opal/runtime/opal_progress.h"
#include <lithe/lithe.h>
#include <lithe/futex.h>
#include <stdlib.h>
#include <time.h>

/* DE-SPIN of MPI_Probe / MPI_Mprobe (2026-10-24, .build_logs/despin/mpi/):
 * LITHE_PML_CM_NOSPIN_PROBE=1 (default 0). The loop below was `while (!matched)
 * { iprobe; opal_progress(); }`: a pure poll that holds the hart until a PEER posts
 * the message, also when that peer is a co-resident context without a hart. With the
 * gate: LITHE_PML_CM_PROBE_SPIN misses (default 256), then sleep -- on the hosted
 * same-OS short-circuit's per-slot futex word, which a same-OS sender bumps and wakes
 * (opal_progress_probe_wait_fn), for at most a backed-off timeout (20 us doubling to
 * LITHE_PML_CM_PROBE_MAX_US, default 1000) that covers OFI arrivals, which wake
 * nothing (no CQ entry exists for an unexpected message, so there is no fd to park
 * on). Without the hook, the timed sleep alone (still a poll, but the hart is free).
 * Contexts that cannot block cooperatively keep the old loop.
 * A/B (login, .build_logs/despin/mpi/res_p2p.txt, res_tprobe*.txt; 2000 ping-pong
 * probes, 3 harts): with 8 contexts on 3 harts the old poll has catastrophic tails --
 * probe 8 B 1.29 us/op median but 531, 1781, 5439, 216 us/op in 4 of 18 runs, mprobe
 * with 50 us of receiver work up to 367 us/op (vs 51 median) -- the probers hold the
 * harts the senders need. Spin 16 removed the tails but cost the median 4x when harts
 * are spare (probe 0.84 -> 3.4 us, futex sleep+wake on every probe); spin 256 keeps the
 * median (0.80 vs 0.875) and bounds the tail (worst 4.9 vs 216 us/op). Default still 0
 * pending the reruns quoted at the gate's flip. */
static inline int mca_pml_cm_nospin_probe(void)
{
    static int cached = -1;
    if (OPAL_UNLIKELY(cached < 0)) {
        const char *e = getenv("LITHE_PML_CM_NOSPIN_PROBE");
        cached = (e && '1' == e[0]) ? 1 : 0;
    }
    return cached;
}
static inline unsigned mca_pml_cm_probe_knob(int which)
{
    static int spin = -1, maxus = -1;
    if (OPAL_UNLIKELY(spin < 0)) {
        const char *e = getenv("LITHE_PML_CM_PROBE_SPIN");
        int v = (e && *e) ? atoi(e) : 256;
        const char *m = getenv("LITHE_PML_CM_PROBE_MAX_US");
        int w = (m && *m) ? atoi(m) : 1000;
        maxus = w > 0 ? w : 1000;
        spin = v > 0 ? v : 1;
    }
    return (unsigned) (which ? maxus : spin);
}
/* One step of the blocking-probe wait after a miss; state lives in the caller. */
static inline void mca_pml_cm_probe_wait_step(unsigned *miss, int *armed, int tok,
                                              unsigned *us)
{
    int (*pw)(int, int, unsigned) = opal_progress_probe_wait_fn;
    if (++*miss < mca_pml_cm_probe_knob(0)) {
        return;
    }
    if (!*armed) {
        *armed = 1;
        if (NULL != pw) {
            pw(0, 0, 0); /* count as prober, then re-probe after a fresh snapshot */
        }
        return;
    }
    *us = *us ? *us * 2 : 20;
    if (*us > mca_pml_cm_probe_knob(1)) {
        *us = mca_pml_cm_probe_knob(1);
    }
    if (NULL != pw) {
        pw(2, tok, *us);
    } else {
        int z = 0;
        struct timespec rel = {(time_t) (*us / 1000000u), (long) (*us % 1000000u) * 1000L};
        (void) futex_wait_timeout(&z, 0, &rel);
    }
}
#define MCA_PML_CM_PROBE_STATE                                          \
    const int _pns = mca_pml_cm_nospin_probe() && lithe_can_block();     \
    unsigned _pmiss = 0, _pus = 0;                                      \
    int _parmed = 0, _ptok = 0
#define MCA_PML_CM_PROBE_SNAPSHOT                                       \
    do {                                                                \
        if (_parmed && NULL != opal_progress_probe_wait_fn)             \
            _ptok = opal_progress_probe_wait_fn(1, 0, 0);               \
    } while (0)
#define MCA_PML_CM_PROBE_MISS                                           \
    do {                                                                \
        if (_pns) mca_pml_cm_probe_wait_step(&_pmiss, &_parmed, _ptok, &_pus); \
    } while (0)
#define MCA_PML_CM_PROBE_DONE                                           \
    do {                                                                \
        if (_parmed && NULL != opal_progress_probe_wait_fn)             \
            opal_progress_probe_wait_fn(3, 0, 0);                       \
    } while (0)
#else
#define MCA_PML_CM_PROBE_STATE int _pns_unused = 0
#define MCA_PML_CM_PROBE_SNAPSHOT do { } while (0)
#define MCA_PML_CM_PROBE_MISS do { (void) _pns_unused; } while (0)
#define MCA_PML_CM_PROBE_DONE do { } while (0)
#endif


BEGIN_C_DECLS

struct mca_mtl_request_t;

struct ompi_pml_cm_t {
    mca_pml_base_module_t super;
    int                   free_list_num;
    int                   free_list_max;
    int                   free_list_inc;
};
typedef struct ompi_pml_cm_t ompi_pml_cm_t;
extern ompi_pml_cm_t ompi_pml_cm;

/* PML interface functions */
OMPI_DECLSPEC extern int mca_pml_cm_add_procs(struct ompi_proc_t **procs, size_t nprocs);
OMPI_DECLSPEC extern int mca_pml_cm_del_procs(struct ompi_proc_t **procs, size_t nprocs);

OMPI_DECLSPEC extern int mca_pml_cm_enable(bool enable);
OMPI_DECLSPEC extern int mca_pml_cm_progress(void);

OMPI_DECLSPEC extern int mca_pml_cm_add_comm(struct ompi_communicator_t* comm);
OMPI_DECLSPEC extern int mca_pml_cm_del_comm(struct ompi_communicator_t* comm);


__opal_attribute_always_inline__ static inline int
mca_pml_cm_irecv_init(void *addr,
                      size_t count,
                      ompi_datatype_t * datatype,
                      int src,
                      int tag,
                      struct ompi_communicator_t *comm,
                      struct ompi_request_t **request)
{
    mca_pml_cm_hvy_recv_request_t *recvreq;
    uint32_t flags = 0;
#if OPAL_ENABLE_HETEROGENEOUS_SUPPORT
    ompi_proc_t* ompi_proc;
#endif

    MCA_PML_CM_HVY_RECV_REQUEST_ALLOC(recvreq);
    if( OPAL_UNLIKELY(NULL == recvreq) ) return OMPI_ERR_OUT_OF_RESOURCE;

    MCA_PML_CM_HVY_RECV_REQUEST_INIT(recvreq, ompi_proc, comm, tag, src,
                                     datatype, addr, count, flags, true);

    *request = (ompi_request_t*) recvreq;

    return OMPI_SUCCESS;
}

__opal_attribute_always_inline__ static inline int
mca_pml_cm_irecv(void *addr,
                 size_t count,
                 ompi_datatype_t * datatype,
                 int src,
                 int tag,
                 struct ompi_communicator_t *comm,
                 struct ompi_request_t **request)
{
    int ret;
    uint32_t flags = 0;
    mca_pml_cm_thin_recv_request_t *recvreq;
#if OPAL_ENABLE_HETEROGENEOUS_SUPPORT
    ompi_proc_t* ompi_proc = NULL;
#endif

    MCA_PML_CM_THIN_RECV_REQUEST_ALLOC(recvreq);
    if( OPAL_UNLIKELY(NULL == recvreq) ) return OMPI_ERR_OUT_OF_RESOURCE;

    MCA_PML_CM_THIN_RECV_REQUEST_INIT(recvreq,
                                      ompi_proc,
                                      comm,
                                      src,
                                      datatype,
                                      addr,
                                      count,
                                      flags);

    MCA_PML_CM_THIN_RECV_REQUEST_START(recvreq, comm, tag, src, ret);

    if( OPAL_LIKELY(OMPI_SUCCESS == ret) ) *request = (ompi_request_t*) recvreq;

    return ret;
}

__opal_attribute_always_inline__ static inline void
mca_pml_cm_recv_fast_completion(struct mca_mtl_request_t *mtl_request)
{
    // Do nothing!
    ompi_request_complete(mtl_request->ompi_req, true);
    return;
}

__opal_attribute_always_inline__ static inline int
mca_pml_cm_recv(void *addr,
                size_t count,
                ompi_datatype_t * datatype,
                int src,
                int tag,
                struct ompi_communicator_t *comm,
                ompi_status_public_t * status)
{
    int ret;
    uint32_t flags = 0;
#if OPAL_ENABLE_HETEROGENEOUS_SUPPORT
    ompi_proc_t *ompi_proc;
#endif
    opal_convertor_t convertor;
    mca_pml_cm_request_t req;
    mca_mtl_request_t *req_mtl =
            alloca(sizeof(mca_mtl_request_t) + ompi_mtl->mtl_request_size);

    OBJ_CONSTRUCT(&convertor, opal_convertor_t);
    req_mtl->ompi_req = &req.req_ompi;
    req_mtl->completion_callback = mca_pml_cm_recv_fast_completion;

    req.req_pml_type = MCA_PML_CM_REQUEST_RECV_THIN;
    req.req_free_called = false;
    req.req_ompi.req_complete = false;
    req.req_ompi.req_complete_cb = NULL;
    req.req_ompi.req_state = OMPI_REQUEST_ACTIVE;
    req.req_ompi.req_status.MPI_TAG = OMPI_ANY_TAG;
    req.req_ompi.req_status.MPI_ERROR = OMPI_SUCCESS;
    req.req_ompi.req_status._cancelled = 0;

#if OPAL_ENABLE_HETEROGENEOUS_SUPPORT
    if( MPI_ANY_SOURCE == src ) {
        ompi_proc = ompi_comm_peer_lookup(comm, ompi_comm_rank(comm));
    } else {
        ompi_proc = ompi_comm_peer_lookup( comm, src );
    }

    MCA_PML_CM_SWITCH_ACCELERATOR_CONVERTOR_OFF(flags, datatype, count);

    opal_convertor_copy_and_prepare_for_recv(
	ompi_proc->super.proc_convertor,
		&(datatype->super),
		count,
		addr,
                flags,
		&convertor );
#else
    MCA_PML_CM_SWITCH_ACCELERATOR_CONVERTOR_OFF(flags, datatype, count);

    opal_convertor_copy_and_prepare_for_recv(
	ompi_mpi_local_convertor,
		&(datatype->super),
		count,
		addr,
                flags,
		&convertor );
#endif

    ret = OMPI_MTL_CALL(irecv(ompi_mtl,
                              comm,
                              src,
                              tag,
                              &convertor,
                              req_mtl));
    if( OPAL_UNLIKELY(OMPI_SUCCESS != ret) ) {
	OBJ_DESTRUCT(&convertor);
        return ret;
    }

    ompi_request_wait_completion(&req.req_ompi);

    if (MPI_STATUS_IGNORE != status) {
        OMPI_COPY_STATUS(status, req.req_ompi.req_status, false);
    }
    ret = req.req_ompi.req_status.MPI_ERROR;
    OBJ_DESTRUCT(&convertor);
    return ret;
}

__opal_attribute_always_inline__ static inline int
mca_pml_cm_isend_init(const void* buf,
                        size_t count,
                        ompi_datatype_t* datatype,
                        int dst,
                        int tag,
                        mca_pml_base_send_mode_t sendmode,
                        ompi_communicator_t* comm,
                        ompi_request_t** request)
{
    mca_pml_cm_hvy_send_request_t *sendreq;
    uint32_t flags = 0;
#if OPAL_ENABLE_HETEROGENEOUS_SUPPORT
    ompi_proc_t* ompi_proc;
#endif

    MCA_PML_CM_HVY_SEND_REQUEST_ALLOC(sendreq, comm, dst, ompi_proc);
    if (OPAL_UNLIKELY(NULL == sendreq)) return OMPI_ERR_OUT_OF_RESOURCE;

    MCA_PML_CM_HVY_SEND_REQUEST_INIT(sendreq, ompi_proc, comm, tag, dst,
                                     datatype, sendmode, true, false, buf, count, flags);

    /* Work around a leak in start by marking this request as complete. The
     * problem occurred because we do not have a way to differentiate an
     * initial request and an incomplete pml request in start. This line
     * allows us to detect this state. */
    sendreq->req_send.req_base.req_pml_complete = true;

    *request = (ompi_request_t*) sendreq;

    return OMPI_SUCCESS;
}

__opal_attribute_always_inline__ static inline int
mca_pml_cm_isend(const void* buf,
                   size_t count,
                   ompi_datatype_t* datatype,
                   int dst,
                   int tag,
                   mca_pml_base_send_mode_t sendmode,
                   ompi_communicator_t* comm,
                   ompi_request_t** request)
{
    int ret;
    uint32_t flags = 0;

    if(sendmode == MCA_PML_BASE_SEND_BUFFERED ) {
        mca_pml_cm_hvy_send_request_t* sendreq;
#if OPAL_ENABLE_HETEROGENEOUS_SUPPORT
        ompi_proc_t* ompi_proc = NULL;
#endif

        MCA_PML_CM_HVY_SEND_REQUEST_ALLOC(sendreq, comm, dst, ompi_proc);
        if (OPAL_UNLIKELY(NULL == sendreq)) return OMPI_ERR_OUT_OF_RESOURCE;

        MCA_PML_CM_HVY_SEND_REQUEST_INIT(sendreq,
                                         ompi_proc,
                                         comm,
                                         tag,
                                         dst,
                                         datatype,
                                         sendmode,
                                         false,
                                         false,
                                         buf,
                                         count,
                                         flags);

        MCA_PML_CM_HVY_SEND_REQUEST_START( sendreq, ret);

        if (OPAL_LIKELY(OMPI_SUCCESS == ret)) *request = (ompi_request_t*) sendreq;

    } else {
        mca_pml_cm_thin_send_request_t* sendreq;
#if OPAL_ENABLE_HETEROGENEOUS_SUPPORT
        ompi_proc_t* ompi_proc = NULL;
#endif
        MCA_PML_CM_THIN_SEND_REQUEST_ALLOC(sendreq, comm, dst, ompi_proc);
        if (OPAL_UNLIKELY(NULL == sendreq)) return OMPI_ERR_OUT_OF_RESOURCE;

        MCA_PML_CM_THIN_SEND_REQUEST_INIT(sendreq,
                                          ompi_proc,
                                          comm,
                                          tag,
                                          dst,
                                          datatype,
                                          sendmode,
                                          buf,
                                          count,
                                          flags);

        MCA_PML_CM_THIN_SEND_REQUEST_START(
                                           sendreq,
                                           comm,
                                           tag,
                                           dst,
                                           sendmode,
                                           false,
                                           ret);

        if (OPAL_LIKELY(OMPI_SUCCESS == ret)) *request = (ompi_request_t*) sendreq;

    }

    return ret;
}

__opal_attribute_always_inline__ static inline int
mca_pml_cm_send(const void *buf,
                size_t count,
                ompi_datatype_t* datatype,
                int dst,
                int tag,
                mca_pml_base_send_mode_t sendmode,
                ompi_communicator_t* comm)
{
    int ret = OMPI_ERROR;
    uint32_t flags = 0;
    ompi_proc_t * ompi_proc;

    if(sendmode == MCA_PML_BASE_SEND_BUFFERED) {
        mca_pml_cm_hvy_send_request_t *sendreq;

        MCA_PML_CM_HVY_SEND_REQUEST_ALLOC(sendreq, comm, dst, ompi_proc);
        if (OPAL_UNLIKELY(NULL == sendreq)) return OMPI_ERR_OUT_OF_RESOURCE;

        MCA_PML_CM_HVY_SEND_REQUEST_INIT(sendreq,
                                         ompi_proc,
                                         comm,
                                         tag,
                                         dst,
                                         datatype,
                                         sendmode,
                                         false,
                                         false,
                                         buf,
                                         count,
                                         flags);
        MCA_PML_CM_HVY_SEND_REQUEST_START(sendreq, ret);
        if (OPAL_UNLIKELY(OMPI_SUCCESS != ret)) {
            MCA_PML_CM_HVY_SEND_REQUEST_RETURN(sendreq);
            return ret;
        }

        ompi_request_free( (ompi_request_t**)&sendreq );
    } else {
        opal_convertor_t convertor;
	OBJ_CONSTRUCT(&convertor, opal_convertor_t);
#if !(OPAL_ENABLE_HETEROGENEOUS_SUPPORT)
	if (opal_datatype_is_contiguous_memory_layout(&datatype->super, count)) {

		convertor.remoteArch = ompi_mpi_local_convertor->remoteArch;
		convertor.flags      = ompi_mpi_local_convertor->flags;
		convertor.master     = ompi_mpi_local_convertor->master;

                /* Switches off device detection if
                   MTL set MCA_MTL_BASE_FLAG_ACCELERATOR_INIT_DISABLE during init */
                MCA_PML_CM_SWITCH_ACCELERATOR_CONVERTOR_OFF(flags, datatype, count);
                convertor.flags      |= flags;
                /* Sets CONVERTOR_ACCELERATOR flag if device buffer */
                opal_convertor_prepare_for_send(&convertor, &datatype->super, count, (unsigned char *)buf);
    } else
#endif
	{
		ompi_proc = ompi_comm_peer_lookup(comm, dst);

                MCA_PML_CM_SWITCH_ACCELERATOR_CONVERTOR_OFF(flags, datatype, count);

		opal_convertor_copy_and_prepare_for_send(
		ompi_proc->super.proc_convertor,
			&datatype->super, count, buf, flags,
			&convertor);
	}

        ret = OMPI_MTL_CALL(send(ompi_mtl,
                                 comm,
                                 dst,
                                 tag,
                                 &convertor,
                                 sendmode));
	OBJ_DESTRUCT(&convertor);
    }

    return ret;
}

__opal_attribute_always_inline__ static inline int
mca_pml_cm_iprobe(int src, int tag,
                   struct ompi_communicator_t *comm,
                   int *matched, ompi_status_public_t * status)
{
    return OMPI_MTL_CALL(iprobe(ompi_mtl,
                                comm, src, tag,
                                matched, status));
}

__opal_attribute_always_inline__ static inline int
mca_pml_cm_probe(int src, int tag,
                  struct ompi_communicator_t *comm,
                  ompi_status_public_t * status)
{
    int ret, matched = 0;
    MCA_PML_CM_PROBE_STATE;

    while (true) {
        MCA_PML_CM_PROBE_SNAPSHOT;
        ret = OMPI_MTL_CALL(iprobe(ompi_mtl,
                                   comm, src, tag,
                                   &matched, status));
        if (OMPI_SUCCESS != ret) break;
        if (matched) break;
        opal_progress();
        MCA_PML_CM_PROBE_MISS;
    }
    MCA_PML_CM_PROBE_DONE;

    return ret;
}

__opal_attribute_always_inline__ static inline int
mca_pml_cm_improbe(int src,
                   int tag,
                   struct ompi_communicator_t* comm,
                   int *matched,
                   struct ompi_message_t **message,
                   ompi_status_public_t* status)
{
    return OMPI_MTL_CALL(improbe(ompi_mtl,
                                 comm, src, tag,
                                 matched, message,
                                 status));
}

__opal_attribute_always_inline__ static inline int
mca_pml_cm_mprobe(int src,
                  int tag,
                  struct ompi_communicator_t* comm,
                  struct ompi_message_t **message,
                  ompi_status_public_t* status)
{
    int ret, matched = 0;
    MCA_PML_CM_PROBE_STATE;

    while (true) {
        MCA_PML_CM_PROBE_SNAPSHOT;
        ret = OMPI_MTL_CALL(improbe(ompi_mtl,
                                    comm, src, tag,
                                    &matched, message,
                                    status));
        if (OMPI_SUCCESS != ret) break;
        if (matched) break;
        opal_progress();
        MCA_PML_CM_PROBE_MISS;
    }
    MCA_PML_CM_PROBE_DONE;

    return ret;
}

__opal_attribute_always_inline__ static inline int
mca_pml_cm_imrecv(void *buf,
                  size_t count,
                  ompi_datatype_t *datatype,
                  struct ompi_message_t **message,
                  struct ompi_request_t **request)
{
    int ret;
    uint32_t flags = 0;
    mca_pml_cm_thin_recv_request_t *recvreq;
#if OPAL_ENABLE_HETEROGENEOUS_SUPPORT
    ompi_proc_t* ompi_proc;
#endif
    ompi_communicator_t *comm = (*message)->comm;

    MCA_PML_CM_THIN_RECV_REQUEST_ALLOC(recvreq);
    if( OPAL_UNLIKELY(NULL == recvreq) ) return OMPI_ERR_OUT_OF_RESOURCE;

    MCA_PML_CM_THIN_RECV_REQUEST_INIT(recvreq,
                                      ompi_proc,
                                      comm,
                                      (*message)->peer,
                                      datatype,
                                      buf,
                                      count,
                                      flags);

    MCA_PML_CM_THIN_RECV_REQUEST_MATCHED_START(recvreq, message, ret);

    if( OPAL_LIKELY(OMPI_SUCCESS == ret) ) *request = (ompi_request_t*) recvreq;

    return ret;
}

__opal_attribute_always_inline__ static inline int
mca_pml_cm_mrecv(void *buf,
                 size_t count,
                 ompi_datatype_t *datatype,
                 struct ompi_message_t **message,
                 ompi_status_public_t* status)
{
    int ret;
    uint32_t flags = 0;
    mca_pml_cm_thin_recv_request_t *recvreq;
#if OPAL_ENABLE_HETEROGENEOUS_SUPPORT
    ompi_proc_t* ompi_proc;
#endif
    ompi_communicator_t *comm = (*message)->comm;

    MCA_PML_CM_THIN_RECV_REQUEST_ALLOC(recvreq);
    if( OPAL_UNLIKELY(NULL == recvreq) ) return OMPI_ERR_OUT_OF_RESOURCE;

    MCA_PML_CM_THIN_RECV_REQUEST_INIT(recvreq,
                                      ompi_proc,
                                      comm,
                                      (*message)->peer,
                                      datatype,
                                      buf,
                                      count,
                                      flags);

    MCA_PML_CM_THIN_RECV_REQUEST_MATCHED_START(recvreq,
                                               message, ret);
    if( OPAL_UNLIKELY(OMPI_SUCCESS != ret) ) {
        MCA_PML_CM_THIN_RECV_REQUEST_RETURN(recvreq);
        return ret;
    }

    ompi_request_wait_completion(&recvreq->req_base.req_ompi);

    if (MPI_STATUS_IGNORE != status) {
        OMPI_COPY_STATUS(status, recvreq->req_base.req_ompi.req_status, false);
    }
    ret = recvreq->req_base.req_ompi.req_status.MPI_ERROR;
    ompi_request_free( (ompi_request_t**)&recvreq );

    return ret;
}

OMPI_DECLSPEC extern int mca_pml_cm_start(size_t count, ompi_request_t** requests);


OMPI_DECLSPEC extern int mca_pml_cm_dump(struct ompi_communicator_t* comm,
                                         int verbose);

OMPI_DECLSPEC extern int mca_pml_cm_cancel(struct ompi_request_t *request, int flag);

END_C_DECLS

#endif  /* PML_CM_H_HAS_BEEN_INCLUDED */
