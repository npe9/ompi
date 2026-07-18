/*
 * Copyright (c) 2013-2018 Intel, Inc. All rights reserved
 * Copyright (c) 2017      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2019-2022 Triad National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2018-2023 Amazon.com, Inc. or its affiliates.  All Rights reserved.
 *                         reserved.
 * Copyright (c) 2021      Cisco Systems, Inc.  All rights reserved
 * Copyright (c) 2021      The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2022      IBM Corporation.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef MTL_OFI_H_HAS_BEEN_INCLUDED
#define MTL_OFI_H_HAS_BEEN_INCLUDED

#include "ompi/mca/mtl/mtl.h"
#include "ompi/mca/mtl/base/base.h"
#include "opal/datatype/opal_convertor.h"
#include "opal/util/show_help.h"
#include "opal/util/printf.h"

#include <rdma/fabric.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_eq.h>
#include <rdma/fi_errno.h>
#include <rdma/fi_tagged.h>
#include <sys/epoll.h>

#include "ompi_config.h"
#include "ompi/proc/proc.h"
#include "ompi/mca/mtl/mtl.h"
#include "opal/class/opal_list.h"
#include "ompi/communicator/communicator.h"
#include "opal/datatype/opal_convertor.h"
#include "ompi/mca/mtl/base/base.h"
#include "ompi/mca/mtl/base/mtl_base_datatype.h"
#include "ompi/message/message.h"
#include "opal/mca/common/ofi/common_ofi.h"
#include "opal/mca/accelerator/base/base.h"
#include "opal/util/proc.h"

#include "mtl_ofi_opt.h"
#include "mtl_ofi_types.h"
#include "mtl_ofi_request.h"
#include "mtl_ofi_endpoint.h"
#include "mtl_ofi_compat.h"

#if HAVE_LITHE
#include <parlib/reactor.h>
#include <parlib/arch.h>
#include <lithe/fork_join_sched.h>
#include <stdlib.h>

#include "opal/mca/threads/mutex.h"
#include "mtl_ofi_hosted_sc.h"
#endif

BEGIN_C_DECLS

extern mca_mtl_ofi_module_t ompi_mtl_ofi;
extern mca_base_framework_t ompi_mtl_base_framework;

extern int ompi_mtl_ofi_add_procs(struct mca_mtl_base_module_t *mtl,
                                  size_t nprocs,
                                  struct ompi_proc_t** procs);

extern int ompi_mtl_ofi_del_procs(struct mca_mtl_base_module_t *mtl,
                                  size_t nprocs,
                                  struct ompi_proc_t **procs);

extern int ompi_mtl_ofi_add_comm(struct mca_mtl_base_module_t *mtl,
                                 struct ompi_communicator_t *comm);
extern int ompi_mtl_ofi_del_comm(struct mca_mtl_base_module_t *mtl,
                                 struct ompi_communicator_t *comm);

static inline int ompi_mtl_ofi_comm_rank(struct ompi_communicator_t *comm)
{
    return ompi_comm_rank(comm);
}

int ompi_mtl_ofi_progress_no_inline(void);
int ompi_mtl_ofi_progress_block_no_inline(void);

void ompi_mtl_ofi_thread_ctxt_set(int ctxt_id);
int ompi_mtl_ofi_thread_ctxt_get(void);

#if HAVE_LITHE
/**
 * Nested opal_progress from OFI CQ callbacks must not iterate every ctxt lock
 * (outer progress order vs another uthread). We count Lithe-context nesting via
 * parlib dtls and only drain all SEP CQs on the outermost ompi_mtl_ofi_progress
 * entry from each logical MPI rank context.
 */
int ompi_mtl_ofi_litheme_mc_outer_progress_enter(void);
void ompi_mtl_ofi_litheme_mc_outer_progress_leave(void);
#endif

#define MCA_MTL_OFI_CID_NOT_EXCHANGED 2
#define MCA_MTL_OFI_CID_EXCHANGING    1 
#define MCA_MTL_OFI_CID_EXCHANGED     0

static inline void ompi_mtl_ofi_init_cq_attr(struct fi_cq_attr *cq_attr)
{
    cq_attr->format = FI_CQ_FORMAT_TAGGED;
    cq_attr->size = ompi_mtl_ofi.ofi_progress_event_count;
#if HAVE_LITHE
    cq_attr->wait_obj = FI_WAIT_FD;
#endif
}

static inline int ompi_mtl_ofi_open_cq(struct fi_cq_attr *cq_attr,
                                       struct fid_cq **cq, int *cq_wait_fd)
{
    *cq_wait_fd = -1;
    int ret = fi_cq_open(ompi_mtl_ofi.domain, cq_attr, cq, NULL);
#if HAVE_LITHE
    if (0 == ret) {
        enum fi_wait_obj wait_obj = FI_WAIT_UNSPEC;
        ret = fi_control(&(*cq)->fid, FI_GETWAITOBJ, &wait_obj);
        if (0 == ret) {
            if (FI_WAIT_FD == wait_obj) {
                int wait_fd = -1;
                ret = fi_control(&(*cq)->fid, FI_GETWAIT, &wait_fd);
                if (0 == ret) {
                    *cq_wait_fd = wait_fd;
                    return 0;
                }
                opal_output_verbose(1, opal_common_ofi.output,
                                    "%s:%d: FI_GETWAIT failed after FI_WAIT_FD fi_cq_open: %s(%d); "
                                    "disabling Lithe blocking progress\n",
                                    __FILE__, __LINE__, fi_strerror(-ret), ret);
                ompi_mtl_ofi.progress_block_enabled = false;
                return 0;
            }
            return 0;
        }
        opal_output_verbose(1, opal_common_ofi.output,
                            "%s:%d: FI_GETWAITOBJ failed after fi_cq_open: %s(%d); "
                            "disabling Lithe blocking progress\n",
                            __FILE__, __LINE__, fi_strerror(-ret), ret);
        ompi_mtl_ofi.progress_block_enabled = false;
        return 0;
    }

    /* The Lithe block callback is only correct for fd-backed CQ waits:
     * parlib parks the continuation on FI_GETWAIT's fd and the normal OMPI
     * progress loop drains completions after readiness. If the provider rejects
     * FI_WAIT_FD, keep OFI usable but only enable blocking progress if the
     * provider's fallback confirms FI_WAIT_FD via FI_GETWAITOBJ/FI_GETWAIT. */
    opal_output_verbose(1, opal_common_ofi.output,
                        "%s:%d: FI_WAIT_FD fi_cq_open failed: %s(%d); "
                        "retrying with FI_WAIT_UNSPEC\n",
                        __FILE__, __LINE__, fi_strerror(-ret), ret);
    cq_attr->wait_obj = FI_WAIT_UNSPEC;
    ret = fi_cq_open(ompi_mtl_ofi.domain, cq_attr, cq, NULL);
    if (0 == ret) {
        enum fi_wait_obj wait_obj = FI_WAIT_UNSPEC;
        int control_ret = fi_control(&(*cq)->fid, FI_GETWAITOBJ, &wait_obj);
        if (0 == control_ret && FI_WAIT_FD == wait_obj) {
            int wait_fd = -1;
            control_ret = fi_control(&(*cq)->fid, FI_GETWAIT, &wait_fd);
            if (0 == control_ret) {
                *cq_wait_fd = wait_fd;
            } else {
                ompi_mtl_ofi.progress_block_enabled = false;
            }
        } else {
            ompi_mtl_ofi.progress_block_enabled = false;
        }
    } else {
        ompi_mtl_ofi.progress_block_enabled = false;
    }
#else
    (void)cq_wait_fd;
#endif
    return ret;
}

typedef struct {
    uint32_t c_index:30;
    uint32_t c_index_state:2;
} c_index_vec_t;

typedef struct mca_mtl_comm_t {
    opal_object_t super;
    c_index_vec_t *c_index_vec;
} mca_mtl_comm_t;

OBJ_CLASS_DECLARATION(mca_mtl_comm_t);

struct mca_mtl_ofi_cid_hdr_t {
    ompi_comm_extended_cid_t hdr_cid;
    int16_t                  hdr_src_c_index;
    int32_t                  hdr_src;
    bool                     need_response;
    bool                     ofi_cq_data;
};

typedef struct mca_mtl_ofi_cid_hdr_t mca_mtl_ofi_cid_hdr_t;

/* Set OFI context for operations which generate completion events */
__opal_attribute_always_inline__ static inline void
set_thread_context(int ctxt)
{
    ompi_mtl_ofi_thread_ctxt_set(ctxt);
}

#if HAVE_LITHE
/* Forward decl: defined below after lithe multicontext helpers. */
static inline int ompi_mtl_ofi_lithe_multicontext_ctxt_index(void);
#endif

/* Retrieve OFI context to use for CQ poll */
__opal_attribute_always_inline__ static inline void
get_thread_context(int *ctxt)
{
#if HAVE_LITHE
    /* Hosted ranks: always derive SEP ctxt from current logical vpid.
     * DTLS/thread-local caches can lag across Lithe context switches; vpid%RPH
     * is the source of truth (same as send/recv addressing). */
    {
        int lithe_ix = ompi_mtl_ofi_lithe_multicontext_ctxt_index();
        if (lithe_ix >= 0) {
            *ctxt = lithe_ix;
            return;
        }
    }
#endif
    *ctxt = ompi_mtl_ofi_thread_ctxt_get();
}

#define MTL_OFI_CONTEXT_LOCK(ctxt_id) \
OPAL_LIKELY(!opal_mutex_atomic_trylock(&ompi_mtl_ofi.ofi_ctxt[ctxt_id].context_lock))

#define MTL_OFI_CONTEXT_UNLOCK(ctxt_id) \
opal_mutex_atomic_unlock(&ompi_mtl_ofi.ofi_ctxt[ctxt_id].context_lock)

#if HAVE_LITHE
/**
 * Lithe multicontext: several MPI ranks are uthreads on the same vcore. The trylock-based
 * MTL_OFI_CONTEXT_LOCK path skips CQ progress when busy, starving peers. Use blocking
 * opal_mutex_lock on the context lock instead (recursive mutex: progress may nest from CQ callbacks).
 */
__opal_attribute_always_inline__ static inline int
ompi_mtl_ofi_lithe_multicontext_active(void)
{
    /* Cached once at first read in opal/util/proc.{h,c}. Was getenv() per call
     * before, which dominated lithified profiles (~57% of CPU at 384 ranks). */
    return opal_lithe_env_cache_active();
}

__opal_attribute_always_inline__ static inline void
mtl_ofi_cq_context_enter_multicontext(int ctxt_id)
{
    /* Spin on lithe opal_mutex_trylock + scarcity yield — NOT
     * opal_mutex_atomic_trylock (different lock word) and NOT blocking
     * opal_mutex_lock (parks the uthread for a short fi_cq_read section). */
    unsigned spin = 0;
    while (0 != opal_mutex_trylock(&ompi_mtl_ofi.ofi_ctxt[ctxt_id].context_lock)) {
        if ((++spin & 63u) == 0u && lithe_fork_join_should_yield_to_runnable()) {
            lithe_context_yield();
        } else {
            cpu_relax();
        }
    }
}

/* Non-blocking variant: returns 1 if the ctxt lock was acquired, 0 otherwise.
 * Used by the peer-ctxt sweep in ompi_mtl_ofi_progress so that an outer
 * progress call never blocks on a peer uthread currently draining its CQ.
 * Must use lithe opal_mutex_trylock (same lock as enter/exit), not atomic. */
__opal_attribute_always_inline__ static inline int
mtl_ofi_cq_context_trylock_multicontext(int ctxt_id)
{
    return (0 == opal_mutex_trylock(&ompi_mtl_ofi.ofi_ctxt[ctxt_id].context_lock));
}

__opal_attribute_always_inline__ static inline void
mtl_ofi_cq_context_exit_multicontext(int ctxt_id)
{
    opal_mutex_unlock(&ompi_mtl_ofi.ofi_ctxt[ctxt_id].context_lock);
}

/**
 * Lithe hosted multicontext: logical MPI ranks are Lithe contexts with distinct opal vpid.
 * RTE assigns vpids H*RPH+k for host index H and local slot k in [0,RPH). OFI scalable endpoints use
 * one tx/rx context index per slot — it MUST match k (vpid % RPH), not communicator cid % N.
 * Using cid tied all contexts to the same OFI context, breaking cross-node matching/collectives.
 */
__opal_attribute_always_inline__ static inline int
ompi_mtl_ofi_lithe_multicontext_ctxt_index(void)
{
    unsigned long rph;

    if (!ompi_mtl_ofi_lithe_multicontext_active() || ompi_mtl_ofi.total_ctxts_used <= 0) {
        return -1;
    }
    rph = opal_lithe_env_cache_rph();
    if (rph < 2UL) {
        return -1;
    }
    {
        opal_vpid_t v = opal_proc_local_get()->proc_name.vpid;
        int ctxt = (int) ((uint64_t) v % rph);
        if (ctxt >= ompi_mtl_ofi.total_ctxts_used) {
            ctxt %= ompi_mtl_ofi.total_ctxts_used;
        }
        return ctxt;
    }
}

__opal_attribute_always_inline__ static inline int
ompi_mtl_ofi_ctxt_index_for_comm(struct ompi_communicator_t *comm)
{
    int lithe_ix = ompi_mtl_ofi_lithe_multicontext_ctxt_index();
    if (lithe_ix >= 0) {
        return lithe_ix;
    }
    if (ompi_mtl_ofi.total_ctxts_used > 0) {
        return (int) (comm->c_contextid.cid_sub.u64 % (uint64_t) ompi_mtl_ofi.total_ctxts_used);
    }
    return 0;
}
#else
__opal_attribute_always_inline__ static inline int ompi_mtl_ofi_lithe_multicontext_active(void)
{
    return 0;
}
static inline int ompi_mtl_ofi_ctxt_index_for_comm(struct ompi_communicator_t *comm)
{
    if (ompi_mtl_ofi.total_ctxts_used > 0) {
        return (int) (comm->c_contextid.cid_sub.u64 % (uint64_t) ompi_mtl_ofi.total_ctxts_used);
    }
    return 0;
}
#endif /* HAVE_LITHE */

/**
 * fi_rx_addr() for a send: second parameter is the **peer's** scalable-endpoint RX context index.
 * Lithe multicontext: that index is the peer's logical slot (vpid % RPH), not the sender's local TX ctxt.
 * Receive-side fi_rx_addr uses the local receive context index (caller passes \p local_ctxt).
 */
#if HAVE_LITHE
__opal_attribute_always_inline__ static inline int
ompi_mtl_ofi_sep_peer_rx_ctxt(struct ompi_proc_t *peer_proc, int local_ctxt)
{
    unsigned long rph;

    if (NULL == peer_proc) {
        return local_ctxt;
    }
    if (!ompi_mtl_ofi_lithe_multicontext_active()) {
        return local_ctxt;
    }
    /* Multi regular-EP: peer_fiaddr already selects the peer's EP; do not
     * apply SEP rx-ctxt encoding (rx_ctx_bits==0, but be explicit). */
    if (ompi_mtl_ofi.hosted_multi_ep) {
        return 0;
    }
    rph = opal_lithe_env_cache_rph();
    if (rph < 2UL) {
        return local_ctxt;
    }
    {
        opal_vpid_t pv = peer_proc->super.proc_name.vpid;
        int ctxt;

        if (pv == OPAL_VPID_INVALID || pv == OPAL_VPID_WILDCARD) {
            return local_ctxt;
        }
        ctxt = (int) ((uint64_t) pv % rph);
        if (ompi_mtl_ofi.total_ctxts_used > 0 && ctxt >= ompi_mtl_ofi.total_ctxts_used) {
            ctxt %= ompi_mtl_ofi.total_ctxts_used;
        }
        return ctxt;
    }
}
#else
__opal_attribute_always_inline__ static inline int
ompi_mtl_ofi_sep_peer_rx_ctxt(struct ompi_proc_t *peer_proc, int local_ctxt)
{
    (void) peer_proc;
    return local_ctxt;
}
#endif /* HAVE_LITHE */

__opal_attribute_always_inline__ static inline int
ompi_mtl_ofi_context_process_cq(int ctxt_id, struct fi_cq_tagged_entry *wc, ssize_t ret)
{
    int count = 0, i, events_read;
    ompi_mtl_ofi_request_t *ofi_req = NULL;
    struct fi_cq_err_entry error = { 0 };

    if (ret > 0) {
        count+= ret;
        events_read = ret;
        for (i = 0; i < events_read; i++) {
            if (NULL != wc[i].op_context) {
                ofi_req = TO_OFI_REQ(wc[i].op_context);
                assert(ofi_req);
                ret = ofi_req->event_callback(&wc[i], ofi_req);
                if (OMPI_SUCCESS != ret) {
                    opal_output(0, "%s:%d: Error returned by request event callback: %zd.\n"
                                   "*** The Open MPI OFI MTL is aborting the MPI job (via exit(3)).\n",
                                   __FILE__, __LINE__, ret);
                    fflush(stderr);
                    exit(1);
                }
            }
        }
    } else if (OPAL_UNLIKELY(ret == -FI_EAVAIL)) {
        /**
         * An error occurred and is being reported via the CQ.
         * Read the error and forward it to the upper layer.
         */
        ret = fi_cq_readerr(ompi_mtl_ofi.ofi_ctxt[ctxt_id].cq,
                            &error,
                            0);
        if (0 > ret) {
            /*
             * In multi-threaded scenarios, any thread that attempts to read
             * a CQ when there's a pending error CQ entry gets an
             * -FI_EAVAIL. Without any serialization here (which is okay,
             * since libfabric will protect access to critical CQ objects),
             * all threads proceed to read from the error CQ, but only one
             * thread fetches the entry while others get -FI_EAGAIN
             * indicating an empty queue, which is not erroneous.
             */
            if (ret == -FI_EAGAIN)
                return count;
            opal_output(0, "%s:%d: Error returned from fi_cq_readerr: %s(%zd).\n"
                           "*** The Open MPI OFI MTL is aborting the MPI job (via exit(3)).\n",
                           __FILE__, __LINE__, fi_strerror(-ret), ret);
            fflush(stderr);
            exit(1);
        }

        assert(error.op_context);
        ofi_req = TO_OFI_REQ(error.op_context);
        assert(ofi_req);
        ret = ofi_req->error_callback(&error, ofi_req);
        if (OMPI_SUCCESS != ret) {
                opal_output(0, "%s:%d: Error returned by request error callback: %zd.\n"
                               "*** The Open MPI OFI MTL is aborting the MPI job (via exit(3)).\n",
                               __FILE__, __LINE__, ret);
            fflush(stderr);
            exit(1);
        }
    } else if (ret != -FI_EAGAIN && ret != -EINTR) {
        opal_output(0, "%s:%d: Error returned from fi_cq_read: %s(%zd).\n"
                       "*** The Open MPI OFI MTL is aborting the MPI job (via exit(3)).\n",
                       __FILE__, __LINE__, fi_strerror(-ret), ret);
        fflush(stderr);
        exit(1);
    }

    return count;
}

__opal_attribute_always_inline__ static inline int
ompi_mtl_ofi_context_progress(int ctxt_id)
{
    ssize_t ret;
    struct fi_cq_tagged_entry ompi_mtl_ofi_wc[MTL_OFI_MAX_PROG_EVENT_COUNT];

    /**
     * Read the work completions from the CQ.
     * From the completion's op_context, we get the associated OFI request.
     * Call the request's callback.
     */
    ret = fi_cq_read(ompi_mtl_ofi.ofi_ctxt[ctxt_id].cq, (void *)&ompi_mtl_ofi_wc,
                     ompi_mtl_ofi.ofi_progress_event_count);
    return ompi_mtl_ofi_context_process_cq(ctxt_id, ompi_mtl_ofi_wc, ret);
}

__opal_attribute_always_inline__ static inline int
ompi_mtl_ofi_context_progress_block(int ctxt_id)
{
    int wait_fd = ompi_mtl_ofi.ofi_ctxt[ctxt_id].cq_wait_fd;
    int count = 0;

    if (wait_fd < 0) {
        return 0;
    }

    /* opal_progress() polls low-priority callbacks only periodically. Before
     * parking, force one nonblocking CQ drain so short collective reductions do
     * not pay an epoll handoff just because this progress tick skipped OFI. */
    count = ompi_mtl_ofi_context_progress(ctxt_id);
    if (count > 0) {
        return count;
    }

#if HAVE_LITHE
    /* Bounded CQ spin. Yield to RUNNABLE peers only under hart scarcity
     * (lithe_fork_join_should_yield_to_runnable); yielding whenever
     * runnable_count>0 causes a two-waiter yield-storm on VCORE>=K. */
    {
        unsigned int spin_max = lithe_progress_spin_max();
        for (unsigned int si = 0; si < spin_max; si++) {
            count = ompi_mtl_ofi_context_progress(ctxt_id);
            if (count > 0)
                return count;
            if (lithe_fork_join_should_yield_to_runnable()) {
                lithe_context_yield();
                count = ompi_mtl_ofi_context_progress(ctxt_id);
                if (count > 0)
                    return count;
                continue;
            }
            cpu_relax();
        }
        if (lithe_fork_join_should_yield_to_runnable()) {
            lithe_context_yield();
            count = ompi_mtl_ofi_context_progress(ctxt_id);
            if (count > 0)
                return count;
        }
    }
#endif

    /* Lithe request waits are continuations, not pthread waits. Use OFI only
     * for its waitable readiness fd, then drain completions through OMPI's
     * normal CQ processor after readiness. This avoids both busy polling and
     * an unbounded fi_cq_sread() that can sleep before non-CQ collective state
     * has been advanced. */
    int rev = parlib_reactor_wait(wait_fd, EPOLLIN | EPOLLERR | EPOLLHUP, -1);
    if (rev <= 0) {
        return 0;
    }

    return ompi_mtl_ofi_context_progress(ctxt_id);
}

#if HAVE_LITHE
/* Wake any uthread parked on this CQ wait_fd. Required whenever a co-resident
 * context drains events that another waiter may have armed for — otherwise
 * unlock-around-park races become lost-wakeup hangs. */
__opal_attribute_always_inline__ static inline void
mtl_ofi_mc_wake_cq_waiters(int ctxt_id, int got)
{
    int wfd;
    if (got <= 0) {
        return;
    }
    wfd = ompi_mtl_ofi.ofi_ctxt[ctxt_id].cq_wait_fd;
    if (wfd >= 0) {
        (void) parlib_reactor_cancel_fd(wfd);
    }
}

/* Own-CQ drain + optional peer trylock sweep. Never parks.
 * blocking_own=0: trylock own CQ (spin path — do not block a hart on the
 * shared no-SEP mutex while a peer holds it for fi_cq_read).
 * blocking_own=1: blocking lock (short non-spin progress / post-wake drain). */
__opal_attribute_always_inline__ static inline int
mtl_ofi_mc_progress_once(int ctxt_id, int outer, int sweep_peers, int blocking_own)
{
    int count = 0;

    if (blocking_own) {
        mtl_ofi_cq_context_enter_multicontext(ctxt_id);
    } else if (!mtl_ofi_cq_context_trylock_multicontext(ctxt_id)) {
        return 0;
    }
    count += ompi_mtl_ofi_context_progress(ctxt_id);
    mtl_ofi_cq_context_exit_multicontext(ctxt_id);
    mtl_ofi_mc_wake_cq_waiters(ctxt_id, count);

    if (outer && sweep_peers && (count == 0 || ompi_mtl_ofi.hosted_multi_ep)) {
        int j;
        int n = ompi_mtl_ofi.total_ctxts_used;
        for (j = 1; j < n; j++) {
            int k = (ctxt_id + j) % n;
            if (mtl_ofi_cq_context_trylock_multicontext(k)) {
                int got = ompi_mtl_ofi_context_progress(k);
                count += got;
                mtl_ofi_mc_wake_cq_waiters(k, got);
                mtl_ofi_cq_context_exit_multicontext(k);
            }
        }
    }
    return count;
}
#endif /* HAVE_LITHE */

__opal_attribute_always_inline__ static inline int
ompi_mtl_ofi_progress(void)
{
    int count = 0, ctxt_id = 0, i;
    static volatile uint32_t num_calls = 0;

    get_thread_context(&ctxt_id);

    if (ompi_mtl_ofi_lithe_multicontext_active()) {
#if HAVE_LITHE
        /* Hosted multicontext: drain own SEP CQ first, then (outermost only)
         * trylock-sweep every peer CQ. The prior every-16th / stop-on-first
         * throttle was for process-per-rank flamegraphs; at RPH>=5 it left
         * peer completions stranded and Allreduce returned wrong sums.
         * Nested CQ→opal_progress still drains only the invoking ctxt. */
        int outer = ompi_mtl_ofi_litheme_mc_outer_progress_enter();

        mtl_ofi_cq_context_enter_multicontext(ctxt_id);
        count += ompi_mtl_ofi_context_progress(ctxt_id);
        mtl_ofi_cq_context_exit_multicontext(ctxt_id);
        /* Shared no-SEP CQ: any drain must wake parkers (unlock-around-park). */
        mtl_ofi_mc_wake_cq_waiters(ctxt_id, count);

        /* Multi regular-EP: always trylock-sweep peer CQs on outer progress.
         * Completions land on the destination EP's CQ; skipping the sweep
         * when own CQ had TX events stranded peer recvs (P1K2 hang).
         * After draining a peer CQ, cancel any uthread parked on that CQ's
         * wait_fd — fi_cq_read consumes the event that would have woken them
         * (lost-wakeup hang). Same cancel applies to shared single-CQ. */
        if (outer && (count == 0 || ompi_mtl_ofi.hosted_multi_ep)) {
            int j;
            int n = ompi_mtl_ofi.total_ctxts_used;
            (void) num_calls;
            for (j = 1; j < n; j++) {
                int k = (ctxt_id + j) % n;
                if (mtl_ofi_cq_context_trylock_multicontext(k)) {
                    int got = ompi_mtl_ofi_context_progress(k);
                    count += got;
                    mtl_ofi_mc_wake_cq_waiters(k, got);
                    mtl_ofi_cq_context_exit_multicontext(k);
                }
            }
        }
        ompi_mtl_ofi_litheme_mc_outer_progress_leave();
        return count;
#endif
    } else if (ompi_mtl_ofi.mpi_thread_multiple) {
        if (MTL_OFI_CONTEXT_LOCK(ctxt_id)) {
            count += ompi_mtl_ofi_context_progress(ctxt_id);
            MTL_OFI_CONTEXT_UNLOCK(ctxt_id);
        }
    } else {
        count += ompi_mtl_ofi_context_progress(ctxt_id);
    }

#if OPAL_HAVE_THREAD_LOCAL
    /*
     * Try to progress other CQs in round-robin fashion.
     * Progress is only made if no events were read from the CQ
     * local to the calling thread past 16 times.
     * Lithe multicontext: outer ompi_mtl_ofi_progress drains every ctxt CQ sequentially
     * but nested entries (CQ callbacks calling opal_progress) drain only thread-local ctxt
     * via ompi_mtl_ofi_litheme_mc_outer_progress_enter().
     */
    if (OPAL_UNLIKELY((count == 0) && !ompi_mtl_ofi_lithe_multicontext_active() &&
        ompi_mtl_ofi.mpi_thread_multiple && (((num_calls++) & 0xF) == 0 ))) {
        for (i = 0; i < ompi_mtl_ofi.total_ctxts_used - 1; i++) {
            ctxt_id = (ctxt_id + 1) % ompi_mtl_ofi.total_ctxts_used;

            if (MTL_OFI_CONTEXT_LOCK(ctxt_id)) {
                count += ompi_mtl_ofi_context_progress(ctxt_id);
                MTL_OFI_CONTEXT_UNLOCK(ctxt_id);
            }

            /* Upon any work done, exit to let other threads take lock */
            if (OPAL_LIKELY(count > 0)) {
                break;
            }
        }
    }
#endif

    return count;
}

__opal_attribute_always_inline__ static inline int
ompi_mtl_ofi_progress_block(void)
{
    int count = 0, ctxt_id = 0;
    static volatile uint32_t num_calls_block = 0;

    get_thread_context(&ctxt_id);

    if (ompi_mtl_ofi_lithe_multicontext_active()) {
#if HAVE_LITHE
        /* Hosted ranks share CQ(s). NEVER hold context_lock across CQ spin,
         * lithe_context_yield, or parlib_reactor_wait — that serialized all
         * co-resident ranks on one Lithe mutex (P1K2 ~56–180µs vs vanilla ~6µs)
         * and turned should_yield into yield-while-holding-lock churn.
         * Lock only around fi_cq_read; wake parkers on every successful drain. */
        int outer = ompi_mtl_ofi_litheme_mc_outer_progress_enter();
        int wait_fd = ompi_mtl_ofi.ofi_ctxt[ctxt_id].cq_wait_fd;
        unsigned int spin_max = lithe_progress_spin_max();
        unsigned int si;
        (void) num_calls_block;

        /* Spin with short blocking CQ drains (lock held only around fi_cq_read).
         * Pure trylock+infinite-park deadlocked co-resident ranks on shared
         * no-SEP CQ (both park, nobody drains). Prefer yield under scarcity
         * over infinite park when RPH>=2.
         *
         * Single-OS + SC: when harts are scarce, park on the posted SC recv
         * instead of yield→empty FJS steal (P1K4 residual). Keep the spin so a
         * helper hart can finish the peer send before we block (early-park
         * before spin_max regressed P1K4 to ~70µs). Multi-OS: unchanged. */
        count = mtl_ofi_mc_progress_once(ctxt_id, outer, 1, 1);
        if (count > 0) {
            ompi_mtl_ofi_litheme_mc_outer_progress_leave();
            return count;
        }

        for (si = 0; si < spin_max; si++) {
            count = mtl_ofi_mc_progress_once(ctxt_id, outer, 1, 1);
            if (count > 0) {
                ompi_mtl_ofi_litheme_mc_outer_progress_leave();
                return count;
            }
            if (lithe_fork_join_should_yield_to_runnable()) {
                if (ompi_mtl_ofi_hosted_sc_enabled()
                    && ompi_mtl_ofi_hosted_sc_try_park_pending()) {
                    count = mtl_ofi_mc_progress_once(ctxt_id, outer, 1, 1);
                    ompi_mtl_ofi_litheme_mc_outer_progress_leave();
                    return count;
                }
                lithe_context_yield();
                continue;
            }
            cpu_relax();
        }

        /* Single-OS-process hosted (LITHE_MTL_OFI_SINGLE_OS=1 from launcher when
         * NTASKS==1): do not park on the CQ fd — shared no-SEP waiters race on
         * edge wakeups, and park floors P1K2 Barrier+Allreduce. SC park under
         * scarcity (above); else yield if scarce, drain once more, return to
         * wait_sync. Multi-OS: CQ park so remote completions can wake us
         * (P2K2 needs this; no-park → ~800µs). */
        {
            static int single_os = -1;
            if (single_os < 0) {
                const char *e = getenv("LITHE_MTL_OFI_SINGLE_OS");
                single_os = (e && e[0] == '1' && e[1] == '\0') ? 1 : 0;
            }
            if (single_os || wait_fd < 0) {
                if (lithe_fork_join_should_yield_to_runnable()) {
                    if (single_os && ompi_mtl_ofi_hosted_sc_enabled()
                        && ompi_mtl_ofi_hosted_sc_try_park_pending()) {
                        count = mtl_ofi_mc_progress_once(ctxt_id, outer, 1, 1);
                        ompi_mtl_ofi_litheme_mc_outer_progress_leave();
                        return count;
                    }
                    lithe_context_yield();
                }
                count = mtl_ofi_mc_progress_once(ctxt_id, outer, 1, 1);
                ompi_mtl_ofi_litheme_mc_outer_progress_leave();
                return count;
            }
        }
        /* Multi-OS: serialize CQ park behind context_lock (one waiter on the
         * shared wait_fd). Yield only under hart scarcity — not whenever
         * runnable_count>0 (the park+pump helper is often RUNNABLE and would
         * suppress CQ park entirely → no kernel entry → cxi hang). */
        if (lithe_fork_join_should_yield_to_runnable()) {
            lithe_context_yield();
            count = mtl_ofi_mc_progress_once(ctxt_id, outer, 1, 1);
            ompi_mtl_ofi_litheme_mc_outer_progress_leave();
            return count;
        }
        mtl_ofi_cq_context_enter_multicontext(ctxt_id);
        count += ompi_mtl_ofi_context_progress_block(ctxt_id);
        mtl_ofi_cq_context_exit_multicontext(ctxt_id);
        mtl_ofi_mc_wake_cq_waiters(ctxt_id, count);
        ompi_mtl_ofi_litheme_mc_outer_progress_leave();
        return count;
#endif
    } else {
        count += ompi_mtl_ofi_context_progress_block(ctxt_id);
    }

    return count;
}

/**
 * Progress after -FI_EAGAIN on a specific TX/RX context.
 * Lithe multicontext: avoid full ompi_mtl_ofi_progress() from FI call paths — it may
 * run nested round-robin or confuse lock order with the recursive context mutex. Drain
 * only this ctxt's CQ with the same enter/exit as ompi_mtl_ofi_progress.
 */
__opal_attribute_always_inline__ static inline void
ompi_mtl_ofi_progress_after_eagain(int ctxt_id)
{
#if HAVE_LITHE
    if (ompi_mtl_ofi_lithe_multicontext_active()) {
        mtl_ofi_cq_context_enter_multicontext(ctxt_id);
        (void) ompi_mtl_ofi_context_progress(ctxt_id);
        mtl_ofi_cq_context_exit_multicontext(ctxt_id);
        return;
    }
#endif
    (void) ompi_mtl_ofi_progress(); /* pthread / single-rank: preserve global progress */
}

/**
 * When attempting to execute an OFI operation we need to handle
 * resource overrun cases. When a call to an OFI OP fails with -FI_EAGAIN
 * the OFI mtl will attempt to progress any pending Completion Queue
 * events that may prevent additional operations to be enqueued.
 * If the call to ofi progress is successful, then the function call
 * will be retried.
 *
 * Use MTL_OFI_RETRY_UNTIL_DONE_CTXT when ctxt_id is the OFI context index for this op
 * (required for Lithe multicontext correctness).
 */
#if HAVE_LITHE
/*
 * Hosted + single regular EP (no SEP / no multi-EP): cxi is not safe for
 * concurrent fi_t* from multiple Lithe contexts on one EP — Allreduce wrong
 * sums at RPH>=4. Hold the ctxt lock only around the fabric post (not the
 * wait), so peers can still enter the collective and progress.
 */
#define MTL_OFI_RETRY_UNTIL_DONE_CTXT(FUNC, RETURN, CTXT_ID)                 \
    do {                                                                     \
        do {                                                                 \
            const int _mtl_ofi_ser_post =                                    \
                (ompi_mtl_ofi_lithe_multicontext_active() &&                  \
                 0 == ompi_mtl_ofi.enable_sep &&                             \
                 0 == ompi_mtl_ofi.hosted_multi_ep);                         \
            if (_mtl_ofi_ser_post) {                                         \
                mtl_ofi_cq_context_enter_multicontext(CTXT_ID);              \
            }                                                                \
            RETURN = FUNC;                                                   \
            if (_mtl_ofi_ser_post) {                                         \
                mtl_ofi_cq_context_exit_multicontext(CTXT_ID);               \
            }                                                                \
            if (OPAL_LIKELY(0 == RETURN)) {                                  \
                break;                                                       \
            }                                                                \
            if (OPAL_LIKELY(RETURN == -FI_EAGAIN)) {                         \
                ompi_mtl_ofi_progress_after_eagain(CTXT_ID);                 \
            }                                                                \
        } while (OPAL_LIKELY(-FI_EAGAIN == RETURN));                        \
    } while (0)
#else
#define MTL_OFI_RETRY_UNTIL_DONE_CTXT(FUNC, RETURN, CTXT_ID)  \
    do {                                                      \
        do {                                                  \
            RETURN = FUNC;                                    \
            if (OPAL_LIKELY(0 == RETURN)) {                   \
                break;                                        \
            }                                                 \
            if (OPAL_LIKELY(RETURN == -FI_EAGAIN)) {          \
                ompi_mtl_ofi_progress_after_eagain(CTXT_ID);  \
            }                                                 \
        } while (OPAL_LIKELY(-FI_EAGAIN == RETURN));         \
    } while (0)
#endif

#define MTL_OFI_RETRY_UNTIL_DONE(FUNC, RETURN) \
    MTL_OFI_RETRY_UNTIL_DONE_CTXT((FUNC), (RETURN), 0)

#define MTL_OFI_LOG_FI_ERR(err, string)                                     \
    do {                                                                    \
        opal_output_verbose(1, opal_common_ofi.output,                      \
                            "%s:%d:%s: %s\n",                               \
                            __FILE__, __LINE__, string, fi_strerror(-err)); \
    } while(0);

/**
 * Memory registration functions
 */

/** Called before any libfabric or registration calls */
__opal_attribute_always_inline__ static inline void
ompi_mtl_ofi_set_mr_null(ompi_mtl_ofi_request_t *ofi_req) {
    ofi_req->mr = NULL;
}

/**
 * Registers user buffer with Libfabric domain if
 * buffer is a device buffer and provider has fi_mr_hmem
 */
static
int ompi_mtl_ofi_register_buffer(struct opal_convertor_t *convertor,
                                 ompi_mtl_ofi_request_t *ofi_req,
                                 void* buffer) {
    ofi_req->mr = NULL;
    if (ofi_req->length <= 0 || NULL == buffer) {
        return OMPI_SUCCESS;
    }

#if OPAL_OFI_HAVE_FI_MR_IFACE

    if ((convertor->flags & CONVERTOR_ACCELERATOR) && ompi_mtl_ofi.hmem_needs_reg) {
        /* Register buffer */
        int ret;
        struct fi_mr_attr attr = {0};
        struct iovec iov = {0};

        iov.iov_base = buffer;
        iov.iov_len = ofi_req->length;
        attr.mr_iov = &iov;
        attr.iov_count = 1;
        attr.access = FI_SEND | FI_RECV;
        attr.offset = 0;
        attr.context = NULL;
        if (false == ompi_mtl_base_selected_component->accelerator_support) {
            goto reg;
        } else if (0 == strcmp(opal_accelerator_base_selected_component.base_version.mca_component_name, "cuda")) {
            attr.iface = FI_HMEM_CUDA;
            opal_accelerator.get_device(&attr.device.cuda);
#if OPAL_OFI_HAVE_FI_HMEM_ROCR
        } else if (0 == strcmp(opal_accelerator_base_selected_component.base_version.mca_component_name, "rocm")) {
            attr.iface = FI_HMEM_ROCR;
            opal_accelerator.get_device(&attr.device.cuda);
#endif
        } else {
            return OPAL_ERROR;
        }
reg:
        ret = fi_mr_regattr(ompi_mtl_ofi.domain, &attr, 0, &ofi_req->mr);

        if (ret) {
            opal_show_help("help-mtl-ofi.txt", "Buffer Memory Registration Failed", true,
                           opal_accelerator_base_selected_component.base_version.mca_component_name,
                           buffer, ofi_req->length,
                           fi_strerror(-ret), ret);
            ofi_req->mr = NULL;
            return OMPI_ERROR;
        }
    }

#endif

    return OMPI_SUCCESS;
}

/** Deregister buffer */
__opal_attribute_always_inline__ static inline int
ompi_mtl_ofi_deregister_buffer(ompi_mtl_ofi_request_t *ofi_req) {
    if (ofi_req->mr) {
        int ret;
        ret = fi_close(&ofi_req->mr->fid);
        if (ret) {
            opal_show_help("help-mtl-ofi.txt", "OFI call fail", true,
                           "fi_close",
                           ompi_process_info.nodename, __FILE__, __LINE__,
                           fi_strerror(-ret), ofi_req->mr->fid);
            return OMPI_ERROR;
        }
        ofi_req->mr = NULL;
    }
    return OMPI_SUCCESS;
}

/** Deregister and free a buffer */
static
int ompi_mtl_ofi_deregister_and_free_buffer(ompi_mtl_ofi_request_t *ofi_req) {
    int ret = OMPI_SUCCESS;
    ret = ompi_mtl_ofi_deregister_buffer(ofi_req);
    if (OPAL_UNLIKELY(OMPI_SUCCESS != ret)) {
        return ret;
    }
    if (OPAL_UNLIKELY(NULL != ofi_req->buffer)) {
        /* If no convertor has been provided, assume it's a host buffer */
        if (NULL == ofi_req->convertor) {
            free(ofi_req->buffer);
        /* If the convertor is not an accelerator buffer, or we do not have
           accelerator_support, it must be a host buffer. */
        } else if (!(ofi_req->convertor->flags & CONVERTOR_ACCELERATOR) ||
            false == ompi_mtl_base_selected_component->accelerator_support) {
            free(ofi_req->buffer);
        /* The buffer must be an accelerator buffer */
        } else {
            ret = opal_accelerator.mem_release(MCA_ACCELERATOR_NO_DEVICE_ID, ofi_req->buffer);
        }
    }
    ofi_req->buffer = NULL;
    return ret;
}

/* MTL interface functions */
int ompi_mtl_ofi_finalize(struct mca_mtl_base_module_t *mtl);

__opal_attribute_always_inline__ static inline int
ompi_mtl_ofi_get_error(int error_num)
{
    int ret;

    switch (error_num) {
    case 0:
        ret = OMPI_SUCCESS;
        break;
    default:
        ret = OMPI_ERROR;
    }

    return ret;
}

__opal_attribute_always_inline__ static inline int
ompi_mtl_ofi_send_callback(struct fi_cq_tagged_entry *wc,
                           ompi_mtl_ofi_request_t *ofi_req)
{
    assert(ofi_req->completion_count > 0);
    ofi_req->completion_count--;
    return OMPI_SUCCESS;
}


/*
 * special send callback for excid send operation.
 * Since the send excid operation cannot block
 * waiting for completion of the send operation,
 * we have to free the internal message buffer allocated
 * as part of the excid operation here as well as the
 * request itself.
 */
__opal_attribute_always_inline__ static inline int
ompi_mtl_ofi_send_excid_callback(struct fi_cq_tagged_entry *wc,
                           ompi_mtl_ofi_request_t *ofi_req)
{
    assert(ofi_req->completion_count > 0);
    free(ofi_req->buffer);
    ofi_req->completion_count--; /* no one's waiting on this */
    free(ofi_req);
    return OMPI_SUCCESS;
}

__opal_attribute_always_inline__ static inline int
ompi_mtl_ofi_send_error_callback(struct fi_cq_err_entry *error,
                                 ompi_mtl_ofi_request_t *ofi_req)
{
    switch(error->err) {
        case FI_ETRUNC:
            ofi_req->status.MPI_ERROR = MPI_ERR_TRUNCATE;
            break;
        default:
            ofi_req->status.MPI_ERROR = MPI_ERR_INTERN;
    }
    return ofi_req->event_callback(NULL, ofi_req);
}

__opal_attribute_always_inline__ static inline int
ompi_mtl_ofi_send_ack_callback(struct fi_cq_tagged_entry *wc,
                               ompi_mtl_ofi_request_t *ofi_req)
{
    ompi_mtl_ofi_request_t *parent_req = ofi_req->parent;

    free(ofi_req);

    parent_req->event_callback(NULL, parent_req);

    return OMPI_SUCCESS;
}

__opal_attribute_always_inline__ static inline int
ompi_mtl_ofi_send_ack_error_callback(struct fi_cq_err_entry *error,
                                     ompi_mtl_ofi_request_t *ofi_req)
{
    ompi_mtl_ofi_request_t *parent_req = ofi_req->parent;

    free(ofi_req);

    parent_req->status.MPI_ERROR = MPI_ERR_INTERN;

    return parent_req->error_callback(error, parent_req);
}

__opal_attribute_always_inline__ static inline int
ompi_mtl_ofi_isend_callback(struct fi_cq_tagged_entry *wc,
                            ompi_mtl_ofi_request_t *ofi_req)
{
    assert(ofi_req->completion_count > 0);
    ofi_req->completion_count--;

    if (0 == ofi_req->completion_count) {
        /* Request completed */
        ompi_mtl_ofi_deregister_and_free_buffer(ofi_req);

        ofi_req->super.ompi_req->req_status.MPI_ERROR =
            ofi_req->status.MPI_ERROR;

        ofi_req->super.completion_callback(&ofi_req->super);
    }

    return OMPI_SUCCESS;
}

/* Return OFI context ID associated with the specific communicator */
__opal_attribute_always_inline__ static inline int
ompi_mtl_ofi_map_comm_to_ctxt(uint32_t comm_id)
{
    /* For non-thread-grouping use case, only one context is used which is
     * associated to MPI_COMM_WORLD, so use that. */
    if (0 == ompi_mtl_ofi.thread_grouping) {
        comm_id = 0;
    }

    return ompi_mtl_ofi.comm_to_context[comm_id];
}

__opal_attribute_always_inline__ static inline int
ompi_mtl_ofi_post_recv_excid_buffer(bool blocking, struct ompi_communicator_t *comm, int src);

__opal_attribute_always_inline__ static inline int
ompi_mtl_ofi_send_excid(struct mca_mtl_base_module_t *mtl,
                  struct ompi_communicator_t *comm,
                  int dest,
                  bool ofi_cq_data,
                  bool is_send);

__opal_attribute_always_inline__ static inline int
ompi_mtl_ofi_recv_excid_error_callback(struct fi_cq_err_entry *error,
                                 ompi_mtl_ofi_request_t *ofi_req)
{
    ompi_status_public_t *status;
    assert(ofi_req->super.ompi_req);
    status = &ofi_req->super.ompi_req->req_status;
    status->MPI_TAG = MTL_OFI_GET_TAG(ofi_req->match_bits);
    status->MPI_SOURCE = mtl_ofi_get_source((struct fi_cq_tagged_entry *) error);

    switch (error->err) {
        case FI_ETRUNC:
            status->MPI_ERROR = MPI_ERR_TRUNCATE;
            break;
        case FI_ECANCELED:
            status->_cancelled = true;
            break;
        default:
            status->MPI_ERROR = MPI_ERR_INTERN;
    }

    ofi_req->super.completion_callback(&ofi_req->super);
    return OMPI_SUCCESS;
}

__opal_attribute_always_inline__ static inline int
ompi_mtl_ofi_post_recv_excid_buffer_callback(struct fi_cq_tagged_entry *wc,
                                             ompi_mtl_ofi_request_t *ofi_req)
{
    ofi_req->completion_count--;
    int ret;
    mca_mtl_ofi_cid_hdr_t *buffer = (mca_mtl_ofi_cid_hdr_t *)ofi_req->buffer;
    ompi_comm_extended_cid_t excid;
    ompi_communicator_t *comm;
    int src = buffer->hdr_src;
    mca_mtl_comm_t *mtl_comm;

    excid.cid_base = buffer->hdr_cid.cid_base;
    excid.cid_sub.u64 = buffer->hdr_cid.cid_sub.u64;
    for (int i = 0; i < 8; i++) {
        excid.cid_sub.u8[i] = buffer->hdr_cid.cid_sub.u8[i];
    }

    comm = ompi_comm_lookup_cid(excid);
    if (comm == NULL) {
        comm = ompi_comm_lookup(buffer->hdr_src_c_index);
    }

    if (comm == NULL) {
        return OMPI_SUCCESS;
    }

    mtl_comm = comm->c_mtl_comm;

    if (mtl_comm->c_index_vec[src].c_index_state == MCA_MTL_OFI_CID_NOT_EXCHANGED
        && buffer->need_response) {
        mtl_comm->c_index_vec[src].c_index = buffer->hdr_src_c_index;
        mtl_comm->c_index_vec[src].c_index_state = MCA_MTL_OFI_CID_EXCHANGED;
        ret = ompi_mtl_ofi_send_excid(ofi_req->mtl, comm, src, buffer->ofi_cq_data, false);
    } else {
        mtl_comm->c_index_vec[src].c_index_state = MCA_MTL_OFI_CID_EXCHANGED;
        mtl_comm->c_index_vec[src].c_index = buffer->hdr_src_c_index;
    }

    ret = ompi_mtl_ofi_post_recv_excid_buffer(false, comm, -1);
    return ret;
}

__opal_attribute_always_inline__ static inline int
ompi_mtl_ofi_post_recv_excid_buffer(bool blocking, struct ompi_communicator_t *comm, int src)
{
    int ctxt_id = 0;
    ssize_t ret;
    ompi_mtl_ofi_request_t *ofi_req = malloc(sizeof(ompi_mtl_ofi_request_t));
    mca_mtl_ofi_cid_hdr_t *start = malloc(sizeof(mca_mtl_ofi_cid_hdr_t));
    size_t length = sizeof(mca_mtl_ofi_cid_hdr_t);
    mca_mtl_comm_t *mtl_comm;
    
    mtl_comm = comm->c_mtl_comm;

    ctxt_id = ompi_mtl_ofi_ctxt_index_for_comm(comm);
    set_thread_context(ctxt_id);

    ofi_req->type = OMPI_MTL_OFI_RECV;
    ofi_req->event_callback = ompi_mtl_ofi_post_recv_excid_buffer_callback;
    ofi_req->error_callback = ompi_mtl_ofi_recv_excid_error_callback;
    ofi_req->buffer = start;
    ofi_req->length = length;
    ofi_req->convertor = NULL;
    ofi_req->req_started = false;
    ofi_req->status.MPI_ERROR = OMPI_SUCCESS;
    ofi_req->remote_addr = 0UL;
    ofi_req->match_bits = 0UL;
    ofi_req->completion_count = 1;
    ofi_req->comm = comm;

    MTL_OFI_RETRY_UNTIL_DONE_CTXT(fi_recv(ompi_mtl_ofi.ofi_ctxt[ctxt_id].rx_ep,
                                      start,
                                      length,
                                      NULL,
                                      FI_ADDR_UNSPEC,
                                      (void *)&ofi_req->ctx), ret, ctxt_id);
    if (OPAL_UNLIKELY(0 > ret)) {
        if (NULL != ofi_req->buffer) {
            free(ofi_req->buffer);
        }
        MTL_OFI_LOG_FI_ERR(ret, "fi_recv failed");
        return ompi_mtl_ofi_get_error(ret);
    }

    if (blocking) {
        assert(src != -1);
        while (mtl_comm->c_index_vec[src].c_index_state > MCA_MTL_OFI_CID_EXCHANGED) {
            ompi_mtl_ofi_progress_after_eagain(ctxt_id);
        }
    }

    return OMPI_SUCCESS;
}

__opal_attribute_always_inline__ static inline int
ompi_mtl_ofi_ssend_recv(ompi_mtl_ofi_request_t *ack_req,
                  struct ompi_communicator_t *comm,
                  fi_addr_t *src_addr,
                  ompi_mtl_ofi_request_t *ofi_req,
                  mca_mtl_ofi_endpoint_t *endpoint,
                  uint64_t *match_bits,
                  int tag)
{
    ssize_t ret = OMPI_SUCCESS;
    int ctxt_id = 0;

    ctxt_id = ompi_mtl_ofi_ctxt_index_for_comm(comm);
    set_thread_context(ctxt_id);

    ack_req = malloc(sizeof(ompi_mtl_ofi_request_t));
    assert(ack_req);

    ack_req->parent = ofi_req;
    ack_req->event_callback = ompi_mtl_ofi_send_ack_callback;
    ack_req->error_callback = ompi_mtl_ofi_send_ack_error_callback;

    ofi_req->completion_count += 1;

    {
        fi_addr_t ack_src = *src_addr;
#if HAVE_LITHE
        /* Hosted dest-slot demux: directed source addrs are ambiguous on a
         * shared EP; match the ack by tag (original dest-slot + ack bit). */
        if (ompi_mtl_ofi.hosted_dst_in_cqd_tag || ompi_mtl_ofi.hosted_multi_ep) {
            ack_src = ompi_mtl_ofi.any_addr;
        }
#endif
        MTL_OFI_RETRY_UNTIL_DONE_CTXT(fi_trecv(ompi_mtl_ofi.ofi_ctxt[ctxt_id].rx_ep,
                                          NULL,
                                          0,
                                          NULL,
                                          ack_src,
                                          *match_bits | ompi_mtl_ofi.sync_send_ack,
                                          0, /* Exact match, no ignore bits */
                                          (void *) &ack_req->ctx), ret, ctxt_id);
    }
    if (OPAL_UNLIKELY(0 > ret)) {
        opal_output_verbose(1, opal_common_ofi.output,
                            "%s:%d: fi_trecv failed: %s(%zd)",
                            __FILE__, __LINE__, fi_strerror(-ret), ret);
        free(ack_req);
        return ompi_mtl_ofi_get_error(ret);
    }

     /* The SYNC_SEND tag bit is set for the send operation only.*/
    MTL_OFI_SET_SYNC_SEND(*match_bits);
    return OMPI_SUCCESS;
}

/*
 * this routine is invoked in the case of communicators which are not using a
 * global cid, i.e. those created using MPI_Comm_create_from_group/
 * MPI_Intercomm_create_from_groups in order to exchange the local cid used
 * by the sender for this supplied communicator.  This function is only invoked
 * for the first message sent to a given receiver.
 */
static int
ompi_mtl_ofi_send_excid(struct mca_mtl_base_module_t *mtl,
                  struct ompi_communicator_t *comm,
                  int dest,
                  bool ofi_cq_data,
                  bool is_send)
{
    ssize_t ret = OMPI_SUCCESS;
    ompi_mtl_ofi_request_t *ofi_req = NULL;
    int ctxt_id = 0;
    mca_mtl_ofi_cid_hdr_t *start = NULL;
    ompi_proc_t *ompi_proc = NULL;
    mca_mtl_ofi_endpoint_t *endpoint = NULL;
    fi_addr_t sep_peer_fiaddr = 0;
    mca_mtl_comm_t *mtl_comm;
    
    ofi_req = (ompi_mtl_ofi_request_t *)malloc(sizeof(ompi_mtl_ofi_request_t));
    if (NULL == ofi_req) {
        ret =  OMPI_ERR_OUT_OF_RESOURCE;
        goto fn_exit;
    }

    start = (mca_mtl_ofi_cid_hdr_t *)malloc(sizeof(mca_mtl_ofi_cid_hdr_t));
    if (NULL == start) {
        ret = OMPI_ERR_OUT_OF_RESOURCE;
        goto fn_exit;
    }

    mtl_comm = comm->c_mtl_comm;

    ctxt_id = ompi_mtl_ofi_ctxt_index_for_comm(comm);
    set_thread_context(ctxt_id);

    /**
     * Create a send request, start it and wait until it completes.
     */
    ofi_req->type = OMPI_MTL_OFI_SEND;
    ofi_req->event_callback = ompi_mtl_ofi_send_excid_callback;
    ofi_req->error_callback = ompi_mtl_ofi_send_error_callback;
    ofi_req->buffer = start;

    ompi_proc = ompi_comm_peer_lookup(comm, dest);
    endpoint = ompi_mtl_ofi_get_endpoint(mtl, ompi_proc);

    /* For Scalable Endpoints, gather target receive context */
    sep_peer_fiaddr = fi_rx_addr(endpoint->peer_fiaddr,
                                 ompi_mtl_ofi_sep_peer_rx_ctxt(ompi_proc, ctxt_id),
                                 ompi_mtl_ofi.rx_ctx_bits);

    start->hdr_cid = comm->c_contextid;
    start->hdr_src = ompi_mtl_ofi_comm_rank(comm);
    start->hdr_src_c_index = comm->c_index;
    start->ofi_cq_data = ofi_cq_data;
    if (mtl_comm->c_index_vec[dest].c_index_state > MCA_MTL_OFI_CID_EXCHANGED) {
        start->need_response = true;
    } else {
        start->need_response = false;
    }
    size_t length = sizeof(mca_mtl_ofi_cid_hdr_t);

    ofi_req->length = length;
    ofi_req->status.MPI_ERROR = OMPI_SUCCESS;
    ofi_req->completion_count = 0;
    if (OPAL_UNLIKELY(length > endpoint->mtl_ofi_module->max_msg_size)) {
        opal_show_help("help-mtl-ofi.txt",
            "message too big", false,
            length, endpoint->mtl_ofi_module->max_msg_size);
        ret = OMPI_ERROR;
        goto fn_exit;
    }

    if (ompi_mtl_ofi.max_inject_size >= length) {
        if (ofi_cq_data) {
            MTL_OFI_RETRY_UNTIL_DONE_CTXT(fi_injectdata(ompi_mtl_ofi.ofi_ctxt[ctxt_id].tx_ep,
                                            start,
                                            length,
                                            ompi_mtl_ofi_comm_rank(comm),
                                            sep_peer_fiaddr), ret, ctxt_id);
        } else {
            MTL_OFI_RETRY_UNTIL_DONE_CTXT(fi_inject(ompi_mtl_ofi.ofi_ctxt[ctxt_id].tx_ep,
                                            start,
                                            length,
                                            sep_peer_fiaddr), ret, ctxt_id);
        }
        if (OPAL_UNLIKELY(0 > ret)) {
            MTL_OFI_LOG_FI_ERR(ret,
                               ofi_cq_data ? "fi_injectdata failed"
                               : "fi_inject failed");

        }
    } else {
        ofi_req->completion_count = 1;
        if (ofi_cq_data) {
            MTL_OFI_RETRY_UNTIL_DONE_CTXT(fi_senddata(ompi_mtl_ofi.ofi_ctxt[ctxt_id].tx_ep,
                                          start,
                                          length,
                                          NULL,
                                          ompi_mtl_ofi_comm_rank(comm),
                                          sep_peer_fiaddr,
                                          (void *) &ofi_req->ctx), ret, ctxt_id);
        } else {
            MTL_OFI_RETRY_UNTIL_DONE_CTXT(fi_send(ompi_mtl_ofi.ofi_ctxt[ctxt_id].tx_ep,
                                          start,
                                          length,
                                          NULL,
                                          sep_peer_fiaddr,
                                          (void *) &ofi_req->ctx), ret, ctxt_id);
        }
        if (OPAL_UNLIKELY(0 > ret)) {
            MTL_OFI_LOG_FI_ERR(ret,
                               ofi_cq_data ? "fi_tsenddata failed"
                               : "fi_tsend failed");
        }
    }

    ret = ompi_mtl_ofi_get_error(ret);
    ofi_req->status.MPI_ERROR = ret;

fn_exit:

    if ((OMPI_SUCCESS != ret) || (ofi_req->completion_count == 0)) {
        if (NULL != ofi_req) free(ofi_req);
        if (NULL != start) free(start);
    }

    return ret;
}

__opal_attribute_always_inline__ static inline int
ompi_mtl_ofi_send_generic(struct mca_mtl_base_module_t *mtl,
                          struct ompi_communicator_t *comm,
                          int dest,
                          int tag,
                          struct opal_convertor_t *convertor,
                          mca_pml_base_send_mode_t mode,
                          bool ofi_cq_data)
{
    ssize_t ret = OMPI_SUCCESS;
    ompi_mtl_ofi_request_t ofi_req;
    int ompi_ret, ctxt_id = 0, c_index_for_tag;
    void *start;
    bool free_after;
    size_t length;
    uint64_t match_bits;
    ompi_proc_t *ompi_proc = NULL;
    mca_mtl_ofi_endpoint_t *endpoint = NULL;
    ompi_mtl_ofi_request_t *ack_req = NULL; /* For synchronous send */
    fi_addr_t src_addr = 0;
    fi_addr_t sep_peer_fiaddr = 0;
    mca_mtl_comm_t *mtl_comm;

    if (OPAL_LIKELY(OMPI_COMM_IS_GLOBAL_INDEX(comm))) {
        c_index_for_tag = comm->c_index;
    } else {
        mtl_comm = comm->c_mtl_comm;
        if (mtl_comm->c_index_vec[dest].c_index_state == MCA_MTL_OFI_CID_NOT_EXCHANGED) {
            mtl_comm->c_index_vec[dest].c_index_state = MCA_MTL_OFI_CID_EXCHANGING;
            ompi_ret = ompi_mtl_ofi_send_excid(mtl, comm, dest, ofi_cq_data, true);
        }

        if (mtl_comm->c_index_vec[dest].c_index_state > MCA_MTL_OFI_CID_EXCHANGED) {
             while (mtl_comm->c_index_vec[dest].c_index_state > MCA_MTL_OFI_CID_EXCHANGED) {
                ompi_ret = ompi_mtl_ofi_post_recv_excid_buffer(true, comm, dest);
            }
        }
        c_index_for_tag = mtl_comm->c_index_vec[dest].c_index;
    }

    ompi_mtl_ofi_set_mr_null(&ofi_req);
    ctxt_id = ompi_mtl_ofi_ctxt_index_for_comm(comm);

    set_thread_context(ctxt_id);

    /**
     * Create a send request, start it and wait until it completes.
     */
    ofi_req.event_callback = ompi_mtl_ofi_send_callback;
    ofi_req.error_callback = ompi_mtl_ofi_send_error_callback;

    ompi_proc = ompi_comm_peer_lookup(comm, dest);
    endpoint = ompi_mtl_ofi_get_endpoint(mtl, ompi_proc);

    /* For Scalable Endpoints, gather target receive context */
    sep_peer_fiaddr = fi_rx_addr(endpoint->peer_fiaddr,
                                 ompi_mtl_ofi_sep_peer_rx_ctxt(ompi_proc, ctxt_id),
                                 ompi_mtl_ofi.rx_ctx_bits);

    ompi_ret = ompi_mtl_datatype_pack(convertor, &start, &length, &free_after);
    if (OPAL_UNLIKELY(OMPI_SUCCESS != ompi_ret)) {
        return ompi_ret;
    }

    ofi_req.buffer = (free_after) ? start : NULL;
    ofi_req.length = length;
    ofi_req.status.MPI_ERROR = OMPI_SUCCESS;
    ofi_req.completion_count = 0;
    ofi_req.convertor = convertor;

    if (OPAL_UNLIKELY(length > endpoint->mtl_ofi_module->max_msg_size)) {
        opal_show_help("help-mtl-ofi.txt",
            "message too big", false,
            length, endpoint->mtl_ofi_module->max_msg_size);
        return OMPI_ERROR;
    }

#if HAVE_LITHE
    /* Same-OS hosted ranks: memcpy match, skip cxi/OFI loopback. */
    if (!(convertor->flags & CONVERTOR_ACCELERATOR)
        && ompi_mtl_ofi_hosted_sc_enabled()
        && ompi_mtl_ofi_hosted_sc_try_send(comm, dest, tag, start, length,
                                           free_after, mode, &ofi_req, false)) {
        return ofi_req.status.MPI_ERROR;
    }
#endif

    if (ofi_cq_data) {
        /* Hosted no-SEP CQD: dest-slot in tag (see mtl_ofi_create_send_tag_CQD). */
        match_bits = mtl_ofi_create_send_tag_CQD(c_index_for_tag, tag, dest);
        src_addr = sep_peer_fiaddr;
    } else {
        match_bits = mtl_ofi_create_send_tag(c_index_for_tag,
                                             ompi_mtl_ofi_comm_rank(comm), tag);
        /* src_addr is ignored when FI_DIRECTED_RECV is not supported */
    }

    if (OPAL_UNLIKELY(MCA_PML_BASE_SEND_SYNCHRONOUS == mode)) {
        ofi_req.status.MPI_ERROR = ompi_mtl_ofi_ssend_recv(ack_req, comm, &src_addr,
                                                           &ofi_req, endpoint,
                                                           &match_bits, tag);
        if (OPAL_UNLIKELY(ofi_req.status.MPI_ERROR != OMPI_SUCCESS))
            goto free_request_buffer;
    }

    /** Inject does not currently support device memory
     *  https://github.com/ofiwg/libfabric/issues/5861
     */
    if (!(convertor->flags & CONVERTOR_ACCELERATOR)
        && (ompi_mtl_ofi.max_inject_size >= length)) {
        if (ofi_cq_data) {
            MTL_OFI_RETRY_UNTIL_DONE_CTXT(fi_tinjectdata(ompi_mtl_ofi.ofi_ctxt[ctxt_id].tx_ep,
                                            start,
                                            length,
                                            ompi_mtl_ofi_comm_rank(comm),
                                            sep_peer_fiaddr,
                                            match_bits), ret, ctxt_id);
        } else {
            MTL_OFI_RETRY_UNTIL_DONE_CTXT(fi_tinject(ompi_mtl_ofi.ofi_ctxt[ctxt_id].tx_ep,
                                            start,
                                            length,
                                            sep_peer_fiaddr,
                                            match_bits), ret, ctxt_id);
        }
        if (OPAL_UNLIKELY(0 > ret)) {
            MTL_OFI_LOG_FI_ERR(ret,
                               ofi_cq_data ? "fi_tinjectdata failed"
                               : "fi_tinject failed");
            if (ack_req) {
                fi_cancel((fid_t)ompi_mtl_ofi.ofi_ctxt[ctxt_id].tx_ep, &ack_req->ctx);
                free(ack_req);
            }

            ofi_req.status.MPI_ERROR = ompi_mtl_ofi_get_error(ret);
            goto free_request_buffer;
        }
    } else {
        ompi_ret = ompi_mtl_ofi_register_buffer(convertor, &ofi_req, start);
        if (OPAL_UNLIKELY(OMPI_SUCCESS != ompi_ret)) {
            return ompi_ret;
        }
        ofi_req.completion_count += 1;
        if (ofi_cq_data) {
            MTL_OFI_RETRY_UNTIL_DONE_CTXT(fi_tsenddata(ompi_mtl_ofi.ofi_ctxt[ctxt_id].tx_ep,
                                          start,
                                          length,
                                          (NULL == ofi_req.mr) ? NULL : ofi_req.mr->mem_desc,
                                          ompi_mtl_ofi_comm_rank(comm),
                                          sep_peer_fiaddr,
                                          match_bits,
                                          (void *) &ofi_req.ctx), ret, ctxt_id);
        } else {
            MTL_OFI_RETRY_UNTIL_DONE_CTXT(fi_tsend(ompi_mtl_ofi.ofi_ctxt[ctxt_id].tx_ep,
                                          start,
                                          length,
                                          (NULL == ofi_req.mr) ? NULL : ofi_req.mr->mem_desc,
                                          sep_peer_fiaddr,
                                          match_bits,
                                          (void *) &ofi_req.ctx), ret, ctxt_id);
        }
        if (OPAL_UNLIKELY(0 > ret)) {
            MTL_OFI_LOG_FI_ERR(ret,
                               ofi_cq_data ? "fi_tsenddata failed"
                               : "fi_tsend failed");
            ofi_req.status.MPI_ERROR = ompi_mtl_ofi_get_error(ret);
            goto free_request_buffer;
        }
    }

    /**
     * Wait until the request is completed.
     * ompi_mtl_ofi_send_callback() updates this variable.
     */
    while (0 < ofi_req.completion_count) {
        ompi_mtl_ofi_progress_after_eagain(ctxt_id);
    }

free_request_buffer:
    ompi_mtl_ofi_deregister_and_free_buffer(&ofi_req);

    return ofi_req.status.MPI_ERROR;
}

/*
 * This routine is invoked in the case where a Recv finds the
 * MTL_OFI_IS_SYNC_SEND flag was set, indicating the sender issued an SSend and
 * is blocking while it waits on an ACK message.
 *
 * Issue a fire-and-forget send back to the src with a matching tag so that
 * the sender may continue progress.
 * Requires ofi_req->remote_addr and ofi_req->comm to be set.
 */
static int
ompi_mtl_ofi_gen_ssend_ack(struct fi_cq_tagged_entry *wc,
                           ompi_mtl_ofi_request_t *ofi_req)
{
    /**
     * If this recv is part of an MPI_Ssend operation, then we send an
     * acknowledgment back to the sender.
     * The ack message is sent without generating a completion event in
     * the completion queue by not setting FI_COMPLETION in the flags to
     * fi_tsendmsg(FI_SELECTIVE_COMPLETION).
     * This is done since the 0 byte message requires no
     * notification on the send side for a successful completion.
     * If a failure occurs the provider will notify the error
     * in the cq_readerr during OFI progress. Once the message has been
     * successfully processed the request is marked as completed.
     *
     * Turns out that there is a bug in the argument checking
     * in the CXI provider (at least the vendor 1.15.2.0 and earlier versions)
     * fi_tsendmsg method.  So we have to feed a dummy iovec argument
     * into fi_tsendmsg with a NULL buffer and zero iov_len, hence
     * the d_iovect, etc.
     */
    int ctxt_id = 0;
    ssize_t ret;
    ompi_proc_t *ompi_proc = NULL;
    mca_mtl_ofi_endpoint_t *endpoint = NULL;
    int src = mtl_ofi_get_source(wc);
    struct fi_msg_tagged tagged_msg;
    struct iovec d_iovec = {.iov_base = NULL, .iov_len = 0};

    ctxt_id = ompi_mtl_ofi_ctxt_index_for_comm(ofi_req->comm);

    ret = MPI_SUCCESS;
    
    /**
     * If the recv request was posted for any source,
     * we need to extract the source's actual address.
     */
    ompi_proc = ompi_comm_peer_lookup(ofi_req->comm, src);
    endpoint = ompi_mtl_ofi_get_endpoint(ofi_req->mtl, ompi_proc);
    ofi_req->remote_addr = fi_rx_addr(endpoint->peer_fiaddr,
                                      ompi_mtl_ofi_sep_peer_rx_ctxt(ompi_proc, ctxt_id),
                                      ompi_mtl_ofi.rx_ctx_bits);

    tagged_msg.msg_iov = &d_iovec;
    tagged_msg.desc = NULL;
    tagged_msg.iov_count = 1;
    tagged_msg.addr = ofi_req->remote_addr;
    /**
    * We must continue to use the user's original tag but remove the
    * sync_send protocol tag bit and instead apply the sync_send_ack
    * tag bit to complete the initiator's sync send receive.
    */
    tagged_msg.tag = (wc->tag | ompi_mtl_ofi.sync_send_ack) & ~ompi_mtl_ofi.sync_send;
    tagged_msg.context = NULL;
    tagged_msg.data = 0;

    MTL_OFI_RETRY_UNTIL_DONE_CTXT(fi_tsendmsg(ompi_mtl_ofi.ofi_ctxt[ctxt_id].tx_ep,
                                &tagged_msg, 0), ret, ctxt_id);
    if (OPAL_UNLIKELY(0 > ret)) {
        MTL_OFI_LOG_FI_ERR(ret, "fi_tsendmsg failed during ompi_mtl_ofi_gen_ssend_ack");
        ret = OMPI_ERROR;
    }
    return ret;
}

__opal_attribute_always_inline__ static inline int
ompi_mtl_ofi_isend_generic(struct mca_mtl_base_module_t *mtl,
                   struct ompi_communicator_t *comm,
                   int dest,
                   int tag,
                   struct opal_convertor_t *convertor,
                   mca_pml_base_send_mode_t mode,
                   bool blocking,
                   mca_mtl_request_t *mtl_request,
                   bool ofi_cq_data)
{
    ssize_t ret = OMPI_SUCCESS;
    ompi_mtl_ofi_request_t *ofi_req = (ompi_mtl_ofi_request_t *) mtl_request;
    int ompi_ret, ctxt_id = 0, c_index_for_tag;
    void *start;
    size_t length;
    bool free_after;
    uint64_t match_bits;
    ompi_proc_t *ompi_proc = NULL;
    mca_mtl_ofi_endpoint_t *endpoint = NULL;
    ompi_mtl_ofi_request_t *ack_req = NULL; /* For synchronous send */
    fi_addr_t sep_peer_fiaddr = 0;
    mca_mtl_comm_t *mtl_comm;

    ompi_mtl_ofi_set_mr_null(ofi_req);

    if (OMPI_COMM_IS_GLOBAL_INDEX(comm)) {
        c_index_for_tag = comm->c_index;
    } else {
        mtl_comm = comm->c_mtl_comm;
        if (mtl_comm->c_index_vec[dest].c_index_state == MCA_MTL_OFI_CID_NOT_EXCHANGED) {
            mtl_comm->c_index_vec[dest].c_index_state = MCA_MTL_OFI_CID_EXCHANGING;
            ompi_ret = ompi_mtl_ofi_send_excid(mtl, comm, dest, ofi_cq_data, true);
        }
        if (mtl_comm->c_index_vec[dest].c_index_state > MCA_MTL_OFI_CID_EXCHANGED) {
            while (mtl_comm->c_index_vec[dest].c_index_state > MCA_MTL_OFI_CID_EXCHANGED) {
                ompi_ret = ompi_mtl_ofi_post_recv_excid_buffer(true, comm, dest);
            }
        }
        c_index_for_tag = mtl_comm->c_index_vec[dest].c_index;
    }

    ctxt_id = ompi_mtl_ofi_ctxt_index_for_comm(comm);
    set_thread_context(ctxt_id);

    ofi_req->event_callback = ompi_mtl_ofi_isend_callback;
    ofi_req->error_callback = ompi_mtl_ofi_send_error_callback;

    ompi_proc = ompi_comm_peer_lookup(comm, dest);
    endpoint = ompi_mtl_ofi_get_endpoint(mtl, ompi_proc);

    /* For Scalable Endpoints, gather target receive context */
    sep_peer_fiaddr = fi_rx_addr(endpoint->peer_fiaddr,
                                 ompi_mtl_ofi_sep_peer_rx_ctxt(ompi_proc, ctxt_id),
                                 ompi_mtl_ofi.rx_ctx_bits);

    ompi_ret = ompi_mtl_datatype_pack(convertor, &start, &length, &free_after);
    if (OPAL_UNLIKELY(OMPI_SUCCESS != ompi_ret)) return ompi_ret;

    ofi_req->type = OMPI_MTL_OFI_SEND;
    ofi_req->buffer = (free_after) ? start : NULL;
    ofi_req->length = length;
    ofi_req->status.MPI_ERROR = OMPI_SUCCESS;
    ofi_req->completion_count = 1;
    ofi_req->convertor = convertor;

    if (OPAL_UNLIKELY(length > endpoint->mtl_ofi_module->max_msg_size)) {
        opal_show_help("help-mtl-ofi.txt",
            "message too big", false,
            length, endpoint->mtl_ofi_module->max_msg_size);
        return OMPI_ERROR;
    }

#if HAVE_LITHE
    if (!(convertor->flags & CONVERTOR_ACCELERATOR)
        && ompi_mtl_ofi_hosted_sc_enabled()
        && ompi_mtl_ofi_hosted_sc_try_send(comm, dest, tag, start, length,
                                           free_after, mode, ofi_req, true)) {
        return ofi_req->status.MPI_ERROR;
    }
#endif

    if (ofi_cq_data) {
        /* Hosted no-SEP CQD: dest-slot in tag (see mtl_ofi_create_send_tag_CQD). */
        match_bits = mtl_ofi_create_send_tag_CQD(c_index_for_tag, tag, dest);
    } else {
        match_bits = mtl_ofi_create_send_tag(c_index_for_tag,
                          ompi_mtl_ofi_comm_rank(comm), tag);
        /* src_addr is ignored when FI_DIRECTED_RECV  is not supported */
    }

    if (OPAL_UNLIKELY(MCA_PML_BASE_SEND_SYNCHRONOUS == mode)) {
        ofi_req->status.MPI_ERROR = ompi_mtl_ofi_ssend_recv(ack_req, comm, &sep_peer_fiaddr,
                                                           ofi_req, endpoint,
                                                           &match_bits, tag);
        if (OPAL_UNLIKELY(ofi_req->status.MPI_ERROR != OMPI_SUCCESS))
            goto free_request_buffer;
    }

    ompi_ret = ompi_mtl_ofi_register_buffer(convertor, ofi_req, start);
    if (OPAL_UNLIKELY(OMPI_SUCCESS != ompi_ret)) {
        return ompi_ret;
    }


    /** Inject does not currently support device memory
     *  https://github.com/ofiwg/libfabric/issues/5861
     */
    if (!(convertor->flags & CONVERTOR_ACCELERATOR)
        && (ompi_mtl_ofi.max_inject_size >= length)) {
        if (ofi_cq_data) {
            ret = fi_tinjectdata(ompi_mtl_ofi.ofi_ctxt[ctxt_id].tx_ep,
                    start,
                    length,
                    ompi_mtl_ofi_comm_rank(comm),
                    sep_peer_fiaddr,
                    match_bits);
        } else {
            ret = fi_tinject(ompi_mtl_ofi.ofi_ctxt[ctxt_id].tx_ep,
                    start,
                    length,
                    sep_peer_fiaddr,
                    match_bits);
        }

        if(OPAL_LIKELY(ret == 0)) {
            ofi_req->event_callback(NULL, ofi_req);
            return ofi_req->status.MPI_ERROR;
        } else if(ret != -FI_EAGAIN) {
            MTL_OFI_LOG_FI_ERR(ret,
                               ofi_cq_data ? "fi_tinjectdata failed"
                               : "fi_tinject failed");
            if (ack_req) {
                fi_cancel((fid_t)ompi_mtl_ofi.ofi_ctxt[ctxt_id].tx_ep, &ack_req->ctx);
                free(ack_req);
            }
            ofi_req->status.MPI_ERROR = ompi_mtl_ofi_get_error(ret);
            ofi_req->event_callback(NULL, ofi_req);
            return ofi_req->status.MPI_ERROR;
        }
        /* otherwise fall back to the standard fi_tsend path */
    }


    if (ofi_cq_data) {
        MTL_OFI_RETRY_UNTIL_DONE_CTXT(fi_tsenddata(ompi_mtl_ofi.ofi_ctxt[ctxt_id].tx_ep,
                                      start,
                                      length,
                                      (NULL == ofi_req->mr) ? NULL : ofi_req->mr->mem_desc,
                                      ompi_mtl_ofi_comm_rank(comm),
                                      sep_peer_fiaddr,
                                      match_bits,
                                      (void *) &ofi_req->ctx), ret, ctxt_id);
    } else {
        MTL_OFI_RETRY_UNTIL_DONE_CTXT(fi_tsend(ompi_mtl_ofi.ofi_ctxt[ctxt_id].tx_ep,
                                      start,
                                      length,
                                      (NULL == ofi_req->mr) ? NULL : ofi_req->mr->mem_desc,
                                      sep_peer_fiaddr,
                                      match_bits,
                                      (void *) &ofi_req->ctx), ret, ctxt_id);
    }
    if (OPAL_UNLIKELY(0 > ret)) {
        MTL_OFI_LOG_FI_ERR(ret,
                           ofi_cq_data ? "fi_tsenddata failed"
                           : "fi_tsend failed");
        ofi_req->status.MPI_ERROR = ompi_mtl_ofi_get_error(ret);
    }

free_request_buffer:
    if (OPAL_UNLIKELY(OMPI_SUCCESS != ofi_req->status.MPI_ERROR)) {
        ompi_mtl_ofi_deregister_and_free_buffer(ofi_req);
    }

    return ofi_req->status.MPI_ERROR;
}

/**
 * Called when a completion for a posted recv is received.
 */
__opal_attribute_always_inline__ static inline int
ompi_mtl_ofi_recv_callback(struct fi_cq_tagged_entry *wc,
                           ompi_mtl_ofi_request_t *ofi_req)
{
    int ompi_ret;
    int src = mtl_ofi_get_source(wc);
    ompi_status_public_t *status = NULL;

    assert(ofi_req->super.ompi_req);
    status = &ofi_req->super.ompi_req->req_status;

#if HAVE_LITHE
    /* Dual-posted ANY_SOURCE: OFI won the race — drop SC posted entry. */
    if (ompi_mtl_ofi_hosted_sc_enabled()) {
        if (ofi_req->req_started) {
            /* SC already completed this request; ignore late CQ event. */
            return OMPI_SUCCESS;
        }
        ompi_mtl_ofi_hosted_sc_ofi_recv_claimed(ofi_req);
    }
#endif

    /**
     * Any event associated with a request starts it.
     * This prevents a started request from being cancelled.
     */
    ofi_req->req_started = true;

    status->MPI_ERROR = MPI_SUCCESS;
    status->MPI_SOURCE = src;
    status->MPI_TAG = MTL_OFI_GET_TAG(wc->tag);
    status->_ucount = wc->len;

    if (OPAL_UNLIKELY(wc->len > ofi_req->length)) {
        opal_output_verbose(1, opal_common_ofi.output,
                            "truncate expected: %ld %ld",
                            wc->len, ofi_req->length);
        status->MPI_ERROR = MPI_ERR_TRUNCATE;
    }

    ompi_mtl_ofi_deregister_buffer(ofi_req);

    /**
     * Unpack data into recv buffer if necessary.
     */
    if (OPAL_UNLIKELY(ofi_req->buffer)) {
        ompi_ret = ompi_mtl_datatype_unpack(ofi_req->convertor,
                                            ofi_req->buffer,
                                            wc->len);
        if (OPAL_UNLIKELY(OMPI_SUCCESS != ompi_ret)) {
            opal_output_verbose(1, opal_common_ofi.output,
                                "%s:%d: ompi_mtl_datatype_unpack failed: %d",
                                __FILE__, __LINE__, ompi_ret);
            status->MPI_ERROR = ompi_ret;
        }
    }

    /**
    * We can only accept MTL_OFI_SYNC_SEND in the standard recv callback.
    * MTL_OFI_SYNC_SEND_ACK should only be received in the send_ack
    * callback.
    */
    assert(!MTL_OFI_IS_SYNC_SEND_ACK(wc->tag));

    if (OPAL_UNLIKELY(MTL_OFI_IS_SYNC_SEND(wc->tag))) {
        ompi_ret = ompi_mtl_ofi_gen_ssend_ack(wc, ofi_req);

        if (OPAL_UNLIKELY(OMPI_SUCCESS != ompi_ret)) {
            opal_output_verbose(1, opal_common_ofi.output,
                                "%s:%d: ompi_mtl_ofi_gen_ssend_ack failed: %d",
                                __FILE__, __LINE__, ompi_ret);
            status->MPI_ERROR = ompi_ret;
        }
    }

    ofi_req->super.completion_callback(&ofi_req->super);

    return status->MPI_ERROR;
}

/**
 * Called when an error occurred on a recv request.
 */
__opal_attribute_always_inline__ static inline int
ompi_mtl_ofi_recv_error_callback(struct fi_cq_err_entry *error,
                                 ompi_mtl_ofi_request_t *ofi_req)
{
    ompi_status_public_t *status;
    assert(ofi_req->super.ompi_req);
    status = &ofi_req->super.ompi_req->req_status;
    status->MPI_TAG = MTL_OFI_GET_TAG(ofi_req->match_bits);
    status->MPI_SOURCE = mtl_ofi_get_source((struct fi_cq_tagged_entry *) error);

    switch (error->err) {
        case FI_ETRUNC:
            status->MPI_ERROR = MPI_ERR_TRUNCATE;
            break;
        case FI_ECANCELED:
            status->_cancelled = true;
            break;
        default:
            status->MPI_ERROR = MPI_ERR_INTERN;
    }

    ofi_req->super.completion_callback(&ofi_req->super);
    return OMPI_SUCCESS;
}

__opal_attribute_always_inline__ static inline int
ompi_mtl_ofi_irecv_generic(struct mca_mtl_base_module_t *mtl,
                   struct ompi_communicator_t *comm,
                   int src,
                   int tag,
                   struct opal_convertor_t *convertor,
                   mca_mtl_request_t *mtl_request,
                   bool ofi_cq_data)
{
    int ompi_ret = OMPI_SUCCESS, ctxt_id = 0;
    ssize_t ret;
    uint64_t match_bits, mask_bits;
    fi_addr_t remote_addr = ompi_mtl_ofi.any_addr;
    ompi_proc_t *ompi_proc = NULL;
    mca_mtl_ofi_endpoint_t *endpoint = NULL;
    ompi_mtl_ofi_request_t *ofi_req = (ompi_mtl_ofi_request_t*) mtl_request;
    void *start;
    size_t length;
    bool free_after;
    mca_mtl_comm_t *mtl_comm;

    ompi_mtl_ofi_set_mr_null(ofi_req);

    if (!OMPI_COMM_IS_GLOBAL_INDEX(comm)) {
        mtl_comm = comm->c_mtl_comm;
        if ((src == MPI_ANY_SOURCE || mtl_comm->c_index_vec[src].c_index_state > MCA_MTL_OFI_CID_EXCHANGED) &&
             !ompi_mtl_ofi.has_posted_initial_buffer) {
            ompi_mtl_ofi.has_posted_initial_buffer = true;
            ompi_ret = ompi_mtl_ofi_post_recv_excid_buffer(false, comm, -1);
        }
        if (src >= 0 && mtl_comm->c_index_vec[src].c_index_state == MCA_MTL_OFI_CID_NOT_EXCHANGED) {
            mtl_comm->c_index_vec[src].c_index_state = MCA_MTL_OFI_CID_EXCHANGING;
            ompi_ret = ompi_mtl_ofi_send_excid(mtl, comm, src, ofi_cq_data, false);
        }
    }

    ctxt_id = ompi_mtl_ofi_ctxt_index_for_comm(comm);

    set_thread_context(ctxt_id);

    if (ofi_cq_data) {
        if (MPI_ANY_SOURCE != src) {
            ompi_proc = ompi_comm_peer_lookup(comm, src);
            endpoint = ompi_mtl_ofi_get_endpoint(mtl, ompi_proc);
            /* Multi regular-EP / hosted dest-slot demux: co-resident ranks
             * share one EP address so directed-recv source fi_addrs are
             * ambiguous. Use UNSPEC; match by dest-slot tag (+ CQ data). */
            if (!ompi_mtl_ofi.hosted_multi_ep &&
                !ompi_mtl_ofi.hosted_dst_in_cqd_tag) {
                remote_addr = fi_rx_addr(endpoint->peer_fiaddr, ctxt_id,
                                         ompi_mtl_ofi.rx_ctx_bits);
            }
        }

        mtl_ofi_create_recv_tag_CQD(&match_bits, &mask_bits, comm->c_index,
                                    tag, src);
    } else {
        mtl_ofi_create_recv_tag(&match_bits, &mask_bits, comm->c_index, src,
                                tag);
        /* src_addr is ignored when FI_DIRECTED_RECV is not used */
    }

    ompi_ret = ompi_mtl_datatype_recv_buf(convertor,
                                          &start,
                                          &length,
                                          &free_after);
    if (OPAL_UNLIKELY(OMPI_SUCCESS != ompi_ret)) {
        return ompi_ret;
    }

    ofi_req->type = OMPI_MTL_OFI_RECV;
    ofi_req->event_callback = ompi_mtl_ofi_recv_callback;
    ofi_req->error_callback = ompi_mtl_ofi_recv_error_callback;
    ofi_req->comm = comm;
    ofi_req->buffer = (free_after) ? start : NULL;
    ofi_req->length = length;
    ofi_req->convertor = convertor;
    ofi_req->req_started = false;
    ofi_req->status.MPI_ERROR = OMPI_SUCCESS;
    ofi_req->remote_addr = remote_addr;
    ofi_req->match_bits = match_bits;

    ompi_ret = ompi_mtl_ofi_register_buffer(convertor, ofi_req, start);
    if (OPAL_UNLIKELY(OMPI_SUCCESS != ompi_ret)) {
        return ompi_ret;
    }

#if HAVE_LITHE
    if (!(convertor->flags & CONVERTOR_ACCELERATOR)
        && ompi_mtl_ofi_hosted_sc_enabled()) {
        int sc = ompi_mtl_ofi_hosted_sc_try_irecv(comm, src, tag, start, length,
                                                  ofi_req);
        if (1 == sc) {
            /* Fully handled (matched UE or posted local-only). */
            return OMPI_SUCCESS;
        }
        /* sc==2: ANY_SOURCE dual-post — also fi_trecv below. sc==0: OFI only. */
        if (2 == sc && ofi_req->req_started) {
            /* Matched via SC before we could post fi_trecv. */
            return OMPI_SUCCESS;
        }
    }
#endif

    MTL_OFI_RETRY_UNTIL_DONE_CTXT(fi_trecv(ompi_mtl_ofi.ofi_ctxt[ctxt_id].rx_ep,
                                      start,
                                      length,
                                      (NULL == ofi_req->mr) ? NULL : ofi_req->mr->mem_desc,
                                      remote_addr,
                                      match_bits,
                                      mask_bits,
                                      (void *)&ofi_req->ctx), ret, ctxt_id);
    if (OPAL_UNLIKELY(0 > ret)) {
        ompi_mtl_ofi_deregister_and_free_buffer(ofi_req);

        MTL_OFI_LOG_FI_ERR(ret, "fi_trecv failed");
        return ompi_mtl_ofi_get_error(ret);
    }

#if HAVE_LITHE
    if (ompi_mtl_ofi_hosted_sc_enabled()) {
        ompi_mtl_ofi_hosted_sc_mark_ofi_posted(ofi_req);
        if (ofi_req->req_started) {
            /* SC matched during/after fi_trecv — cancel the OFI post. */
            (void) fi_cancel((fid_t) ompi_mtl_ofi.ofi_ctxt[ctxt_id].rx_ep,
                             &ofi_req->ctx);
        }
    }
#endif

    return OMPI_SUCCESS;
}

/**
 * Called when a mrecv request completes.
 */
__opal_attribute_always_inline__ static inline int
ompi_mtl_ofi_mrecv_callback(struct fi_cq_tagged_entry *wc,
                            ompi_mtl_ofi_request_t *ofi_req)
{
    struct mca_mtl_request_t *mrecv_req = ofi_req->mrecv_req;
    ompi_status_public_t *status = &mrecv_req->ompi_req->req_status;
    status->MPI_SOURCE = mtl_ofi_get_source(wc);
    status->MPI_TAG = MTL_OFI_GET_TAG(wc->tag);
    status->MPI_ERROR = MPI_SUCCESS;
    status->_ucount = wc->len;
    int ompi_ret;

    ompi_mtl_ofi_deregister_and_free_buffer(ofi_req);

    if (OPAL_UNLIKELY(MTL_OFI_IS_SYNC_SEND(wc->tag))) {
        ompi_ret = ompi_mtl_ofi_gen_ssend_ack(wc, ofi_req);

        if (OPAL_UNLIKELY(OMPI_SUCCESS != ompi_ret)) {
            opal_output_verbose(1, opal_common_ofi.output,
                                "%s:%d: ompi_mtl_ofi_gen_ssend_ack failed: %d",
                                __FILE__, __LINE__, ompi_ret);
            status->MPI_ERROR = ompi_ret;
        }
    }

    free(ofi_req);

    mrecv_req->completion_callback(mrecv_req);

    return status->MPI_ERROR;
}

/**
 * Called when an error occurred on a mrecv request.
 */
__opal_attribute_always_inline__ static inline int
ompi_mtl_ofi_mrecv_error_callback(struct fi_cq_err_entry *error,
                                  ompi_mtl_ofi_request_t *ofi_req)
{
    struct mca_mtl_request_t *mrecv_req = ofi_req->mrecv_req;
    ompi_status_public_t *status = &mrecv_req->ompi_req->req_status;
    status->MPI_TAG = MTL_OFI_GET_TAG(ofi_req->match_bits);
    status->MPI_SOURCE = mtl_ofi_get_source((struct fi_cq_tagged_entry  *) error);

    switch (error->err) {
        case FI_ETRUNC:
            status->MPI_ERROR = MPI_ERR_TRUNCATE;
            break;
        case FI_ECANCELED:
            status->_cancelled = true;
            break;
        default:
            status->MPI_ERROR = MPI_ERR_INTERN;
    }

    ompi_mtl_ofi_deregister_and_free_buffer(ofi_req);

    free(ofi_req);

    mrecv_req->completion_callback(mrecv_req);

    return OMPI_SUCCESS;
}

__opal_attribute_always_inline__ static inline int
ompi_mtl_ofi_imrecv(struct mca_mtl_base_module_t *mtl,
                    struct opal_convertor_t *convertor,
                    struct ompi_message_t **message,
                    struct mca_mtl_request_t *mtl_request)
{
    ompi_mtl_ofi_request_t *ofi_req =
        (ompi_mtl_ofi_request_t *)(*message)->req_ptr;
    void *start;
    size_t length;
    bool free_after;
    struct iovec iov;
    struct fi_msg_tagged msg;
    int ompi_ret, ctxt_id = 0;
    ssize_t ret;
    uint64_t msgflags = FI_CLAIM | FI_COMPLETION;
    struct ompi_communicator_t *comm = (*message)->comm;

    ompi_mtl_ofi_set_mr_null(ofi_req);

    ctxt_id = ompi_mtl_ofi_ctxt_index_for_comm(comm);

    set_thread_context(ctxt_id);

    ompi_ret = ompi_mtl_datatype_recv_buf(convertor,
                                          &start,
                                          &length,
                                          &free_after);
    if (OPAL_UNLIKELY(OMPI_SUCCESS != ompi_ret)) {
        return ompi_ret;
    }

    ofi_req->type = OMPI_MTL_OFI_RECV;
    ofi_req->event_callback = ompi_mtl_ofi_mrecv_callback;
    ofi_req->error_callback = ompi_mtl_ofi_mrecv_error_callback;
    ofi_req->buffer = (free_after) ? start : NULL;
    ofi_req->length = length;
    ofi_req->convertor = convertor;
    ofi_req->status.MPI_ERROR = OMPI_SUCCESS;
    ofi_req->mrecv_req = mtl_request;
    ofi_req->comm = comm;


    ompi_ret = ompi_mtl_ofi_register_buffer(convertor, ofi_req, start);
    if (OPAL_UNLIKELY(OMPI_SUCCESS != ompi_ret)) {
        return ompi_ret;
    }

    /**
     * fi_trecvmsg with FI_CLAIM
     */
    iov.iov_base = start;
    iov.iov_len = length;
    msg.msg_iov = &iov;
    msg.desc = (NULL == ofi_req->mr) ? NULL : ofi_req->mr->mem_desc;
    msg.iov_count = 1;
    msg.addr = 0;
    msg.tag = ofi_req->match_bits;
    msg.ignore = ofi_req->mask_bits;
    msg.context = (void *)&ofi_req->ctx;
    msg.data = 0;

    MTL_OFI_RETRY_UNTIL_DONE_CTXT(fi_trecvmsg(ompi_mtl_ofi.ofi_ctxt[ctxt_id].rx_ep, &msg, msgflags), ret, ctxt_id);
    if (OPAL_UNLIKELY(0 > ret)) {
        ompi_mtl_ofi_deregister_and_free_buffer(ofi_req);
        MTL_OFI_LOG_FI_ERR(ret, "fi_trecvmsg failed");
        return ompi_mtl_ofi_get_error(ret);
    }

    *message = MPI_MESSAGE_NULL;

    return OMPI_SUCCESS;
}

/**
 * Called when a probe request completes.
 */
__opal_attribute_always_inline__ static inline int
ompi_mtl_ofi_probe_callback(struct fi_cq_tagged_entry *wc,
                            ompi_mtl_ofi_request_t *ofi_req)
{
    ofi_req->match_state = 1;
    ofi_req->match_bits = wc->tag;
    ofi_req->status.MPI_SOURCE = mtl_ofi_get_source(wc);
    ofi_req->status.MPI_TAG = MTL_OFI_GET_TAG(wc->tag);
    ofi_req->status.MPI_ERROR = MPI_SUCCESS;
    ofi_req->status._ucount = wc->len;
    ofi_req->completion_count--;

    return OMPI_SUCCESS;
}

/**
 * Called when a probe request encounters an error.
 */
__opal_attribute_always_inline__ static inline int
ompi_mtl_ofi_probe_error_callback(struct fi_cq_err_entry *error,
                                  ompi_mtl_ofi_request_t *ofi_req)
{
    ofi_req->completion_count--;

    /*
     * Receives posted with FI_PEEK and friends will get an error
     * completion with FI_ENOMSG. This just indicates the lack of a match for
     * the probe and is not an error case. All other error cases are
     * provider-internal errors and should be flagged as such.
     */
    if (error->err == FI_ENOMSG)
        return OMPI_SUCCESS;

    ofi_req->status.MPI_ERROR = MPI_ERR_INTERN;

    return OMPI_ERROR;
}

__opal_attribute_always_inline__ static inline int
ompi_mtl_ofi_iprobe_generic(struct mca_mtl_base_module_t *mtl,
                    struct ompi_communicator_t *comm,
                    int src,
                    int tag,
                    int *flag,
                    struct ompi_status_public_t *status,
                    bool ofi_cq_data)
{
    struct ompi_mtl_ofi_request_t ofi_req;
    ompi_proc_t *ompi_proc = NULL;
    mca_mtl_ofi_endpoint_t *endpoint = NULL;
    fi_addr_t remote_proc = ompi_mtl_ofi.any_addr;
    uint64_t match_bits, mask_bits;
    ssize_t ret;
    struct fi_msg_tagged msg;
    uint64_t msgflags = FI_PEEK | FI_COMPLETION;
    int ctxt_id = 0;
    mca_mtl_comm_t *mtl_comm;

    if (!OMPI_COMM_IS_GLOBAL_INDEX(comm)) {
        mtl_comm = comm->c_mtl_comm;
        if ((src == MPI_ANY_SOURCE || mtl_comm->c_index_vec[src].c_index_state > MCA_MTL_OFI_CID_EXCHANGED) &&
                             !ompi_mtl_ofi.has_posted_initial_buffer) {
            ompi_mtl_ofi.has_posted_initial_buffer = true;
            ret = ompi_mtl_ofi_post_recv_excid_buffer(false, comm, -1);
        }
        if (src >= 0 && mtl_comm->c_index_vec[src].c_index_state == MCA_MTL_OFI_CID_NOT_EXCHANGED) {
            mtl_comm->c_index_vec[src].c_index_state = MCA_MTL_OFI_CID_EXCHANGING;
            ret = ompi_mtl_ofi_send_excid(mtl, comm, src, ofi_cq_data, false);
        }
    }

    ctxt_id = ompi_mtl_ofi_ctxt_index_for_comm(comm);
    set_thread_context(ctxt_id);

    if (ofi_cq_data) {
     /* If the source is known, use its peer_fiaddr. */
        if (MPI_ANY_SOURCE != src) {
            ompi_proc = ompi_comm_peer_lookup( comm, src );
            endpoint = ompi_mtl_ofi_get_endpoint(mtl, ompi_proc);
            if (!ompi_mtl_ofi.hosted_multi_ep &&
                !ompi_mtl_ofi.hosted_dst_in_cqd_tag) {
                remote_proc = fi_rx_addr(endpoint->peer_fiaddr, ctxt_id,
                                         ompi_mtl_ofi.rx_ctx_bits);
            }
        }

        mtl_ofi_create_recv_tag_CQD(&match_bits, &mask_bits, comm->c_index,
                                    tag, src);
    }
    else {
        mtl_ofi_create_recv_tag(&match_bits, &mask_bits, comm->c_index, src,
                                tag);
        /* src_addr is ignored when FI_DIRECTED_RECV is not used */
    }

    /**
     * fi_trecvmsg with FI_PEEK:
     * Initiate a search for a match in the hardware or software queue.
     * If successful, libfabric will enqueue a context entry into the completion
     * queue to make the search nonblocking.  This code will poll until the
     * entry is enqueued.
     */
    msg.msg_iov = NULL;
    msg.desc = NULL;
    msg.iov_count = 0;
    msg.addr = remote_proc;
    msg.tag = match_bits;
    msg.ignore = mask_bits;
    msg.context = (void *)&ofi_req.ctx;
    msg.data = 0;

    ofi_req.type = OMPI_MTL_OFI_PROBE;
    ofi_req.event_callback = ompi_mtl_ofi_probe_callback;
    ofi_req.error_callback = ompi_mtl_ofi_probe_error_callback;
    ofi_req.completion_count = 1;
    ofi_req.match_state = 0;

    MTL_OFI_RETRY_UNTIL_DONE_CTXT(fi_trecvmsg(ompi_mtl_ofi.ofi_ctxt[ctxt_id].rx_ep, &msg, msgflags), ret, ctxt_id);
    if (OPAL_UNLIKELY(0 > ret)) {
        MTL_OFI_LOG_FI_ERR(ret, "fi_trecvmsg failed");
        return ompi_mtl_ofi_get_error(ret);
    }

    while (0 < ofi_req.completion_count) {
        ompi_mtl_ofi_progress_after_eagain(ctxt_id);
    }

    *flag = ofi_req.match_state;
    if (1 == *flag) {
        if (MPI_STATUS_IGNORE != status) {
            OMPI_COPY_STATUS(status, ofi_req.status, false);
        }
    }

    return OMPI_SUCCESS;
}

__opal_attribute_always_inline__ static inline int
ompi_mtl_ofi_improbe_generic(struct mca_mtl_base_module_t *mtl,
                     struct ompi_communicator_t *comm,
                     int src,
                     int tag,
                     int *matched,
                     struct ompi_message_t **message,
                     struct ompi_status_public_t *status,
                     bool ofi_cq_data)
{
    struct ompi_mtl_ofi_request_t *ofi_req;
    ompi_proc_t *ompi_proc = NULL;
    mca_mtl_ofi_endpoint_t *endpoint = NULL;
    fi_addr_t remote_proc = ompi_mtl_ofi.any_addr;
    uint64_t match_bits, mask_bits;
    ssize_t ret;
    struct fi_msg_tagged msg;
    uint64_t msgflags = FI_PEEK | FI_CLAIM | FI_COMPLETION;
    int ctxt_id = 0;
    mca_mtl_comm_t *mtl_comm;

    if (!OMPI_COMM_IS_GLOBAL_INDEX(comm)) {
        mtl_comm = comm->c_mtl_comm;
        if ((src == MPI_ANY_SOURCE || mtl_comm->c_index_vec[src].c_index_state > MCA_MTL_OFI_CID_EXCHANGED) 
             && !ompi_mtl_ofi.has_posted_initial_buffer) {
            ompi_mtl_ofi.has_posted_initial_buffer = true;
            ret = ompi_mtl_ofi_post_recv_excid_buffer(false, comm, -1);
        }
        if (src >= 0 && mtl_comm->c_index_vec[src].c_index_state == MCA_MTL_OFI_CID_NOT_EXCHANGED) {
            mtl_comm->c_index_vec[src].c_index_state = MCA_MTL_OFI_CID_EXCHANGING;
            ret = ompi_mtl_ofi_send_excid(mtl, comm, src, ofi_cq_data, false);
        }
    }

    ctxt_id = ompi_mtl_ofi_ctxt_index_for_comm(comm);
    set_thread_context(ctxt_id);

    ofi_req = malloc(sizeof *ofi_req);
    if (NULL == ofi_req) {
        return OMPI_ERROR;
    }

    /**
     * If the source is known, use its peer_fiaddr.
     */

    if (ofi_cq_data) {
        if (MPI_ANY_SOURCE != src) {
            ompi_proc = ompi_comm_peer_lookup( comm, src );
            endpoint = ompi_mtl_ofi_get_endpoint(mtl, ompi_proc);
            if (!ompi_mtl_ofi.hosted_multi_ep &&
                !ompi_mtl_ofi.hosted_dst_in_cqd_tag) {
                remote_proc = fi_rx_addr(endpoint->peer_fiaddr, ctxt_id,
                                         ompi_mtl_ofi.rx_ctx_bits);
            }
        }

        mtl_ofi_create_recv_tag_CQD(&match_bits, &mask_bits, comm->c_index,
                                    tag, src);
    }
    else {
        /* src_addr is ignored when FI_DIRECTED_RECV is not used */
        mtl_ofi_create_recv_tag(&match_bits, &mask_bits, comm->c_index, src,
                                tag);
    }

    /**
     * fi_trecvmsg with FI_PEEK and FI_CLAIM:
     * Initiate a search for a match in the hardware or software queue.
     * If successful, libfabric will enqueue a context entry into the completion
     * queue to make the search nonblocking.  This code will poll until the
     * entry is enqueued.
     */
    msg.msg_iov = NULL;
    msg.desc = NULL;
    msg.iov_count = 0;
    msg.addr = remote_proc;
    msg.tag = match_bits;
    msg.ignore = mask_bits;
    msg.context = (void *)&ofi_req->ctx;
    msg.data = 0;

    ofi_req->type = OMPI_MTL_OFI_PROBE;
    ofi_req->event_callback = ompi_mtl_ofi_probe_callback;
    ofi_req->error_callback = ompi_mtl_ofi_probe_error_callback;
    ofi_req->completion_count = 1;
    ofi_req->match_state = 0;
    ofi_req->mask_bits = mask_bits;

    MTL_OFI_RETRY_UNTIL_DONE_CTXT(fi_trecvmsg(ompi_mtl_ofi.ofi_ctxt[ctxt_id].rx_ep, &msg, msgflags), ret, ctxt_id);
    if (OPAL_UNLIKELY(0 > ret)) {
        MTL_OFI_LOG_FI_ERR(ret, "fi_trecvmsg failed");
        free(ofi_req);
        return ompi_mtl_ofi_get_error(ret);
    }

    while (0 < ofi_req->completion_count) {
        ompi_mtl_ofi_progress_after_eagain(ctxt_id);
    }

    *matched = ofi_req->match_state;
    if (1 == *matched) {
        if (MPI_STATUS_IGNORE != status) {
            OMPI_COPY_STATUS(status, ofi_req->status, false);
        }

        (*message) = ompi_message_alloc();
        if (NULL == (*message)) {
            return OMPI_ERR_OUT_OF_RESOURCE;
        }

        (*message)->comm = comm;
        (*message)->req_ptr = ofi_req;
        (*message)->peer = ofi_req->status.MPI_SOURCE;
        (*message)->count = ofi_req->status._ucount;

    } else {
        (*message) = MPI_MESSAGE_NULL;
        free(ofi_req);
    }

    return OMPI_SUCCESS;
}

__opal_attribute_always_inline__ static inline int
ompi_mtl_ofi_cancel(struct mca_mtl_base_module_t *mtl,
                    mca_mtl_request_t *mtl_request,
                    int flag)
{
    int ret, ctxt_id = 0;
    ompi_mtl_ofi_request_t *ofi_req = (ompi_mtl_ofi_request_t*) mtl_request;

    switch (ofi_req->type) {
        case OMPI_MTL_OFI_PROBE:
        case OMPI_MTL_OFI_SEND:
            /**
             * Cannot cancel sends yet
             */
            break;

        case OMPI_MTL_OFI_RECV:
            /**
             * Cancel a receive request only if it hasn't been matched yet.
             * The event queue needs to be drained to make sure there isn't
             * any pending receive completion event.
             */
#if HAVE_LITHE
            if (ompi_mtl_ofi_hosted_sc_enabled()
                && ompi_mtl_ofi_hosted_sc_cancel_recv(ofi_req)) {
                break;
            }
#endif
            ctxt_id = ompi_mtl_ofi_map_comm_to_ctxt(ofi_req->comm->c_index);
            ompi_mtl_ofi_progress_after_eagain(ctxt_id);

            if (!ofi_req->req_started) {
                ret = fi_cancel((fid_t)ompi_mtl_ofi.ofi_ctxt[ctxt_id].rx_ep,
                               &ofi_req->ctx);
                if (0 == ret) {
                    if (ofi_req->req_started)
                        goto ofi_cancel_not_possible;
                } else {
ofi_cancel_not_possible:
                    /**
                     * Could not cancel the request.
                     */
                    ofi_req->super.ompi_req->req_status._cancelled = false;
                }
            }
            break;

        default:
            return OMPI_ERROR;
    }

    return OMPI_SUCCESS;
}

#ifdef MCA_ompi_mtl_DIRECT_CALL

__opal_attribute_always_inline__ static inline int
ompi_mtl_ofi_send(struct mca_mtl_base_module_t *mtl,
                  struct ompi_communicator_t *comm,
                  int dest,
                  int tag,
                  struct opal_convertor_t *convertor,
                  mca_pml_base_send_mode_t mode)
{
    return ompi_mtl_ofi_send_generic(mtl, comm, dest, tag,
                                    convertor, mode,
                                    ompi_mtl_ofi.fi_cq_data);
}

__opal_attribute_always_inline__ static inline int
ompi_mtl_ofi_isend(struct mca_mtl_base_module_t *mtl,
               struct ompi_communicator_t *comm,
               int dest,
               int tag,
               struct opal_convertor_t *convertor,
               mca_pml_base_send_mode_t mode,
               bool blocking,
               mca_mtl_request_t *mtl_request)
{
    return ompi_mtl_ofi_isend_generic(mtl, comm, dest, tag,
                                    convertor, mode, blocking, mtl_request,
                                    ompi_mtl_ofi.fi_cq_data);
}

__opal_attribute_always_inline__ static inline int
ompi_mtl_ofi_irecv(struct mca_mtl_base_module_t *mtl,
               struct ompi_communicator_t *comm,
               int src,
               int tag,
               struct opal_convertor_t *convertor,
               mca_mtl_request_t *mtl_request)
{
    return ompi_mtl_ofi_irecv_generic(mtl, comm, src, tag,
                                    convertor, mtl_request,
                                    ompi_mtl_ofi.fi_cq_data);
}

__opal_attribute_always_inline__ static inline int
ompi_mtl_ofi_iprobe(struct mca_mtl_base_module_t *mtl,
                struct ompi_communicator_t *comm,
                int src,
                int tag,
                int *flag,
                struct ompi_status_public_t *status)
{
    return ompi_mtl_ofi_iprobe_generic(mtl, comm, src, tag,
                                    flag, status,
                                    ompi_mtl_ofi.fi_cq_data);
}

__opal_attribute_always_inline__ static inline int
ompi_mtl_ofi_improbe(struct mca_mtl_base_module_t *mtl,
                 struct ompi_communicator_t *comm,
                 int src,
                 int tag,
                 int *matched,
                 struct ompi_message_t **message,
                 struct ompi_status_public_t *status)
{
    return ompi_mtl_ofi_improbe_generic(mtl, comm, src, tag,
                                    matched, message, status,
                                    ompi_mtl_ofi.fi_cq_data);
}
#endif

END_C_DECLS

#endif  /* MTL_OFI_H_HAS_BEEN_INCLUDED */
