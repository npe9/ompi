/*
 * Hosted Lithe multicontext: same-OS-process short-circuit for MTL/OFI.
 *
 * Co-resident logical ranks (vpid/RPH equal) exchange via memcpy + local
 * match queues instead of cxi/OFI loopback. Remote OS peers still use OFI.
 * Works for single-OS and multi-OS; multi-OS ANY_SOURCE is OFI-only (no
 * dual-post). RD collectives use specific ranks so co-resident steps SC.
 *
 * Enable (default on when hosted RPH>=2): LITHE_MTL_OFI_HOSTED_SC=1
 * Disable: LITHE_MTL_OFI_HOSTED_SC=0
 */
#ifndef MTL_OFI_HOSTED_SC_H
#define MTL_OFI_HOSTED_SC_H

#include "ompi_config.h"

#if HAVE_LITHE

#include "ompi/mca/pml/pml.h"
#include <stdbool.h>
#include <stddef.h>

struct ompi_communicator_t;
struct ompi_proc_t;
struct ompi_mtl_ofi_request_t;
typedef struct ompi_mtl_ofi_request_t ompi_mtl_ofi_request_t;

BEGIN_C_DECLS

int ompi_mtl_ofi_hosted_sc_init(void);
void ompi_mtl_ofi_hosted_sc_finalize(void);

/* True when short-circuit is active for this process. */
int ompi_mtl_ofi_hosted_sc_enabled(void);

/* True iff peer shares this OS process (vpid/RPH host equality). */
int ompi_mtl_ofi_hosted_sc_same_os(struct ompi_proc_t *peer);

/*
 * Blocking / nonblocking send. Caller has packed convertor into start/length.
 * Returns 1 if handled (skip OFI); 0 to fall through to OFI.
 */
int ompi_mtl_ofi_hosted_sc_try_send(struct ompi_communicator_t *comm, int dest,
                                    int tag, void *start, size_t length,
                                    bool free_after,
                                    mca_pml_base_send_mode_t mode,
                                    ompi_mtl_ofi_request_t *ofi_req,
                                    bool is_isend);

/*
 * Irecv. Returns 1 if fully handled (no fi_trecv); 0 = OFI only.
 * (Return 2 / dual-post is unused — multi-OS ANY_SOURCE is OFI-only.)
 */
int ompi_mtl_ofi_hosted_sc_try_irecv(struct ompi_communicator_t *comm, int src,
                                     int tag, void *start, size_t length,
                                     ompi_mtl_ofi_request_t *ofi_req);

/* After fi_trecv for dual-posted ANY_SOURCE: mark cancel-safe (or drop if SC won). */
void ompi_mtl_ofi_hosted_sc_mark_ofi_posted(ompi_mtl_ofi_request_t *ofi_req);

/* OFI recv completed: remove dual-posted entry if still on SC queue. */
void ompi_mtl_ofi_hosted_sc_ofi_recv_claimed(ompi_mtl_ofi_request_t *ofi_req);

/* Cancel a recv that may be on the SC posted list. Returns 1 if cancelled. */
int ompi_mtl_ofi_hosted_sc_cancel_recv(ompi_mtl_ofi_request_t *ofi_req);

/*
 * Single-OS SC wait helper for wait_sync scarce path. If this rank has a
 * pending same-OS posted recv: cpu_relax spin (avoid yield→empty steal);
 * after a long streak, park until matching enqueue unblocks. Returns 1 if
 * handled (caller must not yield), 0 otherwise. Multi-OS returns 0.
 */
int ompi_mtl_ofi_hosted_sc_try_park_pending(void);

END_C_DECLS

#endif /* HAVE_LITHE */
#endif /* MTL_OFI_HOSTED_SC_H */
