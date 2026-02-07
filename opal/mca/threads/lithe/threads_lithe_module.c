/* OPAL Lithe Threading Module - Proper Scheduler Architecture */

#include "opal_config.h"
#include "opal/mca/threads/threads.h"
#include "opal/mca/threads/lithe/threads_lithe.h"
#include "opal/constants.h"

#include <lithe/lithe.h>
#include <lithe/fork_join_sched.h>
#include <lithe/mutex.h>
#include <lithe/condvar.h>
#include <parlib/parlib.h>
#include <parlib/uthread.h>
#include <parlib/vcore.h>
#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <execinfo.h>
#include <sys/mman.h>
#include <time.h>
#include <errno.h>
#include <unistd.h>

/* Parlib declarations */
extern bool in_vcore_context(void);

/* OPAL scheduler - exported for use by component */
lithe_fork_join_sched_t *opal_sched = NULL;
bool opal_sched_entered = false;

/* Check if Lithe is ready (current_uthread exists) */
extern __thread struct uthread *current_uthread;
static inline bool lithe_is_ready(void) {
    return current_uthread != NULL;
}

/*
 * Use real PMIx mutex layout so m_lock_opal offset is correct.
 * Include path: -I$(top_srcdir)/3rd-party/openpmix (from opal, top_srcdir is ompi5 root).
 */
#include "src/threads/pmix_mutex.h"
#include "src/threads/pmix_threads.h"
#include "src/class/pmix_object.h"
#include <stddef.h>

static inline void **pmix_mutex_m_lock_opal_ptr(pmix_mutex_t *m) {
    return (void **)((char *)m + offsetof(pmix_mutex_t, m_lock_opal));
}

/* Extern declaration - resolves to libpmix's function */
extern void pmix_set_threading_backend(
    pmix_mutex_lock_fn_t lock_fn,
    pmix_mutex_unlock_fn_t unlock_fn,
    pmix_mutex_trylock_fn_t trylock_fn);

/* Helper: Ensure m_lock_opal is initialized atomically */
static inline lithe_mutex_t *ensure_lithe_mutex(struct pmix_mutex_t *m) {
    void **p = pmix_mutex_m_lock_opal_ptr(m);
    lithe_mutex_t *existing = __atomic_load_n(p, __ATOMIC_ACQUIRE);
    if (existing) return existing;
    
    lithe_mutex_t *lm = malloc(sizeof(lithe_mutex_t));
    if (!lm) return NULL;
    lithe_mutex_init(lm, NULL);
    
    lithe_mutex_t *expected = NULL;
    if (__atomic_compare_exchange_n(p, &expected, lm, 0, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE)) {
        return lm;
    }
    free(lm);
    return expected;
}

/* PMIx backend implementations using Lithe.
 * Each PMIx mutex uses its m_lock_opal field to store a Lithe mutex.
 * NO pthread synchronization - only Lithe primitives! */
static int pmix_lithe_lock(struct pmix_mutex_t *m) {
    lithe_mutex_t *lm = ensure_lithe_mutex(m);
    if (lm) {
        lithe_mutex_lock(lm);
    }
    return 0;
}

static int pmix_lithe_unlock(struct pmix_mutex_t *m) {
    void **p = pmix_mutex_m_lock_opal_ptr(m);
    lithe_mutex_t *lm = __atomic_load_n(p, __ATOMIC_ACQUIRE);
    if (lm) {
        lithe_mutex_unlock(lm);
    }
    return 0;
}

static int pmix_lithe_trylock(struct pmix_mutex_t *m) {
    lithe_mutex_t *lm = ensure_lithe_mutex(m);
    if (lm) {
        return lithe_mutex_trylock(lm);
    }
    return 0;
}

/* PMIx object lock backend - use obj_lock_opal to store lithe_mutex_t* */
static inline void **pmix_object_obj_lock_opal_ptr(pmix_object_t *obj) {
    return (void **)((char *)obj + offsetof(pmix_object_t, obj_lock_opal));
}

static inline lithe_mutex_t *ensure_lithe_mutex_for_object(pmix_object_t *obj) {
    void **p = pmix_object_obj_lock_opal_ptr(obj);
    lithe_mutex_t *existing = __atomic_load_n(p, __ATOMIC_ACQUIRE);
    if (existing) return existing;
    lithe_mutex_t *lm = malloc(sizeof(lithe_mutex_t));
    if (!lm) return NULL;
    lithe_mutex_init(lm, NULL);
    lithe_mutex_t *expected = NULL;
    if (__atomic_compare_exchange_n(p, &expected, lm, 0, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE)) {
        return lm;
    }
    free(lm);
    return expected;
}

static void pmix_lithe_obj_lock_init(pmix_object_t *obj) {
    lithe_mutex_t *lm = malloc(sizeof(lithe_mutex_t));
    if (lm) {
        lithe_mutex_init(lm, NULL);
        void **p = pmix_object_obj_lock_opal_ptr(obj);
        __atomic_store_n(p, lm, __ATOMIC_RELEASE);
    }
}

static void pmix_lithe_obj_lock_destroy(pmix_object_t *obj) {
    void **p = pmix_object_obj_lock_opal_ptr(obj);
    lithe_mutex_t *lm = __atomic_exchange_n(p, NULL, __ATOMIC_ACQ_REL);
    if (lm) {
        free(lm);
    }
}

static int pmix_lithe_obj_lock_lock(pmix_object_t *obj) {
    lithe_mutex_t *lm = ensure_lithe_mutex_for_object(obj);
    if (lm) lithe_mutex_lock(lm);
    return 0;
}

static int pmix_lithe_obj_lock_unlock(pmix_object_t *obj) {
    void **p = pmix_object_obj_lock_opal_ptr(obj);
    lithe_mutex_t *lm = __atomic_load_n(p, __ATOMIC_ACQUIRE);
    if (lm) lithe_mutex_unlock(lm);
    return 0;
}

/* Class lock - single global mutex for pmix_class_initialize() */
static lithe_mutex_t pmix_class_init_mutex;
static int pmix_class_init_mutex_initialized = 0;

static void pmix_lithe_class_lock(void) {
    if (!__atomic_load_n(&pmix_class_init_mutex_initialized, __ATOMIC_ACQUIRE)) {
        lithe_mutex_init(&pmix_class_init_mutex, NULL);
        __atomic_store_n(&pmix_class_init_mutex_initialized, 1, __ATOMIC_RELEASE);
    }
    lithe_mutex_lock(&pmix_class_init_mutex);
}

static void pmix_lithe_class_unlock(void) {
    lithe_mutex_unlock(&pmix_class_init_mutex);
}

extern void pmix_set_object_lock_backend(
    pmix_obj_lock_init_fn_t init_fn,
    pmix_obj_lock_destroy_fn_t destroy_fn,
    pmix_obj_lock_lock_fn_t lock_fn,
    pmix_obj_lock_unlock_fn_t unlock_fn);
extern void pmix_set_class_lock_backend(pmix_class_lock_fn_t lock_fn, pmix_class_lock_fn_t unlock_fn);
extern void pmix_set_thread_is_current_backend(pmix_thread_is_current_fn_t fn);

static int pmix_lithe_thread_is_current(pthread_t handle) {
    lithe_context_t *cur = lithe_context_self();
    return (cur != NULL && (void*)cur == (void*)(uintptr_t)handle) ? 1 : 0;
}

/* PMIx thread backend interface (weak symbol - may not exist) */
struct pmix_thread_t;
typedef int (*pmix_thread_create_fn_t)(struct pmix_thread_t *);
typedef int (*pmix_thread_join_fn_t)(struct pmix_thread_t *, void **);

/* Extern declaration - resolves to libpmix's function */
extern void pmix_set_thread_backend(
    pmix_thread_create_fn_t create_fn,
    pmix_thread_join_fn_t join_fn);

/* PMIx condvar structure - MUST match pmix_condition_t layout exactly!
 * pmix_condition_t is: { pthread_cond_t pthread_cond; void *lithe_cond; }
 * We use lithe_cond field to store our Lithe condvar.
 * NO pthread synchronization! */
struct pmix_condition_internal {
    pthread_cond_t pthread_cond;  /* Unused in Lithe mode */
    lithe_condvar_t *lithe_cv;    /* Same offset as lithe_cond in pmix_condition_t */
    /* NO MORE FIELDS - next memory is pmix_lock_t.active! */
};

/* PMIx condvar backend implementations using Lithe condvars */
static int pmix_lithe_cond_init(struct pmix_condition_t *c) {
    struct pmix_condition_internal *cond = (struct pmix_condition_internal *)c;
    cond->lithe_cv = malloc(sizeof(lithe_condvar_t));
    if (cond->lithe_cv) {
        lithe_condvar_init(cond->lithe_cv);
    }
    return 0;
}

static int pmix_lithe_cond_destroy(struct pmix_condition_t *c) {
    struct pmix_condition_internal *cond = (struct pmix_condition_internal *)c;
    if (cond->lithe_cv) {
        free(cond->lithe_cv);
        cond->lithe_cv = NULL;
    }
    return 0;
}

/* Track if we have a separate progress context */
static volatile bool have_progress_context = false;
static lithe_fork_join_context_t *pmix_progress_context = NULL;

static int pmix_lithe_cond_wait(struct pmix_condition_t *c, struct pmix_mutex_t *m) {
    struct pmix_condition_internal *cond = (struct pmix_condition_internal *)c;
    lithe_mutex_t *lm = __atomic_load_n(pmix_mutex_m_lock_opal_ptr(m), __ATOMIC_ACQUIRE);
    
    if (cond->lithe_cv && lm) {
        lithe_condvar_wait(cond->lithe_cv, lm);
    } else {
        pmix_lithe_unlock(m);
        lithe_context_yield();
        pmix_lithe_lock(m);
    }
    return 0;
}

static int pmix_lithe_cond_signal(struct pmix_condition_t *c) {
    struct pmix_condition_internal *cond = (struct pmix_condition_internal *)c;
    if (cond->lithe_cv) {
        lithe_condvar_signal(cond->lithe_cv);
    }
    return 0;
}

static int pmix_lithe_cond_broadcast(struct pmix_condition_t *c) {
    struct pmix_condition_internal *cond = (struct pmix_condition_internal *)c;
    if (cond->lithe_cv) {
        lithe_condvar_broadcast(cond->lithe_cv);
    }
    
    return 0;
}

/* Forward declarations for PMIx thread backend */
static int pmix_lithe_thread_create(struct pmix_thread_t *t);
static int pmix_lithe_thread_join(struct pmix_thread_t *t, void **ret);

/* Debug: set LITHE_DEBUG=1 to enable one-time init messages (no hot-path logging) */
static int lithe_debug_enabled(void) {
    return getenv("LITHE_DEBUG") != NULL;
}

/* Early constructor - sets ALL PMIx backends */
__attribute__((constructor(101)))
static void early_pmix_backend_init(void) {
    if (lithe_debug_enabled())
        fprintf(stderr, "[LITHE-BACKEND] early_pmix_backend_init called\n");
    
    /* Set condvar backend */
    pmix_set_condvar_backend(pmix_lithe_cond_wait, pmix_lithe_cond_signal,
                             pmix_lithe_cond_broadcast, pmix_lithe_cond_init,
                             pmix_lithe_cond_destroy);
    /* Set thread backend - PMIx threads will be Lithe contexts */
    pmix_set_thread_backend(pmix_lithe_thread_create, pmix_lithe_thread_join);
    /* Set mutex backend - per-object Lithe mutexes, no pthread sync! */
    pmix_set_threading_backend(pmix_lithe_lock, pmix_lithe_unlock, pmix_lithe_trylock);
    /* Object lock backend - per-object Lithe mutex via obj_lock_opal */
    pmix_set_object_lock_backend(pmix_lithe_obj_lock_init, pmix_lithe_obj_lock_destroy,
                                 pmix_lithe_obj_lock_lock, pmix_lithe_obj_lock_unlock);
    /* Class lock backend - single Lithe mutex for class_initialize() */
    pmix_set_class_lock_backend(pmix_lithe_class_lock, pmix_lithe_class_unlock);
    /* Thread-is-current for progress thread checks */
    pmix_set_thread_is_current_backend(pmix_lithe_thread_is_current);
    if (lithe_debug_enabled())
        fprintf(stderr, "[LITHE-BACKEND] all backends set\n");
}

/* PMIx thread structure - matches pmix_thread_t layout 
 * Determined empirically: t_run is at offset 112 */
struct pmix_thread_internal {
    char super_padding[112]; /* pmix_object_t super */
    void *t_run;
    void *t_arg;
    pthread_t t_handle;
};

/* PMIx progress context wrapper - runs the actual PMIx thread function as a Lithe context */
static void pmix_lithe_context_wrapper(void *arg) {
    struct pmix_thread_internal *pt = (struct pmix_thread_internal *)arg;
    if (lithe_debug_enabled())
        fprintf(stderr, "[LITHE-BACKEND] Progress context STARTING! t_run=%p\n", pt->t_run);
    void (*func)(void*) = (void (*)(void*))pt->t_run;
    func(pt);  /* Call the actual PMIx thread function */
    if (lithe_debug_enabled())
        fprintf(stderr, "[LITHE-BACKEND] Progress context EXITING!\n");
}

/* PMIx thread creation - creates a Lithe context (NO pthreads).
 * The context will cooperate with lithe via wrapped syscalls (poll, epoll_wait, etc). */
static int pmix_lithe_thread_create(struct pmix_thread_t *t) {
    struct pmix_thread_internal *pt = (struct pmix_thread_internal *)t;
    if (lithe_debug_enabled())
        fprintf(stderr, "[LITHE-BACKEND] thread_create t_run=%p, opal_sched=%p\n", pt->t_run, (void*)opal_sched);
    
    /* Make sure we have a scheduler */
    if (!opal_sched) {
        opal_sched = lithe_fork_join_sched_create();
        if (!opal_sched) {
            if (lithe_debug_enabled())
                fprintf(stderr, "[LITHE-BACKEND] Failed to create scheduler!\n");
            return -1;
        }
        if (lithe_debug_enabled())
            fprintf(stderr, "[LITHE-BACKEND] Created scheduler %p\n", (void*)opal_sched);
    }
    
    /* Enter the scheduler if not already entered */
    if (!opal_sched_entered) {
        lithe_sched_t *cur = lithe_sched_current();
        if (cur != NULL && !in_vcore_context()) {
            lithe_sched_enter((lithe_sched_t *)opal_sched);
            opal_sched_entered = true;
            if (lithe_debug_enabled())
                fprintf(stderr, "[LITHE-BACKEND] Entered scheduler\n");
        }
    }
    
    /* Create a Lithe context for the PMIx progress thread */
    pmix_progress_context = lithe_fork_join_context_create(opal_sched, 262144, 
                                                          pmix_lithe_context_wrapper, pt);
    if (!pmix_progress_context) {
        if (lithe_debug_enabled())
            fprintf(stderr, "[LITHE-BACKEND] thread_create failed: lithe_fork_join_context_create returned NULL\n");
        return -1;
    }
    
    /* Mark that we have a progress context */
    have_progress_context = true;
    
    if (lithe_debug_enabled())
        fprintf(stderr, "[LITHE-BACKEND] Created PMIx progress context %p\n", (void*)pmix_progress_context);
    pt->t_handle = (pthread_t)(uintptr_t)pmix_progress_context;
    return 0;
}

static int pmix_lithe_thread_join(struct pmix_thread_t *t, void **ret) {
    struct pmix_thread_internal *pt = (struct pmix_thread_internal *)t;
    
    /* Join the Lithe context */
    if (pt->t_handle != (pthread_t)0 && pmix_progress_context != NULL) {
        if (lithe_debug_enabled())
            fprintf(stderr, "[LITHE-BACKEND] thread_join: waiting for progress context\n");
        lithe_fork_join_sched_join_one(opal_sched);
        lithe_fork_join_context_destroy(pmix_progress_context);
        pmix_progress_context = NULL;
        pt->t_handle = (pthread_t)0;
        if (lithe_debug_enabled())
            fprintf(stderr, "[LITHE-BACKEND] thread_join: done\n");
    }
    if (ret) *ret = NULL;
    return 0;
}

/* Local function declarations */
static int lithe_init(void);
static int lithe_finalize(void);
static int lithe_thread_create(opal_thread_fn_t func, void *arg, opal_thread_t *t, int *priority);
static int lithe_thread_join(opal_thread_t *t, void **exit_status);
static int lithe_thread_self(opal_thread_t *t);
static int lithe_thread_equal(opal_thread_t t1, opal_thread_t t2);
static int lithe_yield_fn(void);
static int lithe_set_affinity(opal_thread_t *t, void *topo, int bitmap_index);
static int lithe_get_affinity(opal_thread_t *t, void *topo, int bitmap_index);

/* Compatibility for PMIx */
typedef void(opal_threads_pthreads_yield_fn_t)(void);
static void yield_wrapper(void) { lithe_context_yield(); }
__attribute__((visibility("default"))) 
opal_threads_pthreads_yield_fn_t *opal_threads_pthreads_yield_fn = &yield_wrapper;

/* Wrapper */
static void lithe_wrapper(void *arg) {
    opal_thread_t *t = (opal_thread_t*)arg;
    if (t && t->t_run) t->t_run((opal_object_t*)t);
}

/* Thread handle stored in t_handle (pthread_t is uintptr_t) */
struct lithe_thread_handle {
    lithe_fork_join_context_t *ctx;
};

/* Module - matches opal_threads_base_module_t */
opal_threads_base_module_t opal_threads_lithe_module = {
    .threads_init = lithe_init,
    .threads_finalize = lithe_finalize,
    .thread_create = lithe_thread_create,
    .thread_join = lithe_thread_join,
    .thread_self = lithe_thread_self,
    .thread_equal = lithe_thread_equal,
    .thread_yield = lithe_yield_fn,
    .thread_set_affinity = lithe_set_affinity,
    .thread_get_affinity = lithe_get_affinity,
};

static int lithe_init(void) {
    extern __thread struct uthread *current_uthread;
    if (lithe_debug_enabled())
        fprintf(stderr, "[LITHE-INIT] current_uthread=%p\n", (void*)current_uthread);
    if (current_uthread == NULL) {
        if (lithe_debug_enabled())
            fprintf(stderr, "[LITHE-INIT] current_uthread is NULL, returning error\n");
        return OPAL_ERROR;
    }
    if (opal_sched != NULL && opal_sched_entered) {
        if (lithe_debug_enabled())
            fprintf(stderr, "[LITHE-INIT] Already initialized\n");
        return OPAL_SUCCESS;
    }
    
    opal_sched = lithe_fork_join_sched_create();
    if (lithe_debug_enabled())
        fprintf(stderr, "[LITHE-INIT] Created scheduler %p\n", (void*)opal_sched);
    if (opal_sched == NULL) return OPAL_ERROR;
    
    lithe_sched_t *cur = lithe_sched_current();
    if (lithe_debug_enabled())
        fprintf(stderr, "[LITHE-INIT] current_sched=%p, in_vcore=%d\n", (void*)cur, in_vcore_context());
    if (cur != NULL && !in_vcore_context()) {
        if (lithe_debug_enabled())
            fprintf(stderr, "[LITHE-INIT] Entering scheduler\n");
        lithe_sched_enter((lithe_sched_t *)opal_sched);
        opal_sched_entered = true;
        if (lithe_debug_enabled())
            fprintf(stderr, "[LITHE-INIT] Entered scheduler\n");
    } else {
        if (lithe_debug_enabled())
            fprintf(stderr, "[LITHE-INIT] NOT entering scheduler (cur=%p, in_vcore=%d)\n", (void*)cur, in_vcore_context());
    }
    
    /* Initialize PMIx threading backend to use Lithe - per-object mutexes, no pthread sync! */
    pmix_set_threading_backend(pmix_lithe_lock, pmix_lithe_unlock, pmix_lithe_trylock);
    
    /* Initialize PMIx thread backend to create Lithe contexts instead of pthreads */
    pmix_set_thread_backend(pmix_lithe_thread_create, pmix_lithe_thread_join);
    
    /* Initialize PMIx condvar backend to use Lithe condvars instead of pthreads */
    pmix_set_condvar_backend(pmix_lithe_cond_wait, pmix_lithe_cond_signal,
                             pmix_lithe_cond_broadcast, pmix_lithe_cond_init,
                             pmix_lithe_cond_destroy);
    
    return OPAL_SUCCESS;
}

static int lithe_finalize(void) {
    if (opal_sched_entered) { lithe_sched_exit(); opal_sched_entered = false; }
    if (opal_sched) { lithe_fork_join_sched_destroy(opal_sched); opal_sched = NULL; }
    return OPAL_SUCCESS;
}

static int lithe_thread_create(opal_thread_fn_t func, void *arg,
                               opal_thread_t *t, int *priority) {
    (void)arg; (void)priority;
    if (!opal_sched_entered || !opal_sched) return OPAL_ERROR;
    
    t->t_run = func;
    lithe_fork_join_context_t *ctx = lithe_fork_join_context_create(opal_sched, 262144, lithe_wrapper, t);
    if (!ctx) return OPAL_ERROR;
    
    struct lithe_thread_handle *h = malloc(sizeof(*h));
    if (!h) { lithe_fork_join_context_destroy(ctx); return OPAL_ERROR; }
    h->ctx = ctx;
    t->t_handle = (pthread_t)(uintptr_t)h;
    return OPAL_SUCCESS;
}

static int lithe_thread_join(opal_thread_t *t, void **exit_status) {
    if (!t->t_handle) return OPAL_ERR_BAD_PARAM;
    struct lithe_thread_handle *h = (struct lithe_thread_handle *)(uintptr_t)t->t_handle;
    lithe_fork_join_sched_join_one(opal_sched);
    lithe_fork_join_context_destroy(h->ctx);
    free(h);
    if (exit_status) *exit_status = NULL;
    t->t_handle = (pthread_t)0;
    return OPAL_SUCCESS;
}

static int lithe_thread_self(opal_thread_t *t) {
    (void)t;
    return OPAL_SUCCESS;
}

static int lithe_thread_equal(opal_thread_t t1, opal_thread_t t2) {
    return t1.t_handle == t2.t_handle;
}

static int lithe_yield_fn(void) {
    lithe_context_yield();
    return OPAL_SUCCESS;
}

static int lithe_set_affinity(opal_thread_t *t, void *topo, int bitmap_index) {
    (void)t; (void)topo; (void)bitmap_index;
    return OPAL_SUCCESS;
}

static int lithe_get_affinity(opal_thread_t *t, void *topo, int bitmap_index) {
    (void)t; (void)topo; (void)bitmap_index;
    return OPAL_SUCCESS;
}

/* Public API functions */
int opal_threads_lithe_init(void) { return lithe_init(); }
int opal_threads_lithe_finalize(void) { return lithe_finalize(); }
int opal_threads_lithe_thread_create(opal_thread_fn_t func, void *arg) {
    opal_thread_t t;
    return lithe_thread_create(func, arg, &t, NULL);
}
int opal_threads_lithe_thread_join(opal_thread_t *thread) {
    return lithe_thread_join(thread, NULL);
}
int opal_threads_lithe_yield(void) { return lithe_yield_fn(); }
lithe_context_t *opal_get_next_helper_context(void) { return NULL; }
void opal_threads_lithe_preinit(void) {}
