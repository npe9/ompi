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
#include <sched.h>
#include <time.h>
#include <errno.h>
#include <unistd.h>

#include "src/threads/pmix_threads.h"

extern bool in_vcore_context(void);
extern void uthread_default_vcore_entry(void) __attribute__((noreturn));

lithe_fork_join_sched_t *opal_sched = NULL;
bool opal_sched_entered = false;

volatile int opal_lithe_vcore_ready = 0;

/* pmix_thread_start may not see pmix_lithe_get_fork_join_sched() after OPAL's
 * register in rare link/order cases; OPAL publishes the same pointer here. */
lithe_fork_join_sched_t *opal_lithe_pmix_export_sched = NULL;

/*
 * Strong vcore_entry overrides libvcore_confstub's no-op.
 * Called by parlib when a vcore wakes from futex_wait.
 * Must never return.
 */
void __attribute__((noreturn)) vcore_entry(void)
{
    uthread_default_vcore_entry();
    __builtin_unreachable();
}

extern __thread struct uthread *current_uthread;
static inline bool lithe_is_ready(void) {
    return current_uthread != NULL;
}

static int lithe_debug_enabled(void) {
    return getenv("LITHE_DEBUG") != NULL;
}

static int lithe_init(void);
static int lithe_finalize(void);
static int lithe_thread_create(opal_thread_fn_t func, void *arg, opal_thread_t *t, int *priority);
static int lithe_thread_join(opal_thread_t *t, void **exit_status);
static int lithe_thread_self(opal_thread_t *t);
static int lithe_thread_equal(opal_thread_t t1, opal_thread_t t2);
static int lithe_yield_fn(void);
static int lithe_set_affinity(opal_thread_t *t, void *topo, int bitmap_index);
static int lithe_get_affinity(opal_thread_t *t, void *topo, int bitmap_index);

typedef void(opal_threads_pthreads_yield_fn_t)(void);
static void yield_wrapper(void) { lithe_context_yield(); sched_yield(); }
__attribute__((visibility("default")))
opal_threads_pthreads_yield_fn_t *opal_threads_pthreads_yield_fn = &yield_wrapper;

static void lithe_wrapper(void *arg) {
    opal_thread_t *t = (opal_thread_t*)arg;
    if (t && t->t_run) t->t_run((opal_object_t*)t);
}

struct lithe_thread_handle {
    lithe_fork_join_context_t *ctx;
};

void opal_threads_lithe_ensure_opal_fork_join_sched(void)
{
    if (opal_sched == NULL) {
        opal_sched = lithe_fork_join_sched_create();
        if (opal_sched == NULL) {
            fprintf(stderr, "[OPAL-Lithe] lithe_fork_join_sched_create returned NULL\n");
            return;
        }
    }
    opal_lithe_pmix_export_sched = opal_sched;
    pmix_lithe_register_fork_join_sched(opal_sched);
    if (!opal_sched_entered) {
        lithe_sched_t *cur = lithe_sched_current();
        if (cur != NULL) {
            lithe_sched_enter((lithe_sched_t *)opal_sched);
            opal_sched_entered = true;
        }
    }
}

/* OpenPMIx weak hook: pmix_thread_start may run before any OPAL mutex. */
void opal_pmix_lithe_host_ensure_sched(void)
{
    lithe_ensure_main_on_vcore0();
    opal_threads_lithe_ensure_opal_fork_join_sched();
}

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
    LITHE_ASSERT_VCORE();
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

    opal_threads_lithe_ensure_opal_fork_join_sched();
    if (lithe_debug_enabled()) {
        lithe_sched_t *cur = lithe_sched_current();
        fprintf(stderr, "[LITHE-INIT] opal_sched=%p entered=%d current_sched=%p in_vcore=%d\n",
                (void *)opal_sched, (int)opal_sched_entered, (void *)cur, in_vcore_context());
    }
    if (opal_sched == NULL) {
        return OPAL_ERROR;
    }

    return OPAL_SUCCESS;
}

static int lithe_finalize(void) {
    /* Intentionally keep the scheduler alive. The threads framework closes
     * before PMIx during OPAL shutdown (forward iteration of opal_init_frameworks),
     * but PMIx finalize still needs to join its progress thread via
     * __wrap_pmix_thread_join → lithe_fork_join_sched_join_one.
     * Tearing down the scheduler here causes uthread tls_desc=NULL aborts.
     * The OS reclaims everything at process exit. */
    return OPAL_SUCCESS;
}

static int lithe_thread_create(opal_thread_fn_t func, void *arg,
                               opal_thread_t *t, int *priority) {
    LITHE_ASSERT_VCORE();
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
    LITHE_ASSERT_VCORE();
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
    LITHE_ASSERT_VCORE();
    lithe_context_yield();
    sched_yield();
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

void ensure_main_fj_context(void)
{
    opal_threads_lithe_ensure_opal_fork_join_sched();
}

/* ================================================================
 * Lithe-native TSD (no pthread keys).
 *
 * Keys are plain integers.  Per-context values live in a global hash
 * table keyed by (context_ptr, tsd_key).  Before the Lithe scheduler
 * is active (lithe_context_self() == NULL), a dedicated "main" entry
 * is used so that early OPAL/PMIx init can create and use TSD.
 *
 * Synchronisation uses a bare CAS spinlock so it works before and
 * after the Lithe scheduler is running.
 * ================================================================ */

#include "opal/mca/threads/lithe/threads_lithe_tsd.h"
#include <string.h>

/* --- low-level spinlock (no lithe / no pthread dependency) --- */

static volatile int tsd_spinlock = 0;

static inline void tsd_spin_lock(void)
{
    while (__sync_lock_test_and_set(&tsd_spinlock, 1))
        while (tsd_spinlock)
            __asm__ volatile("pause" ::: "memory");
}

static inline void tsd_spin_unlock(void)
{
    __sync_lock_release(&tsd_spinlock);
}

/* --- key registry --- */

struct tsd_key_info {
    opal_tsd_key_t key;
    void (*destructor)(void *);
};

static struct tsd_key_info *tsd_key_registry;
static size_t              tsd_key_count;
static size_t              tsd_key_cap;
static opal_tsd_key_t      tsd_next_key = 1;  /* 0 = invalid */

/* --- per-context value storage --- */

struct tsd_entry {
    opal_tsd_key_t   key;
    void            *value;
    struct tsd_entry *next;
};

struct tsd_ctx_bucket {
    uintptr_t            ctx_id;  /* 0 = pre-scheduler main */
    struct tsd_entry    *entries;
    struct tsd_ctx_bucket *next;
};

#define TSD_HASH_SIZE 256
static struct tsd_ctx_bucket *tsd_hash[TSD_HASH_SIZE];

static inline size_t tsd_hash_fn(uintptr_t ctx_id)
{
    return (ctx_id >> 4) % TSD_HASH_SIZE;
}

static struct tsd_entry **tsd_get_list(uintptr_t ctx_id)
{
    size_t h = tsd_hash_fn(ctx_id);
    struct tsd_ctx_bucket *b = tsd_hash[h];
    while (b) {
        if (b->ctx_id == ctx_id)
            return &b->entries;
        b = b->next;
    }
    b = calloc(1, sizeof(*b));
    if (!b) return NULL;
    b->ctx_id = ctx_id;
    b->next   = tsd_hash[h];
    tsd_hash[h] = b;
    return &b->entries;
}

static inline uintptr_t tsd_current_ctx_id(void)
{
    lithe_context_t *ctx = lithe_context_self();
    return ctx ? (uintptr_t)ctx : 0;
}

/* --- public API ------------------------------------------------- */

int opal_tsd_key_create(opal_tsd_key_t *key, void (*destructor)(void *))
{
    if (!key) return OPAL_ERR_BAD_PARAM;

    tsd_spin_lock();

    opal_tsd_key_t k = tsd_next_key++;

    if (tsd_key_count >= tsd_key_cap) {
        size_t nc = tsd_key_cap ? tsd_key_cap * 2 : 16;
        struct tsd_key_info *nr = realloc(tsd_key_registry, nc * sizeof(*nr));
        if (!nr) { tsd_spin_unlock(); return OPAL_ERR_OUT_OF_RESOURCE; }
        tsd_key_registry = nr;
        tsd_key_cap = nc;
    }
    tsd_key_registry[tsd_key_count].key = k;
    tsd_key_registry[tsd_key_count].destructor = destructor;
    tsd_key_count++;

    tsd_spin_unlock();

    *key = k;
    return OPAL_SUCCESS;
}

int opal_lithe_tsd_key_delete(opal_tsd_key_t key)
{
    if (key == 0) return OPAL_ERR_BAD_PARAM;

    tsd_spin_lock();
    for (size_t i = 0; i < tsd_key_count; i++) {
        if (tsd_key_registry[i].key == key) {
            tsd_key_registry[i] = tsd_key_registry[--tsd_key_count];
            break;
        }
    }
    tsd_spin_unlock();
    return OPAL_SUCCESS;
}

int opal_lithe_tsd_set(opal_tsd_key_t key, void *value)
{
    if (key == 0) return OPAL_ERR_BAD_PARAM;

    uintptr_t cid = tsd_current_ctx_id();

    tsd_spin_lock();

    struct tsd_entry **list = tsd_get_list(cid);
    if (!list) { tsd_spin_unlock(); return OPAL_ERR_OUT_OF_RESOURCE; }

    struct tsd_entry *e = *list;
    while (e) {
        if (e->key == key) { e->value = value; tsd_spin_unlock(); return OPAL_SUCCESS; }
        e = e->next;
    }

    e = malloc(sizeof(*e));
    if (!e) { tsd_spin_unlock(); return OPAL_ERR_OUT_OF_RESOURCE; }
    e->key   = key;
    e->value = value;
    e->next  = *list;
    *list    = e;

    tsd_spin_unlock();
    return OPAL_SUCCESS;
}

int opal_lithe_tsd_get(opal_tsd_key_t key, void **valuep)
{
    if (key == 0 || !valuep) return OPAL_ERR_BAD_PARAM;

    uintptr_t cid = tsd_current_ctx_id();

    tsd_spin_lock();

    struct tsd_entry **list = tsd_get_list(cid);
    if (!list) { *valuep = NULL; tsd_spin_unlock(); return OPAL_SUCCESS; }

    struct tsd_entry *e = *list;
    while (e) {
        if (e->key == key) { *valuep = e->value; tsd_spin_unlock(); return OPAL_SUCCESS; }
        e = e->next;
    }

    *valuep = NULL;
    tsd_spin_unlock();
    return OPAL_SUCCESS;
}

int opal_threads_lithe_enter_sched_early(void)
{
    ensure_main_fj_context();
    return 0;
}
