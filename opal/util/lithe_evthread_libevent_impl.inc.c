/*
 * Included from opal and embedded OpenPMIx (see lithe_evthread_libevent.c and
 * pmix_lithe_evthread_wire.c). One TU links into libopen-pal, another into libpmix
 * so prte/prted get lith evthread callbacks without duplicate global symbols.
 */
#ifndef LITHE_EVTHREAD_WIRE_FN
#    error Define LITHE_EVTHREAD_WIRE_FN to the wired int (*)(void) name before include
#endif
#ifndef LITHE_EVTHREAD_EXPORT
#    define LITHE_EVTHREAD_EXPORT /* nothing */
#endif

#include <stdlib.h>
#include <stdint.h>

#include <event2/thread.h>

#include <lithe/lithe.h>
#include <lithe/mutex.h>
#include <lithe/condvar.h>

static void *opal_evthread_lithe_lock_alloc(unsigned locktype)
{
    lithe_mutex_t *lk = calloc(1, sizeof(*lk));
    lithe_mutexattr_t attr;

    if (lk == NULL) {
        return NULL;
    }
    lithe_mutexattr_init(&attr);
    if (locktype & EVTHREAD_LOCKTYPE_RECURSIVE) {
        lithe_mutexattr_settype(&attr, LITHE_MUTEX_RECURSIVE);
    }
    if (0 != lithe_mutex_init(lk, &attr)) {
        free(lk);
        return NULL;
    }
    return lk;
}

static void opal_evthread_lithe_lock_free(void *lock, unsigned locktype)
{
    (void) locktype;
    if (lock != NULL) {
        free(lock);
    }
}

static int opal_evthread_lithe_lock(unsigned mode, void *lock)
{
    lithe_mutex_t *lk = lock;
    int r;

    if (mode & EVTHREAD_TRY) {
        r = lithe_mutex_trylock(lk);
        return (0 == r) ? 0 : r;
    }
    r = lithe_mutex_lock(lk);
    return (0 == r) ? 0 : -1;
}

static int opal_evthread_lithe_unlock(unsigned mode, void *lock)
{
    (void) mode;
    lithe_mutex_t *lk = lock;
    return (0 == lithe_mutex_unlock(lk)) ? 0 : -1;
}

static void *opal_evthread_lithe_cond_alloc(unsigned condtype)
{
    lithe_condvar_t *cv;

    (void) condtype;
    cv = calloc(1, sizeof(*cv));
    if (cv != NULL && 0 != lithe_condvar_init(cv)) {
        free(cv);
        return NULL;
    }
    return cv;
}

static void opal_evthread_lithe_cond_free(void *cond)
{
    free(cond);
}

static int opal_evthread_lithe_cond_signal(void *cond, int broadcast)
{
    lithe_condvar_t *cv = cond;

    if (broadcast) {
        return (0 == lithe_condvar_broadcast(cv)) ? 0 : -1;
    }
    return (0 == lithe_condvar_signal(cv)) ? 0 : -1;
}

static int opal_evthread_lithe_cond_wait(void *cond, void *lock, const struct timeval *tv)
{
    lithe_condvar_t *cv = cond;
    lithe_mutex_t *mtx = lock;
    int rc;

    (void) tv;
    /*
     * Libevent 2.1 in-tree code uses untimed EVTHREAD_COND_WAIT for event_base
     * threading. There is no lithe_condvar_timedwait; non-NULL tv blocks until
     * signalled (timeout not enforced).
     */
    rc = lithe_condvar_wait(cv, mtx);
    return (0 == rc) ? 0 : -1;
}

static unsigned long opal_evthread_lithe_thread_id_cb(void)
{
    lithe_context_t *self = lithe_context_self();

    if (NULL != self) {
        return (unsigned long) (uintptr_t) self;
    }
    /* No pthread identity: sequential fallback until lith attaches a context. */
    static volatile uint64_t nid_seq;

    return (unsigned long) __sync_add_and_fetch(&nid_seq, 1ULL);
}

static int lithe_evthread_libevent_configured;

LITHE_EVTHREAD_EXPORT int LITHE_EVTHREAD_WIRE_FN(void)
{
    struct evthread_lock_callbacks lock_cbs = {
        EVTHREAD_LOCK_API_VERSION,
        EVTHREAD_LOCKTYPE_RECURSIVE,
        opal_evthread_lithe_lock_alloc,
        opal_evthread_lithe_lock_free,
        opal_evthread_lithe_lock,
        opal_evthread_lithe_unlock,
    };
    struct evthread_condition_callbacks cond_cbs = {
        EVTHREAD_CONDITION_API_VERSION,
        opal_evthread_lithe_cond_alloc,
        opal_evthread_lithe_cond_free,
        opal_evthread_lithe_cond_signal,
        opal_evthread_lithe_cond_wait,
    };

    if (lithe_evthread_libevent_configured) {
        return 0;
    }

    evthread_set_id_callback(opal_evthread_lithe_thread_id_cb);
    if (0 != evthread_set_lock_callbacks(&lock_cbs)) {
        return -1;
    }
    if (0 != evthread_set_condition_callbacks(&cond_cbs)) {
        return -1;
    }
    lithe_evthread_libevent_configured = 1;
    return 0;
}

#undef LITHE_EVTHREAD_WIRE_FN
#undef LITHE_EVTHREAD_EXPORT
