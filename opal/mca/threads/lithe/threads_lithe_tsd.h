/*
 * OPAL Lithe TSD — per-context thread-specific data without pthreads.
 *
 * Key type is a plain integer.  Values are stored per-Lithe-context
 * (uthread) in a global hash table managed by functions in
 * threads_lithe_module.c.  Before the scheduler is entered
 * (lithe_context_self() == NULL) a dedicated "main" slot is used so
 * that early OPAL init can create and use TSD keys.
 */

#ifndef OPAL_MCA_THREADS_LITHE_THREADS_LITHE_TSD_H
#define OPAL_MCA_THREADS_LITHE_THREADS_LITHE_TSD_H

#include "opal_config.h"
#include "opal/constants.h"
#include <stdint.h>

BEGIN_C_DECLS

typedef uintptr_t opal_tsd_key_t;

extern int opal_lithe_tsd_key_delete(opal_tsd_key_t key);
extern int opal_lithe_tsd_set(opal_tsd_key_t key, void *value);
extern int opal_lithe_tsd_get(opal_tsd_key_t key, void **valuep);

static inline int opal_tsd_key_delete(opal_tsd_key_t key)
{
    return opal_lithe_tsd_key_delete(key);
}

static inline int opal_tsd_set(opal_tsd_key_t key, void *value)
{
    return opal_lithe_tsd_set(key, value);
}

static inline int opal_tsd_get(opal_tsd_key_t key, void **valuep)
{
    return opal_lithe_tsd_get(key, valuep);
}

END_C_DECLS

#endif /* OPAL_MCA_THREADS_LITHE_THREADS_LITHE_TSD_H */
