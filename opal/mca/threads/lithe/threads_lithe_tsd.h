/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2024 Lithe Integration Team. All rights reserved.
 *
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef OPAL_MCA_THREADS_LITHE_THREADS_LITHE_TSD_H
#define OPAL_MCA_THREADS_LITHE_THREADS_LITHE_TSD_H

#include "opal/constants.h"

/* Lithe does not support thread-specific data (TSD) natively. */
typedef int opal_tsd_key_t;

static inline int opal_tsd_key_delete(opal_tsd_key_t key)
{
    return OPAL_ERR_NOT_SUPPORTED;
}

static inline int opal_tsd_set(opal_tsd_key_t key, void *value)
{
    return OPAL_ERR_NOT_SUPPORTED;
}

static inline int opal_tsd_get(opal_tsd_key_t key, void **valuep)
{
    return OPAL_ERR_NOT_SUPPORTED;
}

#endif /* OPAL_MCA_THREADS_LITHE_THREADS_LITHE_TSD_H */ 