/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2006-2015 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2007-2013 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2014      Intel, Inc. All rights reserved.
 * Copyright (c) 2018      Amazon.com, Inc. or its affiliates.  All Rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "opal/constants.h"
#include "opal/util/argv.h"
#include "opal/util/opal_environ.h"
#include "opal/util/printf.h"

#define OPAL_DEFAULT_TMPDIR "/tmp"

/*
 * Merge two environ-like char arrays, ensuring that there are no
 * duplicate entries
 */
char **opal_environ_merge(char **minor, char **major)
{
    int i;
    char **ret = NULL;
    char *name, *value;

    /* Check for bozo cases */

    if (NULL == major) {
        if (NULL == minor) {
            return NULL;
        } else {
            return opal_argv_copy(minor);
        }
    }

    /* First, copy major */

    ret = opal_argv_copy(major);

    /* Do we have something in minor? */

    if (NULL == minor) {
        return ret;
    }

    /* Now go through minor and call opal_setenv(), but with overwrite
       as false */

    for (i = 0; NULL != minor[i]; ++i) {
        value = strchr(minor[i], '=');
        if (NULL == value) {
            opal_setenv(minor[i], NULL, false, &ret);
        } else {

            /* strdup minor[i] in case it's a constat string */

            name = strdup(minor[i]);
            value = name + (value - minor[i]);
            *value = '\0';
            opal_setenv(name, value + 1, false, &ret);
            free(name);
        }
    }

    /* All done */

    return ret;
}

/*
 * Portable version of setenv(), allowing editing of any environ-like
 * array
 */
/* See opal_environ.h. The owned array has spare capacity: an add writes the new
 * terminator first, then publishes the entry (release), so a reader walking the array
 * sees the old end or the new entry, never a hole. A full array, or one glibc has
 * replaced (a later setenv(3) reallocs its own last array, not ours), is copied into a
 * bigger owned array that is published with one pointer store; the old one is leaked,
 * as are replaced strings (glibc leaks those too). Writers serialise on a spinlock. */
static char **lithe_env_owned = NULL;
static size_t lithe_env_cap = 0;
static volatile int lithe_env_lock = 0;
int opal_lithe_environ_set(const char *name, const char *value, int overwrite)
{
    static int on = -1;
    size_t nl, n, i;
    char *s, **ep;

    if (on < 0) {
        const char *e = getenv("OPAL_LITHE_ENV_SAFE");
        on = (NULL != e && '1' == e[0]) ? 1 : 0;
    }
    if (!on) {
        return setenv(name, value, overwrite);
    }
    if (NULL == name || '\0' == name[0] || NULL != strchr(name, '=')) {
        return -1;
    }
    nl = strlen(name);
    while (__atomic_exchange_n(&lithe_env_lock, 1, __ATOMIC_ACQUIRE)) {
        __asm__ __volatile__("pause" ::: "memory");
    }
    ep = environ;
    for (n = 0; NULL != ep && NULL != ep[n]; ++n) {
        if (0 == strncmp(ep[n], name, nl) && '=' == ep[n][nl]) {
            break;
        }
    }
    if (NULL != ep && NULL != ep[n] && !overwrite) {
        __atomic_store_n(&lithe_env_lock, 0, __ATOMIC_RELEASE);
        return 0;
    }
    s = malloc(nl + strlen(value) + 2);
    if (NULL == s) {
        __atomic_store_n(&lithe_env_lock, 0, __ATOMIC_RELEASE);
        return -1;
    }
    sprintf(s, "%s=%s", name, value);
    if (NULL != ep && NULL != ep[n]) {
        __atomic_store_n(&ep[n], s, __ATOMIC_RELEASE); /* old string leaked */
    } else {
        /* n = current length */
        if (ep != lithe_env_owned || n + 2 > lithe_env_cap) {
            size_t cap = 2 * (n + 2) + 64;
            char **na = malloc(cap * sizeof(char *));
            if (NULL == na) {
                free(s);
                __atomic_store_n(&lithe_env_lock, 0, __ATOMIC_RELEASE);
                return -1;
            }
            for (i = 0; i < n; ++i) {
                na[i] = ep[i];
            }
            na[n] = s;
            na[n + 1] = NULL;
            lithe_env_owned = na;
            lithe_env_cap = cap;
            __atomic_store_n(&environ, na, __ATOMIC_RELEASE); /* old array leaked */
        } else {
            ep[n + 1] = NULL;
            __atomic_store_n(&ep[n], s, __ATOMIC_RELEASE);
        }
    }
    __atomic_store_n(&lithe_env_lock, 0, __ATOMIC_RELEASE);
    return 0;
}

int opal_setenv(const char *name, const char *value, bool overwrite, char ***env)
{
    int i;
    char *newvalue, *compare;
    size_t len;

    /* Make the new value */

    if (NULL == value) {
        value = "";
        opal_asprintf(&newvalue, "%s=", name);
    } else {
        opal_asprintf(&newvalue, "%s=%s", name, value);
    }
    if (NULL == newvalue) {
        return OPAL_ERR_OUT_OF_RESOURCE;
    }

    /* Check the bozo case */

    if (NULL == env) {
        return OPAL_ERR_BAD_PARAM;
    } else if (NULL == *env) {
        i = 0;
        opal_argv_append(&i, env, newvalue);
        free(newvalue);
        return OPAL_SUCCESS;
    }

    /* If this is the "environ" array, use putenv */
    if (*env == environ) {
        /* THIS IS POTENTIALLY A MEMORY LEAK!  But I am doing it
           so that we don't violate the law of least
           astonishment for OPAL developers (i.e., those that don't
           check the return code of opal_setenv() and notice that we
           returned an error if you passed in the real environ) */
#if defined(HAVE_SETENV)
        opal_lithe_environ_set(name, value, overwrite);
        /* setenv copies the value, so we can free it here */
        free(newvalue);
#else
        len = strlen(name);
        for (i = 0; (*env)[i] != NULL; ++i) {
            if (0 == strncmp((*env)[i], name, len)) {
                /* if we find the value in the environ, then
                 * we need to check the overwrite flag to determine
                 * the correct response */
                if (overwrite) {
                    /* since it was okay to overwrite, do so */
                    putenv(newvalue);
                    /* putenv does NOT copy the value, so we
                     * cannot free it here */
                    return OPAL_SUCCESS;
                }
                /* since overwrite was not allowed, we return
                 * an error as we cannot perform the requested action */
                free(newvalue);
                return OPAL_EXISTS;
            }
        }
        /* since the value wasn't found, we can add it */
        putenv(newvalue);
        /* putenv does NOT copy the value, so we
         * cannot free it here */
#endif
        return OPAL_SUCCESS;
    }

    /* Make something easy to compare to */

    opal_asprintf(&compare, "%s=", name);
    if (NULL == compare) {
        free(newvalue);
        return OPAL_ERR_OUT_OF_RESOURCE;
    }
    len = strlen(compare);

    /* Look for a duplicate that's already set in the env */

    for (i = 0; (*env)[i] != NULL; ++i) {
        if (0 == strncmp((*env)[i], compare, len)) {
            if (overwrite) {
                free((*env)[i]);
                (*env)[i] = newvalue;
                free(compare);
                return OPAL_SUCCESS;
            } else {
                free(compare);
                free(newvalue);
                return OPAL_EXISTS;
            }
        }
    }

    /* If we found no match, append this value */

    i = opal_argv_count(*env);
    opal_argv_append(&i, env, newvalue);

    /* All done */

    free(compare);
    free(newvalue);
    return OPAL_SUCCESS;
}

/*
 * Portable version of unsetenv(), allowing editing of any
 * environ-like array
 */
int opal_unsetenv(const char *name, char ***env)
{
    int i;
    char *compare;
    size_t len;
    bool found;

    /* Check for bozo case */

    if (NULL == *env) {
        return OPAL_SUCCESS;
    }

    /* Make something easy to compare to */

    opal_asprintf(&compare, "%s=", name);
    if (NULL == compare) {
        return OPAL_ERR_OUT_OF_RESOURCE;
    }
    len = strlen(compare);

    /* Look for a duplicate that's already set in the env.  If we find
       it, free it, and then start shifting all elements down one in
       the array. */

    found = false;
    for (i = 0; (*env)[i] != NULL; ++i) {
        if (0 != strncmp((*env)[i], compare, len)) {
            continue;
        }
        if (environ != *env) {
            free((*env)[i]);
        }
        for (; (*env)[i] != NULL; ++i) {
            (*env)[i] = (*env)[i + 1];
        }
        found = true;
        break;
    }
    free(compare);

    /* All done */

    return (found) ? OPAL_SUCCESS : OPAL_ERR_NOT_FOUND;
}

const char *opal_tmp_directory(void)
{
    const char *str;

    if (NULL == (str = getenv("TMPDIR"))) {
        if (NULL == (str = getenv("TEMP"))) {
            if (NULL == (str = getenv("TMP"))) {
                str = OPAL_DEFAULT_TMPDIR;
            }
        }
    }
    return str;
}

const char *opal_home_directory(void)
{
    char *home = getenv("HOME");

    return home;
}
