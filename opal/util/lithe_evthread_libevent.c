/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*-
 * See lithe_evthread_libevent_impl.inc.c
 */

#include "opal_config.h"

#if HAVE_LITHE

#    define LITHE_EVTHREAD_WIRE_FN         opal_lithe_evthread_wire_libevent
#    define LITHE_EVTHREAD_EXPORT          OPAL_DECLSPEC
#    include "lithe_evthread_libevent_impl.inc.c"

#endif /* HAVE_LITHE */
