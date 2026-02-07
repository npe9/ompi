#
# Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
#                         University Research and Technology
#                         Corporation.  All rights reserved.
# Copyright (c) 2004-2005 The University of Tennessee and The University
#                         of Tennessee Research Foundation.  All rights
#                         reserved.
# Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
#                         University of Stuttgart.  All rights reserved.
# Copyright (c) 2004-2005 The Regents of the University of California.
#                         All rights reserved.
# Copyright (c) 2010      Cisco Systems, Inc.  All rights reserved.
# Copyright (c) 2014-2015 Intel, Inc. All rights reserved.
# $COPYRIGHT$
#
# Additional copyrights may follow
#
# $HEADER$
#

# Need PMIx mutex layout for offsetof(pmix_mutex_t, m_lock_opal). Use openpmix source.
AM_CPPFLAGS = $(opal_lithe_CPPFLAGS) -I$(top_srcdir)/3rd-party/openpmix

sources = \
    threads_lithe.h \
    threads_lithe_component.c \
    threads_lithe_condition.c \
    threads_lithe_module.c \
    threads_lithe_internal.h

lib_sources = $(sources)


if MCA_BUILD_opal_threads_lithe_DSO
component_noinst =
component_install = mca_threads_lithe.la
else
component_noinst = libmca_threads_lithe.la
component_install =
endif

mcacomponentdir = $(opallibdir)
mcacomponent_LTLIBRARIES = $(component_install)
mca_threads_lithe_la_SOURCES = $(sources)
mca_threads_lithe_la_LDFLAGS = -module -avoid-version
mca_threads_lithe_la_LIBADD = $(top_builddir)/opal/lib@OPAL_LIB_NAME@.la \
        $(opal_lithe_LIBS)

noinst_LTLIBRARIES = $(component_noinst)
libmca_threads_lithe_la_SOURCES = $(sources)
libmca_threads_lithe_la_LDFLAGS = -module -avoid-version $(opal_lithe_LDFLAGS)
libmca_threads_lithe_la_LIBADD = $(opal_lithe_LIBS)
