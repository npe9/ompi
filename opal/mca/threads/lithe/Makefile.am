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
# Copyright (c) 2010-2011 IBM Corporation.  All rights reserved.
# Copyright (c) 2011-2013 Los Alamos National Security, LLC.
#                         All rights reserved.
# Copyright (c) 2014-2015 Intel, Inc. All rights reserved.
# Copyright (c) 2014-2015 Research Organization for Information Science
#                         and Technology (RIST). All rights reserved.
# Copyright (c) 2016      IBM Corporation.  All rights reserved.
# Copyright (c) 2016-2017 Intel, Inc. All rights reserved.
# Copyright (c) 2017      Research Organization for Information Science
#                         and Technology (RIST). All rights reserved.
# Copyright (c) 2017-2018 IBM Corporation.  All rights reserved.
# Copyright (c) 2018      Intel, Inc. All rights reserved.
# Copyright (c) 2024      NVIDIA Corporation.  All rights reserved.
# $COPYRIGHT$
#
# Additional copyrights may follow
#
# $HEADER$
#

# Make the output library in this directory, and name it either
# mca_<type>_<name>.la (for DSO builds) or libmca_<type>_<name>.la
# (for static builds).

if MCA_BUILD_opal_threads_lithe_DSO
component_noinst =
component_install = mca_threads_lithe.la
else
component_noinst = libmca_threads_lithe.la
component_install =
endif

AM_CPPFLAGS = $(threads_lithe_CPPFLAGS)
AM_CPPFLAGS += -I/tmp/lithe-install/include -I/tmp/parlib-install/include

mcacomponentdir = $(opallibdir)
mcacomponent_LTLIBRARIES = $(component_install)
mca_threads_lithe_la_SOURCES = \
    lithe_threads.h \
    lithe_threads.c
mca_threads_lithe_la_LDFLAGS = -module -avoid-version
mca_threads_lithe_la_LIBADD = $(threads_lithe_LIBS)

noinst_LTLIBRARIES = $(component_noinst)
libmca_threads_lithe_la_SOURCES = \
    $(mca_threads_lithe_la_SOURCES)
libmca_threads_lithe_la_LDFLAGS = -module -avoid-version
libmca_threads_lithe_la_LIBADD = $(threads_lithe_LIBS) 