#
# Copyright (c) 2004-2006 The Regents of the University of California.
#                         All rights reserved.
# Copyright (c) 2009      High Performance Computing Center Stuttgart,
#                         University of Stuttgart.  All rights reserved.
# Copyright (c) 2010      Cisco Systems, Inc.  All rights reserved.
# $COPYRIGHT$
#
# Additional copyrights may follow
#
# $HEADER$
#

# Make the output library in this directory, and name it either
# mca_<type>_<name>.la (for DSO builds) or libmca_<type>_<name>.la
# (for static builds).


if MCA_BUILD_ompi_rte_pisces_DSO
component_noinst =
component_install = mca_rte_pisces.la
else
component_noinst = libmca_rte_pisces.la
component_install =
endif

local_sources = \
			rte_pisces.c \
			rte_pisces_component.c \
			rte_pisces_module.c

mcacomponentdir = $(ompilibdir)
mcacomponent_LTLIBRARIES = $(component_install)
mca_rte_pisces_la_SOURCES = $(local_sources)
mca_rte_pisces_la_LIBADD = $(rte_pisces_LIBS)
mca_rte_pisces_la_LDFLAGS = -module -avoid-version $(rte_pisces_LDFLAGS)

noinst_LTLIBRARIES = $(component_noinst)
libmca_rte_pisces_la_SOURCES = $(local_sources)
libmca_rte_pisces_la_LIBADD = $(rte_pisces_LIBS)
libmca_rte_pisces_la_LDFLAGS = -module -avoid-version $(rte_pisces_LDFLAGS)

