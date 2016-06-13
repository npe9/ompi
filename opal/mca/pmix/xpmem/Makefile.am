#
# Copyright (c) 2016      Intel, Inc.  All rights reserved.
# $COPYRIGHT$
#
# Additional copyrights may follow
#
# $HEADER$
#

sources = \
        pmix_xpmem.h \
        pmix_xpmem_component.c \
				pmix_xpmem_hash.c \
        pmix_xpmem.c

# Make the output library in this directory, and name it either
# mca_<type>_<name>.la (for DSO builds) or libmca_<type>_<name>.la
# (for static builds).

if MCA_BUILD_opal_pmix_xpmem_DSO
component_noinst =
component_install = mca_pmix_xpmem.la
else
component_noinst = libmca_pmix_xpmem.la
component_install =
endif

mcacomponentdir = $(opallibdir)
mcacomponent_LTLIBRARIES = $(component_install)
mca_pmix_xpmem_la_SOURCES = $(sources)
mca_pmix_xpmem_la_LIBADD = -lhobbes
mca_pmix_xpmem_la_LDFLAGS = -module -avoid-version -L/home/npe/src/nvl.me/src/guests/simple_busybox/src/xpmem/hobbes/libhobbes

noinst_LTLIBRARIES = $(component_noinst)
libmca_pmix_xpmem_la_SOURCES =$(sources)
mca_pmix_xpmem_la_LIBADD = -lhobbes
libmca_pmix_xpmem_la_LDFLAGS = -module -avoid-version -L/home/npe/src/nvl.me/src/guests/simple_busybox/src/xpmem/hobbes/libhobbes
