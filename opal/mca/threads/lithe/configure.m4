# -*- shell-script -*-
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

# MCA_threads_lithe_PRIORITY
# --------------------------
AC_DEFUN([MCA_opal_threads_lithe_PRIORITY], [30])

# MCA_threads_lithe_CONFIG([action-if-found], [action-if-not-found])
# -----------------------------------------------------------
AC_DEFUN([MCA_opal_threads_lithe_CONFIG],[
    AC_CONFIG_FILES([opal/mca/threads/lithe/Makefile])

    OPAL_VAR_SCOPE_PUSH([lithe_happy])

    # Check for lithe
    OAC_CHECK_PACKAGE([lithe],
                       [lithe],
                     [lithe/lithe.h],
                       [lithe],
                       [lithe_lib_init],
                       [lithe_happy=yes],
                       [lithe_happy=no])

    AS_IF([test "$lithe_happy" = "yes"],
          [$1],
          [$2])

    OPAL_VAR_SCOPE_POP
])dnl 