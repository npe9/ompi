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
# Copyright (c) 2018      Intel, Inc.  All rights reserved.
# Copyright (c) 2024      NVIDIA Corporation.  All rights reserved.
# $COPYRIGHT$
#
# Additional copyrights may follow
#
# $HEADER$
#

AC_DEFUN([OPAL_CONFIG_LITHE],[

    AC_ARG_WITH([lithe],
                [AS_HELP_STRING([--with-lithe=DIR],
                                [Specify location of lithe installation.  Error if lithe support cannot be found.])])

    AC_ARG_WITH([lithe-libdir],
                [AS_HELP_STRING([--with-lithe-libdir=DIR],
                                [Search for lithe libraries in DIR])])

    opal_check_lithe_save_CPPFLAGS=$CPPFLAGS
    opal_check_lithe_save_LDFLAGS=$LDFLAGS
    opal_check_lithe_save_LIBS=$LIBS

    # First check for parlib dependency
    OAC_CHECK_PACKAGE([parlib],
                      [opal_parlib],
                      [parlib/parlib.h],
                      [parlib],
                      [uthread_lib_init],
                      [parlib_happy=yes],
                      [parlib_happy=no])
    
    # Then check for lithe with parlib dependency
    AS_IF([test $parlib_happy = yes],
          [opal_check_lithe_save_CPPFLAGS=$CPPFLAGS
           CPPFLAGS="$CPPFLAGS $opal_parlib_CPPFLAGS"
           OAC_CHECK_PACKAGE([lithe],
                             [opal_lithe],
                             [lithe/lithe.h],
                             [lithe],
                             [lithe_lib_init],
                             [opal_lithe_happy=yes],
                             [opal_lithe_happy=no])
           CPPFLAGS=$opal_check_lithe_save_CPPFLAGS],
          [opal_lithe_happy=no])

    AS_IF([test $opal_lithe_happy = yes],
          [TPKG_CFLAGS="$opal_parlib_CPPFLAGS $opal_lithe_CPPFLAGS"
           TPKG_FCFLAGS="$opal_parlib_CPPFLAGS $opal_lithe_CPPFLAGS"
           TPKG_CXXFLAGS="$opal_parlib_CPPFLAGS $opal_lithe_CPPFLAGS"
           TPKG_CPPFLAGS="$opal_parlib_CPPFLAGS $opal_lithe_CPPFLAGS"
           TPKG_CXXCPPFLAGS="$opal_parlib_CPPFLAGS $opal_lithe_CPPFLAGS"
           TPKG_LDFLAGS="$opal_parlib_LDFLAGS $opal_lithe_LDFLAGS"
           TPKG_LIBS="$opal_parlib_LIBS $opal_lithe_LIBS"])

    AC_SUBST([opal_parlib_CPPFLAGS])
    AC_SUBST([opal_parlib_LDFLAGS])
    AC_SUBST([opal_parlib_LIBS])
    AC_SUBST([opal_lithe_CPPFLAGS])
    AC_SUBST([opal_lithe_LDFLAGS])
    AC_SUBST([opal_lithe_LIBS])

    CPPFLAGS="${opal_check_lithe_save_CPPFLAGS} ${opal_lithe_CPPFLAGS}"
    LDFLAGS=$opal_check_lithe_save_LDFLAGS
    LIBS=$opal_check_lithe_save_LIBS

    AS_IF([test "$opal_lithe_happy" = "yes"],
          [$1],
          [$2])
])dnl

AC_DEFUN([MCA_opal_threads_lithe_PRIORITY], [30])

AC_DEFUN([MCA_opal_threads_lithe_COMPILE_MODE], [
    AC_MSG_CHECKING([for MCA component $2:$3 compile mode])
    $4="static"
    AC_MSG_RESULT([$$4])
])

# If component was selected, $1 will be 1 and we should set the base header
AC_DEFUN([MCA_opal_threads_lithe_POST_CONFIG],[
    AS_IF([test "$1" = "1"], 
          [opal_thread_type_found="lithe"
           AC_DEFINE_UNQUOTED([MCA_threads_base_include_HEADER],
                              ["opal/mca/threads/lithe/threads_lithe_threads.h"],
                              [Header to include for threads implementation])
           AC_DEFINE_UNQUOTED([MCA_threads_mutex_base_include_HEADER],
                              ["opal/mca/threads/lithe/threads_lithe_mutex.h"],
                              [Header to include for mutex implementation])
           AC_DEFINE_UNQUOTED([MCA_threads_tsd_base_include_HEADER],
                              ["opal/mca/threads/lithe/threads_lithe_tsd.h"],
                              [Header to include for tsd implementation])
           THREAD_CFLAGS="$TPKG_CFLAGS"
           THREAD_FCFLAGS="$TPKG_FCFLAGS"
           THREAD_CXXFLAGS="$TPKG_CXXFLAGS"
           THREAD_CPPFLAGS="$TPKG_CPPFLAGS"
           THREAD_CXXCPPFLAGS="$TPKG_CXXCPPFLAGS"
           THREAD_LDFLAGS="$TPKG_LDFLAGS"
           THREAD_LIBS="$TPKG_LIBS"
           LIBS="$LIBS $THREAD_LIBS"
           LDFLAGS="$LDFLAGS $THREAD_LDFLAGS"
         ])
])dnl

# MCA_threads_lithe_CONFIG(action-if-can-compile,
#                        [action-if-cant-compile])
# ------------------------------------------------
AC_DEFUN([MCA_opal_threads_lithe_CONFIG],[
    AC_CONFIG_FILES([opal/mca/threads/lithe/Makefile])

    AS_IF([test "$with_threads" = "lithe"],
          [OPAL_CONFIG_LITHE([lithe_works=1],[lithe_works=0])],
          [lithe_works=0])

    AS_IF([test "$lithe_works" = "1"],
          [$1
           opal_thread_type_found="lithe"],
          [$2])
]) 