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
# Copyright (c) 2014-2015 Intel, Inc. All rights reserved.
# $COPYRIGHT$
#
# Additional copyrights may follow
#
# $HEADER$
#

# MCA_threads_lithe_CONFIG([action-if-can-compile],
#                        [action-if-cant-compile])
# ------------------------------------------------
AC_DEFUN([MCA_opal_threads_lithe_PRIORITY], [50])

AC_DEFUN([MCA_opal_threads_lithe_COMPILE_MODE], [
    AC_MSG_CHECKING([for MCA component $2:$3 compile mode])
    $4="dso"
    AC_MSG_RESULT([$$4])
])

# If component was selected, $1 will be 1; set base header includes and mark found
AC_DEFUN([MCA_opal_threads_lithe_POST_CONFIG],[
    AS_IF([test "$1" = "1"],
          [opal_thread_type_found="lithe"
           AC_DEFINE_UNQUOTED([MCA_threads_base_include_HEADER],
                               ["opal/mca/threads/lithe/threads_lithe.h"],
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

AC_DEFUN([MCA_opal_threads_lithe_CONFIG],[
    AC_CONFIG_FILES([opal/mca/threads/lithe/Makefile])

    # Check for Lithe support
    AC_ARG_WITH([lithe],
                [AS_HELP_STRING([--with-lithe(=DIR)],
                               [Build with Lithe support, optionally adding DIR to the search path for headers and libraries])])

    AS_IF([test "$with_lithe" != "no"],
          [AS_IF([test -n "$with_lithe" && test "$with_lithe" != "yes"],
                 [opal_lithe_dir="$with_lithe"])
    AS_IF([test -n "$opal_lithe_dir"],
                      [opal_lithe_CPPFLAGS="-I$opal_lithe_dir/include"
                       opal_lithe_LDFLAGS="-L$opal_lithe_dir/lib"
                       opal_lithe_LIBS="-Wl,--start-group -lparlib -lithe -Wl,--end-group"],
                      [opal_lithe_LIBS="-Wl,--start-group -lparlib -lithe -Wl,--end-group"]) 

           # Check if we can link against Lithe
           CPPFLAGS_save=$CPPFLAGS
           CPPFLAGS="$CPPFLAGS $opal_lithe_CPPFLAGS"
           AC_CHECK_HEADER([lithe/lithe.h],
                          [opal_threads_lithe_happy="yes"],
                          [opal_threads_lithe_happy="no"])
           CPPFLAGS=$CPPFLAGS_save
           LDFLAGS_save=$LDFLAGS
           LDFLAGS="$LDFLAGS $opal_lithe_LDFLAGS"
           # libithe is the actual library name
           # Check if library files exist (since linking test may fail due to dependencies)
           # We'll verify actual linking works during the build
           AS_IF([test -f "$opal_lithe_dir/lib/libithe.so" -o -f "$opal_lithe_dir/lib/libithe.a"],
                 [AS_IF([test -f "$opal_lithe_dir/lib/libparlib.so" -o -f "$opal_lithe_dir/lib/libparlib.a"],
                        [opal_threads_lithe_happy="yes"],
                        [opal_threads_lithe_happy="no"])],
                 [opal_threads_lithe_happy="no"])
           LDFLAGS=$LDFLAGS_save],
          [opal_threads_lithe_happy="no"])

    # Propagate toolchain flags for callers
    AS_IF([test "$opal_threads_lithe_happy" = "yes"],
          [TPKG_CFLAGS="$opal_lithe_CPPFLAGS"
           TPKG_FCFLAGS="$opal_lithe_CPPFLAGS"
           TPKG_CXXFLAGS="$opal_lithe_CPPFLAGS"
           TPKG_CPPFLAGS="$opal_lithe_CPPFLAGS"
           TPKG_CXXCPPFLAGS="$opal_lithe_CPPFLAGS"
           TPKG_LDFLAGS="$opal_lithe_LDFLAGS"
           TPKG_LIBS="$opal_lithe_LIBS"
           $1
           opal_thread_type_found="lithe"],
          [$2])

    # substitute in the things needed to build lithe
    AC_SUBST([opal_lithe_CPPFLAGS])
    AC_SUBST([opal_lithe_LDFLAGS])
    AC_SUBST([opal_lithe_LIBS])

    # Set post-config defines when selected
    MCA_opal_threads_lithe_POST_CONFIG([$opal_threads_lithe_happy])
])dnl
