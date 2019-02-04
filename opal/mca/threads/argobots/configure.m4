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
# Copyright (c) 2015      Research Organization for Information Science
#                         and Technology (RIST). All rights reserved.
# $COPYRIGHT$
#
# Additional copyrights may follow
#
# $HEADER$
#
AC_DEFUN([MCA_opal_threads_argobots_PRIORITY], [30])

AC_DEFUN([MCA_opal_threads_argobots_COMPILE_MODE], [
    AC_MSG_CHECKING([for MCA component $2:$3 compile mode])
    $4="static"
    AC_MSG_RESULT([$$4])
])

AC_DEFUN([MCA_opal_threads_argobots_POST_CONFIG],[
    AS_IF([test "$1" = "1"], [threads_base_include="argobots/threads_argobots.h"])
])dnl

AC_DEFUN([MCA_opal_mutex_argobots_POST_CONFIG],[
    AS_IF([test "$1" = "1"], [mutex_base_include="argobots/mutex_unix.h"])
])dnl

# MCA_threads_argobots_CONFIG(action-if-can-compile,
#                        [action-if-cant-compile])
# ------------------------------------------------
AC_DEFUN([MCA_opal_threads_argobots_CONFIG],[
    AC_CONFIG_FILES([opal/mca/threads/argobots/Makefile])

    AS_IF([test "$with_threads" = "argobots"],
          [threads_argobots_happy="yes"
           threads_argobots_should_use=1],
          [threads_argobots_should_use=0
           AS_IF([test "$with_threads" = ""],
                 [threads_argobots_happy="yes"],
                 [threads_argobots_happy="no"])])

    AS_IF([test "$threads_argobots_happy" = "yes"],
          [AC_CHECK_HEADERS([abt.h])
           AC_CHECK_LIB(abt, ABT_key_create,
                         [threads_argobots_happy="yes"],
                         [threads_argobots_happy="no"])])

   AS_IF([test "$threads_argobots_happy" = "no" && \
          test "$threads_argobots_should_use" = "1"],
         [AC_MSG_ERROR([argobots threads requested but not available.  Aborting.])])

    AS_IF([test "$threads_argobots_happy" = "yes"],
          [$1],
          [$2])
])
