dnl
dnl Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
dnl                         University Research and Technology
dnl                         Corporation.  All rights reserved.
dnl Copyright (c) 2004-2005 The University of Tennessee and The University
dnl                         of Tennessee Research Foundation.  All rights
dnl                         reserved.
dnl Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
dnl                         University of Stuttgart.  All rights reserved.
dnl Copyright (c) 2004-2005 The Regents of the University of California.
dnl                         All rights reserved.
dnl Copyright (c) 2012      Cisco Systems, Inc.  All rights reserved.
dnl Copyright (c) 2014      Intel, Inc. All rights reserved.
dnl Copyright (c) 2014-2016 Research Organization for Information Science
dnl                         and Technology (RIST). All rights reserved.
dnl $COPYRIGHT$
dnl
dnl Additional copyrights may follow
dnl
dnl $HEADER$
dnl
dnl OPAL_CONFIG_ARGOBOTS_THREADS()
dnl
dnl Configure Argobots threads, setting the following variables (but
dnl  not calling AC_SUBST on them).

AC_DEFUN([OPAL_CONFIG_ARGOBOTS_THREADS],[
    AC_CHECK_HEADERS([abt.h],
                     [AC_CHECK_LIB([abt],[ABT_init],
                                    [threads_argobots_happy="yes"],
                                    [threads_argobots_happy="no"])],
                     [threads_argobots_happy="no"])

    AS_IF([test "$threads_argobots_happy" = "yes"],
          [$1],
          [$2])
])dnl
