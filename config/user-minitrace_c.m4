dnl -------------------------------------------------------- -*- autoconf -*-
dnl SPDX-License-Identifier: Apache-2.0

dnl
dnl minitrace_c.m4: ZFS's minitrace_c autoconf macros
dnl

AC_DEFUN([ZFS_AC_CONFIG_USER_MINITRACE_C], [
AC_MSG_CHECKING([for telemetry])
ZFS_CHECK_MINITRACE_C
if test "$has_minitrace_c" = "1"; then
    AC_DEFINE([ENABLE_MINITRACE_C], [1], [compile and link with minitrace_c])
fi
])

AC_DEFUN([ZFS_CHECK_MINITRACE_C], [
enable_minitrace_c=no
AC_ARG_WITH([minitrace_c], [AS_HELP_STRING([--with-minitrace_c=DIR], [use a specific minitrace_c library])],
[
  if test "$withval" != "no"; then
    enable_minitrace_c=yes
    minitrace_c_base_dir="$withval"
    case "$withval" in
      yes)
        minitrace_c_base_dir="/usr"
        AC_MSG_CHECKING(checking for minitrace_c includes standard directories)
	;;
      *":"*)
        minitrace_c_include="`echo $withval |sed -e 's/:.*$//'`"
        minitrace_c_ldflags="`echo $withval |sed -e 's/^.*://'`"
        AC_MSG_CHECKING(checking for minitrace_c includes in $minitrace_c_include libs in $minitrace_c_ldflags)
        ;;
      *)
        minitrace_c_include="$withval/include"
        minitrace_c_ldflags="$withval/lib"
        AC_MSG_CHECKING(checking for minitrace_c includes in $withval)
        ;;
    esac
  fi
])
has_minitrace_c=0
if test "$enable_minitrace_c" != "no"; then
  minitrace_c_have_headers=0
  minitrace_c_have_libs=0
  if test "$minitrace_c_base_dir" != "/usr"; then
    CFLAGS="${CFLAGS} -I${minitrace_c_include}"
    LDFLAGS="${LDFLAGS} -L${minitrace_c_ldflags}"
    LIBTOOL_LINK_FLAGS="${LIBTOOL_LINK_FLAGS} -R${minitrace_c_ldflags}"
  fi
  func="${minitrace_c_prefix}mtr_c_ver"
  AC_CHECK_LIB(minitrace_c, ${func}, [minitrace_c_have_libs=1])
  if test "$minitrace_c_have_libs" != "0"; then
    AC_CHECK_HEADERS([minitrace_c/minitrace_c.h], [minitrace_c_have_headers=1])
  fi
  if test "$minitrace_c_have_headers" != "0"; then
    has_minitrace_c=1
    LIBS="${LIBS} -lminitrace_c -lstdc++"
    AC_DEFINE(has_minitrace_c, [1], [Link/compile against minitrace_c])
  else
    AC_MSG_ERROR([Couldn't find a minitrace_c installation])
  fi
fi
AC_SUBST(has_minitrace_c)
])
