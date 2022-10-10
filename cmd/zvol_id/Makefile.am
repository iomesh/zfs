include $(top_srcdir)/config/Rules.am

if BUILD_LINUX
DEFAULT_INCLUDES += -I$(top_srcdir)/include/os/linux/zfs
endif

# Disable GCC stack protection for zvol_id.  This is a kludge and should be
# removed once https://github.com/openzfs/zfs/issues/569 is resolved.
AM_CFLAGS += -fno-stack-protector

udev_PROGRAMS = zvol_id

zvol_id_SOURCES = \
	zvol_id_main.c

include $(top_srcdir)/config/CppCheck.am
