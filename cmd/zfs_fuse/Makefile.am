include $(top_srcdir)/config/Rules.am

# FIXME(hping): use config to find fuse3 path
DEFAULT_INCLUDES += -I/usr/include/fuse3

# Unconditionally enable ASSERTs
AM_CPPFLAGS += -DDEBUG -UNDEBUG -DZFS_DEBUG $(FUSE3)

bin_PROGRAMS = zfs_fuse

zfs_fuse_SOURCES = zfs_fuse.c

zfs_fuse_LDADD = \
	$(abs_top_builddir)/lib/libuzfs/libuzfs.la \
	$(abs_top_builddir)/lib/libzpool/libzpool.la

zfs_fuse_LDADD += -lfuse3 -lpthread

include $(top_srcdir)/config/CppCheck.am
