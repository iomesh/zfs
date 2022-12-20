/*
 *  Copyright (C) 2007-2010 Lawrence Livermore National Security, LLC.
 *  Copyright (C) 2007 The Regents of the University of California.
 *  Produced at Lawrence Livermore National Laboratory (cf, DISCLAIMER).
 *  Written by Brian Behlendorf <behlendorf1@llnl.gov>.
 *  UCRL-CODE-235197
 *
 *  This file is part of the SPL, Solaris Porting Layer.
 *
 *  The SPL is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU General Public License as published by the
 *  Free Software Foundation; either version 2 of the License, or (at your
 *  option) any later version.
 *
 *  The SPL is distributed in the hope that it will be useful, but WITHOUT
 *  ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 *  FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU General Public License along
 *  with the SPL.  If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef _LIBSPL_VNODE_H
#define	_LIBSPL_VNODE_H

#include <sys/kmem.h>
#include <sys/types.h>
#include <sys/time.h>

/*
 * Prior to linux-2.6.33 only O_DSYNC semantics were implemented and
 * they used the O_SYNC flag.  As of linux-2.6.33 the this behavior
 * was properly split in to O_SYNC and O_DSYNC respectively.
 */
#ifndef O_DSYNC
#define	O_DSYNC		O_SYNC
#endif

#define	F_FREESP	11 	/* Free file space */


#if defined(SEEK_HOLE) && defined(SEEK_DATA)
#define	F_SEEK_DATA	SEEK_DATA
#define	F_SEEK_HOLE	SEEK_HOLE
#endif

/*
 * The vnode AT_ flags are mapped to the Linux ATTR_* flags.
 * This allows them to be used safely with an iattr structure.
 * The AT_XVATTR flag has been added and mapped to the upper
 * bit range to avoid conflicting with the standard Linux set.
 */
#undef AT_UID
#undef AT_GID

#define	ATTR_MODE   (1 << 0)
#define	ATTR_UID    (1 << 1)
#define	ATTR_GID    (1 << 2)
#define	ATTR_SIZE   (1 << 3)
#define	ATTR_ATIME  (1 << 4)
#define	ATTR_MTIME  (1 << 5)
#define	ATTR_CTIME  (1 << 6)
#define	ATTR_ATIME_SET  (1 << 7)
#define	ATTR_MTIME_SET  (1 << 8)
#define	ATTR_FORCE  (1 << 9) /* Not a change, but a change it */
#define	ATTR_ATTR_FLAG  (1 << 10)
#define	ATTR_KILL_SUID  (1 << 11)
#define	ATTR_KILL_SGID  (1 << 12)
#define	ATTR_FILE   (1 << 13)
#define	ATTR_KILL_PRIV  (1 << 14)
#define	ATTR_OPEN   (1 << 15) /* Truncating from open(O_TRUNC) */
#define	ATTR_TIMES_SET  (1 << 16)
#define	ATTR_TOUCH  (1 << 17)

#define	AT_MODE		ATTR_MODE
#define	AT_UID		ATTR_UID
#define	AT_GID		ATTR_GID
#define	AT_SIZE		ATTR_SIZE
#define	AT_ATIME	ATTR_ATIME
#define	AT_MTIME	ATTR_MTIME
#define	AT_CTIME	ATTR_CTIME

#define	ATTR_XVATTR	(1U << 31)
#define	AT_XVATTR	ATTR_XVATTR

#define	ATTR_IATTR_MASK	(ATTR_MODE | ATTR_UID | ATTR_GID | ATTR_SIZE | \
			ATTR_ATIME | ATTR_MTIME | ATTR_CTIME | ATTR_FILE)

#define	CRCREAT		0x01
#define	RMFILE		0x02

#define	B_INVAL		0x01
#define	B_TRUNC		0x02

#define	LOOKUP_DIR		0x01
#define	LOOKUP_XATTR		0x02
#define	CREATE_XATTR_DIR	0x04
#define	ATTR_NOACLCHECK		0x20

typedef struct vattr {
	uint32_t	va_mask;	/* attribute bit-mask */
	ushort_t	va_mode;	/* acc mode */
	uid_t		va_uid;		/* owner uid */
	gid_t		va_gid;		/* owner gid */
	long		va_fsid;	/* fs id */
	long		va_nodeid;	/* node # */
	uint32_t	va_nlink;	/* # links */
	uint64_t	va_size;	/* file size */
	inode_timespec_t va_atime;	/* last acc */
	inode_timespec_t va_mtime;	/* last mod */
	inode_timespec_t va_ctime;	/* last chg */
	dev_t		va_rdev;	/* dev */
	uint64_t	va_nblocks;	/* space used */
	uint32_t	va_blksize;	/* block size */
	struct dentry	*va_dentry;	/* dentry to wire */
} vattr_t;
#endif /* _LIBSPL_VNODE_H */
