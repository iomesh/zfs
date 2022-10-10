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

#ifndef _LIBSPL_VFS_H
#define	_LIBSPL_VFS_H

#include_next <sys/vfs.h>

#define	MAXFIDSZ	64

typedef struct spl_fid {
	union {
		long fid_pad;
		struct {
			ushort_t len;		/* length of data in bytes */
			char data[MAXFIDSZ];	/* data (variable len) */
		} _fid;
	} un;
} fid_t;

#define	fid_len		un._fid.len
#define	fid_data	un._fid.data

#endif /* LIBSPL_VFS_H */
