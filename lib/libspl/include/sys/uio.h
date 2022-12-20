/*
 * CDDL HEADER START
 *
 * The contents of this file are subject to the terms of the
 * Common Development and Distribution License, Version 1.0 only
 * (the "License").  You may not use this file except in compliance
 * with the License.
 *
 * You can obtain a copy of the license at usr/src/OPENSOLARIS.LICENSE
 * or http://www.opensolaris.org/os/licensing.
 * See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * When distributing Covered Code, include this CDDL HEADER in each
 * file and include the License file at usr/src/OPENSOLARIS.LICENSE.
 * If applicable, add the following below this CDDL HEADER, with the
 * fields enclosed by brackets "[]" replaced with your own identifying
 * information: Portions Copyright [yyyy] [name of copyright owner]
 *
 * CDDL HEADER END
 */
/*
 * Copyright 2005 Sun Microsystems, Inc.  All rights reserved.
 * Use is subject to license terms.
 */

/*	Copyright (c) 1984, 1986, 1987, 1988, 1989 AT&T	*/
/*	  All Rights Reserved  	*/

/*
 * University Copyright- Copyright (c) 1982, 1986, 1988
 * The Regents of the University of California
 * All Rights Reserved
 *
 * University Acknowledgment- Portions of this document are derived from
 * software developed by the University of California, Berkeley, and its
 * contributors.
 */

#ifndef	_LIBSPL_SYS_UIO_H
#define	_LIBSPL_SYS_UIO_H

#include <sys/types.h>
#include <sys/debug.h>
#include <string.h>
#include_next <sys/uio.h>

#ifdef __APPLE__
#include <sys/_types/_iovec_t.h>
#endif

#include <stdint.h>
typedef struct iovec iovec_t;

#if defined(__linux__) || defined(__APPLE__)
typedef enum zfs_uio_rw {
	UIO_READ =	0,
	UIO_WRITE =	1,
} zfs_uio_rw_t;

typedef enum zfs_uio_seg {
	UIO_USERSPACE =	0,
	UIO_SYSSPACE =	1,
} zfs_uio_seg_t;

#elif defined(__FreeBSD__)
typedef enum uio_seg  zfs_uio_seg_t;
#endif

typedef struct zfs_uio {
	struct iovec	*uio_iov;	   /* pointer to array of iovecs */
	int		uio_iovcnt;	   /* number of iovecs */
	offset_t	uio_loffset;	   /* file offset */
	zfs_uio_seg_t	uio_segflg;	   /* address space (kernel or user) */
	boolean_t	uio_fault_disable; /* for compatibility */
	uint16_t	uio_fmode;	   /* file mode flags */
	uint16_t	uio_extflg;	   /* extended flags */
	ssize_t		uio_resid;	   /* residual count */
	size_t		uio_skip;	   /* skip count in current iov */
} zfs_uio_t;

#define	zfs_uio_segflg(uio)		(uio)->uio_segflg
#define	zfs_uio_offset(uio)		(uio)->uio_loffset
#define	zfs_uio_resid(uio)		(uio)->uio_resid
#define	zfs_uio_iovcnt(uio)		(uio)->uio_iovcnt
#define	zfs_uio_iovlen(uio, idx)	(uio)->uio_iov[(idx)].iov_len
#define	zfs_uio_iovbase(uio, idx)	(uio)->uio_iov[(idx)].iov_base
#define	zfs_uio_fault_disable(u, set)	(u)->uio_fault_disable = set
#define	zfs_uio_rlimit_fsize(z, u)	(0)
#define	zfs_uio_fault_move(p, n, rw, u)	zfs_uiomove((p), (n), (rw), (u))

static inline void
zfs_uio_iovec_init(zfs_uio_t *uio, struct iovec *iov,
    unsigned long nr_segs, offset_t offset, zfs_uio_seg_t seg, ssize_t resid,
    size_t skip)
{
	ASSERT(seg == UIO_USERSPACE || seg == UIO_SYSSPACE);

	uio->uio_iov = iov;
	uio->uio_iovcnt = nr_segs;
	uio->uio_loffset = offset;
	uio->uio_segflg = seg;
	uio->uio_fault_disable = B_FALSE;
	uio->uio_fmode = 0;
	uio->uio_extflg = 0;
	uio->uio_resid = resid;
	uio->uio_skip = skip;
}

static inline void
zfs_uio_setoffset(zfs_uio_t *uio, offset_t off)
{
	uio->uio_loffset = off;
}

static inline void
zfs_uio_advance(zfs_uio_t *uio, size_t size)
{
	uio->uio_resid -= size;
	uio->uio_loffset += size;
}

/*
 * Move "n" bytes at byte address "p"; "rw" indicates the direction
 * of the move, and the I/O parameters are provided in "uio", which is
 * update to reflect the data which was moved.  Returns 0 on success or
 * a non-zero errno on failure.
 */
static inline int
zfs_uiomove(void *p, size_t n, zfs_uio_rw_t rw, zfs_uio_t *uio)
{
	struct iovec *iov = uio->uio_iov;
	size_t skip = uio->uio_skip;
	ulong_t cnt;

	while (n && uio->uio_resid) {
		cnt = MIN(iov->iov_len - skip, n);
		switch (uio->uio_segflg) {
		case UIO_USERSPACE:
			if (rw == UIO_READ)
				bcopy(p, iov->iov_base + skip, cnt);
			else
				bcopy(iov->iov_base + skip, p, cnt);
			break;
		default:
			ASSERT(0);
		}
		skip += cnt;
		if (skip == iov->iov_len) {
			skip = 0;
			uio->uio_iov = (++iov);
			uio->uio_iovcnt--;
		}
		uio->uio_skip = skip;
		uio->uio_resid -= cnt;
		uio->uio_loffset += cnt;
		p = (caddr_t)p + cnt;
		n -= cnt;
	}
	return (0);
}

/*
 * The same as zfs_uiomove() but doesn't modify uio structure.
 * return in cbytes how many bytes were copied.
 */
static inline int
zfs_uiocopy(void *p, size_t n, zfs_uio_rw_t rw, zfs_uio_t *uio, size_t *cbytes)
{
	zfs_uio_t uio_copy;
	int ret;

	ASSERT(uio->uio_segflg == UIO_USERSPACE);
	bcopy(uio, &uio_copy, sizeof (zfs_uio_t));
	ret = zfs_uiomove(p, n, rw, &uio_copy);
	*cbytes = uio->uio_resid - uio_copy.uio_resid;

	return (ret);
}

/*
 * Drop the next n chars out of *uio.
 */
static inline void
zfs_uioskip(zfs_uio_t *uio, size_t n)
{
	ASSERT(uio->uio_segflg == UIO_USERSPACE);

	if (n > uio->uio_resid)
		return;

	uio->uio_skip += n;
	while (uio->uio_iovcnt &&
	    uio->uio_skip >= uio->uio_iov->iov_len) {
		uio->uio_skip -= uio->uio_iov->iov_len;
		uio->uio_iov++;
		uio->uio_iovcnt--;
	}
	uio->uio_loffset += n;
	uio->uio_resid -= n;
}

static inline int
zfs_uio_prefaultpages(ssize_t n, zfs_uio_t *uio)
{
	return (0);
}

#endif	/* _SYS_UIO_H */
