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

#ifndef _SPL_KMEM_CACHE_H
#define	_SPL_KMEM_CACHE_H

/*
 * Slab allocation interfaces.  The SPL slab differs from the standard
 * Linux SLAB or SLUB primarily in that each cache may be backed by slabs
 * allocated from the physical or virtual memory address space.  The virtual
 * slabs allow for good behavior when allocation large objects of identical
 * size.  This slab implementation also supports both constructors and
 * destructors which the Linux slab does not.
 */
typedef enum kmc_bit {
	KMC_BIT_NODEBUG		= 1,	/* Default behavior */
	KMC_BIT_KVMEM		= 7,	/* Use kvmalloc linux allocator  */
	KMC_BIT_SLAB		= 8,	/* Use Linux slab cache */
	KMC_BIT_DEADLOCKED	= 14,	/* Deadlock detected */
	KMC_BIT_GROWING		= 15,	/* Growing in progress */
	KMC_BIT_REAPING		= 16,	/* Reaping in progress */
	KMC_BIT_DESTROY		= 17,	/* Destroy in progress */
	KMC_BIT_TOTAL		= 18,	/* Proc handler helper bit */
	KMC_BIT_ALLOC		= 19,	/* Proc handler helper bit */
	KMC_BIT_MAX		= 20,	/* Proc handler helper bit */
} kmc_bit_t;

/* kmem move callback return values */
typedef enum kmem_cbrc {
	KMEM_CBRC_YES		= 0,	/* Object moved */
	KMEM_CBRC_NO		= 1,	/* Object not moved */
	KMEM_CBRC_LATER		= 2,	/* Object not moved, try again later */
	KMEM_CBRC_DONT_NEED	= 3,	/* Neither object is needed */
	KMEM_CBRC_DONT_KNOW	= 4,	/* Object unknown */
} kmem_cbrc_t;

#define	KMC_NODEBUG		(1 << KMC_BIT_NODEBUG)
#define	KMC_KVMEM		(1 << KMC_BIT_KVMEM)
#define	KMC_SLAB		(1 << KMC_BIT_SLAB)
#define	KMC_DEADLOCKED		(1 << KMC_BIT_DEADLOCKED)
#define	KMC_GROWING		(1 << KMC_BIT_GROWING)
#define	KMC_REAPING		(1 << KMC_BIT_REAPING)
#define	KMC_DESTROY		(1 << KMC_BIT_DESTROY)
#define	KMC_TOTAL		(1 << KMC_BIT_TOTAL)
#define	KMC_ALLOC		(1 << KMC_BIT_ALLOC)
#define	KMC_MAX			(1 << KMC_BIT_MAX)

#define	KMC_REAP_CHUNK		INT_MAX
#define	KMC_DEFAULT_SEEKS	1

#define	KMC_RECLAIM_ONCE	0x1	/* Force a single shrinker pass */

#define	POINTER_INVALIDATE(_pp)		/* nothing */
#define	POINTER_IS_VALID(_p)	0

#define	kmem_cache_create(_a, _b, _c, _d, _e, _f, _g, _h, _i) \
	umem_cache_create(_a, _b, _c, _d, _e, _f, _g, _h, _i)
#define	kmem_cache_destroy(_c)	umem_cache_destroy(_c)
#define	kmem_cache_alloc(_c, _f) umem_cache_alloc(_c, _f)
#define	kmem_cache_free(_c, _b)	umem_cache_free(_c, _b)
#define	kmem_debugging()	0
#define	kmem_cache_reap_now(_c)	umem_cache_reap_now(_c);
#define	kmem_cache_set_move(_c, _cb)	/* nothing */

typedef umem_cache_t kmem_cache_t;


#endif	/* _SPL_KMEM_CACHE_H */
