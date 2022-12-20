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
 * Copyright 2006 Sun Microsystems, Inc.  All rights reserved.
 * Use is subject to license terms.
 */

#ifndef _LIBSPL_STRING_H
#define	_LIBSPL_STRING_H

#include_next <string.h>

#ifndef HAVE_STRLCAT
extern size_t strlcat(char *dst, const char *src, size_t dstsize);
#endif

#ifndef HAVE_STRLCPY
extern size_t strlcpy(char *dst, const char *src, size_t len);
#endif

static inline int ddi_copyin(const void *from, void *to, size_t len, int flags)
{
	memcpy(to, from, len);
	return (0);
}

static inline int ddi_copyout(const void *from, void *to, size_t len, int flags)
{
	memcpy(to, from, len);
	return (0);
}

static inline int xcopyin(const void *from, void *to, size_t len)
{
	memcpy(to, from, len);
	return (0);
}

static inline int xcopyout(const void *from, void *to, size_t len)
{
	memcpy(to, from, len);
	return (0);
}

static inline int
copyinstr(const void *from, void *to, size_t len, size_t *done)
{
	if (len == 0)
		return (-1);

	/* XXX: Should return ENAMETOOLONG if 'strlen(from) > len' */

	memset(to, 0, len);
	if (xcopyin(from, to, len - 1) == 0) {
		if (done) {
			*done = 0;
		}
	} else {
		if (done) {
			*done = len - 1;
		}
	}

	return (0);
}


#endif
