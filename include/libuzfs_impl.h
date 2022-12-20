/*
 * CDDL HEADER START
 *
 * The contents of this file are subject to the terms of the
 * Common Development and Distribution License (the "License").
 * You may not use this file except in compliance with the License.
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
 * Copyright (c) 2022 SmartX Inc. All rights reserved.
 */

#ifndef	_LIBUZFS_IMPL_H
#define	_LIBUZFS_IMPL_H

#include <libuzfs.h>

#include <sys/zfs_context.h>
#include <sys/spa.h>
#include <sys/dmu.h>
#include <sys/sa.h>

#ifdef	__cplusplus
extern "C" {
#endif

struct libuzfs_zpool_handle {
	char zpool_name[ZFS_MAX_DATASET_NAME_LEN];
	spa_t *spa;
};

struct libuzfs_dataset_handle {
	char name[ZFS_MAX_DATASET_NAME_LEN];
	objset_t *os;
	zilog_t	*zilog;
	uint64_t sb_ino;
	sa_attr_type_t	*uzfs_attr_table;
};

typedef enum {
    TYPE_FILE,
    TYPE_DIR,
    TYPE_SYMLINK,
    TYPE_SOCK,
    TYPE_FIFO,
    TYPE_CHR,
    TYPE_BLK
} FileType;

struct uzfs_attr{
    uint64_t ino;
    uint64_t pino;
    uint32_t psid;
    FileType ftype;
    uint64_t gen;
    uint32_t nlink;
    uint32_t perm;
    uint32_t uid;
    uint32_t gid;
    uint64_t size;
    uint64_t blksize;
    uint64_t blocks;
    uint32_t nsid;
    struct timespec atime;
    struct timespec mtime;
    struct timespec ctime;
    struct timespec btime;
};

#ifdef	__cplusplus
}
#endif

#endif	/* _LIBUZFS_IMPL_H */
