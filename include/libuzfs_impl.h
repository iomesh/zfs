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
	uint64_t max_blksz;
	sa_attr_type_t	*uzfs_attr_table;
};

struct libuzfs_kvattr_iterator {
	nvlist_t *kvattrs_in_sa;
	nvpair_t *current_pair;
	zap_cursor_t zc;
	uint64_t zap_obj;
};

#define	UZFS_SIZE_OFFSET 0
#define	UZFS_GEN_OFFSET 8
#define	UZFS_UID_OFFSET 16
#define	UZFS_GID_OFFSET 20
#define	UZFS_PARENT_OFFSET 24

extern void libuzfs_object_attr_init(libuzfs_dataset_handle_t *dhp,
    sa_handle_t *sa_hdl, dmu_tx_t *tx);
extern void libuzfs_setup_dataset_sa(libuzfs_dataset_handle_t *dhp);
extern int libuzfs_get_xattr_zap_obj(libuzfs_dataset_handle_t *dhp,
    uint64_t ino, uint64_t *xattr_zap_obj);

#ifdef	__cplusplus
}
#endif

#endif	/* _LIBUZFS_IMPL_H */
