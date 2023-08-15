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

#include <sys/zap.h>
#include <libuzfs.h>

#include <sys/zfs_context.h>
#include <sys/zfs_rlock.h>
#include <sys/spa.h>
#include <sys/dmu.h>
#include <sys/sa.h>

#ifdef	__cplusplus
extern "C" {
#endif

typedef enum uzfs_attr_type {
	UZFS_PINO,
	UZFS_PSID,
	UZFS_FTYPE,
	UZFS_GEN,
	UZFS_NLINK,
	UZFS_PERM,
	UZFS_UID,
	UZFS_GID,
	UZFS_SIZE,
	UZFS_NSID,
	UZFS_DSID,
	UZFS_OID,
	UZFS_OGEN,
	UZFS_ATIME,
	UZFS_MTIME,
	UZFS_CTIME,
	UZFS_BTIME,
	UZFS_ZXATTR, // sa index for dir xattr inode
	UZFS_XATTR,  // sa index for sa xattr (name, value) pairs
	UZFS_END
} uzfs_attr_type_t;

struct libuzfs_zpool_handle {
	char zpool_name[ZFS_MAX_DATASET_NAME_LEN];
	spa_t *spa;
};

typedef struct hash_bucket {
	avl_tree_t tree;
	kmutex_t mutex;
} hash_bucket_t;

typedef struct libuzfs_node {
	avl_node_t node;
	zfs_rangelock_t rl;
	sa_handle_t *sa_hdl;
	uint64_t u_size;
	uint64_t u_blksz;
	uint64_t u_maxblksz;
	uint64_t u_obj;

	// access this using atomic function
	uint64_t ref_count;
} libuzfs_node_t;

#define	NUM_NODE_BUCKETS 997
struct libuzfs_dataset_handle {
	char name[ZFS_MAX_DATASET_NAME_LEN];
	objset_t *os;
	zilog_t	*zilog;
	uint64_t sb_ino;
	uint64_t max_blksz;
	sa_attr_type_t	*uzfs_attr_table;
	hash_bucket_t nodes_buckets[NUM_NODE_BUCKETS];
};

struct libuzfs_kvattr_iterator {
	nvlist_t *kvattrs_in_sa;
	nvpair_t *current_pair;
	zap_cursor_t zc;
	uint64_t zap_obj;
};

struct libuzfs_zap_iterator {
	zap_cursor_t zc;
	zap_attribute_t za;
};

#define	UZFS_SIZE_OFFSET 0
#define	UZFS_GEN_OFFSET 8
#define	UZFS_UID_OFFSET 16
#define	UZFS_GID_OFFSET 20
#define	UZFS_PARENT_OFFSET 24

extern void libuzfs_inode_attr_init(libuzfs_dataset_handle_t *dhp,
    sa_handle_t *sa_hdl, dmu_tx_t *tx, libuzfs_inode_type_t type);
extern void libuzfs_setup_dataset_sa(libuzfs_dataset_handle_t *dhp);
extern int libuzfs_get_xattr_zap_obj(libuzfs_dataset_handle_t *dhp,
    uint64_t ino, uint64_t *xattr_zap_obj);
extern int
libuzfs_acquire_node(libuzfs_dataset_handle_t *dhp,
    uint64_t obj, libuzfs_node_t **upp);
extern void
libuzfs_release_node(libuzfs_dataset_handle_t *dhp, libuzfs_node_t *up);

#ifdef	__cplusplus
}
#endif

#endif	/* _LIBUZFS_IMPL_H */
