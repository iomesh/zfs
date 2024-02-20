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
	UZFS_GEN,
	UZFS_SIZE,
	UZFS_MTIME,
	UZFS_ZXATTR, // sa index for zap xattr inode
	UZFS_RESERVED,
	UZFS_XATTR_HIGH, // high priority kv pairs
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
	uint64_t ref_count;
	libuzfs_dataset_handle_t *dhp;
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
	kmutex_t objs_lock[NUM_NODE_BUCKETS];
};

struct libuzfs_kvattr_iterator {
	nvlist_t *hp_kvattrs_in_sa;
	nvlist_t *kvattrs_in_sa;
	nvpair_t *cur_hp_sa_pair;
	nvpair_t *cur_sa_pair;
	zap_cursor_t zc;
	uint64_t zap_obj;
};

struct libuzfs_zap_iterator {
	zap_cursor_t zc;
	zap_attribute_t za;
};

#define	UZFS_MAX_BLOCKSIZE		(1<<18)

// dnode layout(512): 192(dnode data) + 32(bonus header)
// + 32(max reserve) + 256(max kv capacity)
#define	UZFS_BONUS_LEN_DEFAULT		DN_BONUS_SIZE(512)
#define	UZFS_MAX_RESERVED_DEFAULT	32
#define	UZFS_KV_CAPACITY_DEFAULT	256

// dnode layout(1024): 192(dnode data) + 32(bonus header)
// + 192(max reserve) + 608(max kv capacity)
#define	UZFS_BONUS_LEN_1K		DN_BONUS_SIZE(1024)
#define	UZFS_MAX_RESERVED_1K		192
#define	UZFS_KV_CAPACITY_1K		608

extern void libuzfs_inode_attr_init(libuzfs_dataset_handle_t *dhp,
    sa_handle_t *sa_hdl, dmu_tx_t *tx, libuzfs_inode_type_t type, uint64_t gen);
extern void libuzfs_setup_dataset_sa(libuzfs_dataset_handle_t *dhp);
extern int libuzfs_get_xattr_zap_obj(libuzfs_dataset_handle_t *dhp,
    uint64_t ino, uint64_t *xattr_zap_obj);
extern int libuzfs_acquire_node(libuzfs_dataset_handle_t *dhp,
    uint64_t obj, libuzfs_node_t **upp);
extern void libuzfs_release_node(libuzfs_node_t *up);

#ifdef	__cplusplus
}
#endif

#endif	/* _LIBUZFS_IMPL_H */
