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
 * Copyright (c) 2022, SmartX Inc. All rights reserved.
 */

#ifndef	_LIBUZFS_H
#define	_LIBUZFS_H

#include <sync_ops.h>
#include "sys/spa.h"
#include "sys/stdtypes.h"
#include "sys/time.h"
#include <libnvpair.h>
#include <sys/dmu.h>
#include <time.h>

#ifdef	__cplusplus
extern "C" {
#endif

typedef enum {
	INODE_FILE = 0,
	INODE_DIR  = 1,
	INODE_DATA_OBJ = 2,
} libuzfs_inode_type_t;

typedef enum {
	TYPE_FILE,
	TYPE_DIR,
	TYPE_SYMLINK,
	TYPE_SOCK,
	TYPE_FIFO,
	TYPE_CHR,
	TYPE_BLK
} FileType;

typedef enum {
	KVSET_NONE = 0,
	KVSET_HIGH_PRIORITY = 1 << 0,
	KVSET_NEED_LOG = 1 << 1,
} libuzfs_kvset_option_t;

struct uzfs_inode_attr {
	uint64_t gen;
	uint32_t blksize; // not on disk
	uint64_t blocks; // not on disk
};

struct uzfs_object_attr {
	uint64_t gen;
	uint64_t size;
	timespec_t mtime;
	uint32_t blksize; // not on disk
	uint64_t blocks; // not on disk
};

struct uzfs_dentry {
	uint64_t value;
	uint64_t whence;
	uint32_t size;
	char name[0];
};

typedef struct libuzfs_zpool_handle libuzfs_zpool_handle_t;
typedef struct libuzfs_dataset_handle libuzfs_dataset_handle_t;
typedef struct uzfs_inode_attr uzfs_inode_attr_t;
typedef struct uzfs_object_attr uzfs_object_attr_t;
typedef struct libuzfs_kvattr_iterator libuzfs_kvattr_iterator_t;
typedef struct libuzfs_zap_iterator libuzfs_zap_iterator_t;
typedef struct libuzfs_inode_handle libuzfs_inode_handle_t;

extern int libuzfs_inode_handle_get(
    libuzfs_dataset_handle_t *dhp, boolean_t is_data_inode,
    uint64_t ino, uint64_t gen, libuzfs_inode_handle_t **ihp);
extern void libuzfs_inode_handle_rele(libuzfs_inode_handle_t *ihp);

typedef int (*filldir_t)(void *, const char *, int, loff_t, u64, unsigned);

#define	UZFS_XATTR_MAXVALUELEN (8192)

extern void libuzfs_set_ops(const coroutine_ops_t *,
    const co_mutex_ops_t *, const co_cond_ops_t *,
    const co_rwlock_ops_t *, const aio_ops_t *,
    const thread_ops_t *, const taskq_ops_t *,
    void (*print_log)(const char *, int));

// only have effect when in debug binary
extern void libuzfs_enable_debug_msg(void);
extern void libuzfs_disable_debug_msg(void);

extern void libuzfs_init(void);
extern void libuzfs_fini(void);
extern void libuzfs_set_zpool_cache_path(const char *zpool_cache);

extern int libuzfs_zpool_create(const char *zpool, const char *path,
    nvlist_t *props, nvlist_t *fsprops);

extern int libuzfs_zpool_destroy(const char *zpool);
extern libuzfs_zpool_handle_t *libuzfs_zpool_open(const char *zpool, int *err);
extern void libuzfs_zpool_close(libuzfs_zpool_handle_t *zhp);

extern int libuzfs_zpool_import(const char *dev_path,
    char *pool_name, int size);
extern int libuzfs_zpool_export(const char *pool_name);

extern void libuzfs_zpool_prop_set(libuzfs_zpool_handle_t *zhp,
    zpool_prop_t prop, uint64_t value);

extern int libuzfs_zpool_prop_get(libuzfs_zpool_handle_t *zhp,
    zpool_prop_t prop, uint64_t *value);

extern int libuzfs_dataset_create(const char *dsname);
extern void libuzfs_dataset_destroy(const char *dsname);
extern libuzfs_dataset_handle_t *libuzfs_dataset_open(const char *dsname,
    int *err, uint32_t dnodesize, uint32_t max_blksz);
extern void libuzfs_dataset_close(libuzfs_dataset_handle_t *dhp);

extern uint64_t libuzfs_dataset_get_superblock_ino(
    libuzfs_dataset_handle_t *dhp);

extern int libuzfs_object_stat(libuzfs_dataset_handle_t *dhp, uint64_t obj,
    dmu_object_info_t *doi);

extern int libuzfs_objects_create(libuzfs_dataset_handle_t *dhp, uint64_t *objs,
    int num_objs, uint64_t *gen);

extern int libuzfs_object_delete(libuzfs_inode_handle_t *ihp);

extern uint64_t libuzfs_object_list(libuzfs_dataset_handle_t *dhp);

extern int libuzfs_object_read(libuzfs_inode_handle_t *ihp,
    uint64_t offset, uint64_t size, char *buf);

extern int libuzfs_object_write(libuzfs_inode_handle_t *ihp,
    uint64_t offset, struct iovec *iovs, int iov_cnt, boolean_t sync);

extern int libuzfs_object_get_attr(libuzfs_inode_handle_t *ihp,
    uzfs_object_attr_t *attr);

extern void libuzfs_object_sync(libuzfs_inode_handle_t *ihp);

extern int libuzfs_object_truncate(libuzfs_inode_handle_t *ihp,
    uint64_t offset, uint64_t size);

extern uint64_t libuzfs_get_max_synced_opid(libuzfs_dataset_handle_t *dhp);
extern void libuzfs_dump_txg_opids(libuzfs_dataset_handle_t *dhp);

extern uint64_t libuzfs_get_last_synced_txg(libuzfs_dataset_handle_t *dhp);
extern void libuzfs_wait_synced(libuzfs_dataset_handle_t *dhp);

extern libuzfs_zap_iterator_t *libuzfs_new_zap_iterator(
    libuzfs_dataset_handle_t *dhp, uint64_t obj, int *err);

extern int libuzfs_zap_iterator_advance(libuzfs_zap_iterator_t *iter);

extern ssize_t libuzfs_zap_iterator_name(libuzfs_zap_iterator_t *iter,
    char *name, size_t size);

extern size_t libuzfs_zap_iterator_value_size(libuzfs_zap_iterator_t *iter);

extern void libuzfs_zap_iterator_fini(libuzfs_zap_iterator_t *iter);

extern int libuzfs_zap_create(libuzfs_dataset_handle_t *dhp, uint64_t *obj,
    uint64_t *txg);

extern int libuzfs_zap_claim(libuzfs_dataset_handle_t *dhp, uint64_t obj);

extern int libuzfs_zap_delete(libuzfs_dataset_handle_t *dhp, uint64_t obj,
    uint64_t *txg);

extern int libuzfs_zap_add(libuzfs_dataset_handle_t *dhp, uint64_t obj,
    const char *key, int integer_size, uint64_t num_integers, const void *val,
    uint64_t *txg);

extern int libuzfs_zap_remove(libuzfs_dataset_handle_t *dhp, uint64_t obj,
    const char *key, uint64_t *txg);

extern int libuzfs_zap_update(libuzfs_dataset_handle_t *dhp, uint64_t obj,
    const char *key, int integer_size, uint64_t num_integers, const void *val,
    uint64_t *txg);

extern int libuzfs_zap_lookup(libuzfs_dataset_handle_t *dhp, uint64_t obj,
    const char *key, int integer_size, uint64_t num_integers, void *val);

extern int libuzfs_zap_count(libuzfs_dataset_handle_t *dhp, uint64_t obj,
    uint64_t *count);

// libuzfs_exit should be called after other attrs are set
extern int libuzfs_inode_create(libuzfs_dataset_handle_t *dhp, uint64_t *ino,
    libuzfs_inode_type_t type, libuzfs_inode_handle_t **ihpp, uint64_t *gen);

extern int libuzfs_inode_claim(libuzfs_dataset_handle_t *dhp, uint64_t ino,
    uint64_t gen, libuzfs_inode_type_t type);

extern int libuzfs_inode_get_kvobj(libuzfs_dataset_handle_t *dhp,
    uint64_t ino, uint64_t *kvobj);

extern int libuzfs_dentry_create(libuzfs_inode_handle_t *ihp,
    const char *name, uint64_t value, uint64_t *txg);

extern int libuzfs_dentry_delete(libuzfs_inode_handle_t *ihp,
    const char *name, uint64_t *txg);

extern int libuzfs_dentry_lookup(libuzfs_inode_handle_t *ihp,
    const char *name, uint64_t *value);

extern int libuzfs_dentry_iterate(libuzfs_inode_handle_t *ihp,
    uint64_t whence, uint32_t size, char *buf, uint32_t *num);

extern int libuzfs_fs_create(const char *fsname);
extern void libuzfs_fs_destroy(const char *fsname);
extern int libuzfs_fs_init(const char *fsname, uint64_t *fsid);
extern int libuzfs_fs_fini(uint64_t fsid);
extern int libuzfs_getroot(uint64_t fsid, uint64_t *ino);
extern int libuzfs_getattr(uint64_t fsid, uint64_t ino, struct stat *stat);
extern int libuzfs_lookup(uint64_t fsid, uint64_t dino, char *name,
    uint64_t *ino);

extern int libuzfs_mkdir(uint64_t fsid, uint64_t dino, char *name,
    umode_t mode, uint64_t *ino);

extern int libuzfs_rmdir(uint64_t fsid, uint64_t dino, char *name);
extern int libuzfs_readdir(uint64_t fsid, uint64_t ino, void *dirent,
    filldir_t filldir, loff_t pos);

extern int libuzfs_create(uint64_t fsid, uint64_t dino, char *name,
    umode_t mode, uint64_t *ino);

extern int libuzfs_remove(uint64_t fsid, uint64_t dino, char *name);
extern int libuzfs_rename(uint64_t fsid, uint64_t sdino, char *sname,
    uint64_t tdino, char *tname);

extern int libuzfs_read(uint64_t fsid, uint64_t ino, zfs_uio_t *uio,
    int ioflag);

extern int libuzfs_write(uint64_t fsid, uint64_t ino, zfs_uio_t *uio,
    int ioflag);

extern int libuzfs_fsync(uint64_t fsid, uint64_t ino, int syncflag);

extern int libuzfs_inode_delete(libuzfs_inode_handle_t *ihp,
    libuzfs_inode_type_t inode_type, uint64_t *txg);
extern int libuzfs_inode_getattr(libuzfs_inode_handle_t *ihp,
    uzfs_inode_attr_t *attr, char *reserved, int *size);
extern int libuzfs_inode_setattr(libuzfs_inode_handle_t *ihp,
    const char *reserved, uint32_t size, uint64_t *txg);
extern int libuzfs_object_setmtime(libuzfs_inode_handle_t *ihp,
    const struct timespec *mtime, boolean_t sync);
extern int libuzfs_inode_set_kvattr(libuzfs_inode_handle_t *ihp,
    const char *name, const char *value, uint64_t size,
    uint64_t *txg, uint32_t option);
extern ssize_t libuzfs_inode_get_kvattr(libuzfs_inode_handle_t *ihp,
    const char *name, char *value, uint64_t size);
extern int libuzfs_inode_remove_kvattr(libuzfs_inode_handle_t *ihp,
    const char *name, uint64_t *txg);

extern libuzfs_kvattr_iterator_t *libuzfs_new_kvattr_iterator(
    libuzfs_inode_handle_t *ihp, int *err);
extern ssize_t libuzfs_next_kvattr_name(libuzfs_kvattr_iterator_t *iter,
    char *buf, int size);
extern void libuzfs_kvattr_iterator_fini(libuzfs_kvattr_iterator_t *iter);

extern void libuzfs_dataset_space(libuzfs_dataset_handle_t *dhp,
    uint64_t *refdbytes, uint64_t *availbytes, uint64_t *usedobjs,
    uint64_t *availobjs);
extern int libuzfs_object_next_hole(libuzfs_inode_handle_t *ihp, uint64_t *off);
extern void libuzfs_wait_log_commit(libuzfs_dataset_handle_t *dhp);

extern int libuzfs_dataset_expand(libuzfs_dataset_handle_t *dhp);

extern void libuzfs_set_fail_percent(int fail_percent);

extern int libuzfs_start_manual_trim(libuzfs_dataset_handle_t *dhp);
extern int libuzfs_object_next_block(libuzfs_inode_handle_t *ihp,
    uint64_t *offset, uint64_t *size);

extern void libuzfs_debug_main(int argc, char **argv);
#ifdef	__cplusplus
}
#endif

#endif	/* _LIBUZFS_H */
