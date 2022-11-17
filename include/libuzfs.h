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

#include <libnvpair.h>
#include <sys/dmu.h>

#ifdef	__cplusplus
extern "C" {
#endif

typedef enum {
	INODE_FILE = 0,
	INODE_DIR  = 1,
} libuzfs_inode_type_t;

typedef struct libuzfs_zpool_handle libuzfs_zpool_handle_t;
typedef struct libuzfs_dataset_handle libuzfs_dataset_handle_t;

typedef int (*filldir_t)(void *, const char *, int, loff_t, u64, unsigned);

extern void libuzfs_init(void);
extern void libuzfs_fini(void);
extern void libuzfs_set_zpool_cache_path(const char *zpool_cache);

extern int libuzfs_zpool_create(const char *zpool, const char *path,
    nvlist_t *props, nvlist_t *fsprops);

extern int libuzfs_zpool_destroy(const char *zpool);
extern libuzfs_zpool_handle_t *libuzfs_zpool_open(const char *zpool);
extern void libuzfs_zpool_close(libuzfs_zpool_handle_t *zhp);

extern void libuzfs_zpool_prop_set(libuzfs_zpool_handle_t *zhp,
    zpool_prop_t prop, uint64_t value);

extern int libuzfs_zpool_prop_get(libuzfs_zpool_handle_t *zhp,
    zpool_prop_t prop, uint64_t *value);

extern int libuzfs_dataset_create(const char *dsname);
extern void libuzfs_dataset_destroy(const char *dsname);
extern libuzfs_dataset_handle_t *libuzfs_dataset_open(const char *dsname);
extern void libuzfs_dataset_close(libuzfs_dataset_handle_t *dhp);

extern int libuzfs_object_stat(libuzfs_dataset_handle_t *dhp, uint64_t obj,
    dmu_object_info_t *doi);

extern int libuzfs_object_create(libuzfs_dataset_handle_t *dhp, uint64_t *obj,
    uint64_t opid);

extern int libuzfs_object_delete(libuzfs_dataset_handle_t *dhp, uint64_t obj,
    uint64_t opid);

extern int libuzfs_object_claim(libuzfs_dataset_handle_t *dhp, uint64_t obj,
    uint64_t opid);

extern int libuzfs_object_list(libuzfs_dataset_handle_t *dhp);

extern int libuzfs_object_read(libuzfs_dataset_handle_t *dhp, uint64_t obj,
    uint64_t offset, uint64_t size, char *buf);

extern int libuzfs_object_write(libuzfs_dataset_handle_t *dhp, uint64_t obj,
    uint64_t offset, uint64_t size, const char *buf);

extern int libuzfs_object_getattr(libuzfs_dataset_handle_t *dhp, uint64_t obj,
    struct stat *sp);

extern int libuzfs_object_setattr(libuzfs_dataset_handle_t *dhp, uint64_t obj,
    const struct stat *sp, uint64_t opid);

extern uint64_t libuzfs_get_max_synced_opid(libuzfs_dataset_handle_t *dhp);
extern void libuzfs_dump_txg_opids(libuzfs_dataset_handle_t *dhp);

extern void libuzfs_wait_synced(libuzfs_dataset_handle_t *dhp);

extern int libuzfs_zap_create(libuzfs_dataset_handle_t *dhp, uint64_t *obj,
    uint64_t opid);

extern int libuzfs_zap_delete(libuzfs_dataset_handle_t *dhp, uint64_t obj,
    uint64_t opid);

extern int libuzfs_zap_add(libuzfs_dataset_handle_t *dhp, uint64_t obj,
    const char *key, int integer_size, uint64_t num_integers, const void *val,
    uint64_t opid);

extern int libuzfs_zap_remove(libuzfs_dataset_handle_t *dhp, uint64_t obj,
    const char *key, uint64_t opid);

extern int libuzfs_zap_update(libuzfs_dataset_handle_t *dhp, uint64_t obj,
    const char *key, int integer_size, uint64_t num_integers, const void *val,
    uint64_t opid);

extern int libuzfs_zap_lookup(libuzfs_dataset_handle_t *dhp, uint64_t obj,
    const char *key, int integer_size, uint64_t num_integers, void *val);

extern int libuzfs_zap_count(libuzfs_dataset_handle_t *dhp, uint64_t obj,
    uint64_t *count);

extern int libuzfs_inode_create(libuzfs_dataset_handle_t *dhp, uint64_t *ino,
    libuzfs_inode_type_t type, uint64_t opid);

extern int libuzfs_inode_delete(libuzfs_dataset_handle_t *dhp, uint64_t ino,
    libuzfs_inode_type_t type, uint64_t opid);

extern int libuzfs_inode_getattr(libuzfs_dataset_handle_t *dhp, uint64_t ino,
    struct stat *sp);

extern int libuzfs_inode_setattr(libuzfs_dataset_handle_t *dhp, uint64_t ino,
    const struct stat *sp, uint64_t opid);

extern int libuzfs_inode_get_kvobj(libuzfs_dataset_handle_t *dhp,
    uint64_t ino, uint64_t *kvobj);

extern int libuzfs_inode_set_kvattr(libuzfs_dataset_handle_t *dhp, uint64_t ino,
    const char *name, const char *value, uint64_t size, int flags, uint64_t opid);

extern int libuzfs_inode_get_kvattr(libuzfs_dataset_handle_t *dhp, uint64_t ino,
    const char *name, char *value, uint64_t size, int flags);

extern int libuzfs_inode_remove_kvattr(libuzfs_dataset_handle_t *dhp,
    uint64_t ino, const char *name, uint64_t opid);

extern int libuzfs_dentry_create(libuzfs_dataset_handle_t *dhp, uint64_t dino,
    const char *name, uint64_t *value, uint64_t num, uint64_t opid);

extern int libuzfs_dentry_delete(libuzfs_dataset_handle_t *dhp, uint64_t dino,
    const char *name, uint64_t opid);

extern int libuzfs_dentry_lookup(libuzfs_dataset_handle_t *dhp, uint64_t dino,
    const char *name, uint64_t *value, uint64_t num);

extern int libuzfs_fs_create(const char *fsname);
extern void libuzfs_fs_destroy(const char *fsname);
extern int libuzfs_fs_init(const char* fsname, uint64_t* fsid);
extern int libuzfs_fs_fini(uint64_t fsid);
extern int libuzfs_getroot(uint64_t fsid, uint64_t* ino);
extern int libuzfs_getattr(uint64_t fsid, uint64_t ino, struct stat* stat);
extern int libuzfs_lookup(uint64_t fsid, uint64_t dino, char* name, uint64_t* ino);
extern int libuzfs_mkdir(uint64_t fsid, uint64_t dino, char* name, umode_t mode, uint64_t *ino);
extern int libuzfs_rmdir(uint64_t fsid, uint64_t dino, char* name);
extern int libuzfs_readdir(uint64_t fsid, uint64_t ino, void *dirent, filldir_t filldir, loff_t pos);
extern int libuzfs_create(uint64_t fsid, uint64_t dino, char* name, umode_t mode, uint64_t *ino);
extern int libuzfs_remove(uint64_t fsid, uint64_t dino, char* name);
extern int libuzfs_rename(uint64_t fsid, uint64_t sdino, char* sname, uint64_t tdino, char* tname);

extern int libuzfs_read(uint64_t fsid, uint64_t ino, zfs_uio_t *uio, int ioflag);
extern int libuzfs_write(uint64_t fsid, uint64_t ino, zfs_uio_t *uio, int ioflag);
extern int libuzfs_fsync(uint64_t fsid, uint64_t ino, int syncflag);

#ifdef	__cplusplus
}
#endif

#endif	/* _LIBUZFS_H */