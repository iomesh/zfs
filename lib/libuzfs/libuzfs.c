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

#include <asm-generic/errno-base.h>
#include <bits/stdint-uintn.h>
#include <stdint.h>
#include <sys/zfs_context.h>
#include <sys/spa.h>
#include <sys/dmu.h>
#include <sys/dbuf.h>
#include <sys/zap.h>
#include <sys/dmu_objset.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/zio.h>
#include <sys/zil.h>
#include <sys/zil_impl.h>
#include <sys/vdev_impl.h>
#include <sys/vdev_file.h>
#include <sys/spa_impl.h>
#include <sys/dsl_prop.h>
#include <sys/dsl_dataset.h>
#include <sys/dsl_destroy.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include <umem.h>
#include <ctype.h>
#include <math.h>
#include <sys/fs/zfs.h>
#include <libnvpair.h>
#include <libzutil.h>
#include <sys/zfs_vnops.h>
#include <sys/zfs_znode.h>
#include <sys/zfs_vfsops.h>
#include <sys/zfs_ioctl_impl.h>
#include <sys/sa_impl.h>

#include "atomic.h"
#include "libuzfs.h"
#include "libuzfs_impl.h"
#include "sys/dnode.h"
#include "sys/nvpair.h"
#include "sys/sa.h"
#include "sys/stdtypes.h"
#include "sys/sysmacros.h"
#include "sys/txg.h"
#include "sys/zfs_debug.h"
#include "sys/zfs_refcount.h"
#include "sys/zfs_rlock.h"

static boolean_t change_zpool_cache_path = B_FALSE;

static libuzfs_inode_handle_t *libuzfs_create_inode_with_type_impl(
    libuzfs_dataset_handle_t *, uint64_t *, boolean_t,
    libuzfs_inode_type_t, dmu_tx_t *, uint64_t);

static inline int libuzfs_object_write_impl(libuzfs_inode_handle_t *,
    uint64_t, struct iovec *, int, boolean_t, uint64_t);

static void
libuzfs_rangelock_cb(zfs_locked_range_t *new, void *arg)
{
	// no need to modify lock range for truncate
	if (new->lr_length == UINT64_MAX) {
		return;
	}

	libuzfs_inode_handle_t *ihp = arg;
	// If we need to grow the block size then lock the whole file range.
	uint64_t end_size = MAX(ihp->u_size, new->lr_offset + new->lr_length);
	if (ihp->u_blksz < end_size && (ihp->u_blksz < ihp->dhp->max_blksz ||
	    !ISP2(ihp->u_blksz))) {
		new->lr_offset = 0;
		new->lr_length = UINT64_MAX;
	}
}

static int
libuzfs_sa_handle_get(libuzfs_dataset_handle_t *dhp,
    uint64_t ino, sa_handle_t **sa_hdlp)
{
	ASSERT(MUTEX_HELD(&dhp->objs_lock[ino % NUM_NODE_BUCKETS]));
	dmu_buf_t *db;
	objset_t *os = dhp->os;
	int err = sa_buf_hold(os, ino, NULL, &db);
	if (err != 0) {
		return (err);
	}

	sa_handle_t *sa_hdl = dmu_buf_get_user(db);
	if (sa_hdl != NULL) {
		sa_buf_rele(db, NULL);
	} else {
		err = sa_handle_get_from_db(os, db, NULL,
		    SA_HDL_SHARED, &sa_hdl);
		if (err != 0) {
			sa_buf_rele(db, NULL);
			sa_hdl = NULL;
		}
	}

	*sa_hdlp = sa_hdl;

	return (err);
}

static void
libuzfs_inode_handle_init(libuzfs_inode_handle_t *ihp,
    sa_handle_t *sa_hdl, libuzfs_dataset_handle_t *dhp,
    uint64_t ino, uint64_t gen, boolean_t is_data_inode)
{
	ihp->sa_hdl = sa_hdl;
	ihp->dhp = dhp;
	sa_attr_type_t *attr_tbl = dhp->uzfs_attr_table;
	rw_init(&ihp->hp_kvattr_cache_lock, NULL, RW_DEFAULT, NULL);
	VERIFY0(libuzfs_get_nvlist_from_handle(attr_tbl, &ihp->hp_kvattr_cache,
	    sa_hdl, UZFS_XATTR_HIGH));
	ihp->ino = ino;
	ihp->rc = 1;
	ihp->gen = gen;
	ihp->is_data_inode = B_TRUE;

	if (is_data_inode) {
		VERIFY0(sa_lookup(sa_hdl, attr_tbl[UZFS_SIZE],
		    &ihp->u_size, sizeof (ihp->u_size)));
		dmu_buf_impl_t *db = (dmu_buf_impl_t *)sa_get_db(sa_hdl);
		DB_DNODE_ENTER(db);
		dnode_t *dn = DB_DNODE(db);
		DB_DNODE_EXIT(db);
		ihp->u_blksz = dn->dn_datablksz;
		zfs_rangelock_init(&ihp->rl, libuzfs_rangelock_cb, ihp);
	}
}

int
libuzfs_inode_handle_get(libuzfs_dataset_handle_t *dhp,
    boolean_t is_data_inode, uint64_t ino,
    uint64_t gen, libuzfs_inode_handle_t **ihpp)
{
	kmutex_t *mp = &dhp->objs_lock[ino % NUM_NODE_BUCKETS];
	mutex_enter(mp);

	sa_handle_t *sa_hdl = NULL;
	int err = libuzfs_sa_handle_get(dhp, ino, &sa_hdl);
	if (sa_hdl == NULL) {
		mutex_exit(mp);
		return (err);
	}

	libuzfs_inode_handle_t *ihp = sa_get_userdata(sa_hdl);
	if (ihp != NULL) {
		if (unlikely(ihp->gen != gen && gen != -1ul)) {
			mutex_exit(mp);
			return (ENOENT);
		}
		++ihp->rc;
	} else {
		sa_attr_type_t *attr_tbl = dhp->uzfs_attr_table;

		uint64_t stored_gen;
		err = sa_lookup(sa_hdl, attr_tbl[UZFS_GEN],
		    &stored_gen, sizeof (stored_gen));
		if (err != 0) {
			sa_handle_destroy(sa_hdl);
			mutex_exit(mp);
			return (err);
		}

		if (unlikely(stored_gen != gen && gen != -1ul)) {
			sa_handle_destroy(sa_hdl);
			mutex_exit(mp);
			return (ENOENT);
		}

		ihp = umem_zalloc(
		    sizeof (libuzfs_inode_handle_t), UMEM_NOFAIL);
		libuzfs_inode_handle_init(ihp, sa_hdl,
		    dhp, ino, gen, is_data_inode);

		sa_set_userp(sa_hdl, ihp);
	}

	mutex_exit(mp);
	*ihpp = ihp;

	return (0);
}

void
libuzfs_inode_handle_rele(libuzfs_inode_handle_t *ihp)
{
	kmutex_t *mp = &ihp->dhp->objs_lock[ihp->ino % NUM_NODE_BUCKETS];
	mutex_enter(mp);
	rw_destroy(&ihp->hp_kvattr_cache_lock);
	if (--ihp->rc <= 0) {
		ASSERT(ihp->hp_kvattr_cache);
		nvlist_free(ihp->hp_kvattr_cache);
		sa_handle_destroy(ihp->sa_hdl);
		umem_free(ihp, sizeof (libuzfs_inode_handle_t));
	}

	mutex_exit(mp);
}

typedef struct dir_emit_ctx {
	char *buf;
	char *cur;
	uint32_t size;
} dir_emit_ctx_t;

static void
dump_debug_buffer(void)
{
	ssize_t ret __attribute__((unused));

	/*
	 * We use write() instead of printf() so that this function
	 * is safe to call from a signal handler.
	 */
	ret = write(STDOUT_FILENO, "\n", 1);
	zfs_dbgmsg_print("libuzfs");
}

#define	FATAL_MSG_SZ	1024
#define	MAX_POOLS_IN_ONE_DEV	256

char *fatal_msg;

static void
fatal(int do_perror, char *message, ...)
{
	va_list args;
	int save_errno = errno;
	char *buf = NULL;

	(void) fflush(stdout);
	buf = umem_alloc(FATAL_MSG_SZ, UMEM_NOFAIL);

	va_start(args, message);
	(void) sprintf(buf, "libuzfs: ");
	/* LINTED */
	(void) vsprintf(buf + strlen(buf), message, args);
	va_end(args);
	if (do_perror) {
		(void) snprintf(buf + strlen(buf), FATAL_MSG_SZ - strlen(buf),
		    ": %s", strerror(save_errno));
	}
	(void) fprintf(stderr, "%s\n", buf);
	fatal_msg = buf;			/* to ease debugging */

	dump_debug_buffer();

	exit(3);
}

extern aio_ops_t aio_ops;

void
libuzfs_set_sync_ops(const coroutine_ops_t *co,
    const co_mutex_ops_t *mo, const co_cond_ops_t *condo,
    const co_rwlock_ops_t *ro, const aio_ops_t *ao,
    const thread_ops_t *tho, const taskq_ops_t *tqo)
{
	co_ops = *co;
	co_mutex_ops = *mo;
	co_cond_ops = *condo;
	co_rwlock_ops = *ro;
	aio_ops = *ao;
	thread_ops = *tho;
	taskq_ops = *tqo;
}

static uint64_t
libuzfs_get_ashift(void)
{
	return (UZFS_VDEV_ASHIFT);
}

static nvlist_t *
make_vdev_file(const char *path, char *aux, const char *pool, size_t size,
    uint64_t ashift)
{
	char *pathbuf = NULL;
	nvlist_t *file = NULL;

	pathbuf = umem_alloc(MAXPATHLEN, UMEM_NOFAIL);

	if (ashift == 0)
		ashift = libuzfs_get_ashift();

	if (size != 0) {
		int fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0666);
		if (fd == -1)
			fatal(1, "can't open %s", path);
		if (ftruncate(fd, size) != 0)
			fatal(1, "can't ftruncate %s", path);
		(void) close(fd);
	}

	file = fnvlist_alloc();
	fnvlist_add_string(file, ZPOOL_CONFIG_TYPE, VDEV_TYPE_AIO_FILE);
	fnvlist_add_string(file, ZPOOL_CONFIG_PATH, path);
	fnvlist_add_uint64(file, ZPOOL_CONFIG_ASHIFT, ashift);

	umem_free(pathbuf, MAXPATHLEN);

	return (file);
}

static nvlist_t *
make_vdev_raid(const char *path, char *aux, const char *pool, size_t size,
    uint64_t ashift, int r)
{
	return (make_vdev_file(path, aux, pool, size, ashift));
}

static nvlist_t *
make_vdev_mirror(const char *path, char *aux, const char *pool, size_t size,
    uint64_t ashift, int r, int m)
{
	int c = 0;
	nvlist_t *mirror = NULL;
	nvlist_t **child = NULL;

	if (m < 1)
		return (make_vdev_raid(path, aux, pool, size, ashift, r));

	child = umem_alloc(m * sizeof (nvlist_t *), UMEM_NOFAIL);

	for (c = 0; c < m; c++)
		child[c] = make_vdev_raid(path, aux, pool, size, ashift, r);

	mirror = fnvlist_alloc();
	fnvlist_add_string(mirror, ZPOOL_CONFIG_TYPE, VDEV_TYPE_MIRROR);
	fnvlist_add_nvlist_array(mirror, ZPOOL_CONFIG_CHILDREN, child, m);

	for (c = 0; c < m; c++)
		fnvlist_free(child[c]);

	umem_free(child, m * sizeof (nvlist_t *));

	return (mirror);
}

static nvlist_t *
make_vdev_root(const char *path, char *aux, const char *pool, size_t size,
    uint64_t ashift, const char *class, int r, int m, int t)
{
	int c = 0;
	boolean_t log = B_FALSE;
	nvlist_t *root = NULL;
	nvlist_t **child = NULL;

	ASSERT3S(t, >, 0);

	log = (class != NULL && strcmp(class, "log") == 0);

	child = umem_alloc(t * sizeof (nvlist_t *), UMEM_NOFAIL);

	for (c = 0; c < t; c++) {
		child[c] = make_vdev_mirror(path, aux, pool, size, ashift,
		    r, m);
		fnvlist_add_uint64(child[c], ZPOOL_CONFIG_IS_LOG, log);

		if (class != NULL && class[0] != '\0') {
			ASSERT(m > 1 || log);   /* expecting a mirror */
			fnvlist_add_string(child[c],
			    ZPOOL_CONFIG_ALLOCATION_BIAS, class);
		}
	}

	root = fnvlist_alloc();
	fnvlist_add_string(root, ZPOOL_CONFIG_TYPE, VDEV_TYPE_ROOT);
	fnvlist_add_nvlist_array(root, aux ? aux : ZPOOL_CONFIG_CHILDREN,
	    child, t);

	for (c = 0; c < t; c++)
		fnvlist_free(child[c]);

	umem_free(child, t * sizeof (nvlist_t *));

	return (root);
}

static int
libuzfs_spa_prop_set_uint64(spa_t *spa, zpool_prop_t prop, uint64_t value)
{
	int err = 0;
	nvlist_t *props = NULL;

	props = fnvlist_alloc();
	fnvlist_add_uint64(props, zpool_prop_to_name(prop), value);

	err = spa_prop_set(spa, props);

	fnvlist_free(props);

	if (err == ENOSPC)
		return (err);

	ASSERT0(err);

	return (err);
}

static int
libuzfs_dmu_objset_own(const char *name, dmu_objset_type_t type,
    boolean_t readonly, boolean_t decrypt, void *tag, objset_t **osp)
{
	int err = 0;
	char *cp = NULL;
	char ddname[ZFS_MAX_DATASET_NAME_LEN];

	strcpy(ddname, name);
	cp = strchr(ddname, '@');
	if (cp != NULL)
		*cp = '\0';

	err = dmu_objset_own(name, type, readonly, decrypt, tag, osp);
	return (err);
}

static void
libuzfs_log_create(zilog_t *zilog, dmu_tx_t *tx, uint64_t obj)
{
	itx_t *itx;
	lr_create_t *lr;

	if (zil_replaying(zilog, tx))
		return;

	itx = zil_itx_create(TX_CREATE, sizeof (*lr));

	lr = (lr_create_t *)&itx->itx_lr;
	lr->lr_foid = obj;
	lr->lr_gen = tx->tx_txg;

	zil_itx_assign(zilog, itx, tx);
}

static void
libuzfs_log_remove(zilog_t *zilog, dmu_tx_t *tx, uint64_t obj)
{
	itx_t *itx;
	lr_remove_t *lr;

	if (zil_replaying(zilog, tx))
		return;

	itx = zil_itx_create(TX_REMOVE, sizeof (*lr));

	lr = (lr_remove_t *)&itx->itx_lr;

	// use lr_doid for obj
	lr->lr_doid = obj;

	itx->itx_oid = obj;
	zil_itx_assign(zilog, itx, tx);
}

static void
libuzfs_log_truncate(zilog_t *zilog, dmu_tx_t *tx, int txtype,
    uint64_t obj, uint64_t offset, uint64_t size)
{
	itx_t *itx;
	lr_truncate_t *lr;

	if (zil_replaying(zilog, tx))
		return;

	itx = zil_itx_create(txtype, sizeof (lr_truncate_t));
	lr = (lr_truncate_t *)&itx->itx_lr;
	lr->lr_foid = obj;
	lr->lr_offset = offset;
	lr->lr_length = size;

	itx->itx_sync = B_TRUE;
	zil_itx_assign(zilog, itx, tx);
}

static ssize_t libuzfs_immediate_write_sz = 32 << 10;

static void
libuzfs_log_write(libuzfs_inode_handle_t *ihp, offset_t off,
    ssize_t resid, boolean_t sync, dmu_tx_t *tx)
{
	libuzfs_dataset_handle_t *dhp = ihp->dhp;
	// FIXME(sundengyu): this may cover over 1 blocks when blksize changes
	ASSERT3U(off / dhp->max_blksz, ==, (off + resid - 1) / dhp->max_blksz);

	zilog_t *zilog = dhp->zilog;
	if (zil_replaying(zilog, tx)) {
		return;
	}

	boolean_t wr_copied = resid <= zil_max_copied_data(zilog) &&
	    resid <= libuzfs_immediate_write_sz && sync;
	itx_t *itx = zil_itx_create(TX_WRITE, sizeof (lr_write_t) +
	    (wr_copied ? resid : 0));
	lr_write_t *lr = (lr_write_t *)&itx->itx_lr;
	dmu_buf_impl_t *db = (dmu_buf_impl_t *)sa_get_db(ihp->sa_hdl);
	if (wr_copied) {
		itx->itx_wr_state = WR_COPIED;
		DB_DNODE_ENTER(db);
		int err = dmu_read_by_dnode(DB_DNODE(db), off, resid,
		    lr + 1, DMU_READ_NO_PREFETCH | DMU_READ_NO_SKIP);
		DB_DNODE_EXIT(db);
		if (err == 0) {
			goto set_lr;
		}

		zil_itx_destroy(itx);
		itx = zil_itx_create(TX_WRITE, sizeof (lr_write_t));
		lr = (lr_write_t *)&itx->itx_lr;
	}

	if (resid <= libuzfs_immediate_write_sz) {
		itx->itx_wr_state = WR_NEED_COPY;
	} else {
		itx->itx_wr_state = WR_INDIRECT;
	}

set_lr:
	lr->lr_foid = ihp->ino;
	lr->lr_offset = off;
	lr->lr_length = resid;
	lr->lr_blkoff = 0;
	BP_ZERO(&lr->lr_blkptr);

	itx->itx_sync = sync;
	itx->itx_callback = NULL;
	itx->itx_callback_data = NULL;
	itx->itx_private = dhp;
	itx->itx_gen = ihp->gen;

	zil_itx_assign(zilog, itx, tx);
}

static int libuzfs_object_claim(libuzfs_dataset_handle_t *dhp,
    uint64_t obj, uint64_t gen, libuzfs_inode_type_t type);

static int
libuzfs_replay_create(void *arg1, void *arg2, boolean_t byteswap)
{
	lr_create_t *lr = arg2;
	if (byteswap) {
		byteswap_uint64_array(lr, sizeof (*lr));
	}

	uint64_t obj = LR_FOID_GET_OBJ(lr->lr_foid);
	zfs_dbgmsg("replay create, obj: %ld", obj);

	return (libuzfs_object_claim((libuzfs_dataset_handle_t *)arg1,
	    obj, lr->lr_gen, INODE_DATA_OBJ));
}

static int
libuzfs_replay_remove(void *arg1, void *arg2, boolean_t byteswap)
{
	lr_remove_t *lr = arg2;
	if (byteswap)
		byteswap_uint64_array(lr, sizeof (*lr));

	zfs_dbgmsg("replay remove, obj: %ld", lr->lr_doid);
	libuzfs_dataset_handle_t *dhp = arg1;
	int err = dmu_free_long_range(dhp->os, lr->lr_doid, 0, DMU_OBJECT_END);
	if (err != 0) {
		return (err);
	}

	libuzfs_inode_handle_t *ihp = NULL;
	err = libuzfs_inode_handle_get(dhp, B_TRUE,
	    lr->lr_doid, -1ul, &ihp);
	if (err == 0) {
		err = libuzfs_inode_delete(ihp, INODE_DATA_OBJ, NULL);
	}

	libuzfs_inode_handle_rele(ihp);

	return (err);
}

static void
libuzfs_grow_blocksize(libuzfs_inode_handle_t *ihp,
    uint64_t newblksz, dmu_tx_t *tx)
{
	dmu_buf_impl_t *db = (dmu_buf_impl_t *)sa_get_db(ihp->sa_hdl);
	DB_DNODE_ENTER(db);
	dnode_t *dn = DB_DNODE(db);
	int err = dnode_set_blksz(dn, newblksz, 0, tx);
	if (err != 0) {
		dprintf("failed to update to new"
		    " blocksize %lu from %u, err: %d",
		    newblksz, ihp->u_blksz, err);
		ASSERT(ISP2(ihp->u_blksz));
		VERIFY3U(err, ==, ENOTSUP);
	}
	DB_DNODE_EXIT(db);
	ihp->u_blksz = dn->dn_datablksz;
}

static int
libuzfs_object_truncate_impl(libuzfs_inode_handle_t *ihp,
    uint64_t offset, uint64_t size)
{
	libuzfs_dataset_handle_t *dhp = ihp->dhp;
	objset_t *os = dhp->os;
	int err = 0;
	if (size < ihp->u_size) {
		err = dmu_free_long_range(os, ihp->ino, size, DMU_OBJECT_END);
		if (err) {
			return (err);
		}
	}

	dmu_tx_t *tx = dmu_tx_create(os);
	dmu_tx_hold_sa(tx, ihp->sa_hdl, FALSE);
	uint64_t newblksz = 0;
	if (size > ihp->u_size && size > ihp->u_blksz &&
	    (!ISP2(ihp->u_blksz) || ihp->u_blksz < dhp->max_blksz)) {
		if (ihp->u_blksz > dhp->max_blksz) {
			ASSERT(!ISP2(ihp->u_blksz));
			newblksz = MIN(size, 1 << highbit64(ihp->u_blksz));
		} else {
			newblksz = MIN(size, dhp->max_blksz);
		}
		dmu_tx_hold_write(tx, ihp->ino, 0, newblksz);
	}

	if ((err = dmu_tx_assign(tx, TXG_WAIT)) != 0) {
		dmu_tx_abort(tx);
	} else {
		sa_attr_type_t *sa_tbl = dhp->uzfs_attr_table;
		sa_bulk_attr_t bulk[2];
		int count = 0;
		struct timespec mtime;
		ihp->u_size = size;
		SA_ADD_BULK_ATTR(bulk, count, sa_tbl[UZFS_SIZE],
		    NULL, &ihp->u_size, sizeof (ihp->u_size));
		SA_ADD_BULK_ATTR(bulk, count, sa_tbl[UZFS_MTIME],
		    NULL, &mtime, sizeof (mtime));
		gethrestime(&mtime);
		if (newblksz > 0) {
			libuzfs_grow_blocksize(ihp, newblksz, tx);
		}
		VERIFY0(sa_bulk_update(ihp->sa_hdl, bulk, count, tx));
		libuzfs_log_truncate(dhp->zilog, tx,
		    TX_TRUNCATE, ihp->ino, offset, size);
		dmu_tx_commit(tx);
	}
	return (err);
}

static int
libuzfs_replay_truncate(void *arg1, void *arg2, boolean_t byteswap)
{
	lr_truncate_t *lr = arg2;
	if (byteswap)
		byteswap_uint64_array(lr, sizeof (*lr));

	uint64_t obj = lr->lr_foid;
	uint64_t offset = lr->lr_offset;
	uint64_t size = lr->lr_length;

	zfs_dbgmsg("replay truncate, obj: %ld, off: %ld, size: %ld",
	    obj, offset, size);

	ASSERT3U(offset, ==, 0);
	libuzfs_dataset_handle_t *dhp = arg1;
	libuzfs_inode_handle_t *ihp = NULL;
	int err = libuzfs_inode_handle_get(dhp, B_TRUE,
	    obj, -1ul, &ihp);
	if (err == 0) {
		err = libuzfs_object_truncate_impl(ihp, offset, size);
	}

	libuzfs_inode_handle_rele(ihp);

	return (err);
}

static int
libuzfs_replay_write(void *arg1, void *arg2, boolean_t byteswap)
{
	lr_write_t *lr = arg2;
	void *data = lr + 1;
	if (byteswap)
		byteswap_uint64_array(lr, sizeof (*lr));

	uint64_t obj = lr->lr_foid;
	uint64_t offset = lr->lr_offset;
	uint64_t length = lr->lr_length;
	boolean_t indirect = lr->lr_common.lrc_reclen == sizeof (lr_write_t);

	zfs_dbgmsg("replaying write, obj: %ld, off: %ld, length: %ld,"
	    " indirect: %d, first bytes: %d", obj, offset,
	    length, indirect, *(uint8_t *)data);

	/*
	 * This may be a write from a dmu_sync() for a whole block,
	 * and may extend beyond the current end of the file.
	 * We can't just replay what was written for this TX_WRITE as
	 * a future TX_WRITE2 may extend the eof and the data for that
	 * write needs to be there. So we write the whole block and
	 * reduce the eof. This needs to be done within the single dmu
	 * transaction created within libuzfs_write. So a possible
	 * new end of file is passed through in replay_eof
	 */
	uint64_t replay_eof = 0;
	if (indirect) {
		uint64_t blocksize = BP_GET_LSIZE(&lr->lr_blkptr);
		if (length < blocksize) {
			replay_eof = offset + length;
			offset -= offset % blocksize;
			length = blocksize;
		}
	}

	struct iovec iov;
	iov.iov_base = data;
	iov.iov_len = length;

	libuzfs_dataset_handle_t *dhp = arg1;
	libuzfs_inode_handle_t *ihp = NULL;
	int err = libuzfs_inode_handle_get(dhp, B_TRUE,
	    obj, -1ul, &ihp);
	if (err == 0) {
		err = libuzfs_object_write_impl(ihp, offset,
		    &iov, 1, FALSE, replay_eof);
	}

	libuzfs_inode_handle_rele(ihp);

	return (err);
}

static int
libuzfs_replay_kvattr_set(void *arg1, void *arg2, boolean_t byteswap)
{
	lr_kv_set_t *lr = arg2;
	if (byteswap) {
		byteswap_uint64_array(lr, sizeof (lr_kv_set_t));
	}

	char *name = (char *)(lr + 1);
	char *value = name + lr->lr_name_len + 1;
	libuzfs_dataset_handle_t *dhp = arg1;
	libuzfs_inode_handle_t *ihp = NULL;
	// set is_data_inode to false in case that
	// we need sync kv set for dir like superblock
	int err = libuzfs_inode_handle_get(dhp, B_FALSE,
	    lr->lr_foid, -1ul, &ihp);
	if (err == 0) {
		err = libuzfs_inode_set_kvattr(ihp, name, value,
		    lr->lr_value_size, NULL, lr->option);
	}

	libuzfs_inode_handle_rele(ihp);

	return (err);
}

static int
libuzfs_replay_write2(void *arg1, void *arg2, boolean_t byteswap)
{
	libuzfs_dataset_handle_t *dhp = arg1;
	lr_write_t *lr = arg2;
	if (byteswap) {
		byteswap_uint64_array(lr, sizeof (*lr));
	}

	zfs_dbgmsg("replaying write2, obj: %ld, off: %ld, length: %ld,",
	    lr->lr_foid, lr->lr_offset, lr->lr_length);
	libuzfs_inode_handle_t *ihp = NULL;
	int err = libuzfs_inode_handle_get(dhp, B_TRUE,
	    lr->lr_foid, -1ul, &ihp);
	if (err != 0) {
		return (err);
	}

	uint64_t end = lr->lr_offset + lr->lr_length;
	if (end > ihp->u_size) {
		ihp->u_size = end;
		dmu_tx_t *tx = dmu_tx_create(dhp->os);
		dmu_tx_hold_sa(tx, ihp->sa_hdl, B_FALSE);
		err = dmu_tx_assign(tx, TXG_WAIT);
		if (err != 0) {
			dmu_tx_abort(tx);
			goto out;
		}

		VERIFY0(sa_update(ihp->sa_hdl, dhp->uzfs_attr_table[UZFS_SIZE],
		    &end, sizeof (end), tx));

		/* Ensure the replayed seq is updated */
		(void) zil_replaying(dhp->zilog, tx);
		dmu_tx_commit(tx);
	}

out:
	libuzfs_inode_handle_rele(ihp);
	return (err);
}

static int
libuzfs_replay_setmtime(void *arg1, void *arg2, boolean_t byteswap)
{
	libuzfs_dataset_handle_t *dhp = arg1;
	lr_obj_mtime_set_t *lr = arg2;
	if (byteswap) {
		byteswap_uint64_array(lr, sizeof (*lr));
	}

	zfs_dbgmsg("mtime: tv_sec: %lu, tv_nsec: %lu",
	    lr->mtime[0], lr->mtime[1]);
	libuzfs_inode_handle_t *ihp = NULL;
	int err = libuzfs_inode_handle_get(dhp, B_TRUE,
	    lr->lr_foid, -1ul, &ihp);
	if (err == 0) {
		err = libuzfs_object_setmtime(ihp,
		    (const struct timespec *)lr->mtime, B_FALSE);
	}

	libuzfs_inode_handle_rele(ihp);
	return (err);
}

zil_replay_func_t *libuzfs_replay_vector[TX_MAX_TYPE] = {
	NULL,				/* 0 no such transaction type */
	libuzfs_replay_create,		/* TX_CREATE */
	NULL,				/* TX_MKDIR */
	libuzfs_replay_kvattr_set,	/* TX_MKXATTR */
	NULL,				/* TX_SYMLINK */
	libuzfs_replay_remove,		/* TX_REMOVE */
	NULL,				/* TX_RMDIR */
	NULL,				/* TX_LINK */
	NULL,				/* TX_RENAME */
	libuzfs_replay_write,		/* TX_WRITE */
	libuzfs_replay_truncate,	/* TX_TRUNCATE */
	libuzfs_replay_setmtime,	/* TX_SETATTR */
	NULL,				/* TX_ACL_V0 */
	NULL,				/* TX_ACL */
	NULL,				/* TX_CREATE_ACL */
	NULL,				/* TX_CREATE_ATTR */
	NULL,				/* TX_CREATE_ACL_ATTR */
	NULL,				/* TX_MKDIR_ACL */
	NULL,				/* TX_MKDIR_ATTR */
	NULL,				/* TX_MKDIR_ACL_ATTR */
	libuzfs_replay_write2,				/* TX_WRITE2 */
};

static void
libuzfs_get_done(zgd_t *zgd, int error)
{
	libuzfs_inode_handle_t *ihp = zgd->zgd_private;
	if (zgd->zgd_db) {
		dmu_buf_rele(zgd->zgd_db, zgd);
	}

	zfs_rangelock_exit(zgd->zgd_lr);
	libuzfs_inode_handle_rele(ihp);
	kmem_free(zgd, sizeof (zgd_t));
}

static int
libuzfs_get_data(void *arg, uint64_t gen, lr_write_t *lr, char *buf,
    struct lwb *lwb, zio_t *zio)
{
	libuzfs_dataset_handle_t *dhp = arg;
	uint64_t object = lr->lr_foid;
	uint64_t offset = lr->lr_offset;
	uint64_t size = lr->lr_length;
	libuzfs_inode_handle_t *ihp = NULL;
	int err = libuzfs_inode_handle_get(dhp, B_TRUE,
	    object, gen, &ihp);
	if (err != 0) {
		return (err);
	}

	ASSERT3P(lwb, !=, NULL);
	ASSERT3P(zio, !=, NULL);
	ASSERT3U(size, !=, 0);

	zgd_t *zgd = (zgd_t *)kmem_zalloc(sizeof (zgd_t), KM_SLEEP);
	zgd->zgd_private = ihp;
	zgd->zgd_lwb = lwb;

	objset_t *os = dhp->os;
	if (buf != NULL) {	/* immediate write */
		zgd->zgd_lr = zfs_rangelock_enter(&ihp->rl,
		    offset, size, RL_READER);
		dmu_buf_impl_t *db = (dmu_buf_impl_t *)sa_get_db(ihp->sa_hdl);
		DB_DNODE_ENTER(db);
		dnode_t *dn = DB_DNODE(db);
		DB_DNODE_EXIT(db);
		err = dmu_read_by_dnode(dn, offset, size, lr + 1,
		    DMU_READ_NO_PREFETCH | DMU_READ_NO_SKIP);
	} else {	/* indirect write */
		// considering that the blksz of this object may change, we need
		// to try many times until the blksz is what we loaded
		for (;;) {
			size = ihp->u_blksz;
			// not power of 2 means only one block
			uint64_t blkoff = ISP2(size) ?
			    P2PHASE(offset, size): offset;
			offset -= blkoff;
			zgd->zgd_lr = zfs_rangelock_enter(&ihp->rl, offset,
			    size, RL_READER);
			if (ihp->u_blksz == size) {
				break;
			}
			offset += blkoff;
			zfs_rangelock_exit(zgd->zgd_lr);
		}

		if (lr->lr_offset >= ihp->u_size) {
			err = SET_ERROR(ENOENT);
		}

		dmu_buf_t *db;
		if (err == 0) {
			err = dmu_buf_hold(os, object, offset, zgd,
			    &db, DMU_READ_NO_PREFETCH);
		}

		if (err == 0) {
			blkptr_t *bp = &lr->lr_blkptr;
			zgd->zgd_db = db;
			zgd->zgd_bp = bp;
			ASSERT3U(db->db_offset, ==, offset);
			ASSERT3U(db->db_size, ==, size);
			err = dmu_sync(zio, lr->lr_common.lrc_txg,
			    libuzfs_get_done, zgd);
			ASSERT(err || lr->lr_length <= ihp->u_blksz);

			if (err == 0) {
				return (0);
			}

			if (err == EALREADY) {
				lr->lr_common.lrc_txtype = TX_WRITE2;
				/*
				 * TX_WRITE2 relies on the data previously
				 * written by the TX_WRITE that caused
				 * EALREADY.  We zero out the BP because
				 * it is the old, currently-on-disk BP.
				 */
				zgd->zgd_bp = NULL;
				BP_ZERO(bp);
				err = 0;
			}
		}
	}

	libuzfs_get_done(zgd, err);

	return (err);
}

void
libuzfs_init(void)
{
	kernel_init(SPA_MODE_READ | SPA_MODE_WRITE);
}

void
libuzfs_fini(void)
{
	kernel_fini();
	if (change_zpool_cache_path) {
		free(spa_config_path);
	}
}

void
libuzfs_set_zpool_cache_path(const char *zpool_cache)
{
	spa_config_path = strndup(zpool_cache, MAXPATHLEN);
	change_zpool_cache_path = B_TRUE;
}

// for now, only support one device per pool
int
libuzfs_zpool_create(const char *zpool, const char *path, nvlist_t *props,
    nvlist_t *fsprops)
{
	int err = 0;
	nvlist_t *nvroot = NULL;

	nvroot = make_vdev_root(path, NULL, zpool, 0, 0, NULL, 1, 0, 1);

	props = fnvlist_alloc();
	fnvlist_add_uint64(props, zpool_prop_to_name(ZPOOL_PROP_AUTOTRIM), 1);
	fnvlist_add_uint64(props, zpool_prop_to_name(ZPOOL_PROP_FAILUREMODE),
	    ZIO_FAILURE_MODE_PANIC);
	err = spa_create(zpool, nvroot, props, NULL, NULL);
	fnvlist_free(props);
	if (err) {
		goto out;
	}

out:
	fnvlist_free(nvroot);
	return (err);
}

int
libuzfs_zpool_destroy(const char *zpool)
{
	return (spa_destroy(zpool));
}

libuzfs_zpool_handle_t *
libuzfs_zpool_open(const char *zpool, int *err)
{
	spa_t *spa = NULL;

	*err = spa_open(zpool, &spa, FTAG);
	if (*err)
		return (NULL);

	libuzfs_zpool_handle_t *zhp;
	zhp = umem_alloc(sizeof (libuzfs_zpool_handle_t), UMEM_NOFAIL);
	zhp->spa = spa;
	(void) strlcpy(zhp->zpool_name, zpool, sizeof (zhp->zpool_name));

	return (zhp);
}

void
libuzfs_zpool_close(libuzfs_zpool_handle_t *zhp)
{
	spa_close(zhp->spa, FTAG);
	free(zhp);
}

#ifdef ZFS_DEBUG
extern int fail_percent;
#endif

void
libuzfs_set_fail_percent(int fp)
{
#ifdef ZFS_DEBUG
	fail_percent = fp;
#endif
}

int
libuzfs_zpool_import(const char *dev_path, char *pool_name, int size)
{
	/*
	 * Preferentially open using O_DIRECT to bypass the block device
	 * cache which may be stale for multipath devices.  An EINVAL errno
	 * indicates O_DIRECT is unsupported so fallback to just O_RDONLY.
	 */
	int fd = open(dev_path, O_RDONLY | O_DIRECT | O_CLOEXEC);
#ifdef ZFS_DEBUG
	if (rand() % 100 < fail_percent) {
		close(fd);
		errno = EIO;
		fd = -1;
	}
#endif
	if (fd < 0) {
		zfs_dbgmsg("open dev_path %s failed, err: %d", dev_path, errno);
		if (errno == ENOENT) {
			return (ENODEV);
		} else {
			return (errno);
		}
	}

	nvlist_t *leaf_config = NULL;
	nvlist_t *pools = NULL;
	int num_labels = 0;
	int err = zpool_read_label_secure(fd, &leaf_config, &num_labels);
	if (err != 0) {
		VERIFY(err != ENOENT);
		zfs_dbgmsg("read label error: %d", err);
		goto out;
	}

	zfs_dbgmsg("got %d labels from %s", num_labels, dev_path);
	if (num_labels == 0) {
		err = ENOENT;
		goto out;
	}

	pools = zpool_leaf_to_pools(leaf_config, num_labels, dev_path);
	nvpair_t *elem = nvlist_next_nvpair(pools, NULL);
	VERIFY3U(elem, !=, NULL);
	VERIFY3U(nvlist_next_nvpair(pools, elem), ==, NULL);

	nvlist_t *root_config = NULL;
	VERIFY0(nvpair_value_nvlist(elem, &root_config));
	flockfile(stdout);
	nvlist_print(stdout, root_config);
	funlockfile(stdout);

	char *stored_pool_name;
	VERIFY0(nvlist_lookup_string(root_config,
	    ZPOOL_CONFIG_POOL_NAME, &stored_pool_name));
	int name_len = strlen(stored_pool_name);
	if (name_len >= size) {
		err = ERANGE;
		goto out;
	}
	strncpy(pool_name, stored_pool_name, name_len);
	pool_name[name_len] = '\0';

	err = spa_import(pool_name, root_config, NULL, ZFS_IMPORT_NORMAL);
	VERIFY(err != ENOENT);

out:
	close(fd);
	nvlist_free(leaf_config);
	nvlist_free(pools);

	return (err);
}

int
libuzfs_zpool_export(const char *pool_name)
{
	zfs_dbgmsg("exporting zpool %s", pool_name);
	int err = spa_export(pool_name, NULL, B_TRUE, B_FALSE);
	zfs_dbgmsg("exported zpool %s", pool_name);

	if (err == ENOENT) {
		err = 0;
	}

	return (err);
}

int
libuzfs_dataset_expand(libuzfs_dataset_handle_t *dhp)
{
	spa_t *spa = dhp->os->os_spa;
	vdev_t *root_vdev = spa->spa_root_vdev;
	for (int i = 0; i < root_vdev->vdev_children; ++i) {
		vdev_t *leaf_vdev = root_vdev->vdev_child[i];
		vdev_state_t newstate;

		int err = vdev_online(spa, leaf_vdev->vdev_guid,
		    ZFS_ONLINE_EXPAND, &newstate);
		if (err != 0) {
			return (err);
		}
	}

	return (0);
}

void
libuzfs_zpool_prop_set(libuzfs_zpool_handle_t *zhp, zpool_prop_t prop,
    uint64_t value)
{
	libuzfs_spa_prop_set_uint64(zhp->spa, prop, value);
}

int
libuzfs_zpool_prop_get(libuzfs_zpool_handle_t *zhp, zpool_prop_t prop,
    uint64_t *value)
{
	int err = 0;
	nvlist_t *props = NULL;
	nvlist_t *propval = NULL;

	VERIFY0(spa_prop_get(zhp->spa, &props));

	err = nvlist_lookup_nvlist(props, zpool_prop_to_name(prop), &propval);
	if (err) {
		goto out;
	}

	*value = fnvlist_lookup_uint64(propval, ZPROP_VALUE);

out:
	fnvlist_free(props);
	return (err);
}

static void
libuzfs_objset_create_cb(objset_t *os, void *arg, cred_t *cr, dmu_tx_t *tx)
{
	spa_feature_enable(os->os_spa, SPA_FEATURE_LARGE_BLOCKS, tx);
	spa_feature_enable(os->os_spa, SPA_FEATURE_LARGE_DNODE, tx);

	VERIFY0(zap_create_claim(os, MASTER_NODE_OBJ, DMU_OT_MASTER_NODE,
	    DMU_OT_NONE, 0, tx));

	uint64_t sa_obj = zap_create(os, DMU_OT_SA_MASTER_NODE,
	    DMU_OT_NONE, 0, tx);
	VERIFY0(zap_add(os, MASTER_NODE_OBJ, ZFS_SA_ATTRS, 8, 1, &sa_obj, tx));

	libuzfs_dataset_handle_t *dhp = umem_zalloc(
	    sizeof (libuzfs_dataset_handle_t), UMEM_NOFAIL);
	dhp->os = os;
	dhp->dnodesize = 1024;
	for (int i = 0; i < NUM_NODE_BUCKETS; ++i) {
		mutex_init(&dhp->objs_lock[i], NULL, MUTEX_DEFAULT, NULL);
	}
	libuzfs_setup_dataset_sa(dhp);
	uint64_t sb_obj = 0;
	libuzfs_inode_handle_t *ihp = libuzfs_create_inode_with_type_impl(
	    dhp, &sb_obj, B_FALSE, INODE_DIR, tx, 0);
	libuzfs_inode_handle_rele(ihp);
	VERIFY0(zap_add(os, MASTER_NODE_OBJ, UZFS_SB_OBJ, 8, 1, &sb_obj, tx));
	sa_tear_down(dhp->os);
	umem_free(dhp, sizeof (libuzfs_dataset_handle_t));
}

int
libuzfs_dataset_create(const char *dsname)
{
	return (dmu_objset_create(dsname, DMU_OST_ZFS, 0, NULL,
	    libuzfs_objset_create_cb, NULL));
}

static int
libuzfs_objset_destroy_cb(const char *name, void *arg)
{
	int err = 0;

	/*
	 * Destroy the dataset.
	 */
	if (strchr(name, '@') != NULL) {
		VERIFY0(dsl_destroy_snapshot(name, B_TRUE));
	} else {
		err = dsl_destroy_head(name);
		if (err != EBUSY) {
			/* There could be a hold on this dataset */
			ASSERT0(err);
		}
	}
	return (0);
}

void
libuzfs_dataset_destroy(const char *dsname)
{
	(void) dmu_objset_find(dsname, libuzfs_objset_destroy_cb, NULL,
	    DS_FIND_SNAPSHOTS | DS_FIND_CHILDREN);
}

static int
uzfs_get_file_info(dmu_object_type_t bonustype, const void *data,
    zfs_file_info_t *zoi)
{
	if (bonustype != DMU_OT_SA)
		return (SET_ERROR(ENOENT));

	zoi->zfi_user = 0;
	zoi->zfi_group = 0;
	zoi->zfi_generation = 0;
	return (0);
}

static void
libuzfs_dhp_init(libuzfs_dataset_handle_t *dhp, objset_t *os,
    uint64_t dnodesize)
{
	dhp->os = os;
	dmu_objset_name(os, dhp->name);

	dmu_objset_register_type(DMU_OST_ZFS, uzfs_get_file_info);
	libuzfs_setup_dataset_sa(dhp);

	for (int i = 0; i < NUM_NODE_BUCKETS; ++i) {
		mutex_init(&dhp->objs_lock[i], NULL, 0, NULL);
	}

	dhp->zilog = zil_open(os, libuzfs_get_data);
	dhp->dnodesize = dnodesize;
	zil_replay(os, dhp, libuzfs_replay_vector);

	VERIFY0(zap_lookup(os, MASTER_NODE_OBJ, UZFS_SB_OBJ, 8, 1,
	    &dhp->sb_ino));
}

static void
libuzfs_dhp_fini(libuzfs_dataset_handle_t *dhp)
{
	for (int i = 0; i < NUM_NODE_BUCKETS; ++i) {
		mutex_destroy(&dhp->objs_lock[i]);
	}
}

libuzfs_dataset_handle_t *
libuzfs_dataset_open(const char *dsname, int *err,
    uint32_t dnodesize, uint32_t max_blksz)
{
	libuzfs_dataset_handle_t *dhp = NULL;
	objset_t *os = NULL;

	dhp = umem_zalloc(sizeof (libuzfs_dataset_handle_t), UMEM_NOFAIL);
	dhp->max_blksz = max_blksz == 0 ? UZFS_DEFAULT_BLOCKSIZE : max_blksz;

	*err = libuzfs_dmu_objset_own(dsname, DMU_OST_ZFS, B_FALSE, B_TRUE,
	    dhp, &os);
	if (*err) {
		umem_free(dhp, sizeof (libuzfs_dataset_handle_t));
		return (NULL);
	}

	libuzfs_dhp_init(dhp, os, dnodesize);

	return (dhp);
}

void
libuzfs_dataset_close(libuzfs_dataset_handle_t *dhp)
{
	zil_close(dhp->zilog);
	if (dhp->os->os_sa != NULL) {
		sa_tear_down(dhp->os);
	}
	dmu_objset_disown(dhp->os, B_TRUE, dhp);
	libuzfs_dhp_fini(dhp);
	umem_free(dhp, sizeof (libuzfs_dataset_handle_t));
}

uint64_t
libuzfs_dataset_get_superblock_ino(libuzfs_dataset_handle_t *dhp)
{
	return (dhp->sb_ino);
}

int
libuzfs_object_stat(libuzfs_dataset_handle_t *dhp, uint64_t obj,
    dmu_object_info_t *doi)
{
	int err = 0;
	dmu_buf_t *db = NULL;
	objset_t *os = dhp->os;

	err = dmu_bonus_hold(os, obj, FTAG, &db);
	if (err)
		return (err);

	dmu_object_info_from_db(db, doi);

	dmu_buf_rele(db, FTAG);
	return (0);
}

void
libuzfs_object_sync(libuzfs_inode_handle_t *ihp)
{
	zil_commit(ihp->dhp->zilog, ihp->ino);
}

int
libuzfs_object_truncate(libuzfs_inode_handle_t *ihp,
    uint64_t offset, uint64_t size)
{
	// FIXME(hping): only support truncate now, thus offset is always 0,
	// support fallocate in future, in which case offset could be non-zero
	if (offset != 0)
		return (EINVAL);

	zfs_locked_range_t *lr = zfs_rangelock_enter(&ihp->rl,
	    0, UINT64_MAX, RL_WRITER);
	int err = libuzfs_object_truncate_impl(ihp, offset, size);
	zfs_rangelock_exit(lr);
	if (err == 0) {
		zil_commit(ihp->dhp->zilog, ihp->ino);
	}

	return (err);
}

uint64_t
libuzfs_get_max_synced_opid(libuzfs_dataset_handle_t *dhp)
{
	return (dsl_dataset_get_max_synced_opid(dmu_objset_ds(dhp->os)));
}

void
libuzfs_dump_txg_opids(libuzfs_dataset_handle_t *dhp)
{
	dsl_dataset_dump_txg_opids(dmu_objset_ds(dhp->os));
}

uint64_t
libuzfs_get_last_synced_txg(libuzfs_dataset_handle_t *dhp)
{
	return (spa_last_synced_txg(dhp->os->os_spa));
}

void
libuzfs_wait_synced(libuzfs_dataset_handle_t *dhp)
{
	txg_wait_synced(spa_get_dsl(dhp->os->os_spa), 0);
}

static libuzfs_inode_handle_t *
libuzfs_create_inode_with_type_impl(libuzfs_dataset_handle_t *dhp,
    uint64_t *obj, boolean_t claiming, libuzfs_inode_type_t type,
    dmu_tx_t *tx, uint64_t gen)
{
	// create/claim object
	objset_t *os = dhp->os;
	int dnodesize = dhp->dnodesize;
	int bonuslen = DN_BONUS_SIZE(dnodesize);

	dnode_t *dn = NULL;
	if (type == INODE_FILE || type == INODE_DATA_OBJ) {
		if (claiming) {
			ASSERT(*obj != 0);
			VERIFY0(dmu_object_claim_dnsize(os, *obj,
			    DMU_OT_PLAIN_FILE_CONTENTS,
			    0, DMU_OT_SA, bonuslen, dnodesize, tx));
			VERIFY0(dnode_hold(os, *obj, FTAG, &dn));
		} else {
			*obj = dmu_object_alloc_hold(os,
			    DMU_OT_PLAIN_FILE_CONTENTS,
			    0, 0, DMU_OT_SA,
			    bonuslen, dnodesize, &dn, FTAG, tx);
		}
		if (type == INODE_DATA_OBJ) {
			VERIFY0(dnode_set_blksz(dn, 0, 0, tx));
			if (!claiming) {
				libuzfs_log_create(dhp->zilog, tx, *obj);
			}
		}
	} else if (type == INODE_DIR) {
		if (claiming) {
			ASSERT(*obj != 0);
			VERIFY0(zap_create_claim_dnsize(os, *obj,
			    DMU_OT_DIRECTORY_CONTENTS, DMU_OT_SA,
			    bonuslen, dnodesize, tx));
			VERIFY0(dnode_hold(os, *obj, FTAG, &dn));
		} else {
			*obj = zap_create_impl(os, 0, 0,
			    DMU_OT_DIRECTORY_CONTENTS,
			    0, 0, DMU_OT_SA,
			    bonuslen, dnodesize, &dn, FTAG, tx);
		}
	} else {
		VERIFY0(1);
	}

	if (!claiming) {
		gen = tx->tx_txg;
	}

	libuzfs_inode_handle_t *ihp = umem_zalloc(
	    sizeof (libuzfs_inode_handle_t), UMEM_NOFAIL);
	// sa_hdl will be filled in sa_handle_get_from_db
	ihp->dhp = dhp;
	rw_init(&ihp->hp_kvattr_cache_lock, NULL, RW_DEFAULT, NULL);
	// hp_kvattr_cache will be filled in libuzfs_inode_attr_init
	ihp->ino = *obj;
	ihp->rc = 1;
	ihp->gen = gen;
	ihp->is_data_inode = type == INODE_DATA_OBJ;
	if (ihp->is_data_inode) {
		ihp->u_size = 0;
		ihp->u_blksz = dn->dn_datablksz;
		zfs_rangelock_init(&ihp->rl, libuzfs_rangelock_cb, ihp);
	}

	kmutex_t *mp = &dhp->objs_lock[(*obj) % NUM_NODE_BUCKETS];
	mutex_enter(mp);
	dmu_buf_t *bonus = NULL;
	VERIFY0(dmu_bonus_hold_by_dnode(dn, ihp,
	    &bonus, DMU_READ_NO_PREFETCH));
	VERIFY0(sa_handle_get_from_db(os, bonus,
	    ihp, SA_HDL_SHARED, &ihp->sa_hdl));
	libuzfs_inode_attr_init(ihp, tx);
	dnode_rele(dn, FTAG);
	mutex_exit(mp);

	return (ihp);
}

static int
libuzfs_create_inode_with_type(libuzfs_dataset_handle_t *dhp, uint64_t *obj,
    boolean_t claiming, libuzfs_inode_type_t type, uint64_t gen,
    libuzfs_inode_handle_t **ihpp)
{
	objset_t *os = dhp->os;
	dmu_tx_t *tx = dmu_tx_create(os);
	dmu_tx_hold_sa_create(tx, sizeof (uzfs_inode_attr_t));
	if (type == INODE_DIR) {
		dmu_tx_hold_zap(tx, DMU_NEW_OBJECT, B_TRUE, NULL);
	}
	int err = dmu_tx_assign(tx, TXG_WAIT);
	if (err != 0) {
		dmu_tx_abort(tx);
		return (err);
	}

	*ihpp = libuzfs_create_inode_with_type_impl(dhp, obj,
	    claiming, type, tx, gen);

	dmu_tx_commit(tx);

	return (0);
}

extern int zfs_flags;

extern void
libuzfs_enable_debug_msg(void)
{
	zfs_flags |= ZFS_DEBUG_DPRINTF;
}

extern void
libuzfs_disable_debug_msg(void)
{
	zfs_flags ^= ZFS_DEBUG_DPRINTF;
}

/*
 * object creation is always SYNC, recorded in zil
 */
int
libuzfs_objects_create(libuzfs_dataset_handle_t *dhp, uint64_t *objs,
    int num_objs, uint64_t *gen)
{
	objset_t *os = dhp->os;
	// objects creation needs to be in 1 tx to ensure atomicity
	dmu_tx_t *tx = dmu_tx_create(os);
	dmu_tx_hold_sa_create(tx, sizeof (uzfs_object_attr_t));
	int err = dmu_tx_assign(tx, TXG_WAIT);
	if (err != 0) {
		dmu_tx_abort(tx);
		return (err);
	}

	for (int i = 0; i < num_objs; ++i) {
		libuzfs_inode_handle_t *ihp =
		    libuzfs_create_inode_with_type_impl(dhp, &objs[i],
		    B_FALSE, INODE_DATA_OBJ, tx, 0);
		libuzfs_inode_handle_rele(ihp);
	}

	*gen = tx->tx_txg;

	dmu_tx_commit(tx);

	for (int i = 0; i < num_objs; ++i) {
		zil_submit(dhp->zilog, objs[i]);
	}

	return (0);
}

void
libuzfs_wait_log_commit(libuzfs_dataset_handle_t *dhp)
{
	zil_wait_commit(dhp->zilog);
}

/*
 * object deletion need not be SYNC, so the caller need to call
 * libuzfs_wait_log_commit to wait log commit
 */
int
libuzfs_object_delete(libuzfs_inode_handle_t *ihp)
{
	libuzfs_dataset_handle_t *dhp = ihp->dhp;
	// call dmu_free_long_range first to make dirty data in
	// libuzfs_inode_delete tx as small as possible. if the
	// object is too large, libuzfs_inode_delete may result
	// in much dirty data, which will block the task forever,
	// dmu_free_long_range will split this delete into many
	// small transactions,
	int err = dmu_free_long_range(dhp->os, ihp->ino, 0, DMU_OBJECT_END);
	if (err != 0) {
		return (err);
	}

	err = libuzfs_inode_delete(ihp, INODE_DATA_OBJ, NULL);
	if (err == 0) {
		zil_submit(dhp->zilog, ihp->ino);
	}

	return (err);
}

static int
libuzfs_object_claim(libuzfs_dataset_handle_t *dhp, uint64_t obj,
    uint64_t gen, libuzfs_inode_type_t type)
{
	objset_t *os = dhp->os;
	int dnodesize = dhp->dnodesize;
	int err = dnode_try_claim(os, obj, dnodesize >> DNODE_SHIFT);
	libuzfs_inode_handle_t *ihp = NULL;

	// FIXME(hping): double comfirm the waived error codes
	if (err == ENOSPC || err == EEXIST) {
		dnode_t *dn;
		err = dnode_hold(dhp->os, obj, FTAG, &dn);
		if (err == ENOENT) {
			ASSERT(type != INODE_DATA_OBJ);
			zfs_dbgmsg("object %lu is being deleted, "
			    "wait txg sync..", obj);
			libuzfs_wait_synced(dhp);
			goto do_claim;
		} else if (err == 0) {
			dprintf("object %lu already created, type: %d",
			    obj, dn->dn_type);
			dnode_rele(dn, FTAG);
		}
		return (err);
	} else if (err != 0) {
		return (err);
	}

do_claim:
	err = libuzfs_create_inode_with_type(dhp,
	    &obj, B_TRUE, type, gen, &ihp);
	if (err == 0) {
		libuzfs_inode_handle_rele(ihp);
	}

	return (err);
}

uint64_t
libuzfs_object_list(libuzfs_dataset_handle_t *dhp)
{
	int err = 0;
	uint64_t i = 0;
	uint64_t obj = 0;
	objset_t *os = dhp->os;
	dmu_object_info_t doi;

	memset(&doi, 0, sizeof (doi));

	for (obj = 0; err == 0; err = dmu_object_next(os, &obj, B_FALSE, 0)) {
		if (libuzfs_object_stat(dhp, obj, &doi)) {
			zfs_dbgmsg("skip obj w/o bonus buf: %ld", obj);
			continue;
		} else {
			zfs_dbgmsg("object: %ld", obj);
		}
		i++;
	}

	return (i);
}

static inline int
libuzfs_object_write_impl(libuzfs_inode_handle_t *ihp,
    uint64_t offset, struct iovec *iovs, int iov_cnt,
    boolean_t sync, uint64_t replay_eof)
{
	uint64_t size = 0;
	for (int i = 0; i < iov_cnt; ++i) {
		size += iovs[i].iov_len;
	}

	zfs_uio_t uio;
	zfs_uio_iovec_init(&uio, iovs, iov_cnt,
	    offset, UIO_USERSPACE, size, 0);

	zfs_locked_range_t *lr = zfs_rangelock_enter(&ihp->rl,
	    offset, size, RL_WRITER);

	ASSERT(lr != NULL);

	libuzfs_dataset_handle_t *dhp = ihp->dhp;
	sa_attr_type_t *sa_tbl = dhp->uzfs_attr_table;
	const uint64_t max_blksz = dhp->max_blksz;

	sa_bulk_attr_t bulk[2];
	int count = 0;
	struct timespec mtime;
	SA_ADD_BULK_ATTR(bulk, count, sa_tbl[UZFS_SIZE],
	    NULL, &ihp->u_size, sizeof (ihp->u_size));
	SA_ADD_BULK_ATTR(bulk, count, sa_tbl[UZFS_MTIME],
	    NULL, &mtime, sizeof (mtime));
	objset_t *os = dhp->os;
	int err = 0;
	while ((size = zfs_uio_resid(&uio)) > 0) {
		offset = zfs_uio_offset(&uio);
		uint64_t nwrite = MIN(size,
		    max_blksz - P2PHASE(offset, max_blksz));
		dmu_tx_t *tx = dmu_tx_create(os);
		dmu_tx_hold_sa(tx, ihp->sa_hdl, FALSE);
		dmu_buf_impl_t *db = (dmu_buf_impl_t *)sa_get_db(ihp->sa_hdl);
		DB_DNODE_ENTER(db);
		dmu_tx_hold_write_by_dnode(tx, DB_DNODE(db), offset, nwrite);
		DB_DNODE_EXIT(db);
		if ((err = dmu_tx_assign(tx, TXG_WAIT)) != 0) {
			dmu_tx_abort(tx);
			break;
		}

		// update max blocksize when current blksz too small
		if (lr->lr_length == UINT64_MAX) {
			uint64_t new_blksz = 0;
			uint64_t end_size = MAX(ihp->u_size, size + offset);
			if (ihp->u_blksz > max_blksz) {
				ASSERT(!ISP2(ihp->u_blksz));
				new_blksz = 1<<highbit64(ihp->u_blksz);
				new_blksz = MIN(new_blksz, end_size);
			} else {
				new_blksz = MIN(max_blksz, end_size);
			}

			libuzfs_grow_blocksize(ihp, new_blksz, tx);
			DB_DNODE_ENTER(db);
			ihp->u_blksz = DB_DNODE(db)->dn_datablksz;
			DB_DNODE_EXIT(db);
			zfs_rangelock_reduce(lr, offset, size);
		}

		VERIFY0(dmu_write_uio_dbuf(sa_get_db(ihp->sa_hdl),
		    &uio, nwrite, tx));

		if (replay_eof == 0) {
			uint64_t up_size = 0;
			uint64_t eod = offset + nwrite;
			while ((up_size = atomic_load_64(&ihp->u_size)) < eod) {
				atomic_cas_64(&ihp->u_size, up_size, eod);
			}
		} else if (ihp->u_size < replay_eof) {
			// this is a replay write, use replay_eof as file size
			ihp->u_size = replay_eof;
		}

		gethrestime(&mtime);
		VERIFY0(sa_bulk_update(ihp->sa_hdl, bulk, 2, tx));

		libuzfs_log_write(ihp, offset, nwrite, sync, tx);
		dmu_tx_commit(tx);
	}

	zfs_rangelock_exit(lr);

	if (sync && err == 0) {
		zil_commit(dhp->zilog, ihp->ino);
	}

	return (err);
}

int
libuzfs_object_write(libuzfs_inode_handle_t *ihp, uint64_t offset,
    struct iovec *iovs, int iov_cnt, boolean_t sync)
{
	return (libuzfs_object_write_impl(ihp, offset,
	    iovs, iov_cnt, sync, 0));
}

int
libuzfs_object_read(libuzfs_inode_handle_t *ihp,
    uint64_t offset, uint64_t size, char *buf)
{
	zfs_locked_range_t *lr = zfs_rangelock_enter(&ihp->rl,
	    offset, size, RL_READER);
	ASSERT(lr != NULL);
	int rc = 0;
	if (offset >= ihp->u_size) {
		goto out;
	}

	int read_size = MIN(ihp->u_size - offset, size);

	dmu_buf_impl_t *db = (dmu_buf_impl_t *)sa_get_db(ihp->sa_hdl);
	DB_DNODE_ENTER(db);
	dnode_t *dn = DB_DNODE(db);
	int err = dmu_read_by_dnode(dn, offset, read_size, buf,
	    DMU_READ_NO_PREFETCH | DMU_READ_NO_SKIP);
	DB_DNODE_EXIT(db);
	if (err != 0) {
		rc = -err;
	} else {
		rc = read_size;
	}

out:
	zfs_rangelock_exit(lr);
	return (rc);
}

libuzfs_zap_iterator_t *
libuzfs_new_zap_iterator(libuzfs_dataset_handle_t *dhp, uint64_t obj, int *err)
{
	libuzfs_zap_iterator_t *iter =
	    umem_alloc(sizeof (libuzfs_zap_iterator_t), UMEM_NOFAIL);
	zap_cursor_init(&iter->zc, dhp->os, obj);

	if ((*err = zap_cursor_retrieve(&iter->zc, &iter->za)) != 0) {
		zap_cursor_fini(&iter->zc);
		umem_free(iter, sizeof (libuzfs_zap_iterator_t));
		return (NULL);
	}

	return (iter);
}

int
libuzfs_zap_iterator_advance(libuzfs_zap_iterator_t *iter)
{
	zap_cursor_advance(&iter->zc);
	return (zap_cursor_retrieve(&iter->zc, &iter->za));
}

ssize_t
libuzfs_zap_iterator_name(libuzfs_zap_iterator_t *iter, char *name, size_t size)
{
	int name_len = strlen(iter->za.za_name);
	if (size < name_len + 1) {
		return (-ERANGE);
	}

	strncpy(name, iter->za.za_name, name_len);

	return (name_len);
}

size_t
libuzfs_zap_iterator_value_size(libuzfs_zap_iterator_t *iter)
{
	return (iter->za.za_integer_length * iter->za.za_num_integers);
}

void
libuzfs_zap_iterator_fini(libuzfs_zap_iterator_t *iter)
{
	if (iter != NULL) {
		zap_cursor_fini(&iter->zc);
		umem_free(iter, sizeof (libuzfs_zap_iterator_t));
	}
}

int
libuzfs_zap_create(libuzfs_dataset_handle_t *dhp, uint64_t *obj, uint64_t *txg)
{
	int err = 0;
	dmu_tx_t *tx = NULL;
	objset_t *os = dhp->os;

	tx = dmu_tx_create(os);

	dmu_tx_hold_zap(tx, DMU_NEW_OBJECT, B_TRUE, NULL);

	err = dmu_tx_assign(tx, TXG_WAIT);
	if (err) {
		dmu_tx_abort(tx);
		goto out;
	}

	int dnodesize = dhp->dnodesize;
	int bonuslen = DN_BONUS_SIZE(dnodesize);

	*obj = zap_create_dnsize(os, DMU_OT_DIRECTORY_CONTENTS,
	    DMU_OT_PLAIN_OTHER, bonuslen, dnodesize, tx);

	*txg = tx->tx_txg;
	dmu_tx_commit(tx);

out:
	return (err);
}

int
libuzfs_zap_claim(libuzfs_dataset_handle_t *dhp, uint64_t obj)
{
	int err = 0;
	dmu_tx_t *tx = NULL;
	objset_t *os = dhp->os;

	tx = dmu_tx_create(os);

	dmu_tx_hold_zap(tx, DMU_NEW_OBJECT, B_TRUE, NULL);

	err = dmu_tx_assign(tx, TXG_WAIT);
	if (err) {
		dmu_tx_abort(tx);
		goto out;
	}

	int dnodesize = dhp->dnodesize;
	int bonuslen = DN_BONUS_SIZE(dnodesize);

	VERIFY0(zap_create_claim_dnsize(os, obj, DMU_OT_DIRECTORY_CONTENTS,
	    DMU_OT_PLAIN_OTHER, bonuslen, dnodesize, tx));

	dmu_tx_commit(tx);

out:
	return (err);
}

int
libuzfs_zap_delete(libuzfs_dataset_handle_t *dhp, uint64_t obj, uint64_t *txg)
{
	int err = 0;
	dmu_tx_t *tx = NULL;
	objset_t *os = dhp->os;

	tx = dmu_tx_create(os);

	dmu_tx_hold_free(tx, obj, 0, DMU_OBJECT_END);

	err = dmu_tx_assign(tx, TXG_WAIT);
	if (err) {
		dmu_tx_abort(tx);
		goto out;
	}

	VERIFY0(zap_destroy(os, obj, tx));

	*txg = tx->tx_txg;
	dmu_tx_commit(tx);

out:
	return (err);
}

int
libuzfs_zap_add(libuzfs_dataset_handle_t *dhp, uint64_t obj, const char *key,
    int integer_size, uint64_t num_integers, const void *val, uint64_t *txg)
{
	int err = 0;
	dmu_tx_t *tx = NULL;
	objset_t *os = dhp->os;

	tx = dmu_tx_create(os);

	dmu_tx_hold_zap(tx, obj, B_TRUE, NULL);

	err = dmu_tx_assign(tx, TXG_WAIT);
	if (err) {
		dmu_tx_abort(tx);
		goto out;
	}

	err = zap_add(os, obj, key, integer_size, num_integers, val, tx);

	*txg = tx->tx_txg;
	dmu_tx_commit(tx);

out:
	return (err);
}

int
libuzfs_zap_remove(libuzfs_dataset_handle_t *dhp, uint64_t obj, const char *key,
    uint64_t *txg)
{
	int err = 0;
	dmu_tx_t *tx = NULL;
	objset_t *os = dhp->os;

	tx = dmu_tx_create(os);

	dmu_tx_hold_zap(tx, obj, B_TRUE, NULL);

	err = dmu_tx_assign(tx, TXG_WAIT);
	if (err) {
		dmu_tx_abort(tx);
		goto out;
	}

	err = zap_remove(os, obj, key, tx);

	*txg = tx->tx_txg;
	dmu_tx_commit(tx);

out:
	return (err);
}

int
libuzfs_zap_update(libuzfs_dataset_handle_t *dhp, uint64_t obj, const char *key,
    int integer_size, uint64_t num_integers, const void *val, uint64_t *txg)
{
	int err = 0;
	dmu_tx_t *tx = NULL;
	objset_t *os = dhp->os;

	tx = dmu_tx_create(os);

	dmu_tx_hold_zap(tx, obj, B_TRUE, NULL);

	err = dmu_tx_assign(tx, TXG_WAIT);
	if (err) {
		dmu_tx_abort(tx);
		goto out;
	}

	err = zap_update(os, obj, key, integer_size, num_integers, val, tx);

	*txg = tx->tx_txg;
	dmu_tx_commit(tx);

out:
	return (err);
}

int
libuzfs_zap_lookup(libuzfs_dataset_handle_t *dhp, uint64_t obj, const char *key,
    int integer_size, uint64_t num_integers, void *val)
{
	return (zap_lookup(dhp->os, obj, key, integer_size, num_integers, val));
}

int
libuzfs_zap_count(libuzfs_dataset_handle_t *dhp, uint64_t obj, uint64_t *count)
{
	return (zap_count(dhp->os, obj, count));
}

int
libuzfs_inode_delete(libuzfs_inode_handle_t *ihp,
    libuzfs_inode_type_t inode_type, uint64_t *txg)
{
	uint64_t xattr_zap_obj = 0;
	int err = libuzfs_get_xattr_zap_obj(ihp, &xattr_zap_obj);
	if (err != 0 && err != ENOENT) {
		return (err);
	}

	libuzfs_dataset_handle_t *dhp = ihp->dhp;
	objset_t *os = dhp->os;
	dmu_tx_t *tx = dmu_tx_create(os);
	uint64_t ino = ihp->ino;

	dmu_tx_hold_free(tx, ino, 0, DMU_OBJECT_END);
	if (xattr_zap_obj != 0) {
		dmu_tx_hold_free(tx, xattr_zap_obj, 0, DMU_OBJECT_END);
	}

	if ((err = dmu_tx_assign(tx, TXG_WAIT)) == 0) {
		if (xattr_zap_obj != 0) {
			VERIFY0(zap_destroy(os, xattr_zap_obj, tx));
		}

		if (inode_type == INODE_DIR) {
			VERIFY0(zap_destroy(os, ino, tx));
		} else {
			VERIFY0(dmu_object_free(os, ino, tx));
		}

		if (txg != NULL) {
			*txg = tx->tx_txg;
		}

		if (inode_type == INODE_DATA_OBJ) {
			libuzfs_log_remove(dhp->zilog, tx, ino);
		}

		dmu_tx_commit(tx);
	} else {
		dmu_tx_abort(tx);
	}

	return (err);
}

int
libuzfs_inode_create(libuzfs_dataset_handle_t *dhp, uint64_t *ino,
    libuzfs_inode_type_t type, libuzfs_inode_handle_t **ihpp, uint64_t *gen)
{
	int err = libuzfs_create_inode_with_type(dhp, ino,
	    B_FALSE, type, 0, ihpp);
	if (err == 0) {
		*gen = (*ihpp)->gen;
	}

	return (err);
}

int
libuzfs_inode_claim(libuzfs_dataset_handle_t *dhp, uint64_t ino,
    uint64_t gen, libuzfs_inode_type_t type)
{
	return (libuzfs_object_claim(dhp, ino, gen, type));
}

int
libuzfs_inode_get_kvobj(libuzfs_dataset_handle_t *dhp, uint64_t ino,
    uint64_t *kvobj)
{
	dmu_buf_t *db;

	VERIFY0(dmu_bonus_hold(dhp->os, ino, FTAG, &db));
	bcopy(db->db_data, kvobj, sizeof (*kvobj));
	dmu_buf_rele(db, FTAG);

	return (0);
}

int
libuzfs_dentry_create(libuzfs_inode_handle_t *dihp,
    const char *name, uint64_t value, uint64_t *txg)
{
	return (libuzfs_zap_add(dihp->dhp, dihp->ino, name, 8, 1, &value, txg));
}

int
libuzfs_dentry_delete(libuzfs_inode_handle_t *dihp,
    const char *name, uint64_t *txg)
{
	return (libuzfs_zap_remove(dihp->dhp, dihp->ino, name, txg));
}

int
libuzfs_dentry_lookup(libuzfs_inode_handle_t *dihp,
    const char *name, uint64_t *value)
{
	return (libuzfs_zap_lookup(dihp->dhp, dihp->ino, name, 8, 1, value));
}

static boolean_t
dir_emit(dir_emit_ctx_t *ctx, uint64_t whence, uint64_t value, char *name,
    uint32_t name_len)
{
	int size = offsetof(struct uzfs_dentry, name) + name_len + 1;

	// ensure dentry is aligned to 8 bytes to make Rust happy
	size = roundup(size, sizeof (uint64_t));
	if (ctx->cur + size > ctx->buf + ctx->size) {
		return (B_TRUE);
	}

	struct uzfs_dentry *dentry = (struct uzfs_dentry *)ctx->cur;
	dentry->size = size;
	dentry->whence = whence;
	dentry->value = value;
	memcpy(dentry->name, name, name_len + 1);

	ctx->cur += size;

	return (B_FALSE);
}

int
libuzfs_dentry_iterate(libuzfs_inode_handle_t *dihp,
    uint64_t whence, uint32_t size, char *buf, uint32_t *num)
{
	int		error = 0;
	zap_cursor_t	zc;
	zap_attribute_t	zap;
	boolean_t	done = B_FALSE;
	dir_emit_ctx_t  ctx;

	*num = 0;
	ctx.buf = buf;
	ctx.cur = buf;
	ctx.size = size;
	memset(ctx.buf, 0, size);

	libuzfs_dataset_handle_t *dhp = dihp->dhp;
	uint64_t dino = dihp->ino;
	zap_cursor_init_serialized(&zc, dhp->os, dino, whence);

	while (!done) {
		if ((error = zap_cursor_retrieve(&zc, &zap)))
			break;

		/*
		 * Allow multiple entries provided the first entry is
		 * the object id.  Non-zpl consumers may safely make
		 * use of the additional space.
		 *
		 * XXX: This should be a feature flag for compatibility
		 */
		if (zap.za_integer_length != 8 || zap.za_num_integers == 0) {
			zfs_dbgmsg("zap_readdir: bad directory entry,"
			    " obj = %ld, whence = %ld, length = %d, num = %ld",
			    zap.za_first_integer, whence,
			    zap.za_integer_length, zap.za_num_integers);
			error = SET_ERROR(ENXIO);
			break;
		}

		zap_cursor_advance(&zc);
		whence = zap_cursor_serialize(&zc);

		done = dir_emit(&ctx, whence, zap.za_first_integer, zap.za_name,
		    strlen(zap.za_name));
		if (done)
			break;
		(*num)++;
	}

	zap_cursor_fini(&zc);
	if (error == ENOENT) {
		error = 0;
	}
	return (error);
}

// FIXME(hping)
#define	MAX_NUM_FS (100)
static zfsvfs_t *zfsvfs_array[MAX_NUM_FS];
static int zfsvfs_idx = 0;
static znode_t *rootzp = NULL;

static void
libuzfs_vap_init(vattr_t *vap, struct inode *dir, umode_t mode, cred_t *cr)
{
	vap->va_mask = ATTR_MODE;
	vap->va_mode = mode;
	vap->va_uid = crgetfsuid(cr);

	if (dir && dir->i_mode & S_ISGID) {
		vap->va_gid = KGID_TO_SGID(dir->i_gid);
		if (S_ISDIR(mode))
			vap->va_mode |= S_ISGID;
	} else {
		vap->va_gid = crgetfsgid(cr);
	}
}

static void
libuzfs_fs_create_cb(objset_t *os, void *arg, cred_t *cr, dmu_tx_t *tx)
{
	zfs_create_fs(os, NULL, NULL, tx);
}

int
libuzfs_fs_create(const char *fsname)
{
	return (dmu_objset_create(fsname, DMU_OST_ZFS, 0, NULL,
	    libuzfs_fs_create_cb, NULL));
}

void
libuzfs_fs_destroy(const char *fsname)
{
	(void) dmu_objset_find(fsname, libuzfs_objset_destroy_cb, NULL,
	    DS_FIND_SNAPSHOTS | DS_FIND_CHILDREN);
}

int
libuzfs_fs_init(const char *fsname, uint64_t *fsid)
{
	int error = 0;
	vfs_t *vfs = NULL;
	struct super_block *sb = NULL;
	zfsvfs_t *zfsvfs = NULL;

	vfs = kmem_zalloc(sizeof (vfs_t), KM_SLEEP);

	error = zfsvfs_create(fsname, B_FALSE, &zfsvfs);
	if (error) goto out;

	vfs->vfs_data = zfsvfs;
	zfsvfs->z_vfs = vfs;

	sb = kmem_zalloc(sizeof (struct super_block), KM_SLEEP);
	sb->s_fs_info = zfsvfs;

	zfsvfs->z_sb = sb;

	error = zfsvfs_setup(zfsvfs, B_TRUE);
	if (error) goto out;

	*fsid = zfsvfs_idx;

	zfsvfs_array[zfsvfs_idx++] = zfsvfs;

	return (0);

out:
	if (sb)
		kmem_free(sb, sizeof (struct super_block));
	if (vfs)
		kmem_free(vfs, sizeof (vfs_t));
	if (zfsvfs)
		kmem_free(zfsvfs, sizeof (zfsvfs_t));
	return (-1);
}

int
libuzfs_fs_fini(uint64_t fsid)
{
	zfsvfs_t *zfsvfs = zfsvfs_array[fsid];
	objset_t *os = zfsvfs->z_os;
	vfs_t *vfs = zfsvfs->z_vfs;
	struct super_block *sb = zfsvfs->z_sb;

	struct inode *root_inode = NULL;
	int error = zfs_root(zfsvfs, &root_inode);
	if (error)
		return (1);

	iput(root_inode);
	iput(root_inode);

	// sleep 1 second for zfsvfs draining, otherwise may hit first
	// assert in zfs_unlinked_drain_task
	sleep(1);

	VERIFY(zfsvfs_teardown(zfsvfs, B_TRUE) == 0);

	/*
	 * z_os will be NULL if there was an error in
	 * attempting to reopen zfsvfs.
	 */
	if (os != NULL) {
		/*
		 * Unset the objset user_ptr.
		 */
		mutex_enter(&os->os_user_ptr_lock);
		dmu_objset_set_user(os, NULL);
		mutex_exit(&os->os_user_ptr_lock);

		/*
		 * Finally release the objset
		 */
		dmu_objset_disown(os, B_TRUE, zfsvfs);
	}

	if (sb)
		kmem_free(sb, sizeof (struct super_block));
	if (vfs)
		kmem_free(vfs, sizeof (vfs_t));
	if (zfsvfs)
		kmem_free(zfsvfs, sizeof (zfsvfs_t));

	return (0);
}

int
libuzfs_getroot(uint64_t fsid, uint64_t *ino)
{
	int error = 0;
	struct inode *root_inode = NULL;
	zfsvfs_t *zfsvfs = zfsvfs_array[fsid];

	error = zfs_root(zfsvfs, &root_inode);
	if (error) goto out;

	rootzp = ITOZ(root_inode);
	*ino = root_inode->i_ino;

out:
	return (error);
}

static int
cp_new_stat(struct linux_kstat *stat, struct stat *statbuf)
{
	struct stat tmp;

	tmp.st_ino = stat->ino;
	if (sizeof (tmp.st_ino) < sizeof (stat->ino) && tmp.st_ino != stat->ino)
		return (-EOVERFLOW);
	tmp.st_mode = stat->mode;
	tmp.st_nlink = stat->nlink;
	if (tmp.st_nlink != stat->nlink)
		return (-EOVERFLOW);
	tmp.st_uid = stat->uid;
	tmp.st_gid = stat->gid;
	tmp.st_size = stat->size;
	tmp.st_atime = stat->atime.tv_sec;
	tmp.st_mtime = stat->mtime.tv_sec;
	tmp.st_ctime = stat->ctime.tv_sec;
	tmp.st_blocks = stat->blocks;
	tmp.st_blksize = stat->blksize;
	memcpy(statbuf, &tmp, sizeof (tmp));
	return (0);
}

int
libuzfs_getattr(uint64_t fsid, uint64_t ino, struct stat *stat)
{
	int error = 0;
	znode_t *zp = NULL;
	zfsvfs_t *zfsvfs = zfsvfs_array[fsid];

	ZFS_ENTER(zfsvfs);

	error = zfs_zget(zfsvfs, ino, &zp);
	if (error)
		goto out;

	struct linux_kstat kstatbuf;
	memset(&kstatbuf, 0, sizeof (struct linux_kstat));

	error = zfs_getattr_fast(NULL, ZTOI(zp), &kstatbuf);
	if (error)
		goto out;

	cp_new_stat(&kstatbuf, stat);

out:
	if (zp)
		iput(ZTOI(zp));

	ZFS_EXIT(zfsvfs);
	return (error);
}

int
libuzfs_lookup(uint64_t fsid, uint64_t dino, char *name, uint64_t *ino)
{
	int error = 0;
	znode_t *dzp = NULL;
	znode_t *zp = NULL;
	zfsvfs_t *zfsvfs = zfsvfs_array[fsid];

	ZFS_ENTER(zfsvfs);

	error = zfs_zget(zfsvfs, dino, &dzp);
	if (error)
		goto out;

	error = zfs_lookup(dzp, name, &zp, 0, NULL, NULL, NULL);
	if (error)
		goto out;

	*ino = ZTOI(zp)->i_ino;

out:
	if (zp)
		iput(ZTOI(zp));

	if (dzp)
		iput(ZTOI(dzp));

	ZFS_EXIT(zfsvfs);
	return (error);
}

int
libuzfs_mkdir(uint64_t fsid, uint64_t dino, char *name, umode_t mode,
    uint64_t *ino)
{
	int error = 0;
	znode_t *dzp = NULL;
	znode_t *zp = NULL;
	zfsvfs_t *zfsvfs = zfsvfs_array[fsid];

	ZFS_ENTER(zfsvfs);

	if (dino == zfsvfs->z_root) {
		dzp = rootzp;
	} else {
		error = zfs_zget(zfsvfs, dino, &dzp);
		if (error) goto out;
	}

	vattr_t vap;
	libuzfs_vap_init(&vap, ZTOI(dzp), mode | S_IFDIR, NULL);

	error = zfs_mkdir(dzp, name, &vap, &zp, NULL, 0, NULL);
	if (error) goto out;

	*ino = ZTOI(zp)->i_ino;

out:
	if (zp)
		iput(ZTOI(zp));

	if (dzp && dzp != rootzp)
		iput(ZTOI(dzp));

	ZFS_EXIT(zfsvfs);
	return (error);
}

int
libuzfs_rmdir(uint64_t fsid, uint64_t dino, char *name)
{
	int error = 0;
	znode_t *dzp = NULL;
	zfsvfs_t *zfsvfs = zfsvfs_array[fsid];

	ZFS_ENTER(zfsvfs);

	error = zfs_zget(zfsvfs, dino, &dzp);
	if (error) goto out;

	error = zfs_rmdir(dzp, name, NULL, NULL, 0);
	if (error) goto out;

out:

	if (dzp)
		iput(ZTOI(dzp));

	ZFS_EXIT(zfsvfs);
	return (error);
}

#define	ZPL_DIR_CONTEXT_INIT(_dirent, _actor, _pos) {	\
	.dirent = _dirent,				\
	.actor = _actor,				\
	.pos = _pos,					\
}

int
libuzfs_readdir(uint64_t fsid, uint64_t ino, void *dirent, filldir_t filldir,
    loff_t pos)
{
	int error = 0;
	znode_t *zp = NULL;
	zfsvfs_t *zfsvfs = zfsvfs_array[fsid];

	ZFS_ENTER(zfsvfs);

	error = zfs_zget(zfsvfs, ino, &zp);
	if (error)
		goto out;

	zpl_dir_context_t ctx = ZPL_DIR_CONTEXT_INIT(dirent, filldir, pos);

	error = zfs_readdir(ZTOI(zp), &ctx, NULL);
	if (error)
		goto out;

out:
	if (zp)
		iput(ZTOI(zp));

	ZFS_EXIT(zfsvfs);
	return (error);
}

int
libuzfs_create(uint64_t fsid, uint64_t dino, char *name, umode_t mode,
    uint64_t *ino)
{
	int error = 0;
	znode_t *dzp = NULL;
	znode_t *zp = NULL;
	zfsvfs_t *zfsvfs = zfsvfs_array[fsid];

	ZFS_ENTER(zfsvfs);

	error = zfs_zget(zfsvfs, dino, &dzp);
	if (error)
		goto out;

	vattr_t vap;
	libuzfs_vap_init(&vap, ZTOI(dzp), mode | S_IFREG, NULL);

	error = zfs_create(dzp, name, &vap, 0, mode, &zp, NULL, 0, NULL);
	if (error)
		goto out;

	*ino = ZTOI(zp)->i_ino;

out:
	if (zp)
		iput(ZTOI(zp));

	if (dzp)
		iput(ZTOI(dzp));

	ZFS_EXIT(zfsvfs);
	return (error);
}

int
libuzfs_remove(uint64_t fsid, uint64_t dino, char *name)
{
	int error = 0;
	znode_t *dzp = NULL;
	zfsvfs_t *zfsvfs = zfsvfs_array[fsid];

	ZFS_ENTER(zfsvfs);

	error = zfs_zget(zfsvfs, dino, &dzp);
	if (error) goto out;

	error = zfs_remove(dzp, name, NULL, 0);
	if (error) goto out;

out:

	if (dzp)
		iput(ZTOI(dzp));

	ZFS_EXIT(zfsvfs);
	return (error);
}

int
libuzfs_rename(uint64_t fsid, uint64_t sdino, char *sname, uint64_t tdino,
    char *tname)
{
	int error = 0;
	znode_t *sdzp = NULL;
	znode_t *tdzp = NULL;
	zfsvfs_t *zfsvfs = zfsvfs_array[fsid];

	ZFS_ENTER(zfsvfs);

	error = zfs_zget(zfsvfs, sdino, &sdzp);
	if (error) goto out;

	error = zfs_zget(zfsvfs, tdino, &tdzp);
	if (error) goto out;

	error = zfs_rename(sdzp, sname, tdzp, tname, NULL, 0);
	if (error) goto out;

out:

	if (sdzp)
		iput(ZTOI(sdzp));

	if (tdzp)
		iput(ZTOI(tdzp));

	ZFS_EXIT(zfsvfs);
	return (error);
}

int
libuzfs_read(uint64_t fsid, uint64_t ino, zfs_uio_t *uio, int ioflag)
{
	int error = 0;
	znode_t *zp = NULL;
	zfsvfs_t *zfsvfs = zfsvfs_array[fsid];

	ZFS_ENTER(zfsvfs);

	error = zfs_zget(zfsvfs, ino, &zp);
	if (error) goto out;

	error = zfs_read(zp, uio, ioflag, NULL);
	if (error) goto out;

out:
	if (zp)
		iput(ZTOI(zp));

	ZFS_EXIT(zfsvfs);
	return (error);
}

void
libuzfs_dataset_space(libuzfs_dataset_handle_t *dhp, uint64_t *refdbytes,
    uint64_t *availbytes, uint64_t *usedobjs, uint64_t *availobjs)
{
	dmu_objset_space(dhp->os, refdbytes, availbytes,
	    usedobjs, availobjs);
}

int
libuzfs_object_next_hole(libuzfs_inode_handle_t *ihp,
    uint64_t *off)
{
	dmu_buf_impl_t *db = (dmu_buf_impl_t *)sa_get_db(ihp->sa_hdl);
	DB_DNODE_ENTER(db);
	dnode_t *dn = DB_DNODE(db);

	int err = dnode_next_offset(dn, DNODE_FIND_HOLE, off, 1, 1, 0);
	if (err == ESRCH) {
		*off = UINT64_MAX;
		err = 0;
	}

	DB_DNODE_EXIT(db);
	return (err);
}

int
libuzfs_write(uint64_t fsid, uint64_t ino, zfs_uio_t *uio, int ioflag)
{
	int error = 0;
	znode_t *zp = NULL;
	zfsvfs_t *zfsvfs = zfsvfs_array[fsid];

	ZFS_ENTER(zfsvfs);

	error = zfs_zget(zfsvfs, ino, &zp);
	if (error) goto out;

	error = zfs_write(zp, uio, ioflag, NULL);
	if (error) goto out;

out:
	if (zp)
		iput(ZTOI(zp));

	ZFS_EXIT(zfsvfs);
	return (error);
}

int
libuzfs_fsync(uint64_t fsid, uint64_t ino, int syncflag)
{
	int error = 0;
	znode_t *zp = NULL;
	zfsvfs_t *zfsvfs = zfsvfs_array[fsid];

	ZFS_ENTER(zfsvfs);

	error = zfs_zget(zfsvfs, ino, &zp);
	if (error) goto out;

	error = zfs_fsync(zp, syncflag, NULL);
	if (error) goto out;

out:
	if (zp)
		iput(ZTOI(zp));

	ZFS_EXIT(zfsvfs);
	return (error);
}
