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
#include "coroutine.h"
#include "libuzfs.h"
#include "libuzfs_impl.h"
#include "sys/dnode.h"
#include "sys/nvpair.h"
#include "sys/stdtypes.h"
#include "sys/sysmacros.h"
#include "sys/txg.h"
#include "sys/vdev_trim.h"
#include "sys/zfs_refcount.h"
#include "sys/zfs_rlock.h"

static boolean_t change_zpool_cache_path = B_FALSE;

static void libuzfs_create_inode_with_type_impl(
    libuzfs_dataset_handle_t *, uint64_t *, boolean_t,
    libuzfs_inode_type_t, dmu_tx_t *, uint64_t);

static inline int libuzfs_object_write_impl(libuzfs_dataset_handle_t *dhp,
    uint64_t obj, uint64_t offset, struct iovec *iovs, int iov_cnt,
    boolean_t sync, uint64_t replay_eof);

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
libuzfs_dsl_prop_set_uint64(const char *osname, zfs_prop_t prop, uint64_t value,
    boolean_t inherit)
{
	int err = 0;
	char *setpoint = NULL;
	uint64_t curval = 0;
	const char *propname = zfs_prop_to_name(prop);

	err = dsl_prop_set_int(osname, propname,
	    (inherit ? ZPROP_SRC_NONE : ZPROP_SRC_LOCAL), value);

	if (err == ENOSPC)
		return (err);

	ASSERT0(err);

	setpoint = umem_alloc(MAXPATHLEN, UMEM_NOFAIL);
	VERIFY0(dsl_prop_get_integer(osname, propname, &curval, setpoint));
	umem_free(setpoint, MAXPATHLEN);

	return (err);
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
libuzfs_log_write(libuzfs_dataset_handle_t *dhp, dmu_tx_t *tx,
    libuzfs_node_t *up, offset_t off, ssize_t resid, boolean_t sync)
{
	// this log write covers at most 1 block
	ASSERT3U(off / dhp->max_blksz, ==, (off + resid - 1) / dhp->max_blksz);

	zilog_t *zilog = dhp->zilog;
	if (zil_replaying(zilog, tx)) {
		return;
	}

	itx_t *itx = zil_itx_create(TX_WRITE, sizeof (lr_write_t) +
	    (resid <= zil_max_copied_data(zilog) ? resid : 0));
	lr_write_t *lr = (lr_write_t *)&itx->itx_lr;
	dmu_buf_impl_t *db = (dmu_buf_impl_t *)sa_get_db(up->sa_hdl);
	if (resid <= zil_max_copied_data(zilog)) {
		itx->itx_wr_state = WR_COPIED;
		DB_DNODE_ENTER(db);
		int err = dmu_read_by_dnode(DB_DNODE(db), off, resid,
		    lr + 1, DMU_READ_NO_PREFETCH);
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
	lr->lr_foid = up->u_obj;
	lr->lr_offset = off;
	lr->lr_length = resid;
	lr->lr_blkoff = 0;
	BP_ZERO(&lr->lr_blkptr);

	itx->itx_sync = sync;
	itx->itx_callback = NULL;
	itx->itx_callback_data = NULL;
	itx->itx_private = dhp;

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

	return (libuzfs_inode_delete((libuzfs_dataset_handle_t *)arg1,
	    lr->lr_doid, INODE_DATA_OBJ, NULL));
}

static int
libuzfs_object_truncate_impl(libuzfs_dataset_handle_t *dhp, uint64_t obj,
    uint64_t offset, uint64_t size)
{
	libuzfs_node_t *up;
	int err = libuzfs_acquire_node(dhp, obj, &up);
	if (err != 0) {
		return (err);
	}

	zfs_locked_range_t *lr = zfs_rangelock_enter(&up->rl,
	    size, UINT64_MAX, RL_WRITER);
	ASSERT(lr != NULL);

	objset_t *os = dhp->os;
	if (size < up->u_size) {
		err = dmu_free_long_range(os, obj, size, up->u_size);
		if (err)
			goto out;
	}
	up->u_size = size;

	dmu_tx_t *tx = dmu_tx_create(os);
	dmu_tx_hold_sa(tx, up->sa_hdl, FALSE);
	if ((err = dmu_tx_assign(tx, TXG_WAIT)) != 0) {
		dmu_tx_abort(tx);
	} else {
		sa_attr_type_t *sa_tbl = dhp->uzfs_attr_table;
		sa_bulk_attr_t bulk[2];
		int count = 0;
		struct timespec mtime;
		SA_ADD_BULK_ATTR(bulk, count, sa_tbl[UZFS_SIZE],
		    NULL, &up->u_size, sizeof (up->u_size));
		SA_ADD_BULK_ATTR(bulk, count, sa_tbl[UZFS_MTIME],
		    NULL, &mtime, sizeof (mtime));
		gethrestime(&mtime);
		VERIFY0(sa_bulk_update(up->sa_hdl, bulk, count, tx));
		libuzfs_log_truncate(dhp->zilog, tx,
		    TX_TRUNCATE, obj, offset, size);
		dmu_tx_commit(tx);
	}

out:
	zfs_rangelock_exit(lr);
	libuzfs_release_node(up);
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

	return (libuzfs_object_truncate_impl((libuzfs_dataset_handle_t *)arg1,
	    obj, offset, size));
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
	    " indirect: %d", obj, offset, length, indirect);

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

	return (libuzfs_object_write_impl((libuzfs_dataset_handle_t *)arg1,
	    obj, offset, &iov, 1, FALSE, replay_eof));
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

	int err = libuzfs_inode_set_kvattr(dhp, lr->lr_foid, name, value,
	    lr->lr_value_size, NULL, lr->option);

	return (err);
}

static int
libuzfs_replay_write2(void *arg1, void *arg2, boolean_t byteswap)
{
	libuzfs_dataset_handle_t *dhp = arg1;
	lr_write_t *lr = arg2;
	libuzfs_node_t *up;
	zfs_dbgmsg("replaying write2, obj: %ld, off: %ld, length: %ld,",
	    lr->lr_foid, lr->lr_offset, lr->lr_length);
	int error = libuzfs_acquire_node(dhp, lr->lr_foid, &up);
	if (error != 0) {
		return (error);
	}

	uint64_t end = lr->lr_offset + lr->lr_length;
	if (end > up->u_size) {
		up->u_size = end;
		dmu_tx_t *tx = dmu_tx_create(dhp->os);
		dmu_tx_hold_sa(tx, up->sa_hdl, B_FALSE);
		error = dmu_tx_assign(tx, TXG_WAIT);
		if (error != 0) {
			dmu_tx_abort(tx);
			goto out;
		}

		VERIFY0(sa_update(up->sa_hdl, dhp->uzfs_attr_table[UZFS_SIZE],
		    &end, sizeof (end), tx));

		/* Ensure the replayed seq is updated */
		(void) zil_replaying(dhp->zilog, tx);
		dmu_tx_commit(tx);
	}

out:
	libuzfs_release_node(up);
	return (error);
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
	NULL,				/* TX_SETATTR */
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
	libuzfs_node_t *up = zgd->zgd_private;
	if (zgd->zgd_db) {
		dmu_buf_rele(zgd->zgd_db, zgd);
	}

	zfs_rangelock_exit(zgd->zgd_lr);
	libuzfs_release_node(up);
	kmem_free(zgd, sizeof (zgd_t));
}

static int
libuzfs_get_data(void *arg, uint64_t arg2, lr_write_t *lr, char *buf,
    struct lwb *lwb, zio_t *zio)
{
	libuzfs_dataset_handle_t *dhp = arg;
	uint64_t object = lr->lr_foid;
	uint64_t offset = lr->lr_offset;
	uint64_t size = lr->lr_length;
	libuzfs_node_t *up;
	int error = libuzfs_acquire_node(dhp, object, &up);
	if (error != 0) {
		return (error);
	}

	ASSERT3P(lwb, !=, NULL);
	ASSERT3P(zio, !=, NULL);
	ASSERT3U(size, !=, 0);

	zgd_t *zgd = (zgd_t *)kmem_zalloc(sizeof (zgd_t), KM_SLEEP);
	zgd->zgd_private = up;
	zgd->zgd_lwb = lwb;

	objset_t *os = dhp->os;
	if (buf != NULL) {	/* immediate write */
		zgd->zgd_lr = zfs_rangelock_enter(&up->rl,
		    offset, size, RL_READER);
		error = dmu_read(os, object, offset, size,
		    lr + 1, DMU_READ_NO_PREFETCH);
	} else {	/* indirect write */
		offset -= P2PHASE(offset, dhp->max_blksz);
		zgd->zgd_lr = zfs_rangelock_enter(&up->rl, offset,
		    dhp->max_blksz, RL_READER);

		if (lr->lr_offset >= up->u_size) {
			error = SET_ERROR(ENOENT);
		}

		dmu_buf_t *db;
		if (error == 0) {
			error = dmu_buf_hold(os, object, offset, zgd,
			    &db, DMU_READ_NO_PREFETCH);
		}

		if (error == 0) {
			blkptr_t *bp = &lr->lr_blkptr;
			zgd->zgd_db = db;
			zgd->zgd_bp = bp;
			ASSERT3U(db->db_offset, ==, offset);
			ASSERT3U(db->db_size, ==, up->u_blksz);
			error = dmu_sync(zio, lr->lr_common.lrc_txg,
			    libuzfs_get_done, zgd);
			ASSERT(error || lr->lr_length <= up->u_blksz);

			if (error == 0) {
				return (0);
			}

			if (error == EALREADY) {
				lr->lr_common.lrc_txtype = TX_WRITE2;
				/*
				 * TX_WRITE2 relies on the data previously
				 * written by the TX_WRITE that caused
				 * EALREADY.  We zero out the BP because
				 * it is the old, currently-on-disk BP.
				 */
				zgd->zgd_bp = NULL;
				BP_ZERO(bp);
				error = 0;
			}
		}
	}

	libuzfs_get_done(zgd, error);

	return (error);
}

extern void (*do_backtrace)(void);

void
libuzfs_init(thread_create_func create, thread_exit_func exit,
    thread_join_func join, backtrace_func bt_func)
{
	do_backtrace = bt_func;
	set_thread_funcs(create, exit, join);
	coroutine_init();
	kernel_init(SPA_MODE_READ | SPA_MODE_WRITE);
}

void
libuzfs_fini(void)
{
	kernel_fini();
	coroutine_destroy();
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

static int
pool_active(void *unused, const char *name, uint64_t guid,
    boolean_t *isactive)
{
	*isactive = B_FALSE;
	return (0);
}

static nvlist_t *
refresh_config(void *unused, nvlist_t *tryconfig)
{
	nvlist_t *res;
	nvlist_dup(tryconfig, &res, KM_SLEEP);
	return (res);
}

extern kmutex_t spa_namespace_lock;

int
libuzfs_zpool_import(const char *dev_path, char *pool_name, int size)
{
	importargs_t args = { 0 };
	args.path = (char **)&dev_path;
	args.paths = 1;

	pool_config_ops_t ops = {
		.pco_refresh_config = refresh_config,
		.pco_pool_active = pool_active,
	};
	nvlist_t *pools = zpool_search_import(NULL,
	    &args, &ops);

	if (pools == NULL) {
		return (ENOMEM);
	}

	if (nvlist_empty(pools)) {
		nvlist_free(pools);
		return (ENOENT);
	}

	nvpair_t *elem = nvlist_next_nvpair(pools, NULL);
	VERIFY3U(elem, !=, NULL);
	VERIFY3U(nvlist_next_nvpair(pools, elem), ==, NULL);

	nvlist_t *config = NULL;
	VERIFY0(nvpair_value_nvlist(elem, &config));

	char *stored_pool_name;
	VERIFY0(nvlist_lookup_string(config,
	    ZPOOL_CONFIG_POOL_NAME, &stored_pool_name));
	int name_len = strlen(stored_pool_name);
	if (name_len >= size) {
		nvlist_free(pools);
		return (ERANGE);
	}
	strncpy(pool_name, stored_pool_name, name_len);
	pool_name[name_len] = '\0';

	int err = spa_import(pool_name, config, NULL, ZFS_IMPORT_NORMAL);
	if (err == ENOENT) {
		err = EINVAL;
	}

	if (err == 0) {
		mutex_enter(&spa_namespace_lock);
		spa_t *spa = spa_lookup(pool_name);
		VERIFY(spa != NULL);
		mutex_exit(&spa_namespace_lock);

		vdev_t *root_vdev = spa->spa_root_vdev;
		for (int i = 0; i < root_vdev->vdev_children; ++i) {
			vdev_t *leaf_vdev = root_vdev->vdev_child[i];
			mutex_enter(&leaf_vdev->vdev_trim_lock);
			// spa import may restart trim, so
			// leaf_vdev->vdev_trim_thread might not be NULL
			if (leaf_vdev->vdev_trim_thread == NULL) {
				vdev_trim(leaf_vdev, UINT64_MAX,
				    B_TRUE, B_FALSE);
			}
			mutex_exit(&leaf_vdev->vdev_trim_lock);
		}
	}

	nvlist_free(pools);

	return (err);
}

int
libuzfs_zpool_export(const char *pool_name)
{
	int err = spa_export(pool_name, NULL, B_TRUE, B_FALSE);

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

	libuzfs_dataset_handle_t dhp;
	dhp.os = os;
	libuzfs_setup_dataset_sa(&dhp);
	uint64_t sb_obj = 0;
	libuzfs_create_inode_with_type_impl(&dhp, &sb_obj,
	    B_FALSE, INODE_DIR, tx, 0);
	VERIFY0(zap_add(os, MASTER_NODE_OBJ, UZFS_SB_OBJ, 8, 1, &sb_obj, tx));
}

int
libuzfs_dataset_create(const char *dsname)
{
	return (dmu_objset_create(dsname, DMU_OST_ZFS, 0, NULL,
	    libuzfs_objset_create_cb, NULL));
}

int
libuzfs_dataset_set_props(const char *dsname, uint32_t dnodesize)
{
	int err = libuzfs_dsl_prop_set_uint64(dsname, ZFS_PROP_SYNC,
	    ZFS_SYNC_STANDARD, B_FALSE);
	if (err == 0) {
		err = libuzfs_dsl_prop_set_uint64(dsname, ZFS_PROP_DNODESIZE,
		    dnodesize, B_FALSE);
	}

	return (err);
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

static int
libuzfs_node_compare(const void *x1, const void *x2)
{
	const libuzfs_node_t *node1 = x1;
	const libuzfs_node_t *node2 = x2;

	if (node1->u_obj < node2->u_obj) {
		return (-1);
	}

	if (node1->u_obj == node2->u_obj) {
		return (0);
	}

	return (1);
}

static void
libuzfs_dhp_init(libuzfs_dataset_handle_t *dhp, objset_t *os)
{
	dhp->os = os;
	dmu_objset_name(os, dhp->name);

	zap_lookup(os, MASTER_NODE_OBJ, UZFS_SB_OBJ, 8, 1,
	    &dhp->sb_ino);

	dmu_objset_register_type(DMU_OST_ZFS, uzfs_get_file_info);
	libuzfs_setup_dataset_sa(dhp);

	for (int i = 0; i < NUM_NODE_BUCKETS; ++i) {
		hash_bucket_t *bucket = &dhp->nodes_buckets[i];
		avl_create(&bucket->tree, libuzfs_node_compare,
		    sizeof (libuzfs_node_t), offsetof(libuzfs_node_t, node));
		mutex_init(&bucket->mutex, NULL, 0, NULL);

		mutex_init(&dhp->objs_lock[i], NULL, 0, NULL);
	}

	dhp->zilog = zil_open(os, libuzfs_get_data);
	zil_replay(os, dhp, libuzfs_replay_vector);
}

static void
libuzfs_dhp_fini(libuzfs_dataset_handle_t *dhp)
{
	for (int i = 0; i < NUM_NODE_BUCKETS; ++i) {
		hash_bucket_t *bucket = &dhp->nodes_buckets[i];
		avl_destroy(&bucket->tree);
		ASSERT(avl_is_empty(&bucket->tree));
		mutex_destroy(&bucket->mutex);

		mutex_destroy(&dhp->objs_lock[i]);
	}
}

libuzfs_dataset_handle_t *
libuzfs_dataset_open(const char *dsname, int *err)
{
	libuzfs_dataset_handle_t *dhp = NULL;
	objset_t *os = NULL;

	dhp = umem_alloc(sizeof (libuzfs_dataset_handle_t), UMEM_NOFAIL);
	dhp->max_blksz = UZFS_MAX_BLOCKSIZE;

	*err = libuzfs_dmu_objset_own(dsname, DMU_OST_ZFS, B_FALSE, B_TRUE,
	    dhp, &os);
	if (*err) {
		umem_free(dhp, sizeof (libuzfs_dataset_handle_t));
		return (NULL);
	}

	libuzfs_dhp_init(dhp, os);
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
	free(dhp);
}

int
libuzfs_dataset_get_superblock_ino(libuzfs_dataset_handle_t *dhp,
    uint64_t *sb_ino)
{
	*sb_ino = dhp->sb_ino;
	return (0);
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
libuzfs_object_sync(libuzfs_dataset_handle_t *dhp, uint64_t obj)
{
	zil_commit(dhp->zilog, obj);
}

int
libuzfs_object_truncate(libuzfs_dataset_handle_t *dhp, uint64_t obj,
    uint64_t offset, uint64_t size)
{
	// FIXME(hping): only support truncate now, thus offset is always 0,
	// support fallocate in future, in which case offset could be non-zero
	if (offset != 0)
		return (EINVAL);

	int err = libuzfs_object_truncate_impl(dhp, obj, offset, size);
	if (err == 0) {
		zil_commit(dhp->zilog, obj);
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

static void
libuzfs_create_inode_with_type_impl(libuzfs_dataset_handle_t *dhp,
    uint64_t *obj, boolean_t claiming, libuzfs_inode_type_t type,
    dmu_tx_t *tx, uint64_t gen)
{
	// create/claim object
	objset_t *os = dhp->os;
	int dnodesize = dmu_objset_dnodesize(os);
	int bonuslen = DN_BONUS_SIZE(dnodesize);
	if (type == INODE_FILE || type == INODE_DATA_OBJ) {
		if (claiming) {
			ASSERT(*obj != 0);
			VERIFY0(dmu_object_claim_dnsize(os, *obj,
			    DMU_OT_PLAIN_FILE_CONTENTS,
			    0, DMU_OT_SA, bonuslen, dnodesize, tx));
		} else {
			*obj = dmu_object_alloc_dnsize(os,
			    DMU_OT_PLAIN_FILE_CONTENTS, 0,
			    DMU_OT_SA, bonuslen, dnodesize, tx);
		}
		if (type == INODE_DATA_OBJ) {
			VERIFY0(dmu_object_set_blocksize(os, *obj, 0, 0, tx));
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
		} else {
			*obj = zap_create_dnsize(os, DMU_OT_DIRECTORY_CONTENTS,
			    DMU_OT_SA, bonuslen, dnodesize, tx);
		}
	} else {
		VERIFY0(1);
	}

	if (!claiming) {
		gen = tx->tx_txg;
	}

	kmutex_t *mp = &dhp->objs_lock[(*obj) % NUM_NODE_BUCKETS];
	mutex_enter(mp);
	sa_handle_t *sa_hdl;
	VERIFY0(sa_handle_get(os, *obj, NULL, SA_HDL_PRIVATE, &sa_hdl));
	libuzfs_inode_attr_init(dhp, sa_hdl, tx, type, gen);
	sa_handle_destroy(sa_hdl);
	mutex_exit(mp);
}

static int
libuzfs_create_inode_with_type(libuzfs_dataset_handle_t *dhp, uint64_t *obj,
    boolean_t claiming, libuzfs_inode_type_t type, uint64_t *txg, uint64_t gen)
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

	libuzfs_create_inode_with_type_impl(dhp, obj, claiming, type, tx, gen);

	if (txg != NULL) {
		*txg = tx->tx_txg;
	}
	dmu_tx_commit(tx);

	return (0);
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
		libuzfs_create_inode_with_type_impl(dhp, &objs[i],
		    B_FALSE, INODE_DATA_OBJ, tx, 0);
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
libuzfs_object_delete(libuzfs_dataset_handle_t *dhp, uint64_t obj)
{
	int err = libuzfs_inode_delete(dhp, obj, INODE_DATA_OBJ, NULL);
	if (err == 0) {
		zil_submit(dhp->zilog, obj);
	}

	return (err);
}

static int
libuzfs_object_claim(libuzfs_dataset_handle_t *dhp, uint64_t obj,
    uint64_t gen, libuzfs_inode_type_t type)
{
	objset_t *os = dhp->os;
	int dnodesize = dmu_objset_dnodesize(os);
	int err = dnode_try_claim(os, obj, dnodesize >> DNODE_SHIFT);

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
			zfs_dbgmsg("object %lu already created, type: %d",
			    obj, dn->dn_type);
			dnode_rele(dn, FTAG);
		}
		return (err);
	} else if (err != 0) {
		return (err);
	}

do_claim:
	return (libuzfs_create_inode_with_type(dhp, &obj,
	    TRUE, type, NULL, gen));
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

static void
libuzfs_rangelock_cb(zfs_locked_range_t *new, void *arg)
{
	// no need to modify lock range for truncate
	if (new->lr_length == UINT64_MAX) {
		return;
	}

	libuzfs_node_t *up = arg;
	// If we need to grow the block size then lock the whole file range.
	uint64_t end_size = MAX(up->u_size, new->lr_offset + new->lr_length);
	if (up->u_blksz < end_size && up->u_blksz < up->u_maxblksz) {
		new->lr_offset = 0;
		new->lr_length = UINT64_MAX;
	}
}

static int
libuzfs_node_alloc(libuzfs_dataset_handle_t *dhp,
    uint64_t obj, libuzfs_node_t **upp)
{
	sa_handle_t *sa_hdl;
	int err = sa_handle_get(dhp->os, obj, NULL, SA_HDL_PRIVATE, &sa_hdl);
	if (err != 0) {
		return (err);
	}

	uint64_t obj_size;
	err = sa_lookup(sa_hdl, dhp->uzfs_attr_table[UZFS_SIZE],
	    &obj_size, sizeof (obj_size));
	if (err != 0) {
		sa_handle_destroy(sa_hdl);
		return (err);
	}

	libuzfs_node_t *up = *upp = umem_alloc(sizeof (libuzfs_node_t),
	    UMEM_NOFAIL);
	zfs_rangelock_init(&up->rl, libuzfs_rangelock_cb, up);
	up->sa_hdl = sa_hdl;
	up->u_size = obj_size;
	dnode_t *dn = DB_DNODE((dmu_buf_impl_t *)sa_get_db(sa_hdl));
	up->u_blksz = dn->dn_datablksz;
	up->u_maxblksz = dhp->max_blksz;
	up->u_obj = obj;
	up->ref_count = 1;
	up->dhp = dhp;

	return (0);
}

static void
libuzfs_node_free(libuzfs_node_t *up)
{
	sa_handle_destroy(up->sa_hdl);
	zfs_rangelock_fini(&up->rl);
	umem_free(up, sizeof (libuzfs_node_t));
}

int
libuzfs_acquire_node(libuzfs_dataset_handle_t *dhp,
    uint64_t obj, libuzfs_node_t **upp)
{
	uint64_t idx = obj % NUM_NODE_BUCKETS;
	hash_bucket_t *bucket = &dhp->nodes_buckets[idx];
	libuzfs_node_t node;
	node.u_obj = obj;
	int err = 0;

	mutex_enter(&bucket->mutex);
	avl_index_t where;
	*upp = avl_find(&bucket->tree, &node, &where);
	if ((*upp) != NULL) {
		++(*upp)->ref_count;
	} else if ((err = libuzfs_node_alloc(dhp, obj, upp)) == 0) {
		avl_insert(&bucket->tree, *upp, where);
	}
	mutex_exit(&bucket->mutex);
	return (err);
}

void
libuzfs_release_node(libuzfs_node_t *up)
{
	uint64_t idx = up->u_obj % NUM_NODE_BUCKETS;
	hash_bucket_t *bucket = &up->dhp->nodes_buckets[idx];
	mutex_enter(&bucket->mutex);
	if (--up->ref_count == 0) {
		avl_remove(&bucket->tree, up);
		// TODO(sundengyu): lazily free these resources
		libuzfs_node_free(up);
	}
	mutex_exit(&bucket->mutex);
}

static inline int
libuzfs_object_write_impl(libuzfs_dataset_handle_t *dhp,
    uint64_t obj, uint64_t offset, struct iovec *iovs,
    int iov_cnt, boolean_t sync, uint64_t replay_eof)
{
	int err;
	libuzfs_node_t *up;
	if ((err = libuzfs_acquire_node(dhp, obj, &up)) != 0) {
		return (err);
	}
	dnode_t *dn = DB_DNODE((dmu_buf_impl_t *)sa_get_db(up->sa_hdl));
	uint64_t size = 0;
	for (int i = 0; i < iov_cnt; ++i) {
		size += iovs[i].iov_len;
	}

	zfs_uio_t uio;
	zfs_uio_iovec_init(&uio, iovs, iov_cnt,
	    offset, UIO_USERSPACE, size, 0);

	zfs_locked_range_t *lr = zfs_rangelock_enter(&up->rl,
	    offset, size, RL_WRITER);

	ASSERT(lr != NULL);

	sa_attr_type_t *sa_tbl = dhp->uzfs_attr_table;
	uint64_t max_blksz = dhp->max_blksz;

	sa_bulk_attr_t bulk[2];
	int count = 0;
	struct timespec mtime;
	SA_ADD_BULK_ATTR(bulk, count, sa_tbl[UZFS_SIZE],
	    NULL, &up->u_size, sizeof (up->u_size));
	SA_ADD_BULK_ATTR(bulk, count, sa_tbl[UZFS_MTIME],
	    NULL, &mtime, sizeof (mtime));
	objset_t *os = dhp->os;
	while ((size = zfs_uio_resid(&uio)) > 0) {
		offset = zfs_uio_offset(&uio);
		uint64_t nwrite = MIN(size,
		    max_blksz - P2PHASE(offset, max_blksz));
		dmu_tx_t *tx = dmu_tx_create(os);
		dmu_tx_hold_sa(tx, up->sa_hdl, FALSE);
		dmu_tx_hold_write_by_dnode(tx, dn, offset, nwrite);
		if ((err = dmu_tx_assign(tx, TXG_WAIT)) != 0) {
			dmu_tx_abort(tx);
			break;
		}

		// update max blocksize when current blksz too small
		if (lr->lr_length == UINT64_MAX) {
			uint64_t end_size = MAX(up->u_size, size + offset);
			uint64_t new_blksz = MIN(max_blksz, end_size);
			ASSERT3U(new_blksz, >, up->u_blksz);
			VERIFY0(dnode_set_blksz(dn, new_blksz, 0, tx));
			up->u_blksz = P2ROUNDUP(new_blksz, SPA_MINBLOCKSIZE);
			zfs_rangelock_reduce(lr, offset, size);
		}

		VERIFY0(dmu_write_uio_dnode(dn, &uio, nwrite, tx));

		if (replay_eof == 0) {
			uint64_t up_size = 0;
			uint64_t eod = offset + nwrite;
			while ((up_size = atomic_load_64(&up->u_size)) < eod) {
				atomic_cas_64(&up->u_size, up_size, eod);
			}
		} else if (up->u_size < replay_eof) {
			// this is a replay write, use replay_eof as file size
			up->u_size = replay_eof;
		}

		gethrestime(&mtime);
		VERIFY0(sa_bulk_update(up->sa_hdl, bulk, 2, tx));

		libuzfs_log_write(dhp, tx, up, offset, nwrite, sync);
		dmu_tx_commit(tx);
	}

	zfs_rangelock_exit(lr);

	if (sync && err == 0) {
		zil_commit(dhp->zilog, obj);
	}

	libuzfs_release_node(up);

	return (err);
}

int
libuzfs_object_write(libuzfs_dataset_handle_t *dhp, uint64_t obj,
    uint64_t offset, struct iovec *iovs, int iov_cnt, boolean_t sync)
{
	return (libuzfs_object_write_impl(dhp, obj, offset,
	    iovs, iov_cnt, sync, 0));
}

int
libuzfs_object_read(libuzfs_dataset_handle_t *dhp, uint64_t obj,
    uint64_t offset, uint64_t size, char *buf)
{
	libuzfs_node_t *up;
	int rc = libuzfs_acquire_node(dhp, obj, &up);
	if (rc != 0) {
		return (-rc);
	}

	zfs_locked_range_t *lr = zfs_rangelock_enter(&up->rl,
	    offset, size, RL_READER);
	ASSERT(lr != NULL);
	if (offset >= up->u_size) {
		goto out;
	}

	int read_size = MIN(up->u_size - offset, size);

	int err = dmu_read(dhp->os, obj, offset,
	    read_size, buf, DMU_READ_NO_PREFETCH);
	if (err != 0) {
		rc = -err;
	} else {
		rc = read_size;
	}

out:
	zfs_rangelock_exit(lr);
	libuzfs_release_node(up);
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

	int dnodesize = dmu_objset_dnodesize(os);
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

	int dnodesize = dmu_objset_dnodesize(os);
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
libuzfs_inode_delete(libuzfs_dataset_handle_t *dhp, uint64_t ino,
    libuzfs_inode_type_t type, uint64_t *txg)
{
	uint64_t xattr_zap_obj = 0;
	int err = libuzfs_get_xattr_zap_obj(dhp, ino, &xattr_zap_obj);
	if (err != 0 && err != ENOENT) {
		return (err);
	}

	objset_t *os = dhp->os;
	dmu_tx_t *tx = dmu_tx_create(os);

	dmu_tx_hold_free(tx, ino, 0, DMU_OBJECT_END);
	if (xattr_zap_obj != 0) {
		dmu_tx_hold_free(tx, xattr_zap_obj, 0, DMU_OBJECT_END);
	}

	if ((err = dmu_tx_assign(tx, TXG_WAIT)) == 0) {
		if (xattr_zap_obj != 0) {
			VERIFY0(zap_destroy(os, xattr_zap_obj, tx));
		}

		if (type == INODE_DIR) {
			VERIFY0(zap_destroy(os, ino, tx));
		} else {
			VERIFY0(dmu_object_free(os, ino, tx));
		}

		if (txg != NULL) {
			*txg = tx->tx_txg;
		}

		if (type == INODE_DATA_OBJ) {
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
    libuzfs_inode_type_t type, uint64_t *txg)
{
	return (libuzfs_create_inode_with_type(dhp, ino,
	    B_FALSE, type, txg, 0));
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

int libuzfs_dentry_create(libuzfs_dataset_handle_t *dhp, uint64_t dino,
    const char *name, uint64_t value, uint64_t *txg)
{
	return (libuzfs_zap_add(dhp, dino, name, 8, 1, &value, txg));
}

int libuzfs_dentry_delete(libuzfs_dataset_handle_t *dhp, uint64_t dino,
    const char *name, uint64_t *txg)
{
	return (libuzfs_zap_remove(dhp, dino, name, txg));
}

int libuzfs_dentry_lookup(libuzfs_dataset_handle_t *dhp, uint64_t dino,
    const char *name, uint64_t *value)
{
	return (libuzfs_zap_lookup(dhp, dino, name, 8, 1, value));
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
libuzfs_dentry_iterate(libuzfs_dataset_handle_t *dhp, uint64_t dino,
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
libuzfs_object_next_hole(libuzfs_dataset_handle_t *dhp, uint64_t obj,
    uint64_t *off)
{
	dnode_t *dn;
	int err = dnode_hold(dhp->os, obj, FTAG, &dn);
	if (err) {
		return (err);
	}

	err = dnode_next_offset(dn, DNODE_FIND_HOLE, off, 1, 1, 0);
	if (err == ESRCH) {
		*off = UINT64_MAX;
		err = 0;
	}

	dnode_rele(dn, FTAG);

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
