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

#include "libuzfs_impl.h"

static pthread_mutex_t g_lock = PTHREAD_MUTEX_INITIALIZER;
static int g_refcount = 0;
static boolean_t change_zpool_cache_path = B_FALSE;

static void libuzfs_create_inode_with_type_impl(
    libuzfs_dataset_handle_t *dhp, uint64_t *obj,
    boolean_t claiming, libuzfs_inode_type_t type,
    dmu_tx_t *tx);

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
	return (SPA_MINBLOCKSHIFT);
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
	fnvlist_add_string(file, ZPOOL_CONFIG_TYPE, VDEV_TYPE_FILE);
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

	itx = zil_itx_create(txtype, sizeof (*lr));
	lr = (lr_truncate_t *)&itx->itx_lr;
	lr->lr_foid = obj;
	lr->lr_offset = offset;
	lr->lr_length = size;

	itx->itx_sync = B_TRUE;
	zil_itx_assign(zilog, itx, tx);
}

static void
libuzfs_log_write(libuzfs_dataset_handle_t *dhp, dmu_tx_t *tx, int txtype,
    uint64_t obj, offset_t off, ssize_t resid, boolean_t sync)
{
	zilog_t *zilog = dhp->zilog;
	dmu_buf_t *db_fake;
	dmu_buf_impl_t *db;
	itx_wr_state_t write_state;

	// TODO(hping): also return if obj is unlinked
	if (zil_replaying(zilog, tx)) {
		return;
	}

	VERIFY0(dmu_bonus_hold(zilog->zl_os, obj, FTAG, &db_fake));
	db = (dmu_buf_impl_t *)db_fake;

	write_state = WR_COPIED;

	while (resid) {
		itx_t *itx;
		lr_write_t *lr;
		itx_wr_state_t wr_state = write_state;
		ssize_t len = resid;

		/*
		 * A WR_COPIED record must fit entirely in one log block.
		 * Large writes can use WR_NEED_COPY, which the ZIL will
		 * split into multiple records across several log blocks
		 * if necessary.
		 */
		if (wr_state == WR_COPIED && resid > zil_max_copied_data(zilog))
			wr_state = WR_NEED_COPY;

		itx = zil_itx_create(txtype, sizeof (*lr) +
		    (wr_state == WR_COPIED ? len : 0));
		lr = (lr_write_t *)&itx->itx_lr;

		/*
		 * For WR_COPIED records, copy the data into the lr_write_t.
		 */
		if (wr_state == WR_COPIED) {
			int err;
			DB_DNODE_ENTER(db);
			err = dmu_read_by_dnode(DB_DNODE(db), off, len, lr + 1,
			    DMU_READ_NO_PREFETCH);
			if (err != 0) {
				zil_itx_destroy(itx);
				itx = zil_itx_create(txtype, sizeof (*lr));
				lr = (lr_write_t *)&itx->itx_lr;
				wr_state = WR_NEED_COPY;
			}
			DB_DNODE_EXIT(db);
		}

		itx->itx_wr_state = wr_state;
		lr->lr_foid = obj;
		lr->lr_offset = off;
		lr->lr_length = len;
		lr->lr_blkoff = 0;
		BP_ZERO(&lr->lr_blkptr);

		itx->itx_private = NULL;

		if (!sync)
			itx->itx_sync = B_FALSE;

		itx->itx_callback = NULL;
		itx->itx_callback_data = NULL;
		itx->itx_private = dhp;
		zil_itx_assign(zilog, itx, tx);

		off += len;
		resid -= len;
	}

	dmu_buf_rele(db_fake, FTAG);
}

static int
libuzfs_replay_create(void *arg1, void *arg2, boolean_t byteswap)
{
	libuzfs_dataset_handle_t *dhp = arg1;
	lr_create_t *lr = arg2;
	uint64_t obj;
	int dnodesize;
	int error;

	if (byteswap) {
		byteswap_uint64_array(lr, sizeof (*lr));
	}

	obj = LR_FOID_GET_OBJ(lr->lr_foid);

	dnodesize = dmu_objset_dnodesize(dhp->os);

	error = dnode_try_claim(dhp->os, obj, dnodesize >> DNODE_SHIFT);
	if (error)
		return (error);

	printf("replay create, obj: %ld\n", obj);

	return (libuzfs_object_claim(dhp, obj));
}

static int
libuzfs_replay_remove(void *arg1, void *arg2, boolean_t byteswap)
{
	libuzfs_dataset_handle_t *dhp = arg1;
	lr_remove_t *lr = arg2;
	objset_t *os = dhp->os;
	int err;
	uint64_t obj;
	dmu_tx_t *tx;

	if (byteswap)
		byteswap_uint64_array(lr, sizeof (*lr));

	obj = lr->lr_doid;

	tx = dmu_tx_create(os);

	dmu_tx_hold_free(tx, obj, 0, DMU_OBJECT_END);

	err = dmu_tx_assign(tx, TXG_WAIT);
	if (err) {
		dmu_tx_abort(tx);
		goto out;
	}

	VERIFY0(dmu_object_free(os, obj, tx));

	dmu_tx_commit(tx);

	printf("replay remove, obj: %ld\n", obj);
out:
	return (err);
}

static int
libuzfs_replay_truncate(void *arg1, void *arg2, boolean_t byteswap)
{
	libuzfs_dataset_handle_t *dhp = arg1;
	lr_truncate_t *lr = arg2;
	uint64_t obj;
	uint64_t offset;
	uint64_t size;

	if (byteswap)
		byteswap_uint64_array(lr, sizeof (*lr));

	obj = lr->lr_foid;
	offset = lr->lr_offset;
	size = lr->lr_length;

	printf("replay truncate, obj: %ld, off: %ld, size: %ld\n", obj, offset,
	    size);

	return (libuzfs_object_truncate(dhp, obj, offset, size));
}

static int
libuzfs_replay_write(void *arg1, void *arg2, boolean_t byteswap)
{
	int err = 0;
	libuzfs_dataset_handle_t *dhp = arg1;
	lr_write_t *lr = arg2;
	objset_t *os = dhp->os;
	void *data = lr + 1;
	uint64_t offset, length, obj;
	dmu_tx_t *tx;
	uzfs_object_attr_t *attr = NULL;
	dmu_buf_t *db;

	if (byteswap)
		byteswap_uint64_array(lr, sizeof (*lr));

	obj = lr->lr_foid;
	offset = lr->lr_offset;
	length = lr->lr_length;

	err = dmu_bonus_hold(os, obj, FTAG, &db);
	if (err)
		return (err);

	attr = db->db_data;


	tx = dmu_tx_create(os);

	dmu_tx_hold_write(tx, lr->lr_foid, offset, length);

	err = dmu_tx_assign(tx, TXG_WAIT);
	if (err) {
		dmu_tx_abort(tx);
		goto out;
	}

	dmu_write(os, obj, offset, length, data, tx);

	if (offset + length > attr->size) {
		attr->size = offset + length;
	}

	dmu_tx_commit(tx);

	printf("replay write, obj: %ld, off: %ld, length: %ld, size: %ld\n",
	    obj, offset, length, attr->size);

out:
	dmu_buf_rele(db, FTAG);
	return (err);
}

zil_replay_func_t *libuzfs_replay_vector[TX_MAX_TYPE] = {
	NULL,				/* 0 no such transaction type */
	libuzfs_replay_create,		/* TX_CREATE */
	NULL,				/* TX_MKDIR */
	NULL,				/* TX_MKXATTR */
	NULL,				/* TX_SYMLINK */
	libuzfs_replay_remove,		/* TX_REMOVE */
	NULL,				/* TX_RMDIR */
	NULL,				/* TX_LINK */
	NULL,				/* TX_RENAME */
	libuzfs_replay_write,		/* TX_WRITE */
	libuzfs_replay_truncate,	/* TX_TRUNCATE */
	NULL,				/* TX_SETATTR */
	NULL,				/* TX_ACL */
	NULL,				/* TX_CREATE_ACL */
	NULL,				/* TX_CREATE_ATTR */
	NULL,				/* TX_CREATE_ACL_ATTR */
	NULL,				/* TX_MKDIR_ACL */
	NULL,				/* TX_MKDIR_ATTR */
	NULL,				/* TX_MKDIR_ACL_ATTR */
	NULL,				/* TX_WRITE2 */
};

static int
libuzfs_get_data(void *arg, uint64_t arg2, lr_write_t *lr, char *buf,
    struct lwb *lwb, zio_t *zio)
{
	libuzfs_dataset_handle_t *dhp = arg;
	objset_t *os = dhp->os;
	uint64_t object = lr->lr_foid;
	uint64_t offset = lr->lr_offset;
	uint64_t size = lr->lr_length;
	int error = 0;

	ASSERT3P(lwb, !=, NULL);
	ASSERT3P(zio, !=, NULL);
	ASSERT3U(size, !=, 0);

	if (buf != NULL) {	/* immediate write */
		error = dmu_read(os, object, offset, size, buf,
		    DMU_READ_NO_PREFETCH);
		ASSERT0(error);
	} else {
		/* TODO(hping): support indirect write */
		ASSERT(0);
	}

	return (error);
}

void
libuzfs_init()
{
	(void) pthread_mutex_lock(&g_lock);
	if (g_refcount == 0) {
		kernel_init(SPA_MODE_READ | SPA_MODE_WRITE);
	}
	g_refcount++;

	(void) pthread_mutex_unlock(&g_lock);
}

void
libuzfs_fini()
{
	(void) pthread_mutex_lock(&g_lock);
	ASSERT3S(g_refcount, >, 0);

	if (g_refcount > 0)
		g_refcount--;

	if (g_refcount == 0) {
		kernel_fini();
		if (change_zpool_cache_path) {
			free(spa_config_path);
		}
	}
	(void) pthread_mutex_unlock(&g_lock);
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

	err = spa_create(zpool, nvroot, props, NULL, NULL);
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
libuzfs_zpool_open(const char *zpool)
{
	int err = 0;
	spa_t *spa = NULL;

	err = spa_open(zpool, &spa, FTAG);
	if (err)
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
	strncpy(pool_name, stored_pool_name, size);

	int err = spa_import(pool_name, config, NULL, ZFS_IMPORT_NORMAL);
	if (err == ENOENT) {
		err = EINVAL;
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
	    B_FALSE, INODE_DIR, tx);
	VERIFY0(zap_add(os, MASTER_NODE_OBJ, UZFS_SB_OBJ, 8, 1, &sb_obj, tx));
}

int
libuzfs_dataset_create(const char *dsname)
{
	int err = 0;

	err = dmu_objset_create(dsname, DMU_OST_ZFS, 0, NULL,
	    libuzfs_objset_create_cb, NULL);
	if (err)
		return (err);

	return (libuzfs_dsl_prop_set_uint64(dsname, ZFS_PROP_SYNC,
	    ZFS_SYNC_STANDARD, B_FALSE));
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

static void
libuzfs_dhp_init(libuzfs_dataset_handle_t *dhp, objset_t *os)
{
	dhp->os = os;
	dhp->zilog = dmu_objset_zil(os);
	dmu_objset_name(os, dhp->name);

	zap_lookup(os, MASTER_NODE_OBJ, UZFS_SB_OBJ, 8, 1,
	    &dhp->sb_ino);
}

static void
libuzfs_dhp_fini(libuzfs_dataset_handle_t *dhp)
{
}

static int
uzfs_get_file_info(dmu_object_type_t bonustype, const void *data,
    zfs_file_info_t *zoi)
{
	if (bonustype != DMU_OT_SA)
		return (SET_ERROR(ENOENT));

	zoi->zfi_project = ZFS_DEFAULT_PROJID;

	/*
	 * If we have a NULL data pointer
	 * then assume the id's aren't changing and
	 * return EEXIST to the dmu to let it know to
	 * use the same ids
	 */
	if (data == NULL)
		return (SET_ERROR(EEXIST));

	const sa_hdr_phys_t *sap = data;
	if (sap->sa_magic == 0) {
		/*
		 * This should only happen for newly created files
		 * that haven't had the znode data filled in yet.
		 */
		zoi->zfi_user = 0;
		zoi->zfi_group = 0;
		zoi->zfi_generation = 0;
		return (0);
	}

	sa_hdr_phys_t sa = *sap;
	boolean_t swap = B_FALSE;
	if (sa.sa_magic == BSWAP_32(SA_MAGIC)) {
		sa.sa_magic = SA_MAGIC;
		sa.sa_layout_info = BSWAP_16(sa.sa_layout_info);
		swap = B_TRUE;
	}
	VERIFY3U(sa.sa_magic, ==, SA_MAGIC);

	int hdrsize = sa_hdrsize(&sa);
	VERIFY3U(hdrsize, >=, sizeof (sa_hdr_phys_t));

	uintptr_t data_after_hdr = (uintptr_t)data + hdrsize;
	uint32_t zfi_user = *((uint32_t *)(data_after_hdr + UZFS_UID_OFFSET));
	uint32_t zfi_group = *((uint32_t *)(data_after_hdr + UZFS_GID_OFFSET));
	zoi->zfi_generation = *((uint64_t *)(data_after_hdr + UZFS_GEN_OFFSET));

	if (swap) {
		zfi_user = BSWAP_32(zfi_user);
		zfi_group = BSWAP_32(zfi_group);
		zoi->zfi_project = BSWAP_64(zoi->zfi_project);
		zoi->zfi_generation = BSWAP_64(zoi->zfi_generation);
	}
	zoi->zfi_user = zfi_user;
	zoi->zfi_group = zfi_group;
	return (0);
}

libuzfs_dataset_handle_t *
libuzfs_dataset_open(const char *dsname)
{
	int err = 0;
	libuzfs_dataset_handle_t *dhp = NULL;
	objset_t *os = NULL;
	zilog_t *zilog = NULL;

	dhp = umem_alloc(sizeof (libuzfs_dataset_handle_t), UMEM_NOFAIL);

	err = libuzfs_dmu_objset_own(dsname, DMU_OST_ZFS, B_FALSE, B_TRUE,
	    dhp, &os);
	if (err)
		return (NULL);

	libuzfs_dhp_init(dhp, os);
	dmu_objset_register_type(DMU_OST_ZFS, uzfs_get_file_info);

	zilog = dhp->zilog;

	zil_replay(os, dhp, libuzfs_replay_vector);

	zilog = zil_open(os, libuzfs_get_data);

	libuzfs_setup_dataset_sa(dhp);
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
	int err = 0;
	dmu_tx_t *tx;

	err = dmu_free_long_range(dhp->os, obj, offset, size);
	if (err)
		return (err);

	tx = dmu_tx_create(dhp->os);
	err = dmu_tx_assign(tx, TXG_WAIT);
	if (err) {
		dmu_tx_abort(tx);
		return (err);
	}

	libuzfs_log_truncate(dhp->zilog, tx, TX_TRUNCATE, obj, offset, size);
	dmu_tx_commit(tx);
	zil_commit(dhp->zilog, obj);

	return (0);
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
    dmu_tx_t *tx)
{
	// create/claim object
	objset_t *os = dhp->os;
	int dnodesize = dmu_objset_dnodesize(os);
	int bonuslen = DN_BONUS_SIZE(dnodesize);
	if (type == INODE_FILE) {
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
	} else {
		if (claiming) {
			ASSERT(*obj != 0);
			VERIFY0(zap_create_claim_dnsize(os, *obj,
			    DMU_OT_DIRECTORY_CONTENTS, DMU_OT_SA,
			    bonuslen, dnodesize, tx));
		} else {
			*obj = zap_create_dnsize(os, DMU_OT_DIRECTORY_CONTENTS,
			    DMU_OT_SA, bonuslen, dnodesize, tx);
		}
	}

	sa_handle_t *sa_hdl;
	VERIFY0(sa_handle_get(os, *obj, NULL, SA_HDL_PRIVATE, &sa_hdl));
	libuzfs_object_attr_init(dhp, sa_hdl, tx);
	sa_handle_destroy(sa_hdl);
}

static int
libuzfs_create_inode_with_type(libuzfs_dataset_handle_t *dhp, uint64_t *obj,
    boolean_t claiming, libuzfs_inode_type_t type, uint64_t *txg)
{
	objset_t *os = dhp->os;
	dmu_tx_t *tx = dmu_tx_create(os);
	dmu_tx_hold_sa_create(tx, sizeof (uzfs_attr_t));
	if (type == INODE_DIR) {
		dmu_tx_hold_zap(tx, DMU_NEW_OBJECT, B_TRUE, NULL);
	}
	int err = dmu_tx_assign(tx, TXG_WAIT);
	if (err != 0) {
		dmu_tx_abort(tx);
		return (err);
	}

	libuzfs_create_inode_with_type_impl(dhp, obj, claiming, type, tx);

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
libuzfs_object_create(libuzfs_dataset_handle_t *dhp, uint64_t *obj,
    uint64_t *gen)
{
	int err = 0;
	dmu_tx_t *tx = NULL;
	objset_t *os = dhp->os;
	dmu_buf_t *db = NULL;

	tx = dmu_tx_create(os);

	dmu_tx_hold_bonus(tx, DMU_NEW_OBJECT);

	err = dmu_tx_assign(tx, TXG_WAIT);
	if (err) {
		dmu_tx_abort(tx);
		goto out;
	}

	int dnodesize = dmu_objset_dnodesize(os);
	int bonuslen = DN_BONUS_SIZE(dnodesize);
	int blocksize = 0;
	int ibshift = 0;

	*obj = dmu_object_alloc_dnsize(os, DMU_OT_PLAIN_FILE_CONTENTS, 0,
	    DMU_OT_PLAIN_OTHER, bonuslen, dnodesize, tx);

	VERIFY0(dmu_object_set_blocksize(os, *obj, blocksize, ibshift, tx));

	libuzfs_log_create(dhp->zilog, tx, *obj);

	uzfs_object_attr_t attr;
	attr.gen = tx->tx_txg;
	attr.size = 0;

	VERIFY0(dmu_bonus_hold(os, *obj, FTAG, &db));
	dmu_buf_will_dirty(db, tx);
	VERIFY0(dmu_set_bonus(db, sizeof (uzfs_object_attr_t), tx));

	bcopy(&attr, db->db_data, sizeof (uzfs_object_attr_t));
	dmu_buf_rele(db, FTAG);

	if (gen != NULL) {
		*gen = tx->tx_txg;
	}

	dmu_tx_commit(tx);
	zil_commit(dhp->zilog, *obj);

out:
	return (err);
}

/*
 * object deletion is always SYNC, recorded in zil
 */
int
libuzfs_object_delete(libuzfs_dataset_handle_t *dhp, uint64_t obj)
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

	VERIFY0(dmu_object_free(os, obj, tx));

	libuzfs_log_remove(dhp->zilog, tx, obj);
	dmu_tx_commit(tx);
	zil_commit(dhp->zilog, obj);

out:
	return (err);
}

int
libuzfs_object_claim(libuzfs_dataset_handle_t *dhp, uint64_t obj)
{
	int err = 0;
	dmu_tx_t *tx = NULL;
	objset_t *os = dhp->os;
	dmu_buf_t *db = NULL;

	int dnodesize = dmu_objset_dnodesize(os);
	int bonuslen = DN_BONUS_SIZE(dnodesize);
	int blocksize = 0;
	int ibs = 0;

	err = dnode_try_claim(os, obj, dnodesize >> DNODE_SHIFT);

	// FIXME(hping): double comfirm the waived error codes
	if (err == ENOSPC || err == EEXIST) {
		printf("object %ld already created\n", obj);
		return (0);
	} else if (err != 0) {
		return (err);
	}

	tx = dmu_tx_create(os);

	dmu_tx_hold_bonus(tx, DMU_NEW_OBJECT);

	err = dmu_tx_assign(tx, TXG_WAIT);
	if (err)
		goto out;

	VERIFY0(dmu_object_claim_dnsize(os, obj, DMU_OT_PLAIN_FILE_CONTENTS, 0,
	    DMU_OT_PLAIN_OTHER, bonuslen, dnodesize, tx));

	VERIFY0(dmu_object_set_blocksize(os, obj, blocksize, ibs, tx));

	uzfs_object_attr_t attr;
	attr.gen = tx->tx_txg;

	VERIFY0(dmu_bonus_hold(os, obj, FTAG, &db));
	dmu_buf_will_dirty(db, tx);
	VERIFY0(dmu_set_bonus(db, sizeof (uzfs_object_attr_t), tx));

	bcopy(&attr, db->db_data, sizeof (uzfs_object_attr_t));
	dmu_buf_rele(db, FTAG);

	dmu_tx_commit(tx);

	return (0);

out:
	dmu_tx_abort(tx);
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
			printf("skip obj w/o bonus buf: %ld\n", obj);
			continue;
		} else {
			printf("object: %ld\n", obj);
		}
		i++;
	}

	return (i);
}

int
libuzfs_object_getattr(libuzfs_dataset_handle_t *dhp, uint64_t obj,
    uzfs_object_attr_t *attr)
{
	int err = 0;
	objset_t *os = dhp->os;
	dmu_buf_t *db;

	err = dmu_bonus_hold(os, obj, FTAG, &db);
	if (err)
		return (err);

	bcopy(db->db_data, attr, sizeof (uzfs_object_attr_t));
	dmu_buf_rele(db, FTAG);

	return (0);
}

int
libuzfs_object_get_gen(libuzfs_dataset_handle_t *dhp, uint64_t obj,
    uint64_t *gen)
{
	int err = 0;
	uzfs_object_attr_t attr;
	memset(&attr, 0, sizeof (uzfs_object_attr_t));
	err = libuzfs_object_getattr(dhp, obj, &attr);
	if (err != 0)
		return (err);

	if (gen != NULL)
		*gen = attr.gen;

	return (0);
}

int
libuzfs_object_get_size(libuzfs_dataset_handle_t *dhp, uint64_t obj,
    uint64_t *size)
{
	int err = 0;
	uzfs_object_attr_t attr;
	memset(&attr, 0, sizeof (uzfs_object_attr_t));
	err = libuzfs_object_getattr(dhp, obj, &attr);
	if (err != 0)
		return (err);

	if (size != NULL)
		*size = attr.size;

	return (0);
}

int
libuzfs_object_write(libuzfs_dataset_handle_t *dhp, uint64_t obj,
    uint64_t offset, uint64_t size, const char *buf, boolean_t sync)
{
	int err = 0;
	objset_t *os = dhp->os;
	zilog_t *zilog = dhp->zilog;
	dmu_tx_t *tx = NULL;
	uzfs_object_attr_t *attr = NULL;
	dmu_buf_t *db;

	err = dmu_bonus_hold(os, obj, FTAG, &db);
	if (err)
		return (err);

	attr = db->db_data;

	tx = dmu_tx_create(os);

	dmu_tx_hold_write(tx, obj, offset, size);

	err = dmu_tx_assign(tx, TXG_WAIT);
	if (err) {
		dmu_tx_abort(tx);
		goto out;
	}

	dmu_write(os, obj, offset, size, buf, tx);

	libuzfs_log_write(dhp, tx, TX_WRITE, obj, offset, size, sync);

	if (offset + size > attr->size) {
		attr->size = offset + size;
		dmu_buf_will_dirty(db, tx);
	}

	dmu_tx_commit(tx);

	if (sync)
		zil_commit(zilog, obj);

out:
	dmu_buf_rele(db, FTAG);
	return (err);
}

int
libuzfs_object_read(libuzfs_dataset_handle_t *dhp, uint64_t obj,
    uint64_t offset, uint64_t size, char *buf)
{
	objset_t *os = dhp->os;
	return (dmu_read(os, obj, offset, size, buf, DMU_READ_NO_PREFETCH));
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
	    B_FALSE, type, txg));
}

int
libuzfs_inode_claim(libuzfs_dataset_handle_t *dhp, uint64_t ino,
    libuzfs_inode_type_t type)
{
	int err = 0;
	int dnodesize = dmu_objset_dnodesize(dhp->os);

	err = dnode_try_claim(dhp->os, ino, dnodesize >> DNODE_SHIFT);

	// FIXME(hping): double comfirm the waived error codes
	if (err == ENOSPC || err == EEXIST) {
		printf("object %ld already created\n", ino);
		return (0);
	} else if (err != 0) {
		return (err);
	}

	return (libuzfs_create_inode_with_type(dhp, &ino,
	    B_TRUE, type, NULL));
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
			printf("zap_readdir: bad directory entry, obj = %ld,"
			    " whence = %ld, length = %d, num = %ld\n",
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
