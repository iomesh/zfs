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

#include "libuzfs_impl.h"

static boolean_t change_zpool_cache_path = B_FALSE;

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

// TODO(hping): add zil support
zil_replay_func_t *libuzfs_replay_vector[TX_MAX_TYPE] = {
	NULL,			/* 0 no such transaction type */
	NULL,			/* TX_CREATE */
	NULL,			/* TX_MKDIR */
	NULL,			/* TX_MKXATTR */
	NULL,			/* TX_SYMLINK */
	NULL,			/* TX_REMOVE */
	NULL,			/* TX_RMDIR */
	NULL,			/* TX_LINK */
	NULL,			/* TX_RENAME */
	NULL,			/* TX_WRITE */
	NULL,			/* TX_TRUNCATE */
	NULL,			/* TX_SETATTR */
	NULL,			/* TX_ACL */
	NULL,			/* TX_CREATE_ACL */
	NULL,			/* TX_CREATE_ATTR */
	NULL,			/* TX_CREATE_ACL_ATTR */
	NULL,			/* TX_MKDIR_ACL */
	NULL,			/* TX_MKDIR_ATTR */
	NULL,			/* TX_MKDIR_ACL_ATTR */
	NULL,			/* TX_WRITE2 */
};

/*
 * ZIL get_data callbacks
 */

static int
libuzfs_get_data(void *arg, uint64_t arg2, lr_write_t *lr, char *buf,
    struct lwb *lwb, zio_t *zio)
{
	return (0);
}

void
libuzfs_init()
{
	kernel_init(SPA_MODE_READ | SPA_MODE_WRITE);
}

void
libuzfs_fini()
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
	int err = 0;
	uint64_t sb_obj = 0;

	err = zap_create_claim(os, MASTER_NODE_OBJ, DMU_OT_MASTER_NODE,
	    DMU_OT_NONE, 0, tx);
	ASSERT(err == 0);

	int dnodesize = dmu_objset_dnodesize(os);
	int bonuslen = DN_BONUS_SIZE(dnodesize);

	sb_obj = zap_create_dnsize(os, DMU_OT_DIRECTORY_CONTENTS, DMU_OT_PLAIN_OTHER,
	    bonuslen, dnodesize, tx);

	err = zap_add(os, MASTER_NODE_OBJ, UZFS_SB_OBJ, 8, 1, &sb_obj, tx);
	ASSERT(err == 0);
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

libuzfs_dataset_handle_t *
libuzfs_dataset_open(const char *dsname)
{
	libuzfs_dataset_handle_t *dhp = NULL;
	objset_t *os = NULL;
	zilog_t *zilog = NULL;

	dhp = umem_alloc(sizeof (libuzfs_dataset_handle_t), UMEM_NOFAIL);

	VERIFY0(libuzfs_dmu_objset_own(dsname, DMU_OST_ZFS, B_FALSE, B_TRUE,
	    dhp, &os));

	libuzfs_dhp_init(dhp, os);

	zilog = dhp->zilog;

	zil_replay(os, dhp, libuzfs_replay_vector);

	zilog = zil_open(os, libuzfs_get_data);

	return (dhp);
}

void
libuzfs_dataset_close(libuzfs_dataset_handle_t *dhp)
{
	zil_close(dhp->zilog);
	dmu_objset_disown(dhp->os, B_TRUE, dhp);
	libuzfs_dhp_fini(dhp);
	free(dhp);
}

int
libuzfs_dataset_get_superblock_ino(libuzfs_dataset_handle_t *dhp, uint64_t *sb_ino)
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

uint64_t libuzfs_get_max_synced_opid(libuzfs_dataset_handle_t *dhp)
{
	return dsl_dataset_get_max_synced_opid(dmu_objset_ds(dhp->os));
}

void libuzfs_dump_txg_opids(libuzfs_dataset_handle_t *dhp)
{
	dsl_dataset_dump_txg_opids(dmu_objset_ds(dhp->os));
}

uint64_t libuzfs_get_last_synced_txg(libuzfs_dataset_handle_t *dhp)
{
	return spa_last_synced_txg(dhp->os->os_spa);
}

void libuzfs_wait_synced(libuzfs_dataset_handle_t *dhp) {
	txg_wait_synced(spa_get_dsl(dhp->os->os_spa), 0);
}

int
libuzfs_object_create(libuzfs_dataset_handle_t *dhp, uint64_t *obj, uint64_t *txg)
{
	int err = 0;
	dmu_tx_t *tx = NULL;
	objset_t *os = dhp->os;

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

	*txg = tx->tx_txg;
	dmu_tx_commit(tx);

out:
	return (err);
}

int
libuzfs_object_delete(libuzfs_dataset_handle_t *dhp, uint64_t obj, uint64_t *txg)
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

	*txg = tx->tx_txg;
	dmu_tx_commit(tx);

out:
	return (err);
}

int
libuzfs_object_claim(libuzfs_dataset_handle_t *dhp, uint64_t obj)
{
	int err = 0;
	dmu_tx_t *tx = NULL;
	objset_t *os = dhp->os;

	int dnodesize = dmu_objset_dnodesize(os);
	int bonuslen = DN_BONUS_SIZE(dnodesize);
	int blocksize = 0;
	int ibs = 0;

	tx = dmu_tx_create(os);

	dmu_tx_hold_bonus(tx, DMU_NEW_OBJECT);

	err = dmu_tx_assign(tx, TXG_WAIT);
	if (err) {
		dmu_tx_abort(tx);
		goto out;
	}

	err = dmu_object_claim_dnsize(os, obj, DMU_OT_PLAIN_FILE_CONTENTS, 0,
		DMU_OT_PLAIN_OTHER, bonuslen, dnodesize, tx);
	if (err)
		goto out;

	VERIFY0(dmu_object_set_blocksize(os, obj, blocksize, ibs, tx));

	dmu_tx_commit(tx);

out:
	return (err);
}

int
TEST_libuzfs_object_claim(libuzfs_dataset_handle_t *dhp, uint64_t obj, uint64_t *txg)
{
	int err = 0;
	dmu_tx_t *tx = NULL;
	objset_t *os = dhp->os;

	int dnodesize = dmu_objset_dnodesize(os);
	int bonuslen = DN_BONUS_SIZE(dnodesize);
	int type = DMU_OT_UINT64_OTHER;
	int bonus_type = DMU_OT_UINT64_OTHER;
	int blocksize = 0;
	int ibs = 0;

	tx = dmu_tx_create(os);

	dmu_tx_hold_bonus(tx, DMU_NEW_OBJECT);

	err = dmu_tx_assign(tx, TXG_WAIT);
	if (err) {
		dmu_tx_abort(tx);
		goto out;
	}

	err = dmu_object_claim_dnsize(os, obj, type, 0, bonus_type, bonuslen,
	    dnodesize, tx);
	if (err)
		goto out;

	VERIFY0(dmu_object_set_blocksize(os, obj, blocksize, ibs, tx));

	*txg = tx->tx_txg;
	dmu_tx_commit(tx);

out:
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
libuzfs_object_getattr(libuzfs_dataset_handle_t *dhp, uint64_t obj, void *attr, uint64_t size)
{
	objset_t *os = dhp->os;
	dmu_buf_t *db;

	VERIFY0(dmu_bonus_hold(os, obj, FTAG, &db));

	bcopy(db->db_data + 8, attr, size);
	dmu_buf_rele(db, FTAG);

	return 0;
}

int
libuzfs_object_setattr(libuzfs_dataset_handle_t *dhp, uint64_t obj, const void *attr, uint64_t size,
    uint64_t *txg)
{
	int err = 0;
	objset_t *os = dhp->os;
	dmu_buf_t *db;
	dmu_tx_t *tx;

	tx = dmu_tx_create(os);

	dmu_tx_hold_bonus(tx, obj);

	err = dmu_tx_assign(tx, TXG_WAIT);
	if (err) {
		dmu_buf_rele(db, FTAG);
		dmu_tx_abort(tx);
		goto out;
	}

	VERIFY0(dmu_bonus_hold(os, obj, FTAG, &db));

	// 8 Bytes for kv obj
	ASSERT(size + 8 < db->db_size);

	dmu_buf_will_dirty(db, tx);

	VERIFY0(dmu_set_bonus(db, size + 8, tx));
	bcopy(attr, db->db_data + 8, size);
	dmu_buf_rele(db, FTAG);

	*txg = tx->tx_txg;
	dmu_tx_commit(tx);

out:
	return err;
}

int
libuzfs_object_write(libuzfs_dataset_handle_t *dhp, uint64_t obj,
    uint64_t offset, uint64_t size, const char *buf)
{
	int err = 0;
	objset_t *os = dhp->os;
	dmu_tx_t *tx = NULL;

	tx = dmu_tx_create(os);

	dmu_tx_hold_write(tx, obj, offset, size);

	err = dmu_tx_assign(tx, TXG_WAIT);
	if (err) {
		dmu_tx_abort(tx);
		goto out;
	}

	dmu_write(os, obj, offset, size, buf, tx);

	dmu_tx_commit(tx);
	txg_wait_synced(spa_get_dsl(os->os_spa), 0);

out:
	return (err);
}

int
libuzfs_object_read(libuzfs_dataset_handle_t *dhp, uint64_t obj,
    uint64_t offset, uint64_t size, char *buf)
{
	int err = 0;
	objset_t *os = dhp->os;
	dmu_object_info_t doi;

	memset(&doi, 0, sizeof (doi));

	err = libuzfs_object_stat(dhp, obj, &doi);
	if (err)
		return (err);

	err = dmu_read(os, obj, offset, size, buf, DMU_READ_NO_PREFETCH);
	if (err)
		return (err);

	return (0);
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

	err = zap_create_claim_dnsize(os, obj, DMU_OT_DIRECTORY_CONTENTS,
	    DMU_OT_PLAIN_OTHER, bonuslen, dnodesize, tx);
	if (err) {
		dmu_tx_abort(tx);
		goto out;
	}

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
libuzfs_zap_add(libuzfs_dataset_handle_t *dhp, uint64_t obj, const char *key, int integer_size,
		uint64_t num_integers, const void *val, uint64_t *txg)
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
	if (err) {
		dmu_tx_abort(tx);
		goto out;
	}

	*txg = tx->tx_txg;
	dmu_tx_commit(tx);

out:
	return (err);
}

int
libuzfs_zap_remove(libuzfs_dataset_handle_t *dhp, uint64_t obj, const char *key, uint64_t *txg)
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
	if (err) {
		dmu_tx_abort(tx);
		goto out;
	}

	*txg = tx->tx_txg;
	dmu_tx_commit(tx);

out:
	return (err);
}

int
libuzfs_zap_update(libuzfs_dataset_handle_t *dhp, uint64_t obj, const char *key, int integer_size,
		   uint64_t num_integers, const void *val, uint64_t *txg)
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
	if (err) {
		dmu_tx_abort(tx);
		goto out;
	}

	*txg = tx->tx_txg;
	dmu_tx_commit(tx);

out:
	return (err);
}

int
libuzfs_zap_lookup(libuzfs_dataset_handle_t *dhp, uint64_t obj, const char *key, int integer_size,
		   uint64_t num_integers, void *val)
{
	return zap_lookup(dhp->os, obj, key, integer_size, num_integers, val);

}

int
libuzfs_zap_count(libuzfs_dataset_handle_t *dhp, uint64_t obj, uint64_t *count)
{
	return zap_count(dhp->os, obj, count);
}

int libuzfs_inode_create(libuzfs_dataset_handle_t *dhp, uint64_t *ino, libuzfs_inode_type_t type, uint64_t *txg)
{
	if (type == INODE_FILE)
		return libuzfs_object_create(dhp, ino, txg);

	if (type == INODE_DIR)
		return libuzfs_zap_create(dhp, ino, txg);

	return EINVAL;
}

int libuzfs_inode_claim(libuzfs_dataset_handle_t *dhp, uint64_t ino, libuzfs_inode_type_t type)
{
	if (type == INODE_FILE)
		return libuzfs_object_claim(dhp, ino);

	if (type == INODE_DIR)
		return libuzfs_zap_claim(dhp, ino);

	return EINVAL;
}

static int
libuzfs_inode_kvobj_delete(libuzfs_dataset_handle_t *dhp, uint64_t ino, libuzfs_inode_type_t type,
    uint64_t kvobj, uint64_t *txg)
{
	int err = 0;
	dmu_tx_t *tx = NULL;
	objset_t *os = dhp->os;

	tx = dmu_tx_create(os);

	dmu_tx_hold_free(tx, kvobj, 0, DMU_OBJECT_END);
	dmu_tx_hold_free(tx, ino, 0, DMU_OBJECT_END);

	err = dmu_tx_assign(tx, TXG_WAIT);
	if (err) {
		dmu_tx_abort(tx);
		goto out;
	}

	VERIFY0(zap_destroy(os, kvobj, tx));

	if (type == INODE_FILE)
		VERIFY0(dmu_object_free(os, ino, tx));
	else if (type == INODE_DIR)
		VERIFY0(zap_destroy(os, ino, tx));

	*txg = tx->tx_txg;
	dmu_tx_commit(tx);

out:
	return (err);
}

int
libuzfs_inode_delete(libuzfs_dataset_handle_t *dhp, uint64_t ino, libuzfs_inode_type_t type,
    uint64_t *txg)
{
	if (type != INODE_FILE && type != INODE_DIR)
		return EINVAL;

	uint64_t kvobj = 0;
	VERIFY0(libuzfs_inode_get_kvobj(dhp, ino, &kvobj));

	if (kvobj == 0) {
		if (type == INODE_FILE)
			return libuzfs_object_delete(dhp, ino, txg);
		if (type == INODE_DIR)
			return libuzfs_zap_delete(dhp, ino, txg);
	}

	return libuzfs_inode_kvobj_delete(dhp, ino, type, kvobj, txg);
}

int
libuzfs_inode_getattr(libuzfs_dataset_handle_t *dhp, uint64_t ino, void *attr, uint64_t size)
{
	return libuzfs_object_getattr(dhp, ino, attr, size);
}

int
libuzfs_inode_setattr(libuzfs_dataset_handle_t *dhp, uint64_t ino, const void *attr, uint64_t size,
    uint64_t *txg)
{
	return libuzfs_object_setattr(dhp, ino, attr, size, txg);
}

static int
libuzfs_object_kvattr_create_add(libuzfs_dataset_handle_t *dhp, uint64_t ino,
    const char *key, const char *value, uint64_t size, int flags, uint64_t *txg)
{
	int err = 0;
	dmu_tx_t *tx = NULL;
	objset_t *os = dhp->os;
	dmu_buf_t *db;
	uint64_t kvobj = 0;

	tx = dmu_tx_create(os);

	dmu_tx_hold_zap(tx, DMU_NEW_OBJECT, B_TRUE, NULL);
	dmu_tx_hold_bonus(tx, ino);

	err = dmu_tx_assign(tx, TXG_WAIT);
	if (err) {
		dmu_tx_abort(tx);
		goto out;
	}

	int dnodesize = dmu_objset_dnodesize(os);
	int bonuslen = DN_BONUS_SIZE(dnodesize);

	kvobj = zap_create_dnsize(os, DMU_OT_DIRECTORY_CONTENTS,
	    DMU_OT_PLAIN_OTHER, bonuslen, dnodesize, tx);

	err = zap_add(os, kvobj, key, 1, size, value, tx);
	if (err) {
		dmu_tx_abort(tx);
		goto out;
	}

	VERIFY0(dmu_bonus_hold(os, ino, FTAG, &db));
	dmu_buf_will_dirty(db, tx);
	bcopy(&kvobj, db->db_data, sizeof(kvobj));
	dmu_buf_rele(db, FTAG);

	*txg = tx->tx_txg;
	dmu_tx_commit(tx);

	libuzfs_wait_synced(dhp);

out:
	return (err);
}

int
libuzfs_inode_get_kvobj(libuzfs_dataset_handle_t *dhp, uint64_t ino, uint64_t *kvobj)
{
	dmu_buf_t *db;

	VERIFY0(dmu_bonus_hold(dhp->os, ino, FTAG, &db));
	bcopy(db->db_data, kvobj, sizeof(*kvobj));
	dmu_buf_rele(db, FTAG);

	return 0;
}

int
libuzfs_inode_get_kvattr(libuzfs_dataset_handle_t *dhp, uint64_t ino, const char *name,
    char *value, uint64_t size, int flags)
{
	int err = 0;
	uint64_t kvobj;

	err = libuzfs_inode_get_kvobj(dhp, ino, &kvobj);
	if (err)
		return err;

	if (kvobj == 0)
		return ENOENT;

	return libuzfs_zap_lookup(dhp, kvobj, name, 1, size, value);
}

// TODO(hping): remove kvobj when no kv attr
int
libuzfs_inode_remove_kvattr(libuzfs_dataset_handle_t *dhp, uint64_t ino, const char *name,
    uint64_t *txg)
{
	int err = 0;
	uint64_t kvobj;

	err = libuzfs_inode_get_kvobj(dhp, ino, &kvobj);
	if (err)
		return err;

	if (kvobj == 0)
		return ENOENT;

	return libuzfs_zap_remove(dhp, kvobj, name, txg);
}

int
libuzfs_inode_set_kvattr(libuzfs_dataset_handle_t *dhp, uint64_t ino, const char *name,
    const char *value, uint64_t size, int flags, uint64_t *txg)
{
	int err = 0;
	uint64_t kvobj;

	err = libuzfs_inode_get_kvobj(dhp, ino, &kvobj);
	if (err)
		return err;

	if (kvobj == 0)
		return libuzfs_object_kvattr_create_add(dhp, ino, name, value, size, flags, txg);

	return libuzfs_zap_update(dhp, kvobj, name, 1, size, value, txg);
}

int libuzfs_dentry_create(libuzfs_dataset_handle_t *dhp, uint64_t dino, const char *name,
    uint64_t *value, uint64_t num, uint64_t *txg)
{
	return libuzfs_zap_add(dhp, dino, name, 8, num, value, txg);
}

int libuzfs_dentry_delete(libuzfs_dataset_handle_t *dhp, uint64_t dino, const char *name,
    uint64_t *txg)
{
	return libuzfs_zap_remove(dhp, dino, name, txg);
}

int libuzfs_dentry_lookup(libuzfs_dataset_handle_t *dhp, uint64_t dino, const char *name,
    uint64_t *value, uint64_t num)
{
	return libuzfs_zap_lookup(dhp, dino, name, 8, num, value);
}

// FIXME(hping)
#define MAX_NUM_FS (100)
static zfsvfs_t *zfsvfs_array[MAX_NUM_FS];
static int zfsvfs_idx = 0;
static znode_t *rootzp = NULL;

static void libuzfs_vap_init(vattr_t *vap, struct inode *dir, umode_t mode, cred_t *cr)
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
	return dmu_objset_create(fsname, DMU_OST_ZFS, 0, NULL, libuzfs_fs_create_cb, NULL);
}

void
libuzfs_fs_destroy(const char *fsname)
{
	(void) dmu_objset_find(fsname, libuzfs_objset_destroy_cb, NULL,
	    DS_FIND_SNAPSHOTS | DS_FIND_CHILDREN);
}

int libuzfs_fs_init(const char* fsname, uint64_t *fsid)
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

	sb = kmem_zalloc(sizeof(struct super_block), KM_SLEEP);
	sb->s_fs_info = zfsvfs;

	zfsvfs->z_sb = sb;

	error = zfsvfs_setup(zfsvfs, B_TRUE);
	if (error) goto out;

	*fsid = zfsvfs_idx;

	zfsvfs_array[zfsvfs_idx++] = zfsvfs;

	return 0;

out:
	if (sb)
		kmem_free(sb, sizeof (struct super_block));
	if(vfs)
		kmem_free(vfs, sizeof (vfs_t));
	if(zfsvfs)
		kmem_free(zfsvfs, sizeof (zfsvfs_t));
	return -1;
}

int libuzfs_fs_fini(uint64_t fsid)
{
	zfsvfs_t *zfsvfs = zfsvfs_array[fsid];
	objset_t *os = zfsvfs->z_os;
	vfs_t *vfs = zfsvfs->z_vfs;
	struct super_block *sb = zfsvfs->z_sb;

	struct inode* root_inode = NULL;
	int error = zfs_root(zfsvfs, &root_inode);
	if (error) return 1;
	iput(root_inode);
	iput(root_inode);
	// sleep 1 second for zfsvfs draining, otherwise may hit first assert in zfs_unlinked_drain_task
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
	if(vfs)
		kmem_free(vfs, sizeof (vfs_t));
	if(zfsvfs)
		kmem_free(zfsvfs, sizeof (zfsvfs_t));

	return 0;
}

int libuzfs_getroot(uint64_t fsid, uint64_t* ino)
{
	int error = 0;
	struct inode* root_inode = NULL;
	zfsvfs_t *zfsvfs = zfsvfs_array[fsid];

	error = zfs_root(zfsvfs, &root_inode);
	if (error) goto out;

	rootzp = ITOZ(root_inode);
	*ino = root_inode->i_ino;

out:
	return error;
}

static int cp_new_stat(struct linux_kstat *stat, struct stat *statbuf)
{
	struct stat tmp;

	tmp.st_ino = stat->ino;
	if (sizeof(tmp.st_ino) < sizeof(stat->ino) && tmp.st_ino != stat->ino)
		return -EOVERFLOW;
	tmp.st_mode = stat->mode;
	tmp.st_nlink = stat->nlink;
	if (tmp.st_nlink != stat->nlink)
		return -EOVERFLOW;
	tmp.st_uid = stat->uid;
	tmp.st_gid = stat->gid;
	tmp.st_size = stat->size;
	tmp.st_atime = stat->atime.tv_sec;
	tmp.st_mtime = stat->mtime.tv_sec;
	tmp.st_ctime = stat->ctime.tv_sec;
	tmp.st_blocks = stat->blocks;
	tmp.st_blksize = stat->blksize;
	memcpy(statbuf,&tmp,sizeof(tmp));
	return 0;
}

int libuzfs_getattr(uint64_t fsid, uint64_t ino, struct stat* stat)
{
	int error = 0;
	znode_t *zp = NULL;
	zfsvfs_t *zfsvfs = zfsvfs_array[fsid];

	ZFS_ENTER(zfsvfs);

	error = zfs_zget(zfsvfs, ino, &zp);
	if (error) goto out;

	struct linux_kstat kstatbuf;
	memset(&kstatbuf, 0, sizeof(struct linux_kstat));

	error = zfs_getattr_fast(NULL, ZTOI(zp), &kstatbuf);
	if (error) goto out;

	cp_new_stat(&kstatbuf, stat);

out:
	if (zp)
		iput(ZTOI(zp));

	ZFS_EXIT(zfsvfs);
	return error;
}

int libuzfs_lookup(uint64_t fsid, uint64_t dino, char* name, uint64_t* ino)
{
	int error = 0;
	znode_t *dzp = NULL;
	znode_t *zp = NULL;
	zfsvfs_t *zfsvfs = zfsvfs_array[fsid];

	ZFS_ENTER(zfsvfs);

	error = zfs_zget(zfsvfs, dino, &dzp);
	if (error) goto out;

	error = zfs_lookup(dzp, name, &zp, 0, NULL, NULL, NULL);
	if (error) goto out;

	*ino = ZTOI(zp)->i_ino;

out:
	if (zp)
		iput(ZTOI(zp));

	if (dzp)
		iput(ZTOI(dzp));

	ZFS_EXIT(zfsvfs);
	return error;
}

int libuzfs_mkdir(uint64_t fsid, uint64_t dino, char* name, umode_t mode, uint64_t *ino)
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
	return error;
}

int libuzfs_rmdir(uint64_t fsid, uint64_t dino, char* name)
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
	return error;
}

#define	ZPL_DIR_CONTEXT_INIT(_dirent, _actor, _pos) {	\
	.dirent = _dirent,				\
	.actor = _actor,				\
	.pos = _pos,					\
}

int libuzfs_readdir(uint64_t fsid, uint64_t ino, void *dirent, filldir_t filldir, loff_t pos)
{
	int error = 0;
	znode_t *zp = NULL;
	zfsvfs_t *zfsvfs = zfsvfs_array[fsid];

	ZFS_ENTER(zfsvfs);

	error = zfs_zget(zfsvfs, ino, &zp);
	if (error) goto out;

	zpl_dir_context_t ctx = ZPL_DIR_CONTEXT_INIT(dirent, filldir, pos);

	error = zfs_readdir(ZTOI(zp), &ctx, NULL);
	if (error) goto out;

out:
	if (zp)
		iput(ZTOI(zp));

	ZFS_EXIT(zfsvfs);
	return error;
}

int libuzfs_create(uint64_t fsid, uint64_t dino, char* name, umode_t mode, uint64_t *ino)
{
	int error = 0;
	znode_t *dzp = NULL;
	znode_t *zp = NULL;
	zfsvfs_t *zfsvfs = zfsvfs_array[fsid];

	ZFS_ENTER(zfsvfs);

	error = zfs_zget(zfsvfs, dino, &dzp);
	if (error) goto out;

	vattr_t vap;
	libuzfs_vap_init(&vap, ZTOI(dzp), mode | S_IFREG, NULL);

	error = zfs_create(dzp, name, &vap, 0, mode, &zp, NULL, 0, NULL);
	if (error) goto out;

	*ino = ZTOI(zp)->i_ino;

out:
	if (zp)
		iput(ZTOI(zp));

	if (dzp)
		iput(ZTOI(dzp));

	ZFS_EXIT(zfsvfs);
	return error;
}

int libuzfs_remove(uint64_t fsid, uint64_t dino, char* name)
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
	return error;
}

int libuzfs_rename(uint64_t fsid, uint64_t sdino, char* sname, uint64_t tdino, char* tname)
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
	return error;
}

int libuzfs_read(uint64_t fsid, uint64_t ino, zfs_uio_t *uio, int ioflag)
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
	return error;
}

int libuzfs_write(uint64_t fsid, uint64_t ino, zfs_uio_t *uio, int ioflag)
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
	return error;
}

int libuzfs_fsync(uint64_t fsid, uint64_t ino, int syncflag)
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
	return error;
}
