#include "libuzfs.h"
#include "sys/spa.h"
#include "sys/stdtypes.h"
#include "sys/zfs_context.h"
#include "sys/zil.h"
#include <bits/stdint-uintn.h>
#include <sys/sa.h>
#include <sys/dbuf.h>
#include <time.h>
#include <sys/zap.h>
#include <sys/dmu.h>
#include <sys/nvpair.h>
#include <umem.h>
#include <libuzfs_impl.h>
#include <sys/dmu_objset.h>
#include <sys/zfs_znode.h>
#include <sys/sa_impl.h>

sa_attr_reg_t uzfs_attr_table[UZFS_END+1] = {
	{"UZFS_GEN", sizeof (uint64_t), SA_UINT64_ARRAY, 0},
	{"UZFS_SIZE", sizeof (uint64_t), SA_UINT64_ARRAY, 1},
	{"UZFS_MTIME", sizeof (uint64_t) * 2, SA_UINT64_ARRAY, 2},
	{"UZFS_ZXATTR", sizeof (uint64_t), SA_UINT64_ARRAY, 3},
	{"UZFS_RESERVED", 0, SA_UINT8_ARRAY, 0},
	{"UZFS_XATTR_HIGH", 0, SA_UINT8_ARRAY, 0},
	{"UZFS_XATTR", 0, SA_UINT8_ARRAY, 0},
	{NULL, 0, 0, 0}
};

static uint32_t
libuzfs_get_max_reserved_len(sa_handle_t *sa_hdl)
{
	ASSERT(sa_hdl->sa_bonus != NULL);
	switch (sa_hdl->sa_bonus->db_size) {
		case UZFS_BONUS_LEN_DEFAULT:
			return (UZFS_MAX_RESERVED_DEFAULT);
		case UZFS_BONUS_LEN_1K:
			return (UZFS_MAX_RESERVED_1K);
		default:
			panic("unexpected bonus len: %lu",
			    sa_hdl->sa_bonus->db_size);
	}
	return (0);
}

static uint32_t
libuzfs_get_max_hp_kvs_capacity(sa_handle_t *sa_hdl)
{
	ASSERT(sa_hdl->sa_bonus != NULL);
	switch (sa_hdl->sa_bonus->db_size) {
		case UZFS_BONUS_LEN_DEFAULT:
			return (UZFS_KV_CAPACITY_DEFAULT);
		case UZFS_BONUS_LEN_1K:
			return (UZFS_KV_CAPACITY_1K);
		default:
			panic("unexpected bonus len: %lu",
			    sa_hdl->sa_bonus->db_size);
	}
	return (0);
}

void
libuzfs_setup_dataset_sa(libuzfs_dataset_handle_t *dhp)
{
	uint64_t sa_obj;
	VERIFY0(zap_lookup(dhp->os, MASTER_NODE_OBJ,
	    ZFS_SA_ATTRS, 8, 1, &sa_obj));
	VERIFY0(sa_setup(dhp->os, sa_obj, uzfs_attr_table,
	    UZFS_END, &dhp->uzfs_attr_table));
}

void
libuzfs_inode_attr_init(libuzfs_inode_handle_t *ihp, dmu_tx_t *tx)
{
	sa_bulk_attr_t sa_attrs[UZFS_END];
	int cnt = 0;
	uint64_t size = 0;
	timespec_t mtime = {0, 0};

	sa_attr_type_t *attr_tbl = ihp->dhp->uzfs_attr_table;

	SA_ADD_BULK_ATTR(sa_attrs, cnt, attr_tbl[UZFS_GEN],
	    NULL, &ihp->gen, sizeof (ihp->gen));

	if (ihp->is_data_inode) {
		SA_ADD_BULK_ATTR(sa_attrs, cnt, attr_tbl[UZFS_SIZE],
		    NULL, &size, sizeof (size));
		SA_ADD_BULK_ATTR(sa_attrs, cnt, attr_tbl[UZFS_MTIME],
		    NULL, &mtime, sizeof (mtime));
	} else {
		SA_ADD_BULK_ATTR(sa_attrs, cnt, attr_tbl[UZFS_RESERVED],
		    NULL, NULL, 0);
	}

	VERIFY0(nvlist_alloc(&ihp->hp_kvattr_cache, NV_UNIQUE_NAME, KM_SLEEP));

	uint64_t xattr_sa_size;
	VERIFY0(nvlist_size(ihp->hp_kvattr_cache,
	    &xattr_sa_size, NV_ENCODE_XDR));

	char *xattr_sa_data = vmem_alloc(xattr_sa_size, KM_SLEEP);
	VERIFY0(nvlist_pack(ihp->hp_kvattr_cache, &xattr_sa_data,
	    &xattr_sa_size, NV_ENCODE_XDR, KM_SLEEP));

	// add high priority kv before normal kv to place it in bonous buffer
	SA_ADD_BULK_ATTR(sa_attrs, cnt, attr_tbl[UZFS_XATTR_HIGH],
	    NULL, xattr_sa_data, xattr_sa_size);
	SA_ADD_BULK_ATTR(sa_attrs, cnt, attr_tbl[UZFS_XATTR],
	    NULL, xattr_sa_data, xattr_sa_size);

	VERIFY0(sa_replace_all_by_template(ihp->sa_hdl, sa_attrs, cnt, tx));

	vmem_free(xattr_sa_data, xattr_sa_size);
}

int
libuzfs_get_xattr_zap_obj(libuzfs_inode_handle_t *ihp, uint64_t *xattr_zap_obj)
{
	sa_attr_type_t *sa_tbl = ihp->dhp->uzfs_attr_table;
	return (sa_lookup(ihp->sa_hdl, sa_tbl[UZFS_ZXATTR], xattr_zap_obj,
	    sizeof (*xattr_zap_obj)));
}

static void
libuzfs_get_object_size(sa_handle_t *sa_hdl, sa_attr_type_t zxattr,
    uint64_t *nblks, uint32_t *blksize)
{
	uint64_t zxattr_obj;
	sa_object_size(sa_hdl, blksize, (u_longlong_t *)nblks);
	if (sa_lookup(sa_hdl, zxattr, &zxattr_obj, sizeof (zxattr_obj)) == 0) {
		dnode_t *dn;
		uint64_t nblks_zxattr;
		uint32_t blksize_zxattr;
		if (dnode_hold(sa_hdl->sa_os, zxattr_obj, FTAG, &dn) == 0) {
			dmu_object_size_from_db((dmu_buf_t *)dn->dn_dbuf,
			    &blksize_zxattr, (u_longlong_t *)&nblks_zxattr);
			*nblks += nblks_zxattr;
		}
	}
}

int
libuzfs_object_get_attr(libuzfs_inode_handle_t *ihp,
    uzfs_object_attr_t *attr)
{
	sa_bulk_attr_t sa_attrs[UZFS_END];
	int cnt = 0;
	sa_attr_type_t *attr_tbl = ihp->dhp->uzfs_attr_table;
	libuzfs_get_object_size(ihp->sa_hdl, attr_tbl[UZFS_ZXATTR],
	    &attr->blocks, &attr->blksize);
	SA_ADD_BULK_ATTR(sa_attrs, cnt, attr_tbl[UZFS_GEN],
	    NULL, &attr->gen, sizeof (attr->gen));
	SA_ADD_BULK_ATTR(sa_attrs, cnt, attr_tbl[UZFS_SIZE],
	    NULL, &attr->size, sizeof (attr->size));
	SA_ADD_BULK_ATTR(sa_attrs, cnt, attr_tbl[UZFS_MTIME],
	    NULL, &attr->mtime, sizeof (attr->mtime));
	return (sa_bulk_lookup(ihp->sa_hdl, sa_attrs, cnt));
}

int
libuzfs_inode_getattr(libuzfs_inode_handle_t *ihp,
    uzfs_inode_attr_t *attr, char *reserved, int *size)
{
	sa_attr_type_t *attr_tbl = ihp->dhp->uzfs_attr_table;
	int err = sa_size(ihp->sa_hdl, attr_tbl[UZFS_RESERVED], size);
	if (err == 0) {
		sa_bulk_attr_t sa_attrs[UZFS_END];
		int cnt = 0;
		SA_ADD_BULK_ATTR(sa_attrs, cnt, attr_tbl[UZFS_RESERVED],
		    NULL, reserved, *size);
		SA_ADD_BULK_ATTR(sa_attrs, cnt, attr_tbl[UZFS_GEN],
		    NULL, &attr->gen, sizeof (attr->gen));
		sa_object_size(ihp->sa_hdl, &attr->blksize,
		    (u_longlong_t *)&attr->blocks);
		err = sa_bulk_lookup(ihp->sa_hdl, sa_attrs, cnt);
	}

	return (err);
}

int
libuzfs_inode_setattr(libuzfs_inode_handle_t *ihp,
    const char *reserved, uint32_t size, uint64_t *txg)
{
	sa_handle_t *sa_hdl = ihp->sa_hdl;
	objset_t *os = ihp->dhp->os;
	ASSERT3U(size, <=, libuzfs_get_max_reserved_len(sa_hdl));
	dmu_tx_t *tx = dmu_tx_create(os);
	dmu_tx_hold_sa(tx, sa_hdl, B_FALSE);
	int err = 0;
	if ((err = dmu_tx_assign(tx, TXG_WAIT)) != 0) {
		dmu_tx_abort(tx);
		return (err);
	}

	VERIFY0(sa_update(sa_hdl, ihp->dhp->uzfs_attr_table[UZFS_RESERVED],
	    (void *)reserved, size, tx));

	if (txg != NULL) {
		*txg = tx->tx_txg;
	}

	dmu_tx_commit(tx);

	return (0);
}

static void
libuzfs_log_setmtime(zilog_t *zilog, uint64_t obj,
    const struct timespec *mtime, dmu_tx_t *tx)
{
	if (zil_replaying(zilog, tx)) {
		return;
	}

	itx_t *itx = zil_itx_create(TX_SETATTR,
	    sizeof (lr_obj_mtime_set_t));
	lr_obj_mtime_set_t *lr = (lr_obj_mtime_set_t *)&itx->itx_lr;
	lr->lr_foid = obj;
	lr->mtime[0] = mtime->tv_sec;
	lr->mtime[1] = mtime->tv_nsec;
	zil_itx_assign(zilog, itx, tx);
}

int
libuzfs_object_setmtime(libuzfs_inode_handle_t *ihp,
    const struct timespec *mtime, boolean_t sync)
{
	dmu_tx_t *tx = dmu_tx_create(ihp->dhp->os);
	dmu_tx_hold_sa(tx, ihp->sa_hdl, FALSE);
	int err = dmu_tx_assign(tx, TXG_WAIT);
	libuzfs_dataset_handle_t *dhp = ihp->dhp;
	if (err == 0) {
		VERIFY0(sa_update(ihp->sa_hdl, dhp->uzfs_attr_table[UZFS_MTIME],
		    (void *)mtime, sizeof (struct timespec), tx));

		libuzfs_log_setmtime(dhp->zilog, ihp->ino, mtime, tx);

		dmu_tx_commit(tx);
	} else {
		dmu_tx_abort(tx);
	}

	if (sync) {
		zil_commit(dhp->zilog, ihp->ino);
	}

	return (err);
}

int
libuzfs_get_nvlist_from_handle(const sa_attr_type_t *sa_tbl,
    nvlist_t **nvl, sa_handle_t *sa_hdl, sa_attr_type_t xattr)
{
	int xattr_sa_size;
	int err = sa_size(sa_hdl, sa_tbl[xattr], &xattr_sa_size);
	ASSERT(err != ENOENT);
	if (err != 0) {
		return (err);
	}

	char *xattr_sa_data = vmem_alloc(xattr_sa_size, KM_SLEEP);
	err = sa_lookup(sa_hdl, sa_tbl[xattr],
	    xattr_sa_data, xattr_sa_size);
	if (err == 0) {
		err = nvlist_unpack(xattr_sa_data,
		    xattr_sa_size, nvl, KM_SLEEP);
	}
	vmem_free(xattr_sa_data, xattr_sa_size);

	return (err);
}

/*
 * after settinging new entry, if the size
 * of nvlist is too big to be saved by sa, this function
 * will remove the name from nvlist and return EFBIG
 */
static int
libuzfs_kvattr_update_nvlist(nvlist_t *nvl, const char *name,
    const char *value, size_t value_size, size_t *xattr_sa_size,
    size_t max_size)
{
	int err = nvlist_add_byte_array(nvl, name,
	    (uchar_t *)value, (uint_t)value_size);
	if (err != 0) {
		return (err);
	}

	err = nvlist_size(nvl, xattr_sa_size, NV_ENCODE_XDR);
	if (err != 0) {
		return (err);
	}

	if (*xattr_sa_size >= max_size) {
		err = nvlist_remove(nvl, name, DATA_TYPE_BYTE_ARRAY);
		ASSERT(err != ENOENT);
		if (err == 0) {
			err = nvlist_size(nvl, xattr_sa_size, NV_ENCODE_XDR);
		}
		if (err != 0) {
			return (err);
		}
		return (EFBIG);
	}

	return (0);
}

static boolean_t
libuzfs_lp_kvattr_exists(sa_handle_t *sa_hdl, sa_attr_type_t *sa_tbl,
    const char *name, int *err)
{
	nvlist_t *nvl;
	*err = libuzfs_get_nvlist_from_handle(sa_tbl,
	    &nvl, sa_hdl, UZFS_XATTR);
	if (*err != 0) {
		return (B_FALSE);
	}

	if (nvlist_exists(nvl, name)) {
		nvlist_free(nvl);
		return (B_TRUE);
	}

	nvlist_free(nvl);

	uint64_t zap_obj;
	*err = sa_lookup(sa_hdl, sa_tbl[UZFS_ZXATTR],
	    &zap_obj, sizeof (zap_obj));
	if (*err == 0) {
		return (zap_contains(sa_hdl->sa_os, zap_obj, name) == 0);
	} else if (*err == ENOENT) {
		*err = 0;
		return (B_FALSE);
	} else {
		return (B_FALSE);
	}
}

static void
libuzfs_log_kvattr_set(zilog_t *zilog, dmu_tx_t *tx, uint64_t obj,
    const char *name, const char *value, uint64_t value_len, uint32_t option)
{
	if ((option & KVSET_NEED_LOG) == 0 || zil_replaying(zilog, tx)) {
		return;
	}

	uint64_t name_len = strlen(name);
	itx_t *itx = zil_itx_create(TX_MKXATTR,
	    sizeof (lr_kv_set_t) + name_len + 1 + value_len);
	lr_kv_set_t *lr = (lr_kv_set_t *)&itx->itx_lr;
	lr->lr_foid = obj;
	lr->lr_name_len = name_len;
	lr->lr_value_size = value_len;
	lr->option = option;
	itx->itx_sync = B_TRUE;

	char *dst = (char *)lr + sizeof (lr_kv_set_t);
	memcpy(dst, name, name_len + 1);
	dst += name_len + 1;
	memcpy(dst, value, value_len);

	zil_itx_assign(zilog, itx, tx);
}

// setting high priority kvattr will first check whether
// the hp area has enough space, if not enough, that kv will
// be removed from hp area and inserted into normal sa space,
// if normal sa space not enough, kv will be inserted into
// an extra zap object
int
libuzfs_inode_set_kvattr(libuzfs_inode_handle_t *ihp,
    const char *name, const char *value, uint64_t size,
    uint64_t *txg, uint32_t option)
{
	if (size > UZFS_XATTR_MAXVALUELEN) {
		return (EFBIG);
	}

	libuzfs_dataset_handle_t *dhp = ihp->dhp;
	objset_t *os = dhp->os;
	sa_handle_t *sa_hdl = ihp->sa_hdl;
	nvlist_t *nvl = NULL;
	char *xattr_sa_data = NULL;
	size_t xattr_sa_size = 0;
	sa_attr_type_t *sa_tbl = dhp->uzfs_attr_table;
	char *hp_xattr_data = NULL;
	size_t hp_xattr_data_size = 0;

	int err = 0;
	// try to insert this kv into high priority area
	if (option & KVSET_HIGH_PRIORITY) {
		sa_attr_type_t *sa_tbl = dhp->uzfs_attr_table;
		boolean_t existed = nvlist_exists(ihp->hp_kvattr_cache, name);
		// old value not in hp area, we need to check
		// whether it exists in normal area,
		// if so, fall back to normal kvattr set
		if (!existed) {
			boolean_t existed_in_lp = libuzfs_lp_kvattr_exists(
			    sa_hdl, sa_tbl, name, &err);

			if (err != 0) {
				return (err);
			}

			if (existed_in_lp) {
				goto set_normal;
			}
		}

		// we need to acquire lock here prevent concurrency get hp cache
		rw_enter(&ihp->hp_kvattr_cache_lock, RW_WRITER);
		err = libuzfs_kvattr_update_nvlist(ihp->hp_kvattr_cache,
		    name, value, size, &hp_xattr_data_size,
		    libuzfs_get_max_hp_kvs_capacity(sa_hdl));
		rw_exit(&ihp->hp_kvattr_cache_lock);
		if (err != 0 && err != EFBIG) {
			return (err);
		}

		if (existed || err == 0) {
			hp_xattr_data = umem_alloc(hp_xattr_data_size,
			    UMEM_NOFAIL);
			VERIFY0(nvlist_pack(ihp->hp_kvattr_cache,
			    &hp_xattr_data, &hp_xattr_data_size,
			    NV_ENCODE_XDR, KM_SLEEP));
		}

		// hp area has enough space, just put in hp area
		if (err == 0) {
			dmu_tx_t *tx = dmu_tx_create(dhp->os);
			dmu_tx_hold_sa(tx, sa_hdl, B_TRUE);
			if ((err = dmu_tx_assign(tx, TXG_WAIT)) != 0) {
				dmu_tx_abort(tx);
			} else {
				VERIFY0(sa_update(sa_hdl,
				    sa_tbl[UZFS_XATTR_HIGH], hp_xattr_data,
				    hp_xattr_data_size, tx));
				if (txg != NULL) {
					*txg = tx->tx_txg;
				}
				libuzfs_log_kvattr_set(dhp->zilog, tx, ihp->ino,
				    name, value, size, option);
				dmu_tx_commit(tx);
			}
			goto out_free_sa_data;
		}
	}

set_normal:
	err = libuzfs_get_nvlist_from_handle(sa_tbl, &nvl, sa_hdl, UZFS_XATTR);
	if (err == ENOENT) {
		err = nvlist_alloc(&nvl, NV_UNIQUE_NAME, KM_SLEEP);
	}
	if (err != 0) {
		goto out_free_sa_data;
	}

	// try to put new kv in bounus/spill buffer
	boolean_t existed_in_sa = nvlist_exists(nvl, name);
	boolean_t sa_space_enough = B_TRUE;
	err = libuzfs_kvattr_update_nvlist(nvl, name,
	    value, size, &xattr_sa_size,
	    MIN(DXATTR_MAX_SA_SIZE, XATTR_SIZE_MAX));
	if (err == EFBIG) {
		sa_space_enough = B_FALSE;
	} else if (err != 0) {
		goto out_free_sa_data;
	}
	ASSERT(xattr_sa_size > 0);

	if (existed_in_sa || sa_space_enough) {
		xattr_sa_data = vmem_alloc(xattr_sa_size, KM_SLEEP);
		err = nvlist_pack(nvl, &xattr_sa_data, &xattr_sa_size,
		    NV_ENCODE_XDR, KM_SLEEP);
		if (err != 0) {
			goto out_free_sa_data;
		}
	}

	boolean_t existed_in_zap = B_FALSE;
	uint64_t xattr_zap_obj = DMU_NEW_OBJECT;
	uint64_t zap_entries_count = 0;
	if (!existed_in_sa || !sa_space_enough) {
		err = sa_lookup(sa_hdl, sa_tbl[UZFS_ZXATTR],
		    &xattr_zap_obj, sizeof (xattr_zap_obj));

		if (err == 0 && sa_space_enough) {
			err = zap_contains(os, xattr_zap_obj, name);
			existed_in_zap = err == 0;
		}

		if (err != ENOENT && err != 0) {
			goto out_free_sa_data;
		}

		if (existed_in_zap && sa_space_enough) {
			err = zap_count(os, xattr_zap_obj, &zap_entries_count);
			if (err != 0) {
				goto out_free_sa_data;
			}
		}
	}

	dmu_tx_t *tx = dmu_tx_create(os);
	dmu_tx_hold_sa(tx, sa_hdl, B_TRUE);
	if (existed_in_zap && sa_space_enough && zap_entries_count == 1) {
		dmu_tx_hold_free(tx, xattr_zap_obj, 0, DMU_OBJECT_END);
	} else if (existed_in_zap || !sa_space_enough) {
		// if the old kv exists in zap or sa space
		// no enough to put that kv
		// we need to operate zap obj
		dmu_tx_hold_zap(tx, xattr_zap_obj, !sa_space_enough, name);
	}

	if ((err = dmu_tx_assign(tx, TXG_WAIT)) == 0) {
		// existed_in_sa: we need to set/remove kv from sa
		// sa_space_enough: we need to set kv into sa
		if (existed_in_sa || sa_space_enough) {
			VERIFY0(sa_update(sa_hdl, sa_tbl[UZFS_XATTR],
			    xattr_sa_data, xattr_sa_size, tx));
		}

		if (hp_xattr_data != NULL) {
			VERIFY0(sa_update(sa_hdl, sa_tbl[UZFS_XATTR_HIGH],
			    hp_xattr_data, hp_xattr_data_size, tx));
		}

		// existed_in_zap && sa_space_enough means we should
		// delete old kv from zap and insert new kv into sa
		if (existed_in_zap && sa_space_enough) {
			ASSERT(zap_entries_count >= 1);
			if (zap_entries_count == 1) {
				VERIFY0(zap_destroy(os, xattr_zap_obj, tx));
				VERIFY0(sa_remove(sa_hdl,
				    sa_tbl[UZFS_ZXATTR], tx));
			} else {
				VERIFY0(zap_remove(os, xattr_zap_obj,
				    name, tx));
			}
		}

		if (!sa_space_enough) {
			if (xattr_zap_obj == DMU_NEW_OBJECT) {
				int dnodesize = dmu_objset_dnodesize(os);
				// use sa to make sure check valid
				// can be called for this object
				xattr_zap_obj = zap_create_dnsize(os,
				    DMU_OT_DIRECTORY_CONTENTS, DMU_OT_SA, 0,
				    dnodesize, tx);
				VERIFY0(sa_update(sa_hdl, sa_tbl[UZFS_ZXATTR],
				    &xattr_zap_obj, sizeof (xattr_zap_obj),
				    tx));
			}
			VERIFY0(zap_update(os, xattr_zap_obj, name,
			    1, size, value, tx));
		}

		libuzfs_log_kvattr_set(dhp->zilog, tx, ihp->ino, name,
		    value, size, option);

		if (txg != NULL) {
			*txg = tx->tx_txg;
		}
		dmu_tx_commit(tx);
	} else {
		dmu_tx_abort(tx);
	}

out_free_sa_data:
	if (hp_xattr_data != NULL) {
		vmem_free(hp_xattr_data, hp_xattr_data_size);
	}
	if (xattr_sa_data != NULL) {
		vmem_free(xattr_sa_data, xattr_sa_size);
	}
	if (nvl != NULL) {
		nvlist_free(nvl);
	}
	return (err);
}

static ssize_t
libuzfs_inode_get_kvattr_zap(sa_handle_t *sa_hdl, sa_attr_type_t *sa_tbl,
    const char *name, char *value, size_t size)
{
	// get zap object from sa
	objset_t *os = sa_hdl->sa_os;
	uint64_t xattr_zap_obj;
	int err = sa_lookup(sa_hdl, sa_tbl[UZFS_ZXATTR],
	    &xattr_zap_obj, sizeof (uint64_t));
	if (err != 0) {
		return (-err);
	}

	uint64_t integer_size = 0;
	uint64_t integer_num = 0;
	err = zap_length(os, xattr_zap_obj, name,
	    &integer_size, &integer_num);
	if (err != 0) {
		return (-err);
	}

	ASSERT(integer_size == 1);
	ASSERT(integer_num <= size);
	err = zap_lookup(os, xattr_zap_obj, name, 1, size, value);
	if (err != 0) {
		return (-err);
	}

	return (integer_num);
}

static ssize_t
libuzfs_inode_get_kvattr_sa(sa_handle_t *sa_hdl, const sa_attr_type_t *sa_tbl,
    const char *name, char *value, size_t size, sa_attr_type_t xattr)
{
	nvlist_t *nvl;
	int err = libuzfs_get_nvlist_from_handle(sa_tbl, &nvl, sa_hdl, xattr);
	if (err != 0) {
		return (-err);
	}

	uchar_t *nv_value;
	uint_t nv_size = 0;
	err = nvlist_lookup_byte_array(nvl, name, &nv_value, &nv_size);
	if (err == 0 && nv_size <= size) {
		memcpy(value, nv_value, nv_size);
	}
	nvlist_free(nvl);
	if (err != 0) {
		return (-err);
	}

	if (nv_size > size) {
		return (-ERANGE);
	}
	return (nv_size);
}

ssize_t
libuzfs_inode_get_kvattr(libuzfs_inode_handle_t *ihp,
    const char *name, char *value, uint64_t size)
{
	sa_handle_t *sa_hdl = ihp->sa_hdl;
	libuzfs_dataset_handle_t *dhp = ihp->dhp;
	uchar_t *nv_value;
	uint_t nv_size = 0;
	rw_enter(&ihp->hp_kvattr_cache_lock, RW_READER);
	int rc = nvlist_lookup_byte_array(ihp->hp_kvattr_cache,
	    name, &nv_value, &nv_size);
	rw_exit(&ihp->hp_kvattr_cache_lock);
	if (rc == 0) {
		if (nv_size > size) {
			return (-ERANGE);
		}
		memcpy(value, nv_value, nv_size);
		return (nv_size);
	}

	rc = -rc;
	if (rc == -ENOENT) {
		rc = libuzfs_inode_get_kvattr_sa(sa_hdl, dhp->uzfs_attr_table,
		    name, value, size, UZFS_XATTR);
		if (rc == -ENOENT) {
			rc = libuzfs_inode_get_kvattr_zap(sa_hdl,
			    dhp->uzfs_attr_table, name, value, size);
		}
	}

	return (rc);
}

static int
libuzfs_inode_remove_kvattr_from_sa(sa_handle_t *sa_hdl, sa_attr_type_t *sa_tbl,
    const char *name, uint64_t *txg, sa_attr_type_t xattr, nvlist_t *nvl)
{
	boolean_t need_free = nvl == NULL;
	int err = 0;
	if (need_free) {
		err = libuzfs_get_nvlist_from_handle(sa_tbl,
		    &nvl, sa_hdl, xattr);
		if (err != 0) {
			return (err);
		}
	}

	err = nvlist_remove(nvl, name, DATA_TYPE_BYTE_ARRAY);
	if (err != 0) {
		goto out1;
	}

	uint64_t xattr_sa_size = 0;
	err = nvlist_size(nvl, &xattr_sa_size, NV_ENCODE_XDR);
	ASSERT(xattr_sa_size > 0);
	if (err != 0) {
		goto out1;
	}

	char *xattr_sa_data = vmem_alloc(xattr_sa_size, KM_SLEEP);
	err = nvlist_pack(nvl, &xattr_sa_data,
	    &xattr_sa_size, NV_ENCODE_XDR, KM_SLEEP);
	if (err != 0) {
		goto out2;
	}

	dmu_tx_t *tx = dmu_tx_create(sa_hdl->sa_os);
	dmu_tx_hold_sa(tx, sa_hdl, B_TRUE);
	if ((err = dmu_tx_assign(tx, TXG_WAIT)) == 0) {
		ASSERT(xattr_sa_data != NULL);
		VERIFY0(sa_update(sa_hdl, sa_tbl[xattr],
		    xattr_sa_data, xattr_sa_size, tx));

		if (txg != NULL) {
			*txg = tx->tx_txg;
		}
		dmu_tx_commit(tx);
	} else {
		dmu_tx_abort(tx);
	}

out2:
	vmem_free(xattr_sa_data, xattr_sa_size);
out1:
	if (need_free) {
		nvlist_free(nvl);
	}
	return (err);
}

static int
libuzfs_inode_remove_kvattr_from_zap(sa_handle_t *sa_hdl,
    sa_attr_type_t *sa_tbl, const char *name, uint64_t *txg)
{
	uint64_t xattr_zap_obj = 0;
	int err = sa_lookup(sa_hdl, sa_tbl[UZFS_ZXATTR],
	    &xattr_zap_obj, sizeof (xattr_zap_obj));
	if (err != 0) {
		return (err);
	}

	objset_t *os = sa_hdl->sa_os;
	err = zap_contains(os, xattr_zap_obj, name);
	if (err != 0) {
		return (err);
	}

	uint64_t zap_entries_count = 0;
	err = zap_count(os, xattr_zap_obj, &zap_entries_count);
	if (err != 0) {
		return (err);
	}

	ASSERT(zap_entries_count > 0);
	dmu_tx_t *tx = dmu_tx_create(os);
	if (zap_entries_count == 1) {
		dmu_tx_hold_free(tx, xattr_zap_obj, 0, DMU_OBJECT_END);
		dmu_tx_hold_sa(tx, sa_hdl, B_TRUE);
	} else {
		dmu_tx_hold_zap(tx, xattr_zap_obj, B_FALSE, name);
	}

	if ((err = dmu_tx_assign(tx, TXG_WAIT)) == 0) {
		if (zap_entries_count == 1) {
			VERIFY0(dmu_object_free(os, xattr_zap_obj, tx));
			VERIFY0(sa_remove(sa_hdl, sa_tbl[UZFS_ZXATTR], tx));
		} else {
			VERIFY0(zap_remove(os, xattr_zap_obj, name, tx));
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
libuzfs_inode_remove_kvattr(libuzfs_inode_handle_t *ihp,
    const char *name, uint64_t *txg)
{
	sa_handle_t *sa_hdl = ihp->sa_hdl;
	libuzfs_dataset_handle_t *dhp = ihp->dhp;
	sa_attr_type_t *sa_tbl = dhp->uzfs_attr_table;
	int err = libuzfs_inode_remove_kvattr_from_sa(sa_hdl, sa_tbl, name,
	    txg, UZFS_XATTR_HIGH, ihp->hp_kvattr_cache);

	if (err == ENOENT) {
		err = libuzfs_inode_remove_kvattr_from_sa(sa_hdl, sa_tbl, name,
		    txg, UZFS_XATTR, NULL);

		if (err == ENOENT) {
			err = libuzfs_inode_remove_kvattr_from_zap(sa_hdl,
			    sa_tbl, name, txg);
		}
	}
	return (err);
}

libuzfs_kvattr_iterator_t *
libuzfs_new_kvattr_iterator(libuzfs_inode_handle_t *ihp, int *err)
{
	sa_handle_t *sa_hdl = ihp->sa_hdl;
	libuzfs_dataset_handle_t *dhp = ihp->dhp;
	libuzfs_kvattr_iterator_t *iter =
	    umem_zalloc(sizeof (libuzfs_kvattr_iterator_t), UMEM_NOFAIL);

	sa_attr_type_t *sa_tbl = dhp->uzfs_attr_table;
	*err = libuzfs_get_nvlist_from_handle(sa_tbl,
	    &iter->kvattrs_in_sa, sa_hdl, UZFS_XATTR);
	if (*err != 0) {
		goto fail;
	}

	*err = libuzfs_get_nvlist_from_handle(sa_tbl,
	    &iter->hp_kvattrs_in_sa, sa_hdl, UZFS_XATTR_HIGH);
	if (*err != 0) {
		goto fail;
	}

	iter->cur_hp_sa_pair = nvlist_next_nvpair(iter->hp_kvattrs_in_sa, NULL);
	iter->cur_sa_pair = nvlist_next_nvpair(iter->kvattrs_in_sa, NULL);

	*err = sa_lookup(sa_hdl, sa_tbl[UZFS_ZXATTR],
	    &iter->zap_obj, sizeof (iter->zap_obj));
	if (*err != 0 && *err != ENOENT) {
		goto fail;
	}

	if (*err == 0) {
		zap_cursor_init(&iter->zc, dhp->os, iter->zap_obj);
	}
	*err = 0;

	return (iter);

fail:
	if (iter->kvattrs_in_sa != NULL) {
		nvlist_free(iter->kvattrs_in_sa);
	}
	if (iter->hp_kvattrs_in_sa != NULL) {
		nvlist_free(iter->hp_kvattrs_in_sa);
	}
	umem_free(iter, sizeof (libuzfs_kvattr_iterator_t));
	return (NULL);
}

ssize_t
libuzfs_next_kvattr_name(libuzfs_kvattr_iterator_t *iter, char *buf, int size)
{
	if (iter->cur_hp_sa_pair != NULL) {
		char *name = nvpair_name(iter->cur_hp_sa_pair);
		int name_len = strlen(name);
		if (size - 1 < name_len) {
			return (-ERANGE);
		}
		strncpy(buf, name, size);
		iter->cur_hp_sa_pair = nvlist_next_nvpair(
		    iter->hp_kvattrs_in_sa, iter->cur_hp_sa_pair);
		return (name_len);
	}

	if (iter->cur_sa_pair != NULL) {
		char *name = nvpair_name(iter->cur_sa_pair);
		int name_len = strlen(name);
		if (size - 1 < name_len) {
			return (-ERANGE);
		}
		strncpy(buf, name, size);
		iter->cur_sa_pair =
		    nvlist_next_nvpair(iter->kvattrs_in_sa, iter->cur_sa_pair);
		return (name_len);
	}

	zap_attribute_t za;
	if (iter->zap_obj != 0 && zap_cursor_retrieve(&iter->zc, &za) == 0) {
		int name_len = strlen(za.za_name);
		if (size - 1 < name_len) {
			return (-ERANGE);
		}
		strncpy(buf, za.za_name, size);
		zap_cursor_advance(&iter->zc);
		return (name_len);
	}

	return (0);
}

void
libuzfs_kvattr_iterator_fini(libuzfs_kvattr_iterator_t *iter)
{
	VERIFY3P(iter, !=, NULL);
	nvlist_free(iter->kvattrs_in_sa);
	nvlist_free(iter->hp_kvattrs_in_sa);
	if (iter->zap_obj != 0) {
		zap_cursor_fini(&iter->zc);
	}
	umem_free(iter, sizeof (libuzfs_kvattr_iterator_t));
}
