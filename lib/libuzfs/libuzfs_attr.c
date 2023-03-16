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
	UZFS_BLKSIZE,
	UZFS_BLOCKS,
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

sa_attr_reg_t uzfs_attr_table[UZFS_END+1] = {
	{"UZFS_PINO", sizeof (uint64_t), SA_UINT64_ARRAY, 0},
	{"UZFS_PSID", sizeof (uint32_t), SA_UINT32_ARRAY, 1},
	{"UZFS_FTYPE", sizeof (FileType), SA_UINT32_ARRAY, 2},
	{"UZFS_GEN", sizeof (uint64_t), SA_UINT64_ARRAY, 3},
	{"UZFS_NLINK", sizeof (uint32_t), SA_UINT32_ARRAY, 4},
	{"UZFS_PERM", sizeof (uint32_t), SA_UINT32_ARRAY, 5},
	{"UZFS_UID", sizeof (uint32_t), SA_UINT32_ARRAY, 6},
	{"UZFS_GID", sizeof (uint32_t), SA_UINT32_ARRAY, 7},
	{"UZFS_SIZE", sizeof (uint64_t), SA_UINT64_ARRAY, 8},
	{"UZFS_BLKSIZE", sizeof (uint64_t), SA_UINT64_ARRAY, 9},
	{"UZFS_BLOCKS", sizeof (uint64_t), SA_UINT64_ARRAY, 10},
	{"UZFS_NSID", sizeof (uint32_t), SA_UINT32_ARRAY, 11},
	{"UZFS_DSID", sizeof (uint32_t), SA_UINT32_ARRAY, 12},
	{"UZFS_OID", sizeof (uint64_t), SA_UINT64_ARRAY, 13},
	{"UZFS_OGEN", sizeof (uint64_t), SA_UINT64_ARRAY, 14},
	{"UZFS_ATIME", sizeof (uint64_t) * 2, SA_UINT64_ARRAY, 15},
	{"UZFS_MTIME", sizeof (uint64_t) * 2, SA_UINT64_ARRAY, 16},
	{"UZFS_CTIME", sizeof (uint64_t) * 2, SA_UINT64_ARRAY, 17},
	{"UZFS_BTIME", sizeof (uint64_t) * 2, SA_UINT64_ARRAY, 18},
	{"UZFS_ZXATTR", sizeof (uint64_t), SA_UINT64_ARRAY, 19},
	{"UZFS_XATTR", 0, SA_UINT8_ARRAY, 0},
	{NULL, 0, 0, 0}
};

void
libuzfs_setup_dataset_sa(libuzfs_dataset_handle_t *dhp)
{
	uint64_t sa_obj;
	VERIFY0(zap_lookup(dhp->os, MASTER_NODE_OBJ,
	    ZFS_SA_ATTRS, 8, 1, &sa_obj));
	VERIFY0(sa_setup(dhp->os, sa_obj, uzfs_attr_table,
	    UZFS_END, &dhp->uzfs_attr_table));
}

// make sure sa_attrs has enough space
static void
libuzfs_add_bulk_attr(libuzfs_dataset_handle_t *dhp, sa_bulk_attr_t *sa_attrs,
    int *cnt, uzfs_attr_t *attr)
{
	sa_attr_type_t *sa_tbl = dhp->uzfs_attr_table;
	SA_ADD_BULK_ATTR(sa_attrs, (*cnt), sa_tbl[UZFS_SIZE],
	    NULL, &attr->size, 8);
	SA_ADD_BULK_ATTR(sa_attrs, (*cnt), sa_tbl[UZFS_GEN],
	    NULL, &attr->gen, 8);
	SA_ADD_BULK_ATTR(sa_attrs, (*cnt), sa_tbl[UZFS_UID],
	    NULL, &attr->uid, 4);
	SA_ADD_BULK_ATTR(sa_attrs, (*cnt), sa_tbl[UZFS_GID],
	    NULL, &attr->gid, 4);
	SA_ADD_BULK_ATTR(sa_attrs, (*cnt), sa_tbl[UZFS_PINO],
	    NULL, &attr->pino, 8);
	SA_ADD_BULK_ATTR(sa_attrs, (*cnt), sa_tbl[UZFS_PSID],
	    NULL, &attr->psid, 4);
	SA_ADD_BULK_ATTR(sa_attrs, (*cnt), sa_tbl[UZFS_FTYPE],
	    NULL, &attr->ftype, 4);
	SA_ADD_BULK_ATTR(sa_attrs, (*cnt), sa_tbl[UZFS_NLINK],
	    NULL, &attr->nlink, 4);
	SA_ADD_BULK_ATTR(sa_attrs, (*cnt), sa_tbl[UZFS_PERM],
	    NULL, &attr->perm, 4);
	SA_ADD_BULK_ATTR(sa_attrs, (*cnt), sa_tbl[UZFS_BLKSIZE],
	    NULL, &attr->blksize, 8);
	SA_ADD_BULK_ATTR(sa_attrs, (*cnt), sa_tbl[UZFS_BLOCKS],
	    NULL, &attr->blocks, 8);
	SA_ADD_BULK_ATTR(sa_attrs, (*cnt), sa_tbl[UZFS_NSID],
	    NULL, &attr->nsid, 4);
	SA_ADD_BULK_ATTR(sa_attrs, (*cnt), sa_tbl[UZFS_DSID],
	    NULL, &attr->dsid, 4);
	SA_ADD_BULK_ATTR(sa_attrs, (*cnt), sa_tbl[UZFS_OID],
	    NULL, &attr->oid, 8);
	SA_ADD_BULK_ATTR(sa_attrs, (*cnt), sa_tbl[UZFS_OGEN],
	    NULL, &attr->ogen, 8);
	SA_ADD_BULK_ATTR(sa_attrs, (*cnt), sa_tbl[UZFS_ATIME],
	    NULL, &attr->atime, 16);
	SA_ADD_BULK_ATTR(sa_attrs, (*cnt), sa_tbl[UZFS_MTIME],
	    NULL, &attr->mtime, 16);
	SA_ADD_BULK_ATTR(sa_attrs, (*cnt), sa_tbl[UZFS_CTIME],
	    NULL, &attr->ctime, 16);
	SA_ADD_BULK_ATTR(sa_attrs, (*cnt), sa_tbl[UZFS_BTIME],
	    NULL, &attr->btime, 16);
}

void
libuzfs_object_attr_init(libuzfs_dataset_handle_t *dhp,
    sa_handle_t *sa_hdl, dmu_tx_t *tx)
{
	sa_bulk_attr_t sa_attrs[UZFS_END];
	int cnt = 0;
	uzfs_attr_t attr;
	memset(&attr, 0, sizeof (attr));
	libuzfs_add_bulk_attr(dhp, sa_attrs, &cnt, &attr);

	nvlist_t *nvl;
	VERIFY0(nvlist_alloc(&nvl, NV_UNIQUE_NAME, KM_SLEEP));

	uint64_t xattr_sa_size;
	VERIFY0(nvlist_size(nvl, &xattr_sa_size, NV_ENCODE_XDR));

	char *xattr_sa_data = vmem_alloc(xattr_sa_size, KM_SLEEP);
	VERIFY0(nvlist_pack(nvl, &xattr_sa_data, &xattr_sa_size,
	    NV_ENCODE_XDR, KM_SLEEP));

	SA_ADD_BULK_ATTR(sa_attrs, cnt, dhp->uzfs_attr_table[UZFS_XATTR],
	    NULL, xattr_sa_data, xattr_sa_size);

	VERIFY0(sa_replace_all_by_template(sa_hdl, sa_attrs, cnt, tx));

	vmem_free(xattr_sa_data, xattr_sa_size);
	nvlist_free(nvl);
}

int
libuzfs_get_xattr_zap_obj(libuzfs_dataset_handle_t *dhp, uint64_t ino,
    uint64_t *xattr_zap_obj)
{
	objset_t *os = dhp->os;
	sa_handle_t *sa_hdl = NULL;
	int err = sa_handle_get(os, ino, NULL, SA_HDL_PRIVATE, &sa_hdl);
	if (err != 0) {
		return (err);
	}

	sa_attr_type_t *sa_tbl = dhp->uzfs_attr_table;
	err = sa_lookup(sa_hdl, sa_tbl[UZFS_ZXATTR], xattr_zap_obj,
	    sizeof (*xattr_zap_obj));
	sa_handle_destroy(sa_hdl);
	return (err);
}

int
libuzfs_inode_getattr(libuzfs_dataset_handle_t *dhp, uint64_t ino,
    uzfs_attr_t *attr)
{
	sa_handle_t *sa_hdl;
	int err = sa_handle_get(dhp->os, ino, NULL, SA_HDL_PRIVATE, &sa_hdl);
	if (err != 0) {
		return (err);
	}

	sa_bulk_attr_t sa_attrs[UZFS_END];
	int cnt = 0;
	libuzfs_add_bulk_attr(dhp, sa_attrs, &cnt, attr);
	err = sa_bulk_lookup(sa_hdl, sa_attrs, cnt);
	attr->ino = ino;

	sa_handle_destroy(sa_hdl);
	return (err);
}

int
libuzfs_inode_setattr(libuzfs_dataset_handle_t *dhp, uint64_t ino,
    const uzfs_attr_t *attr, uint64_t *txg)
{
	sa_handle_t *sa_hdl;
	objset_t *os = dhp->os;
	int err = sa_handle_get(os, ino, NULL, SA_HDL_PRIVATE, &sa_hdl);
	if (err != 0) {
		return (err);
	}

	dmu_tx_t *tx = dmu_tx_create(os);
	dmu_tx_hold_sa(tx, sa_hdl, B_FALSE);
	if ((err = dmu_tx_assign(tx, TXG_WAIT)) != 0) {
		dmu_tx_abort(tx);
		return (err);
	}

	sa_bulk_attr_t sa_attrs[UZFS_END];
	int cnt = 0;
	libuzfs_add_bulk_attr(dhp, sa_attrs, &cnt, (uzfs_attr_t *)attr);
	VERIFY0(sa_bulk_update(sa_hdl, sa_attrs, cnt, tx));

	sa_handle_destroy(sa_hdl);

	if (txg != NULL) {
		*txg = tx->tx_txg;
	}

	dmu_tx_commit(tx);

	return (0);
}

static int
libuzfs_get_nvlist_from_handle(const sa_attr_type_t *sa_tbl,
    nvlist_t **nvl, sa_handle_t *sa_hdl)
{
	int xattr_sa_size;
	int err = sa_size(sa_hdl, sa_tbl[UZFS_XATTR], &xattr_sa_size);
	ASSERT(err != ENOENT);
	if (err != 0) {
		return (err);
	}

	char *xattr_sa_data = vmem_alloc(xattr_sa_size, KM_SLEEP);
	err = sa_lookup(sa_hdl, sa_tbl[UZFS_XATTR],
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
    const char *value, size_t value_size, size_t *xattr_sa_size)
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

	if (*xattr_sa_size > DXATTR_MAX_SA_SIZE ||
	    *xattr_sa_size > SA_ATTR_MAX_LEN) {
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

int
libuzfs_inode_set_kvattr(libuzfs_dataset_handle_t *dhp, uint64_t ino,
    const char *name, const char *value, uint64_t size,
    int flags, uint64_t *txg)
{
	if (size > UZFS_XATTR_MAXVALUELEN) {
		return (EFBIG);
	}

	sa_handle_t *sa_hdl = NULL;
	objset_t *os = dhp->os;
	int err = sa_handle_get(os, ino, NULL, SA_HDL_PRIVATE, &sa_hdl);
	if (err != 0) {
		return (err);
	}

	nvlist_t *nvl;
	sa_attr_type_t *sa_tbl = dhp->uzfs_attr_table;
	err = libuzfs_get_nvlist_from_handle(sa_tbl, &nvl, sa_hdl);
	if (err == ENOENT) {
		err = nvlist_alloc(&nvl, NV_UNIQUE_NAME, KM_SLEEP);
	}
	if (err != 0) {
		goto out_handle;
	}

	boolean_t existed_in_sa = nvlist_exists(nvl, name);
	boolean_t sa_space_enough = B_TRUE;
	size_t xattr_sa_size = 0;
	err = libuzfs_kvattr_update_nvlist(nvl, name,
	    value, size, &xattr_sa_size);
	if (err == EFBIG) {
		sa_space_enough = B_FALSE;
	} else if (err != 0) {
		goto out_nvl;
	}
	ASSERT(xattr_sa_size > 0);

	char *xattr_sa_data = NULL;
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
		dmu_tx_hold_zap(tx, xattr_zap_obj, !sa_space_enough, name);
	}

	if ((err = dmu_tx_assign(tx, TXG_WAIT)) == 0) {
		if (existed_in_sa || sa_space_enough) {
			VERIFY0(sa_update(sa_hdl, sa_tbl[UZFS_XATTR],
			    xattr_sa_data, xattr_sa_size, tx));
		}

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
				xattr_zap_obj = zap_create_dnsize(os,
				    DMU_OT_DIRECTORY_CONTENTS, DMU_OT_NONE, 0,
				    dnodesize, tx);
				VERIFY0(sa_update(sa_hdl, sa_tbl[UZFS_ZXATTR],
				    &xattr_zap_obj, sizeof (xattr_zap_obj),
				    tx));
			}
			VERIFY0(zap_update(os, xattr_zap_obj, name,
			    1, size, value, tx));
		}

		if (txg != NULL) {
			*txg = tx->tx_txg;
		}
		dmu_tx_commit(tx);
	} else {
		dmu_tx_abort(tx);
	}

out_free_sa_data:
	if (xattr_sa_data != NULL) {
		vmem_free(xattr_sa_data, xattr_sa_size);
	}
out_nvl:
	nvlist_free(nvl);
out_handle:
	sa_handle_destroy(sa_hdl);
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
    const char *name, char *value, size_t size)
{
	nvlist_t *nvl;
	int err = libuzfs_get_nvlist_from_handle(sa_tbl, &nvl, sa_hdl);
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
libuzfs_inode_get_kvattr(libuzfs_dataset_handle_t *dhp, uint64_t ino,
    const char *name, char *value, uint64_t size, int flags)
{
	sa_handle_t *sa_hdl = NULL;
	int err = sa_handle_get(dhp->os, ino, NULL, SA_HDL_PRIVATE, &sa_hdl);
	if (err != 0) {
		return (-err);
	}

	ssize_t rc = libuzfs_inode_get_kvattr_sa(sa_hdl, dhp->uzfs_attr_table,
	    name, value, size);
	if (rc == -ENOENT) {
		rc = libuzfs_inode_get_kvattr_zap(sa_hdl, dhp->uzfs_attr_table,
		    name, value, size);
	}

	sa_handle_destroy(sa_hdl);
	return (rc);
}

static int
libuzfs_inode_remove_kvattr_from_sa(sa_handle_t *sa_hdl, sa_attr_type_t *sa_tbl,
    const char *name, uint64_t *txg)
{
	nvlist_t *nvl = NULL;
	int err = libuzfs_get_nvlist_from_handle(sa_tbl, &nvl, sa_hdl);
	if (err != 0) {
		return (err);
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
		VERIFY0(sa_update(sa_hdl, sa_tbl[UZFS_XATTR],
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
	nvlist_free(nvl);
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
libuzfs_inode_remove_kvattr(libuzfs_dataset_handle_t *dhp, uint64_t ino,
    const char *name, uint64_t *txg)
{
	sa_handle_t *sa_hdl = NULL;
	objset_t *os = dhp->os;
	int err = sa_handle_get(os, ino, NULL, SA_HDL_PRIVATE, &sa_hdl);
	if (err != 0) {
		return (err);
	}

	sa_attr_type_t *sa_tbl = dhp->uzfs_attr_table;
	err = libuzfs_inode_remove_kvattr_from_sa(sa_hdl, sa_tbl, name, txg);
	if (err == ENOENT) {
		err = libuzfs_inode_remove_kvattr_from_zap(sa_hdl,
		    sa_tbl, name, txg);
	}

	sa_handle_destroy(sa_hdl);
	return (err);
}

libuzfs_kvattr_iterator_t *
libuzfs_new_kvattr_iterator(libuzfs_dataset_handle_t *dhp,
    uint64_t ino, int *err)
{
	sa_handle_t *sa_hdl = NULL;
	objset_t *os = dhp->os;
	*err = sa_handle_get(os, ino, NULL, SA_HDL_PRIVATE, &sa_hdl);
	if (*err != 0) {
		return (NULL);
	}

	libuzfs_kvattr_iterator_t *iter =
	    umem_alloc(sizeof (libuzfs_kvattr_iterator_t), UMEM_NOFAIL);
	memset(iter, 0, sizeof (libuzfs_kvattr_iterator_t));

	sa_attr_type_t *sa_tbl = dhp->uzfs_attr_table;
	*err = libuzfs_get_nvlist_from_handle(sa_tbl,
	    &iter->kvattrs_in_sa, sa_hdl);
	if (*err != 0) {
		goto fail;
	}

	iter->current_pair = nvlist_next_nvpair(iter->kvattrs_in_sa, NULL);

	*err = sa_lookup(sa_hdl, sa_tbl[UZFS_ZXATTR],
	    &iter->zap_obj, sizeof (iter->zap_obj));
	if (*err != 0 && *err != ENOENT) {
		goto fail;
	}

	if (*err == 0) {
		zap_cursor_init(&iter->zc, dhp->os, iter->zap_obj);
	}
	sa_handle_destroy(sa_hdl);
	return (iter);

fail:
	if (iter->kvattrs_in_sa != NULL) {
		nvlist_free(iter->kvattrs_in_sa);
	}
	umem_free(iter, sizeof (libuzfs_kvattr_iterator_t));
	sa_handle_destroy(sa_hdl);
	return (NULL);
}

ssize_t
libuzfs_next_kvattr_name(libuzfs_kvattr_iterator_t *iter, char *buf, int size)
{
	if (iter->current_pair != NULL) {
		char *name = nvpair_name(iter->current_pair);
		int name_len = strlen(name);
		if (size - 1 < name_len) {
			return (-ERANGE);
		}
		strncpy(buf, name, size);
		iter->current_pair =
		    nvlist_next_nvpair(iter->kvattrs_in_sa, iter->current_pair);
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
	if (iter->zap_obj != 0) {
		zap_cursor_fini(&iter->zc);
	}
	umem_free(iter, sizeof (libuzfs_kvattr_iterator_t));
}
