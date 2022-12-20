#include <sys/avl.h>
#include <sys/sa.h>
#include <sys/sa_impl.h>
#include <sys/dbuf.h>
#include <time.h>
#include <sys/zfs_znode.h>
#include <sys/zap.h>
#include <sys/dmu.h>
#include <sys/nvpair.h>
#include <umem.h>
#include <libuzfs_impl.h>
#include <stdio.h>
#include <sys/dmu_objset.h>

typedef enum uzfs_attr_type {
    UZFS_INO,
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
    UZFS_ATIME,
    UZFS_MTIME,
    UZFS_CTIME,
    UZFS_BTIME,
    UZFS_DXATTR, // sa index for dir xattr inode
    UZFS_XATTR,  // sa index for sa xattr (name, value) pairs
    UZFS_END
} uzfs_attr_type_t;

sa_attr_reg_t uzfs_attr_table[UZFS_END+1] = {
    {"UZFS_INO", sizeof (uint64_t), SA_UINT64_ARRAY, 0},
    {"UZFS_PINO", sizeof (uint64_t), SA_UINT64_ARRAY, 1},
    {"UZFS_PSID", sizeof (uint32_t), SA_UINT32_ARRAY, 2},
    {"UZFS_FTYPE", sizeof (FileType), SA_UINT32_ARRAY, 3},
    {"UZFS_GEN", sizeof (uint64_t), SA_UINT64_ARRAY, 4},
    {"UZFS_NLINK", sizeof (uint32_t), SA_UINT32_ARRAY, 5},
    {"UZFS_PERM", sizeof (uint32_t), SA_UINT32_ARRAY, 6},
    {"UZFS_UID", sizeof (uint32_t), SA_UINT32_ARRAY, 7},
    {"UZFS_GID", sizeof (uint32_t), SA_UINT32_ARRAY, 8},
    {"UZFS_SIZE", sizeof (uint64_t), SA_UINT64_ARRAY, 9},
    {"UZFS_BLKSIZE", sizeof (uint64_t), SA_UINT64_ARRAY, 10},
    {"UZFS_BLOCKS", sizeof (uint64_t), SA_UINT64_ARRAY, 11},
    {"UZFS_NSID", sizeof (uint32_t), SA_UINT32_ARRAY, 12},
    {"UZFS_ATIME", sizeof (uint64_t) * 2, SA_UINT64_ARRAY, 13},
    {"UZFS_MTIME", sizeof (uint64_t) * 2, SA_UINT64_ARRAY, 14},
    {"UZFS_CTIME", sizeof (uint64_t) * 2, SA_UINT64_ARRAY, 15},
    {"UZFS_BTIME", sizeof (uint64_t) * 2, SA_UINT64_ARRAY, 16},
    {"UZFS_DXATTR", sizeof (uint64_t), SA_UINT64_ARRAY, 17},
    {"UZFS_XATTR", 0, SA_UINT8_ARRAY, 0},
    {NULL, 0, 0, 0}
};

void
libuzfs_create_objset_attr(objset_t *os, dmu_tx_t *tx)
{
    int err = zap_create_claim(os, MASTER_NODE_OBJ, DMU_OT_MASTER_NODE, 
                               DMU_OT_NONE, 0, tx);
    ASSERT(err == 0);

    uint64_t sa_obj = zap_create(os, DMU_OT_SA_MASTER_NODE,
                                 DMU_OT_NONE, 0, tx);
    err = zap_add(os, MASTER_NODE_OBJ, ZFS_SA_ATTRS, 8, 1, &sa_obj, tx);
    ASSERT(err == 0);
}

void
libuzfs_open_dataset_attr(libuzfs_dataset_handle_t *dhp)
{
    uint64_t sa_obj;
    VERIFY0(zap_lookup(dhp->os, MASTER_NODE_OBJ, ZFS_SA_ATTRS, 8, 1, &sa_obj));
    sa_setup(dhp->os, sa_obj, uzfs_attr_table, UZFS_END, &dhp->uzfs_attr_table);
}

// make sure sa_attrs has enough space
static void
libuzfs_add_bulk_attr(libuzfs_dataset_handle_t *dhp, sa_bulk_attr_t *sa_attrs,
                      int *cnt, uzfs_attr_t *attr)
{
    sa_attr_type_t *attr_tbl = dhp->uzfs_attr_table;
    SA_ADD_BULK_ATTR(sa_attrs, (*cnt), attr_tbl[UZFS_INO], NULL, &attr->ino, 8);
    SA_ADD_BULK_ATTR(sa_attrs, (*cnt), attr_tbl[UZFS_PINO], NULL, &attr->pino, 8);
    SA_ADD_BULK_ATTR(sa_attrs, (*cnt), attr_tbl[UZFS_PSID], NULL, &attr->psid, 4);
    SA_ADD_BULK_ATTR(sa_attrs, (*cnt), attr_tbl[UZFS_FTYPE], NULL, &attr->ftype, 4);
    SA_ADD_BULK_ATTR(sa_attrs, (*cnt), attr_tbl[UZFS_GEN], NULL, &attr->gen, 8);
    SA_ADD_BULK_ATTR(sa_attrs, (*cnt), attr_tbl[UZFS_NLINK], NULL, &attr->nlink, 4);
    SA_ADD_BULK_ATTR(sa_attrs, (*cnt), attr_tbl[UZFS_PERM], NULL, &attr->perm, 4);
    SA_ADD_BULK_ATTR(sa_attrs, (*cnt), attr_tbl[UZFS_UID], NULL, &attr->uid, 4);
    SA_ADD_BULK_ATTR(sa_attrs, (*cnt), attr_tbl[UZFS_GID], NULL, &attr->gid, 4);
    SA_ADD_BULK_ATTR(sa_attrs, (*cnt), attr_tbl[UZFS_SIZE], NULL, &attr->size, 8);
    SA_ADD_BULK_ATTR(sa_attrs, (*cnt), attr_tbl[UZFS_BLKSIZE], NULL, &attr->blksize, 8);
    SA_ADD_BULK_ATTR(sa_attrs, (*cnt), attr_tbl[UZFS_BLOCKS], NULL, &attr->blocks, 8);
    SA_ADD_BULK_ATTR(sa_attrs, (*cnt), attr_tbl[UZFS_NSID], NULL, &attr->nsid, 4);
    SA_ADD_BULK_ATTR(sa_attrs, (*cnt), attr_tbl[UZFS_ATIME], NULL, &attr->atime, 16);
    SA_ADD_BULK_ATTR(sa_attrs, (*cnt), attr_tbl[UZFS_MTIME], NULL, &attr->mtime, 16);
    SA_ADD_BULK_ATTR(sa_attrs, (*cnt), attr_tbl[UZFS_CTIME], NULL, &attr->ctime, 16);
    SA_ADD_BULK_ATTR(sa_attrs, (*cnt), attr_tbl[UZFS_BTIME], NULL, &attr->btime, 16);
}

static int
libuzfs_object_attr_init(libuzfs_dataset_handle_t *dhp, sa_handle_t *sa_hdl, dmu_tx_t *tx)
{
    sa_bulk_attr_t sa_attrs[20];
    int cnt = 0;
    uzfs_attr_t attr;
    memset(&attr, 0, sizeof(attr));
    libuzfs_add_bulk_attr(dhp, sa_attrs, &cnt, &attr);
    return sa_replace_all_by_template(sa_hdl, sa_attrs, cnt, tx);
}

static void
libuzfs_create_inode_with_type_impl(libuzfs_dataset_handle_t *dhp, uint64_t *obj,
                                    boolean_t claiming, libuzfs_inode_type_t type,
                                    dmu_tx_t *tx)
{
    objset_t *os = dhp->os;
    // create/claim object
    int dnodesize = dmu_objset_dnodesize(os);
    int bonuslen = DN_BONUS_SIZE(dnodesize);
    if (type == INODE_FILE) {
        if (claiming) {
            ASSERT(*obj != 0);
            VERIFY0(dmu_object_claim_dnsize(os, *obj, DMU_OT_PLAIN_FILE_CONTENTS, 0,
                                            DMU_OT_SA, bonuslen, dnodesize, tx));
        } else {
            *obj = dmu_object_alloc_dnsize(os, DMU_OT_PLAIN_FILE_CONTENTS, 0,
                                           DMU_OT_SA, bonuslen, dnodesize, tx);
        }
    } else {
        if (claiming) {
            ASSERT(*obj != 0);
            VERIFY0(zap_create_claim_dnsize(os, *obj, DMU_OT_DIRECTORY_CONTENTS,
                                            DMU_OT_SA, bonuslen, dnodesize, tx));
        } else {
            *obj = zap_create_dnsize(os, DMU_OT_DIRECTORY_CONTENTS,
                                     DMU_OT_SA, bonuslen, dnodesize, tx);
        }
    }

    sa_handle_t *sa_hdl;
    sa_handle_get(os, *obj, NULL, SA_HDL_PRIVATE, &sa_hdl);
    VERIFY0(libuzfs_object_attr_init(dhp, sa_hdl, tx));
    // maybe we shouldn't destroy handle here
    sa_handle_destroy(sa_hdl);
}

int
libuzfs_create_inode_with_type(libuzfs_dataset_handle_t *dhp, uint64_t *obj,
                               boolean_t claiming, libuzfs_inode_type_t type,
                               uint64_t *txg)
{
    objset_t *os = dhp->os;
    dmu_tx_t *tx = dmu_tx_create(os);
    dmu_tx_hold_sa_create(tx, sizeof(uzfs_attr_t));
    if (type == INODE_DIR) {
        dmu_tx_hold_zap(tx, DMU_NEW_OBJECT, B_TRUE, NULL);
    }
    int err = dmu_tx_assign(tx, TXG_WAIT);
    if (err != 0) {
        dmu_tx_abort(tx);
        return err;
    }
    libuzfs_create_inode_with_type_impl(dhp, obj, claiming, type, tx);
    if (txg != NULL) {
        *txg = tx->tx_txg;
    }
    dmu_tx_commit(tx);
    return 0;
}

int
libuzfs_inode_delete(libuzfs_dataset_handle_t *dhp, uint64_t ino,
                     libuzfs_inode_type_t type, uint64_t *txg)
{
    objset_t *os = dhp->os;
    sa_handle_t *sa_hdl;
    int err = sa_handle_get(os, ino, NULL, SA_HDL_PRIVATE, &sa_hdl);
    if (err != 0) {
        return err;
    }

    dmu_tx_t *tx = dmu_tx_create(os);
    dmu_tx_hold_free(tx, ino, 0, DMU_OBJECT_END);
    uint64_t xattr_obj = 0;
    sa_attr_type_t *attr_tbl = dhp->uzfs_attr_table;
    err = sa_lookup(sa_hdl, attr_tbl[UZFS_DXATTR],
                    &xattr_obj, sizeof(xattr_obj));

    uint64_t *value_objs = NULL;
    uint64_t obj_cnt = 0;
    if (err == 0 && xattr_obj != 0) {
        dmu_tx_hold_free(tx, xattr_obj, 0, DMU_OBJECT_END);
        if ((err = zap_count(os, xattr_obj, &obj_cnt)) != 0) {
            goto out;
        }
        if (obj_cnt > 0) {
            value_objs = (uint64_t *)umem_alloc(obj_cnt * sizeof(uint64_t),
                                                UMEM_NOFAIL);
        }
        zap_cursor_t zc;
        zap_attribute_t attr;
        uint64_t pos = 0;
        for (zap_cursor_init(&zc, os, xattr_obj); 
             zap_cursor_retrieve(&zc, &attr) == 0;
             zap_cursor_advance(&zc)) {
            ASSERT(attr.za_integer_length == 8);
            ASSERT(attr.za_num_integers == 1);

            uint64_t value_obj;
            VERIFY0(zap_lookup(os, xattr_obj, attr.za_name, 8, 1, &value_obj));
            value_objs[pos++] = value_obj;
            dmu_tx_hold_free(tx, value_obj, 0, DMU_OBJECT_END);
        }
        zap_cursor_fini(&zc);
    }

    if ((err = dmu_tx_assign(tx, TXG_WAIT)) != 0) {
        goto out;
    }

    if (xattr_obj != 0) {
        zap_destroy(os, xattr_obj, tx);
        for (uint64_t i = 0; i < obj_cnt; ++i) {
            dmu_object_free(os, value_objs[i], tx);
        }
    }

    sa_handle_destroy(sa_hdl);
    if (type == INODE_DIR) {
        VERIFY0(zap_destroy(os, ino, tx));
    } else {
        VERIFY0(dmu_object_free(os, ino, tx));
    }

out:
    if (obj_cnt > 0) {
        umem_free(value_objs, sizeof(uint64_t) * obj_cnt);
    }

    if (err == 0) {
        if (txg != NULL) {
            *txg = tx->tx_txg;
        }
        dmu_tx_commit(tx);
    } else {
        sa_handle_destroy(sa_hdl);
        dmu_tx_abort(tx);
    }
    return err;
}

int
libuzfs_inode_getattr(libuzfs_dataset_handle_t *dhp, uint64_t ino,
                      void *attr, uint64_t size)
{
    ASSERT(size == sizeof(uzfs_attr_t));
    sa_handle_t *sa_hdl;
    int err = sa_handle_get(dhp->os, ino, NULL, SA_HDL_PRIVATE, &sa_hdl);
    if (err != 0) {
        return err;
    }

    sa_bulk_attr_t sa_attrs[20];
    int cnt = 0;
    libuzfs_add_bulk_attr(dhp, sa_attrs, &cnt, (uzfs_attr_t *)attr);
    err = sa_bulk_lookup(sa_hdl, sa_attrs, cnt);

    sa_handle_destroy(sa_hdl);
    return err;
}

int
libuzfs_inode_setattr(libuzfs_dataset_handle_t *dhp, uint64_t ino,
                      const void *attr, uint64_t size, uint64_t *txg)
{
    ASSERT(size == sizeof(uzfs_attr_t));
    sa_handle_t *sa_hdl;
    objset_t *os = dhp->os;
    int err = sa_handle_get(os, ino, NULL, SA_HDL_PRIVATE, &sa_hdl);
    if (err != 0) {
        return err;
    }

    dmu_tx_t *tx = dmu_tx_create(os);
    dmu_tx_hold_sa(tx, sa_hdl, B_FALSE);
    if ((err = dmu_tx_assign(tx, TXG_WAIT)) != 0) {
        dmu_tx_abort(tx);
        return err;
    }

    sa_bulk_attr_t sa_attrs[20];
    int cnt = 0;
    libuzfs_add_bulk_attr(dhp, sa_attrs, &cnt, (uzfs_attr_t *)attr);
    VERIFY0(sa_bulk_update(sa_hdl, sa_attrs, cnt, tx));
    sa_handle_destroy(sa_hdl);
    if (txg != NULL) {
        *txg = tx->tx_txg;
    }
    dmu_tx_commit(tx);
    return 0;
}

static int
libuzfs_get_nvlist_from_handle(const sa_attr_type_t *sa_tbl, nvlist_t **nvl, sa_handle_t *sa_hdl)
{
    int xattr_size;
    int err = sa_size(sa_hdl, sa_tbl[UZFS_XATTR], &xattr_size);
    if (err != 0) {
        return err;
    }

    char *obj = vmem_alloc(xattr_size, KM_SLEEP);
    err = sa_lookup(sa_hdl, sa_tbl[UZFS_XATTR], obj, xattr_size);
    if (err != 0) {
        return err;
    }

    err = nvlist_unpack(obj, xattr_size, nvl, KM_SLEEP);
    vmem_free(obj, xattr_size);
    return err;
}

static int
libuzfs_kvattr_set_nvlist(nvlist_t *nvl, int err_check_sa, int *err_check_sa_enough,
                          const char *name, const char *value, size_t value_size, 
                          size_t *xattr_sa_size)
{
    int err = 0;
    if (value == NULL && err_check_sa == 0) {
        err = nvlist_remove(nvl, name, DATA_TYPE_BYTE_ARRAY);
    } else if (value != NULL) {
        err = nvlist_add_byte_array(nvl, name, (uchar_t *)value, (uint_t)value_size);
    }
    if (err != 0) {
        return err;
    }
    err = nvlist_size(nvl, xattr_sa_size, NV_ENCODE_XDR);
    if (err != 0) {
        return err;
    }
    if (*xattr_sa_size > DXATTR_MAX_SA_SIZE || *xattr_sa_size > SA_ATTR_MAX_LEN) {
        *err_check_sa_enough = EFBIG;
        if ((err = nvlist_remove(nvl, name, DATA_TYPE_BYTE_ARRAY)) != 0 
             && value != NULL) {
            return err;
        }
        return nvlist_size(nvl, xattr_sa_size, NV_ENCODE_XDR);
    }
    return err;
}

static void
libuzfs_save_xattr_dir(sa_handle_t *sa_hdl, libuzfs_dataset_handle_t *dhp,
                       uint64_t xattr_obj, uint64_t value_obj, const char *name, 
                       const char *value, size_t size, dmu_tx_t *tx)
{
    sa_attr_type_t *attr_tbl = dhp->uzfs_attr_table;
    objset_t *os = sa_hdl->sa_os;
    int dnodesize = dmu_objset_dnodesize(os);
    if (xattr_obj == DMU_NEW_OBJECT) {
        xattr_obj = zap_create_dnsize(os, DMU_OT_DIRECTORY_CONTENTS, 
                                      DMU_OT_NONE, 0, dnodesize, tx);
        VERIFY0(sa_update(sa_hdl, attr_tbl[UZFS_DXATTR], &xattr_obj, 8, tx));
    }
    if (value_obj == DMU_NEW_OBJECT) {
        libuzfs_create_inode_with_type_impl(dhp, &value_obj, B_FALSE, INODE_FILE, tx);
        VERIFY0(zap_update(os, xattr_obj, name, 8, 1, &value_obj, tx));
    }

    dmu_write(os, value_obj, 0, size, value, tx);
    sa_handle_t *value_obj_hdl = NULL;
    VERIFY0(sa_handle_get(os, value_obj, NULL, SA_HDL_PRIVATE, &value_obj_hdl));
    VERIFY0(sa_update(value_obj_hdl, attr_tbl[UZFS_SIZE], &size, 8, tx));
    sa_handle_destroy(value_obj_hdl);
}

int
libuzfs_inode_set_kvattr(libuzfs_dataset_handle_t *dhp, uint64_t ino,
    					 const char *name, const char *value,
						 uint64_t size, int flags, uint64_t *txg)
{
    // 1. this name is in sa: 
    //    1.1 sa has enough space for new value, just put in
    //    1.2 no enough space, delete from sa and put in dir
    // 2. in dir
    //    2.1 sa enough space, delete from dir and put in sa
    //    2.2 not enough, put in dir
    // 3. neigther
    //    3.1 sa enough, put in sa
    //    3.2 not enough, put in dir
    sa_handle_t	*sa_hdl;
    objset_t *os = dhp->os;
    int err = sa_handle_get(os, ino, NULL, SA_HDL_PRIVATE, &sa_hdl);
    if (err != 0) {
        return err;
    }

    nvlist_t *nvl;
    sa_attr_type_t *attr_tbl = dhp->uzfs_attr_table;
    err = libuzfs_get_nvlist_from_handle(attr_tbl, &nvl, sa_hdl);
    if (err == ENOENT) {
        err = nvlist_alloc(&nvl, NV_UNIQUE_NAME, KM_SLEEP);
    }

    if (err != 0) {
        goto out;
    }

    int err_check_sa = !nvlist_exists(nvl, name);
    int err_check_sa_enough = 0;
    size_t xattr_size = 0;
    err = libuzfs_kvattr_set_nvlist(nvl, err_check_sa, &err_check_sa_enough, 
                                    name, value, size, &xattr_size);
    if (err != 0) {
        goto out2;
    }

    uint64_t xattr_obj = DMU_NEW_OBJECT;
    int err_check_dir = sa_lookup(sa_hdl, attr_tbl[UZFS_DXATTR], 
                                  &xattr_obj, sizeof(xattr_obj));
    ASSERT(err_check_dir == ENOENT || err_check_dir == 0);
    uint64_t value_obj = DMU_NEW_OBJECT;
    if (err_check_sa != 0 && err_check_dir == 0) {
        err_check_dir = zap_lookup(os, xattr_obj, name, 8, 1, &value_obj);
        if (err_check_dir != ENOENT && err_check_dir != 0) {
            err = err_check_dir;
            goto out2;
        }
    }

    // this is remove operation, but we didn't find the name in either sa or dir
    if (value == NULL && err_check_sa != 0 && err_check_dir != 0) {
        err = ENOENT;
        goto out2;
    }

    dmu_tx_t *tx = dmu_tx_create(os);
    dmu_tx_hold_sa(tx, sa_hdl, B_TRUE);
    dmu_tx_hold_zap(tx, xattr_obj, B_TRUE, name);
    dmu_tx_hold_sa_create(tx, sizeof(uzfs_attr_t));
    dmu_tx_hold_write(tx, value_obj, 0, size);
    if ((err_check_sa_enough == 0 || value == NULL) &&
        err_check_sa != 0 && err_check_dir == 0) {
        dmu_tx_hold_free(tx, value_obj, 0, DMU_OBJECT_END);
    }

    if ((err = dmu_tx_assign(tx, TXG_WAIT)) != 0) {
        goto out3;
    }

    // name was deleted/inserted in nvl before, so we just need to save the nvl
    if (err_check_sa == 0 || (err_check_sa_enough == 0 && value != NULL)) {
        if (xattr_size > 0) {
            char *packed_nvl = vmem_alloc(xattr_size, KM_SLEEP);
            VERIFY0(nvlist_pack(nvl, &packed_nvl, &xattr_size,
                                NV_ENCODE_XDR, KM_SLEEP));
            VERIFY0(sa_update(sa_hdl, attr_tbl[UZFS_XATTR],
                                packed_nvl, xattr_size, tx));
            vmem_free(packed_nvl, xattr_size);
        } else {
            VERIFY0(sa_remove(sa_hdl, attr_tbl[UZFS_XATTR], tx));
        }
    }

    // free the object that saves old value and remove its entry in xattr_obj
    if ((err_check_sa_enough == 0 || value == NULL) &&
        err_check_sa != 0 && err_check_dir == 0) {
        dmu_object_free(os, value_obj, tx);
        // should we delete the xattr_obj and its sa entry when xattr_obj is empty?
        VERIFY0(zap_remove(os, xattr_obj, name, tx));
    }

    if (value != NULL && err_check_sa_enough != 0) {
        libuzfs_save_xattr_dir(sa_hdl, dhp, xattr_obj, value_obj,
                               name, value, size, tx);
    }

out3:
    if (err != 0) {
        dmu_tx_abort(tx);
    } else {
        if (txg != NULL) {
            *txg = tx->tx_txg;
        }
        dmu_tx_commit(tx);
    }
out2:
    nvlist_free(nvl);
out:
    sa_handle_destroy(sa_hdl);
    return err;
}

static int
libuzfs_inode_get_kvattr_dir(const sa_attr_type_t *sa_tbl, sa_handle_t *sa_hdl,
                             const char *name, char *value, size_t size)
{
    // get zap object from sa
    uint64_t xattr_obj;
    int err = sa_lookup(sa_hdl, sa_tbl[UZFS_DXATTR],
                        &xattr_obj, sizeof(uint64_t));
    if (err != 0) {
        return err;
    }

    objset_t *os = sa_hdl->sa_os;
    uint64_t value_obj;
    if((err = zap_lookup(os, xattr_obj, name, 8, 1, &value_obj)) != 0) {
        return err;
    }

    // TODO(sundengyu): use one dmu_read
    sa_handle_t *value_obj_hdl = NULL;
    err = sa_handle_get(os, value_obj, NULL, SA_HDL_PRIVATE, &value_obj_hdl);
    if (err != 0) {
        return err;
    }
    uint64_t stored_size;
    err = sa_lookup(value_obj_hdl, sa_tbl[UZFS_SIZE], &stored_size, 8);
    sa_handle_destroy(value_obj_hdl);
    if (err == 0 && stored_size > size) {
        err = ERANGE;
    } else if (err == 0) {
        err = dmu_read(os, value_obj, 0, stored_size, value, DMU_READ_NO_PREFETCH);
    }
    return err;
}

static int
libuzfs_inode_get_kvattr_sa(const sa_attr_type_t *sa_tbl, sa_handle_t *sa_hdl,
                            const char *name, char *value, size_t size)
{
    nvlist_t *nvl;
    int err = libuzfs_get_nvlist_from_handle(sa_tbl, &nvl, sa_hdl);
    if (err != 0) {
        return err;
    }

    uchar_t *nv_value;
    uint_t nv_size = 0;
    err = nvlist_lookup_byte_array(nvl, name, &nv_value, &nv_size);
    if (err == 0 && nv_size <= size) {
        if (value != NULL) {
            memcpy(value, nv_value, nv_size);
        }
    }
    nvlist_free(nvl);

    if (nv_size > size) {
        return ERANGE;
    }
    return err;
}

int
libuzfs_inode_get_kvattr(libuzfs_dataset_handle_t *dhp, uint64_t ino,
                         const char *name, char *value, uint64_t size,
                         int flags)
{
    sa_handle_t	*sa_hdl;
    int err = sa_handle_get(dhp->os, ino, NULL, SA_HDL_PRIVATE, &sa_hdl);
    if (err != 0) {
        return err;
    }

    err = libuzfs_inode_get_kvattr_sa(dhp->uzfs_attr_table, sa_hdl,
                                      name, value, size);
    if (err == 0 || err != ENOENT) {
        goto out;
    }

    err = libuzfs_inode_get_kvattr_dir(dhp->uzfs_attr_table, sa_hdl,
                                       name, value, size);

out:
    sa_handle_destroy(sa_hdl);
    return err;
}

int
libuzfs_inode_remove_kvattr(libuzfs_dataset_handle_t *dhp, uint64_t ino,
                            const char *name, uint64_t *txg)
{
    return libuzfs_inode_set_kvattr(dhp, ino, name, NULL, 0, 0, txg);
}
