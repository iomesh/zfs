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
 * Copyright (c) 2023, SmartX Inc. All rights reserved.
 */

#include "atomic.h"
#include "sys/fs/zfs.h"
#include "sys/spa.h"
#include "sys/stdtypes.h"
#include "sys/zfs_context.h"
#include "umem.h"
#include <asm/unistd_64.h>
#include <bits/stdint-uintn.h>
#include <errno.h>
#include <linux/fs.h>
#include <stdio.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/vdev_file.h>
#include <sys/vdev_impl.h>
#include <sys/zio.h>
#include <sys/abd.h>
#include <sys/syscall.h>
#include <linux/aio_abi.h>
#include <unistd.h>
#include <semaphore.h>
#ifdef _KERNEL
#include <linux/falloc.h>
#endif

aio_ops_t aio_ops = {NULL};

static void
vdev_aio_file_hold(vdev_t *vd)
{
	ASSERT(vd->vdev_path != NULL);
}

static void
vdev_aio_file_rele(vdev_t *vd)
{
	ASSERT(vd->vdev_path != NULL);
}

static mode_t
vdev_aio_file_open_mode(spa_mode_t spa_mode)
{
	mode_t mode = 0;

	if ((spa_mode & SPA_MODE_READ) && (spa_mode & SPA_MODE_WRITE)) {
		mode = O_RDWR;
	} else if (spa_mode & SPA_MODE_READ) {
		mode = O_RDONLY;
	} else if (spa_mode & SPA_MODE_WRITE) {
		mode = O_WRONLY;
	}

	return (mode | O_LARGEFILE | O_DIRECT);
}

#ifdef ZFS_DEBUG
int fail_percent = 0;
#endif

static void
aio_done(void *arg, int64_t res)
{
	zio_t *zio = arg;
	if (zio->io_type == ZIO_TYPE_READ) {
		abd_return_buf_copy(zio->io_abd,
		    zio->buf, zio->io_size);
	} else if (zio->io_type == ZIO_TYPE_WRITE) {
		abd_return_buf(zio->io_abd,
		    zio->buf, zio->io_size);
	}

#ifdef ZFS_DEBUG
	if (rand() % 100 < fail_percent) {
		res = -EIO;
	}
#endif

	if (res < 0) {
		zio->io_error = -res;
	} else if (res < zio->io_size) {
		zfs_dbgmsg("partial io, type: %d, "
		    "offset: %lu, size: %lu, actual"
		    " iosize: %ld", zio->io_type,
		    zio->io_offset, zio->io_size, res);
		zio->io_error = SET_ERROR(ENOSPC);
	}

	zio_interrupt(zio);
}

static int
vdev_aio_file_open(vdev_t *vd, uint64_t *psize, uint64_t *max_psize,
    uint64_t *logical_ashift, uint64_t *physical_ashift)
{
	vdev_file_t *vf;
	struct stat64 zfa;

	/*
	 * Rotational optimizations only make sense on block devices.
	 */
	vd->vdev_nonrot = B_TRUE;

	/*
	 * Allow TRIM on file based vdevs.  This may not always be supported,
	 * since it depends on your kernel version and underlying filesystem
	 * type but it is always safe to attempt.
	 */
	vd->vdev_has_trim = B_TRUE;

	/*
	 * Disable secure TRIM on file based vdevs.  There is no way to
	 * request this behavior from the underlying filesystem.
	 */
	vd->vdev_has_securetrim = B_FALSE;

	/*
	 * We must have a pathname, and it must be absolute.
	 */
	if (vd->vdev_path == NULL || vd->vdev_path[0] != '/') {
		vd->vdev_stat.vs_aux = VDEV_AUX_BAD_LABEL;
		return (SET_ERROR(EINVAL));
	}

	/*
	 * Reopen the device if it's not currently open.  Otherwise,
	 * just update the physical size of the device.
	 */
	if (vd->vdev_tsd != NULL) {
		ASSERT(vd->vdev_reopening);
		vf = vd->vdev_tsd;
		goto skip_open;
	}

	vf = vd->vdev_tsd = kmem_zalloc(sizeof (vdev_file_t), KM_SLEEP);

	/*
	 * We always open the files from the root of the global zone, even if
	 * we're in a local zone.  If the user has gotten to this point, the
	 * administrator has already decided that the pool should be available
	 * to local zone users, so the underlying devices should be as well.
	 */
	ASSERT(vd->vdev_path != NULL && vd->vdev_path[0] == '/');

	vf->vf_fd = open(vd->vdev_path,
	    vdev_aio_file_open_mode(spa_mode(vd->vdev_spa)));
	if (vf->vf_fd < 0) {
		vd->vdev_stat.vs_aux = VDEV_AUX_OPEN_FAILED;
		return (errno);
	}

#ifdef _KERNEL
	/*
	 * Make sure it's a regular file.
	 */
	if (zfs_file_getattr(fp, &zfa)) {
		return (SET_ERROR(ENODEV));
	}
	if (!S_ISREG(zfa.zfa_mode)) {
		vd->vdev_stat.vs_aux = VDEV_AUX_OPEN_FAILED;
		return (SET_ERROR(ENODEV));
	}
#endif

skip_open:

	if (fstat64_blk(vf->vf_fd, &zfa) < 0) {
		vd->vdev_stat.vs_aux = VDEV_AUX_OPEN_FAILED;
		return (errno);
	}

	*max_psize = *psize = zfa.st_size;
	*logical_ashift = UZFS_VDEV_ASHIFT;
	*physical_ashift = UZFS_VDEV_ASHIFT;

	if (!vd->vdev_reopening) {
		vf->aio_handle = aio_ops.register_aio_fd(vf->vf_fd, aio_done);
	}

	return (0);
}

static void
vdev_aio_file_close(vdev_t *vd)
{
	vdev_file_t *vf = vd->vdev_tsd;

	if (vd->vdev_reopening || vf == NULL)
		return;

	if (vf->vf_fd >= 0) {
		aio_ops.unregister_aio_fd(vf->aio_handle);
		close(vf->vf_fd);
	}

	vd->vdev_delayed_close = B_FALSE;
	kmem_free(vf, sizeof (vdev_file_t));
	vd->vdev_tsd = NULL;
}

static void
submit_zio_task(zio_t *zio)
{
	void *aio_handle = ((vdev_file_t *)zio->io_vd->vdev_tsd)->aio_handle;
	switch (zio->io_type) {
	case ZIO_TYPE_IOCTL:
		VERIFY3U(zio->io_cmd, ==, DKIOCFLUSHWRITECACHE);
		aio_ops.submit_aio_fsync(aio_handle, zio);
		break;
	case ZIO_TYPE_READ:
		zio->buf = abd_borrow_buf(zio->io_abd, zio->io_size);
		aio_ops.submit_aio_read(aio_handle, zio->io_offset,
		    zio->buf, zio->io_size, zio);
		break;
	case ZIO_TYPE_WRITE:
		zio->buf = abd_borrow_buf_copy(zio->io_abd, zio->io_size);
		aio_ops.submit_aio_write(aio_handle, zio->io_offset,
		    zio->buf, zio->io_size, zio);
		break;
	default:
		panic("zio type not expected: %d", zio->io_type);
		break;
	}
}

static void
do_trim_work(void *arg)
{
	zio_t *zio = arg;
	vdev_t *vd = zio->io_vd;
	uint64_t discard_granularity = vd->discard_granularity;
	if (unlikely(discard_granularity == 0)) {
		discard_granularity = 262144;
	}
	VERIFY(ISP2(discard_granularity));

	vdev_file_t *vf = vd->vdev_tsd;
	uint64_t offset = P2ROUNDUP(zio->io_offset, discard_granularity);
	uint64_t end = zio->io_offset + zio->io_size;
	end = end / discard_granularity * discard_granularity;
	uint64_t size = end > offset ? end - offset : 0;

	VERIFY(offset >= zio->io_offset);
	VERIFY(end <= zio->io_offset + zio->io_size);
	VERIFY(size <= zio->io_size);

	if (size > 0) {
		uint64_t range[2] = {offset, size};
		if (TEMP_FAILURE_RETRY(ioctl(vf->vf_fd, BLKDISCARD, range))) {
			zio->io_error = errno;
		}
	}

	zio_interrupt(zio);
}

static void
vdev_aio_file_io_start(zio_t *zio)
{
	vdev_t *vd = zio->io_vd;

	if (zio->io_type == ZIO_TYPE_IOCTL) {
		/* XXPOLICY */
		if (!vdev_readable(vd)) {
			zio->io_error = SET_ERROR(ENXIO);
			zio_interrupt(zio);
			return;
		}

		switch (zio->io_cmd) {
		case DKIOCFLUSHWRITECACHE:
			if (zfs_nocacheflush)
				break;

			// make vdev_aio_file_io_start nonblock
			submit_zio_task(zio);
			return;

		default:
			zio->io_error = SET_ERROR(ENOTSUP);
		}

		zio_execute(zio);
		return;
	} else if (zio->io_type == ZIO_TYPE_TRIM) {
		thread_create(NULL, 0, do_trim_work, zio, 0,
		    NULL, TS_RUN | TS_BLOCKING, defclsyspri);
		return;
	}

	zio->io_target_timestamp = zio_handle_io_delay(zio);

	submit_zio_task(zio);
}

static void
vdev_aio_file_io_done(zio_t *zio)
{
	(void) zio;
}

vdev_ops_t vdev_aio_file_ops = {
	.vdev_op_init = NULL,
	.vdev_op_fini = NULL,
	.vdev_op_open = vdev_aio_file_open,
	.vdev_op_close = vdev_aio_file_close,
	.vdev_op_asize = vdev_default_asize,
	.vdev_op_min_asize = vdev_default_min_asize,
	.vdev_op_min_alloc = NULL,
	.vdev_op_io_start = vdev_aio_file_io_start,
	.vdev_op_io_done = vdev_aio_file_io_done,
	.vdev_op_state_change = NULL,
	.vdev_op_need_resilver = NULL,
	.vdev_op_hold = vdev_aio_file_hold,
	.vdev_op_rele = vdev_aio_file_rele,
	.vdev_op_remap = NULL,
	.vdev_op_xlate = vdev_default_xlate,
	.vdev_op_rebuild_asize = NULL,
	.vdev_op_metaslab_init = NULL,
	.vdev_op_config_generate = NULL,
	.vdev_op_nparity = NULL,
	.vdev_op_ndisks = NULL,
	.vdev_op_type = VDEV_TYPE_AIO_FILE,	/* name of this vdev type */
	.vdev_op_leaf = B_TRUE			/* leaf vdev */
};

void
vdev_aio_file_init(void)
{
}

void
vdev_aio_file_fini(void)
{
}
