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
#include "sys/avl.h"
#include "sys/fs/zfs.h"
#include "sys/spa.h"
#include "sys/stdtypes.h"
#include "sys/uio.h"
#include "sys/zfs_context.h"
#include "umem.h"
#include <asm/unistd_64.h>
#include <bits/stdint-uintn.h>
#include <bits/types/struct_iovec.h>
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

#define	MAX_AIO_EVENTS	4096

typedef struct io_workers_ctx {
	aio_context_t	io_ctx;		// io context for aio
	kmutex_t	submit_lock;	// make sure only 1 submitter
	zio_t		*submit_head;	// current head of submit list
	kthread_t	*reaper;	// identifies the reaper
	volatile boolean_t	stop;	// whether reaper should stop
} io_workers_ctx_t;

static io_workers_ctx_t workers_ctx;

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

	return (0);
}

static void
vdev_aio_file_close(vdev_t *vd)
{
	vdev_file_t *vf = vd->vdev_tsd;

	if (vd->vdev_reopening || vf == NULL)
		return;

	if (vf->vf_fd >= 0) {
		close(vf->vf_fd);
	}

	vd->vdev_delayed_close = B_FALSE;
	kmem_free(vf, sizeof (vdev_file_t));
	vd->vdev_tsd = NULL;
}

static int
zio_compare(const void *arg1, const void *arg2)
{
	const zio_t *zio1 = arg1;
	const zio_t *zio2 = arg2;
	ASSERT(zio1->io_type == zio2->io_type);

	int fd1 = ((vdev_file_t *)zio1->io_vd->vdev_tsd)->vf_fd;
	int fd2 = ((vdev_file_t *)zio2->io_vd->vdev_tsd)->vf_fd;

	if (fd1 < fd2) {
		return (-1);
	}
	if (fd1 > fd2) {
		return (1);
	}

	if (zio1->io_offset < zio2->io_offset) {
		return (-1);
	}
	if (zio1->io_offset > zio2->io_offset) {
		return (1);
	}

	return (0);
}

static boolean_t
can_merge(zio_t *zio1, zio_t *zio2)
{
	int fd1 = ((vdev_file_t *)zio1->io_vd->vdev_tsd)->vf_fd;
	int fd2 = ((vdev_file_t *)zio2->io_vd->vdev_tsd)->vf_fd;

	return (fd1 == fd2 && zio2->io_offset == zio1->io_offset
	    + zio1->io_size);
}

static void
merge_and_submit_tasks(avl_tree_t *merge_tree, uint16_t opcode)
{
	zio_t *cur = avl_first(merge_tree);
	while (cur != NULL) {
		zio_t *head = cur;
		cur->next = NULL;
		struct iovec iovs[64];
		iovs[0].iov_base = cur->buf;
		iovs[0].iov_len = cur->io_size;
		uint64_t offset = cur->io_offset;
		int niovs = 1;

		cur = AVL_NEXT(merge_tree, cur);
		while (niovs < 64 && cur != NULL) {
			if (can_merge(head, cur)) {
				iovs[niovs].iov_base = cur->buf;
				iovs[niovs].iov_len = cur->io_size;
				++niovs;
				cur->next = head;
				head = cur;
			} else {
				break;
			}
			cur = AVL_NEXT(merge_tree, cur);
		}

		struct iocb iocb;
		memset(&iocb, 0, sizeof (iocb));
		iocb.aio_data = (uint64_t)head;
		iocb.aio_lio_opcode = opcode;
		iocb.aio_fildes = ((vdev_file_t *)head->io_vd->vdev_tsd)->vf_fd;
		iocb.aio_offset = offset;
		iocb.aio_buf = (uint64_t)iovs;
		iocb.aio_nbytes = niovs;
		struct iocb *ptr = &iocb;
		VERIFY3S(TEMP_FAILURE_RETRY(syscall(SYS_io_submit,
		    workers_ctx.io_ctx, 1, &ptr)), ==, 1);
	}
}

static void
do_submit_zio_tasks(void)
{
	mutex_enter(&workers_ctx.submit_lock);

	zio_t *head = atomic_load_ptr(&workers_ctx.submit_head);
	while (1) {
		zio_t *cur_head = atomic_cas_ptr(&workers_ctx.submit_head,
		    head, NULL);
		if (cur_head == head) {
			break;
		}
		head = cur_head;
	}

	avl_tree_t read_merge_tree;
	avl_create(&read_merge_tree, zio_compare,
	    sizeof (zio_t), offsetof(zio_t, merge_tree_node));
	avl_tree_t write_merge_tree;
	avl_create(&write_merge_tree, zio_compare,
	    sizeof (zio_t), offsetof(zio_t, merge_tree_node));

	while (head != NULL) {
		zio_t *next = head->next;
		int fd = ((vdev_file_t *)head->io_vd->vdev_tsd)->vf_fd;
		switch (head->io_type) {
		case ZIO_TYPE_READ:
			head->buf = abd_borrow_buf(head->io_abd, head->io_size);
			avl_add(&read_merge_tree, head);
			break;
		case ZIO_TYPE_WRITE:
			head->buf = abd_borrow_buf_copy(
			    head->io_abd, head->io_size);
			avl_add(&write_merge_tree, head);
			break;
		case ZIO_TYPE_IOCTL: {
			struct iocb iocb;
			struct iocb *ptr = &iocb;
			memset(&iocb, 0, sizeof (iocb));
			iocb.aio_data = (uint64_t)head;
			iocb.aio_fildes = fd;
			iocb.aio_lio_opcode = IOCB_CMD_FSYNC;
			VERIFY3S(TEMP_FAILURE_RETRY(syscall(SYS_io_submit,
			    workers_ctx.io_ctx, 1, &ptr)), ==, 1);
		}
			break;
		default:
			cmn_err(CE_PANIC, "unknown zio type: %d",
			    head->io_type);
			break;
		}
		head = next;
	}

	merge_and_submit_tasks(&read_merge_tree, IOCB_CMD_PREADV);
	merge_and_submit_tasks(&write_merge_tree, IOCB_CMD_PWRITEV);
	mutex_exit(&workers_ctx.submit_lock);
}

static void
submit_zio_task(zio_t *zio)
{
	zio->next = atomic_load_ptr(&workers_ctx.submit_head);
	boolean_t earlier = B_TRUE;
	while (1) {
		zio_t *next = zio->next;
		earlier = next == NULL;
		zio_t *head = atomic_cas_ptr(&workers_ctx.submit_head,
		    next, zio);
		if (head == next) {
			break;
		}
		zio->next = head;
	}

	if (earlier) {
		do_submit_zio_tasks();
	}
}

static void
do_trim_work(void *arg)
{
	zio_t *zio = arg;
	vdev_t *vd = zio->io_vd;
	vdev_file_t *vf = vd->vdev_tsd;
	uint64_t range[2] = {zio->io_offset, zio->io_size};
	if (TEMP_FAILURE_RETRY(ioctl(vf->vf_fd, BLKDISCARD, range))) {
		zio->io_error = errno;
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
		thread_create(NULL, 0, do_trim_work, zio,
		    0, NULL, TS_RUN | TS_BLOCKING, defclsyspri);
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

static void
zio_task_reaper(void *args)
{
#define	MAX_PROCESSED_EVENTS 64
	io_workers_ctx_t *ctx = args;
	struct io_event events[MAX_PROCESSED_EVENTS];
	struct timespec interval = {1, 0};
	while (!atomic_load_32(&ctx->stop)) {
		int nevents = syscall(SYS_io_getevents, ctx->io_ctx,
		    1, MAX_PROCESSED_EVENTS, events, &interval);
		if (nevents < 0) {
			cmn_err(CE_WARN, "failed to call io genevents,"
			    " error: %d", errno);
		}
		for (int i = 0; i < nevents; ++i) {
			zio_t *zio = (zio_t *)events[i].data;
			ssize_t res = events[i].res;

			while (zio != NULL) {
				zio_t *next = zio->next;

				if (zio->io_type == ZIO_TYPE_READ) {
					abd_return_buf_copy(zio->io_abd,
					    zio->buf, zio->io_size);
				} else if (zio->io_type == ZIO_TYPE_WRITE) {
					abd_return_buf(zio->io_abd,
					    zio->buf, zio->io_size);
				}

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
				zio = next;
			}
		}
	}
}

void
vdev_aio_file_init(void)
{
	memset(&workers_ctx, 0, sizeof (workers_ctx));
	VERIFY0(syscall(SYS_io_setup, MAX_AIO_EVENTS, &workers_ctx.io_ctx));
	workers_ctx.stop = B_FALSE;
	workers_ctx.submit_head = NULL;

	mutex_init(&workers_ctx.submit_lock, NULL, MUTEX_DEFAULT, NULL);

	workers_ctx.reaper = thread_create(NULL, 0, zio_task_reaper,
	    &workers_ctx, 0, NULL, TS_RUN | TS_JOINABLE | TS_BLOCKING,
	    defclsyspri);
}

void
vdev_aio_file_fini(void)
{
	workers_ctx.stop = B_TRUE;
	thread_join(workers_ctx.reaper);
	VERIFY0(syscall(SYS_io_destroy, workers_ctx.io_ctx));
	mutex_destroy(&workers_ctx.submit_lock);
}
