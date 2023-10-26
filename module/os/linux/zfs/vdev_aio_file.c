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
#include <stdio.h>
#include <string.h>
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

typedef struct zio_task {
	zio_t *zio;
	void *buf;
	uint32_t state;
	struct zio_task *next;
} zio_task_t;

typedef struct io_workers_ctx {
	aio_context_t	io_ctx;		// io context for aio
	zio_task_t	*head;		// current head of submit list
	sem_t		submit_sem;	// semaphore to wake up the submitter
	boolean_t	stop;		// whether reaper should stop
	pthread_t	submitter;	// identifies the submitter
	kthread_t	*reaper;	// identifies the reaper
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

static void
submit_zio_task(zio_t *zio)
{
	zio_task_t *task = umem_alloc(sizeof (zio_task_t), UMEM_NOFAIL);
	task->zio = zio;
	task->next = atomic_load_ptr(&workers_ctx.head);
	boolean_t earlier = B_TRUE;
	while (1) {
		earlier = task->next == NULL;
		zio_task_t *next = task->next;
		zio_task_t *head = atomic_cas_ptr(&workers_ctx.head,
		    task->next, task);
		if (head == next) {
			break;
		}
		task->next = head;
	}

	if (earlier) {
		VERIFY0(sem_post(&workers_ctx.submit_sem));
	}
}

static void
vdev_aio_file_io_start(zio_t *zio)
{
	vdev_t *vd = zio->io_vd;
	vdev_file_t *vf = vd->vdev_tsd;

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
		int mode = 0;

		ASSERT3U(zio->io_size, !=, 0);
#ifdef __linux__
		mode = FALLOC_FL_PUNCH_HOLE;
#endif
		// TODO(sundengyu): make fallocate async
		// and remove FALLOC_FL_KEEP_SIZE
		if (fallocate(vf->vf_fd, mode, zio->io_offset,
		    zio->io_size) < 0) {
			zio->io_error = errno;
		}
		zio_execute(zio);
		return;
	}

	zio->io_target_timestamp = zio_handle_io_delay(zio);

#ifdef ENABLE_MINITRACE_C
	if (unlikely(zio->span != NULL)) {
		mtr_span s = mtr_create_child_span_enter("read zio start", zio->span);
		mtr_destroy_span(s);
	}
#endif
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
prep_task(zio_task_t *task, struct iocb *io_cb)
{
	memset(io_cb, 0, sizeof (*io_cb));
	io_cb->aio_data = (uint64_t)task;
	zio_t *zio = task->zio;
	io_cb->aio_fildes = ((vdev_file_t *)zio->io_vd->vdev_tsd)->vf_fd;
	io_cb->aio_nbytes = zio->io_size;
	io_cb->aio_offset = zio->io_offset;
	switch (zio->io_type) {
	case ZIO_TYPE_IOCTL:
		VERIFY3U(zio->io_cmd, ==, DKIOCFLUSHWRITECACHE);
		io_cb->aio_lio_opcode = IOCB_CMD_FSYNC;
		break;
	case ZIO_TYPE_READ:
		task->buf = abd_borrow_buf(zio->io_abd, zio->io_size);
		io_cb->aio_buf = (uint64_t)task->buf;
		io_cb->aio_lio_opcode = IOCB_CMD_PREAD;
		break;
	case ZIO_TYPE_WRITE:
		task->buf = abd_borrow_buf_copy(zio->io_abd, zio->io_size);
		io_cb->aio_buf = (uint64_t)task->buf;
		io_cb->aio_lio_opcode = IOCB_CMD_PWRITE;
		break;
	default:
		panic("zio type not expected: %d", zio->io_type);
		break;
	}
}

static void*
zio_task_submitter(void *args)
{
	io_workers_ctx_t *ctx = args;
	while (likely(!ctx->stop)) {
		sem_wait(&ctx->submit_sem);
		// fetch current head
		zio_task_t *head = atomic_load_ptr(&workers_ctx.head);
		while (1) {
			zio_task_t *cur_head = atomic_cas_ptr(&workers_ctx.head,
			    head, NULL);
			if (cur_head == head) {
				break;
			}
			head = cur_head;
		}

		// TODO(sundengyu): submit tasks in a batch
		struct iocb iocb;
		struct iocb *ptr = &iocb;
		while (head != NULL) {
#ifdef ENABLE_MINITRACE_C
			zio_t *zio = head->zio;
			if (unlikely(zio->span != NULL)) {
				mtr_span s = mtr_create_child_span_enter("read zio submit", zio->span);
				mtr_destroy_span(s);
			}
#endif
			prep_task(head, ptr);
			head = head->next;
			VERIFY3S(TEMP_FAILURE_RETRY(syscall(__NR_io_submit,
			    ctx->io_ctx, 1, &ptr)), ==, 1);
		}
	}

	return (NULL);
}

static void
zio_task_reaper(void *args)
{
#define	MAX_PROCESSED_EVENTS 64
	io_workers_ctx_t *ctx = args;
	struct io_event events[MAX_PROCESSED_EVENTS];
	struct timespec interval = {1, 0};

	while (likely(!ctx->stop)) {
		int nevents = syscall(__NR_io_getevents, ctx->io_ctx,
		    1, MAX_PROCESSED_EVENTS, events, &interval);
		for (int i = 0; i < nevents; ++i) {
			zio_task_t *task = (zio_task_t *)events[i].data;

			zio_t *zio = task->zio;
			if (zio->io_type == ZIO_TYPE_READ) {
				abd_return_buf_copy(zio->io_abd,
				    task->buf, zio->io_size);
			} else if (zio->io_type == ZIO_TYPE_WRITE) {
				abd_return_buf(zio->io_abd,
				    task->buf, zio->io_size);
			}

#ifdef ENABLE_MINITRACE_C
			if (unlikely(zio->span != NULL)) {
				mtr_span s = mtr_create_child_span_enter("read zio interrupt", zio->span);
				mtr_destroy_span(s);
			}
#endif
			ssize_t res = events[i].res;
			if (res < 0) {
				zio->io_error = -res;
			} else if (res < zio->io_size) {
				zio->io_error = SET_ERROR(ENOSPC);
			}

			umem_free(task, sizeof (zio_task_t));

			zio_interrupt(zio);
		}
	}
}

void
vdev_aio_file_init(void)
{
	memset(&workers_ctx, 0, sizeof (workers_ctx));
	VERIFY0(syscall(__NR_io_setup, MAX_AIO_EVENTS, &workers_ctx.io_ctx));
	workers_ctx.stop = B_FALSE;
	workers_ctx.head = NULL;

	VERIFY0(sem_init(&workers_ctx.submit_sem, 0, 0));

	pthread_create(&workers_ctx.submitter, NULL,
	    zio_task_submitter, &workers_ctx);
	pthread_setname_np((pthread_t)workers_ctx.submitter,
	    "zio_task_submitter");

	workers_ctx.reaper = thread_create(NULL, 0, zio_task_reaper,
	    &workers_ctx, 0, NULL, TS_RUN | TS_JOINABLE | TS_NEW_RUNTIME,
	    defclsyspri);
}

void
vdev_aio_file_fini(void)
{
	workers_ctx.stop = B_TRUE;
	thread_join(workers_ctx.reaper);
	VERIFY0(sem_post(&workers_ctx.submit_sem));
	pthread_join(workers_ctx.submitter, NULL);
	VERIFY0(syscall(__NR_io_destroy, workers_ctx.io_ctx));
	sem_destroy(&workers_ctx.submit_sem);
}
