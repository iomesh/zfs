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
 * Copyright (c) 2005, 2010, Oracle and/or its affiliates. All rights reserved.
 * Copyright (c) 2011, 2020 by Delphix. All rights reserved.
 */

#include "atomic.h"
#include "sys/fs/zfs.h"
#include "umem.h"
#include <bits/types/struct_iovec.h>
#include <bits/types/struct_timeval.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <sys/time.h>
#include <sys/vdev_file.h>
#include <sys/vdev_impl.h>
#include <sys/zio.h>
#include <sys/abd.h>
#include <liburing.h>
#include <unistd.h>
#ifdef _KERNEL
#include <linux/falloc.h>
#endif

/*
 * By default, the logical/physical ashift for file vdevs is set to
 * SPA_MINBLOCKSHIFT (9). This allows all file vdevs to use 512B (1 << 9)
 * blocksizes. Users may opt to change one or both of these for testing
 * or performance reasons. Care should be taken as these values will
 * impact the vdev_ashift setting which can only be set at vdev creation
 * time.
 */
unsigned long vdev_file_logical_ashift = 12;
unsigned long vdev_file_physical_ashift = 12;

#define	URING_SIZE 4096

typedef enum task_stat {
	INIT = 0,
	TAIL = 1,
	BODY = 2,
} task_stat_t;

#define NUM_IOVECS	64
typedef struct zio_task {
	zio_t *zio;
	uint32_t state;
	struct iovec *iovecs;
	int n_vecs;
	struct timeval create;
	struct timeval submit;
	struct zio_task *next;
} zio_task_t;

typedef struct uring_reaper_ctx {
	struct io_uring ring;

	kmutex_t mutex;
	zio_task_t *tail;

	// for thread control
	kthread_t *reaper_thread;
	boolean_t stop;
} uring_reaper_ctx_t;

static uring_reaper_ctx_t reaper_ctx;

static void
vdev_file_hold(vdev_t *vd)
{
	ASSERT(vd->vdev_path != NULL);
}

static void
vdev_file_rele(vdev_t *vd)
{
	ASSERT(vd->vdev_path != NULL);
}

static mode_t
vdev_file_open_mode(spa_mode_t spa_mode)
{
	mode_t mode = 0;

	if ((spa_mode & SPA_MODE_READ) && (spa_mode & SPA_MODE_WRITE)) {
		mode = O_RDWR;
	} else if (spa_mode & SPA_MODE_READ) {
		mode = O_RDONLY;
	} else if (spa_mode & SPA_MODE_WRITE) {
		mode = O_WRONLY;
	}

	return (mode | O_LARGEFILE);
}

static int
vdev_file_open(vdev_t *vd, uint64_t *psize, uint64_t *max_psize,
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

	// TODO(sundengyu): register this fd
	vf->vf_fd = open(vd->vdev_path,
	    vdev_file_open_mode(spa_mode(vd->vdev_spa)));
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

	printf("disk capacity: %luGB\n", zfa.st_size >> 30);
	*max_psize = *psize = zfa.st_size;
	*logical_ashift = vdev_file_logical_ashift;
	*physical_ashift = vdev_file_physical_ashift;

	return (0);
}

static void
vdev_file_close(vdev_t *vd)
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

static boolean_t
insert_task(zio_task_t *task, zio_task_t **list_tail)
{
	zio_task_t *tail = atomic_load_ptr(list_tail);
	while (1) {
		// tail is null, try to be the leader
		if (tail == NULL) {
			tail = atomic_cas_ptr(list_tail, NULL, task);
			if (tail == NULL) {
				break;
			}
		}

		zio_task_t *next = atomic_cas_ptr(&tail->next, NULL, task);
		if (next == NULL) {
			atomic_cas_ptr(list_tail, tail, task);
			if (atomic_cas_32(&tail->state, INIT, BODY) == INIT) {
				// joined the last group, just return
				return B_TRUE;
			}
			break;
		}

		zio_task_t *cur_tail = atomic_cas_ptr(list_tail, tail, next);
		if (tail == cur_tail) {
			tail = next;
		} else {
			tail = cur_tail;
		}
	}

	return B_FALSE;
}

static struct io_uring_sqe *
get_uring_sqe_no_fail(struct io_uring *ring)
{
	struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
	if (unlikely(sqe == NULL)) {
		// TODO(sundengyu): deal with submit error
		VERIFY0(io_uring_submit(ring));
		sqe = io_uring_get_sqe(ring);
		VERIFY3P(sqe, !=, NULL);
	}

	return (sqe);
}

static void
prep_task(zio_task_t *task, struct io_uring_sqe *sqe)
{
	zio_t *zio = task->zio;
	int fd = ((vdev_file_t *)zio->io_vd->vdev_tsd)->vf_fd;
	switch (zio->io_type) {
	case ZIO_TYPE_IOCTL:
		VERIFY3U(zio->io_cmd, ==, DKIOCFLUSHWRITECACHE);
		io_uring_prep_fsync(sqe, fd, O_SYNC | O_DSYNC);
		break;
	case ZIO_TYPE_READ:
		task->n_vecs = abd_prep_iovecs(zio->io_abd, zio->io_size, &task->iovecs);
		io_uring_prep_readv(sqe, fd, task->iovecs, task->n_vecs, zio->io_offset);
		break;
	case ZIO_TYPE_WRITE:
		task->n_vecs = abd_prep_iovecs(zio->io_abd, zio->io_size, &task->iovecs);
		io_uring_prep_writev(sqe, fd, task->iovecs, task->n_vecs, zio->io_offset);
		break;
	default:
		panic("zio type not expected: %d", zio->io_type);
		break;
	}
}

static uint64_t
micros_elapsed(struct timeval *before, struct timeval *after)
{
	return (after->tv_sec - before->tv_sec) * 1000000 +
	    after->tv_usec - before->tv_usec;
}

static void
submit_tasks(zio_task_t *task, struct io_uring *ring)
{
	while (1) {
		struct io_uring_sqe *sqe = get_uring_sqe_no_fail(ring);
		zio_t *zio = task->zio;
		if (unlikely(zio == NULL)) {
			io_uring_prep_nop(sqe);
		} else {
			prep_task(task, sqe);
		}

		io_uring_sqe_set_data(sqe, task);

		gettimeofday(&task->submit, NULL);
		io_uring_submit(ring);

		if (atomic_cas_32(&task->state, INIT, TAIL) == INIT) {
			break;
		}

		task = atomic_load_ptr(&task->next);
	}
}

static void
submit_zio_task(zio_t *zio)
{
	zio_task_t *task = umem_alloc(sizeof (zio_task_t), UMEM_NOFAIL);
	task->zio = zio;
	task->state = INIT;
	task->next = NULL;
	gettimeofday(&task->create, NULL);

	if (insert_task(task, &reaper_ctx.tail)) {
		return;
	}

	submit_tasks(task, &reaper_ctx.ring);
}

static void
vdev_file_io_start(zio_t *zio)
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

			/*
			 * We cannot safely call vfs_fsync() when PF_FSTRANS
			 * is set in the current context.  Filesystems like
			 * XFS include sanity checks to verify it is not
			 * already set, see xfs_vm_writepage().  Therefore
			 * the sync must be dispatched to a different context.
			 */
			if (__spl_pf_fstrans_check()) {
				submit_zio_task(zio);
				return;
			}

			if (fsync(vf->vf_fd) < 0) {
				zio->io_error = errno;
			}
			break;
		default:
			zio->io_error = SET_ERROR(ENOTSUP);
		}

		zio_execute(zio);
		return;
	} else if (zio->io_type == ZIO_TYPE_TRIM) {
		int mode = 0;

		ASSERT3U(zio->io_size, !=, 0);
#ifdef __linux__
		mode = FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE;
#endif
		zio->io_error = fallocate(vf->vf_fd, mode,
		    zio->io_offset, zio->io_size);
		zio_execute(zio);
		return;
	}

	zio->io_target_timestamp = zio_handle_io_delay(zio);

	submit_zio_task(zio);
}

static void
vdev_file_io_done(zio_t *zio)
{
	(void) zio;
}

vdev_ops_t vdev_file_ops = {
	.vdev_op_init = NULL,
	.vdev_op_fini = NULL,
	.vdev_op_open = vdev_file_open,
	.vdev_op_close = vdev_file_close,
	.vdev_op_asize = vdev_default_asize,
	.vdev_op_min_asize = vdev_default_min_asize,
	.vdev_op_min_alloc = NULL,
	.vdev_op_io_start = vdev_file_io_start,
	.vdev_op_io_done = vdev_file_io_done,
	.vdev_op_state_change = NULL,
	.vdev_op_need_resilver = NULL,
	.vdev_op_hold = vdev_file_hold,
	.vdev_op_rele = vdev_file_rele,
	.vdev_op_remap = NULL,
	.vdev_op_xlate = vdev_default_xlate,
	.vdev_op_rebuild_asize = NULL,
	.vdev_op_metaslab_init = NULL,
	.vdev_op_config_generate = NULL,
	.vdev_op_nparity = NULL,
	.vdev_op_ndisks = NULL,
	.vdev_op_type = VDEV_TYPE_FILE,		/* name of this vdev type */
	.vdev_op_leaf = B_TRUE			/* leaf vdev */
};

static void
zio_task_reaper(void *args)
{
	uring_reaper_ctx_t *ctx = args;
	struct io_uring_cqe *cqe;
	int err = 0;
	uint64_t total_tasks_r = 0;
	uint64_t total_tasks_w = 0;
	uint64_t submit_latency_r = 0;
	uint64_t submit_latency_w = 0;
	uint64_t completion_latency_r = 0;
	uint64_t completion_latency_w = 0;
	while ((err = io_uring_wait_cqe(&ctx->ring, &cqe)) == 0) {
		zio_task_t *task = io_uring_cqe_get_data(cqe);
		zio_t *zio = task->zio;
		struct timeval now;
		if (zio == NULL) {
			printf("READ: avg submit latency: %luus, avg completion latency: %luus\n",
			    submit_latency_r / total_tasks_r, completion_latency_r / total_tasks_r);
			printf("WRITE: avg submit latency: %luus, avg completion latency: %luus\n",
			    submit_latency_w / total_tasks_w, completion_latency_w / total_tasks_w);
			return;
		}

		io_uring_cqe_seen(&ctx->ring, cqe);

		if (cqe->res < 0) {
			zio->io_error = cqe->res;
		} else {
			zio->io_error = 0;
		}

		gettimeofday(&now, NULL);
		if (zio->io_type == ZIO_TYPE_READ) {
			total_tasks_r++;
			submit_latency_r += micros_elapsed(&task->create, &task->submit);
			completion_latency_r += micros_elapsed(&task->submit, &now);
			zio->io_done_ts = gethrtime();
			VERIFY(zio->ctime);
			VERIFY(zio->io_start_ts);
			abd_free_iovecs(task->iovecs, task->n_vecs);
			// int trace = zio->io_pipeline_trace;
			// int stage = 0;
			// while (trace > 0) {
			// 	if (trace & 1) {
			// 		printf("stage%d ", stage);
			// 	}
			// 	stage++;
			// 	trace >>= 1;
			// }
			// printf("\n");
		} else if (zio->io_type == ZIO_TYPE_WRITE) {
			total_tasks_w++;
			submit_latency_w += micros_elapsed(&task->create, &task->submit);
			completion_latency_w += micros_elapsed(&task->submit, &now);
			abd_free_iovecs(task->iovecs, task->n_vecs);
		}

		zio_delay_interrupt(zio);
	}

	VERIFY0(-err);
}

void
vdev_file_init(void)
{
	mutex_init(&reaper_ctx.mutex, NULL, MUTEX_DEFAULT, NULL);
	reaper_ctx.tail = NULL;

	VERIFY0(io_uring_queue_init(URING_SIZE, &reaper_ctx.ring, IORING_SETUP_SQPOLL));

	reaper_ctx.stop = B_FALSE;
	reaper_ctx.reaper_thread = thread_create(NULL, 0, zio_task_reaper,
	    &reaper_ctx, 0, NULL, TS_RUN | TS_JOINABLE, defclsyspri);
	pthread_setname_np((pthread_t)reaper_ctx.reaper_thread, "io-reaper");
}

void
vdev_file_fini(void)
{
	submit_zio_task(NULL);
	thread_join(reaper_ctx.reaper_thread);

	// destroy the uring
	io_uring_queue_exit(&reaper_ctx.ring);

	mutex_destroy(&reaper_ctx.mutex);
}

/*
 * From userland we access disks just like files.
 */
#ifndef _KERNEL

vdev_ops_t vdev_disk_ops = {
	.vdev_op_init = NULL,
	.vdev_op_fini = NULL,
	.vdev_op_open = vdev_file_open,
	.vdev_op_close = vdev_file_close,
	.vdev_op_asize = vdev_default_asize,
	.vdev_op_min_asize = vdev_default_min_asize,
	.vdev_op_min_alloc = NULL,
	.vdev_op_io_start = vdev_file_io_start,
	.vdev_op_io_done = vdev_file_io_done,
	.vdev_op_state_change = NULL,
	.vdev_op_need_resilver = NULL,
	.vdev_op_hold = vdev_file_hold,
	.vdev_op_rele = vdev_file_rele,
	.vdev_op_remap = NULL,
	.vdev_op_xlate = vdev_default_xlate,
	.vdev_op_rebuild_asize = NULL,
	.vdev_op_metaslab_init = NULL,
	.vdev_op_config_generate = NULL,
	.vdev_op_nparity = NULL,
	.vdev_op_ndisks = NULL,
	.vdev_op_type = VDEV_TYPE_DISK,		/* name of this vdev type */
	.vdev_op_leaf = B_TRUE			/* leaf vdev */
};

#endif

ZFS_MODULE_PARAM(zfs_vdev_file, vdev_file_, logical_ashift, ULONG, ZMOD_RW,
	"Logical ashift for file-based devices");
ZFS_MODULE_PARAM(zfs_vdev_file, vdev_file_, physical_ashift, ULONG, ZMOD_RW,
	"Physical ashift for file-based devices");
