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
 * Copyright 2010 Sun Microsystems, Inc.  All rights reserved.
 * Use is subject to license terms.
 */
/*
 * Copyright 2011 Nexenta Systems, Inc. All rights reserved.
 * Copyright 2012 Garrett D'Amore <garrett@damore.org>.  All rights reserved.
 * Copyright (c) 2014 by Delphix. All rights reserved.
 */

#include "atomic.h"
#include "sync_ops.h"
#include "sys/avl.h"
#include "sys/kmem.h"
#include "sys/list.h"
#include "sys/stdtypes.h"
#include "sys/time.h"
#include "sys/zfs_debug.h"
#include <stdint.h>
#include <stdlib.h>
#include <strings.h>
#include <sys/zfs_context.h>
#include <time.h>

taskq_t *system_taskq;
taskq_t *system_delay_taskq;

taskq_ops_t taskq_ops;

taskq_t	*
taskq_create(const char *tq_name, int nthreads,
    pri_t _c, int _d, int _e, uint_t flags)
{
	if (flags & TASKQ_THREADS_CPU_PCT) {
		int pct;
		ASSERT3S(nthreads, >=, 0);
		ASSERT3S(nthreads, <=, 100);
		pct = MIN(nthreads, 100);
		pct = MAX(pct, 0);

		nthreads = (sysconf(_SC_NPROCESSORS_ONLN) * pct * 2) / 100;
		nthreads = MAX(nthreads, 1);	/* need at least 1 thread */
	}
	return (taskq_ops.taskq_create(tq_name, nthreads));
}

taskqid_t
taskq_dispatch(taskq_t *tq, task_func_t func, void *arg, uint_t flag)
{
	return (taskq_ops.taskq_dispatch(tq, func, arg, flag));
}

taskqid_t
taskq_dispatch_delay(taskq_t *tq, task_func_t func, void *arg, uint_t _,
    clock_t expire_time)
{
	clock_t delta = expire_time - ddi_get_lbolt();
	if (delta < 0) {
		delta = 0;
	}
	struct timespec ts;
	ts.tv_sec = delta / hz;
	ts.tv_nsec = delta % hz * NANOSEC / hz;
	return (taskq_ops.taskq_delay_dispatch(tq, func, arg, &ts));
}

static void
taskq_ent_func(void *arg)
{
	taskq_ent_t *task = arg;
	task_func_t *func = task->func;
	arg = task->arg;
	task->func = NULL;
	task->arg = NULL;
	func(arg);
}

void
taskq_dispatch_ent(taskq_t *tq, task_func_t func, void *arg, uint_t flag,
    taskq_ent_t *task)
{
	ASSERT(taskq_empty_ent(task));
	task->func = func;
	task->arg = arg;
	taskq_ops.taskq_dispatch(tq, taskq_ent_func, task, flag);
}

int
taskq_empty_ent(taskq_ent_t *task)
{
	return (task->func == NULL && task->arg == NULL);
}

void
taskq_init_ent(taskq_ent_t *task)
{
	task->func = NULL;
	task->arg = NULL;
}

void
taskq_destroy(taskq_t *tq)
{
	taskq_ops.taskq_destroy(tq);
}

void
taskq_wait(taskq_t *tq)
{
	taskq_ops.taskq_wait(tq);
}

void
taskq_wait_id(taskq_t *tq, taskqid_t id)
{
	taskq_ops.taskq_wait_id(tq, id);
}

void
taskq_wait_outstanding(taskq_t *tq, taskqid_t _)
{
	taskq_ops.taskq_wait(tq);
}

int
taskq_member(taskq_t *tq, kthread_t *id)
{
	return (taskq_ops.taskq_member(tq, (uint64_t)id));
}

taskq_t	*
taskq_of_curthread(void)
{
	return (taskq_ops.taskq_of_curthread());
}

int
taskq_cancel_id(taskq_t *tq, taskqid_t id)
{
	if (id == TASKQID_INVALID) {
		return (ENOENT);
	}

	return (taskq_ops.taskq_cancel_id(tq, id));
}

void
system_taskq_init(void)
{
	system_taskq = taskq_create("system_taskq", 64, maxclsyspri, 4, 512,
	    TASKQ_DYNAMIC | TASKQ_PREPOPULATE);
	system_delay_taskq = taskq_create("delay_taskq", 4, maxclsyspri, 4,
	    512, TASKQ_DYNAMIC | TASKQ_PREPOPULATE);
}

void
system_taskq_fini(void)
{
	taskq_destroy(system_taskq);
	system_taskq = NULL; /* defensive */
	taskq_destroy(system_delay_taskq);
	system_delay_taskq = NULL;
	tsd_destroy(&taskq_tsd);
}
