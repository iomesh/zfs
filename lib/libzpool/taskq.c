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

taskq_t *system_taskq;
taskq_t *system_delay_taskq;

static uint32_t taskq_tsd;

#define	TASKQ_ACTIVE	0x00010000

static void taskq_thread(void *arg);

static taskq_ent_t *
search_task_by_id(list_t *task_list, taskqid_t id)
{
	for (taskq_ent_t *t = list_head(task_list);
	    t != NULL; t = list_next(task_list, t)) {
		if (t->id == id) {
			return (t);
		}
	}

	return (NULL);
}

static taskq_ent_t *
task_alloc(taskq_t *tq, boolean_t wait)
{
	ASSERT(MUTEX_HELD(&tq->tq_lock));
	taskq_ent_t *task = NULL;

	for (;;) {
		task = list_remove_head(&tq->free_list);
		if (task != NULL) {
			break;
		}

		if (tq->tq_nalloc < tq->tq_maxalloc) {
			task = kmem_zalloc(sizeof (taskq_ent_t), KM_SLEEP);
			++tq->tq_nalloc;
			break;
		}

		if (!wait) {
			break;
		}

		cv_wait(&tq->tq_maxwait, &tq->tq_lock);
	}

	return (task);
}

static void
task_free(taskq_t *tq, taskq_ent_t *task)
{
	ASSERT(MUTEX_HELD(&tq->tq_lock));
	if (tq->tq_nalloc <= tq->tq_minalloc) {
		list_insert_head(&tq->free_list, task);
	} else {
		kmem_free(task, sizeof (taskq_ent_t));
		--tq->tq_nalloc;
	}

	cv_broadcast(&tq->tq_maxwait);
}

static inline void
taskq_dispatch_ent_locked(taskq_t *tq, uint_t flags, taskq_ent_t *task)
{
	ASSERT(MUTEX_HELD(&tq->tq_lock));
	if (flags & (TQ_FRONT | TQ_NOQUEUE)) {
		list_insert_head(&tq->prio_list, task);
	} else {
		list_insert_head(&tq->pend_list, task);
	}

	if (tq->tq_nthreads < tq->tq_maxthreads) {
		taskq_thread_t *tq_thread = kmem_alloc(
		    sizeof (taskq_thread_t), KM_SLEEP);
		tq_thread->tq = tq;
		++tq->tq_nthreads;
		list_insert_head(&tq->tq_threadlist, tq_thread);
		kthread_t *id = thread_create(NULL, 0,
		    taskq_thread, tq_thread, 0, NULL, 0, 0);
		tq_thread->id = id;
	}
}

taskqid_t
taskq_dispatch(taskq_t *tq, task_func_t func, void *arg, uint_t tqflags)
{
	mutex_enter(&tq->tq_lock);
	ASSERT(tq->active);
	boolean_t wait = !(tqflags & TQ_NOSLEEP);
	taskq_ent_t *task = task_alloc(tq, wait);
	if (task == NULL) {
		mutex_exit(&tq->tq_lock);
		return (0);
	}

	task->tqent_func = func;
	task->tqent_arg = arg;
	taskqid_t id = ++tq->tq_id_next;
	task->id = id;
	list_insert_head(&tq->task_list, task);
	taskq_dispatch_ent_locked(tq, tqflags, task);
	mutex_exit(&tq->tq_lock);

	return (id);
}

static void
delay_dispatch_thread(void *arg)
{
	delayed_task_args_t *args = arg;
	taskq_t *tq = args->tq;

	mutex_enter(&tq->tq_lock);
	while (args->task != NULL) {
		if (cv_timedwait(&args->notify, &tq->tq_lock,
		    args->expire_time) < 0) {
			break;
		}
	}

	if (args->task != NULL) {
		list_remove(&tq->delay_list, args->task);
		taskq_dispatch_ent_locked(tq,
		    TQ_NOQUEUE, args->task);
	}
	--tq->tq_ndelay_threads;
	cv_broadcast(&tq->tq_completion);
	mutex_exit(&tq->tq_lock);

	cv_destroy(&args->notify);
	kmem_free(args, sizeof (delayed_task_args_t));
}

taskqid_t
taskq_dispatch_delay(taskq_t *tq, task_func_t func, void *arg, uint_t tqflags,
    clock_t expire_time)
{
	delayed_task_args_t *delay_args = kmem_zalloc(
	    sizeof (delayed_task_args_t), KM_SLEEP);
	boolean_t wait = !(tqflags & TQ_NOSLEEP);
	mutex_enter(&tq->tq_lock);
	ASSERT(tq->active);
	taskq_ent_t *task = task_alloc(tq, wait);
	if (task == NULL) {
		mutex_exit(&tq->tq_lock);
		return (0);
	}

	taskqid_t id = ++tq->tq_id_next;
	task->id = id;
	task->tqent_func = func;
	task->tqent_arg = arg;
	task->delay_args = delay_args;
	delay_args->task = task;
	delay_args->tq = tq;
	delay_args->expire_time = expire_time;
	++tq->tq_ndelay_threads;
	cv_init(&delay_args->notify, NULL, CV_DEFAULT, NULL);

	list_insert_head(&tq->task_list, task);
	list_insert_head(&tq->delay_list, task);
	mutex_exit(&tq->tq_lock);

	(void) thread_create(NULL, 0, delay_dispatch_thread,
	    delay_args, 0, NULL, 0, 0);
	return (id);
}

int
taskq_empty_ent(taskq_ent_t *task)
{
	return (task->node.next == NULL && task->node.prev == NULL);
}

void
taskq_init_ent(taskq_ent_t *task)
{
	bzero(task, sizeof (taskq_ent_t));
	task->preallocated = B_TRUE;
}

void
taskq_dispatch_ent(taskq_t *tq, task_func_t func, void *arg, uint_t flags,
    taskq_ent_t *task)
{
	/*
	 * Enqueue the task to the underlying queue.
	 */
	mutex_enter(&tq->tq_lock);
	task->tqent_func = func;
	task->tqent_arg = arg;
	task->id = ++tq->tq_id_next;

	taskq_dispatch_ent_locked(tq, flags, task);
	mutex_exit(&tq->tq_lock);
}

void
taskq_wait(taskq_t *tq)
{
	mutex_enter(&tq->tq_lock);
	while (tq->tq_nthreads > 0 || tq->tq_ndelay_threads > 0) {
		cv_wait(&tq->tq_wait_cv, &tq->tq_lock);
	}
	mutex_exit(&tq->tq_lock);
}

void
taskq_wait_id(taskq_t *tq, taskqid_t id)
{
	mutex_enter(&tq->tq_lock);
	while (search_task_by_id(&tq->task_list, id) != NULL) {
		cv_wait(&tq->tq_completion, &tq->tq_lock);
	}
	mutex_exit(&tq->tq_lock);
}

void
taskq_wait_outstanding(taskq_t *tq, taskqid_t id)
{
	taskq_wait(tq);
}

static void
taskq_thread(void *arg)
{
	taskq_thread_t *tq_thread = arg;
	taskq_t *tq = tq_thread->tq;
	VERIFY0(tsd_set(taskq_tsd, tq));

	mutex_enter(&tq->tq_lock);
	for (;;) {
		taskq_ent_t *task = list_remove_head(&tq->prio_list);
		if (task == NULL) {
			if ((task = list_remove_head(&tq->pend_list)) == NULL) {
				--tq->tq_nthreads;
				list_remove(&tq->tq_threadlist, tq_thread);
				cv_broadcast(&tq->tq_wait_cv);
				mutex_exit(&tq->tq_lock);
				kmem_free(tq_thread, sizeof (taskq_thread_t));
				return;
			}
		}

		mutex_exit(&tq->tq_lock);

		boolean_t preallocated = task->preallocated;
		task->tqent_func(task->tqent_arg);

		mutex_enter(&tq->tq_lock);
		if (!preallocated) {
			list_remove(&tq->task_list, task);
			task_free(tq, task);
		}
		cv_broadcast(&tq->tq_completion);
	}
}

/*ARGSUSED*/
taskq_t *
taskq_create(const char *name, int nthreads, pri_t pri,
    int minalloc, int maxalloc, uint_t flags)
{
	taskq_t *tq = kmem_zalloc(sizeof (taskq_t), KM_SLEEP);

	if (flags & TASKQ_THREADS_CPU_PCT) {
		int pct;
		ASSERT3S(nthreads, >=, 0);
		ASSERT3S(nthreads, <=, 100);
		pct = MIN(nthreads, 100);
		pct = MAX(pct, 0);

		nthreads = (sysconf(_SC_NPROCESSORS_ONLN) * pct) / 100;
		nthreads = MAX(nthreads, 1);	/* need at least 1 thread */
	} else {
		ASSERT3S(nthreads, >=, 1);
	}

	(void) strncpy(tq->tq_name, name, TASKQ_NAMELEN);
	mutex_init(&tq->tq_lock, NULL, MUTEX_DEFAULT, NULL);
	cv_init(&tq->tq_wait_cv, NULL, CV_DEFAULT, NULL);
	tq->tq_id_next = 1;
	tq->tq_nthreads = 0;
	tq->tq_ndelay_threads = 0;
	tq->tq_maxthreads = nthreads;
	list_create(&tq->tq_threadlist, sizeof (taskq_thread_t),
	    offsetof(taskq_thread_t, node));

	tq->tq_nalloc = 0;
	tq->tq_minalloc = minalloc;
	tq->tq_maxalloc = maxalloc;
	cv_init(&tq->tq_maxwait, NULL, CV_DEFAULT, NULL);
	list_create(&tq->free_list, sizeof (taskq_ent_t),
	    offsetof(taskq_ent_t, node));

	list_create(&tq->prio_list, sizeof (taskq_ent_t),
	    offsetof(taskq_ent_t, node));
	list_create(&tq->pend_list, sizeof (taskq_ent_t),
	    offsetof(taskq_ent_t, node));
	list_create(&tq->delay_list, sizeof (taskq_ent_t),
	    offsetof(taskq_ent_t, node));
	list_create(&tq->task_list, sizeof (taskq_ent_t),
	    offsetof(taskq_ent_t, global_node));
	tq->active = B_TRUE;

	if (flags & TASKQ_PREPOPULATE) {
		for (int i = 0; i < minalloc; ++i) {
			taskq_ent_t *t = kmem_zalloc(
			    sizeof (taskq_ent_t), KM_SLEEP);
			list_insert_head(&tq->free_list, t);
		}
		tq->tq_nalloc = minalloc;
	}

	cv_init(&tq->tq_completion, NULL, CV_DEFAULT, NULL);

	return (tq);
}

void
taskq_destroy(taskq_t *tq)
{
	tq->active = B_FALSE;
	taskq_wait(tq);
	ASSERT3U(tq->tq_nthreads, ==, 0);
	ASSERT3U(tq->tq_ndelay_threads, ==, 0);

	mutex_destroy(&tq->tq_lock);
	cv_destroy(&tq->tq_wait_cv);
	cv_destroy(&tq->tq_maxwait);
	cv_destroy(&tq->tq_completion);
	kmem_free(tq, sizeof (taskq_t));
}

int
taskq_member(taskq_t *tq, kthread_t *tid)
{
	mutex_enter(&tq->tq_lock);
	for (taskq_thread_t *cur = list_head(&tq->tq_threadlist);
	    cur != NULL; cur = list_next(&tq->tq_threadlist, cur)) {
		if (cur->id == tid) {
			mutex_exit(&tq->tq_lock);
			return (1);
		}
	}
	mutex_exit(&tq->tq_lock);

	return (0);
}

taskq_t *
taskq_of_curthread(void)
{
	return (tsd_get(taskq_tsd));
}

static inline taskq_ent_t *
taskq_cancel_id_locked(taskq_t *tq, taskqid_t id)
{
	ASSERT(MUTEX_HELD(&tq->tq_lock));
	taskq_ent_t *ent = NULL;
	if ((ent = search_task_by_id(&tq->delay_list, id)) != NULL) {
		list_remove(&tq->delay_list, ent);
		cv_signal(&ent->delay_args->notify);
		ent->delay_args->task = NULL;
	} else if ((ent = search_task_by_id(&tq->pend_list, id)) != NULL) {
		list_remove(&tq->pend_list, ent);
	} else if ((ent = search_task_by_id(&tq->prio_list, id)) != NULL) {
		list_remove(&tq->prio_list, ent);
	}

	return (ent);
}

int
taskq_cancel_id(taskq_t *tq, taskqid_t id)
{
	mutex_enter(&tq->tq_lock);
	taskq_ent_t *t = taskq_cancel_id_locked(tq, id);
	if (t != NULL) {
		ASSERT3U(t->id, ==, id);
		if (!t->preallocated) {
			list_remove(&tq->task_list, t);
			task_free(tq, t);
		}
	} else {
		while (search_task_by_id(&tq->task_list, id) != NULL) {
			cv_wait(&tq->tq_completion, &tq->tq_lock);
		}
	}
	mutex_exit(&tq->tq_lock);

	return (0);
}

void
system_taskq_init(void)
{
	tsd_create(&taskq_tsd, NULL);
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
