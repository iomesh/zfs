#ifndef _TIMER_THREAD_H
#define	_TIMER_THREAD_H

#include <bits/pthreadtypes.h>
#include <bits/types/struct_timespec.h>
#include <syscall.h>
#include <linux/futex.h>
#include <unistd.h>
#include "sys/avl.h"
#include "sys/stdtypes.h"
#include "sys/time.h"
#include <atomic.h>
#include "umem.h"
#include <semaphore.h>

typedef enum timer_task_state {
	INIT,
	RUNING,
	ABORTED,
} timer_task_state_t;

typedef struct timer_task {
	timer_task_state_t state;
	avl_node_t avl_node;
	void (*func) (void *);
	void *arg;
	struct timespec expire_time;
	uint32_t refcount;
	struct timer_task *next_pending;
	struct timer_task *next_expire;
} timer_task_t;

#define	PENDING_BUCKETS	31
typedef struct timer_thread {
	sem_t notify;
	boolean_t stop;
	avl_tree_t tasks;
	pthread_mutex_t list_locks[PENDING_BUCKETS];
	timer_task_t *pending_list_heads[PENDING_BUCKETS];
	pthread_t tid;
} timer_thread_t;

extern void timer_thread_init(timer_thread_t *thread);
// make sure all tasks are done before calling this func
extern void timer_thread_fini(timer_thread_t *thread);
extern timer_task_t *timer_thread_schedule(timer_thread_t *thread,
    void (*func) (void *), void *arg, const struct timespec *expire_time);

static inline boolean_t
timer_thread_unschedule(timer_task_t *task)
{
	int state = atomic_cas_32(&task->state, INIT, ABORTED);
	return (state == INIT || state == ABORTED);
}

static inline void
dec_timer_task_ref(timer_task_t *task)
{
	if (atomic_dec_32_nv(&task->refcount) == 0) {
		umem_free(task, sizeof (timer_task_t));
	}
}

#endif