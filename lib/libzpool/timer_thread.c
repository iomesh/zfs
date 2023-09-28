#include <pthread.h>
#include <timer_thread.h>
#include "sys/avl.h"
#include "sys/stdtypes.h"
#include "sys/time.h"
#include "umem.h"
#include "atomic.h"
#include <stddef.h>

static inline int
timespec_compare(const struct timespec *lhs, const struct timespec *rhs)
{
	if (lhs->tv_sec < rhs->tv_sec)
		return (-1);
	if (lhs->tv_sec > rhs->tv_sec)
		return (1);
	if (lhs->tv_nsec == rhs->tv_sec)
		return (0);
	return (lhs->tv_nsec > rhs->tv_nsec ? 1 : -1);
}

static inline int
timer_task_compare(const void *v1, const void *v2)
{
	const timer_task_t *lhs = v1;
	const timer_task_t *rhs = v2;

	return (timespec_compare(&lhs->expire_time, &rhs->expire_time));
}

static void *
timer_thread_func(void *arg)
{
	timer_thread_t *timer_thread = arg;
	while (!atomic_load_32(&timer_thread->stop)) {
		// consume pending tasks
		for (int i = 0; i < PENDING_BUCKETS; ++i) {
			VERIFY0(pthread_mutex_lock(
			    &timer_thread->list_locks[i]));
			timer_task_t *head =
			    timer_thread->pending_list_heads[i];
			timer_thread->pending_list_heads[i] = NULL;
			pthread_mutex_unlock(&timer_thread->list_locks[i]);

			while (head != NULL) {
				avl_index_t where;
				timer_task_t *expire_head = avl_find(
				    &timer_thread->tasks, head,
				    &where);
				if (expire_head != NULL) {
					VERIFY0(timer_task_compare(
					    expire_head, head));
					head->next_expire =
					    expire_head->next_expire;
					expire_head->next_expire = head;
				} else {
					head->next_expire = NULL;
					avl_insert(&timer_thread->tasks,
					    head, where);
				}
				head = head->next_pending;
			}
		}

		// execute expired timer task
		struct timespec now;
		VERIFY0(clock_gettime(CLOCK_REALTIME, &now));
		timer_task_t *task = avl_first(&timer_thread->tasks);
		struct timespec *next_wake_up = NULL;
		while (task != NULL) {
			if (timespec_compare(&now, &task->expire_time) < 0) {
				next_wake_up = &task->expire_time;
				break;
			}

			timer_task_t *next = AVL_NEXT(&timer_thread->tasks,
			    task);

			avl_remove(&timer_thread->tasks, task);

			timer_task_t *cur = task;
			while (cur != NULL) {
				if (atomic_cas_32(&cur->state,
				    INIT, RUNING) == INIT) {
					cur->func(cur->arg);
				}
				timer_task_t *next = cur->next_expire;
				dec_timer_task_ref(cur);
				cur = next;
			}

			task = next;
		}

		if (next_wake_up == NULL) {
			sem_wait(&timer_thread->notify);
		} else {
			sem_timedwait(&timer_thread->notify, next_wake_up);
		}
	}
	return (NULL);
}

void
timer_thread_init(timer_thread_t *thread)
{
	VERIFY0(sem_init(&thread->notify, 0, 0));
	thread->stop = B_FALSE;
	avl_create(&thread->tasks, timer_task_compare,
	    sizeof (timer_task_t), offsetof(timer_task_t, avl_node));
	for (int i = 0; i < PENDING_BUCKETS; ++i) {
		VERIFY0(pthread_mutex_init(&thread->list_locks[i], NULL));
		thread->pending_list_heads[i] = NULL;
	}
	VERIFY0(pthread_create(&thread->tid, NULL, timer_thread_func, thread));
}

void
timer_thread_fini(timer_thread_t *thread)
{
	atomic_store_32(&thread->stop, B_TRUE);
	VERIFY0(sem_post(&thread->notify));
	VERIFY0(pthread_join(thread->tid, NULL));
}

// TODO(sundengyu): return immediataly if expire_time alredy passed
timer_task_t *
timer_thread_schedule(timer_thread_t *thread, void (*func) (void *),
    void *arg, const struct timespec *expire_time)
{
	if (atomic_load_32(&thread->stop) == B_TRUE) {
		return (NULL);
	}

	timer_task_t *task = umem_alloc(sizeof (timer_task_t), UMEM_NOFAIL);
	task->state = INIT;
	task->func = func;
	task->arg = arg;
	task->expire_time.tv_nsec = expire_time->tv_nsec;
	task->expire_time.tv_sec = expire_time->tv_sec;
	// 2 refcounts are held by timer_thread and the caller
	task->refcount = 2;

	int bucket = pthread_self() % PENDING_BUCKETS;
	VERIFY0(pthread_mutex_lock(&thread->list_locks[bucket]));
	task->next_pending = thread->pending_list_heads[bucket];
	boolean_t earlier = task->next_pending == NULL;
	thread->pending_list_heads[bucket] = task;
	VERIFY0(pthread_mutex_unlock(&thread->list_locks[bucket]));

	if (earlier) {
		VERIFY0(sem_post(&thread->notify));
	}

	return (task);
}
