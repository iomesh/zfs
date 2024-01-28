#include "atomic.h"
#include "coroutine_impl.h"
#include "libcontext.h"
#include "sys/list.h"
#include "sys/stdtypes.h"
#include "sys/zfs_context.h"
#include "umem.h"
#include <asm-generic/errno-base.h>
#include <asm-generic/errno.h>
#include <bits/stdint-uintn.h>
#include <bits/types/struct_timespec.h>
#include <libintl.h>
#include <pthread.h>
#include <sched.h>
#include <semaphore.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <time.h>
#include <coroutine.h>
#include <timer_thread.h>
#include <stddef.h>
#include <libuzfs.h>
#include <unistd.h>

static __thread uzfs_coroutine_t *thread_local_coroutine = NULL;
static timer_thread_t timer_thread;
static uint32_t cur_key_idx = 0;

#define	MAX_COROUTINE_POOL_SIZE	100
static __thread uzfs_coroutine_t *coroutine_pool_head = NULL;
static __thread int cur_coroutine_pool_size = 0;

#define	DEFAULT_STACK_SIZE	(1<<20)
#define	DEFAULT_GUARD_SIZE	getpagesize()

static inline void
wakeup_coroutine(uzfs_coroutine_t *coroutine)
{
	VERIFY3U(atomic_cas_32(&coroutine->co_state, COROUTINE_PENDING,
	    COROUTINE_RUNNABLE), ==, COROUTINE_PENDING);
	coroutine->wake(coroutine->wake_arg);
}

static void
cutex_init(cutex_t *cutex)
{
	cutex->value = 0;
	list_create(&cutex->waiter_list, sizeof (uzfs_coroutine_t),
	    offsetof(uzfs_coroutine_t, node));
	VERIFY0(pthread_mutex_init(&cutex->waiter_lock, NULL));
}

static void
cutex_fini(cutex_t *cutex)
{
	VERIFY0(pthread_mutex_destroy(&cutex->waiter_lock));
}

static void
erase_from_cutex_and_wakeup(void *arg)
{
	uzfs_coroutine_t *coroutine = arg;
	VERIFY0(pthread_mutex_lock(&coroutine->cutex->waiter_lock));
	ASSERT(coroutine->waiter_state == CUTEX_WAITER_NONE ||
	    coroutine->waiter_state == CUTEX_WAITER_READY);
	if (coroutine->waiter_state == CUTEX_WAITER_NONE) {
		coroutine->waiter_state = CUTEX_WAITER_TIMEOUT;
		list_remove(&coroutine->cutex->waiter_list, coroutine);
	}
	VERIFY0(pthread_mutex_unlock(&coroutine->cutex->waiter_lock));

	if (coroutine->waiter_state == CUTEX_WAITER_TIMEOUT) {
		wakeup_coroutine(coroutine);
	}
}

static int
cutex_wait(cutex_t *cutex, int expected_value, const struct timespec *abstime)
{
	ASSERT(thread_local_coroutine != NULL);

	if (atomic_load_32(&cutex->value) != expected_value) {
		return (EWOULDBLOCK);
	}

	if (abstime != NULL) {
		struct timespec now;
		clock_gettime(CLOCK_REALTIME, &now);
		if (timespec_compare(&now, abstime) > 0) {
			return (ETIMEDOUT);
		}
	}

	VERIFY0(pthread_mutex_lock(&cutex->waiter_lock));

	// the waker would call wake up before we add this coroutine
	// to the waiter list, so we need to double-check the value in the lock
	if (atomic_load_32(&cutex->value) != expected_value) {
		VERIFY0(pthread_mutex_unlock(&cutex->waiter_lock));
		return (EWOULDBLOCK);
	}

	timer_task_t *task = NULL;
	thread_local_coroutine->waiter_state = CUTEX_WAITER_NONE;
	thread_local_coroutine->cutex = cutex;
	thread_local_coroutine->co_state = COROUTINE_PENDING;
	// add current coroutine to the wait list
	// what if wake up happens after this insertion and before context swap?
	// will lost-wakeup happens?
	// for our usage, the scheduler should double-check whether the
	// coroutine is runnable before supending it to avoid lost-wakeup
	list_insert_tail(&cutex->waiter_list, thread_local_coroutine);

	if (abstime != NULL) {
		task = timer_thread_schedule(&timer_thread,
		    erase_from_cutex_and_wakeup,
		    thread_local_coroutine, abstime);
		VERIFY(task != NULL);
		thread_local_coroutine->expire_task = task;
	}
	VERIFY0(pthread_mutex_unlock(&cutex->waiter_lock));

	libuzfs_coroutine_yield();

	// the timeout task is already running, wait for that
	if (unlikely(task != NULL && !timer_thread_unschedule(task))) {
		// the timer func is quick, so use busyloop to wait
		// for the completion
		SPIN_UNTIL(atomic_load_32(&task->refcount) <= 1, 30);
	}

	if (task != NULL) {
		dec_timer_task_ref(task);
		thread_local_coroutine->expire_task = NULL;
	}

	switch (thread_local_coroutine->waiter_state) {
	case CUTEX_WAITER_NONE:
		panic("unexpected coroutine state");
		break;
	case CUTEX_WAITER_READY:
		return (0);
	case CUTEX_WAITER_TIMEOUT:
		return (ETIMEDOUT);
	}

	return (0);
}

static int
cutex_wake_one(cutex_t *cutex)
{
	VERIFY0(pthread_mutex_lock(&cutex->waiter_lock));
	uzfs_coroutine_t *front = list_remove_head(&cutex->waiter_list);
	if (front == NULL) {
		VERIFY0(pthread_mutex_unlock(&cutex->waiter_lock));
		return (0);
	}
	front->waiter_state = CUTEX_WAITER_READY;
	VERIFY0(pthread_mutex_unlock(&cutex->waiter_lock));

	if (front->expire_task != NULL) {
		timer_thread_unschedule(front->expire_task);
	}
	wakeup_coroutine(front);

	return (1);
}

// wake up the first waiter of cutex1, and move all waiters of cutex1 to cutex2
static int
cutex_requeue(cutex_t *cutex1, cutex_t *cutex2)
{
	VERIFY0(pthread_mutex_lock(&cutex1->waiter_lock));
	VERIFY0(pthread_mutex_lock(&cutex2->waiter_lock));

	uzfs_coroutine_t *front = list_remove_head(&cutex1->waiter_list);
	if (front != NULL) {
		front->waiter_state = CUTEX_WAITER_READY;
		list_move_tail(&cutex2->waiter_list, &cutex1->waiter_list);
	}
	VERIFY0(pthread_mutex_unlock(&cutex1->waiter_lock));
	VERIFY0(pthread_mutex_unlock(&cutex2->waiter_lock));

	if (front == NULL) {
		return (0);
	}

	if (front->expire_task != NULL) {
		timer_thread_unschedule(front->expire_task);
	}
	wakeup_coroutine(front);
	return (1);
}

void
libuzfs_coroutine_yield(void)
{
	thread_local_coroutine->pending = B_TRUE;
	if (unlikely(thread_local_coroutine->record_backtrace != NULL)) {
		thread_local_coroutine->record_backtrace(
		    thread_local_coroutine->task_id);
	}

	if (thread_local_coroutine->run_as_thread) {
		VERIFY0(TEMP_FAILURE_RETRY(sem_wait(
		    thread_local_coroutine->wake_arg)));
	} else {
		jump_fcontext(&thread_local_coroutine->my_ctx,
		    thread_local_coroutine->main_ctx, 0, B_TRUE);
	}
}

void
coroutine_wake_and_yield(void)
{
	ASSERT(!thread_local_coroutine->run_as_thread);
	thread_local_coroutine->wake(thread_local_coroutine->wake_arg);
	libuzfs_coroutine_yield();
}

void
libuzfs_coroutine_exit(void)
{
	ASSERT(!thread_local_coroutine->run_as_thread);
	jump_fcontext(&thread_local_coroutine->my_ctx,
	    thread_local_coroutine->main_ctx, 0, B_TRUE);
}

void *
libuzfs_current_coroutine_arg(void)
{
	return (thread_local_coroutine->wake_arg);
}

static void
task_runner(intptr_t _)
{
	uzfs_coroutine_t *coroutine = thread_local_coroutine;
	ASSERT(!coroutine->run_as_thread);
	coroutine->bottom_fpp = __builtin_frame_address(0);
	*coroutine->bottom_fpp = coroutine->saved_fp;
	coroutine->fn(coroutine->arg);
	jump_fcontext(&coroutine->my_ctx, coroutine->main_ctx, 0, B_TRUE);
}

static inline void
allocate_stack_storage(uzfs_coroutine_t *coroutine,
    int stack_size, int guard_size)
{
	const int page_size = getpagesize();
	ASSERT((stack_size & (page_size - 1)) == 0);
	ASSERT((guard_size & (page_size - 1)) == 0);
	const int memsize = stack_size + guard_size;
	void* const mem = mmap(NULL, memsize, (PROT_READ | PROT_WRITE),
	    (MAP_PRIVATE | MAP_ANONYMOUS), -1, 0);
	VERIFY3P(mem, !=, MAP_FAILED);

	VERIFY3U(((intptr_t)mem & (page_size - 1)), ==, 0);
	if (mprotect(mem, guard_size, PROT_NONE) < 0) {
		cmn_err(CE_WARN, "Failed to call mprotect during alloc "
		    "coroutine stack. mem: %p, guard_size: %d, err: %d, "
		    "try to increase /proc/sys/vm/max_map_count",
		    mem, guard_size, errno);
	}
	coroutine->guard_size = guard_size;
	coroutine->stack_size = stack_size;
	coroutine->stack_bottom = mem + memsize;
}

uzfs_coroutine_t *
libuzfs_new_coroutine(void (*fn)(void *), void *arg, uint64_t task_id,
    boolean_t foreground, void (*record_backtrace)(uint64_t))
{
	uzfs_coroutine_t *coroutine = NULL;
	if (unlikely(!foreground || coroutine_pool_head == NULL)) {
		coroutine = umem_zalloc(sizeof (uzfs_coroutine_t),
		    UMEM_NOFAIL);
		// use fixed stack size and guard to reuse stack
		allocate_stack_storage(coroutine, DEFAULT_STACK_SIZE,
		    DEFAULT_GUARD_SIZE);
	} else {
		coroutine = coroutine_pool_head;
		coroutine_pool_head = coroutine_pool_head->next_in_pool;
		--cur_coroutine_pool_size;
	}

	coroutine->co_state = COROUTINE_RUNNABLE;
	coroutine->my_ctx = make_fcontext(coroutine->stack_bottom,
	    DEFAULT_STACK_SIZE, task_runner);
	coroutine->fn = fn;
	coroutine->arg = arg;
	coroutine->pending = B_FALSE;
	coroutine->wake = NULL;
	coroutine->wake_arg = NULL;
	coroutine->record_backtrace = record_backtrace;
	coroutine->cutex = NULL;
	coroutine->task_id = task_id;
	coroutine->expire_task = NULL;
	coroutine->specific_head = NULL;
	coroutine->foreground = foreground;
	coroutine->next_in_pool = NULL;
	coroutine->bottom_fpp = NULL;
	coroutine->saved_fp = NULL;
	coroutine->run_as_thread = B_FALSE;
	return (coroutine);
}

// now that this coroutine may be accessed by multi thread,
// what if someone still want to run this coroutine after coroutine_destroy?
// this cannot happen because a coroutine will only be destroyed after the
// coroutine is completed
void
libuzfs_destroy_coroutine(uzfs_coroutine_t *coroutine)
{
	co_specific_t *cur = coroutine->specific_head;
	while (cur != NULL) {
		co_specific_t *next = cur->next;
		umem_free(cur, sizeof (co_specific_t));
		cur = next;
	}
	VERIFY3U(coroutine->co_state, ==, COROUTINE_DONE);
	if (likely(coroutine->foreground &&
	    ++cur_coroutine_pool_size < MAX_COROUTINE_POOL_SIZE)) {
		coroutine->next_in_pool = coroutine_pool_head;
		coroutine_pool_head = coroutine;
	} else {
		int memsize = coroutine->stack_size + coroutine->guard_size;
		VERIFY0(munmap(coroutine->stack_bottom - memsize, memsize));
		umem_free(coroutine, sizeof (uzfs_coroutine_t));
	}
}

static noinline void *
current_pc(void)
{
	return (__builtin_return_address(0));
}

static void
thread_coroutine_waker(void *arg)
{
	VERIFY0(sem_post(arg));
}

void
libuzfs_run_in_thread(void (*func)(void *), void *arg,
    uint64_t task_id, void (*record_backtrace)(uint64_t))
{
	sem_t sem;
	VERIFY0(sem_init(&sem, 0, 0));

	uzfs_coroutine_t coroutine;
	memset(&coroutine, 0, sizeof (coroutine));
	coroutine.run_as_thread = B_TRUE;
	coroutine.wake_arg = &sem;
	coroutine.wake = thread_coroutine_waker;
	coroutine.record_backtrace = record_backtrace;
	coroutine.task_id = task_id;

	thread_local_coroutine = &coroutine;
	func(arg);
	thread_local_coroutine = NULL;
}

boolean_t
libuzfs_run_coroutine(uzfs_coroutine_t *coroutine,
    void (*wake)(void *), void *wake_arg)
{
	ASSERT(thread_local_coroutine == NULL);

	switch (atomic_load_32(&coroutine->co_state)) {
	case COROUTINE_RUNNABLE:
		coroutine->pending = 0;
		thread_local_coroutine = coroutine;
		coroutine->wake = wake;
		coroutine->wake_arg = wake_arg;

		// when the coroutine is first run, the data where the frame
		// pointer to is the task_runner, so we cannot overwrite that
		// data. after the first run, this address are used as frame
		// pointer so we can just set the address
		if (coroutine->bottom_fpp) {
			*coroutine->bottom_fpp = __builtin_frame_address(0);
		} else {
			coroutine->saved_fp = __builtin_frame_address(0);
		}
		// TODO(sundengyu): use one instruction other that a function
		// call this implementation may not be compatible with arm arch
		*((void **)coroutine->stack_bottom - 1) = current_pc();

		jump_fcontext(&coroutine->main_ctx,
		    coroutine->my_ctx, 0, B_TRUE);
		thread_local_coroutine = NULL;
		// if pending, state will be set already
		if (!coroutine->pending) {
			coroutine->co_state = COROUTINE_DONE;
		}
		break;
	case COROUTINE_PENDING:
		return (B_TRUE);
	case COROUTINE_DONE:
		return (B_FALSE);
	}

	return (coroutine->pending);
}

void
coroutine_key_create(uint32_t *idx)
{
	*idx = atomic_inc_32_nv(&cur_key_idx);
	ASSERT(*idx < MAX_LOCAL);
}

void*
coroutine_getkey(uint32_t idx)
{
	co_specific_t *cur = thread_local_coroutine->specific_head;
	while (cur != NULL) {
		if (cur->key == idx) {
			return (cur->value);
		}
		cur = cur->next;
	}

	return (NULL);
}

int
coroutine_setkey(uint32_t idx, void *local)
{
	co_specific_t *cur = thread_local_coroutine->specific_head;
	while (cur != NULL) {
		if (cur->key == idx) {
			cur->value = local;
			return (0);
		}

		cur = cur->next;
	}

	co_specific_t *new = umem_alloc(sizeof (co_specific_t), UMEM_NOFAIL);
	new->next = thread_local_coroutine->specific_head;
	new->key = idx;
	new->value = local;
	thread_local_coroutine->specific_head = new;
	return (0);
}

uint64_t
uzfs_coroutine_self(void)
{
	ASSERT(thread_local_coroutine != NULL);
	return (thread_local_coroutine->task_id);
}

void
coroutine_init(void)
{
	timer_thread_init(&timer_thread);
}

void
coroutine_destroy(void)
{
	timer_thread_fini(&timer_thread);
}

static void
timed_wakeup(void *arg)
{
	uzfs_coroutine_t *coroutine = arg;
	wakeup_coroutine(coroutine);
}

void
coroutine_sleep(const struct timespec *sleep_time)
{
	if (sleep_time->tv_nsec < 0 || sleep_time->tv_sec < 0 ||
	    (sleep_time->tv_nsec == 0 && sleep_time->tv_sec == 0)) {
		return;
	}

	struct timespec now;
	clock_gettime(CLOCK_REALTIME, &now);
	now.tv_sec += sleep_time->tv_sec;
	now.tv_nsec += sleep_time->tv_nsec;
	thread_local_coroutine->co_state = COROUTINE_PENDING;

	timer_task_t *task = timer_thread_schedule(&timer_thread,
	    timed_wakeup, thread_local_coroutine, &now);
	dec_timer_task_ref(task);
	libuzfs_coroutine_yield();
}

void
co_mutex_init(co_mutex_t *mutex)
{
	mutex->refcount = 0;
	cutex_init(&mutex->lock);
	mutex->owner = NULL;
}

void
co_mutex_destroy(co_mutex_t *mutex)
{
	ASSERT(mutex->owner == NULL);
	SPIN_UNTIL(atomic_load_32(&mutex->refcount) == 0, 30);
	cutex_fini(&mutex->lock);
}

typedef struct mutex_internal {
	uint8_t locked;
	uint8_t contended;
	uint16_t padding;
} mutex_internal_t;

const mutex_internal_t mutex_contended_raw	= {1, 1, 0};
const mutex_internal_t mutex_locked_raw		= {1, 0, 0};

#define	MUTEX_CONTENDED	(*(uint32_t *)(&mutex_contended_raw))
#define	MUTEX_LOCKED	(*(uint32_t *)(&mutex_locked_raw))

static inline void
co_mutex_contended(co_mutex_t *mutex)
{
	while (atomic_swap_32(&mutex->lock.value,
	    MUTEX_CONTENDED) & MUTEX_LOCKED) {
		// ignore return value
		(void) cutex_wait(&mutex->lock, MUTEX_CONTENDED, NULL);
	}
}

boolean_t
co_mutex_held(co_mutex_t *mutex)
{
	return (mutex->owner == thread_local_coroutine);
}

void
co_mutex_lock(co_mutex_t *mutex)
{
	uint8_t *locked = &((mutex_internal_t *)(&mutex->lock.value))->locked;
	if (atomic_swap_8(locked, 1)) {
		co_mutex_contended(mutex);
	}
	ASSERT(thread_local_coroutine != NULL);
	ASSERT3P(mutex->owner, ==, NULL);
	mutex->owner = thread_local_coroutine;
}

int
co_mutex_trylock(co_mutex_t *mutex)
{
	uint8_t *locked = &((mutex_internal_t *)(&mutex->lock.value))->locked;
	if (atomic_swap_8(locked, 1)) {
		return (0);
	}
	ASSERT(thread_local_coroutine != NULL);
	mutex->owner = thread_local_coroutine;
	return (1);
}

void
co_mutex_unlock(co_mutex_t *mutex)
{
	atomic_inc_32(&mutex->refcount);
	ASSERT(mutex->owner == thread_local_coroutine);
	mutex->owner = NULL;
	if (atomic_swap_32(&mutex->lock.value, 0) != MUTEX_LOCKED) {
		cutex_wake_one(&mutex->lock);
	}
	atomic_dec_32(&mutex->refcount);
}

void
co_cond_init(co_cond_t *cv)
{
	cutex_init(&cv->seq);
	cv->lock = NULL;
	cv->refcount = 0;
}

void
co_cond_destroy(co_cond_t *cv)
{
	SPIN_UNTIL(atomic_load_32(&cv->refcount) == 0, 30);
	cutex_fini(&cv->seq);
}

int
co_cond_wait(co_cond_t *cv, co_mutex_t *mutex)
{
	return (co_cond_timedwait(cv, mutex, NULL));
}

int
co_cond_timedwait(co_cond_t *cv, co_mutex_t *mutex,
    const struct timespec *abstime)
{
	const uint32_t expected_seq = atomic_load_32(&cv->seq.value);
	if (atomic_load_ptr(&cv->lock) != mutex) {
		if (atomic_cas_ptr(&cv->lock, NULL, mutex) != NULL) {
			return (EINVAL);
		}
	}

	co_mutex_unlock(mutex);
	// cutex_wait may never wait, which caused spurious wake-up,
	// so the caller should
	// re-check the condition after wakeup like the following
	//
	// co_mutex_lock(&mutex)
	// while (condition) {
	// 	co_cond_wait(&cv, &mutex);
	// }
	// co_mutex_unlock(&mutex);
	int ret = cutex_wait(&cv->seq, expected_seq, abstime);
	if (ret == EWOULDBLOCK) {
		ret = 0;
	}
	co_mutex_contended(mutex);
	mutex->owner = thread_local_coroutine;

	return (ret);
}

int
co_cond_signal(co_cond_t *cv)
{
	atomic_inc_32(&cv->refcount);
	atomic_inc_32(&cv->seq.value);
	// waiter may be waken up after atomic inc
	int ret = cutex_wake_one(&cv->seq);
	atomic_dec_32(&cv->refcount);
	return (ret);
}

int
co_cond_broadcast(co_cond_t *cv)
{
	co_mutex_t *mutex = atomic_load_ptr(&cv->lock);
	if (mutex == NULL) {
		return (0);
	}

	atomic_inc_32(&cv->refcount);
	atomic_inc_32(&cv->seq.value);
	int ret = cutex_requeue(&cv->seq, &mutex->lock);
	atomic_dec_32(&cv->refcount);
	return (ret);
}

#define	READ_LOCKED	1
#define	RWLOCK_MASK	((1 << 30) - 1)
#define	WRITE_LOCKED	RWLOCK_MASK
#define	MAX_READERS	(RWLOCK_MASK - 1)
#define	READERS_WAITING	(1 << 30)
#define	WRITERS_WAITING	(1 << 31)

void
co_rw_lock_init(co_rw_lock_t *rwlock)
{
	rwlock->state = 0;
	rwlock->owner = NULL;
	co_mutex_init(&rwlock->lock);
	co_cond_init(&rwlock->readers_cv);
	co_cond_init(&rwlock->writers_cv);
}

void
co_rw_lock_destroy(co_rw_lock_t *rwlock)
{
	co_mutex_destroy(&rwlock->lock);
	co_cond_destroy(&rwlock->readers_cv);
	co_cond_destroy(&rwlock->writers_cv);
}

static inline boolean_t
is_write_locked(uint32_t state)
{
	return ((state & RWLOCK_MASK) == RWLOCK_MASK);
}

static inline boolean_t
reached_max_readers(uint32_t state)
{
	return ((state & RWLOCK_MASK) == MAX_READERS);
}

// this function might give false positive, consider a situation
// when there is only one writer waiting, after it grabs the lock,
// it cannot clear the WRITERS_WAITING bit, because it doesn't know
// whether there is other writers waiting, but when it release the lock,
// the cond_signal will return 0, which indicates no other writer waiting,
// only this time can the writer clear the WRITERS_WAITING bit
static inline boolean_t
has_writers_waiting(uint32_t state)
{
	return ((state & WRITERS_WAITING) != 0);
}

void
co_rw_lock_read(co_rw_lock_t *rwlock)
{
	co_mutex_lock(&rwlock->lock);
	// this is a writer-first rwlock, so readers should
	// wait when there are writers waiting.

	// now that has_writers_waiting might give false positive result,
	// will the new reader wait forever when the lock is already
	// acquired by another reader?
	// has_writers_waiting only returns true when the following 2
	// conditions are satisified
	// 1. one or more writers are waiting. last writer will wake
	// up the new reader
	// 2. a writer has grabbed the lock. the writer will wake up
	// all waiting readers
	while (is_write_locked(rwlock->state) ||
	    has_writers_waiting(rwlock->state)) {
		// the waker should clear this bit
		rwlock->state |= READERS_WAITING;
		VERIFY0(co_cond_wait(&rwlock->readers_cv, &rwlock->lock));
	}
	VERIFY(!reached_max_readers(rwlock->state));
	rwlock->state += READ_LOCKED;
	co_mutex_unlock(&rwlock->lock);
}

static inline boolean_t
is_locked(uint32_t state)
{
	return ((state & RWLOCK_MASK) != 0);
}

void
co_rw_lock_write(co_rw_lock_t *rwlock)
{
	co_mutex_lock(&rwlock->lock);
	while (is_locked(rwlock->state)) {
		rwlock->state |= WRITERS_WAITING;
		VERIFY0(co_cond_wait(&rwlock->writers_cv, &rwlock->lock));
	}
	rwlock->state += WRITE_LOCKED;
	rwlock->owner = thread_local_coroutine;
	co_mutex_unlock(&rwlock->lock);
}

int
co_rw_lock_try_read(co_rw_lock_t *rwlock)
{
	int ret = 1;
	co_mutex_lock(&rwlock->lock);
	if (is_write_locked(rwlock->state) ||
	    has_writers_waiting(rwlock->state)) {
		ret = 0;
	} else {
		VERIFY(!reached_max_readers(rwlock->state));
		rwlock->state += READ_LOCKED;
	}
	co_mutex_unlock(&rwlock->lock);

	return (ret);
}

int
co_rw_lock_try_write(co_rw_lock_t *rwlock)
{
	int ret = 1;
	co_mutex_lock(&rwlock->lock);
	if (is_locked(rwlock->state)) {
		ret = 0;
	} else {
		rwlock->owner = thread_local_coroutine;
		rwlock->state += WRITE_LOCKED;
	}
	co_mutex_unlock(&rwlock->lock);

	return (ret);
}

static inline boolean_t
has_readers_waiting(uint32_t state)
{
	return ((state & READERS_WAITING) != 0);
}

void
co_rw_lock_exit(co_rw_lock_t *rwlock)
{
	co_mutex_lock(&rwlock->lock);
	ASSERT(is_locked(rwlock->state));
	if (is_write_locked(rwlock->state)) {
		rwlock->owner = NULL;
		rwlock->state -= WRITE_LOCKED;
	} else {
		rwlock->state -= READ_LOCKED;
	}

	if (!is_locked(rwlock->state)) {
		if (has_writers_waiting(rwlock->state)) {
			// co_cond_signal returns 0 does not mean there is no
			// writer waiting due to spurious wakeup, so the waken
			// writer should double-check the waking condition,
			// which makes this rwlock not such writer-first
			if (co_cond_signal(&rwlock->writers_cv) > 0) {
				co_mutex_unlock(&rwlock->lock);
				return;
			}
			// WRITERS_WAITING check got false positive
			// we should clear that bit and wake all readers
			rwlock->state &= ~WRITERS_WAITING;
		}

		if (has_readers_waiting(rwlock->state)) {
			rwlock->state &= ~READERS_WAITING;
			co_cond_broadcast(&rwlock->readers_cv);
		}
	}
	co_mutex_unlock(&rwlock->lock);
}

boolean_t
co_rw_lock_read_held(co_rw_lock_t *rwlock)
{
	uint32_t readers = atomic_load_32(&rwlock->state) & RWLOCK_MASK;
	return (readers > 0 && readers < WRITE_LOCKED);
}

boolean_t
co_rw_lock_write_held(co_rw_lock_t *rwlock)
{
	return (rwlock->owner == thread_local_coroutine);
}
