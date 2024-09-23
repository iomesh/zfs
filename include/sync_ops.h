#ifndef _COROUTINE_SYNC_H
#define	_COROUTINE_SYNC_H

#include <bits/stdint-intn.h>
#include <bits/stdint-uintn.h>
#include <pthread.h>
#include <sys/stdtypes.h>
#include <time.h>

struct WaiterList {
	void *head;
	void *tail;
	pthread_spinlock_t lock;
};

struct Futex {
	struct WaiterList waiters;
	uint32_t value;
	uint32_t ref_cnt;
};

typedef struct CondVar {
	struct Futex futex;
} co_cond_t;

typedef struct Mutex {
	uint64_t owner;
	struct Futex futex;
} co_mutex_t;

typedef struct RwLock {
	uint64_t owner;
	struct Futex state;
	struct Futex writer_notify;
} co_rwlock_t;

typedef void coroutine_key_create_func_t(uint32_t *, void (*destructor)(void*));
typedef void* coroutine_getkey_func_t(uint32_t);
typedef int coroutine_setkey_func_t(uint32_t, void *);
typedef uint64_t uzfs_coroutine_self_func_t(void);
typedef void coroutine_sched_yield_func_t(void);
typedef void coroutine_sleep_func_t(const struct timespec *);

typedef struct coroutine_ops {
	coroutine_key_create_func_t	*coroutine_key_create;
	coroutine_getkey_func_t		*coroutine_getkey;
	coroutine_setkey_func_t		*coroutine_setkey;
	uzfs_coroutine_self_func_t	*uzfs_coroutine_self;
	coroutine_sched_yield_func_t	*coroutine_sched_yield;
	coroutine_sleep_func_t		*coroutine_sleep;
} coroutine_ops_t;

typedef int co_mutex_held_func_t(co_mutex_t *);
typedef void co_mutex_init_func_t(co_mutex_t *);
typedef void co_mutex_destroy_func_t(co_mutex_t *);
typedef void co_mutex_lock_func_t(co_mutex_t *);
typedef int co_mutex_trylock_func_t(co_mutex_t *);
typedef void co_mutex_unlock_func_t(co_mutex_t *);

typedef struct co_mutex_ops {
	co_mutex_held_func_t		*co_mutex_held;
	co_mutex_init_func_t		*co_mutex_init;
	co_mutex_destroy_func_t		*co_mutex_destroy;
	co_mutex_lock_func_t		*co_mutex_lock;
	co_mutex_trylock_func_t		*co_mutex_trylock;
	co_mutex_unlock_func_t		*co_mutex_unlock;
} co_mutex_ops_t;

typedef void co_cond_init_func_t(co_cond_t *);
typedef void co_cond_destroy_func_t(co_cond_t *);
typedef int co_cond_wait_func_t(co_cond_t *, co_mutex_t *);
typedef int co_cond_timedwait_func_t(co_cond_t *, co_mutex_t *,
    const struct timespec *);
typedef int co_cond_signal_func_t(co_cond_t *);
typedef int co_cond_broadcast_func_t(co_cond_t *);

typedef struct co_cond_ops {
	co_cond_init_func_t		*co_cond_init;
	co_cond_destroy_func_t		*co_cond_destroy;
	co_cond_wait_func_t		*co_cond_wait;
	co_cond_timedwait_func_t	*co_cond_timedwait;
	co_cond_signal_func_t		*co_cond_signal;
	co_cond_broadcast_func_t	*co_cond_broadcast;
} co_cond_ops_t;

typedef int co_rw_lock_read_held_func_t(co_rwlock_t *);
typedef int co_rw_lock_write_held_func_t(co_rwlock_t *);
typedef void co_rw_lock_init_func_t(co_rwlock_t *);
typedef void co_rw_lock_destroy_func_t(co_rwlock_t *);
typedef void co_rw_lock_read_func_t(co_rwlock_t *);
typedef void co_rw_lock_write_func_t(co_rwlock_t *);
typedef int co_rw_lock_try_read_func_t(co_rwlock_t *);
typedef int co_rw_lock_try_write_func_t(co_rwlock_t *);
typedef void co_rw_lock_exit_func_t(co_rwlock_t *);

typedef struct co_rwlock_ops {
	co_rw_lock_read_held_func_t	*co_rw_lock_read_held;
	co_rw_lock_write_held_func_t	*co_rw_lock_write_held;
	co_rw_lock_init_func_t		*co_rw_lock_init;
	co_rw_lock_destroy_func_t	*co_rw_lock_destroy;
	co_rw_lock_read_func_t		*co_rw_lock_read;
	co_rw_lock_write_func_t		*co_rw_lock_write;
	co_rw_lock_try_read_func_t	*co_rw_lock_try_read;
	co_rw_lock_try_write_func_t	*co_rw_lock_try_write;
	co_rw_lock_exit_func_t		*co_rw_lock_exit;
} co_rwlock_ops_t;

typedef void *register_aio_fd_func_t(int, void (*)(void *, int64_t));
typedef void unregister_aio_fd_func_t(void *);
typedef void submit_aio_read_func_t(const void *, uint64_t,
    char *, uint64_t, void *);
typedef void submit_aio_write_func_t(const void *, uint64_t,
    const char *, uint64_t, void *);
typedef void submit_aio_fsync_func_t(const void *, void *);

typedef struct aio_ops {
	register_aio_fd_func_t		*register_aio_fd;
	unregister_aio_fd_func_t	*unregister_aio_fd;
	submit_aio_read_func_t		*submit_aio_read;
	submit_aio_write_func_t		*submit_aio_write;
	submit_aio_fsync_func_t		*submit_aio_fsync;
} aio_ops_t;

typedef uint64_t uthread_create_func_t(void (*)(void *), void *, int);
typedef void uthread_exit_func_t(void);
typedef void uthread_join_func_t(uint64_t);

typedef struct thread_ops {
	uthread_create_func_t	*uthread_create;
	uthread_exit_func_t	*uthread_exit;
	uthread_join_func_t	*uthread_join;
} thread_ops_t;

typedef void *taskq_create_func_t(const char *, int);
typedef uint64_t taskq_dispatch_func_t(const void *,
    void (*)(void *), void *, uint32_t);
typedef uint64_t taskq_delay_dispatch_func_t(const void *,
    void (*)(void *), void *, const struct timespec *);
typedef int taskq_member_func_t(const void *, uint64_t);
typedef void *taskq_of_curthread_func_t(void);
typedef void taskq_wait_func_t(const void *);
typedef void taskq_destroy_func_t(const void *);
typedef void taskq_wait_id_func_t(const void *, uint64_t);
typedef int taskq_cancel_id_func_t(const void *, uint64_t);
typedef int taskq_is_empty_func_t(const void *);
typedef int taskq_nalloc_func_t(const void *);

typedef struct taskq_ops {
	taskq_create_func_t		*taskq_create;
	taskq_dispatch_func_t		*taskq_dispatch;
	taskq_delay_dispatch_func_t	*taskq_delay_dispatch;
	taskq_member_func_t		*taskq_member;
	taskq_of_curthread_func_t	*taskq_of_curthread;
	taskq_wait_func_t		*taskq_wait;
	taskq_destroy_func_t		*taskq_destroy;
	taskq_wait_id_func_t		*taskq_wait_id;
	taskq_cancel_id_func_t		*taskq_cancel_id;
	taskq_is_empty_func_t		*taskq_is_empty;
	taskq_nalloc_func_t		*taskq_nalloc;
} taskq_ops_t;

typedef void print_log_func_t(const char *, int);
typedef void kstat_install_func_t(const char *, void *, int);
typedef void kstat_uninstall_func_t(const char *);
typedef void backtrace_func_t(void);
typedef void record_txg_delay_func_t(const void *, int, uint64_t);
typedef void record_zio_func_t(const void *, const int64_t *, int);

typedef struct stat_ops {
	print_log_func_t	*print_log;
	kstat_install_func_t	*kstat_install;
	kstat_uninstall_func_t	*kstat_uinstall;
	backtrace_func_t	*backtrace;
	record_txg_delay_func_t	*record_txg_delays;
	record_zio_func_t	*record_zio;
} stat_ops_t;

#endif
