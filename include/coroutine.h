#ifndef _COROUTINE_H
#define	_COROUTINE_H

#include "coroutine_impl.h"
#include "sys/stdtypes.h"
typedef struct uzfs_coroutine uzfs_coroutine_t;

extern void coroutine_init(void);
extern void coroutine_fini(void);
extern void coroutine_key_create(uint32_t *idx);
// key delete is not needed for now,
// all keys will be deleted when coroutine destroyed
// extern void coroutine_key_delete(uint32_t idx);
extern void* coroutine_getkey(uint32_t idx);
extern int coroutine_setkey(uint32_t idx, void *local);
extern uint64_t uzfs_coroutine_self(void);
extern void coroutine_wake_and_yield(void);
extern void coroutine_sleep(const struct timespec *sleep_time);

typedef struct co_mutex co_mutex_t;

extern boolean_t co_mutex_held(co_mutex_t *mutex);
extern void co_mutex_init(co_mutex_t *mutex);
extern void co_mutex_destroy(co_mutex_t *mutex);
extern void co_mutex_lock(co_mutex_t *mutex);
extern int co_mutex_trylock(co_mutex_t *mutex);
extern void co_mutex_unlock(co_mutex_t *mutex);

typedef struct co_cond co_cond_t;

extern void co_cond_init(co_cond_t *cv);
extern void co_cond_destroy(co_cond_t *cv);
extern int co_cond_wait(co_cond_t *cv, co_mutex_t *mutex);
extern int co_cond_timedwait(co_cond_t *cv, co_mutex_t *mutex,
    const struct timespec *abstime);
extern int co_cond_signal(co_cond_t *cv);
extern int co_cond_broadcast(co_cond_t *cv);

typedef struct co_rw_lock co_rw_lock_t;

extern boolean_t co_rw_lock_read_held(co_rw_lock_t *rwlock);
extern boolean_t co_rw_lock_write_held(co_rw_lock_t *rwlock);
extern void co_rw_lock_init(co_rw_lock_t *rwlock);
extern void co_rw_lock_destroy(co_rw_lock_t *rwlock);
extern void co_rw_lock_read(co_rw_lock_t *rwlock);
extern void co_rw_lock_write(co_rw_lock_t *rwlock);
extern int co_rw_lock_try_read(co_rw_lock_t *rwlock);
extern int co_rw_lock_try_write(co_rw_lock_t *rwlock);
extern void co_rw_lock_exit(co_rw_lock_t *rwlock);
// TODO(sundengyu): implement read lock upgrade
// extern int co_rw_lock_tryupgrade(co_rw_lock_t *rwlock);

#endif
