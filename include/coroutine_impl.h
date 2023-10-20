#ifndef _COROUTINE_IMPL_H
#define	_COROUTINE_IMPL_H

#include "sys/list.h"
#include "sys/stdtypes.h"
#include "timer_thread.h"
#include <bits/stdint-uintn.h>
#include <bits/types/struct_timespec.h>
#include <pthread.h>
#include <sched.h>
#include <sys/ucontext.h>

#ifdef ENABLE_MINITRACE_C
#include <minitrace_c/minitrace_c.h>
#endif

// TODO(sundengyu): use object pool to manage cutex
typedef struct cutex {
	uint32_t value;
	// TODO(sundengyu): use a spin lock
	pthread_mutex_t waiter_lock;
	// TODO(sundnegyu): use a smaller list
	list_t waiter_list;
} cutex_t;

typedef enum cutex_waiter_state {
	CUTEX_WAITER_NONE,
	CUTEX_WAITER_TIMEOUT,
	CUTEX_WAITER_READY,
} cutex_waiter_state_t;

typedef struct co_specific {
	uint32_t key;
	void *value;
	struct co_specific *next;
} co_specific_t;

enum coroutine_state {
	COROUTINE_RUNNABLE,
	COROUTINE_PENDING,
	COROUTINE_DONE,
};

#define	MAX_LOCAL	UINT32_MAX
struct uzfs_coroutine {
	uint32_t co_state;
	list_node_t node;
	ucontext_t main_ctx;
	ucontext_t my_ctx;
	boolean_t pending; // only accessed by its coroutine
	void (*wake) (void *);
	void *arg;
	cutex_waiter_state_t waiter_state; // protected by cutex waiter lock
	cutex_t *cutex;
	uint64_t task_id;
	timer_task_t *expire_task;
	co_specific_t *specific_head;
	boolean_t foreground;
	struct uzfs_coroutine *next_in_pool;
#ifdef ENABLE_MINITRACE_C
	mtr_span *current_parent_span;
#endif
};

extern void cutex_init(cutex_t *cutex);
extern void cutex_fini(cutex_t *cutex);
extern int cutex_wait(cutex_t *cutex, int expected_value,
    const struct timespec *abstime);
extern int cutex_wake_one(cutex_t *cutex);
// wake up the first waiter of cutex1, and move all waiters of cutex1 to cutex2
extern int cutex_requeue(cutex_t *cutex1, cutex_t *cutex2);

struct co_mutex {
	cutex_t lock;
	uint32_t refcount;
	struct uzfs_coroutine *owner;
};

struct co_cond {
	cutex_t seq;
	struct co_mutex *lock;
	uint32_t refcount;
};

// TODO(sundengyu): alleviate the cacheline ping-pong problem
// like folly shared mutex if we really have that problem
struct co_rw_lock {
	struct co_mutex lock;
	struct co_cond readers_cv;
	struct co_cond writers_cv;

	// The state consists of a 30-bit reader counter,
	// a 'readers waiting' flag, and a 'writers waiting' flag.
	// Bits 0..30:
	//   0: Unlocked
	//   1..=0x3FFF_FFFE: Locked by N readers
	//   0x3FFF_FFFF: Write locked
	// Bit 30: Readers are waiting on this cutex.
	// Bit 31: Writers are waiting on the writer_notify cutex.
	uint32_t state;

	struct uzfs_coroutine *owner;
};

#ifndef cpu_relax
#if defined(ARCH_CPU_ARM_FAMILY)
#define	cpu_relax() asm volatile("yield\n": : :"memory")
#else
#define	cpu_relax() asm volatile("pause\n": : :"memory")
#endif
#endif

#define	SPIN_UNTIL(condition, nspins)			\
{							\
	uint64_t cur_spins = 0;				\
	while (!(condition)) {				\
		if (++cur_spins % nspins == 0) {	\
			sched_yield();			\
		} else {				\
			cpu_relax();			\
		}					\
	}						\
}							\

#endif
