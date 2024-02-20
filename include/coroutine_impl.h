#ifndef _COROUTINE_IMPL_H
#define	_COROUTINE_IMPL_H

#include "libcontext.h"
#include "sys/list.h"
#include "sys/stdtypes.h"
#include "timer_thread.h"
#include <bits/stdint-uintn.h>
#include <bits/types/struct_timespec.h>
#include <pthread.h>
#include <sched.h>

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
	fcontext_t main_ctx;
	fcontext_t my_ctx;
	char *stack_bottom;
	int stack_size;
	int guard_size;
	void (*fn)(void *);
	void *arg;
	boolean_t pending; // only accessed by its coroutine
	boolean_t yielded;
	void (*wake) (void *);
	void (*record_backtrace) (uint64_t);
	void *wake_arg;
	cutex_waiter_state_t waiter_state; // protected by cutex waiter lock
	cutex_t *cutex; // the cutex this coroutine sleeps on
	uint64_t task_id; // uniquely identifies the coroutine
	timer_task_t *expire_task;
	co_specific_t *specific_head; // head of coroutine specific list
	boolean_t foreground;
	struct uzfs_coroutine *next_in_pool;
	void **bottom_fpp;
	void *saved_fp;
};

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

// cpu relax provides a hint to the processor that the code
// is in a spin-wait loop, allowing the processor to optimize
// its internal operations for this type of workload. This can
// help to reduce power consumption and improve performance by
// reducing the number of unnecessary instruction fetches and
// pipeline stalls.
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
