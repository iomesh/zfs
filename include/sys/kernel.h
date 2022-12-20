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

#ifndef	_SYS_KERNEL_H
#define	_SYS_KERNEL_H

// zfs_znode.c
typedef boolean_t bool;
typedef unsigned int u32;
typedef unsigned long int u64;
typedef	unsigned short umode_t;
typedef unsigned int kuid_t;
typedef unsigned int kgid_t;
#define	make_kuid(ns, uid) KUIDT_INIT(uid)
#define	make_kgid(ns, gid) KGIDT_INIT(gid)
#define	KUIDT_INIT(value) ((kuid_t)value)
#define	KGIDT_INIT(value) ((kgid_t)value)

static inline kuid_t __kuid_val(kuid_t uid)
{
	return (uid);
}

static inline kgid_t __kgid_val(kgid_t gid)
{
	return (gid);
}

typedef unsigned gfp_t;

typedef struct {
	volatile uint32_t counter;
} atomic_t;

typedef atomic_t atomic_long_t;

typedef pthread_spinlock_t spinlock_t;
#define	spin_lock(lock) pthread_spin_lock(lock)
#define	spin_unlock(lock) pthread_spin_unlock(lock)

struct dentry {
	struct inode *d_inode;
};

struct path {};

struct super_block {
	unsigned long		s_flags;
	struct dentry		*s_root;
	atomic_t		s_active;
	void 			*s_fs_info;	/* Filesystem private info */
	u32			s_time_gran;
};

typedef unsigned fmode_t;

typedef int (*filldir_t)(void *, const char *, int, loff_t, u64, unsigned);

struct file_operations {};
struct inode_operations {};

enum { MAX_NESTED_LINKS = 8 };

struct linux_kstat {
	u64			ino;
	dev_t			dev;
	umode_t			mode;
	unsigned int		nlink;
	kuid_t			uid;
	kgid_t			gid;
	dev_t			rdev;
	loff_t			size;
	struct timespec		atime;
	struct timespec 	mtime;
	struct timespec 	ctime;
	unsigned long   	blksize;
	unsigned long long	blocks;
};

struct inode {
	umode_t			i_mode;
	unsigned short		i_opflags;
	kuid_t			i_uid;
	kgid_t			i_gid;
	unsigned int		i_flags;
	const struct inode_operations	*i_op;
	struct super_block	*i_sb;
	unsigned long		i_ino;
	union {
		const unsigned int i_nlink;
		unsigned int __i_nlink;
	};
	loff_t			i_size;
	struct timespec		i_atime;
	struct timespec		i_mtime;
	struct timespec		i_ctime;
	spinlock_t		i_lock;
	unsigned short		i_bytes;
	unsigned int		i_blkbits;
	blkcnt_t		i_blocks;
	unsigned long		i_state;
	uint64_t		i_version;
	atomic_t		i_count;
	const struct file_operations	*i_fop;
	u32			i_generation;
};

extern void atomic_set(atomic_t *v, int i);

extern void inode_init_once(struct inode *inode);
extern struct inode *igrab(struct inode *inode);
extern void iput(struct inode *inode);
extern struct inode *new_inode(struct super_block *sb);
extern void init_special_inode(struct inode *, umode_t, dev_t);
extern void inode_set_flags(struct inode *inode, unsigned int flags,
    unsigned int mask);
extern int insert_inode_locked(struct inode *inode);
extern void unlock_new_inode(struct inode *inode);
extern void mark_inode_dirty(struct inode *inode);
extern void set_nlink(struct inode *inode, unsigned int nlink);
extern void i_size_write(struct inode *inode, loff_t i_size);
extern void truncate_setsize(struct inode *inode, loff_t newsize);

extern int timespec_compare(const struct timespec *lhs,
    const struct timespec *rhs);

static inline void zfs_uid_write(struct inode *ip, uid_t uid)
{
	ip->i_uid = make_kuid(ip->i_sb->s_user_ns, uid);
}

static inline void zfs_gid_write(struct inode *ip, gid_t gid)
{
	ip->i_gid = make_kgid(ip->i_sb->s_user_ns, gid);
}

#define	container_of(ptr, type, member) 				\
(									\
	{								\
		const typeof(((type *)0)->member) *__mptr = (ptr);	\
		(type *)((char *)__mptr - offsetof(type, member));	\
	}								\
)

#define	S_APPEND    4   /* Append-only file */
#define	S_IMMUTABLE 8   /* Immutable file */

#define	PAGE_SHIFT  12
#define	PAGE_SIZE   (1 << PAGE_SHIFT)
#define	PAGE_MASK   (~(PAGE_SIZE-1))

// policy.c
#define	CAP_SYS_ADMIN		21
#define	CAP_DAC_OVERRIDE	1
#define	CAP_DAC_READ_SEARCH	2
#define	CAP_FOWNER		3
#define	CAP_FSETID		4
#define	CAP_SETGID		6

extern struct cred *kcred;
extern boolean_t has_capability(proc_t *t, int cap);
extern boolean_t capable(int cap);

#define	zpl_inode_owner_or_capable(ns, ip)  inode_owner_or_capable(ip)
extern boolean_t inode_owner_or_capable(const struct inode *inode);

#define	mutex_owned(mp) MUTEX_HELD(mp)

// zfs_vnops.c
typedef struct zpl_dir_context {
	void *dirent;
	const filldir_t actor;
	loff_t pos;
} zpl_dir_context_t;

extern int write_inode_now(struct inode *inode, int sync);
static inline void task_io_account_read(int64_t n) {}
static inline void task_io_account_write(int64_t n) {}

// zfs_dir.c
#define	ED_CASE_CONFLICT	0x10
extern int atomic_read(const atomic_t *v);
extern void drop_nlink(struct inode *inode);
extern void clear_nlink(struct inode *inode);
extern void inc_nlink(struct inode *inode);

// zfs_vnops_os.c
#define	FMODE_WRITE		(0x2)

#define	TIME_MAX		INT64_MAX
#define	TIME_MIN		INT64_MIN

#define	TIMESPEC_OVERFLOW(ts)		\
	((ts)->tv_sec < TIME_MIN || (ts)->tv_sec > TIME_MAX)


enum writeback_sync_modes {
    WB_SYNC_NONE,   /* Don't wait on anything */
    WB_SYNC_ALL,    /* Wait on every mapping */
};

extern int atomic_add_unless(atomic_t *v, int a, int u);
extern void remove_inode_hash(struct inode *inode);
extern bool zpl_dir_emit(zpl_dir_context_t *ctx, const char *name, int namelen,
    uint64_t ino, unsigned type);
extern loff_t i_size_read(const struct inode *inode);

#define	zpl_generic_fillattr(user_ns, ip, sp)	 generic_fillattr(ip, sp)
extern void generic_fillattr(struct inode *inode, struct linux_kstat *stat);

#define	zpl_inode_timestamp_truncate(ts, ip)	\
	timespec_trunc(ts, (ip)->i_sb->s_time_gran)

extern struct timespec timespec_trunc(struct timespec t, unsigned gran);
extern uid_t zfs_uid_read(struct inode *inode);
extern gid_t zfs_gid_read(struct inode *inode);

// zfs_ctldir.c
#define	LOOKUP_FOLLOW		0x0001
#define	LOOKUP_DIRECTORY	0x0002

#define	S_IRWXUGO   (S_IRWXU|S_IRWXG|S_IRWXO)

#define	IS_ERR(ptr) (B_FALSE)

extern struct timespec current_time(struct inode *inode);
extern struct inode *ilookup(struct super_block *sb, unsigned long ino);
extern struct dentry *d_obtain_alias(struct inode *);
extern boolean_t d_mountpoint(struct dentry *dentry);
extern void dput(struct dentry *);
extern int kern_path(const char *name, unsigned int flags, struct path *path);
extern void path_put(const struct path *path);
extern int zfsctl_snapshot_unmount(const char *snapname, int flags);

extern const struct file_operations zpl_fops_root;
extern const struct inode_operations zpl_ops_root;
extern const struct file_operations zpl_fops_snapdir;
extern const struct inode_operations zpl_ops_snapdir;
extern const struct file_operations zpl_fops_shares;
extern const struct inode_operations zpl_ops_shares;
extern const struct file_operations simple_dir_operations;
extern const struct inode_operations simple_dir_inode_operations;

// zfs_vfsops.c
#define	SB_RDONLY   MS_RDONLY
#define	SB_POSIXACL MS_POSIXACL
#define	SB_NOATIME  MS_NOATIME
#define	SB_MANDLOCK MS_MANDLOCK

#define	MAX_LFS_FILESIZE INT64_MAX

// FIXME(hping): use clock functions
#define	jiffies (0)

extern void shrink_dcache_sb(struct super_block *sb);
extern void d_prune_aliases(struct inode *inode);

#define	printk printf

extern int fls(int x);

#define	typecheck(type, x)			\
(						\
	{					\
		type __dummy;			\
		typeof(x) __dummy2;		\
		(void) (&__dummy == &__dummy2);	\
		1;				\
	}					\
)

#define	time_after(a, b)			\
	(typecheck(unsigned long, a) &&		\
	typecheck(unsigned long, b) &&		\
	((long)((b) - (a)) < 0))

#define	time_before(a, b)	time_after(b, a)

#undef HAVE_SINGLE_SHRINKER_CALLBACK

// zfs_ioctl.c
#define	FKIOCTL				0x80000000
#define	is_system_labeled()		0
#define	zvol_tag(zv)			(NULL)

static inline boolean_t zfs_proc_is_caller(proc_t *t) { return B_FALSE; }

// libuzfs.c
extern uid_t crgetfsuid(const cred_t *cr);
extern gid_t crgetfsgid(const cred_t *cr);

// libzfs
extern long uzfs_ioctl(unsigned cmd, unsigned long arg);

#undef HAVE_TMPFILE
#undef HAVE_INODE_TIMESPEC64_TIMES
#undef HAVE_SPLIT_SHRINKER_CALLBACK
#undef HAVE_SINGLE_SHRINKER_CALLBACK
#undef HAVE_SUPER_SETUP_BDI_NAME
#undef CONFIG_USER_NS
#undef HAVE_VFS_IOV_ITER

#endif	/* _SYS_KERNEL_H */
