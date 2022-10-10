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

typedef boolean_t bool;
typedef unsigned int u32;
typedef unsigned long int u64;
typedef	unsigned short umode_t;
typedef unsigned int kuid_t;
typedef unsigned int kgid_t;
#define make_kuid(ns, uid) KUIDT_INIT(uid)
#define make_kgid(ns, gid) KGIDT_INIT(gid)
#define KUIDT_INIT(value) ((kuid_t) value )
#define KGIDT_INIT(value) ((kgid_t) value )

static inline kuid_t __kuid_val(kuid_t uid)
{
    return uid;
}

static inline kgid_t __kgid_val(kgid_t gid)
{
    return gid;
}

struct backing_dev_info {
	unsigned long ra_pages;	/* max readahead in PAGE_CACHE_SIZE units */
};

typedef unsigned gfp_t;

struct shrink_control {
	gfp_t gfp_mask;
	unsigned long nr_to_scan;
};

typedef struct {
    volatile uint32_t counter;
} atomic_t;

typedef atomic_t atomic_long_t;

struct shrinker {
	int (*shrink)(struct shrinker *, struct shrink_control *sc);
	int seeks;	/* seeks to recreate an obj */
	long batch;	/* reclaim batch size, 0 = default */

	/* These are for internal use */
	struct list_node list;
	atomic_long_t nr_in_batch; /* objs pending delete */
};

struct super_operations {};
struct export_operations {};

typedef pthread_spinlock_t spinlock_t;
#define spin_lock(lock) pthread_spin_lock(lock)
#define spin_unlock(lock) pthread_spin_unlock(lock)


struct lockref {
    union {
        struct {
            spinlock_t lock;
            unsigned int count;
        };
    };
};

struct qstr {
    union {
        struct {
		u32 hash;
		u32 len;
        };
        u64 hash_len;
    };
    const unsigned char *name;
};

struct dentry {
    unsigned int d_flags;       /* protected by d_lock */
    struct dentry *d_parent;    /* parent directory */
    struct qstr d_name;
    struct inode *d_inode;      /* Where the name belongs to - NULL is negative */
    struct lockref d_lockref;   /* per-dentry lock and refcount */
    const struct dentry_operations *d_op;
    struct super_block *d_sb;   /* The root of the dentry tree */
    unsigned long d_time;       /* used by d_revalidate */
};

struct xattr_handler {
    const char *prefix;
    int flags;  /* fs private flags passed back to the handlers */
    size_t (*list)(struct dentry *dentry, char *list, size_t list_size,
               const char *name, size_t name_len, int handler_flags);
    int (*get)(struct dentry *dentry, const char *name, void *buffer,
           size_t size, int handler_flags);
    int (*set)(struct dentry *dentry, const char *name, const void *buffer,
           size_t size, int flags, int handler_flags);
};
typedef const struct xattr_handler  xattr_handler_t;

struct path {
	struct vfsmount *mnt;
	struct dentry *dentry;
};

struct dentry_operations {
    int (*d_revalidate)(struct dentry *, unsigned int);
    struct vfsmount *(*d_automount)(struct path *);
};

struct super_block {
	unsigned char		s_blocksize_bits;
	unsigned long		s_blocksize;
	loff_t			s_maxbytes;	/* Max file size */
	const struct super_operations	*s_op;
	const struct export_operations *s_export_op;
	unsigned long		s_flags;
	unsigned long		s_magic;
	struct dentry		*s_root;
	atomic_t		s_active;
	const struct xattr_handler **s_xattr;
	struct backing_dev_info *s_bdi;
	void 			*s_fs_info;	/* Filesystem private info */
	u32		   s_time_gran;
	const struct dentry_operations *s_d_op; /* default d_op for dentries */
	struct shrinker s_shrink;	/* per-sb shrinker handle */
};

typedef unsigned fmode_t;

struct file {
    struct path     f_path;
#define f_dentry    f_path.dentry
    struct inode        *f_inode;   /* cached value */
    const struct file_operations    *f_op;
    spinlock_t      f_lock;
    unsigned int        f_flags;
    fmode_t         f_mode;
    loff_t          f_pos;
    u64         f_version;
    struct address_space    *f_mapping;
};

struct kiocb {
    struct file     *ki_filp;
    loff_t          ki_pos;
};

typedef int (*filldir_t)(void *, const char *, int, loff_t, u64, unsigned);

struct file_operations {
    loff_t (*llseek) (struct file *, loff_t, int);
    ssize_t (*read) (struct file *, char *, size_t, loff_t *);
    ssize_t (*write) (struct file *, const char *, size_t, loff_t *);
    ssize_t (*aio_read) (struct kiocb *, const struct iovec *, unsigned long, loff_t);
    ssize_t (*aio_write) (struct kiocb *, const struct iovec *, unsigned long, loff_t);
    int (*readdir) (struct file *, void *, filldir_t);
    long (*unlocked_ioctl) (struct file *, unsigned int, unsigned long);
    int (*open) (struct inode *, struct file *);
    int (*release) (struct inode *, struct file *);
    int (*fsync) (struct file *, loff_t, loff_t, int datasync);
    int (*aio_fsync) (struct kiocb *, int datasync);
    long (*fallocate)(struct file *file, int mode, loff_t offset, loff_t len);
};

struct iattr {
    unsigned int    ia_valid;
    umode_t     ia_mode;
    kuid_t      ia_uid;
    kgid_t      ia_gid;
    loff_t      ia_size;
    struct timespec ia_atime;
    struct timespec ia_mtime;
    struct timespec ia_ctime;
    struct file *ia_file;
};

enum { MAX_NESTED_LINKS = 8 };

struct nameidata {
    struct path path;
    struct qstr last;
    struct path root;
    struct inode    *inode; /* path.dentry.d_inode */
    unsigned int    flags;
    unsigned    seq;
    int     last_type;
    unsigned    depth;
    char *saved_names[MAX_NESTED_LINKS + 1];
};

struct linux_kstat {
    u64     ino;
    dev_t       dev;
    umode_t     mode;
    unsigned int    nlink;
    kuid_t      uid;
    kgid_t      gid;
    dev_t       rdev;
    loff_t      size;
    struct timespec  atime;
    struct timespec mtime;
    struct timespec ctime;
    unsigned long   blksize;
    unsigned long long  blocks;
};

struct inode_operations {
    struct dentry * (*lookup) (struct inode *,struct dentry *, unsigned int);
    void * (*follow_link) (struct dentry *, struct nameidata *);
    int (*readlink) (struct dentry *, char *,int);
    void (*put_link) (struct dentry *, struct nameidata *, void *);

    int (*create) (struct inode *,struct dentry *, umode_t, bool);
    int (*link) (struct dentry *,struct inode *,struct dentry *);
    int (*unlink) (struct inode *,struct dentry *);
    int (*symlink) (struct inode *,struct dentry *,const char *);
    int (*mkdir) (struct inode *,struct dentry *,umode_t);
    int (*rmdir) (struct inode *,struct dentry *);
    int (*mknod) (struct inode *,struct dentry *,umode_t,dev_t);
    int (*rename) (struct inode *, struct dentry *, struct inode *, struct dentry *);
    int (*setattr) (struct dentry *, struct iattr *);
    int (*getattr) (struct vfsmount *mnt, struct dentry *, struct linux_kstat *);
    int (*setxattr) (struct dentry *, const char *,const void *,size_t,int);
    ssize_t (*getxattr) (struct dentry *, const char *, void *, size_t);
    ssize_t (*listxattr) (struct dentry *, char *, size_t);
    int (*removexattr) (struct dentry *, const char *);
};

struct page {
    unsigned long flags;
    struct address_space *mapping;
};

struct address_space_operations {
    int (*set_page_dirty)(struct page *page);
    ssize_t (*direct_IO)(int, struct kiocb *, const struct iovec *iov, loff_t offset, unsigned long nr_segs);
};

struct address_space {
    struct inode        *host;      /* owner: inode, block_device */
    const struct address_space_operations *a_ops;   /* methods */
} __attribute__((aligned(sizeof(long))));

struct inode {
	umode_t			i_mode;
	unsigned short		i_opflags;
	kuid_t			i_uid;
	kgid_t			i_gid;
	unsigned int		i_flags;
	const struct inode_operations	*i_op;
	struct super_block	*i_sb;
	struct address_space	*i_mapping;
	unsigned long		i_ino;
	union {
		const unsigned int i_nlink;
		unsigned int __i_nlink;
	};
	loff_t			i_size;
	struct timespec		i_atime;
	struct timespec		i_mtime;
	struct timespec		i_ctime;
	spinlock_t		i_lock;	/* i_blocks, i_bytes, maybe i_size */
	unsigned short          i_bytes;
	unsigned int		i_blkbits;
	blkcnt_t		i_blocks;
	unsigned long		i_state;
	struct kmutex		i_mutex;
	unsigned long		dirtied_when;	/* jiffies of first dirtying */
	uint64_t		i_version;
	atomic_t		i_count;
	atomic_t		i_dio_count;
	atomic_t		i_writecount;
	const struct file_operations	*i_fop;	/* former ->i_op->default_file_ops */
	struct address_space	i_data;
	u32			i_generation;
	void			*i_private; /* fs or device private pointer */
};

// zfs_znode.c
extern const struct inode_operations zpl_inode_operations;
extern const struct inode_operations zpl_dir_inode_operations;
extern const struct inode_operations zpl_symlink_inode_operations;
extern const struct inode_operations zpl_special_inode_operations;
extern const struct address_space_operations zpl_address_space_operations;
extern const struct file_operations zpl_file_operations;
extern const struct file_operations zpl_dir_file_operations;

inline void inode_init_once(struct inode *inode)
{
    dprintf("%s: %ld\n", __func__, inode->i_ino);
    memset(inode, 0, sizeof(*inode));
}

#define container_of(ptr, type, member) ({          \
    const typeof( ((type *)0)->member ) *__mptr = (ptr);    \
    (type *)( (char *)__mptr - offsetof(type,member) );})

#define S_APPEND    4   /* Append-only file */
#define S_IMMUTABLE 8   /* Immutable file */

#define PAGE_SHIFT  12
#define PAGE_SIZE   (1 << PAGE_SHIFT)
#define PAGE_MASK   (~(PAGE_SIZE-1))

#endif	/* _SYS_KERNEL_H */
