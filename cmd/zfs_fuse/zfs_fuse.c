/*
  FUSE: Filesystem in Userspace
  Copyright (C) 2001-2007  Miklos Szeredi <miklos@szeredi.hu>

  This program can be distributed under the terms of the GNU GPL.
  See the file COPYING.
*/

/** @file
 *
 * minimal example filesystem using low-level API
 *
 */

#define FUSE_USE_VERSION 30

#include <fuse3/fuse_lowlevel.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <assert.h>
#include <fuse3/fuse.h>
#include <libuzfs.h>

struct zfs_fuse_conf_t {
	char	*zfsname;
};

#ifndef offsetof
#define offsetof(TYPE, MEMBER)                  \
    ((size_t) &((TYPE *)0)->MEMBER)
#endif

static struct fuse_opt zfs_fuse_opt[] = {
	{ "--zfsname=%s",	offsetof(struct zfs_fuse_conf_t, zfsname),	0 },
	FUSE_OPT_END
};

uint64_t fsid = 0;
fuse_ino_t root_ino = 0;

/* FUSE handlers */
static void zfs_fuse_init(void *userdata, struct fuse_conn_info *conn)
{
	dprintf("%s: %d\n", __func__, __LINE__);
	int err = 0;
	libuzfs_set_zpool_cache_path("/tmp/zpool.cache");
	libuzfs_init();
	err = libuzfs_fs_init(((struct zfs_fuse_conf_t *)userdata)->zfsname, &fsid);
	assert(err == 0);
	err = libuzfs_getroot(fsid, &root_ino);
	assert(err == 0);
	conn->max_write = 1 << 20;
}

static void zfs_fuse_destroy(void *userdata)
{
	dprintf("%s: %d\n", __func__, __LINE__);
	libuzfs_fs_fini(fsid);
}

static void zfs_fuse_getattr(fuse_req_t req, fuse_ino_t ino,
			     struct fuse_file_info *fi)
{
	dprintf("%s: %d\n", __func__, __LINE__);
	struct stat stbuf;

	(void) fi;

	if (ino == FUSE_ROOT_ID)
		ino = root_ino;

	memset(&stbuf, 0, sizeof(stbuf));

	if (libuzfs_getattr(fsid, ino, &stbuf) == -1)
		goto err;

	fuse_reply_attr(req, &stbuf, 1.0);
	return;
err:
	fuse_reply_err(req, ENOENT);
}

static void zfs_fuse_lookup(fuse_req_t req, fuse_ino_t parent, const char *name)
{
	dprintf("%s: %d\n", __func__, __LINE__);
	struct fuse_entry_param e;
	int err = 0;

	if (parent == FUSE_ROOT_ID)
		parent = root_ino;

	fuse_ino_t ino = 0;
	err = libuzfs_lookup(fsid, parent, name, &ino);
	if (err) goto err;

	e.ino = ino;

	err = libuzfs_getattr(fsid, ino, &e.attr);
	if (err) goto err;

	fuse_reply_entry(req, &e);
	return;

err:
	fuse_reply_err(req, err);
}

static void zfs_fuse_mkdir(fuse_req_t req, fuse_ino_t parent, const char* name, mode_t mode)
{
	dprintf("%s: %d\n", __func__, __LINE__);
	struct fuse_entry_param e;
	int err = 0;

	if (parent == FUSE_ROOT_ID)
		parent = root_ino;

	fuse_ino_t ino = 0;
	err = libuzfs_mkdir(fsid, parent, name, mode, &ino);
	if (err) goto err;

	e.ino = ino;

	err = libuzfs_getattr(fsid, ino, &e.attr);
	if (err) goto err;

	fuse_reply_entry(req, &e);
	return;

err:
	fuse_reply_err(req, err);
}

static void zfs_fuse_rmdir(fuse_req_t req, fuse_ino_t parent, const char* name)
{
	dprintf("%s: %d\n", __func__, __LINE__);
	int err = 0;

	if (parent == FUSE_ROOT_ID)
		parent = root_ino;

	err = libuzfs_rmdir(fsid, parent, name);
	fuse_reply_err(req, err);
}

struct dirents {
	fuse_req_t req;
	char *buf;
	size_t bufsize;
	loff_t bufoff;
};

static int zfs_fuse_fill_dir(void *data, const char *name, int namelen,
		loff_t off, uint64_t ino, unsigned type)
{
	dprintf("%s: %d\n", __func__, __LINE__);
	struct dirents* ents = data;
	char *buf = ents->buf;
	size_t bufsize = ents->bufsize;
	loff_t bufoff = ents->bufoff;
	size_t rem = bufsize - bufoff;

	struct stat st = {
		.st_ino = ino,
		.st_mode = type << 12,
	};

	size_t entsize = fuse_add_direntry(ents->req, buf + bufoff, rem, name, &st, off);
	if (entsize > rem) {
		return EOVERFLOW;
	}

	ents->bufoff += entsize;

	return 0;
}

static void zfs_fuse_readdir(fuse_req_t req, fuse_ino_t ino, size_t size, off_t offset, struct fuse_file_info *fi)
{
	dprintf("%s: %d\n", __func__, __LINE__);
	int err = 0;
	struct dirents ents;

	ents.buf = calloc(size, 1);
	if (!ents.buf) {
		err = ENOMEM;
		goto err;
	}

	ents.req = req;
	ents.bufsize = size;
	ents.bufoff = 0;

	ino = (ino == FUSE_ROOT_ID) ? root_ino : ino;

	err = libuzfs_readdir(fsid, ino, &ents, zfs_fuse_fill_dir, offset);
	if (err) goto err;

	fuse_reply_buf(req, ents.buf, ents.bufoff);
	free(ents.buf);
	return;
err:
	if (ents.buf)
		free(ents.buf);
	fuse_reply_err(req, err);
}

static void zfs_fuse_create(fuse_req_t req, fuse_ino_t parent, const char *name,
		mode_t mode, struct fuse_file_info *fi)
{
	dprintf("%s: %d\n", __func__, __LINE__);
	struct fuse_entry_param e;
	int err = 0;

	if (parent == FUSE_ROOT_ID)
		parent = root_ino;

	fuse_ino_t ino = 0;
	err = libuzfs_create(fsid, parent, name, mode, &ino);
	if (err) goto err;

	e.ino = ino;

	err = libuzfs_getattr(fsid, ino, &e.attr);
	if (err) goto err;

	fuse_reply_create(req, &e, fi);
	return;

err:
	fuse_reply_err(req, err);
}

static void zfs_fuse_unlink(fuse_req_t req, fuse_ino_t parent, const char *name)
{
	dprintf("%s: %d\n", __func__, __LINE__);
	int err = 0;

	if (parent == FUSE_ROOT_ID)
		parent = root_ino;

	err = libuzfs_remove(fsid, parent, name);
	fuse_reply_err(req, err);
}


static void zfs_fuse_rename(fuse_req_t req, fuse_ino_t parent, const char *name,
		fuse_ino_t newparent, const char *newname, unsigned int flags)
{
	dprintf("%s: %d\n", __func__, __LINE__);
	int err = 0;
	parent = (parent == FUSE_ROOT_ID) ? root_ino : parent;
	newparent = (newparent == FUSE_ROOT_ID) ? root_ino : newparent;

	err = libuzfs_rename(fsid, parent, name, newparent, newname);
	fuse_reply_err(req, err);
}

// FIXME(hping): implement this
static void zfs_fuse_setattr(fuse_req_t req, fuse_ino_t ino, struct stat *attr, int to_set,
		struct fuse_file_info *fi)
{
	dprintf("%s: %d\n", __func__, __LINE__);
	struct stat orig_attr = {0};
	int err = libuzfs_getattr(fsid, ino, &orig_attr);
	if (err)
		fuse_reply_err(req, err);
	else
		fuse_reply_attr(req, &orig_attr, 1.0);
}

static void zfs_fuse_fsync(fuse_req_t req, fuse_ino_t ino, int datasync, struct fuse_file_info *fi)
{
	dprintf("%s: %d\n", __func__, __LINE__);
	int err = libuzfs_fsync(fsid, ino, 0);
	fuse_reply_err(req, err);
}

static void zfs_fuse_open(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
	dprintf("%s: %d\n", __func__, __LINE__);
	ino = (ino == FUSE_ROOT_ID) ? root_ino : ino;
	fuse_reply_open(req, fi);
}

static void zfs_fuse_read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
		struct fuse_file_info *fi)
{
	dprintf("%s: %d\n", __func__, __LINE__);
	assert(ino != FUSE_ROOT_ID);

	int err = 0;
	char *buf = calloc(size, 1);
	if (!buf) {
		err = ENOMEM;
		goto err;
	}

	struct iovec iov;
	iov.iov_base = buf;
	iov.iov_len = size;

	zfs_uio_t uio;
	zfs_uio_iovec_init(&uio, &iov, 1, off, UIO_USERSPACE, iov.iov_len, 0);

	err = libuzfs_read(fsid, ino, &uio, 0);
	if (err) {
		goto err;
	}

	fuse_reply_buf(req, buf, size);
	free(buf);
	return;
err:
	fuse_reply_err(req, err);
}

static void zfs_fuse_write(fuse_req_t req, fuse_ino_t ino, const char *buf, size_t size,
		off_t off, struct fuse_file_info *fi)
{
	dprintf("%s: %d\n", __func__, __LINE__);
	assert(ino != FUSE_ROOT_ID);

	int err = 0;

	struct iovec iov;
	iov.iov_base = buf;
	iov.iov_len = size;

	zfs_uio_t uio;
	zfs_uio_iovec_init(&uio, &iov, 1, off, UIO_USERSPACE, iov.iov_len, 0);

	err = libuzfs_write(fsid, ino, &uio, 0);
	if (err) {
		goto err;
	}

	fuse_reply_write(req, size);
	return;
err:
	fuse_reply_err(req, err);
}

static struct fuse_lowlevel_ops zfs_fuse_oper = {
	.init		= zfs_fuse_init,
	.destroy	= zfs_fuse_destroy,
	.getattr	= zfs_fuse_getattr,
	.setattr	= zfs_fuse_setattr,
	.lookup		= zfs_fuse_lookup,
	.mkdir		= zfs_fuse_mkdir,
	.rmdir		= zfs_fuse_rmdir,
//	.opendir	= zfs_fuse_opendir,
//	.releasedir	= zfs_fuse_releasedir,
	.readdir	= zfs_fuse_readdir,
	.create		= zfs_fuse_create,
	.unlink		= zfs_fuse_unlink,
	.rename		= zfs_fuse_rename,
	.fsync		= zfs_fuse_fsync,
	.open		= zfs_fuse_open,
	.read		= zfs_fuse_read,
	.write		= zfs_fuse_write,
};

int main(int argc, char *argv[])
{
	struct fuse_args args = FUSE_ARGS_INIT(argc, argv);
	struct fuse_session *se;
	struct fuse_cmdline_opts opts;
	int ret = -1;

	struct zfs_fuse_conf_t fuse_conf;
	memset(&fuse_conf, 0, sizeof(fuse_conf));
	if (fuse_opt_parse(&args, &fuse_conf, zfs_fuse_opt, NULL) != 0){
		return 1;
	}

	dprintf("starting fuse[%d] %s\n", getpid(), fuse_conf.zfsname);

	if (fuse_parse_cmdline(&args, &opts) != 0)
		return 1;
	if (opts.show_help) {
		printf("usage: %s [options] <mountpoint>\n\n", argv[0]);
		fuse_cmdline_help();
		fuse_lowlevel_help();
		ret = 0;
		goto err_out1;
	} else if (opts.show_version) {
		printf("FUSE library version %s\n", fuse_pkgversion());
		fuse_lowlevel_version();
		ret = 0;
		goto err_out1;
	}

	se = fuse_session_new(&args, &zfs_fuse_oper,
			      sizeof(zfs_fuse_oper), &fuse_conf);
	if (se == NULL)
	    goto err_out1;

	if (fuse_set_signal_handlers(se) != 0)
	    goto err_out2;

	if (fuse_session_mount(se, opts.mountpoint) != 0)
	    goto err_out3;

	fuse_daemonize(opts.foreground);

	/* Block until ctrl+c or fusermount -u */
	if (opts.singlethread)
		ret = fuse_session_loop(se);
	else
		ret = fuse_session_loop_mt(se, opts.clone_fd);

	fuse_session_unmount(se);
err_out3:
	fuse_remove_signal_handlers(se);
err_out2:
	fuse_session_destroy(se);
err_out1:
	free(opts.mountpoint);
	fuse_opt_free_args(&args);

	return ret ? 1 : 0;
}
