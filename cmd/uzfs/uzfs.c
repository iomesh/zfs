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

/*
 * Copyright (c) 2022, SmartX Inc. All rights reserved.
 */

#include <libintl.h>
#include <unistd.h>
#include <stdlib.h>
#include <time.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <libzfs.h>
#include <locale.h>
#include <getopt.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/zfs_context.h>
#include <libuzfs.h>
#include <pthread.h>
#include <sys/nvpair.h>
#include <umem.h>

static int uzfs_zpool_create(int argc, char **argv);
static int uzfs_zpool_destroy(int argc, char **argv);
static int uzfs_zpool_set(int argc, char **argv);
static int uzfs_zpool_get(int argc, char **argv);

static int uzfs_zpool_import(int argc, char **argv);
static int uzfs_zpool_export(int argc, char ** argv);

static int uzfs_dataset_create(int argc, char **argv);
static int uzfs_dataset_destroy(int argc, char **argv);

static int uzfs_dataset_get_sbino(int argc, char **argv);

static int uzfs_objects_create(int argc, char **argv);
static int uzfs_object_delete(int argc, char **argv);
static int uzfs_object_claim(int argc, char **argv);
static int uzfs_object_get_gen(int argc, char **argv);
static int uzfs_object_get_size(int argc, char **argv);
static int uzfs_object_stat(int argc, char **argv);
static int uzfs_object_list(int argc, char **argv);
static int uzfs_object_read(int argc, char **argv);
static int uzfs_object_write(int argc, char **argv);
static int uzfs_object_sync(int argc, char **argv);
static int uzfs_object_truncate(int argc, char **argv);

static int uzfs_object_perf(int argc, char **argv);

static int uzfs_zap_create(int argc, char **argv);
static int uzfs_zap_delete(int argc, char **argv);
static int uzfs_zap_add(int argc, char **argv);
static int uzfs_zap_remove(int argc, char **argv);
static int uzfs_zap_update(int argc, char **argv);
static int uzfs_zap_lookup(int argc, char **argv);
static int uzfs_zap_count(int argc, char **argv);

static int uzfs_fs_create(int argc, char **argv);
static int uzfs_fs_destroy(int argc, char **argv);

static int uzfs_stat(int argc, char **argv);
static int uzfs_mkdir(int argc, char **argv);
static int uzfs_create(int argc, char **argv);
static int uzfs_rm(int argc, char **argv);
static int uzfs_ls(int argc, char **argv);
static int uzfs_mv(int argc, char **argv);

static int uzfs_read(int argc, char **argv);
static int uzfs_write(int argc, char **argv);
static int uzfs_fsync(int argc, char **argv);

static int uzfs_perf(int argc, char **argv);

static int uzfs_inode_create(int argc, char **argv);
static int uzfs_inode_delete(int argc, char **argv);
static int uzfs_inode_getattr(int argc, char **argv);
static int uzfs_inode_setattr(int argc, char **argv);
static int uzfs_inode_get_kvobj(int argc, char **argv);
static int uzfs_inode_get_kvattr(int argc, char **argv);
static int uzfs_inode_set_kvattr(int argc, char **argv);
static int uzfs_inode_rm_kvattr(int argc, char **argv);
static int uzfs_attr_random_test(int argc, char **argv);
static int uzfs_object_test(int argc, char **argv);

static int uzfs_dentry_create(int argc, char **argv);
static int uzfs_dentry_delete(int argc, char **argv);
static int uzfs_dentry_lookup(int argc, char **argv);
static int uzfs_dentry_list(int argc, char **argv);

static int uzfs_io_bench(int argc, char **argv);

static int uzfs_dataset_expand(int argc, char **argv);

typedef enum {
	HELP_ZPOOL_CREATE,
	HELP_ZPOOL_DESTROY,
	HELP_ZPOOL_IMPORT,
	HELP_ZPOOL_EXPORT,
	HELP_ZPOOL_SET,
	HELP_ZPOOL_GET,
	HELP_DATASET_CREATE,
	HELP_DATASET_DESTROY,
	HELP_DATASET_GET_SBINO,
	HELP_OBJECT_CREATE,
	HELP_OBJECT_DELETE,
	HELP_OBJECT_CLAIM,
	HELP_OBJECT_GET_GEN,
	HELP_OBJECT_GET_SIZE,
	HELP_OBJECT_STAT,
	HELP_OBJECT_LIST,
	HELP_OBJECT_READ,
	HELP_OBJECT_WRITE,
	HELP_OBJECT_SYNC,
	HELP_OBJECT_TRUNC,
	HELP_OBJECT_PERF,
	HELP_ZAP_CREATE,
	HELP_ZAP_DELETE,
	HELP_ZAP_ADD,
	HELP_ZAP_REMOVE,
	HELP_ZAP_UPDATE,
	HELP_ZAP_LOOKUP,
	HELP_ZAP_COUNT,
	HELP_FS_CREATE,
	HELP_FS_DESTROY,
	HELP_STAT,
	HELP_MKDIR,
	HELP_CREATE,
	HELP_RM,
	HELP_LS,
	HELP_MV,
	HELP_READ,
	HELP_WRITE,
	HELP_FSYNC,
	HELP_PERF,
	HELP_INODE_CREATE,
	HELP_INODE_DELETE,
	HELP_INODE_GETATTR,
	HELP_INODE_SETATTR,
	HELP_INODE_GET_KVOBJ,
	HELP_INODE_GET_KVATTR,
	HELP_INODE_SET_KVATTR,
	HELP_INODE_RM_KVATTR,
	HELP_DENTRY_CREATE,
	HELP_DENTRY_DELETE,
	HELP_DENTRY_LOOKUP,
	HELP_DENTRY_LIST,
	HELP_ATTR_TEST,
	HELP_OBJECT_TEST,
	HELP_IO_BENCH,
	HELP_DATASET_EXPAND,
} uzfs_help_t;

typedef struct uzfs_command {
	const char	*name;
	int		(*func)(int argc, char **argv);
	uzfs_help_t	usage;
} uzfs_command_t;

/*
 * Master command table.  Each ZFS command has a name, associated function, and
 * usage message.  The usage messages need to be internationalized, so we have
 * to have a function to return the usage message based on a command index.
 *
 * These commands are organized according to how they are displayed in the usage
 * message.  An empty command (one with a NULL name) indicates an empty line in
 * the generic usage message.
 */
static uzfs_command_t command_table[] = {
	{ "create-zpool",	uzfs_zpool_create, 	HELP_ZPOOL_CREATE   },
	{ "destroy-zpool",	uzfs_zpool_destroy, 	HELP_ZPOOL_DESTROY  },
	{ "import-zpool",	uzfs_zpool_import,	HELP_ZPOOL_IMPORT},
	{ "export-zpool",	uzfs_zpool_export,	HELP_ZPOOL_EXPORT},
	{ "set-zpool",		uzfs_zpool_set, 	HELP_ZPOOL_SET	},
	{ "get-zpool",		uzfs_zpool_get, 	HELP_ZPOOL_GET	},
	{ "create-dataset",	uzfs_dataset_create, 	HELP_DATASET_CREATE },
	{ "destroy-dataset",	uzfs_dataset_destroy, 	HELP_DATASET_DESTROY},
	{ "getsbino-dataset",	uzfs_dataset_get_sbino,	HELP_DATASET_GET_SBINO},
	{ "create-objects",	uzfs_objects_create, 	HELP_OBJECT_CREATE  },
	{ "delete-object",	uzfs_object_delete, 	HELP_OBJECT_DELETE  },
	{ "claim-object",	uzfs_object_claim, 	HELP_OBJECT_CLAIM   },
	{ "get-gen-object",	uzfs_object_get_gen, 	HELP_OBJECT_GET_GEN },
	{ "get-size-object",	uzfs_object_get_size, 	HELP_OBJECT_GET_SIZE },
	{ "stat-object",	uzfs_object_stat, 	HELP_OBJECT_STAT    },
	{ "list-object",	uzfs_object_list, 	HELP_OBJECT_LIST    },
	{ "read-object",	uzfs_object_read, 	HELP_OBJECT_READ    },
	{ "write-object",	uzfs_object_write, 	HELP_OBJECT_WRITE   },
	{ "sync-object",	uzfs_object_sync, 	HELP_OBJECT_SYNC    },
	{ "trunc-object",	uzfs_object_truncate, 	HELP_OBJECT_TRUNC   },
	{ "perf-object",	uzfs_object_perf, 	HELP_OBJECT_PERF    },
	{ "create-zap",		uzfs_zap_create, 	HELP_ZAP_CREATE	},
	{ "delete-zap",		uzfs_zap_delete, 	HELP_ZAP_DELETE },
	{ "add-zap",		uzfs_zap_add,		HELP_ZAP_ADD	},
	{ "remove-zap",		uzfs_zap_remove, 	HELP_ZAP_REMOVE	},
	{ "update-zap",		uzfs_zap_update, 	HELP_ZAP_UPDATE	},
	{ "lookup-zap",		uzfs_zap_lookup, 	HELP_ZAP_LOOKUP	},
	{ "count-zap",		uzfs_zap_count, 	HELP_ZAP_COUNT	},
	{ "create-fs",		uzfs_fs_create, 	HELP_FS_CREATE	},
	{ "destroy-fs",		uzfs_fs_destroy, 	HELP_FS_DESTROY	},
	{ "stat",		uzfs_stat,		HELP_STAT	},
	{ "mkdir",		uzfs_mkdir,		HELP_MKDIR	},
	{ "create",		uzfs_create,		HELP_CREATE	},
	{ "rm",			uzfs_rm,		HELP_RM		},
	{ "ls",			uzfs_ls,		HELP_LS		},
	{ "mv",			uzfs_mv,		HELP_MV		},
	{ "read",		uzfs_read,		HELP_READ	},
	{ "write",		uzfs_write,		HELP_WRITE	},
	{ "fsync",		uzfs_fsync,		HELP_FSYNC	},
	{ "perf",		uzfs_perf,		HELP_PERF	},
	{ "create-inode",	uzfs_inode_create, 	HELP_INODE_CREATE    },
	{ "delete-inode",	uzfs_inode_delete, 	HELP_INODE_DELETE    },
	{ "getattr-inode",	uzfs_inode_getattr, 	HELP_INODE_GETATTR   },
	{ "setattr-inode",	uzfs_inode_setattr, 	HELP_INODE_SETATTR   },
	{ "getkvobj-inode",	uzfs_inode_get_kvobj, 	HELP_INODE_GET_KVOBJ },
	{ "getkvattr-inode",	uzfs_inode_get_kvattr, 	HELP_INODE_GET_KVATTR},
	{ "setkvattr-inode",	uzfs_inode_set_kvattr, 	HELP_INODE_SET_KVATTR},
	{ "rmkvattr-inode",	uzfs_inode_rm_kvattr, 	HELP_INODE_RM_KVATTR },
	{ "create-dentry",	uzfs_dentry_create, 	HELP_DENTRY_CREATE   },
	{ "delete-dentry",	uzfs_dentry_delete, 	HELP_DENTRY_DELETE   },
	{ "lookup-dentry",	uzfs_dentry_lookup, 	HELP_DENTRY_LOOKUP   },
	{ "list-dentry",	uzfs_dentry_list, 	HELP_DENTRY_LIST  },
	{ "attr-test",		uzfs_attr_random_test,	HELP_ATTR_TEST	},
	{ "object-test",	uzfs_object_test, 	HELP_OBJECT_TEST},
	{ "io-bench",		uzfs_io_bench,		HELP_IO_BENCH},
	{ "expand-dataset",	uzfs_dataset_expand,	HELP_DATASET_EXPAND},
};

#define	NCOMMAND	(sizeof (command_table) / sizeof (command_table[0]))

uzfs_command_t *current_command;

static const char *
get_usage(uzfs_help_t idx)
{
	switch (idx) {
	case HELP_ZPOOL_CREATE:
		return (gettext("\tcreate-zpool ...\n"));
	case HELP_ZPOOL_DESTROY:
		return (gettext("\tdestroy-zpool ...\n"));
	case HELP_ZPOOL_SET:
		return (gettext("\tset-zpool ...\n"));
	case HELP_ZPOOL_GET:
		return (gettext("\tget-zpool ...\n"));
	case HELP_DATASET_CREATE:
		return (gettext("\tcreate-dataset ...\n"));
	case HELP_DATASET_DESTROY:
		return (gettext("\tdestroy-dataset ...\n"));
	case HELP_DATASET_GET_SBINO:
		return (gettext("\tgetsbino-dataset ...\n"));
	case HELP_OBJECT_CREATE:
		return (gettext("\tcreate-objects ...\n"));
	case HELP_OBJECT_DELETE:
		return (gettext("\tdelete-object ...\n"));
	case HELP_OBJECT_GET_GEN:
		return (gettext("\tget-gen-object ...\n"));
	case HELP_OBJECT_GET_SIZE:
		return (gettext("\tget-size-object ...\n"));
	case HELP_OBJECT_STAT:
		return (gettext("\tstat-object ...\n"));
	case HELP_OBJECT_LIST:
		return (gettext("\tlist-object ...\n"));
	case HELP_OBJECT_READ:
		return (gettext("\tread-object ...\n"));
	case HELP_OBJECT_WRITE:
		return (gettext("\twrite-object ...\n"));
	case HELP_OBJECT_SYNC:
		return (gettext("\tsync-object ...\n"));
	case HELP_OBJECT_TRUNC:
		return (gettext("\ttrunc-object ...\n"));
	case HELP_OBJECT_PERF:
		return (gettext("\tperf-object ...\n"));
	case HELP_OBJECT_CLAIM:
		return (gettext("\tclaim-object ...\n"));
	case HELP_ZAP_CREATE:
		return (gettext("\tcreate-zap ...\n"));
	case HELP_ZAP_DELETE:
		return (gettext("\tdelete-zap ...\n"));
	case HELP_ZAP_ADD:
		return (gettext("\tadd-zap ...\n"));
	case HELP_ZAP_REMOVE:
		return (gettext("\tremove-zap ...\n"));
	case HELP_ZAP_UPDATE:
		return (gettext("\tupdate-zap ...\n"));
	case HELP_ZAP_LOOKUP:
		return (gettext("\tlookup-zap ...\n"));
	case HELP_ZAP_COUNT:
		return (gettext("\tcount-zap ...\n"));
	case HELP_FS_CREATE:
		return (gettext("\tcreate-fs ...\n"));
	case HELP_FS_DESTROY:
		return (gettext("\tdestroy-fs ...\n"));
	case HELP_STAT:
		return (gettext("\tstat ...\n"));
	case HELP_MKDIR:
		return (gettext("\tmkdir ...\n"));
	case HELP_CREATE:
		return (gettext("\tcreate ...\n"));
	case HELP_RM:
		return (gettext("\trm ...\n"));
	case HELP_LS:
		return (gettext("\tls ...\n"));
	case HELP_MV:
		return (gettext("\tmv ...\n"));
	case HELP_READ:
		return (gettext("\tread ...\n"));
	case HELP_WRITE:
		return (gettext("\twrite ...\n"));
	case HELP_FSYNC:
		return (gettext("\tfsync ...\n"));
	case HELP_PERF:
		return (gettext("\tperf ...\n"));
	case HELP_INODE_CREATE:
		return (gettext("\tcreate-inode ...\n"));
	case HELP_INODE_DELETE:
		return (gettext("\tdelete-inode ...\n"));
	case HELP_INODE_GETATTR:
		return (gettext("\tgetattr-inode ...\n"));
	case HELP_INODE_SETATTR:
		return (gettext("\tsetattr-inode ...\n"));
	case HELP_INODE_GET_KVOBJ:
		return (gettext("\tgetkvobj-inode ...\n"));
	case HELP_INODE_GET_KVATTR:
		return (gettext("\tgetkvattr-inode ...\n"));
	case HELP_INODE_SET_KVATTR:
		return (gettext("\tsetkvattr-inode ...\n"));
	case HELP_INODE_RM_KVATTR:
		return (gettext("\trmkvattr-inode ...\n"));
	case HELP_DENTRY_CREATE:
		return (gettext("\tcreate-dentry ...\n"));
	case HELP_DENTRY_DELETE:
		return (gettext("\tdelete-dentry ...\n"));
	case HELP_DENTRY_LOOKUP:
		return (gettext("\tlookup-dentry ...\n"));
	case HELP_DENTRY_LIST:
		return (gettext("\tlist-dentry ...\n"));
	case HELP_ATTR_TEST:
		return (gettext("\tattr-test ...\n"));
	case HELP_ZPOOL_IMPORT:
		return (gettext("\timport-zpool ...\n"));
	case HELP_ZPOOL_EXPORT:
		return (gettext("\texport-zpool ...\n"));
	default:
		__builtin_unreachable();
	}
}


static int
uzfs_dataset_expand(int argc, char **argv)
{
	char *dsname = argv[1];
	libuzfs_dataset_handle_t *dhp = libuzfs_dataset_open(dsname);
	if (dhp == NULL) {
		printf("open dataset %s failed\n", dsname);
		return (-1);
	}

	int err = libuzfs_dataset_expand(dhp);
	printf("expand dataset %s, res: %d\n", dsname, err);

	libuzfs_dataset_close(dhp);
	return (0);
}

/*
 * Display usage message.  If we're inside a command, display only the usage for
 * that command.  Otherwise, iterate over the entire command table and display
 * a complete usage message.
 */
static void
usage(boolean_t requested)
{
	int i;
	FILE *fp = requested ? stdout : stderr;

	if (current_command == NULL) {
		(void) fprintf(fp, gettext("usage: uzfs command args ...\n"));
		(void) fprintf(fp,
		    gettext("where 'command' is one of the following:\n\n"));

		for (i = 0; i < NCOMMAND; i++) {
			if (command_table[i].name == NULL)
				(void) fprintf(fp, "\n");
			else
				(void) fprintf(fp, "%s",
				    get_usage(command_table[i].usage));
		}

		(void) fprintf(fp, gettext("\nEach dataset is of the form: "
		    "pool/[dataset/]*dataset[@name]\n"));
	} else {
		(void) fprintf(fp, gettext("usage:\n"));
		(void) fprintf(fp, "%s", get_usage(current_command->usage));
	}

	/*
	 * See comments at end of main().
	 */
	if (getenv("ZFS_ABORT") != NULL) {
		(void) printf("dumping core by request\n");
		abort();
	}

	exit(requested ? 0 : 2);
}

static void print_stat(const char *name, struct stat *stat)
{
	const char *format =
	    "  File: %s(%s)\n"
	    "  Inode: %lld\n"
	    "  MODE: %x\n"
	    "  Links: %lld\n"
	    "  UID/GID: %d/%d\n"
	    "  SIZE: %d\n"
	    "  BLOCKSIZE: %d\n"
	    "  BLOCKS: %d\n";

	const char *type = S_ISDIR(stat->st_mode) ? "DIR" : "REG FILE";

	printf(format, name, type, stat->st_ino, stat->st_mode, stat->st_nlink,
	    stat->st_uid, stat->st_gid, stat->st_size, stat->st_blksize,
	    stat->st_blocks);
}

static void
print_stat_sa(const char *name, uzfs_attr_t *stat)
{
	const char *format =
	    "ino: %lu\n"
	    "pino: %lu\n"
	    "psid: %u\n"
	    "ftype: %d\n"
	    "gen: %lu\n"
	    "nlink: %u\n"
	    "perm: %u\n"
	    "uid: %u\n"
	    "gid: %u\n"
	    "size: %lu\n"
	    "blksize: %lu\n"
	    "blocks: %lu\n"
	    "nsid: %lu\n"
	    "atime: (%lu, %lu)\n"
	    "mtime: (%lu, %lu)\n"
	    "ctime: (%lu, %lu)\n"
	    "btime: (%lu, %lu)\n";

	printf(format, stat->ino, stat->pino, stat->psid,
	    stat->ftype, stat->gen, stat->nlink, stat->perm,
	    stat->uid, stat->gid, stat->size, stat->blksize,
	    stat->blocks, stat->nsid, stat->atime.tv_sec,
	    stat->atime.tv_nsec, stat->mtime.tv_sec,
	    stat->mtime.tv_nsec, stat->ctime.tv_sec,
	    stat->ctime.tv_nsec, stat->btime.tv_sec,
	    stat->btime.tv_nsec);
}

static int
find_command_idx(char *command, int *idx)
{
	int i;

	for (i = 0; i < NCOMMAND; i++) {
		if (command_table[i].name == NULL)
			continue;

		if (strcmp(command, command_table[i].name) == 0) {
			*idx = i;
			return (0);
		}
	}
	return (-1);
}

int
main(int argc, char **argv)
{
	int error = 0;
	int i = 0;
	char *cmdname;
	char **newargv;

	(void) setlocale(LC_ALL, "");
	(void) setlocale(LC_NUMERIC, "C");
	(void) textdomain(TEXT_DOMAIN);

	opterr = 0;

	/*
	 * Make sure the user has specified some command.
	 */
	if (argc < 2) {
		(void) fprintf(stderr, gettext("missing command\n"));
		usage(B_FALSE);
	}

	dprintf_setup(&argc, argv);

	cmdname = argv[1];

	libuzfs_set_zpool_cache_path("/tmp/zpool.cache");

	libuzfs_init();

	/*
	 * Many commands modify input strings for string parsing reasons.
	 * We create a copy to protect the original argv.
	 */
	newargv = malloc((argc + 1) * sizeof (newargv[0]));
	for (i = 0; i < argc; i++)
		newargv[i] = strdup(argv[i]);
	newargv[argc] = NULL;

	/*
	 * Run the appropriate command.
	 */
	if (find_command_idx(cmdname, &i) == 0) {
		current_command = &command_table[i];
		error = command_table[i].func(argc - 1, newargv + 1);
	} else if (strchr(cmdname, '=') != NULL) {
		verify(find_command_idx("set", &i) == 0);
		current_command = &command_table[i];
		error = command_table[i].func(argc, newargv);
	} else {
		(void) fprintf(stderr, gettext("unrecognized "
		    "command '%s'\n"), cmdname);
		usage(B_FALSE);
		error = 1;
	}

	for (i = 0; i < argc; i++)
		free(newargv[i]);
	free(newargv);

	libuzfs_fini();

	/*
	 * The 'ZFS_ABORT' environment variable causes us to dump core on exit
	 * for the purposes of running ::findleaks.
	 */
	if (getenv("ZFS_ABORT") != NULL) {
		(void) printf("dumping core by request\n");
		abort();
	}

	return (error);
}

int
uzfs_zpool_create(int argc, char **argv)
{
	int err = 0;
	char *zpool = argv[1];
	char *path = argv[2];

	printf("creating zpool %s, devpath: %s\n", zpool, path);

	err = libuzfs_zpool_create(zpool, path, NULL, NULL);
	if (err)
		printf("failed to create zpool: %s, path: %s\n", zpool, path);

	return (err);
}

int
uzfs_zpool_destroy(int argc, char **argv)
{
	int err = 0;
	char *zpool = argv[1];

	printf("destroying zpool %s\n", zpool);

	err = libuzfs_zpool_destroy(zpool);
	if (err)
		printf("failed to destroy zpool: %s\n", zpool);

	return (err);
}

int
uzfs_zpool_set(int argc, char **argv)
{
	char *zpool = argv[1];
	char *prop_name = argv[2];
	uint64_t value = atoll(argv[3]);

	printf("setting zpool %s, %s=%lu\n", zpool, prop_name, value);

	libuzfs_zpool_handle_t *zhp;
	zhp = libuzfs_zpool_open(zpool);
	if (!zhp) {
		printf("failed to open zpool: %s\n", zpool);
		return (-1);
	}

	zpool_prop_t prop = zpool_name_to_prop(prop_name);
	libuzfs_zpool_prop_set(zhp, prop, value);

	libuzfs_zpool_close(zhp);

	return (0);
}

int
uzfs_zpool_get(int argc, char **argv)
{
	int err = 0;
	char *zpool = argv[1];
	char *prop_name = argv[2];

	printf("getting zpool %s, %s\n", zpool, prop_name);

	libuzfs_zpool_handle_t *zhp;
	zhp = libuzfs_zpool_open(zpool);
	if (!zhp) {
		printf("failed to open zpool: %s\n", zpool);
		return (-1);
	}

	uint64_t value = 0;
	zpool_prop_t prop = zpool_name_to_prop(prop_name);
	err = libuzfs_zpool_prop_get(zhp, prop, &value);
	if (err)
		printf("failed to get pool: %s, prop: %s\n", zpool, prop_name);
	else
		printf("prop: %s=%ld\n", prop_name, value);

	libuzfs_zpool_close(zhp);

	return (err);
}

int
uzfs_zpool_import(int argc, char **argv)
{
	char *dev_path = argv[1];
	const int max_pool_name_len = 256;
	char pool_name[max_pool_name_len];
	pool_name[0] = 0;
	int err = libuzfs_zpool_import(dev_path, pool_name, max_pool_name_len);

	printf("import zpool, dev_path: %s, result: %d, pool_name: %s\n",
	    dev_path, err, pool_name);

	return (err);
}

int
uzfs_zpool_export(int argc, char **argv)
{
	char *pool_name = argv[1];
	int err = libuzfs_zpool_export(pool_name);

	printf("export zpool: %s, result: %d\n", pool_name, err);

	return (err);
}

int
uzfs_dataset_create(int argc, char **argv)
{
	int err = 0;
	char *dsname = argv[1];

	printf("creating dataset %s\n", dsname);

	err = libuzfs_dataset_create(dsname);
	if (err)
		printf("failed to create dataset: %s\n", dsname);

	return (err);
}

int
uzfs_dataset_destroy(int argc, char **argv)
{
	char *dsname = argv[1];

	printf("destroying dataset %s\n", dsname);

	libuzfs_dataset_destroy(dsname);

	return (0);
}

int
uzfs_dataset_get_sbino(int argc, char **argv)
{
	int err = 0;
	char *dsname = argv[1];

	printf("getting sb ino for dataset %s\n", dsname);

	libuzfs_dataset_handle_t *dhp = libuzfs_dataset_open(dsname);
	if (!dhp) {
		printf("failed to open dataset: %s\n", dsname);
		return (-1);
	}

	uint64_t sbino = 0;
	err = libuzfs_dataset_get_superblock_ino(dhp, &sbino);

	if (err)
		printf("failed to get sb ino for dataset: %s\n", dsname);
	else
		printf("sbino: %ld, dataset: %s\n", sbino, dsname);

	libuzfs_dataset_close(dhp);

	return (0);
}

int
uzfs_objects_create(int argc, char **argv)
{
	int err = 0;
	char *dsname = argv[1];
	int nobjs = atoi(argv[2]);

	printf("creating object %s\n", dsname);

	libuzfs_dataset_handle_t *dhp = libuzfs_dataset_open(dsname);
	if (!dhp) {
		printf("failed to open dataset: %s\n", dsname);
		return (-1);
	}

	uint64_t *objs = umem_alloc(sizeof (uint64_t) *nobjs, UMEM_NOFAIL);
	uint64_t gen = 0;

	err = libuzfs_objects_create(dhp, objs, nobjs, &gen);
	if (err)
		printf("failed to create object on dataset: %s\n", dsname);
	else {
		printf("created %d objects: ", nobjs);
		for (int i = 0; i < nobjs; ++i) {
			printf("%lu ", objs[i]);
		}
		printf("\n");
	}

	libuzfs_dataset_close(dhp);
	umem_free(objs, sizeof (uint64_t) *nobjs);

	return (err);
}

int
uzfs_object_delete(int argc, char **argv)
{
	int err = 0;
	char *dsname = argv[1];
	uint64_t obj = atoll(argv[2]);

	printf("destroying object %s:%ld\n", dsname, obj);

	libuzfs_dataset_handle_t *dhp = libuzfs_dataset_open(dsname);
	if (!dhp) {
		printf("failed to open dataset: %s\n", dsname);
		return (-1);
	}

	err = libuzfs_object_delete(dhp, obj);
	if (err)
		printf("failed to delete object: %s:%ld\n", dsname, obj);

	libuzfs_dataset_close(dhp);
	return (0);
}

int
uzfs_object_claim(int argc, char **argv)
{
	int err = 0;
	char *dsname = argv[1];
	uint64_t obj = atoll(argv[2]);

	printf("claiming object %s:%ld\n", dsname, obj);

	libuzfs_dataset_handle_t *dhp = libuzfs_dataset_open(dsname);
	if (!dhp) {
		printf("failed to open dataset: %s\n", dsname);
		return (-1);
	}

	err = libuzfs_object_claim(dhp, obj);
	if (err)
		printf("failed to claim object on dataset: %s\n", dsname);

	libuzfs_dataset_close(dhp);

	return (err);
}

int
uzfs_object_get_gen(int argc, char **argv)
{
	int err = 0;
	char *dsname = argv[1];
	uint64_t obj = atoll(argv[2]);

	printf("getting gen of object %s:%ld\n", dsname, obj);

	libuzfs_dataset_handle_t *dhp = libuzfs_dataset_open(dsname);
	if (!dhp) {
		printf("failed to open dataset: %s\n", dsname);
		return (-1);
	}

	uint64_t gen = 0;
	err = libuzfs_object_get_gen(dhp, obj, &gen);
	if (err)
		printf("failed to get gen of object: %s:%ld\n", dsname, obj);
	else
		printf("got object: %s:%ld, gen: %ld\n", dsname, obj, gen);

	libuzfs_dataset_close(dhp);
	return (0);
}

int
uzfs_object_get_size(int argc, char **argv)
{
	int err = 0;
	char *dsname = argv[1];
	uint64_t obj = atoll(argv[2]);

	printf("getting gen of object %s:%ld\n", dsname, obj);

	libuzfs_dataset_handle_t *dhp = libuzfs_dataset_open(dsname);
	if (!dhp) {
		printf("failed to open dataset: %s\n", dsname);
		return (-1);
	}

	uint64_t size = 0;
	err = libuzfs_object_get_size(dhp, obj, &size);
	if (err)
		printf("failed to get gen of object: %s:%ld\n", dsname, obj);
	else
		printf("got object: %s:%ld, size: %ld\n", dsname, obj, size);

	libuzfs_dataset_close(dhp);
	return (0);
}

static char *
uzfs_ot_name(dmu_object_type_t type)
{
	if (type < DMU_OT_NUMTYPES)
		return (dmu_ot[type].ot_name);
	else if ((type & DMU_OT_NEWTYPE) &&
	    ((type & DMU_OT_BYTESWAP_MASK) < DMU_BSWAP_NUMFUNCS))
		return (dmu_ot_byteswap[type & DMU_OT_BYTESWAP_MASK].ob_name);
	else
		return ("UNKNOWN");
}

static void
uzfs_dump_doi(uint64_t object, dmu_object_info_t *doi)
{
	printf("object: %ld\n", object);
	printf("\tdata_block_size: %d\n", doi->doi_data_block_size);
	printf("\tmetadata_block_size: %d\n", doi->doi_metadata_block_size);
	printf("\ttype: %s\n", uzfs_ot_name(doi->doi_type));
	printf("\tbonus_type: %s\n", uzfs_ot_name(doi->doi_bonus_type));
	printf("\tbonus_size: %ld\n", doi->doi_bonus_size);
	printf("\tindirection: %d\n", doi->doi_indirection);
	printf("\tchecksum: %d\n", doi->doi_checksum);
	printf("\tcompress: %d\n", doi->doi_compress);
	printf("\tnblkptr: %d\n", doi->doi_nblkptr);
	printf("\tdnodesize: %ld\n", doi->doi_dnodesize);
	printf("\tphysical_blocks_512: %ld\n", doi->doi_physical_blocks_512);
	printf("\tmax_offset: %ld\n", doi->doi_max_offset);
	printf("\tfill_count: %ld\n", doi->doi_fill_count);
}

int
uzfs_object_stat(int argc, char **argv)
{
	int err = 0;
	char *dsname = argv[1];
	uint64_t obj = atoll(argv[2]);

	printf("stating object %s:%ld\n", dsname, obj);

	libuzfs_dataset_handle_t *dhp = libuzfs_dataset_open(dsname);
	if (!dhp) {
		printf("failed to open dataset: %s\n", dsname);
		return (-1);
	}

	dmu_object_info_t doi;
	memset(&doi, 0, sizeof (doi));

	err = libuzfs_object_stat(dhp, obj, &doi);
	if (err)
		printf("failed to stat object: %s:%ld\n", dsname, obj);
	else
		uzfs_dump_doi(obj, &doi);


	libuzfs_dataset_close(dhp);
	return (0);
}

int
uzfs_object_list(int argc, char **argv)
{
	char *dsname = argv[1];

	printf("listing objects in %s\n", dsname);

	libuzfs_dataset_handle_t *dhp = libuzfs_dataset_open(dsname);
	if (!dhp) {
		printf("failed to open dataset: %s\n", dsname);
		return (-1);
	}

	libuzfs_object_list(dhp);

	libuzfs_dataset_close(dhp);
	return (0);
}

int
uzfs_object_read(int argc, char **argv)
{
	int res = 0;
	char *dsname = argv[1];
	uint64_t obj = atoll(argv[2]);
	int offset = atoi(argv[3]);
	int size = atoi(argv[4]);
	char *buf = NULL;

	printf("reading %s: %ld, off: %d, size: %d\n", dsname, obj, offset,
	    size);

	libuzfs_dataset_handle_t *dhp = libuzfs_dataset_open(dsname);
	if (!dhp) {
		printf("failed to open dataset: %s\n", dsname);
		return (-1);
	}

	buf = umem_zalloc(size + 1, UMEM_NOFAIL);

	res = libuzfs_object_read(dhp, obj, offset, size, buf);
	if (res < 0)
		printf("failed to read object: %s:%ld, err: %d\n",
		    dsname, obj, res);
	else
		printf("read %s: %ld, off: %d, size: %d\n%s\n", dsname, obj,
		    offset, res, buf);

	umem_free(buf, size + 1);
	libuzfs_dataset_close(dhp);
	return (0);
}

static int
verify_data(libuzfs_dataset_handle_t *dhp, uint64_t obj, int size, int offset,
    char *buf)
{
	char *data = umem_zalloc(size, UMEM_NOFAIL);
	VERIFY3U(libuzfs_object_read(dhp, obj, offset, size, data), >=, 0);
	int rc =  memcmp(buf, data, size);
	umem_free(data, size);
	return (rc);
}

static void
pattern_get_bytes(char *buf, int size)
{
	char c = rand() % 26 + 'a';
	memset(buf, c, size);
	printf("pattern: %c, size: %d\n", c, size);
}

// TODO(hping): test zil functionality with crash scenario
int
uzfs_object_write(int argc, char **argv)
{
	int err = 0;
	char *dsname = argv[1];
	uint64_t obj = atoll(argv[2]);
	int offset = atoi(argv[3]);
	int size = atoi(argv[4]);
	char *buf = NULL;

	printf("writing %s: %ld, off: %d, size: %d\n", dsname, obj, offset,
	    size);

	libuzfs_dataset_handle_t *dhp = libuzfs_dataset_open(dsname);
	if (!dhp) {
		printf("failed to open dataset: %s\n", dsname);
		return (-1);
	}

	buf = umem_alloc(size, UMEM_NOFAIL);

	random_get_bytes((uint8_t *)buf, size);

	err = libuzfs_object_write(dhp, obj, offset, size, buf, B_FALSE);
	if (err)
		printf("failed to write object: %s:%ld\n", dsname, obj);

	VERIFY0(verify_data(dhp, obj, size, offset, buf));

	srand(time(NULL));
	pattern_get_bytes(buf, size);

	err = libuzfs_object_write(dhp, obj, offset, size, buf, B_TRUE);
	if (err)
		printf("failed to write object: %s:%ld\n", dsname, obj);

	VERIFY0(verify_data(dhp, obj, size, offset, buf));

	umem_free(buf, size);
	libuzfs_dataset_close(dhp);
	return (0);
}

int
uzfs_object_sync(int argc, char **argv)
{
	char *dsname = argv[1];
	uint64_t obj = atoll(argv[2]);
	int offset = atoi(argv[3]);
	int size = atoi(argv[4]);
	char *buf = NULL;
	int err = 0;

	printf("syncing %s: %ld\n", dsname, obj);

	libuzfs_dataset_handle_t *dhp = libuzfs_dataset_open(dsname);
	if (!dhp) {
		printf("failed to open dataset: %s\n", dsname);
		return (-1);
	}

	buf = umem_alloc(size, UMEM_NOFAIL);

	srand(time(NULL));
	pattern_get_bytes(buf, size);

	err = libuzfs_object_write(dhp, obj, offset, size, buf, B_TRUE);
	if (err)
		printf("failed to write object: %s:%ld\n", dsname, obj);

	VERIFY0(verify_data(dhp, obj, size, offset, buf));

	pattern_get_bytes(buf, size);

	err = libuzfs_object_write(dhp, obj, offset, size, buf, B_FALSE);
	if (err)
		printf("failed to write object: %s:%ld\n", dsname, obj);

	VERIFY0(verify_data(dhp, obj, size, offset, buf));

	umem_free(buf, size);

	libuzfs_object_sync(dhp, obj);

	ASSERT(0);

	libuzfs_dataset_close(dhp);
	return (0);
}

int
uzfs_object_truncate(int argc, char **argv)
{
	int err = 0;
	char *dsname = argv[1];
	uint64_t obj = atoll(argv[2]);
	int offset = atoi(argv[3]);
	int size = atoi(argv[4]);

	printf("truncating %s: %ld, off: %d, size: %d\n", dsname, obj, offset,
	    size);

	libuzfs_dataset_handle_t *dhp = libuzfs_dataset_open(dsname);
	if (!dhp) {
		printf("failed to open dataset: %s\n", dsname);
		return (-1);
	}

	err = libuzfs_object_truncate(dhp, obj, offset, size);
	if (err)
		printf("failed to truncate object: %s:%ld\n", dsname, obj);

	libuzfs_dataset_close(dhp);
	return (0);
}

int
uzfs_zap_create(int argc, char **argv)
{
	int err = 0;
	char *dsname = argv[1];

	printf("creating zap object %s\n", dsname);

	libuzfs_dataset_handle_t *dhp = libuzfs_dataset_open(dsname);
	if (!dhp) {
		printf("failed to open dataset: %s\n", dsname);
		return (-1);
	}

	uint64_t obj = 0;
	uint64_t txg = 0;

	err = libuzfs_zap_create(dhp, &obj, &txg);
	if (err)
		printf("failed to create zap object on dataset: %s\n", dsname);
	else
		printf("created zap object %s:%ld\n", dsname, obj);

	libuzfs_dataset_close(dhp);

	return (err);
}

int
uzfs_zap_delete(int argc, char **argv)
{
	int err = 0;
	char *dsname = argv[1];
	uint64_t obj = atoll(argv[2]);

	printf("destroying zap object %s:%ld\n", dsname, obj);

	libuzfs_dataset_handle_t *dhp = libuzfs_dataset_open(dsname);
	if (!dhp) {
		printf("failed to open dataset: %s\n", dsname);
		return (-1);
	}

	uint64_t txg = 0;

	err = libuzfs_zap_delete(dhp, obj, &txg);
	if (err)
		printf("failed to delete object: %s:%ld\n", dsname, obj);

	libuzfs_dataset_close(dhp);
	return (0);
}

int
uzfs_zap_add(int argc, char **argv)
{
	int err = 0;
	char *dsname = argv[1];
	uint64_t obj = atoll(argv[2]);
	char *key = argv[3];
	uint64_t value = atoll(argv[4]);

	printf("add entry %s:%ld to zap object %s:%ld\n",
	    key, value, dsname, obj);

	libuzfs_dataset_handle_t *dhp = libuzfs_dataset_open(dsname);
	if (!dhp) {
		printf("failed to open dataset: %s\n", dsname);
		return (-1);
	}

	uint64_t txg = 0;

	err = libuzfs_zap_add(dhp, obj, key, 8, 1, &value, &txg);
	if (err)
		printf("failed to add entry to zap object: %s:%ld\n",
		    dsname, obj);

	libuzfs_dataset_close(dhp);
	return (0);
}

int
uzfs_zap_remove(int argc, char **argv)
{
	int err = 0;
	char *dsname = argv[1];
	uint64_t obj = atoll(argv[2]);
	char *key = argv[3];

	printf("remove entry %s from zap object %s:%ld\n",
	    key, dsname, obj);

	libuzfs_dataset_handle_t *dhp = libuzfs_dataset_open(dsname);
	if (!dhp) {
		printf("failed to open dataset: %s\n", dsname);
		return (-1);
	}

	uint64_t txg = 0;

	err = libuzfs_zap_remove(dhp, obj, key, &txg);
	if (err)
		printf("failed to remove entry from zap object: %s:%ld\n",
		    dsname, obj);

	libuzfs_dataset_close(dhp);
	return (0);
}

int
uzfs_zap_update(int argc, char **argv)
{
	int err = 0;
	char *dsname = argv[1];
	uint64_t obj = atoll(argv[2]);
	char *key = argv[3];
	uint64_t value = atoll(argv[4]);

	printf("update entry %s:%ld to zap object %s:%ld\n",
	    key, value, dsname, obj);

	libuzfs_dataset_handle_t *dhp = libuzfs_dataset_open(dsname);
	if (!dhp) {
		printf("failed to open dataset: %s\n", dsname);
		return (-1);
	}

	uint64_t txg = 0;

	err = libuzfs_zap_update(dhp, obj, key, 8, 1, &value, &txg);
	if (err)
		printf("failed to update entry to zap object: %s:%ld\n",
		    dsname, obj);

	libuzfs_dataset_close(dhp);
	return (0);
}

int
uzfs_zap_lookup(int argc, char **argv)
{
	int err = 0;
	char *dsname = argv[1];
	uint64_t obj = atoll(argv[2]);
	char *key = argv[3];
	uint64_t value = 0;

	printf("lookup entry %s from zap object %s:%ld\n", key, dsname, obj);

	libuzfs_dataset_handle_t *dhp = libuzfs_dataset_open(dsname);
	if (!dhp) {
		printf("failed to open dataset: %s\n", dsname);
		return (-1);
	}

	err = libuzfs_zap_lookup(dhp, obj, key, 8, 1, &value);
	if (err)
		printf("failed to lookup entry to zap object: %s:%ld\n",
		    dsname, obj);
	else
		printf("lookup entry to zap object: %s:%ld, %s:%ld\n",
		    dsname, obj, key, value);

	libuzfs_dataset_close(dhp);
	return (0);
}

int
uzfs_zap_count(int argc, char **argv)
{
	int err = 0;
	char *dsname = argv[1];
	uint64_t obj = atoll(argv[2]);
	uint64_t count = 0;

	printf("count entry from zap object %s:%ld\n", dsname, obj);

	libuzfs_dataset_handle_t *dhp = libuzfs_dataset_open(dsname);
	if (!dhp) {
		printf("failed to open dataset: %s\n", dsname);
		return (-1);
	}

	err = libuzfs_zap_count(dhp, obj, &count);
	if (err)
		printf("failed to count entry to zap object: %s:%ld\n",
		    dsname, obj);
	else
		printf("zap object: %s:%ld, count: %ld\n", dsname, obj, count);

	libuzfs_dataset_close(dhp);
	return (0);
}

int
uzfs_inode_create(int argc, char **argv)
{
	int err = 0;
	char *dsname = argv[1];
	int type = atoi(argv[2]); // 0: file, 1: dir

	printf("creating inode %s, type: %d\n", dsname, type);

	libuzfs_dataset_handle_t *dhp = libuzfs_dataset_open(dsname);
	if (!dhp) {
		printf("failed to open dataset: %s\n", dsname);
		return (-1);
	}

	uint64_t obj = 0;
	uint64_t txg = 0;

	err = libuzfs_inode_create(dhp, &obj, type, &txg);
	if (err)
		printf("failed to create inode on dataset: %s\n", dsname);
	else
		printf("created inode %s:%ld\n", dsname, obj);

	libuzfs_dataset_close(dhp);

	return (err);
}

int
uzfs_inode_delete(int argc, char **argv)
{
	int err = 0;
	char *dsname = argv[1];
	uint64_t obj = atoll(argv[2]);
	int type = atoi(argv[3]); // 0: file, 1: dir

	printf("destroying inode %s:%ld, type: %d\n", dsname, obj, type);

	libuzfs_dataset_handle_t *dhp = libuzfs_dataset_open(dsname);
	if (!dhp) {
		printf("failed to open dataset: %s\n", dsname);
		return (-1);
	}

	uint64_t txg = 0;

	err = libuzfs_inode_delete(dhp, obj, type, &txg);
	if (err)
		printf("failed to delete inode: %s:%ld\n", dsname, obj);

	libuzfs_dataset_close(dhp);
	return (0);
}

int
uzfs_inode_getattr(int argc, char **argv)
{
	int err = 0;
	char *dsname = argv[1];
	uint64_t obj = atoll(argv[2]);

	printf("getting attr inode %s, obj: %ld\n", dsname, obj);

	libuzfs_dataset_handle_t *dhp = libuzfs_dataset_open(dsname);
	if (!dhp) {
		printf("failed to open dataset: %s\n", dsname);
		return (-1);
	}

	uzfs_attr_t buf;
	memset(&buf, 0, sizeof (buf));

	err = libuzfs_inode_getattr(dhp, obj, &buf);
	if (err)
		printf("failed to get attr inode %ld on dataset: %s\n",
		    obj, dsname);
	else
		print_stat_sa(NULL, &buf);

	libuzfs_dataset_close(dhp);

	return (err);
}

int
uzfs_inode_setattr(int argc, char **argv)
{
	int err = 0;
	char *dsname = argv[1];
	uint64_t obj = atoll(argv[2]);

	printf("setting attr inode %s, obj: %ld\n", dsname, obj);

	libuzfs_dataset_handle_t *dhp = libuzfs_dataset_open(dsname);
	if (!dhp) {
		printf("failed to open dataset: %s\n", dsname);
		return (-1);
	}

	uzfs_attr_t buf;
	memset(&buf, 0, sizeof (buf));

	buf.ino = obj;
	buf.pino = 0;
	buf.psid = 0;
	buf.ftype = TYPE_FILE;
	buf.gen = 1;
	buf.nlink = 1;
	buf.perm = 0;
	buf.uid = 12358;
	buf.gid = 85321;
	buf.size = 0;
	buf.blksize = 65536;
	buf.blocks = 1;
	buf.nsid = 1;

	err = libuzfs_inode_setattr(dhp, obj, &buf, NULL);
	if (err)
		printf("failed to get attr inode %ld on dataset: %s\n",
		    obj, dsname);
	else
		print_stat_sa(NULL, &buf);

	libuzfs_dataset_close(dhp);

	return (err);
}

int
uzfs_inode_get_kvobj(int argc, char **argv)
{
	int err = 0;
	char *dsname = argv[1];
	uint64_t obj = atoll(argv[2]);

	printf("getting kvobj %s, obj: %ld\n", dsname, obj);

	libuzfs_dataset_handle_t *dhp = libuzfs_dataset_open(dsname);
	if (!dhp) {
		printf("failed to open dataset: %s\n", dsname);
		return (-1);
	}

	uint64_t kvobj = 0;
	err = libuzfs_inode_get_kvobj(dhp, obj, &kvobj);
	if (err)
		printf("failed to get kvattr inode %ld on dataset: %s\n",
		    obj, dsname);
	else
		printf("%s/%ld:: kvobj: %ld\n", dsname, obj, kvobj);

	libuzfs_dataset_close(dhp);

	return (err);
}

int
uzfs_inode_get_kvattr(int argc, char **argv)
{
	char *dsname = argv[1];
	uint64_t obj = atoll(argv[2]);
	char *key = argv[3];

	printf("getting kvattr inode %s, obj: %ld, key: %s\n",
	    dsname, obj, key);

	libuzfs_dataset_handle_t *dhp = libuzfs_dataset_open(dsname);
	if (!dhp) {
		printf("failed to open dataset: %s\n", dsname);
		return (-1);
	}

	char value[64] = {0};
	int rc = libuzfs_inode_get_kvattr(dhp, obj, key, value, 64, 0);
	if (rc < 0)
		printf("failed to get kvattr inode %ld, dataset: %s, key: %s\n",
		    obj, dsname, key);
	else
		printf("%s/%ld:: key: %s, value: %s\n",
		    dsname, obj, key, value);

	libuzfs_dataset_close(dhp);

	if (rc > 0) {
		return (0);
	}
	return (-rc);
}

int
uzfs_inode_set_kvattr(int argc, char **argv)
{
	int err = 0;
	char *dsname = argv[1];
	uint64_t obj = atoll(argv[2]);
	char *key = argv[3];
	char *value = argv[4];

	printf("setting kvattr inode %s, obj: %ld, key: %s, value: %s\n",
	    dsname, obj, key, value);

	libuzfs_dataset_handle_t *dhp = libuzfs_dataset_open(dsname);
	if (!dhp) {
		printf("failed to open dataset: %s\n", dsname);
		return (-1);
	}

	uint64_t txg = 0;
	err = libuzfs_inode_set_kvattr(dhp, obj, key, value, 64, 0, &txg);
	if (err)
		printf("failed to set attr inode %ld on dataset: %s\n",
		    obj, dsname);

	libuzfs_dataset_close(dhp);

	return (err);
}

int
uzfs_inode_rm_kvattr(int argc, char **argv)
{
	int err = 0;
	char *dsname = argv[1];
	uint64_t obj = atoll(argv[2]);
	char *key = argv[3];


	printf("removing kvattr inode %s, obj: %ld, key: %s\n",
	    dsname, obj, key);

	libuzfs_dataset_handle_t *dhp = libuzfs_dataset_open(dsname);
	if (!dhp) {
		printf("failed to open dataset: %s\n", dsname);
		return (-1);
	}

	uint64_t txg = 0;
	err = libuzfs_inode_remove_kvattr(dhp, obj, key, &txg);
	if (err)
		printf("failed to rm kvattr inode %ld on dataset: %s key: %s\n",
		    obj, dsname, key);

	libuzfs_dataset_close(dhp);

	return (err);
}

static int
uzfs_attr_cmp(uzfs_attr_t *lhs, uzfs_attr_t *rhs)
{
	return lhs->psid == rhs->psid && lhs->ftype == rhs->ftype&&
	    lhs->gen == rhs->gen && lhs->nlink == rhs->nlink &&
	    lhs->perm == rhs->perm && lhs->gid == rhs->gid &&
	    lhs->size == rhs->size && lhs->nsid == rhs->nsid &&
	    lhs->atime.tv_nsec == rhs->atime.tv_nsec &&
	    lhs->atime.tv_sec == rhs->atime.tv_sec &&
	    lhs->mtime.tv_nsec == rhs->mtime.tv_nsec &&
	    lhs->mtime.tv_sec == rhs->mtime.tv_sec &&
	    lhs->ctime.tv_nsec == rhs->ctime.tv_nsec &&
	    lhs->ctime.tv_sec == rhs->ctime.tv_sec &&
	    lhs->btime.tv_nsec == rhs->btime.tv_nsec &&
	    lhs->btime.tv_sec == rhs->btime.tv_sec;
}

static boolean_t
uzfs_attr_ops(libuzfs_dataset_handle_t *dhp, uint64_t *ino,
    libuzfs_inode_type_t *type, uzfs_attr_t *cur_attr,
    nvlist_t *nvl, boolean_t *reset)
{
	int delete_proportion = 1;
	int getkvattr_proportion = 2;
	int setkvattr_proportion = 4;
	int deletekvattr_proportion = 2;
	int listkvattr_proportion = 1;
	int getattr_proportion = 6;
	int setattr_proportion = 6;
	int total_proportion = delete_proportion + getkvattr_proportion +
	    setkvattr_proportion + deletekvattr_proportion +
	    listkvattr_proportion + getattr_proportion + setattr_proportion;

	int op = rand() % total_proportion;
	*reset = B_TRUE;
	if (op < delete_proportion) {
		// delete inode
		if (*ino != 0) {
			VERIFY0(libuzfs_inode_delete(dhp, *ino, *type, NULL));
			memset(cur_attr, 0, sizeof (*cur_attr));
			*ino = 0;
			*reset = B_TRUE;
		}
		return (B_TRUE);
	}
	op -= delete_proportion;

	if (*ino == 0) {
		*type = rand() % 2;
		VERIFY0(libuzfs_inode_create(dhp, ino, *type, NULL));
	}
	if (op < getkvattr_proportion) {
		// get all kvattr and check
		nvpair_t *elem = NULL;
		while ((elem = nvlist_next_nvpair(nvl, elem)) != NULL) {
			char *name = nvpair_name(elem);
			char *value = NULL;
			uint_t size;
			VERIFY0(nvpair_value_byte_array(elem,
			    (uchar_t **)(&value), &size));
			char *stored_value =
			    (char *)umem_alloc(size, UMEM_NOFAIL);
			ssize_t rc = libuzfs_inode_get_kvattr(dhp, *ino, name,
			    stored_value, (uint64_t)(size), 0);
			VERIFY3U(rc, ==, size);
			if (memcmp(value, stored_value, size) != 0) {
				return (B_FALSE);
			}
			umem_free(stored_value, size);
		}
		return (B_TRUE);
	}
	op -= getkvattr_proportion;

	if (op < setkvattr_proportion) {
		char *name = NULL;
		int name_size = 0;
		if (rand() % 4 < 1) {
			nvpair_t *elem = NULL;
			if ((elem = nvlist_next_nvpair(nvl, elem)) != NULL) {
				char *stored_name = nvpair_name(elem);
				name_size = strlen(stored_name);
				name = umem_alloc(name_size + 1, UMEM_NOFAIL);
				strcpy(name, stored_name);
				name[name_size] = '\0';
			}
		}
		if (name == NULL) {
			name_size = rand() % 50 + 1;
			name = umem_alloc(name_size + 1, UMEM_NOFAIL);
			for (int i = 0; i < name_size; ++i) {
				name[i] = rand() % 26 + 'a';
			}
			name[name_size] = '\0';
		}

		uint_t value_size = rand() % (UZFS_XATTR_MAXVALUELEN - 1) + 1;
		uchar_t *value = umem_alloc(value_size + 1, UMEM_NOFAIL);
		for (int i = 0; i < value_size; ++i) {
			value[i] = rand() % 26 + 'a';
		}
		value[value_size] = '\0';

		VERIFY0(libuzfs_inode_set_kvattr(dhp, *ino, name,
		    (char *)value, value_size + 1, 0, NULL));
		VERIFY0(nvlist_add_byte_array(nvl, name,
		    value, value_size + 1));
		umem_free(value, value_size + 1);
		if (name_size > 0) {
			umem_free(name, name_size + 1);
		}
		return (B_TRUE);
	}
	op -= setkvattr_proportion;

	if (op < deletekvattr_proportion) {
		// delete one kv_attr
		nvpair_t *elem = NULL;
		if ((elem = nvlist_next_nvpair(nvl, elem)) != NULL) {
			char *name = nvpair_name(elem);
			// printf("remove kvattr, name: %s\n", name);
			int err = libuzfs_inode_remove_kvattr(dhp,
			    *ino, name, NULL);
			if (err != 0) {
				printf("remove kvattr, name: %s, "
				    "failed, err: %d\n", name, err);
				return (B_FALSE);
			}
			VERIFY0(nvlist_remove(nvl, name, DATA_TYPE_BYTE_ARRAY));
		}
		return (B_TRUE);
	}
	op -= deletekvattr_proportion;

	if (op < listkvattr_proportion) {
		int err;
		libuzfs_kvattr_iterator_t *iter =
		    libuzfs_new_kvattr_iterator(dhp, *ino, &err);
		if (iter == NULL) {
			VERIFY3U(err, !=, 0);
			return (B_TRUE);
		}

		char buf[256];
		while (libuzfs_next_kvattr_name(iter, buf, 256) > 0) {
			VERIFY(nvlist_exists(nvl, buf));
		}
		libuzfs_kvattr_iterator_fini(iter);
		return (B_TRUE);
	}
	op -= listkvattr_proportion;

	if (op < getattr_proportion) {
		// get attr
		uzfs_attr_t stored_attr;
		VERIFY0(libuzfs_inode_getattr(dhp, *ino,
		    &stored_attr));
		if (uzfs_attr_cmp(&stored_attr, cur_attr) == 0) {
			printf("cur_attr: \n");
			print_stat_sa(NULL, cur_attr);
			printf("stored_attr: \n");
			print_stat_sa(NULL, &stored_attr);
			printf("\n");
			return (B_FALSE);
		}
		// printf("get ok\n");
		return (B_TRUE);
	}
	op -= getattr_proportion;

	if (op < setattr_proportion) {
		// set attr
		// change 4 byte of cur_attr and set
		// printf("set attr\n");
		int attr_size = sizeof (uzfs_attr_t);
		int start_index = rand() % (attr_size - 3);
		for (int i =  start_index; i < start_index + 4; ++i) {
			((uchar_t *)(cur_attr))[i] = rand() % 256;
		}
		VERIFY0(libuzfs_inode_setattr(dhp, *ino, cur_attr, 0));
		return (B_TRUE);
	}
	return (B_TRUE);
}

static int
uzfs_attr_random_test(int argc, char **argv)
{
	assert(argc == 3);
	char *dsname = argv[1];
	libuzfs_dataset_handle_t *dhp = libuzfs_dataset_open(dsname);
	if (!dhp) {
		printf("failed to open dataset: %s\n", dsname);
		return (-1);
	}

	int nloops = atoi(argv[2]);
	uint64_t ino = 0;
	uzfs_attr_t cur_attr;
	memset(&cur_attr, 0, sizeof (cur_attr));
	nvlist_t *nvl = NULL;
	VERIFY0(nvlist_alloc(&nvl, NV_UNIQUE_NAME, KM_SLEEP));
	libuzfs_inode_type_t type = 0;
	int seed = time(NULL);
	srand(seed);

	printf("testing attr functionalities, "
	    "loops: %d, seed: %d\n", nloops, seed);

	for (int i = 0; i < nloops; ++i) {
		boolean_t reset;
		if (!uzfs_attr_ops(dhp, &ino, &type, &cur_attr, nvl, &reset)) {
			printf("test failed, total loops: %d\n", i);
			break;
		}
		if (reset) {
			nvlist_free(nvl);
			VERIFY0(nvlist_alloc(&nvl, NV_UNIQUE_NAME, KM_SLEEP));
		}
	}

	if (ino != 0) {
		VERIFY0(libuzfs_inode_delete(dhp, ino, type, 0));
	}
	nvlist_free(nvl);
	libuzfs_dataset_close(dhp);

	printf("test end\n");

	return (0);
}

static void
random_object_op(libuzfs_dataset_handle_t *dhp, uint64_t obj,
    int *cur_file_size, char *file_content, int max_size)
{
	int write_proportion = 2;
	int truncate_proportion = 1;
	int read_proportion = 2;
	int total_proportion = write_proportion + truncate_proportion +
	    read_proportion;

	int op = rand() % total_proportion;

	if (op < write_proportion) {
		uint64_t offset = rand() % max_size;
		uint64_t size = rand() % (max_size - offset);
		*cur_file_size = MAX(*cur_file_size, offset + size);
		for (int i = offset; i < offset + size; ++i) {
			file_content[i] = rand() % 256;
		}
		VERIFY0(libuzfs_object_write(dhp, obj, offset,
		    size, file_content + offset, FALSE));
		return;
	}
	op -= write_proportion;

	if (op < truncate_proportion) {
		int res_size = rand() % max_size + 1;
		if (res_size < *cur_file_size) {
			memset(file_content + res_size, 0,
			    *cur_file_size - res_size);
		}
		*cur_file_size = res_size;
		VERIFY0(libuzfs_object_truncate(dhp, obj, 0, res_size));
		return;
	}
	op -= truncate_proportion;

	if (op < read_proportion) {
		char *actual_content = umem_zalloc(max_size, UMEM_NOFAIL);
		VERIFY3U(libuzfs_object_read(dhp, obj, 0,
		    max_size, actual_content), ==, *cur_file_size);
		VERIFY0(memcmp(actual_content, file_content, max_size));
		umem_free(actual_content, max_size);
		return;
	}
	op -= read_proportion;
}

static int
uzfs_object_test(int argc, char **argv)
{
#define	FILE_SIZE (1<<20)
	assert(argc >= 2);
	libuzfs_dataset_handle_t *dhp = libuzfs_dataset_open(argv[1]);
	if (dhp == NULL) {
		printf("failed to open %s\n", argv[1]);
		return (-1);
	}

	for (int i = 0; i < 10; ++i) {
		uint64_t obj;
		uint64_t gen;
		VERIFY0(libuzfs_objects_create(dhp, &obj, 1, &gen));
		printf("new object: %lu, gen: %lu\n", obj, gen);

		char *file_content = umem_zalloc(FILE_SIZE, UMEM_NOFAIL);
		int cur_file_size = 0;
		srand(time(NULL));
		for (int j = 0; j < 1000; ++j) {
			random_object_op(dhp, obj, &cur_file_size,
			    file_content, FILE_SIZE);
		}

		umem_free(file_content, FILE_SIZE);
		VERIFY0(libuzfs_object_delete(dhp, obj));
	}

	libuzfs_dataset_close(dhp);

	return (0);
}

typedef struct bench_thread_ctx {
	libuzfs_dataset_handle_t *dhp;
	uint64_t obj;
	uint64_t blksize;
	uint64_t total_size;
	boolean_t write;
	pthread_t tid;
} bench_thread_ctx_t;

static void *worker_func(void *arg) {
	bench_thread_ctx_t *ctx = arg;

	boolean_t sync = B_FALSE;
	char *buf = umem_alloc(ctx->blksize, UMEM_NOFAIL);
	memset(buf, 0xf1, ctx->blksize);

	uint64_t nwritten = 0;

	while (nwritten < ctx->total_size) {
		if (ctx->write) {
			VERIFY0(libuzfs_object_write(ctx->dhp,
			    ctx->obj, nwritten, ctx->blksize, buf, sync));
		} else {
			VERIFY3U(libuzfs_object_read(ctx->dhp, ctx->obj,
			    nwritten, ctx->blksize, buf), ==, ctx->blksize);
			VERIFY((uint8_t)buf[rand() % ctx->blksize] == 0xf1);
		}
		nwritten += ctx->blksize;
	}

	umem_free(buf, ctx->blksize);
	return (NULL);
}

static uint64_t
micros_elapsed(struct timeval *before, struct timeval *after)
{
	return (after->tv_sec - before->tv_sec) * 1000000 +
	    after->tv_usec - before->tv_usec;
}

/*
 * Usage:
 *   uzfs io-bench <NUM_THREADS> <MEGAS_PER_THREAD> <DEV_NAME1> <DEV_NAME2> ...
 * Example:
 *   $ uzfs io-bench 16 1024 vda vdb
 *   this command will first create zpool vda in /dev/vda, vdb in /dev/vdb, then
 * create 16 objects using zpool vda and vdb, 8 objects in each, then create 16
 * threads to read/write those objects. each thread will first sequetialy write
 * 1024 MB to its object, then sequentialy read 1024 MB from the object.
 */
static int
uzfs_io_bench(int argc, char **argv)
{
	int nthread = atoi(argv[1]);
	uint64_t megas_per_thread = atoi(argv[2]);

	argc -= 3;
	argv += 3;

	uint64_t blksize_kilo = 256;

	libuzfs_dataset_handle_t **dhps = umem_alloc(
	    sizeof (libuzfs_dataset_handle_t *) * argc, UMEM_NOFAIL);

	for (int i = 0; i < argc; ++i) {
		char dev_path[256];
		sprintf(dev_path, "/dev/%s", argv[i]);
		char pool_name[64];
		memset(pool_name, 0, 64);
		int err = libuzfs_zpool_import(dev_path, pool_name, 64);
		printf("zpool import, err: %d, pool_name: %s\n",
		    err, pool_name);
		VERIFY(err == 0 || err == ENOENT || err == EEXIST);
		if (err == 0 || err == EEXIST) {
			VERIFY3U(strcmp(pool_name, argv[i]), ==, 0);
			VERIFY0(libuzfs_zpool_destroy(pool_name));
			printf("destroy zpool: %s\n", pool_name);
		}

		VERIFY0(libuzfs_zpool_create(argv[i], dev_path,
		    NULL, NULL));
		printf("create zpool, pool_name: %s, dev_path: %s\n",
		    argv[i], dev_path);

		char dataset_name[256];
		sprintf(dataset_name, "%s/ds2", argv[i]);
		VERIFY0(libuzfs_dataset_create(dataset_name));
		printf("create dataset: %s\n", dataset_name);

		dhps[i] = libuzfs_dataset_open(dataset_name);
		VERIFY3U(dhps[i], !=, NULL);
	}

	bench_thread_ctx_t *ctxs = umem_alloc(
	    sizeof (bench_thread_ctx_t) * nthread, UMEM_NOFAIL);

	for (int i = 0; i < nthread; ++i) {
		ctxs[i].dhp = dhps[i % argc];
		uint64_t gen;
		VERIFY0(libuzfs_objects_create(dhps[i % argc],
		    &ctxs[i].obj, 1, &gen));
		ctxs[i].blksize = blksize_kilo << 10;
		ctxs[i].total_size = megas_per_thread << 20;
		ctxs[i].write = B_TRUE;
	}

	struct timeval before;
	gettimeofday(&before, NULL);
	for (int i = 0; i < nthread; ++i) {
		pthread_create(&ctxs[i].tid, NULL, worker_func, &ctxs[i]);
	}

	for (int i = 0; i < nthread; ++i) {
		pthread_join(ctxs[i].tid, NULL);
	}

	struct timeval after;
	gettimeofday(&after, NULL);

	uint64_t elapsed_micros = micros_elapsed(&before, &after);
	printf("concurrency: %d, write throughput: %luMB/s\n", nthread,
	    megas_per_thread * nthread * 1000000 / elapsed_micros);

	gettimeofday(&before, NULL);
	for (int i = 0; i < nthread; ++i) {
		ctxs[i].write = B_FALSE;
		pthread_create(&ctxs[i].tid, NULL, worker_func, &ctxs[i]);
	}

	for (int i = 0; i < nthread; ++i) {
		pthread_join(ctxs[i].tid, NULL);
	}

	gettimeofday(&after, NULL);
	elapsed_micros = micros_elapsed(&before, &after);
	printf("concurrency: %d, read throughput: %luMB/s\n", nthread,
	    megas_per_thread * nthread * 1000000 / elapsed_micros);

	for (int i = 0; i < argc; ++i) {
		libuzfs_dataset_close(dhps[i]);
	}

	return (0);
}

int
uzfs_dentry_create(int argc, char **argv)
{
	int err = 0;
	char *dsname = argv[1];
	uint64_t dino = atoll(argv[2]);
	char *name = argv[3];
	uint64_t ino = atoll(argv[4]);
	uint64_t type = atoll(argv[5]);

	printf("creating dentry %s, dino: %ld, name: %s, ino: %ld, type: %ld\n",
	    dsname, dino, name, ino, type);

	libuzfs_dataset_handle_t *dhp = libuzfs_dataset_open(dsname);
	if (!dhp) {
		printf("failed to open dataset: %s\n", dsname);
		return (-1);
	}

	uint64_t txg = 0;
	uint64_t value = ino;
	value |= type << 60;

	err = libuzfs_dentry_create(dhp, dino, name, value, &txg);
	if (err)
		printf("failed to create dentry on dataset: %s\n", dsname);
	else
		printf("created dentry %s, dino: %ld, name: %s, [0x%lx]\n",
		    dsname, dino, name, value);

	libuzfs_dataset_close(dhp);

	return (err);
}

int
uzfs_dentry_delete(int argc, char **argv)
{
	int err = 0;
	char *dsname = argv[1];
	uint64_t dino = atoll(argv[2]);
	char *name = argv[3];

	printf("destroying dentry %s: dino: %ld, name: %s\n",
	    dsname, dino, name);

	libuzfs_dataset_handle_t *dhp = libuzfs_dataset_open(dsname);
	if (!dhp) {
		printf("failed to open dataset: %s\n", dsname);
		return (-1);
	}

	uint64_t txg = 0;

	err = libuzfs_dentry_delete(dhp, dino, name, &txg);
	if (err)
		printf("failed to delete dentry: %s:%ld/%s\n",
		    dsname, dino, name);

	libuzfs_dataset_close(dhp);
	return (0);
}

int
uzfs_dentry_lookup(int argc, char **argv)
{
	int err = 0;
	char *dsname = argv[1];
	uint64_t dino = atoll(argv[2]);
	char *name = argv[3];

	printf("lookup dentry %s: dino: %ld, name: %s\n", dsname, dino, name);

	libuzfs_dataset_handle_t *dhp = libuzfs_dataset_open(dsname);
	if (!dhp) {
		printf("failed to open dataset: %s\n", dsname);
		return (-1);
	}

	uint64_t value = 0;

	err = libuzfs_dentry_lookup(dhp, dino, name, &value);
	if (err)
		printf("failed to lookup dentry: %s:%ld/%s\n",
		    dsname, dino, name);
	else
		printf("looked up dentry: %s:%ld/%s: [0x%lx]\n",
		    dsname, dino, name, value);

	libuzfs_dataset_close(dhp);
	return (0);
}

static uint64_t
dump_dentries(char *buf, uint32_t size, uint32_t num)
{
	uint64_t whence = 0;
	struct uzfs_dentry *cur = (struct uzfs_dentry *)buf;
	for (int i = 0; i < num; i++) {
		printf("%s\t0x%lx\n", cur->name, cur->value);
		whence = cur->whence;
		cur = (struct uzfs_dentry *)((char *)cur + cur->size);
	}

	return (whence);
}

int
uzfs_dentry_list(int argc, char **argv)
{
	int err = 0;
	char *dsname = argv[1];
	uint64_t dino = atoll(argv[2]);

	printf("list dentry %s: dino: %ld\n", dsname, dino);

	libuzfs_dataset_handle_t *dhp = libuzfs_dataset_open(dsname);
	if (!dhp) {
		printf("failed to open dataset: %s\n", dsname);
		return (-1);
	}

	uint32_t num = 0;
	uint64_t whence = 0;
	uint32_t size = 4096;
	char *buf = umem_zalloc(size, UMEM_NOFAIL);

	while (1) {
		num = 0;
		memset(buf, 0, size);
		err = libuzfs_dentry_iterate(dhp, dino, whence, size, buf,
		    &num);
		if (err) {
			printf("failed to iterate dentry: %s:%ld\n", dsname,
			    dino);
			break;
		} else if (num == 0)
			break;
		else
			whence = dump_dentries(buf, size, num);
	}

	umem_free(buf, size);
	libuzfs_dataset_close(dhp);
	return (0);
}

int
uzfs_fs_create(int argc, char **argv)
{
	int err = 0;
	char *fsname = argv[1];

	printf("creating fs %s\n", fsname);

	err = libuzfs_fs_create(fsname);
	if (err)
		printf("failed to create fs: %s\n", fsname);

	return (err);
}

int
uzfs_fs_destroy(int argc, char **argv)
{
	char *fsname = argv[1];

	printf("destroying fs %s\n", fsname);

	libuzfs_fs_destroy(fsname);

	return (0);
}

static int uzfs_stat(int argc, char **argv)
{
	int error = 0;
	char *path = argv[1];

	char fsname[256] = "";
	char target_path[256] = "";

	char *fs_end = strstr(path, "://");
	memcpy(fsname, path, fs_end - path);
	memcpy(target_path, fs_end + 3, strlen(path) - strlen(fsname) - 3);

	printf("stat %s: %s\n", fsname, target_path);

	uint64_t fsid = 0;
	error = libuzfs_fs_init(fsname, &fsid);
	if (error) goto out;

	uint64_t root_ino = 0;

	error = libuzfs_getroot(fsid, &root_ino);
	if (error) goto out;

	char *s = target_path;
	if (*s != '/') {
		printf("path %s must be started with /\n", target_path);
		error = 1;
		goto out;
	}

	s++;

	char *e = strchr(s, '/');

	uint64_t dino = root_ino;
	uint64_t ino = 0;
	boolean_t out_flag = B_FALSE;

	while (s) {
		if (e)
			*e = '\0';
		else
			out_flag = B_TRUE;

		error = libuzfs_lookup(fsid, dino, s, &ino);
		if (error) goto out;

		if (out_flag)
			break;

		s = e + 1;
		e = strchr(s, '/');
		dino = ino;
	}

	if (ino == 0) {
		printf("Empty path\n");
		error = 1;
		goto out;
	}

	struct stat buf;
	memset(&buf, 0, sizeof (struct stat));

	error = libuzfs_getattr(fsid, ino, &buf);
	if (error) {
		printf("Failed to stat %s\n", path);
		goto out;
	}

	print_stat(s, &buf);

out:

	libuzfs_fs_fini(fsid);

	return (error);
}

static int uzfs_mkdir(int argc, char **argv)
{
	int error = 0;
	char *path = argv[1];

	char fsname[256] = "";
	char target_path[256] = "";

	char *fs_end = strstr(path, "://");
	memcpy(fsname, path, fs_end - path);
	memcpy(target_path, fs_end + 3, strlen(path) - strlen(fsname) - 3);

	printf("mkdir %s: %s\n", fsname, target_path);

	uint64_t fsid = 0;
	error = libuzfs_fs_init(fsname, &fsid);
	if (error) goto out;

	uint64_t root_ino = 0;

	error = libuzfs_getroot(fsid, &root_ino);
	if (error) goto out;

	char *s = target_path;
	if (*s != '/') {
		printf("path %s must be started with /\n", target_path);
		error = 1;
		goto out;
	}

	s++;

	char *e = strchr(s, '/');

	uint64_t dino = root_ino;
	uint64_t ino = 0;

	while (e) {
		*e = '\0';

		error = libuzfs_lookup(fsid, dino, s, &ino);
		if (error) goto out;

		s = e + 1;
		e = strchr(s, '/');
		dino = ino;
	}

	error = libuzfs_mkdir(fsid, dino, s, 0, &ino);
	if (error) {
		printf("Failed to mkdir %s\n", path);
		goto out;
	}

	printf("succeeded to mkdir %s\n", path);

out:

	libuzfs_fs_fini(fsid);

	return (error);

}

static int uzfs_create(int argc, char **argv)
{
	int error = 0;
	char *path = argv[1];

	char fsname[256] = "";
	char target_path[256] = "";

	char *fs_end = strstr(path, "://");
	memcpy(fsname, path, fs_end - path);
	memcpy(target_path, fs_end + 3, strlen(path) - strlen(fsname) - 3);

	printf("create %s: %s\n", fsname, target_path);

	uint64_t fsid = 0;
	error = libuzfs_fs_init(fsname, &fsid);
	if (error) goto out;

	uint64_t root_ino = 0;

	error = libuzfs_getroot(fsid, &root_ino);
	if (error) goto out;

	char *s = target_path;
	if (*s != '/') {
		printf("path %s must be started with /\n", target_path);
		error = 1;
		goto out;
	}

	s++;

	char *e = strchr(s, '/');

	uint64_t dino = root_ino;
	uint64_t ino = 0;

	while (e) {
		*e = '\0';

		error = libuzfs_lookup(fsid, dino, s, &ino);
		if (error) goto out;

		s = e + 1;
		e = strchr(s, '/');
		dino = ino;
	}

	error = libuzfs_create(fsid, dino, s, 0, &ino);
	if (error) {
		printf("Failed to create file %s\n", path);
		goto out;
	}

out:

	libuzfs_fs_fini(fsid);

	return (error);
}

static int uzfs_rm(int argc, char **argv)
{
	int error = 0;
	char *path = argv[1];

	char fsname[256] = "";
	char target_path[256] = "";

	char *fs_end = strstr(path, "://");
	memcpy(fsname, path, fs_end - path);
	memcpy(target_path, fs_end + 3, strlen(path) - strlen(fsname) - 3);

	printf("rm %s: %s\n", fsname, target_path);

	uint64_t fsid = 0;
	error = libuzfs_fs_init(fsname, &fsid);
	if (error) goto out;

	uint64_t root_ino = 0;

	error = libuzfs_getroot(fsid, &root_ino);
	if (error) goto out;

	char *s = target_path;
	if (*s != '/') {
		printf("path %s must be started with /\n", target_path);
		error = 1;
		goto out;
	}

	s++;

	char *e = strchr(s, '/');

	uint64_t dino = root_ino;
	uint64_t ino = 0;
	boolean_t out_flag = B_FALSE;

	while (s) {
		if (e)
			*e = '\0';
		else
			out_flag = B_TRUE;

		error = libuzfs_lookup(fsid, dino, s, &ino);
		if (error) goto out;

		if (out_flag)
			break;

		s = e + 1;
		e = strchr(s, '/');
		dino = ino;
	}

	if (ino == 0) {
		printf("Empty path\n");
		error = 1;
		goto out;
	}

	struct stat buf;
	memset(&buf, 0, sizeof (struct stat));

	error = libuzfs_getattr(fsid, ino, &buf);
	if (error) {
		printf("Failed to stat %s\n", path);
		goto out;
	}

	if (S_ISDIR(buf.st_mode)) {
		error = libuzfs_rmdir(fsid, dino, s);
		if (error) {
			printf("Failed to rm dir %s\n", path);
			goto out;
		}
	} else if (S_ISREG(buf.st_mode)) {
		error = libuzfs_remove(fsid, dino, s);
		if (error) {
			printf("Failed to rm file %s\n", path);
			goto out;
		}
	} else {
		printf("Invalid file type\n");
		error = 1;
		goto out;
	}

out:

	libuzfs_fs_fini(fsid);

	return (error);
}

static int uzfs_dir_emit(void *ctx, const char *name, int namelen, loff_t off,
    uint64_t ino, unsigned type)
{
	printf("\t%s\tobjnum: %ld\n", name, ino);
	return (0);
}

static int uzfs_ls(int argc, char **argv)
{
	int error = 0;
	char *path = argv[1];

	char fsname[256] = "";
	char target_path[256] = "";

	char *fs_end = strstr(path, "://");
	memcpy(fsname, path, fs_end - path);
	memcpy(target_path, fs_end + 3, strlen(path) - strlen(fsname) - 3);

	printf("ls %s: %s\n", fsname, target_path);

	uint64_t fsid = 0;
	error = libuzfs_fs_init(fsname, &fsid);
	if (error) goto out;

	uint64_t root_ino = 0;

	error = libuzfs_getroot(fsid, &root_ino);
	if (error) goto out;

	char *s = target_path;
	if (*s != '/') {
		printf("path %s must be started with /\n", target_path);
		error = 1;
		goto out;
	}

	s++;

	char *e = strchr(s, '/');

	uint64_t dino = root_ino;
	uint64_t ino = 0;
	boolean_t out_flag = B_FALSE;

	while (s) {
		if (e)
			*e = '\0';
		else
			out_flag = B_TRUE;

		error = libuzfs_lookup(fsid, dino, s, &ino);
		if (error) goto out;

		if (out_flag)
			break;

		s = e + 1;
		e = strchr(s, '/');
		dino = ino;
	}

	if (ino == 0) {
		printf("Empty path\n");
		error = 1;
		goto out;
	}

	struct stat buf;
	memset(&buf, 0, sizeof (struct stat));

	error = libuzfs_getattr(fsid, ino, &buf);
	if (error) {
		printf("Failed to stat %s\n", path);
		goto out;
	}

	if (S_ISDIR(buf.st_mode)) {
		error = libuzfs_readdir(fsid, ino, NULL, uzfs_dir_emit, 0);
		if (error) {
			printf("Failed to readdir %s\n", path);
			goto out;
		}
	} else if (S_ISREG(buf.st_mode)) {
		printf("%s\t%ld\n", s, buf.st_ino);
	} else {
		printf("Invalid file type\n");
		error = 1;
		goto out;
	}

out:

	libuzfs_fs_fini(fsid);

	return (error);
}

static int uzfs_mv(int argc, char **argv)
{
	int error = 0;
	char *spath = argv[1];
	char *dst_path = argv[2];

	char fsname[256] = "";
	char src_path[256] = "";

	char *fs_end = strstr(spath, "://");
	memcpy(fsname, spath, fs_end - spath);
	memcpy(src_path, fs_end + 3, strlen(spath) - strlen(fsname) - 3);

	printf("mv %s: %s %s\n", fsname, src_path, dst_path);

	uint64_t fsid = 0;
	error = libuzfs_fs_init(fsname, &fsid);
	if (error) goto out;

	uint64_t root_ino = 0;

	error = libuzfs_getroot(fsid, &root_ino);
	if (error) goto out;

	char *s = src_path;
	if (*s != '/') {
		printf("path %s must be started with /\n", src_path);
		error = 1;
		goto out;
	}

	s++;

	char *e = strchr(s, '/');

	uint64_t sdino = root_ino;
	uint64_t sino = 0;

	while (e) {
		*e = '\0';

		error = libuzfs_lookup(fsid, sdino, s, &sino);
		if (error) goto out;

		s = e + 1;
		e = strchr(s, '/');
		sdino = sino;
	}

	char *d = dst_path;
	if (*d != '/') {
		printf("path %s must be started with /\n", dst_path);
		error = 1;
		goto out;
	}

	d++;

	e = strchr(d, '/');

	uint64_t dst_dino = root_ino;
	uint64_t dst_ino = 0;

	while (e) {
		*e = '\0';

		error = libuzfs_lookup(fsid, dst_dino, d, &dst_ino);
		if (error) goto out;

		d = e + 1;
		e = strchr(d, '/');
		dst_dino = dst_ino;
	}


	error = libuzfs_rename(fsid, sdino, s, dst_dino, d);
	if (error) {
		printf("Failed to mv %s %s\n", spath, dst_path);
		goto out;
	}

out:

	libuzfs_fs_fini(fsid);

	return (error);

}

static int uzfs_read(int argc, char **argv)
{
	int error = 0;
	char buf[4096] = "";
	char *path = argv[1];
	int offset = atoi(argv[2]);
	int size = atoi(argv[3]);

	char fsname[256] = "";
	char target_path[256] = "";

	char *fs_end = strstr(path, "://");
	memcpy(fsname, path, fs_end - path);
	memcpy(target_path, fs_end + 3, strlen(path) - strlen(fsname) - 3);

	printf("read %s: %s, offset: %d, size: %d\n",
	    fsname, target_path, offset, size);

	uint64_t fsid = 0;
	error = libuzfs_fs_init(fsname, &fsid);
	if (error) goto out;

	uint64_t root_ino = 0;

	error = libuzfs_getroot(fsid, &root_ino);
	if (error) goto out;

	char *s = target_path;
	if (*s != '/') {
		printf("path %s must be started with /\n", target_path);
		error = 1;
		goto out;
	}

	s++;

	char *e = strchr(s, '/');

	uint64_t dino = root_ino;
	uint64_t ino = 0;
	boolean_t out_flag = B_FALSE;

	while (s) {
		if (e)
			*e = '\0';
		else
			out_flag = B_TRUE;

		error = libuzfs_lookup(fsid, dino, s, &ino);
		if (error) goto out;

		if (out_flag)
			break;

		s = e + 1;
		e = strchr(s, '/');
		dino = ino;
	}

	if (ino == 0) {
		printf("Empty path\n");
		error = 1;
		goto out;
	}

	struct iovec iov;
	iov.iov_base = buf;
	iov.iov_len = size;

	zfs_uio_t uio;
	zfs_uio_iovec_init(&uio, &iov, 1, offset, UIO_USERSPACE, size, 0);

	error = libuzfs_read(fsid, ino, &uio, 0);
	if (error) {
		printf("Failed to read %s\n", path);
		goto out;
	}

	printf("%s\n", buf);

	libuzfs_fs_fini(fsid);

out:
	return (error);
}

static int uzfs_write(int argc, char **argv)
{
	int error = 0;
	char buf[4096] = "";
	char *path = argv[1];
	int offset = atoi(argv[2]);
	int size = atoi(argv[3]);

	char fsname[256] = "";
	char target_path[256] = "";

	char *fs_end = strstr(path, "://");
	memcpy(fsname, path, fs_end - path);
	memcpy(target_path, fs_end + 3, strlen(path) - strlen(fsname) - 3);
	memcpy(buf, argv[4], strlen(argv[4]));

	printf("write %s: %s\n", fsname, target_path);

	uint64_t fsid = 0;
	error = libuzfs_fs_init(fsname, &fsid);
	if (error) goto out;

	uint64_t root_ino = 0;

	error = libuzfs_getroot(fsid, &root_ino);
	if (error) goto out;

	char *s = target_path;
	if (*s != '/') {
		printf("path %s must be started with /\n", target_path);
		error = 1;
		goto out;
	}

	s++;

	char *e = strchr(s, '/');

	uint64_t dino = root_ino;
	uint64_t ino = 0;
	boolean_t out_flag = B_FALSE;

	while (s) {
		if (e)
			*e = '\0';
		else
			out_flag = B_TRUE;

		error = libuzfs_lookup(fsid, dino, s, &ino);
		if (error) goto out;

		if (out_flag)
			break;

		s = e + 1;
		e = strchr(s, '/');
		dino = ino;
	}

	if (ino == 0) {
		printf("Empty path\n");
		error = 1;
		goto out;
	}

	struct iovec iov;
	iov.iov_base = buf;
	iov.iov_len = size;

	zfs_uio_t uio;
	zfs_uio_iovec_init(&uio, &iov, 1, offset, UIO_USERSPACE, size, 0);

	error = libuzfs_write(fsid, ino, &uio, 0);
	if (error) {
		printf("Failed to write %s\n", path);
		goto out;
	}

	libuzfs_fs_fini(fsid);

out:
	return (error);
}

static int uzfs_fsync(int argc, char **argv)
{
	int error = 0;
	char *path = argv[1];

	char fsname[256] = "";
	char target_path[256] = "";

	char *fs_end = strstr(path, "://");
	memcpy(fsname, path, fs_end - path);
	memcpy(target_path, fs_end + 3, strlen(path) - strlen(fsname) - 3);

	printf("write %s: %s\n", fsname, target_path);

	uint64_t fsid = 0;
	error = libuzfs_fs_init(fsname, &fsid);
	if (error) goto out;

	uint64_t root_ino = 0;

	error = libuzfs_getroot(fsid, &root_ino);
	if (error) goto out;

	char *s = target_path;
	if (*s != '/') {
		printf("path %s must be started with /\n", path);
		error = 1;
		goto out;
	}

	s++;

	char *e = strchr(s, '/');

	uint64_t dino = root_ino;
	uint64_t ino = 0;
	boolean_t out_flag = B_FALSE;

	while (s) {
		if (e)
			*e = '\0';
		else
			out_flag = B_TRUE;

		error = libuzfs_lookup(fsid, dino, s, &ino);
		if (error) goto out;

		if (out_flag)
			break;

		s = e + 1;
		e = strchr(s, '/');
		dino = ino;
	}

	if (ino == 0) {
		printf("Empty path\n");
		error = 1;
		goto out;
	}

	error = libuzfs_fsync(fsid, ino, 0);
	if (error) {
		printf("Failed to sync %s\n", path);
		goto out;
	}

	libuzfs_fs_fini(fsid);
out:
	return (error);
}

struct perf_args {
	uint64_t fsid;
	uint64_t dino;
	int op;
	int num;
	int tid;
};

static void* do_perf(void* perf_args)
{
	struct perf_args *args = perf_args;
	uint64_t fsid = args->fsid;
	uint64_t root_ino = args->dino;
	int op = args->op;
	int num = args->num;
	int tid = args->tid;
	int error = 0;
	int i = 0;
	uint64_t ino;
	uint64_t dino;
	char name[20] = "";

	printf("tid: %d\n", tid);
	sprintf(name, "t%d", tid);

	if (op == 1 || op == 3) {
		error = libuzfs_mkdir(fsid, root_ino, name, 0, &ino);
		if (error) {
			printf("Failed to mkdir parent %s\n", name);
			goto out;
		}
	} else {
		error = libuzfs_lookup(fsid, root_ino, name, &ino);
		if (error) {
			printf("Failed to lookup parent dir %s\n", name);
			goto out;
		}
	}

	dino = ino;

	int print_idx = num / 100;
	for (i = 0; i < num; i++) {
		sprintf(name, "%d", i);
		if (op == 0) {
			error = libuzfs_rmdir(fsid, dino, name);
			if (error) {
				printf("Failed to mkdir %s\n", name);
				goto out;
			}
		} else if (op == 1) {
			error = libuzfs_mkdir(fsid, dino, name, 0, &ino);
			if (error) {
				printf("Failed to rmdir %s\n", name);
				goto out;
			}
		} else if (op == 2) {
			error = libuzfs_remove(fsid, dino, name);
			if (error) {
				printf("Failed to remove file %s\n", name);
				goto out;
			}
		} else if (op == 3) {
			error = libuzfs_create(fsid, dino, name, 0, &ino);
			if (error) {
				printf("Failed to create file %s\n", name);
				goto out;
			}
		} else if (op == 4) {
			error = libuzfs_lookup(fsid, dino, name, &ino);
			if (error) goto out;

			struct stat buf;
			memset(&buf, 0, sizeof (struct stat));

			error = libuzfs_getattr(fsid, ino, &buf);
			if (error) {
				printf("Failed to stat %s\n", name);
				goto out;
			}
			// print_stat(name, &buf);
		}
		if (print_idx != 0 && i % print_idx == 0) {
			printf("tid %d: %d%%\n", tid, i / print_idx);
		}
	}

	if (op == 0 || op == 2) {
		sprintf(name, "t%d", tid);
		error = libuzfs_rmdir(fsid, root_ino, name);
		if (error) {
			printf("Failed to rm parent dir %s\n", name);
			goto out;
		}
	}

	printf("tid: %d done\n", tid);

out:
	return (NULL);
}

static int uzfs_perf(int argc, char **argv)
{
	int err = 0;
	char *path = argv[1];

	// 0: rmdir, 1: mkdir, 2: remove file, 3: create file, 4: stat
	int op = atoi(argv[2]);
	int depth = atoi(argv[3]);
	int branch = atoi(argv[4]);
	int num = atoi(argv[5]);
	int n_threads = atoi(argv[6]);

	char fsname[256] = "";
	char target_path[256] = "";
	char *opstr = NULL;

	char *fs_end = strstr(path, "://");
	memcpy(fsname, path, fs_end - path);
	memcpy(target_path, fs_end + 3, strlen(path) - strlen(fsname) - 3);

	switch (op) {
		case 0:
			opstr = "rmdir";
			break;
		case 1:
			opstr = "mkdir";
			break;
		case 2:
			opstr = "rm file";
			break;
		case 3:
			opstr = "create file";
			break;
		case 4:
			opstr = "stat";
			break;
		default:
			printf("invalid op: %d\n", op);
			return (-1);
	}

	printf("%s %s: %s\n", opstr, fsname, target_path);

	uint64_t fsid = 0;
	err = libuzfs_fs_init(fsname, &fsid);
	if (err) goto out;

	uint64_t root_ino = 0;

	err = libuzfs_getroot(fsid, &root_ino);
	if (err) goto out;

	char *s = target_path;
	if (*s != '/') {
		printf("path %s must be started with /\n", target_path);
		err = 1;
		goto out;
	}

	s++;

	char *e = strchr(s, '/');

	uint64_t dino = root_ino;
	uint64_t ino = 0;

	while (e) {
		*e = '\0';

		err = libuzfs_lookup(fsid, dino, s, &ino);
		if (err) goto out;

		s = e + 1;
		e = strchr(s, '/');
		dino = ino;
	}

	struct timeval t1, t2;
	double timeuse;
	gettimeofday(&t1, NULL);

	int i;
	clock_t start, end;
	start = clock();
	pthread_t ntids[100];
	struct perf_args args[100];
	for (i = 0; i < n_threads; i++) {
		args[i].fsid = fsid;
		args[i].dino = dino;
		args[i].op = op;
		args[i].num = num;
		args[i].tid = i;
		err = pthread_create(&ntids[i], NULL, do_perf,
		    (void*)&args[i]);
		if (err != 0) {
			printf("Failed to create thread: %s\n", strerror(err));
			goto out;
		}

	}
	for (i = 0; i < n_threads; i++) {
		pthread_join(ntids[i], NULL);
	}

	end = clock();
	gettimeofday(&t2, NULL);
	timeuse = (t2.tv_sec - t1.tv_sec) +
	    (double)(t2.tv_usec - t1.tv_usec)/1000000.0;

	int totalnum = branch * depth * num * n_threads;
	double clockuse = ((double)(end - start))/CLOCKS_PER_SEC;
	double rate = totalnum / timeuse;
	printf("num: %d\ntime=%fs\nclock=%fs\nrate=%f\n",
	    totalnum, timeuse, clockuse, rate);

out:

	libuzfs_fs_fini(fsid);
	return (err);
}

struct object_perf_args {
	libuzfs_dataset_handle_t *dhp;
	int op;
	int num;
	int tid;
};

static void* do_object_perf(void *object_perf_args)
{
	struct object_perf_args *args = object_perf_args;
	libuzfs_dataset_handle_t *dhp = args->dhp;
	int op = args->op;
	int num = args->num;
	uint64_t tid = args->tid + 1;
	int err = 0;
	int i = 0;
	uint64_t obj = 0;

	printf("tid: %ld\n", tid);

	int print_idx = num / 100;
	for (i = 0; i < num; i++) {
		obj = tid << 32 | i;
		if (op == 0) {
			err = libuzfs_object_delete(dhp, obj);
			if (err) {
				printf("Failed to rm obj %ld\n", obj);
				goto out;
			}
		} else if (op == 1) {
			err = libuzfs_object_claim(dhp, obj);
			if (err) {
				printf("Failed to claim obj %ld\n", obj);
				goto out;
			}
		} else if (op == 2) {
			dmu_object_info_t doi;
			err = libuzfs_object_stat(dhp, obj, &doi);
			if (err) {
				printf("Failed to stat obj %ld\n", obj);
				goto out;
			}
		}
		if (print_idx != 0 && i % print_idx == 0) {
			printf("tid %ld: %d%%\n", tid, i / print_idx);
			printf("last_synced_txg: %ld\n",
			    libuzfs_get_last_synced_txg(dhp));
		}
	}
	libuzfs_wait_synced(dhp);
	printf("tid: %ld done\n", tid);

out:
	return (NULL);
}

static int uzfs_object_perf(int argc, char **argv)
{
	int err = 0;
	char *dsname = argv[1];
	int op = atoi(argv[2]); // 0: rm, 1: create, 2: stat
	int num = atoi(argv[3]);
	int n_threads = atoi(argv[4]);

	char *opstr = NULL;

	switch (op) {
		case 0:
			opstr = "remove";
			break;
		case 1:
			opstr = "create";
			break;
		case 2:
			opstr = "stat";
			break;
		default:
			printf("invalid op: %d\n", op);
			return (-1);
	}

	printf("%s %s\n", opstr, dsname);

	libuzfs_dataset_handle_t *dhp = libuzfs_dataset_open(dsname);
	if (!dhp) {
		printf("failed to open dataset: %s\n", dsname);
		return (-1);
	}

	printf("last_synced_txg: %ld\n", libuzfs_get_last_synced_txg(dhp));

	struct timeval t1, t2;
	double timeuse;
	gettimeofday(&t1, NULL);

	int i;
	clock_t start, end;
	start = clock();
	pthread_t ntids[100];
	struct object_perf_args args[100];
	for (i = 0; i < n_threads; i++) {
		args[i].dhp = dhp;
		args[i].op = op;
		args[i].num = num;
		args[i].tid = i;
		err = pthread_create(&ntids[i], NULL, do_object_perf,
		    (void*)&args[i]);
		if (err != 0) {
			printf("Failed to create thread: %s\n", strerror(err));
			goto out;
		}

	}
	for (i = 0; i < n_threads; i++) {
		pthread_join(ntids[i], NULL);
	}

	end = clock();
	gettimeofday(&t2, NULL);
	timeuse = (t2.tv_sec - t1.tv_sec) +
	    (double)(t2.tv_usec - t1.tv_usec)/1000000.0;

	int totalnum = num * n_threads;
	double clockuse = ((double)(end - start))/CLOCKS_PER_SEC;
	double rate = totalnum / timeuse;
	printf("num: %d\ntime=%fs\nclock=%fs\nrate=%f\n",
	    totalnum, timeuse, clockuse, rate);

	printf("last_synced_txg: %ld\n", libuzfs_get_last_synced_txg(dhp));
out:

	libuzfs_dataset_close(dhp);
	return (err);
}
