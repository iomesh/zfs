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

static int uzfs_zpool_create(int argc, char **argv);
static int uzfs_zpool_destroy(int argc, char **argv);
static int uzfs_zpool_set(int argc, char **argv);
static int uzfs_zpool_get(int argc, char **argv);

static int uzfs_dataset_create(int argc, char **argv);
static int uzfs_dataset_destroy(int argc, char **argv);

static int uzfs_object_create(int argc, char **argv);
static int uzfs_object_delete(int argc, char **argv);
static int uzfs_object_claim(int argc, char **argv);
static int uzfs_object_stat(int argc, char **argv);
static int uzfs_object_list(int argc, char **argv);
static int uzfs_object_read(int argc, char **argv);
static int uzfs_object_write(int argc, char **argv);

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

typedef enum {
	HELP_ZPOOL_CREATE,
	HELP_ZPOOL_DESTROY,
	HELP_ZPOOL_SET,
	HELP_ZPOOL_GET,
	HELP_DATASET_CREATE,
	HELP_DATASET_DESTROY,
	HELP_OBJECT_CREATE,
	HELP_OBJECT_DELETE,
	HELP_OBJECT_CLAIM,
	HELP_OBJECT_STAT,
	HELP_OBJECT_LIST,
	HELP_OBJECT_READ,
	HELP_OBJECT_WRITE,
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
	{ "set-zpool",		uzfs_zpool_set, 	HELP_ZPOOL_SET    },
	{ "get-zpool",		uzfs_zpool_get, 	HELP_ZPOOL_GET    },
	{ "create-dataset",	uzfs_dataset_create, 	HELP_DATASET_CREATE },
	{ "destroy-dataset",	uzfs_dataset_destroy, 	HELP_DATASET_DESTROY},
	{ "create-object",	uzfs_object_create, 	HELP_OBJECT_CREATE  },
	{ "delete-object",	uzfs_object_delete, 	HELP_OBJECT_DELETE  },
	{ "claim-object",	uzfs_object_claim, 	HELP_OBJECT_CLAIM   },
	{ "stat-object",	uzfs_object_stat, 	HELP_OBJECT_STAT    },
	{ "list-object",	uzfs_object_list, 	HELP_OBJECT_LIST    },
	{ "read-object",	uzfs_object_read, 	HELP_OBJECT_READ    },
	{ "write-object",	uzfs_object_write, 	HELP_OBJECT_WRITE   },
	{ "create-zap",		uzfs_zap_create, 	HELP_ZAP_CREATE     },
	{ "delete-zap",		uzfs_zap_delete, 	HELP_ZAP_DELETE     },
	{ "add-zap"	,	uzfs_zap_add,	 	HELP_ZAP_ADD        },
	{ "remove-zap"	,	uzfs_zap_remove, 	HELP_ZAP_REMOVE     },
	{ "update-zap"	,	uzfs_zap_update, 	HELP_ZAP_UPDATE     },
	{ "lookup-zap"	,	uzfs_zap_lookup, 	HELP_ZAP_LOOKUP     },
	{ "count-zap"	,	uzfs_zap_count, 	HELP_ZAP_COUNT      },
	{ "create-fs",		uzfs_fs_create, 	HELP_FS_CREATE      },
	{ "destroy-fs",		uzfs_fs_destroy, 	HELP_FS_DESTROY     },
	{ "stat",		uzfs_stat,		HELP_STAT           },
	{ "mkdir",		uzfs_mkdir,		HELP_MKDIR          },
	{ "create",		uzfs_create,		HELP_CREATE         },
	{ "rm",			uzfs_rm,		HELP_RM             },
	{ "ls",			uzfs_ls,		HELP_LS             },
	{ "mv",			uzfs_mv,		HELP_MV             },
	{ "read",		uzfs_read,		HELP_READ           },
	{ "write",		uzfs_write,		HELP_WRITE          },
	{ "fsync",		uzfs_fsync,		HELP_FSYNC          },
	{ "perf",		uzfs_perf,		HELP_PERF           },
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
	case HELP_OBJECT_CREATE:
		return (gettext("\tcreate-object ...\n"));
	case HELP_OBJECT_DELETE:
		return (gettext("\tdelete-object ...\n"));
	case HELP_OBJECT_STAT:
		return (gettext("\tstat-object ...\n"));
	case HELP_OBJECT_LIST:
		return (gettext("\tlist-object ...\n"));
	case HELP_OBJECT_READ:
		return (gettext("\tread-object ...\n"));
	case HELP_OBJECT_WRITE:
		return (gettext("\twrite-object ...\n"));
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
	default:
		__builtin_unreachable();
	}
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
	uint64_t value = atoi(argv[3]);

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
uzfs_object_create(int argc, char **argv)
{
	int err = 0;
	char *dsname = argv[1];

	printf("creating object %s\n", dsname);

	libuzfs_dataset_handle_t *dhp = libuzfs_dataset_open(dsname);
	if (!dhp) {
		printf("failed to open dataset: %s\n", dsname);
		return (-1);
	}

	uint64_t obj = 0;

	err = libuzfs_object_create(dhp, &obj);
	if (err)
		printf("failed to create object on dataset: %s\n", dsname);
	else
		printf("created object %s:%ld\n", dsname, obj);

	libuzfs_dataset_close(dhp);

	return (err);
}

int
uzfs_object_delete(int argc, char **argv)
{
	int err = 0;
	char *dsname = argv[1];
	uint64_t obj = atoi(argv[2]);

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
	uint64_t obj = atoi(argv[2]);

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
	uint64_t obj = atoi(argv[2]);

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
	int err = 0;
	char *dsname = argv[1];
	uint64_t obj = atoi(argv[2]);
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

	err = libuzfs_object_read(dhp, obj, offset, size, buf);
	if (err)
		printf("failed to read object: %s:%ld\n", dsname, obj);
	else
		printf("read %s: %ld, off: %d, size: %d\n%s\n", dsname, obj,
		    offset, size, buf);

	umem_free(buf, size + 1);
	libuzfs_dataset_close(dhp);
	return (0);
}

int
uzfs_object_write(int argc, char **argv)
{
	int err = 0;
	char *dsname = argv[1];
	uint64_t obj = atoi(argv[2]);
	int offset = atoi(argv[3]);
	int size = strlen(argv[4]);
	char *buf = NULL;

	printf("writing %s: %ld, off: %d, size: %d\n", dsname, obj, offset,
	    size);

	libuzfs_dataset_handle_t *dhp = libuzfs_dataset_open(dsname);
	if (!dhp) {
		printf("failed to open dataset: %s\n", dsname);
		return (-1);
	}

	buf = umem_alloc(size, UMEM_NOFAIL);
	memcpy(buf, argv[4], size);

	err = libuzfs_object_write(dhp, obj, offset, size, buf);
	if (err)
		printf("failed to write object: %s:%ld\n", dsname, obj);

	umem_free(buf, size);
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

	err = libuzfs_zap_create(dhp, &obj);
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
	uint64_t obj = atoi(argv[2]);

	printf("destroying zap object %s:%ld\n", dsname, obj);

	libuzfs_dataset_handle_t *dhp = libuzfs_dataset_open(dsname);
	if (!dhp) {
		printf("failed to open dataset: %s\n", dsname);
		return (-1);
	}

	err = libuzfs_zap_delete(dhp, obj);
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
	uint64_t obj = atoi(argv[2]);
	char *key = argv[3];
	uint64_t value = atoi(argv[4]);

	printf("add entry %s:%ld to zap object %s:%ld\n", key, value, dsname, obj);

	libuzfs_dataset_handle_t *dhp = libuzfs_dataset_open(dsname);
	if (!dhp) {
		printf("failed to open dataset: %s\n", dsname);
		return (-1);
	}

	err = libuzfs_zap_add(dhp, obj, key, 8, 1, &value);
	if (err)
		printf("failed to add entry to zap object: %s:%ld\n", dsname, obj);

	libuzfs_dataset_close(dhp);
	return (0);
}

int
uzfs_zap_remove(int argc, char **argv)
{
	int err = 0;
	char *dsname = argv[1];
	uint64_t obj = atoi(argv[2]);
	char *key = argv[3];

	printf("remove entry %s from zap object %s:%ld\n", key, dsname, obj);

	libuzfs_dataset_handle_t *dhp = libuzfs_dataset_open(dsname);
	if (!dhp) {
		printf("failed to open dataset: %s\n", dsname);
		return (-1);
	}

	err = libuzfs_zap_remove(dhp, obj, key);
	if (err)
		printf("failed to remove entry from zap object: %s:%ld\n", dsname, obj);

	libuzfs_dataset_close(dhp);
	return (0);
}

int
uzfs_zap_update(int argc, char **argv)
{
	int err = 0;
	char *dsname = argv[1];
	uint64_t obj = atoi(argv[2]);
	char *key = argv[3];
	uint64_t value = atoi(argv[4]);

	printf("update entry %s:%ld to zap object %s:%ld\n", key, value, dsname, obj);

	libuzfs_dataset_handle_t *dhp = libuzfs_dataset_open(dsname);
	if (!dhp) {
		printf("failed to open dataset: %s\n", dsname);
		return (-1);
	}

	err = libuzfs_zap_update(dhp, obj, key, 8, 1, &value);
	if (err)
		printf("failed to update entry to zap object: %s:%ld\n", dsname, obj);

	libuzfs_dataset_close(dhp);
	return (0);
}

int
uzfs_zap_lookup(int argc, char **argv)
{
	int err = 0;
	char *dsname = argv[1];
	uint64_t obj = atoi(argv[2]);
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
		printf("failed to lookup entry to zap object: %s:%ld\n", dsname, obj);
	else
		printf("lookup entry to zap object: %s:%ld, %s:%ld\n", dsname, obj, key, value);

	libuzfs_dataset_close(dhp);
	return (0);
}

int
uzfs_zap_count(int argc, char **argv)
{
	int err = 0;
	char *dsname = argv[1];
	uint64_t obj = atoi(argv[2]);
	uint64_t count = 0;

	printf("count entry from zap object %s:%ld\n", dsname, obj);

	libuzfs_dataset_handle_t *dhp = libuzfs_dataset_open(dsname);
	if (!dhp) {
		printf("failed to open dataset: %s\n", dsname);
		return (-1);
	}

	err = libuzfs_zap_count(dhp, obj, &count);
	if (err)
		printf("failed to count entry to zap object: %s:%ld\n", dsname, obj);
	else
		printf("zap object: %s:%ld, count: %ld\n", dsname, obj, count);

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


static void print_stat(const char* name, struct stat* stat)
{
    const char* format =
        "  File: %s(%s)\n"
        "  Inode: %lld\n"
        "  MODE: %x\n"
        "  Links: %lld\n"
        "  UID/GID: %d/%d\n"
        "  SIZE: %d\n"
        "  BLOCKSIZE: %d\n"
        "  BLOCKS: %d\n";

    const char* type = S_ISDIR(stat->st_mode) ? "DIR" : "REG FILE";

    printf(format, name, type, stat->st_ino, stat->st_mode, stat->st_nlink, stat->st_uid, stat->st_gid,
           stat->st_size, stat->st_blksize, stat->st_blocks);
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
	memset(&buf, 0, sizeof(struct stat));

	error = libuzfs_getattr(fsid, ino, &buf);
	if (error) {
		printf("Failed to stat %s\n", path);
		goto out;
	}

	print_stat(s, &buf);

out:

	libuzfs_fs_fini(fsid);

	return error;
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

	return error;

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

	return error;
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
	memset(&buf, 0, sizeof(struct stat));

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

	return error;
}

static int uzfs_dir_emit(void *ctx, const char *name, int namelen, loff_t off, uint64_t ino, unsigned type)
{
	printf("\t%s\tobjnum: %ld\n", name, ino);
	return 0;
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
	memset(&buf, 0, sizeof(struct stat));

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

	return error;
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

    return error;

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

	printf("read %s: %s, offset: %d, size: %d\n", fsname, target_path, offset, size);

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
	zfs_uio_iovec_init(&uio, &iov, 1, offset, UIO_USERSPACE, iov.iov_len, 0);

	error = libuzfs_read(fsid, ino, &uio, 0);
	if (error) {
		printf("Failed to read %s\n", path);
		goto out;
	}

	printf("%s\n", buf);

	libuzfs_fs_fini(fsid);

out:
	return error;
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
	zfs_uio_iovec_init(&uio, &iov, 1, offset, UIO_USERSPACE, iov.iov_len, 0);

	error = libuzfs_write(fsid, ino, &uio, 0);
	if (error) {
		printf("Failed to write %s\n", path);
		goto out;
	}

	libuzfs_fs_fini(fsid);

out:
	return error;
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
	return error;
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
    struct perf_args* args = perf_args;
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
            memset(&buf, 0, sizeof(struct stat));

            error = libuzfs_getattr(fsid, ino, &buf);
            if (error) {
                printf("Failed to stat %s\n", name);
                goto out;
            }
            //print_stat(name, &buf);
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
    return NULL;
}

static int uzfs_perf(int argc, char **argv)
{
    int error = 0;
    char *path = argv[1];
    int op = atoi(argv[2]); // 0: rmdir, 1: mkdir, 2: remove file, 3: create file, 4: stat
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
            return -1;
    }

    printf("%s %s: %s\n", opstr, fsname, target_path);

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

    struct timeval t1,t2;
    double timeuse;
    gettimeofday(&t1,NULL);

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
        error = pthread_create(&ntids[i], NULL, do_perf, (void*)&args[i]);
        if  (error != 0) {
            printf("Failed to create thread: %s\n" ,  strerror (error));
            goto out;
        }

    }
    for (i = 0; i < n_threads; i++) {
        pthread_join(ntids[i],NULL);
    }

    end = clock();
    gettimeofday(&t2,NULL);
    timeuse = (t2.tv_sec - t1.tv_sec) + (double)(t2.tv_usec - t1.tv_usec)/1000000.0;

    int totalnum = branch * depth * num * n_threads;
    double clockuse = ((double)(end - start))/CLOCKS_PER_SEC;
    double rate = totalnum / timeuse;
    printf("num: %d\ntime=%fs\nclock=%fs\nrate=%f\n", totalnum, timeuse, clockuse, rate);

out:

    libuzfs_fs_fini(fsid);

    return error;

}
