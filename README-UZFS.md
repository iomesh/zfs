![img](https://openzfs.github.io/openzfs-docs/_static/img/logo/480px-Open-ZFS-Secondary-Logo-Colour-halfsize.png)

This repo supports running ZFS in userland (uZFS).

This feature is **under development**. Follow below steps to play with it.

1. build it (assume you've already installed the dependency building standard ZFS)

- to use zfs_fuse, you will need to install fuse3-libs

```
./autogen.sh && ./configure && make -j4
```

2. play with it

```
# create a 100MB file as the backend device

$ truncate -s 104857600 /tmp/uzfs-dev.img

# create a zpool

$ ./cmd/zpool/zpool create -U testzp /tmp/uzfs-dev.img

# list the zpool

$ ./cmd/zpool/zpool list -U
NAME     SIZE  ALLOC   FREE  CKPOINT  EXPANDSZ   FRAG    CAP  DEDUP    HEALTH  ALTROOT
testzp    80M    93K  79.9M        -         -     0%     0%  1.00x    ONLINE  -

# create a ZFS filesystem

$ ./cmd/zfs/zfs create -U testzp/uzfs
filesystem successfully created, but it may only be mounted by root

# list the filesystem
$ ./cmd/zfs/zfs list -U
NAME          USED  AVAIL     REFER  MOUNTPOINT
testzp        129K  39.9M       24K  /testzp
testzp/uzfs    24K  39.9M       24K  /testzp/uzfs

# mount the filesystem with zfs_fuse

$ sudo su (use root to mount fuse)
$ ./cmd/zfs_fuse/zfs_fuse -s -o --zfsname=testzp/uzfs /mnt/test
$ mount | grep zfs_fuse
lt-zfs_fuse on /mnt/test type fuse.lt-zfs_fuse (rw,nosuid,nodev,relatime,user_id=0,group_id=0)

# play with the filesystem

$ echo "hello uzfs" > /mnt/test/hello.txt
$ ls /mnt/test/
hello.txt
$ cat /mnt/test/hello.txt
hello uzfs

...

# umount the filesystem

$ umount /mnt/test

# you can also use uzfs tool to access the filesystem

$ ./cmd/uzfs/uzfs ls testzp/uzfs:///
ls testzp/uzfs: /
	.	objnum: 34
	..	objnum: 34
	hello.txt	objnum: 2

$ ./cmd/uzfs/uzfs read testzp/uzfs:///hello.txt 0 10
read testzp/uzfs: /hello.txt, offset: 0, size: 10
hello uzfs

# destroy the system

$ ./cmd/zfs/zfs destroy -U testzp/uzfs

# destroy the zpool

$ ./cmd/zpool/zpool destroy -U testzp

# remove the backend file

$ rm /tmp/uzfs-dev.img
```
