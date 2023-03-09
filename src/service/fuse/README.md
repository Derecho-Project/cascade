# Fuse API

Cascade service supports FUSE(Filesystem in Userspace) API to access data. It is
a standalone client application that links with
[`libfuse`](https://github.com/libfuse/libfuse). Cascade fuse api mount the file
system to interact with Cascade K/V stored objects.

If you didn't see
`-- Looking for include files fuse3/fuse.h, fuse3/fuse_lowlevel.h - found`, it
is probably because the libfuse support is not installed. FUSE application shall
be built as an executable in the built directory
`src/service/fuse/cascade_fuse_client`

Running FUSE POSIX shell application is similar to run cascade cmd client. Once
the cascade service is configured and started, in the client node, running
`../../fuse/cascade_fuse_client [directory_to_mount]` will mount cascade file
systsem to the dedicated directory.

Once fuse application is mounted to the directory, you can access the K/V object
stored in cascade using linux shell command. The structured of the mounted file
system is as following

```bash
.
|-- MetadataService
|-- PersistentCascadeStoreWithStringKey
|   |-- subgroup-0
|       |-- shard-0
|       |-- .cascade
|           |-- key-0
|       ...
|-- VolatileCascadeStoreWithStringKey
|   |-- subgroup-0
|       |-- shard-0
|       |-- .cascade
|       ...
|-- TriggerCascadeStoreWithStringKey
|-- ObjectPools
|   |-- objectpoolpathname-a
|       |-- a1
|           |-- objectpool object 0
|           ...
|           |-- .cascade
|   |-- .cascade
|-- .cascade
```

Support READ commands:

```
cd [dir]        open directory
ls [dir]        list directory
ls -a           list directory with attributes
cat [file]      read file
cat .cascade    read directory metadata information
```

Limitation:

- Support only single-threaded fuse client
- Current read_file keeps a buffer of file_bytes in memory, needs further
  optimization to read large file
- New features to come: WRITE commands to have fuse client interact with cascade
  File write and editing commands, managing directories commands
