#!/usr/bin/env python3

import errno
import filecmp
import getopt
import multiprocessing
import os
import queue
import shutil
import stat
import subprocess
import sys
import tempfile
import threading
import time
from contextlib import contextmanager
from os.path import join as pjoin
from tempfile import NamedTemporaryFile

from derecho.cascade.client import ServiceClientAPI
from util import (
    base_cmdline,
    basename,
    cleanup,
    compare_dirs,
    fuse_proto,
    powerset,
    safe_sleep,
    test_printcap,
    umount,
    wait_for_mount,
)

TEST_FILE = __file__

with open(TEST_FILE, "rb") as fh:
    TEST_DATA = fh.read()


def name_generator(__ctr=[0]):
    __ctr[0] += 1
    return "testfile_%d" % __ctr[0]


options = []
if sys.platform == "linux":
    options.append("clone_fd")


def invoke_directly(mnt_dir, name, options):
    cmdline = base_cmdline + [
        pjoin(basename, "example", name),
        "-f",
        mnt_dir,
        "-o",
        ",".join(options),
    ]
    if name == "cascade_fuse_client":
        # supports single-threading only
        cmdline.append("-s")
    return cmdline


def readdir_inode(dir):
    cmd = base_cmdline + [pjoin(basename, "test", "readdir_inode"), dir]
    with subprocess.Popen(cmd, stdout=subprocess.PIPE, universal_newlines=True) as proc:
        lines = proc.communicate()[0].splitlines()
    lines.sort()
    return lines


def error_msg(run_id, testcase, output, expected):
    msg = "[" + run_id + "]" + testcase + "(output--expected):"
    msg += "(" + ",".join(output)
    msg += " -- " + ",".join(expected) + ")"
    return msg


put_keys = ["key0", "/a/a1/key0", "/b/b1/key1", "/c/c1/c2/key2"]
put_values = ["00", "00000000", "11111111", "22222222"]

vcss_expected_keys = [".cascade", "key-key0", "key-\\a\\a1\\key0"]
pcss_expected_keys = [".cascade", "key-\\b\\b1\\key1", "key-\\c\\c1\\c2\\key2"]

objp_expected_second_level = [".cascade", "a", "b", "c"]
objp_expected_third_level = [
    [],
    [".cascade", "a1"],
    [".cascade", "b1"],
    [".cascade", "c1"],
]
objp_expected_keys = [
    [],
    [".cascade", "key0"],
    [".cascade", "key1"],
    [".cascade", "c2"],
]
objp_expected_keys2 = [".cascade", "key2"]


def initial_setup():
    capi = ServiceClientAPI()
    print("----------- CASCADE INITIAL SETUP   -----------")
    capi.put(
        put_keys[0],
        bytes(put_values[0], "utf-8"),
        subgroup_type="VolatileCascadeStoreWithStringKey",
        subgroup_index=0,
        shard_index=0,
    )
    capi.create_object_pool("/a/a1", "VolatileCascadeStoreWithStringKey", 0)
    capi.create_object_pool("/b/b1", "PersistentCascadeStoreWithStringKey", 0)
    capi.create_object_pool("/c/c1/c2", "PersistentCascadeStoreWithStringKey", 0)
    time.sleep(5)

    capi.put(put_keys[1], bytes(put_values[1], "utf-8"), blocking=True)
    capi.put(put_keys[2], bytes(put_values[2], "utf-8"), blocking=True)
    res = capi.put(put_keys[3], bytes(put_values[3], "utf-8"), blocking=True)
    if res:
        odict = res.get_result()
        # print("------- THRID put result: " + f"{str(odict)}")


# Test 1. check the directory for Cascade subgroups
def tst_sg_dirs(mnt_dir, run_id=""):
    # First-level(subgroup_type) check
    mnt_first_level = os.listdir(mnt_dir)
    expected_first_level = [
        ".cascade",
        "MetadataService",
        "PersistentCascadeStoreWithStringKey",
        "VolatileCascadeStoreWithStringKey",
        "TriggerCascadeNoStoreWithStringKey",
        "ObjectPools",
    ]
    error_m = error_msg(run_id, "first level", mnt_first_level, expected_first_level)
    assert compare_dirs(mnt_first_level, expected_first_level), error_m
    sg_expected_second_level = ["subgroup-0"]
    sg_expected_third_level = ["shard-0"]
    # Second-level(subgroup_index) check
    for subgroup_type in expected_first_level:
        if (
            subgroup_type == ".cascade"
            or subgroup_type == "MetadataService"
            or subgroup_type == "ObjectPools"
        ):
            continue
        mnt_second_level = os.listdir(mnt_dir + "/" + subgroup_type)
        assert compare_dirs(mnt_second_level, sg_expected_second_level)
        # Third-level(shard) check
        mnt_third_level = os.listdir(
            mnt_dir + "/" + subgroup_type + "/" + sg_expected_second_level[0]
        )
        assert compare_dirs(mnt_third_level, sg_expected_third_level)
        # Forth-level(key) check
        if subgroup_type == "PersistentCascadeStoreWithStringKey":
            mnt_forth_level = os.listdir(
                mnt_dir
                + "/"
                + subgroup_type
                + "/"
                + sg_expected_second_level[0]
                + "/"
                + sg_expected_third_level[0]
            )
            error_m = error_msg(
                run_id, "PCSS forthlevel", mnt_forth_level, pcss_expected_keys
            )
            assert compare_dirs(mnt_forth_level, pcss_expected_keys), error_m
        if subgroup_type == "VolatileCascadeStoreWithStringKey":
            mnt_forth_level = os.listdir(
                mnt_dir
                + "/"
                + subgroup_type
                + "/"
                + sg_expected_second_level[0]
                + "/"
                + sg_expected_third_level[0]
            )
            assert compare_dirs(mnt_forth_level, vcss_expected_keys)
    print("[" + run_id + "]" + "--- Test1 SubgroupTypes directories pass! ---")


# Test 2. check the directory for Cascade object_pools
def tst_objp_dirs(mnt_dir, run_id=""):
    mnt_objp_root = os.listdir(mnt_dir + "/" + "ObjectPools")
    error_m = error_msg(
        run_id, "object pool test", mnt_objp_root, objp_expected_second_level
    )
    assert compare_dirs(mnt_objp_root, objp_expected_second_level), error_m
    for i in range(1, 4):
        mnt_objp_dir = os.listdir(
            mnt_dir + "/" + "ObjectPools" + "/" + objp_expected_second_level[i]
        )
        error_m = error_msg(
            run_id,
            f"object pool test{str(i)}",
            mnt_objp_dir,
            objp_expected_third_level[i],
        )
        assert compare_dirs(mnt_objp_dir, objp_expected_third_level[i])
        mnt_objp_key = os.listdir(
            mnt_dir
            + "/"
            + "ObjectPools"
            + "/"
            + objp_expected_second_level[i]
            + "/"
            + objp_expected_third_level[i][1]
        )
        assert compare_dirs(mnt_objp_key, objp_expected_keys[i])
        if i == 3:
            mnt_objp_subdir = os.listdir(
                mnt_dir
                + "/"
                + "ObjectPools"
                + "/"
                + objp_expected_second_level[i]
                + "/"
                + objp_expected_third_level[i][1]
                + "/"
                + objp_expected_keys[i][1]
            )
            assert compare_dirs(mnt_objp_subdir, objp_expected_keys2)
    print("[" + run_id + "]" + "--- Test2 ObjectPools directories pass! ---")


# Test 3. check the open and read content of files
def tst_content(mnt_dir, run_id=""):
    # 1. subgroup content test
    subgroup_type = "/VolatileCascadeStoreWithStringKey/"
    for i in range(1, 2):
        sg_mnt_key = (
            mnt_dir + subgroup_type + "subgroup-0/shard-0/" + vcss_expected_keys[i + 1]
        )
        with open(sg_mnt_key, "r") as fh:
            output = fh.read()
            error_m = error_msg(
                run_id, f"sg content{sg_mnt_key}", output, put_values[i]
            )
            assert output == put_values[i], error_m
    subgroup_type = "/PersistentCascadeStoreWithStringKey/"
    for i in range(2, 4):
        sg_mnt_key = (
            mnt_dir + subgroup_type + "subgroup-0/shard-0/" + pcss_expected_keys[i - 1]
        )
        with open(sg_mnt_key, "r") as fh:
            output = fh.read()
            error_m = error_msg(
                run_id, f"sg content{sg_mnt_key}", output, put_values[i]
            )
            assert output == put_values[i], error_m
    # 2. Objectpool content test
    object_pool_dir = "/ObjectPools"
    for i in range(1, 4):
        objp_mnt_key = mnt_dir + object_pool_dir + put_keys[i]
        with open(objp_mnt_key, "r") as fh:
            output = fh.read()
            error_m = error_msg(
                run_id, f"objp content{objp_mnt_key}", output, put_values[i]
            )
            assert output == put_values[i], error_m
    print("[" + run_id + "]" + "--- Test3 read content pass! ---")


# def tst_attr(mnt_dir):


def cascade_fuse_mount(mnt_dir):
    # 1. create mounting piont
    subprocess.Popen(["mkdir", mnt_dir])
    cmd_input = base_cmdline + [pjoin(basename, "cascade_fuse_client"), mnt_dir, "-f"]
    # 2. run cascade_fuse_client
    mount_process = subprocess.Popen(
        cmd_input, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    # try:
    wait_for_mount(mount_process, mnt_dir)
    # except:
    #     cleanup(mount_process, mnt_dir)
    #     raise
    # else:
    #     umount(mnt_dir)


def cascade_fuse_umount(mnt_dir):
    # 1. umount the mounting process at mnt_dir
    umount(mnt_dir)
    # 2. delete the mounted directory
    subprocess.Popen(["rm", "-r", mnt_dir])
    print(" ~~~~~~~~~~~~~  FINISHED cascade_fuse_client unmount  ~~~~~~~~~~~\n")


def fuse_test_cases(mnt_dir, run_id):
    tst_sg_dirs(mnt_dir, run_id)
    tst_objp_dirs(mnt_dir, run_id)
    tst_content(mnt_dir, run_id)
    # tst_attr(mnt)


def main(argv):

    process_num = 1
    thread_num = 1
    try:
        opts, args = getopt.getopt(argv, "ht:p:", ["threadN=", "processN="])
    except getopt.GetoptError:
        print("fusetest.py  -t <threadNumber> -p <processNumber>")
        sys.exit(2)
    for opt, arg in opts:
        if opt == "-h":
            print("fusetest.py  -t <threadNumber> -p <processNumber>")
            sys.exit()
        elif opt in ("-t", "--ifile"):
            thread_num = int(arg)
        elif opt in ("-p", "--ofile"):
            process_num = int(arg)

    cascade_process = multiprocessing.Process(target=initial_setup)
    cascade_process.start()
    cascade_process.join()

    mnt_dir = pjoin(
        basename, "testdir"
    )  # use build/src/service/fuse/testdir as mounting point
    fuse_process = multiprocessing.Process(target=cascade_fuse_mount, args=(mnt_dir,))
    fuse_process.start()
    fuse_process.join()  # used when fuse not run in foreground in fuse_client.cpp

    # multi-processing test cases
    test_processes = []
    for i in range(process_num):
        run_id = "process_" + str(i)
        proc = multiprocessing.Process(
            target=fuse_test_cases,
            args=(
                mnt_dir,
                run_id,
            ),
        )
        test_processes.append(proc)
        proc.start()
    for proc in test_processes:
        proc.join()

    # multi-threading test cases
    test_threads = []
    for i in range(thread_num):
        run_id = "thread_" + str(i)
        thread = threading.Thread(
            target=fuse_test_cases,
            args=(
                mnt_dir,
                run_id,
            ),
        )
        test_threads.append(thread)
        thread.start()
    for thread in test_threads:
        thread.join()

    # fuse_process.terminate()
    cascade_fuse_umount(mnt_dir)


if __name__ == "__main__":
    main(sys.argv[1:])
