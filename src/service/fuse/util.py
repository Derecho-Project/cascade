#!/usr/bin/env python3
import itertools
import os
import re
import stat
import subprocess
import sys
import time
from os.path import join as pjoin

import pytest

basename = os.path.dirname(os.path.abspath(__file__))
fusermount3_dir = "/home/yy354/opt-dev/bin"


def test_printcap():
    cmdline = base_cmdline + [pjoin(basename, "example", "printcap")]
    proc = subprocess.Popen(cmdline, stdout=subprocess.PIPE, universal_newlines=True)
    (stdout, _) = proc.communicate(30)
    assert proc.returncode == 0

    proto = None
    caps = set()
    for line in stdout.split("\n"):
        if line.startswith("\t"):
            caps.add(line.strip())
            continue

        hit = re.match(r"Protocol version: (\d+)\.(\d+)$", line)
        if hit:
            proto = (int(hit.group(1)), int(hit.group(2)))

    return (proto, caps)


def wait_for_mount(mount_process, mnt_dir, test_fn=os.path.ismount):
    elapsed = 0
    while elapsed < 20:
        if test_fn(mnt_dir):
            print(" -------- Fuse mount succeed -------")
            return True
        if mount_process.poll() is not None:
            pytest.fail("file system process terminated prematurely")
        time.sleep(0.1)
        elapsed += 0.1
    pytest.fail("mountpoint failed to come up")


def compare_dirs(output, expected):
    for dir in output:
        if not dir in expected:
            return False
    for dir in expected:
        if not dir in output:
            return False
    return True


def cleanup(mount_process, mnt_dir):
    # Don't bother trying Valgrind if things already went wrong

    if "bsd" in sys.platform or "dragonfly" in sys.platform:
        cmd = ["umount", "-f", mnt_dir]
    else:
        cmd = [pjoin(basename, "util", "fusermount3"), "-z", "-u", mnt_dir]
    subprocess.call(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
    mount_process.terminate()
    try:
        mount_process.wait(1)
    except subprocess.TimeoutExpired:
        mount_process.kill()


def umount(mnt_dir):

    if "bsd" in sys.platform or "dragonfly" in sys.platform:
        cmdline = ["umount", mnt_dir]
    else:
        # fusermount3 will be setuid root, so we can only trace it with
        # valgrind if we're root
        if os.getuid() == 0:
            cmdline = base_cmdline
        else:
            cmdline = []
        cmdline = cmdline + [pjoin(fusermount3_dir, "fusermount3"), "-z", "-u", mnt_dir]
    subprocess.check_call(cmdline)
    assert not os.path.ismount(mnt_dir)


def safe_sleep(secs):
    """Like time.sleep(), but sleep for at least *secs*
    `time.sleep` may sleep less than the given period if a signal is
    received. This function ensures that we sleep for at least the
    desired time.
    """

    now = time.time()
    end = now + secs
    while now < end:
        time.sleep(end - now)
        now = time.time()


def powerset(iterable):
    s = list(iterable)
    return itertools.chain.from_iterable(
        itertools.combinations(s, r) for r in range(len(s) + 1)
    )


# Use valgrind if requested
if os.environ.get("TEST_WITH_VALGRIND", "no").lower().strip() not in (
    "no",
    "false",
    "0",
):
    base_cmdline = ["valgrind", "-q", "--"]
else:
    base_cmdline = []

# Try to use local fusermount3
os.environ["PATH"] = "%s:%s" % (pjoin(basename, "util"), os.environ["PATH"])
# Put example binaries on PATH
os.environ["PATH"] = "%s:%s" % (pjoin(basename, "example"), os.environ["PATH"])

try:
    (fuse_proto, fuse_caps) = test_printcap()
except:
    # Rely on test to raise error
    fuse_proto = (0, 0)
    fuse_caps = set()
