#!/bin/bash
set -eu
export TMPDIR=/var/tmp
WORKPATH=`mktemp -d`
INSTALL_PREFIX="/usr/local"
if [[ $# -gt 0 ]]; then
    INSTALL_PREFIX=$1
fi

echo "Using INSTALL_PREFIX=${INSTALL_PREFIX}"

cd ${WORKPATH}
git clone https://github.com/rpclib/rpclib.git
cd rpclib
git checkout tags/v2.3.0
mkdir build
cd build
cmake -DCMAKE_INSTALL_PREFIX=${INSTALL_PREFIX} ..
make -j `nproc`
make install
