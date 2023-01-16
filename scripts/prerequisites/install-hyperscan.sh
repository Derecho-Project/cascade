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
git clone https://github.com/songweijia/hyperscan.git
cd hyperscan
git checkout 837992d79c49bc9bce04b19acc3d1275d96b5fd5
mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=${INSTALL_PREFIX} ..
make -j `nproc`
make install
