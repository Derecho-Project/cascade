#!/usr/bin/env bash
set -eu
export TMPDIR=/var/tmp
WORKPATH=`mktemp -d`
INSTALL_PREFIX="/usr/local"
if [[ $# -gt 0 ]]; then
    INSTALL_PREFIX=$1
fi

echo "Using INSTALL_PREFIX=${INSTALL_PREFIX}"

cd ${WORKPATH}
git clone https://github.com/k06a/boolinq.git
cd boolinq
cp -r include/ ${INSTALL_PREFIX}
rm -rf ${WORKPATH}
