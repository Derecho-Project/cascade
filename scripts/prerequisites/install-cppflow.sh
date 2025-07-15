#!/bin/bash
INSTALL_PREFIX="/usr/local"
if [[ $# -gt 0 ]]; then
    INSTALL_PREFIX=$1
fi
git clone https://github.com/serizba/cppflow.git
cd cppflow
git checkout 9ea9519c66d9b1893e2d298db8aa1ee866f903a2
cp -a include ${INSTALL_PREFIX}
