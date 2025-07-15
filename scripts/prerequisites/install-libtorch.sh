#!/bin/bash

if [ $# -lt 1 ]; then
  echo "USAGE: $0 <cpu|gpu>"
  exit 0
fi

INSTALL_PREFIX="/usr/local"
if [[ $# -gt 1 ]]; then
    INSTALL_PREFIX=$2
fi
INSTALL_TYPE=$1
ZIP_FILE=libtorch-cxx11.zip
if [ $INSTALL_TYPE == 'cpu' ]; then
    wget -O $ZIP_FILE https://download.pytorch.org/libtorch/cpu/libtorch-cxx11-abi-shared-with-deps-1.8.1%2Bcpu.zip
else
    wget -O $ZIP_FILE https://download.pytorch.org/libtorch/cu111/libtorch-cxx11-abi-shared-with-deps-1.8.1%2Bcu111.zip
fi
unzip $ZIP_FILE
cp -ar libtorch/* ${INSTALL_PREFIX}
rm -rf libtorch
