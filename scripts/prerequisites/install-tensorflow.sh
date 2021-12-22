#!/bin/bash
if [ $# -lt 2 ]; then
  echo "USAGE: $0 <version> <cpu|gpu>"
  echo "Version example:2.4.1"
  exit -1
fi

# pip3 install tensorflow==${VERSION}
INSTALL_PREFIX=$HOME/opt-dev/
VERSION=$1
ENGINE=$2
FILENAME=libtensorflow-${ENGINE}-linux-x86_64-${VERSION}.tar.gz
wget https://storage.googleapis.com/tensorflow/libtensorflow/${FILENAME}

tar -C ${INSTALL_PREFIX} -xzf ${FILENAME}
