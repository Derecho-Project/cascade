#!/bin/bash
INSTALL_PREFIX=${HOME}/opt-dev

wget https://www.cs.umd.edu/~mount/ANN/Files/1.1.2/ann_1.1.2.tar.gz
tar -xf ann_1.1.2.tar.gz
cd ann_1.1.2
patch Make-config ../ann_1.1.2.Make-config.patch
make linux-g++
cp -ar include ${INSTALL_PREFIX}
cp -ar lib ${INSTALL_PREFIX}
cp -ar bin ${INSTALL_PREFIX}
