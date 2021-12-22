#!/bin/bash
export INSTALL_PREFIX=$HOME/opt-dev/
git clone https://github.com/serizba/cppflow.git
cd cppflow
git checkout 9ea9519c66d9b1893e2d298db8aa1ee866f903a2
cp -a include ${INSTALL_PREFIX}
