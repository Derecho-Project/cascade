#!/bin/bash
apt install -y libopenblas-dev libopencv-dev python3 python-is-python3
git clone https://github.com/apache/incubator-mxnet.git
cd incubator-mxnet
git checkout tags/1.7.0
git submodule update --init
cd 3rdparty/dmlc-core/
git apply ../../../dmlc-core.diff
cd ../../
cmake -DUSE_CUDA=0 -DENABLE_CUDA_RTC=0 -DUSE_JEMALLOC=0 -DUSE_CUDNN=0 -DUSE_MKLDNN=0 -DUSE_CPP_PACKAGE=1
make -j `nproc`
make install
cd cpp-package
make
make install
cd ../..
