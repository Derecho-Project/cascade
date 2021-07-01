#!/bin/bash
if [ $# -lt 2 ]; then
  echo "USAGE: $0 <version> <cpu|gpu>"
  echo "version example:2.4.1"
  exit -1
fi

BUILD_TYPE=$2
export INSTALL_PREFIX=$HOME/opt-dev/

# install required tools
sudo apt install python3-dev python3-pip
sudo apt install apt-transport-https curl gnupg
curl -fsSL https://bazel.build/bazel-release.pub.gpg | gpg --dearmor > bazel.gpg
sudo mv bazel.gpg /etc/apt/trusted.gpg.d/
echo "deb [arch=amd64] https://storage.googleapis.com/bazel-apt stable jdk1.8" | tee /etc/apt/sources.list.d/bazel.list
sudo apt update
sudo apt install bazel-3.7.2
bazel="bazel-3.7.2"
sudo ln -s /usr/bin/bazel-3.7.2 /usr/bin/bazel
pip3 install numpy keras_preprocessing
# clone source code
git clone https://github.com/tensorflow/tensorflow.git
cd tensorflow
if [ $# -gt 1 ]; then
  git checkout tags/v$1
fi
# build
./configure
CONFIG=
if [ $BUILD_TYPE == 'gpu' ]; then
  CONFIG="--config=cuda"
fi
# build
$bazel build $CONFIG //tensorflow/tools/pip_package:build_pip_package
if [ $? -eq 0 ]; then
  ./bazel-bin/tensorflow/tools/pip_package/build_pip_package /tmp/tensorflow_pkg
else
  echo "Failed to build pip package"
  exit $?
fi
if [ $? -eq 0 ]; then
  pip3 install `ls /tmp/tensorflow_pkg/*.whl`
else
  echo "Failed to generate pip package"
  exit $?
fi
if [ $? -eq 0 ]; then
  $bazel test --config opt //tensorflow/tools/lib_package:libtensorflow_test
  $bazel build --config opt //tensorflow/tools/lib_package:libtensorflow
else
  echo "Failed to pip3 install the built tensorflow"
fi
if [ $? -eq 0 ]; then
  tar -C ${INSTALL_PREFIX} -zxf bazel-bin/tensorflow/tools/lib_package/libtensorflow.tar.gz
else
  echo "Failed to build C bindings."
fi
if [ $? -eq 0 ]; then
  pip3 install numpy --upgrade
else
  echo "Failed to install libtensorflow."
fi
if [ $? -eq 0 ]; then
  echo "Successfully installed libtensorflow."
else
  echo "Failed to upgrade numpy"
fi
