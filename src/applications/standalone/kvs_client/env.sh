#!/bin/bash
export LOCAL_OPT=$HOME/opt-dev
export TEST_INSTALL=$HOME/install-test
export C_INCLUDE_PATH=$LOCAL_OPT/include:$TEST_INSTALL/include
export CPLUS_INCLUDE_PATH=$LOCAL_OPT/include:$TEST_INSTALL/include
export CMAKE_PREFIX_PATH=$LOCAL_OPT/:$TEST_INSTALL
export LIBRARY_PATH=$LOCAL_OPT/lib/:$TEST_INSTALL/lib
export LD_LIBRARY_PATH=$LOCAL_OPT/lib/:$TEST_INSTALL/lib
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64/:
