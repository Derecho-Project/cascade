#!/bin/bash
OPT_HOME=$HOME/opt/test_install
# OPT_HOME=$HOME/opt-dbg
export CMAKE_PREFIX_PATH=${OPT_HOME}/
export C_INCLUDE_PATH=${OPT_HOME}/include/
export CPLUS_INCLUDE_PATH=${OPT_HOME}/include/
export LIBRARY_PATH=${OPT_HOME}/lib/
export LD_LIBRARY_PATH=${OPT_HOME}/lib/
