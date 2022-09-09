# Overview
Cascade is a C++17 cloud application framework powered by optimized RDMA data paths. It provides a K/V API for data manipulation in distributed memory and persistent storage. Besides the K/V API, Cascade allows injecting logic on the data paths for low-latency application. The highlights of Cascade's features include the following.

- High-throughput and low latency from zero-copy RDMA and NVMe storage layer.
- Timestamp-indexed versioning capability allows reproducing system states anytime in the past.
- Users can specify the Key and Value types of the K/V API.
- Users can configure the application layout using the group, subgroup, and shard concepts deriving from [Derecho](http://github.com/Derecho-Project/derecho).
- Cascade derives the same fault-tolerance model from [Derecho](http://github.com/Derecho-Project/derecho).

# Using Cascade
- Cascade can be used both as a service, and as a software library
-- Used as a service, the developer would work in a client/server model
-- The use of Cascade as a library is primarily for our own purposes, in creating the Cascade service.  However, this approach could be useful for creating other services that need to layer some other form of functionality over a K/V infrastructure.
- Cascade's most direct and efficient APIs aim at applications coded in C++, which is the language used the Cascade implementation.  
-- Within C++, we have found it useful to combine Cascade with a language-integrated query library such as LINQ (we can support both cpplinq and boolinq).  
-- Doing so permits the developer to treat collections of objects or object histories as sets of K/V tuples, describing "transformations" on the data much as we would in a database setting, and leaving the runtime to make scheduling and object placement decisions on our behalf.  
-- [LINQ](https://docs.microsoft.com/en-us/dotnet/csharp/programming-guide/concepts/linq/) is closely related to models widely used in ML, such as the Spark concept of an RDD, or the Tensor Flow model for tensors and sets of tensors. Cascade is currently enabled with LINQ data retrieving C++ API.
-- We do not plan to require use of LINQ, but we do think it lends itself to concise, elegant code.  During summer 2022 we are extending the API to also support use from C\# via the .NET Core CLR (F# and C++/CLI should also be able to access these extensions).
- Cascade also supports a variety of remoting options.  Through them, Cascade's K/V API can be accessed from other popular high-level languages, notably Java and Python.
- Cascade also offers a File system API that maps to its K/V API through [libfuse](https://github.com/libfuse/libfuse).  

# Installation

## Prerequisites
- Linux (other operating systems don't currently support the RDMA features we use. We recommend Ubuntu18 or Ubuntu20. However, other distributions should also work.)
- A C++ compiler supporting C++17: GCC 8.3+
- CMake 3.10 or newer
- [Lohmann's json parser](https://github.com/nlohmann) v3.2.0 or newer
- Readline library v7.0 or newer. On Ubuntu, use `apt install libreadline-dev` to install it.
- RPC library [rpclib](https://github.com/rpclib/rpclib). For convenience, install it with [this script](scripts/prerequisites/install-rpclib.sh).
- [libfuse](https://github.com/libfuse) v3.9.3 or newer (Optional for file system API)
- [boolinq](https://github.com/k06a/boolinq) or newer (Optional for LINQ API)
- Python 3.5 or newer and [pybind11](https://github.com/pybind/pybind11) (Optional for Python API)
- OpenJDK 11.06 or newer. On Ubuntu, use `apt install openjdk-11-jdk` to install it. (Optional for Java API)
- Derecho v2.2.2. Plesae follow this [document](http://github.com/Derecho-Project/derecho) to install Derecho. Note: this cascade version replies on Derecho commit 3f24e06ed5ad572eb82206e8c1024935d03e903e on the master branch.

## Build Cascade
1) Download Cascade Source Code
```
# git clone https://github.com/Derecho-Project/cascade
```
2) Build Cascade source code
```
# mkdir build
# cd build
# cmake ..
# make -j
```
Please note that the cmake script will check whether the python3 environment (along with pybind11) is available or not. If python environment is not detected, the building process will disable python support quietly. If your `pybind11` is not installed in a standard place, which is very common if `pybind11` is install by `pip3`, you can use `-Dpybind11_DIR=` option for cmake to specify the location of pybind11 as following:
```
cmake -Dpybind11_DIR=/usr/local/lib/python3.8/dist-packages/pybind11/share/cmake/pybind11 ..
```

3) Install Cascade
```
# make install
```
This will install the following cascade components: 
- headers to `${CMAKE_INSTALL_INCLUDEDIR}/include/cascade`
- libraries to `${CMAKE_INSTALL_LIBDIR}`
- binaries(cascade_client, cascade_server, cascade_fuse_client, interactive_test.py, perf_test.py) to `${CMAKE_INSTALL_BINDIR}`

# Usage
There are two ways to use Cascade in an application. You can use Cascade as a standalone service with pre-defined K/V types and configurable layout. Or, you can use the Cascade storage templates (defined in Cascade ) as building blocks to build the application using the Derecho group framework. Please refer to [Cascade service's README](https://github.com/Derecho-Project/cascade/tree/master/src/service) for using Cascade as a service and [cli_example README](https://github.com/Derecho-Project/cascade/tree/master/src/applications/tests/cascade_as_subgroup_classes) for using Cascade components to build your own binary with customized key type and value type.

# New Features to Come
1) Resource management
