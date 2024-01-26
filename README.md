# Overview
Cascade is an AI/ML application hosting framework powered by optimized RDMA data paths. It provides a K/V API for data manipulation in distributed memory and persistent storage. Besides the K/V API, Cascade allows injecting logic on the data paths for low-latency application. The highlights of Cascade's features include the following.

- DAG based ML application interface.
- High-throughput and low latency from zero-copy RDMA and NVMe storage layer.
- Timestamp-indexed versioning capability allows reproducing system states anytime in the past.
- Users can specify the Key and Value types of the K/V API.
- Users can configure the application layout using the group, subgroup, and shard concepts deriving from [Derecho](http://github.com/Derecho-Project/derecho).
- Cascade derives the same fault-tolerance model from [Derecho](http://github.com/Derecho-Project/derecho).

Please refer to our [paper](https://arxiv.org/pdf/2311.17329) for detailed information.

# Release status
As of December 2023, Cascade has the status of a high-quality beta: publicaly available and useable. The current `master` branch allows the development of sophisticated Cascade applications using C/C++ and Python.  Our Cornell deployment and performance evaluation is performed in a cluster computing Ubuntu Linux environment, and we test both with or without RDMA support. The master branch is rapidly approaching the level of stability as we intend for v1.0 release, although more burn-in testing is still required. In January 2024 we anticipate a v1.0 general release of Cascade.

Derecho, used by Cascade, is more mature, with significant use since its initial release in April 2019.  Derecho continues to evolve too, but the rate of new releases is lower.  

## Plan for v1.0
In January 2024 we anticipate a v1.0 general release of Cascade, based on a stable version of the current `master` branch.    Regression testing is underway on this release right now. Although we have decided to focus on Cascade's core functionality in this first general release, we note that many experimental features are available in v1.0, and can be tested or used by those who are interested.  The experimental functionality includes:
- C# support for both K/V store client and User-Defined Logic (UDL) development.
- Java language support for K/V store client development.
- A FUSE file system API for easy read-only access to Cascade data.   This supports most of the standard POSIX file system API, with the exception of partial file writes (files can be fully overwritten, but we do not allow individual blocks within a file to be separately updated).
- A high performance collective communication library called [DCCL](https://github.com/derecho-project/dccl).  DCCL is similar to NCCL/RCCL, and will be expanded to for integrated use with all elements of Cascade later in 2024.
- An object grouping mechanism called "affinity sets", with which data colocation can be fine-tuned to optimize performance when an application depends on multiple objects that would otherwise be spread around due to random hashing within the Cascade sharded K/V storage layer.

## Features Planned for v1.1

Release v1.0 is intended as a baseline.  Early in 2024 we anticipate a follow-on release v1.1 that will elevate additional features to full support.  Some of these are already in the system, while others are still being completed as of December 2023. One highlight will be support for user-defined logic (UDL) hosted in separate processes/docker containers for with end-to-end zero copy data paths.  A docker model can improve  security by isolating untrusted application code.  The approach also allows more parallelism than is possible with UDLs that are running within the Cascade address space per-se (in any single address the Python GIL limits parallelism). We plan to include the full affinity set feature and a sophisticated scheduler for cases where a compute cluster is shared. The following features might (still) be in 'experimental' state in v1.1
- DPDK support, which provides performance improvement over TCP in non-RDMA environments.
- A FUSE file system API for read/write access.
- DCCL

## Features Planned for v1.2

By early summer of 2024, we anticipate a v1.2 release that would include enhanced scheduling to cover streaming scenarios and support for "split" computations, in which one AI DFG spans nodes on an edge cluster and nodes in a cloud-hosted framework. The experimental features in v1.1 are planned to become standard features.

Additionally, we are expecting a new set of zero-copy optimizations to be included in this release, including support for the GPUDirect accelerated datapath and zero-copy integration with Derecho's multicast. Improvements more focused on devops will include application packaging, dynamic application management, and monitoring capabilities.

We recommend coordinating with [Weijia Song](mailto:songweijia@gmail.com) if you plan to experiment with beta features of the system.  Anyone planning to do so should also commit to posting issues on the GitHub issue tracker in the event of a bug, with a minimal example that triggers the problem (as few lines of code as you can manage).  

# Using Cascade
- Cascade can be used both as a service, and as a software library
  - Used as a service, the developer would work in a client/server model
  - Cascade as a library is primarily for internal use in creating the Cascade service.  However, this approach could be useful for creating other services that need to layer some other form of functionality over a K/V infrastructure.
- Cascade's most direct and efficient APIs aim at applications coded in C++, which is the language used the Cascade implementation.
  - Within C++, we have found it useful to combine Cascade with a language-integrated query library such as LINQ (we can support both cpplinq and boolinq).
  - Doing so permits the developer to treat collections of objects or object histories as sets of K/V tuples, describing "transformations" on the data much as we would in a database setting, and leaving the runtime to make scheduling and object placement decisions on our behalf.
  - [LINQ](https://docs.microsoft.com/en-us/dotnet/csharp/programming-guide/concepts/linq/) is closely related to models widely used in ML, such as the Spark concept of an RDD, or the TensorFlow model for tensors and sets of tensors. Cascade is currently enabled with LINQ data retrieving C++ API.
  - We do not plan to require use of LINQ, but we do think it lends itself to concise, elegant code.  We have extended the API to also support use from C\# via the .NET Core CLR. This allows for development of user-defined logic in C\# as well.
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
- Intel's regular expression library [Hyperscan](https://github.com/intel/hyperscan). For convenience, install it with [this script](scripts/prerequisites/install-hyperscan.sh). You need to install ragel compiler if you don't have it. On ubuntu, use `apt-get install ragel` to install it.
- [libfuse](https://github.com/libfuse) v3.9.3 or newer (Optional for file system API)
- [boolinq](https://github.com/k06a/boolinq) or newer (Optional for LINQ API)
- Python 3.8 or newer and [pybind11](https://github.com/pybind/pybind11) (Optional for Python API)
- OpenJDK 11.06 or newer. On Ubuntu, use `apt install openjdk-11-jdk` to install it. (Optional for Java API)
- .NET Framework 6x. Please follow the instructions from Microsoft to install it based on Linux distro [here](https://learn.microsoft.com/en-us/dotnet/core/install/linux-ubuntu). (Optional for C\# API)
- Derecho v2.4.0 Plesae follow this [document](http://github.com/Derecho-Project/derecho) to install Derecho. Note: this cascade version replies on Derecho commit ef3043f on the master branch.

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
cmake -Dpybind11_DIR=`pybind11-config --cmakedir` ..
```

3) Install Cascade
```
# make install
```
This will install the following cascade components:
- headers to `${CMAKE_INSTALL_INCLUDEDIR}/include/cascade`
- libraries to `${CMAKE_INSTALL_LIBDIR}`
- binaries(cascade_client, cascade_server, cascade_fuse_client, interactive_test.py, perf_test.py) to `${CMAKE_INSTALL_BINDIR}`
- python pip library `derecho.cascade`

# Usage
There are two ways to use Cascade in an application. You can use Cascade as a standalone service with pre-defined K/V types and configurable layout. Or, you can use the Cascade storage templates (defined in Cascade ) as building blocks to build the application using the Derecho group framework. Please refer to [Cascade service's README](https://github.com/Derecho-Project/cascade/tree/master/src/service) for using Cascade as a service and [cli_example README](https://github.com/Derecho-Project/cascade/tree/master/src/applications/tests/cascade_as_subgroup_classes) for using Cascade components to build your own binary with customized key type and value type.

Example C/C++ Cascade applications can be found [here](src/applications/standalone).

Example Python Cascade applications can be found [here](src/udl_zoo/python/cfg).

A more systematic user's guide is under preparation.

# New Features to Come
1) UDL containerization with MPROC support
2) DPDK support
3) Application Packaging and management
4) Resource Management
5) GPUDirect support
6) Multiple network support
7) Kafka API
