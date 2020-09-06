# Overview
Cascade is a C++17 cloud application framework powered by optimized RDMA data paths. It provides a K/V API for data manipulation in distributed memory and persistent storage. Besides the K/V API, Cascade allows injecting logic on the data paths for low-latency application. The highlights of Cascade's features include the following.

- High-throughput and low latency from zero-copy RDMA and NVMe storage layer.
- Timestamp-indexed versioning capability allows reproducing system states anytime in the past.
- Users can specify the Key and Value types of the K/V API.
- Users can configure the application layout using the group, subgroup, and shard concepts deriving from [Derecho](http://github.com/Derecho-Project/derecho).
- Cascade derives the same fault-tolerance model from [Derecho](http://github.com/Derecho-Project/derecho).
- Cascade's K/V API supports high-level language like Java and Python (Currently in the [Java](https://github.com/Derecho-Project/cascade/tree/java) and [Python](https://github.com/Derecho-Project/cascade/tree/python) branch).
- Cascade supports a File system API based on the K/V API through [libfuse](https://github.com/libfuse/libfuse).

# Installation

## Prerequisites
- Linux (other operating systems don't currently support the RDMA features we use. We recommand Ubuntu18 or Ubuntu20. However, other distributions should also work.)
- A C++ compiler supporting C++17: GCC 7.3+ or Clang 7+
- CMake 3.10 or newer
- [Lohmann's json parser](https://github.com/nlohmann) v3.2.0 or newer
- [libfuse](https://github.com/libfuse) v3.9.3 or newer (Optional for file system API)
- Python 3.5 or newer and [pybind11](https://github.com/pybind/pybind11) (Optional for Python API)
- OpenJDK 11.06 or newer. On Ubuntu, use `apt install openjdk-11-jdk` to install it. (Optional for Java API)
- [Boolinq 3.0.1](https://github.com/k06a/boolinq) or newer. (Optional for LINQ API)
- Derecho v2.0.1. Plesae following this [document](http://github.com/Derecho-Project/derecho) to install Derecho.

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
3) Install Cascade
```
# make install
```

# Usage
There are two ways to use Cascade in an application. You can use Cascade as a standalone service with pre-defined K/V types and configurable layout. Or, you can use the Cascade storage templates (defined in Cascade ) as building blocks to build the application using the Derecho group framework. Please refer to [Cascade service's README](https://github.com/Derecho-Project/cascade/tree/master/service) for using Cascade as a service and [cli_example README](https://github.com/Derecho-Project/cascade/tree/master/src/test) for using Cascade components to build your own binary with customized Key type and Value type.

# New Features to Come
1) A [LINQ](https://docs.microsoft.com/en-us/dotnet/csharp/programming-guide/concepts/linq/) API over the K/V API (Under construction).
