# C\# .NET DLL Implementation

Last updated 12/1/2022.

This demo represents a "special" UDL responsible for integrating DLLs written in C\#. It has feature
parity with the C++ ocdpo_handler, and enables users to inject logic written in C\# to the Cascade
fast-path through the `dfgs.json` config. Refer to `examples/` for the two examples we have implemented
so far.

- Console Printer (trivial)
- Word Count (basic MapReduce)
- CosmosDB CRUD (TODO)

## Prerequisites

In order to run this, you have to have Derecho and Cascade installed, as well as their 
prerequisites. Additionally, you will need Mono, the open source .NET compiler for Linux, and also
the dotnet SDK.

## Notes

Currently, the path of the .NET Core is hardcoded in the `gateway_to_managed.cpp` as a directive
`CORECLR_DIR`. It is assumed that you are using Linux (this system has been tested on Ubuntu 20/22
in WSL).

## Design

TODO
