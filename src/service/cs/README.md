# C\# API

Last updated 3/13/2023 by `ptwu`.

Cascade service supports data access with C\#. Please follow the instructions below to enable C\# support. 

## Prerequisites

- Linux is required - while .NET is commonly used with Windows, development has been made with
only Linux in mind. We have tested extensively on Ubuntu 22 LTS.
- The .NET SDK: We require at least .NET 6. To install it on Linux, please follow the tutorial
from Microsoft [here](https://learn.microsoft.com/en-us/dotnet/core/install/linux-ubuntu).
- Ensure that you have the right `INSTALL_PREFIX` when using the root `build.sh` script: it should match the path stored in the environment variable
`LD_LIBRARY_PATH`. If you don't make sure of this, <b>the C# DLL will not be able to be loaded</b>.

## Using the API

To use the API in your C\#, you must make sure of whether you're using the client as an external
or member client. A general rule of thumb is that external clients communicate via RPC, and would
be used on client nodes, while server-side nodes like those in UDLs would use the member client
as they have access to the same address space.

To begin, assuming you've built the project, you should see a file structure like so:

```
/build/src/service/cs
├── ExternalClientCS.deps.json
├── ExternalClientCS.dll
├── ExternalClientCS.pdb
├── Makefile
├── MemberClientCS.deps.json
├── MemberClientCS.dll
├── MemberClientCS.pdb
├── cmake_install.cmake
├── libexternal_client_cs.so
├── libmember_client_cs.so
└── ref
    ├── ExternalClientCS.dll
    └── MemberClientCS.dll
```

The DLLs you will need to move in your own build scripts to your C\# project directory are the
`ExternalClientCS.dll` and `MemberClientCS.dll`, based on whether you want an external or member client.

Next, add the DLL to your project's `.csproj` file (replace External with Member if you want 
to use the member client):

```
<ItemGroup>
    <Reference Include="CascadeClient">
        <HintPath>./ExternalClient.dll</HintPath>
    </Reference>
</ItemGroup>
```

Use the namespace of the client in your code:

```
using Derecho.Cascade;
```

Optionally, you can do a static using if you would like to use the definitions and types from
`CascadeClient` without having to preface them with `CascadeClient` (e.g. `CascadeClient.ObjectProperties`)

```
using static Derecho.Cascade.CascadeClient;
```

Create an instance of the client and start using the functions! Refer to the documentation
above each `CascadeClient` class method to see how to use them.

```
CascadeClient client = new CascadeClient();
client.GetMyId(); // --> 2, if running console printer with 3 nodes
client.CreateObjectPool("/console_printer");
client.Put("/console_printer/obj_a", "Hello World");
```

## Using the Client CLI

For experimentation and testing, you can use the client CLI, which offers a command-line interface
for using the app. To build it, `cd` into the `cli` directory and do the following:

1. Run `dotnet build`.

2. Run the resulting executable: `./CascadeClientCLI`.