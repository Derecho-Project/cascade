# C\# API

*(Last updated: 9/15/2022)*

Currently in development.

### Building an unmanaged host in Linux for C# DLL

Before we start, you will want to update the `Host/GatewayToManaged.cpp` file which declares the
dotnet path in a `#define` macro. This is currently on line 14 for Linux.

To test a C# DLL on Linux, do the following (or simply run `sh ./test.sh` in `Host/`):

1. Move your file to the `Host` directory: `mv YourDllFile.dll Host/`

2. Build an executable: `cd Host && g++ -o Host.out -D LINUX jsmn.c GatewayToManaged.cpp SampleHost.cpp -ldl`.

3. This will create a `Host.out` executable which you can run with `./Host.out`. 