# C\# API

*(Last updated: 9/15/2022)*

Currently in development.

### Building an unmanaged host in Linux for C# DLL

This is not intended to be the final feature, but to test a C# DLL on Linux, do the following:

1. Move your file to the `Host` directory: `mv YourDllFile.dll Host/`

2. Build an executable: `cd Host && g++ -o Host.out -D LINUX jsmn.c GatewayToManaged.cpp SampleHost.cpp -ldl`.

3. This will create a `Host.out` executable which you can run with `./Host.out`. 