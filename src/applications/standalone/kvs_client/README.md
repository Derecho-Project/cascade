## How to use this template
1. Make sure Cascade and its dependencies are installed and in path.
1. Copy the kvs\_client folder to your own project folder.
1. Build the project as follows:
```
project-folder $ mkdir build; cd build
project-folder/build $ make
```
Then you should be able to see the `kvs_client` binary in your build folder.

To test it, use the minimal configuration in the `cfg` folder. Please start three terminal consoles and change their directory to `cfg/n0`, `cfg/n1`, and `cfg/n2`. We use folder `cfg/n0` and `cfg/n1` for the configuration for two Cascade server nodes; while `cfg/n2` is for the client node that runs `kvs_client`. In the server consoles, run the server binary as follows to start the server nodes.
```
project-folder/build/cfg/n0 $ cascade_server
Press Enter to Shutdown.
```
```
project-folder/build/cfg/n1 $ cascade_server
Press Enter to Shutdown.
```
When both processes starts successfully, you can should see it prompt `Press Enter to Shutdown.` Then, you can run the test on the client console as follows.
```
project-folder/build/cfg/n2 $ cascade_client
KVS Client Example in C++.
1) Load configuration and connecting to cascade service...
- connected.
2) Create a folder, a.k.a. object pool in the first VolatileCascadeStore subgroup.
node(0) replied with version:8589934592,ts_us:1644263178954079
- /vcss_objects folder is created.
3) List all folders a.k.a. object pools:
        /vcss_objects

4) Put an object with key '/vcss_objects/obj_001'
node(1) replied with version:8589934592,ts_us:1644263178968028
5) Get an object with key '/vcss_objects/obj_001'
node(1) replied with value:ObjectWithStringKey{msg_id: 0ver: 0x200000000, ts: 1644263178968028, prev_ver: ffffffffffffffff, prev_ver_by_key: ffffffffffffffff, id:/vcss_objects/obj_001, data:[size:30, data: v a l u e   o f...]}
project-folder/build/cfg/n2 $
```

This example demonstrates the usage of a subset of Cascade front-end service functions. Please check [`client.cpp`](../../../service/client.cpp) for the full list of Cascade front-end APIs.
