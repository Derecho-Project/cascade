## How to use this template
1. Make sure Cascade and its dependencies are installed and in path.
1. Copy the `console_printer_udl` folder to your own project.
1. Build the project as follows:
```
project-folder $ mkdir build && cd build 
project-folder/build $ cmake ../console_printer_udl/
project-folder/build $ make
```
Then you should see the udl binary `libconsole_printer_udl.so` in your `build` folder.

To test, use the minimal configuration in the `cfg` folder. Please start three terminal consoles and change the current directory to `cfg/n0`, `cfg/n1`, and `cfg/n2`. We use folder `cfg/n0` and `cfg/n1` for the configuration for tow Cascade Server nodes; while `cfg/n2` is for the client node. In the server consoles, start the server as follows.
```
project-folder/build/cfg/n0 $ cascade_server
Press Enter to Shutdown.
```
```
project-folder/build/cfg/n1 $ cascade_server
Press Enter to Shutdown.
```
When both processes starts successfully, you should see it prompt `Press Enter to Shutdown.` Now you can run the client to test if the udl works. To do that, in the client console, run the following commands.
```
project-folder/build/cfg/n2 $ cascade_client create_object_pool /console_printer VCSS 0
node(0) replied with version:4294967296,ts_us:1644272887105954
create_object_pool is done.
-> Succeeded.
project-folder/build/cfg/n2 $ cascade_client op_put /console_printer/obj_a AAAAA
node(1) replied with version:4294967296,ts_us:1644272911367617
-> Succeeded.
```
In the console of node 1 (`cfg/n1`), you should see the following message, showing that the udl is triggered successfully.
```
[console printer ocdpo]: I(0) received an object with key=/console_printer/obj_a, matching prefix=/console_printer/
```
