The standalone applications are templates designed for Cascade application developers. They are NOT built along with other Cascade source. To use them, please install Cascade first.

- [**kvs_client**](kvs_client) shows how to access the data in Cascade K/V stores.
- [**console_printer_udl**](console_printer_udl) shows how to write application logic on Cascade's data path.
- [**notification**](notification) shows how to use the "server side notification" feature of Cascade.
- [**dairy_farm**](dairy_farm) is an IoT demo application showing how to use Cascade with ML/AI capabilities.
- [**dds**](dds) is a data distribution service built on Cascade.

To use those template, you can just copy the folder to your own project folder and build it as following:
```
project-folder $ mkdir build
project-folder/build $ cmake ..
project-folder/build $ make
```
It should build if you have the Cascade and its dependencies installed and in path.
