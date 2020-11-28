# The Cascade Service

The Cascade Service is a configurable K/V store with fast RDMA data paths. Cascade service is the easier one of the two approaches using Cascade because the application access the service through two simple APIs: [`service_client_api.hpp`](https://github.com/Derecho-Project/cascade/blob/master/include/cascade/service_client_api.hpp) and [`service_server_api`](https://github.com/Derecho-Project/cascade/blob/master/include/cascade/service_server_api.hpp) instead of using the more complicated native Derecho API. However, to learn how to use Cascade service, we still need to remind the group, subgroup, shard, and node concepts deriving from Derecho. Curious users can read our [paper](http://www.cs.cornell.edu/ken/derecho-tocs.pdf) for details on Derecho.

The cascade service is composed of a set of distributed processes talking to each other through RDMA data paths. We call each of the processes a `node`. Each `node` is identified by an integer ID. All nodes in the cascade service form a top-level `group`. The nodes doing the same job specified by a C++ Type are grouped into a `Subgroup`. Please note that we allow the C++ Subgroup Type to be reused for multiple subgroups if the logic in different subgroups is the same. One subgroup may do a huge work like managing a database table with billions of lines. In such a case, the nodes in a subgroup can be partitioned into `shards`, each of which takes care of a manageable part of the table. Inside a shard, the nodes are replicas.

The cascade service comes with four pre-defined subgroup types: `VCSU`, `VCSS`, `PCSU`, and `PCSS`, where V for Volatile, P for Persistent, S for 'Store', and the last character is for the key type of the object ('U' for uint64_t and 'S' for std::string). Please refer to [`service_types.hpp`](https://github.com/Derecho-Project/cascade/blob/master/include/cascade/service_types.hpp) for details of those types. All four subgroup types expose a K/V API (see [ICascadeStore](https://github.com/Derecho-Project/cascade/blob/master/include/cascade/cascade.hpp)). The persistent types support versioned and timestamped queries.

Once the cascade service is configured and started, the application can store and retrieve the data using the client API defined in [`service_client_api.hpp`](https://github.com/Derecho-Project/cascade/blob/master/include/cascade/service_client_api.hpp). Core to the client API is an `external client` talking to the Cascade services with an efficient RDMA data path. Please check [`client.cpp`](https://github.com/Derecho-Project/cascade/blob/master/src/service/client.cpp) for how to use the client API.

Cascade service also allows the application to insert logic on the data path. In order for that, the application needs to implement the cascade server API in [`service_server_api.hpp`](https://github.com/Derecho-Project/cascade/blob/master/include/cascade/service_server_api.hpp). We provide an example implementation in [`ondata_library_example.cpp`](https://github.com/Derecho-Project/cascade/blob/master/src/service/ondata_library_example.cpp).

# Configuring Cascade Service
Cascade derives Derecho's configuration file. Besides the derecho configurations, Cascade added a section called `[CASCADE]` in the same file to configure the Cascade service. There are only two options in this section: `ondata_library` and `group_layout`. `ondata_library` specifies the dynamic library containing the server APIs implementation. `group_layout` specifies the group, subgroup, and shard layout of the cascade service. Please read the comments below for how to describe a layout. 

```
[CASCADE]
# Cascade server allows application-defined behavior. The behavior is divided into two parts: one is on-critical data
# path, the other is off-critical data path. The behavior API is defined in <cascade/service_server_api.hpp>. An
# application using this feature needs to implement that API and create a shared library. And then tell the server
# where to find it by "ondata_library". We show a reference implementation in cascade source code:
#     <cascade_source>/src/service/ondata_library_exmaple.cpp
# "ondata_library" is defaulted to empty
ondata_library = 
# Specify group layout here. The layout specifies the following items:
# - How many subgroups of corresponding type (see note below) to create?
# - For each subgroup, how many shards to create?
# - For each shard, what are the minimum number of nodes required(min_nodes_by_shard, defaulted to 1), the maximum
#   number of nodes allowed(max_nodes_by_shard, defaulted to 1), the delivery mode(delivery_modes_by_shard, either
#   "Ordered" or "Raw", defaulted to "Ordered"), and the profile name(profiles_by_shard, defaulted to "DEFAULT")
# Derecho parameters in "[SUBGROUP/<profile>]" will be used for the corresponding shard.
# 
# The setup is defined in a json array, where each element is a dictionary specifying the layout for a corresponding
# subgroup type. OK, I mentioned "corresponding subgroup type" again and here is the mapping between the configuration
# elements and types used to define a Cascade service --- in the cascade service server code, we started a derecho
# group with a list of types, which currently given as "VCSU,VCSS,PCSU,PCSS"; each of the type in the list CORRESPONDS
# to an entry in the layout json array defined here, following the order in the type list. Therefore, with the current
# type list setup, the layout has four elements with the 1st for type VCSU, the 2nd for VCSS, the 3rd for PCSU, and the
# 4th for PCSS. You can define more elements than types, but the rest are ignored without side effect.
# 
# Each dictionary element has two keys: "type_alias" and "layout". The "type_alias" specifies the human-readable name
# (string) for the corresponding (sigh...the first stressless "corresponding") type. The "layout" define, with a json 
# array, the setup of subgroups of this type. Each element (a layout dict) for a subgroup. Each layout dict has four
# entries corresponding to the above four items. The value for each entry is an array whose length is equal to the
# number of shards. Each element in that array is a setting for the corresponding shard.
# For example, the following configuration defined two subgroups of VCSU type. The first subgroup has one shard and the
# second subgroup has three shards
#
# group_layout = '
# [
#     {
#         "type_alias":  "VCSU",
#         "layout":     [
#                           {
#                               "min_nodes_by_shard": [1],
#                               "max_nodes_by_shard": [1],
#                               "delivery_modes_by_shard": ["Ordered"],
#                               "profiles_by_shard": ["DEFAULT"]
#                           },
#                           {
#                               "min_nodes_by_shard": [1,3,5],
#                               "max_nodes_by_shard": [3,5,7],
#                               "delivery_modes_by_shard": ["Ordered","Ordered","Raw"],
#                               "profiles_by_shard": ["VCS2_SHARD1","VCS2_SHARD2","VCS2_SHARD3"]
#                           }
#                       ]
#     },
#     { ... },
#     { ... },
#     { ... }
#
# ]
# '
# Please make sure a pair of single quotation marks ' are used around the json. GetPot formation enforced that for
# multiline values.
group_layout = 
```

# To Run the Cascade Service Example
Once cascade is built, you will find seven folders named `n0` to `n6` in `<build_dir>/src/service/cfg/`. Each of them contains a configuration file to run a demo service node with localhost IP and RDMA API layer over TCP/IP. In the folder, start the node by calling:
```
# ../../cascade_server
```
Since the demo service requires six nodes to start running, let's start the server nodes in `n0`, `n1`, ..., `n5` and leave `n6` for the client by calling
```
# ../../cascade_client
```
Once the client connnects to the service, it is going to show prompt for command.
```
cmd> help
list_all_members
        list all members in top level derecho group.
list_type_members <type> [subgroup_index(0)] [shard_index(0)]
        list members in shard by subgroup type.
list_subgroup_members [subgroup_id(0)] [shard_index(0)]
        list members in shard by subgroup id.
set_member_selection_policy <type> <subgroup_index> <shard_index> <policy> [user_specified_node_id]
        set member selection policy
get_member_selection_policy <type> [subgroup_index(0)] [shard_index(0)]
        get member selection policy
put <type> <key> <value> [pver(-1)] [pver_by_key(-1)] [subgroup_index(0)] [shard_index(0)]
        put an object
remove <type> <key> [subgroup_index(0)] [shard_index(0)]
        remove an object
get <type> <key> [version(-1)] [subgroup_index(0)] [shard_index(0)]
        get an object(by version)
get_by_time <type> <key> <ts_us> [subgroup_index(0)] [shard_index(0)]
        get an object by timestamp
get_size <type> <key> [version(-1)] [subgroup_index(0)] [shard_index(0)]
        get the size of an object(by version)
get_size_by_time <type> <key> <ts_us> [subgroup_index(0)] [shard_index(0)]
        get the size of an object by timestamp
list_keys <type> [version(-1)] [subgroup_index(0)] [shard_index(0)]
        list keys in shard (by version)
list_keys_by_time <type> <ts_us> [subgroup_index(0)] [shard_index(0)]
        list keys in shard by time
list_data_by_prefix <type> <prefix> [version(-1)] [subgroup_index(0)] [shard_index(0)]
         test LINQ api
list_data_between_version <type> <key> <subgroup_index> <shard_index> [version_begin(MIN)] [version_end(MAX)]
         test LINQ api - version_iterator
list_data_of_key_between_timestamp <type> <key> [ts_begin(MIN)] [ts_end(MAX)] [subgroup_index(0)] [shard_index(0)]
         test LINQ api - time_iterator
list_data_in_subgroup <type> <subgroup_index> [version(-1)]
         test LINQ api - subgroup_iterator
quit|exit
        exit the client.
help
        print this message.

type:=VCSU|VCSS|PCSU|PCSS
policy:=FirstMember|LastMember|Random|FixedRandom|RoundRobin|UserSpecified


cmd>
```
Then you can try process like this:
```
cmd> put VCSU 100 ABCDEFG
cmd> get VCSU 100
node(0) replied with value:ObjectWithUInt64Key{ver: 0x1200000000, ts: 1599366091779929, id:100, data:[size:7, data: A B C D E F G]}
```

# The File System API
We also provided a file system API to Cascade. The API is implemented as a libfuse driver talking to the service through an `external client`. Once mounted, the file system presents the data in the following structure:
```
<mount_point>/<subgroupType>/subgroup_index/shard_index/Key
```
You can also check the ".cascade" file to get Cascade service properties like membership. Currently, the fuse file system is ReadOnly. We are working on adding time/version index and write supports to it. 

To start the file system API, run `fuse_client` in n4:
```
# mkdir fcc
# ../../cascade_fuse_client -s -f fcc 
```
Then, the file system api will mount on `fcc` folder.
```
# ls fcc
PCSS  PCSU  VCSS  VCSU
# cat fcc/.cascade
number of nodes in cascade service: 4.
node IDs: 0,1,3,2,
# cat fcc/VCSU/subgroup-0/shard-0/key100
hdABCDEFG#
```
Please note that the file contents are deserialized byte array of the corresponding object.

# Python API
Cascade service supports data access with Python. If you didn't see `<build_path>/src/service/python/interactive_test.py`, it's probably because the python support is not enabled. Please following the [build guidance](../../README.md#build-cascade) for how to enable python support. Otherwise, python support is enabled and you can just use `interactive_test.py` as `cascade_client` to test the python support. It also relies on the `derecho.cfg` configuration file in current work directory.

Cascade python API is managed in `cascade_py.ServiceClientAPI` class. To use this API, you just import the `cascade_py` package and create an object of type `cascade_py.ServiceClientAPI` as follows.
```
import cascade_py
capi = cascade_py.ServiceClientAPI()
```
Then you can just use the methods defined in `ServiceClientAPI` class, which mirror the C++ [`ServiceClientAPI` interface](../../include/cascade/service.hpp#L155). Please use [`interactive_test.py`](python/interactive_test.py) as an example. 

To use this api in your python application, please first install cascade. Then you can start python command and type `import cascade_py` to verify installation as follows:
```
❯ python
Python 3.8.2 (default, Jul 16 2020, 14:00:26) 
[GCC 9.3.0] on linux
Type "help", "copyright", "credits" or "license" for more information.
>>> import cascade_py
>>> print(cascade_py.__doc__)
Python API for Cascade.
>>> 
```
A common failure is python cannot find the `cascade_py` module:
```
❯ python
Python 3.8.2 (default, Jul 16 2020, 14:00:26) 
[GCC 9.3.0] on linux
Type "help", "copyright", "credits" or "license" for more information.
>>> import cascade_py
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
ModuleNotFoundError: No module named 'cascade+py'
>>> 
```
It happens when python runtime cannot locate the shared library (e.g. `cascade_py.cpython-38-x86_64-linux-gnu.so`) containing `cascade_py` module because the shared library is not on python's search path. You can either set the `PYTHONPATH` variable
```
export PYTHONPATH=<path-to-cascade_py-library>
```
or add the path before import it:
```
import sys
sys.path.append('<path-to-cascade_py-library>')
```
to tell python runtime about cascade_py location.

Please note that the python API does not implement the LINQ support yet. We plan to add it later.

# Java API
Cascade service also supports data access using Java. Cascade cmake building script will detect the Java environment to decide whether to enable Java support. We suggest openjdk-14. For example, on Ubuntu, you can install it as follows:
```
# sudo apt install openjdk-14-jdk
```
Once Java support is built, you should see `libcascade_jni.so` and `cascade.jar` in `<build_path>/src/service/java/`. `libcascade_jni.so` is the jni shared library; and `cascade.jar` is the API library for Java applications. You should also find `client_test.jar`, the Java version of `cascade_client`. To test, prepare the `derecho.cfg` in current work directory and try
```
java -cp "cascade.jar:client_test.jar" io.cascade.test.ClientTest
```
Then it will show the same cli once connected to a server.

Sometimes, cmake cannot find Java native interface(JNI) headers and libraries automatically. It will complain that JNI is not found. You can set `JAVA_HOME` environment to tell cmake the location of the JDK as follows (my java is installed in `/usr/lib/jvm/java-14-openjdk-amd64`):
```
export JAVA_HOME=/usr/lib/jvm/java-14-openjdk-amd64
```

To use the Java API, you just add `cascade.jar` to your classpath and import `io.cascade.*;`. The API is defined in `io.cascade.Client` mirroring the C++ [`ServiceClientAPI` interface](../../include/cascade/service.hpp#L155). Please refer to [`ClientTest.java`](java/io/cascade/test/ClientTest.java) as an example. 

Please note that the Java API does not implement the LINQ support yet. We plan to add it later.
