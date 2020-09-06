# How to run the test
We need a set of server nodes to start the service. Then we can start a client node to connect to any of the server nodes to test the K/V store. Please note that the node here means a Derecho node. Please refer to the [Derecho site](http://github.com/Derecho-Project/derecho) for the concepts of node, shard, subgroup, and group. Each node needs a derecho configuration file to start, which specifies the leader, node id, message sizes, RDMA devices to use, etc.. For users' convenience, we provided the configuration for three nodes in src/test/cli_example_cfg/. Each folder contains a file called derecho.cfg specifying the parameters for three nodes: n0, n1, and n2. All three nodes are configured to use the loop device 'lo.' You don't need any changes for testing purposes. A detailed explanation on configurations is on the [Derecho site](http://github.com/Derecho-Project/derecho).
Let's use node n0 and n1 for server nodes and n2 for the client node.

## Starting server node
```
test/cli_example_cfg/n0 $ ../../cli_example server
```
```
test/cli_example_cfg/n1 $ ../../cli_example server
```
Once two server nodes are started, they will show
```
...
Cascade Server finished constructing Derecho group.
Press ENTER to shutdown...
```

## Starting client node
```
test/cli_example_cfg/n2 $ ../../cli_example client
Finished constructing ExternalGroup.
Members in top derecho group:[ 0 1 ]
Members in the single shard of Volatile Cascade Store:[ 0 ]
Members in the single shard of Persistent Cascade Store:[ 1 ]
```
Now you can type `help` to check the command for the K/V store.
```
cmd> help
(v/p)put <object_id> <contents>
    - Put an object
(v/p)get <object_id> [-t timestamp_in_us | -v version_number]
    - Get the latest version of an object if no '-t' or '-v' is specified.
    - '-t' specifies the timestamp in microseconds.
    - '-v' specifies the version.
(v/p)remove <object_id>
    - Remove an object specified by the key.
help
    - print this message.
quit/exit
    - quit the client.
Notes: prefix 'v' specifies the volatile store, 'p' specifies the persistent store.
```
To put a value to the volatile store:
```
cmd> vput 1000 AAA
[18:06:33.131181] [derecho_debug] [Thread 063783] [info] p2p connection to 0 is not establised yet, establishing right now.
put finished with timestamp=12884901888,version=1585433193138330
```
To get a value from the volatile store:
```
cmd> vget 1000 
get finished with object:Object{ver: 0x300000000, ts: 1585433193138330, id:1000, data:[size:3, data: A A A]}
```
