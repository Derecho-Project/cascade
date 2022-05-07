## Cascade Data Distribution Service (DDS)
Cascade DDS is a pub/sub system implemented as a Cascade Application. It consists of three components:
- a client side library for developing DDS applications: `libcascade_dds.so`,
- a server side UDL running in Cascade servers' address space: `libcascade_dds_udl.so`,
- a DDS configuration file defining DDS's metadata service, data plane, and control plane, which will be discussed later: `dds.cfg`, and
- DDS service's object pool (we plan to use 'placement group' for 'object pool' in the future) layout: `dfgs.json`

Use of the Cascade DDS requires some understanding of its internals, so before diving into the client API (which is quite simple), we should first talk about a few of the main concepts relevant to the Cascade DDS, and review the deployment architecture: as a developer, you'll be responsible for setting this up.  With that out of the way, we'll review the client API, which is quite simple and hides most of the internals -- the reason for reviewing them first is because a developer who understands how the system works will obtain better performance and be able to leverage more of the functionality of the system than one who just closes their eyes to these aspects. In the DDS, a topic maps to a key in some _object pool_ in the Cascade Service (similar to a partition group in Kafka). 

An application using the DDS is said to be a "client", or sometimes an "external client" -- the distinction won't be important for this documentation, but refers to whether the client belongs to the pool of servers (the top-level Cascade group), versus just being connected to it on a datacenter or Internet link. To publish to a topic, a DDS client simply publishes a key/value pair with the key corresponding to the topic and the blob value representing the message, using an API we call "put". 

As these messages are received, the Cascade DDS library (technically, what we refer to as a user-defined library (UDL), which is a special form of dynamically linked library loaded into the Cascade servers) will check for subscribers on the associated topic. If so, it relays the message to all the subscribers using Cascade server side notification. To subscribe to a topic, a DDS client does two things: one is to create and register a lambda function (a message handler) in a local registry maintained by the client side library, specifying the topic; the other is to publish (via put) a key/value pair that functions as a control message. The key of the control message is the topic key with a control plane suffix (defined in `dds.cfg`). On receiving a control message, the DDS UDL will pick it from the data plane and modify the subscriber's status in the server's local memory. Unsubscribing is done similarly.

DDS topics are stored as metadata in a separate Cascade object pool defined in `dds.cfg`. The DDS metadata is simply an object pool, which is normally configured as  persistent (retained across complete shutdown/restart sequences). The keys for key-value objects in this pool will be the topic names and the values correspond to messages published to the topic. For example, a key pair "topic_a":"/dds/tiny_text" indicates that the topic "topic_a" is backed up by the object pool "/dds/tiny_text". To publish to "topic_a", the DDS client will append the topic name after the object pool to derive the key "/dds/tiny_text/topic_a" for its internal `put` operation.

The control plane suffix in `dds.cfg` indicates how to separate control messages from data. In the above example, let's assume the control plan suffix is '__control__', which is the default value. To subscribe to "topic_a", a DDS client will put a K/V pair to the same object pool of the data plane "/dds/tiny_text". Instead of using the data plane topic key, it append the control plan suffix to derive the key for the control plane, which is "/dds/tiny_text/topic_a/__control__". The value is a serialized request of subscribing to topic "topic_a".

As noted at the outset, the client library hides all of these details behind a set of [clean and simple APIs](include/cascade_dds/dds.hpp) for application developers.

### A Step-by-step Tutorial
Cascade DDS depends on Cascade and Derecho. Please make sure Derecho and Cascade are installed correctly before following this section.

1) Build Cascade DDS

```
# mkdir build; cd build
# cmake ..
# make -j `nproc`
```
You should see the following three binaries after building it successfully
- `libcascade_dds.so` the client side library,
- `libcascade_dds_udl.so` the UDL for DDS service, and
- `cascade_dds_client` a demo client showing how to use cascade dds.

2) Run Test

In the build folder, you should see a folder called 'cfg', which contains the configuration for a local DDS service deployment, which consists only two Cascade server nodes. 
- *Start the service*. Start two consoles and cd to 'cfg/n0' and 'cfg/n1' respectively. From there, you can start the service by simply run `cascade_server`. Once two servers are all started, you should see a message on both console saying 'Press Enter to Shutdown.'
```
# cd cfg/n0
# cascade_server
Press Enter to Shutdown.
```
- *Create the object pools for DDS metadata and data planes*. We need to create the object pools for DDS metadata and data planes. Those object pools are defined in `dds.json`. Start a third console and change directory to 'cfg/n2', which will be used for the external client.
```
# cd cfg/n2
# cat dds.json
{
    "metadata_pathname": "/dds/metadata",
    "data_plane_pathnames": ["/dds/tiny_text","/dds/big_chunk"],
    "control_plane_suffix": "__control__"
}
```
Here we need to create three object pools. "/dds/metadata" is for the DDS metadata, which should be persistent. "/dds/tiny_text" is for the topics only with small messages like texts, while "/dds/big_chunk" is for the topics with big data chunks. Both the latter two object pools are volatile.
```
# cascade_client create_object_pool /dds/metadata PCSS 0
node(0) replied with version:0,ts_us:1650245329817136
create_object_pool is done.
-> Succeeded.
# cascade_client create_object_pool /dds/tiny_text VCSS 0
node(0) replied with version:1,ts_us:1650245395509445
create_object_pool is done.
-> Succeeded.
# cascade_client create_object_pool /dds/big_chunk VCSS 0
node(0) replied with version:2,ts_us:1650245683243300
create_object_pool is done.
-> Succeeded.
# cascade_client list_object_pools
refreshed object pools:
        /dds/tiny_text
        /dds/metadata
        /dds/big_chunk
-> Succeeded.
```
Please note that, in a real deployment, the two object pools, "dds/tiny_text" and "/dds/big_chunk", should live in subgroups optimized for the different workloads. Here, for demonstration purpose, we put them in the same volatile subgroup.

Then, we create a topic "ta" using the cascade dds client.
```
# ../../cascade_dds_client
cmd> create_topic ta /dds/tiny_text
-> Succeeded.
cmd> list_topics
TOPIC-1
        name:ta
        path:/dds/tiny_text
cmd> 
```
Now, we subscribe to topic "ta".
```
cmd> subscribe ta subscriber_01 
-> Succeeded.
cmd> list_subscribers
1 subscribers found
NAME    TOPIC
=============
subscriber_01   ta
-> Succeeded.
```
Here, the tester will register a message handler which prints the message as a string. However, the API allows a DDS application to register whatever handler.

Then, we publish 10 messages to topic "ta" as follows.
```
cmd> publish ta 10
publisher created for topic:ta
-> Succeeded.
cmd> Message of 22 bytes received in topic 'ta': Message #0 in topic ta
Message of 22 bytes received in topic 'ta': Message #1 in topic ta
Message of 22 bytes received in topic 'ta': Message #2 in topic ta
Message of 22 bytes received in topic 'ta': Message #3 in topic ta
Message of 22 bytes received in topic 'ta': Message #4 in topic ta
Message of 22 bytes received in topic 'ta': Message #5 in topic ta
Message of 22 bytes received in topic 'ta': Message #6 in topic ta
Message of 22 bytes received in topic 'ta': Message #7 in topic ta
Message of 22 bytes received in topic 'ta': Message #8 in topic ta
Message of 22 bytes received in topic 'ta': Message #9 in topic ta
```
The messages shown above are printed by the subscriber.

Please refer to [`dds.hpp`](include/cascade_dds/dds.hpp) for the DDS API. The [cascade DDS tester](src/client.cpp), as a  demonstrates how to implement the above capabilities using the DDS API.

### Limitations and Future Works
There are couple of limitations we plan to address in the future.
1) The DDS UDL can run in multiple threads, therefore the messages in a topic might be sent to the subscribers from different threads. Therefore, a subscriber might receive messages out of order. The current workaround is to limit the size of off critical data path thread pool to 1, by setting `num_workers_for_multicast_ocdp` to 1 in `derecho.cfg`. In the future, we plan to add a "thread stickness feature" to control the affinity of messages and worker threads. Using this feature, we can allow only one worker thread for each topic to guarantee message order without disabling multithreading. 
2) Sometimes, the application wants to leverage the computation power on the Cascade Server nodes to process topic messages. For example, the servers can be used to preprocess high resolution pictures and delete irrelavent pixels. We plan to introduce a server side API for this DDS so that the application can inject such logics to the server side like how we handle the UDLs.
3) We plan to add stateful DDS by allowing buffering recent topic messages and checkpointing.
4) Currently, the DDS service supports only C++ API. We plan to provide Python and Java API soon. 
5) Performance optimization: zero-copy API for big messages.
