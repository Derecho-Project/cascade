## Cascade Data Distribution Service (DDS)
Cascade DDS is a pub/sub system implemented as a Cascade Application. It composes of three components:
- a client side library for developing DDS applications: `libcascade_dds.so`,
- a server side UDL running in Cascade servers' address space: `libcascade_dds_udl.so`,
- a DDS configuration file defining DDS's metadata service, data plane, and control plane, which will be discussed later: `dds.cfg`, and
- DDS service's object pool (we plan to use 'placement group' for 'object pool' in the future) layout: `dfgs.json`

Before diving into the details, let's talk about sever concepts in Cascade DDS. In the DDS, a topic maps to a key in some object pool in the Cascade Service. A DDS client works as an externcal client. To publish to a topic, the DDS client simply put a key/value pair with the key corresponding to the topic and the blob value representing the message. On receiving the messages, the DDS UDL is triggered and check if there are any subscribers on the corresponding topic. If yes, it relays the message to all the subscribers using Cascade server side notification. To subscribe to a topic, the DDS client does two things: one is to register the application-provided lambda a.k.a topic message handler in a local registry maintained by the client side library; the other is to put a key/value pair as a control message. The key of the control message is the topic key with a control plane suffix (defined in `dds.cfg`). On a control message, the DDS UDL will pick it from the data plane and modify the subscriber's status in the server's local memory. Unsubscribing is done similarly.

DDS topics are stored as metadata in a separate Cascade object pool defined in `dds.cfg`. The DDS metadata is simply a persumably persistent object pool. The keys are the topic names and the values are the object pool working as the data plane for that topic. For example, a key pair "topic_a":"/dds/tiny_text" indicates that the topic "topic_a" is backed up by the object pool "/dds/tiny_text". To publish to "topic_a", the DDS client will append the topic name after the object pool to derive the key "/dds/tiny_text/topic_a" for its internal `put` operation.

The control plane suffix in `dds.cfg` indicates how to separate control messages from data. In the above example, let's assume the control plan suffix is '__control__', which is the default value. To subscribe to "topic_a", a DDS client will put a K/V pair to the same object pool of the data plane "/dds/tiny_text". Instead of using the data plane topic key, it append the control plan suffix to derive the key for the control plane, which is "/dds/tiny_text/topic_a/__control__". The value is a serialized request of subscribing to topic "topic_a".

Despite above details of the internal design, the client side library covers them with [clean and simple APIs](include/cascade_dds/dds.hpp) for application developers.

### A Step-by-step Tutorial

### Limitations and Future Works
- Thread stickness
- Server side logic
- Performance evaluation and optimization
- Python and Java API
- RoundRobin/Random server node stickness.
