# Multi-Process UDL support.
Running user coded UDL in Cascade address space is not an optimal option for security reasons. Multi-Process UDL support allows udl to run inside a separate process which communicates with Cascade process using shared memory. This design solved the security issue with minimal overhead.

# Design

```
           Cascade Server Process                   |                     Cascade UDL Process
     -----------------------------------------------+-----------------------------------------------
                                                    |
          [mproc context server] <-------+          |                 
                 |   ^       \---------+ |   [ connector 1 ]              [ mproc udl server 1 ]
                 |   |                 | +---- ctxt req rb <-----------------/  ^  ^
                 |   |                 +-----> ctxt res rb ---------------------+  |     
          [ mproc udl client 1 ]-------------> obj comm rb ------------------------+
                 |   |                              |
                 |   |                       [ connector 2 ]              [ mproc udl server 2 ]
                 |   +------------------------ ctxt req rb <-----------------/  ^  ^
                 +---------------------------> ctxt res rb ---------------------+  |
          [ mproc udl client 2 ]-------------> obj comm rb ------------------------+
    


```

Core concepts:
- mproc (udl) server
- mproc (udl) client
- mproc manager
- mproc context server
- mproc connector
- mproc ctxt request ring buffer
- mproc ctxt response ring buffer
- mproc object commit ring buffer
- object pool shared space
- context shared space
