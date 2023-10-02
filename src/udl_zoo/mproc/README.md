# Multi-Process UDL support.
Running user coded UDL in Cascade address space is not an optimal option for security reasons. Multi-Process UDL support allows udl to run inside a separate process which communicates with Cascade process using shared memory. This design solved the security issue with minimal overhead.
