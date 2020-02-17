# Cascade
Cascade is a LEGO-like distributed storage hierarchy for Cloud applications. It wraps distributed persistent storage and memory resources into a high-performance and fault-tolerant storage system for applications like IoT.  Cascade features
 - High-throughput and low latency from zero-copy RDMA and NVMe storage layer;
 - Timestamp-indexed versioning capability allows reproducing system states anytime in the past;
 - A K/V store API with user-specified Key/Value types; and
 - Application-controlled data placement for optimized data locality.

There are two ways to adopt Cascade in an application. You can use Cascade as a standalone service with configurable numbers and sizes of storage pools. Or, you can use the Cascade storage templates as building blocks to build the application using the Derecho group framework.

More details come later.
