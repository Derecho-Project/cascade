package io.cascade;

import java.util.List;
import java.nio.ByteBuffer;

/**
 * The client API for cascade.
 */
public class Client {
    /* Load JNI library */
    static {
        try {
            System.loadLibrary("cascade_jni");
        } catch (UnsatisfiedLinkError ufle) {
            System.err.println(ufle.getMessage());
        }
    }

    /**
     * A handle that stores the memory address of the corresponding C++
     * ServiceClientAPI.
     */
    private long handle;

    /**
     * Constructor. Creates the client.
     */
    public Client() {
        handle = createClient();
    }

    /**
     * Creates the client.
     * 
     * @return The handle that stores the memory address of the corresponding C++
     *         ServiceClientAPI.
     */
    public native long createClient();

    /**
     * Get all members in the current derecho group.
     * 
     * @return A list of Node IDs that are the members of the current derecho group.
     */
    public native List<Integer> getMembers();

    /**
     * Deprecated.
     * Get all members in the current derecho subgroup and shard.
     * 
     * @param subgroupID The subgroupID of the subgroup.
     * @param shardID    The shardID of the shard.
     * @return A list of Node IDs that are the members of the specified derecho
     *         subgroup and shard.
     */
    // @Depreacated
    // public native List<Integer> getShardMembers(long subgroupID, long shardID);

    /**
     * Get all members in the current derecho subgroup and shard.
     * 
     * @param type          The type of the subgroup. In Cascade, this would be
     *                      VCSS or PCSS.
     * @param subgroupIndex The index of the subgroup with type {@code type}.
     * @param shardID       The index of the shard within the subgroup with type
     *                      {@code type} and subgroup index {@code subgroupIndex}.
     * @return A list of Node IDs that are the members of the specified derecho
     *         subgroup and shard.
     */
    public native List<Integer> getShardMembers(ServiceType type, long subgroupIndex, long shardID);

    /**
     * Set the member selection policy of the specified subgroup and shard.
     * 
     * @param type          The type of the subgroup. In Cascade, this would be
     *                      VCSS or PCSS.
     * @param subgroupIndex The index of the subgroup with type {@code type}.
     * @param shardID       The index of the shard within the subgroup with type
     *                      {@code type} and subgroup index {@code subgroupIndex}.
     * @param policy        The policy that we want to set. See
     *                      {@code ShardMemberSelectionPolicy} class for more
     *                      details.
     */
    public native void setMemberSelectionPolicy(ServiceType type, long subgroupIndex, long shardID,
            ShardMemberSelectionPolicy policy);

    /**
     * Get the member selection policy of the specified subgroup and shard.
     * 
     * @param type          The type of the subgroup. In Cascade, this would be
     *                      VCSS or PCSS.
     * @param subgroupIndex The index of the subgroup with type {@code type}.
     * @param shardID       The index of the shard within the subgroup with type
     *                      {@code type} and subgroup index {@code subgroupIndex}.
     * @return The shard member selection policy of the specified subgroup and
     *         shard. See {@code ShardMemberSelectionPolicy} class for more details.
     */
    public native ShardMemberSelectionPolicy getMemberSelectionPolicy(ServiceType type, long subgroupIndex,
            long shardID);

    /**
     * Put a String key and its corresponding value into cascade. The new object would
     * replace the old object if a new key-value pair with the same key as one put
     * before is put.
     * 
     * @param type          The type of the subgroup. In Cascade, this would be VCSS
     *                      and PCSS
     * @param key           The string key of the key-value pair. Requires:
     *                      {@code key} should be non-negative.
     * @param buf           A Java direct byte buffer that holds the value of the
     *                      key-value pair. Requires: {@code buf} should be a direct
     *                      byte buffer.
     * @param subgroupIndex The index of the subgroup with type {@code type} to put
     *                      this key-value pair into.
     * @param shardID       The index of the shard within the subgroup with type
     *                      {@code type} and subgroup index {@code subgroupIndex} to
     *                      put this key-value pair into.
     * @return A Future that stores a handle to a Bundle object that contains the
     *         version and timestamp of the operation.
     */
    public QueryResults<Bundle> put(ServiceType type, String key, ByteBuffer buf, long subgroupIndex, long shardID) {
        byte[] arr = key.getBytes();
        ByteBuffer bbkey = ByteBuffer.allocateDirect(arr.length);
        bbkey.put(arr);
        // System.out.println("Finished constructing byte buffer!");
        
        long res = putInternal(type, subgroupIndex, shardID, bbkey, buf);
        return new QueryResults<Bundle>(res, 0, type);
    }

    /**
     * Put a byte buffer key and its corresponding value into cascade. The new
     * object would replace the old object if a new key-value pair with the same key
     * as one put before is put.
     * 
     * @param type          The type of the subgroup. In Cascade, this would be
     *                      VCSS or PCSS.
     * @param key           The byte buffer key of the key-value pair. The user
     *                      should serialize their key formats into this byte format
     *                      in order to use this method. 
     *                      would be the long key.
     * @param buf           A Java direct byte buffer that holds the value of the
     *                      key-value pair. Requires: {@code buf} should be a direct
     *                      byte buffer.
     * @param subgroupIndex The index of the subgroup with type {@code type} to put
     *                      this key-value pair into.
     * @param shardID       The index of the shard within the subgroup with type
     *                      {@code type} and subgroup index {@code subgroupIndex} to
     *                      put this key-value pair into.
     * @return A Future that stores a handle to a Bundle object that contains the
     *         version and timestamp of the operation.
     */
    public QueryResults<Bundle> put(ServiceType type, ByteBuffer key, ByteBuffer buf, long subgroupIndex,
            long shardID) {
        long res = putInternal(type, subgroupIndex, shardID, key, buf);
        return new QueryResults<Bundle>(res, 0, type);
    }

    /**
     * Get the value corresponding to the long key from cascade.
     * 
     * @param type          The type of the subgroup. In Cascade, this would be VCSU
     *                      and PCSU. Requires: This field should be VCSU or PCSU.
     * @param key           The long key of the key-value pair. Requires:
     *                      {@code key} should be non-negative.
     * @param version       The version field returned by the Bundle object for put
     *                      operation. Should be -1 when the version field is not
     *                      known.
     * @param subgroupIndex The index of the subgroup with type {@code type} to
     *                      acquire the key-value pair.
     * @param shardID       The index of the shard within the subgroup with type
     *                      {@code type} and subgroup index {@code subgroupIndex} to
     *                      acquire this key-value pair.
     * @return A Future that stores a handle to a direct byte buffer that contains
     *         the value that corresponds to the key in the specified
     *         subgroup/shard. The byte buffer would be empty if the key has not
     *         been put into cascade.
     */
    public QueryResults<ByteBuffer> get(ServiceType type, long key, long version, long subgroupIndex, long shardID) {
        String str = Long.toString(key);
        byte[] arr = str.getBytes();
        ByteBuffer bbkey = ByteBuffer.allocateDirect(arr.length);
        bbkey.put(arr);
        long res = getInternal(type, subgroupIndex, shardID, bbkey, version);
        return new QueryResults<ByteBuffer>(res, 1, type);
    }

    /**
     * Get the value corresponding to the byte buffer key from cascade.
     * 
     * @param type          The type of the subgroup. In Cascade, this would be
     *                      VCSS or PCSS.
     * @param key           The byte buffer key of the key-value pair. The user
     *                      should serialize their key formats into this byte format
     *                      in order to use this method. If you use VCSU and PCSU as
     *                      types, the hexadecimal translation of the byte buffer
     *                      would be the long key.
     * @param version       The version field returned by the Bundle object for put
     *                      operation. Should be -1 when the version field is not
     *                      known.
     * @param subgroupIndex The index of the subgroup with type {@code type} to
     *                      acquire the key-value pair.
     * @param shardID       The index of the shard within the subgroup with type
     *                      {@code type} and subgroup index {@code subgroupIndex} to
     *                      acquire this key-value pair.
     * @return A Future that stores a handle to a direct byte buffer that contains
     *         the value that corresponds to the key in the specified
     *         subgroup/shard. The byte buffer would be empty if the key has not
     *         been put into cascade.
     */
    public QueryResults<ByteBuffer> get(ServiceType type, ByteBuffer key, long version, long subgroupIndex,
            long shardID) {
        long res = getInternal(type, subgroupIndex, shardID, key, version);
        return new QueryResults<ByteBuffer>(res, 1, type);
    }

    /**
     * Get the value corresponding to the long key from cascade by the timestamp.
     * 
     * @param type          The type of the subgroup. Requires: This field should be
     *                      PCSU.
     * @param key           The long key of the key-value pair. Requires:
     *                      {@code key} should be non-negative.
     * @param timestamp     The timestamp field returned by the Bundle object for
     *                      put operation.
     * @param subgroupIndex The index of the subgroup with type {@code type} to
     *                      acquire the key-value pair.
     * @param shardID       The index of the shard within the subgroup with type
     *                      {@code type} and subgroup index {@code subgroupIndex} to
     *                      acquire this key-value pair.
     * @return A Future that stores a handle to a direct byte buffer that contains
     *         the value that corresponds to the key in the specified
     *         subgroup/shard. The byte buffer would be empty if the key has not
     *         been put into cascade.
     */
    public QueryResults<ByteBuffer> getByTime(ServiceType type, long key, long timestamp, long subgroupIndex,
            long shardID) {
        String str = Long.toString(key);
        byte[] arr = str.getBytes();
        ByteBuffer bbkey = ByteBuffer.allocateDirect(arr.length);
        bbkey.put(arr);
        long res = getInternalByTime(type, subgroupIndex, shardID, bbkey, timestamp);
        return new QueryResults<ByteBuffer>(res, 1, type);
    }

    /**
     * Get the value corresponding to the byte buffer key from cascade by timestamp.
     * 
     * @param type          The type of the subgroup. Requires: This field should be
     *                      PCSU or PCSS.
     * @param key           The byte buffer key of the key-value pair. The user
     *                      should serialize their key formats into this byte format
     *                      in order to use this method. If you use VCSU and PCSU as
     *                      types, the hexadecimal translation of the byte buffer
     *                      would be the long key.
     * @param timestamp     The timestamp field returned by the Bundle object for
     *                      put operation. Should be -1 when the version field is
     *                      not known.
     * @param subgroupIndex The index of the subgroup with type {@code type} to
     *                      acquire the key-value pair.
     * @param shardID       The index of the shard within the subgroup with type
     *                      {@code type} and subgroup index {@code subgroupIndex} to
     *                      acquire this key-value pair.
     * @return A Future that stores a handle to a direct byte buffer that contains
     *         the value that corresponds to the key in the specified
     *         subgroup/shard. The byte buffer would be empty if the key has not
     *         been put into cascade.
     */
    public QueryResults<ByteBuffer> getByTime(ServiceType type, ByteBuffer key, long timestamp, long subgroupIndex,
            long shardID) {
        long res = getInternalByTime(type, subgroupIndex, shardID, key, timestamp);
        return new QueryResults<ByteBuffer>(res, 1, type);
    }

    /**
     * Remove a long key and its corresponding value from cascade.
     * 
     * @param type          The type of the subgroup. In Cascade, this would be VCSS
     *                      or PCSS.
     * @param key           The long key of the key-value pair. Requires:
     *                      {@code key} should be non-negative.
     * @param subgroupIndex The index of the subgroup with type {@code type} to put
     *                      this key-value pair into.
     * @param shardID       The index of the shard within the subgroup with type
     *                      {@code type} and subgroup index {@code subgroupIndex} to
     *                      put this key-value pair into.
     * @return A Future that stores a handle to a Bundle object that contains the
     *         version and timestamp of the operation.
     */
    public QueryResults<Bundle> remove(ServiceType type, long key, long subgroupIndex, long shardID) {
        String str = Long.toString(key);
        byte[] arr = str.getBytes();
        ByteBuffer bbkey = ByteBuffer.allocateDirect(arr.length);
        bbkey.put(arr);
        long res = removeInternal(type, subgroupIndex, shardID, bbkey);
        return new QueryResults<Bundle>(res, 0, type);
    }

    /**
     * Remove a byte buffer key and its corresponding value from cascade.
     * 
     * @param type          The type of the subgroup. In Cascade, this would be
     *                      VCSS or PCSS.
     * @param key           The byte buffer key of the key-value pair. The user
     *                      should serialize their key formats into this byte format
     *                      in order to use this method. If you use VCSU and PCSU as
     *                      types, the hexadecimal translation of the byte buffer
     *                      would be the long key.
     * @param subgroupIndex The index of the subgroup with type {@code type} to put
     *                      this key-value pair into.
     * @param shardID       The index of the shard within the subgroup with type
     *                      {@code type} and subgroup index {@code subgroupIndex} to
     *                      put this key-value pair into.
     * @return A Future that stores a handle to a Bundle object that contains the
     *         version and timestamp of the operation.
     */
    public QueryResults<Bundle> remove(ServiceType type, ByteBuffer key, long subgroupIndex, long shardID) {
        long res = removeInternal(type, subgroupIndex, shardID, key);
        return new QueryResults<Bundle>(res, 0, type);
    }

    /**
     * Internal interface for put operation.
     * 
     * @param type          The type of the subgroup. In Cascade, this would be
     *                      VCSS or PCSS.
     * @param subgroupIndex The index of the subgroup with type {@code type} to put
     *                      this key-value pair into.
     * @param shardIndex    The index of the shard within the subgroup with type
     *                      {@code type} and subgroup index {@code subgroupIndex} to
     *                      put this key-value pair into.
     * @param key           The byte buffer key of the key-value pair.
     * @param val           The byte buffer value of the key-value pair.
     * @return A handle of the C++ future that stores the version and timestamp of
     *         the operation.
     */
    private native long putInternal(ServiceType type, long subgroupIndex, long shardIndex, ByteBuffer key,
            ByteBuffer val);

    /**
     * Internal interface for get operation.
     * 
     * @param type          The type of the subgroup. In Cascade, this would be
     *                      VCSS or PCSS.
     * @param subgroupIndex The index of the subgroup with type {@code type} to get
     *                      this key-value pair from.
     * @param shardIndex    The index of the shard within the subgroup with type
     *                      {@code type} and subgroup index {@code subgroupIndex} to
     *                      get this key-value pair from.
     * @param key           The byte buffer key of the key-value pair.
     * @param version       The version returned by the Future object of past put.
     *                      -1 if no version is available.
     * @return A handle of the C++ future that stores the byte buffer for values.
     */
    private native long getInternal(ServiceType type, long subgroupIndex, long shardIndex, ByteBuffer key,
            long version);

    /**
     * Internal interface for get by time operation.
     * 
     * @param type          The type of the subgroup. In Cascade, this would be
     *                      VCSS or PCSS.
     * @param subgroupIndex The index of the subgroup with type {@code type} to get
     *                      this key-value pair from.
     * @param shardIndex    The index of the shard within the subgroup with type
     *                      {@code type} and subgroup index {@code subgroupIndex} to
     *                      get this key-value pair from.
     * @param key           The byte buffer key of the key-value pair.
     * @param timestamp     The timestamp returned by the Future object of past put.
     * @return A handle of the C++ future that stores the byte buffer for values.
     */
    private native long getInternalByTime(ServiceType type, long subgroupIndex, long shardIndex, ByteBuffer key,
            long timestamp);

    /**
     * Internal interface for remove operation.
     * 
     * @param type          The type of the subgroup. In Cascade, this would be
     *                      VCSS or PCSS.
     * @param subgroupIndex The index of the subgroup with type {@code type} to
     *                      remove this key-value pair from.
     * @param shardIndex    The index of the shard within the subgroup with type
     *                      {@code type} and subgroup index {@code subgroupIndex} to
     *                      remove this key-value pair from.
     * @param key           The byte buffer key of the key-value pair.
     * @return A handle of the C++ future that stores the version and timestamp of
     *         the operation.
     */
    private native long removeInternal(ServiceType type, long subgroupIndex, long shardIndex, ByteBuffer key);

}
