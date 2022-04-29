package io.cascade;

import java.util.List;
import java.util.Map;
import java.nio.ByteBuffer;

/**
 * The client API for cascade.
 */
public class Client implements AutoCloseable {
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

    @Override
    public void close() {
        closeClient();
    }

    private native void closeClient();

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
     * @param type          The type of the subgroup.
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
     * @param type          The type of the subgroup.
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
     * @param type          The type of the subgroup.
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
     * @param type          The type of the subgroup.
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
        return new QueryResults<Bundle>(res, 0);
    }

    /**
     * Put a byte buffer key and its corresponding value into cascade. The new
     * object would replace the old object if a new key-value pair with the same key
     * as one put before is put.
     *
     * @param type          The type of the subgroup.
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
        return new QueryResults<Bundle>(res, 0);
    }

    /**
     * Get the value corresponding to the byte buffer key from cascade.
     *
     * @param type          The type of the subgroup.
     * @param key           The byte buffer key of the key-value pair. The user
     *                      should serialize their key formats into this byte format
     *                      in order to use this method. If you use VCSU and PCSU as
     *                      types, the hexadecimal translation of the byte buffer
     *                      would be the long key.
     * @param version       The version field returned by the Bundle object for put
     *                      operation. Should be -1 when the version field is not
     *                      known.
     * @param stable        get stable version or not.
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
    public QueryResults<CascadeObject> get(ServiceType type, ByteBuffer key, long version, boolean stable,
            long subgroupIndex, long shardID) {
        long res = getInternal(type, subgroupIndex, shardID, key, version, stable);
        return new QueryResults<CascadeObject>(res, 1);
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
     * @param stable        get stable version or not.
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
    public QueryResults<CascadeObject> getByTime(ServiceType type, long key, long timestamp, boolean stable, long subgroupIndex,
            long shardID) {
        String str = Long.toString(key);
        byte[] arr = str.getBytes();
        ByteBuffer bbkey = ByteBuffer.allocateDirect(arr.length);
        bbkey.put(arr);
        long res = getInternalByTime(type, subgroupIndex, shardID, bbkey, timestamp, stable);
        return new QueryResults<CascadeObject>(res, 1);
    }

    /**
     * Get the value corresponding to the byte buffer key from cascade by timestamp.
     *
     * @param type          The type of the subgroup.
     * @param key           The byte buffer key of the key-value pair. The user
     *                      should serialize their key formats into this byte format
     *                      in order to use this method. If you use VCSU and PCSU as
     *                      types, the hexadecimal translation of the byte buffer
     *                      would be the long key.
     * @param timestamp     The timestamp field returned by the Bundle object for
     *                      put operation. Should be -1 when the version field is
     *                      not known.
     * @param stable        get stable version or not.
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
    public QueryResults<CascadeObject> getByTime(ServiceType type, ByteBuffer key, long timestamp, boolean stable,
            long subgroupIndex, long shardID) {
        long res = getInternalByTime(type, subgroupIndex, shardID, key, timestamp, stable);
        return new QueryResults<CascadeObject>(res, 1);
    }

    /**
     * Get the value corresponding to the byte buffer key from cascade object
     * pool by timestamp.
     *
     * @param key           The byte buffer key of the key-value pair. The user
     *                      should serialize their key formats into this byte format
     *                      in order to use this method. If you use VCSU and PCSU as
     *                      types, the hexadecimal translation of the byte buffer
     *                      would be the long key.
     * @param timestamp     The timestamp field returned by the Bundle object for
     *                      put operation. Should be -1 when the version field is
     *                      not known.
     * @param stable        Get stable version or not.
     * @return A Future that stores a handle to a direct byte buffer that contains
     *         the value that corresponds to the key in the specified
     *         subgroup/shard. The byte buffer would be empty if the key has not
     *         been put into cascade.
     */
    public QueryResults<CascadeObject> getByTime(ByteBuffer key, long timestamp,
            boolean stable) {
        long res = getInternalByTime(key, timestamp, stable);
        return new QueryResults<CascadeObject>(res, 1);
    }

    /**
     * Get the value corresponding to the byte buffer key from cascade object
     * pool by timestamp.
     *
     * @param key           The string key of the key-value pair.
     * @param timestamp     The timestamp field returned by the Bundle object for
     *                      put operation. Should be -1 when the version field is
     *                      not known.
     * @param stable        Get stable version or not.
     * @return A Future that stores a handle to a direct byte buffer that contains
     *         the value that corresponds to the key in the specified
     *         subgroup/shard. The byte buffer would be empty if the key has not
     *         been put into cascade.
     */
    public QueryResults<CascadeObject> getByTime(String key, long timestamp,
            boolean stable) {
        ByteBuffer bbkey = str2ByteBuffer(key);
        long res = getInternalByTime(bbkey, timestamp, stable);
        return new QueryResults<CascadeObject>(res, 1);
    }

    /**
     * Remove a long key and its corresponding value from cascade.
     *
     * @param type          The type of the subgroup.
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
        return new QueryResults<Bundle>(res, 0);
    }

    /**
     * Remove a byte buffer key and its corresponding value from cascade.
     *
     * @param type          The type of the subgroup.
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
        return new QueryResults<Bundle>(res, 0);
    }

    /**
     * List all keys in the specified shard and subgroup up to a specified persistent
     * version.
     *
     * @param type          The type of the subgroup.
     * @param version       The upper bound persistent version of all keys listed.
     *                      -1 if you want to list all keys.
     * @param subgroupIndex The index of the subgroup with type {@code type} to put
     *                      this key-value pair into.
     * @param shardIndex    The index of the shard within the subgroup with type
     *                      {@code type} and subgroup index {@code subgroupIndex} to
     *                      put this key-value pair into.
     * @return A Future that stores a handle to a List<ByteBuffer> object that contains
     *         all keys included in the operation.
     */
    public QueryResults<List<ByteBuffer>> listKeys(ServiceType type, long version, long subgroupIndex, long shardIndex){
        long res = listKeysInternal(type, version, true/*TODO:stable*/, subgroupIndex, shardIndex);
        return new QueryResults<List<ByteBuffer>>(res, 2);
    }

    /**
     * List all keys in the specified shard and subgroup up to a specified timestamp.
     *
     * @param type          The type of the subgroup.
     * @param timestamp     The upper bound timestamp of all keys listed.
     * @param subgroupIndex The index of the subgroup with type {@code type} to put
     *                      this key-value pair into.
     * @param shardIndex    The index of the shard within the subgroup with type
     *                      {@code type} and subgroup index {@code subgroupIndex} to
     *                      put this key-value pair into.
     * @return A Future that stores a handle to a List<ByteBuffer> object that contains
     *         all keys included in the operation.
     */
    public QueryResults<List<ByteBuffer>> listKeysByTime(ServiceType type, long timestamp, long subgroupIndex, long shardIndex){
        long res = listKeysByTimeInternal(type, timestamp, true/*TODO:stable*/, subgroupIndex, shardIndex);
        return new QueryResults<List<ByteBuffer>>(res, 2);
    }


    /**
     * Get the number of shards within the specified subgroup.
     * @param type          The type of the subgroup.
     * @param subgroupIndex The index of the subgroup with type {@code type} to put
     *                      this key-value pair into.
     * @return The number of shards within the specified subgroup.
     */
    public native long getNumberOfShards(ServiceType type, long subgroupIndex);

    /**
     * Create an object pool.
     *
     * @param pathname          The pathname of the objectpool
     * @param type              The type of the subgroup
     * @param subgroupIndex     The index of the subgroup with type {@code type}
     * @param shardingPolicy    The sharding policy of this object pool
     * @param objectLocations   The specified ObjectLocations
     *
     * @return a future for the result
     */
    public QueryResults<Bundle> createObjectPool(String pathname, ServiceType service_type,
            long subgroupIndex, ShardingPolicy shardingPolicy, Map<String,Integer> objectLocations)
    {
        long res = createObjectPoolInternal(pathname, service_type, subgroupIndex,
                shardingPolicy, objectLocations);
        return new QueryResults<Bundle>(res, 0);
    }

    /**
     * Create an object pool using the default sharding policy and object locations.
     *
     * The default value for the sharding policy and special object location is determined by the
     * C++ implementation.
     *
     * @param pathname      The pathname of the objectpool
     * @param type          The type of the subgroup
     * @param subgroupIndex The index of the subgroup with type {@code type}
     *
     * @return a future for the result
     */
    public QueryResults<Bundle> createObjectPool(String pathname, ServiceType service_type,
            long subgroupIndex)
    {
        long res = createObjectPoolDefaultInternal(pathname, service_type, subgroupIndex);
        return new QueryResults<Bundle>(res, 0);
    }

    /**
     * List object pools
     *
     * @return a list of object pools
     */
    public native List<String> listObjectPools();

    /**
     * Helper function to convert String to ByteBuffer.
     *
     * @param str The String object to be converted.
     *
     * @return A ByteBuffer containing the String.
     */
    private ByteBuffer str2ByteBuffer(String str){
        byte[] arr = str.getBytes();
        ByteBuffer bb = ByteBuffer.allocateDirect(arr.length);
        bb.put(arr);
        return bb;
    }

    /**
     * Put a String key and its corresponding value into cascade using put and
     * forget mechanism.
     *
     * Put_and_forget differs from normal put in that we do not check if the put
     * is successful or not and hence there is no return value.
     *
     * @param type          The type of the subgroup. In Cascade, this would be VCSS
     *                      and PCSS
     * @param key           The string key of the key-value pair. Requires:
     *                      {@code key} should be non-negative.
     * @param val           A String holding the value of the key-value pair.
     * @param subgroupIndex The index of the subgroup with type {@code type} to put
     *                      this key-value pair into.
     * @param shardID       The index of the shard within the subgroup with type
     *                      {@code type} and subgroup index {@code subgroupIndex} to
     *                      put this key-value pair into.
     * @return None.
     */
    public void put_and_forget(ServiceType type, String key, String val,
            long subgroupIndex, long shardID) {
        ByteBuffer bbkey = str2ByteBuffer(key);
        ByteBuffer bbval = str2ByteBuffer(val);
        putAndForgetInternal(type, subgroupIndex, shardID, bbkey, bbval);
        return;
    }

    /**
     * Put a String key and its corresponding value into cascade using put and
     * forget mechanism.
     *
     * Put_and_forget differs from normal put in that we do not check if the put
     * is successful or not and hence there is no return value.
     *
     * @param type          The type of the subgroup. In Cascade, this would be VCSS
     *                      and PCSS
     * @param key           The string key of the key-value pair. Requires:
     *                      {@code key} should be non-negative.
     * @param val           A ByteBuffer holding the value of the key-value pair. Requires:
     *                      {@code val} should be a direct byte buffer.
     * @param subgroupIndex The index of the subgroup with type {@code type} to put
     *                      this key-value pair into.
     * @param shardID       The index of the shard within the subgroup with type
     *                      {@code type} and subgroup index {@code subgroupIndex} to
     *                      put this key-value pair into.
     * @return None.
     */
    public void put_and_forget(ServiceType type, String key, ByteBuffer val,
            long subgroupIndex, long shardID) {
        ByteBuffer bbkey = str2ByteBuffer(key);
        putAndForgetInternal(type, subgroupIndex, shardID, bbkey, val);
        return;
    }

    /**
     * Put a String key and its corresponding value into cascade using put and
     * forget mechanism.
     *
     * Put_and_forget differs from normal put in that we do not check if the put
     * is successful or not and hence there is no return value.
     *
     * @param type          The type of the subgroup. In Cascade, this would be VCSS
     *                      and PCSS
     * @param key           The ByteBuffer key of the key-value pair.
     * @param buf           A Java direct byte buffer that holds the value of the
     *                      key-value pair. Requires: {@code buf} should be a direct
     *                      byte buffer.
     * @param subgroupIndex The index of the subgroup with type {@code type} to put
     *                      this key-value pair into.
     * @param shardID       The index of the shard within the subgroup with type
     *                      {@code type} and subgroup index {@code subgroupIndex} to
     *                      put this key-value pair into.
     * @return None.
     */
    public void put_and_forget(ServiceType type, ByteBuffer key, ByteBuffer buf,
            long subgroupIndex,long shardID) {
        putAndForgetInternal(type, subgroupIndex, shardID, key, buf);
        return;
    }

    /**
     * Get the value corresponding to the key from cascade using multi_get.
     *
     * Multi_get differs from get in that atomic broadcast is involved in the
     * former.
     *
     * @param type          The type of the subgroup.
     * @param key           The byte buffer key of the key-value pair. The user
     *                      should serialize their key formats into this byte
     *                      format in order to use this method. If you use VCSU
     *                      and PCSU as types, the hexadecimal translation of
     *                      the byte buffer would be the long key.
     * @param subgroupIndex The index of the subgroup with type {@code type} to
     *                      acquire the key-value pair.
     * @param shardID       The index of the shard within the subgroup with type
     *                      {@code type} and subgroup index
     *                      {@code subgroupIndex} to acquire this key-value
     *                      pair.
     * @return A Future that stores a handle to a direct byte buffer that
     *         contains the value that corresponds to the key in the specified
     *         subgroup/shard. The byte buffer would be empty if the key has not
     *         been put into cascade.
     */
    public QueryResults<CascadeObject> multi_get(ServiceType type,
            ByteBuffer key, long subgroupIndex, long shardID) {
        long res = multiGetInternal(type, subgroupIndex, shardID, key);
        return new QueryResults<CascadeObject>(res, 1);
    }

    /**
     * Get the value corresponding to the key from cascade using multi_get.
     *
     * Multi_get differs from get in that atomic broadcast is involved in the
     * former.
     *
     * @param type          The type of the subgroup.
     * @param key           The string key of the key-value pair. If you use
     *                      VCSU and PCSU as types, the hexadecimal translation
     *                      of the byte buffer would be the long key.
     * @param subgroupIndex The index of the subgroup with type {@code type} to
     *                      acquire the key-value pair.
     * @param shardID       The index of the shard within the subgroup with type
     *                      {@code type} and subgroup index
     *                      {@code subgroupIndex} to acquire this key-value
     *                      pair.
     * @return A Future that stores a handle to a direct byte buffer that
     *         contains the value that corresponds to the key in the specified
     *         subgroup/shard. The byte buffer would be empty if the key has not
     *         been put into cascade.
     */
    public QueryResults<CascadeObject> multi_get(ServiceType type,
            String key, long subgroupIndex, long shardID) {

        ByteBuffer bbkey = str2ByteBuffer(key);
        long res = multiGetInternal(type, subgroupIndex, shardID, bbkey);
        return new QueryResults<CascadeObject>(res, 1);
    }

    /**
     * Get the value corresponding to the key from cascade object pool using
     * multi_get.
     *
     * Multi_get differs from get in that atomic broadcast is involved in the
     * former.
     *
     * @param key           The byte buffer key of the key-value pair. The user
     *                      should serialize their key formats into this byte
     *                      format in order to use this method. If you use VCSU
     *                      and PCSU as types, the hexadecimal translation of
     *                      the byte buffer would be the long key.
     * @return A Future that stores a handle to a direct byte buffer that
     *         contains the value that corresponds to the key in the specified
     *         subgroup/shard. The byte buffer would be empty if the key has not
     *         been put into cascade.
     */
    public QueryResults<CascadeObject> multi_get(ByteBuffer key) {
        long res = multiGetInternal(key);
        return new QueryResults<CascadeObject>(res, 1);
    }

    /**
     * Get the value corresponding to the key from cascade object pool using
     * multi_get.
     *
     * Multi_get differs from get in that atomic broadcast is involved in the
     * former.
     *
     * @param key           The string key of the key-value pair. If you use
     *                      VCSU and PCSU as types, the hexadecimal translation
     *                      of the byte buffer would be the long key.
     * @return A Future that stores a handle to a direct byte buffer that
     *         contains the value that corresponds to the key in the specified
     *         subgroup/shard. The byte buffer would be empty if the key has not
     *         been put into cascade.
     */
    public QueryResults<CascadeObject> multi_get(String key) {
        ByteBuffer bbkey = str2ByteBuffer(key);
        long res = multiGetInternal(bbkey);
        return new QueryResults<CascadeObject>(res, 1);
    }

    /**
     * Get the value corresponding to the byte buffer key from cascade object pool.
     *
     * @param key           The byte buffer key of the key-value pair. The user
     *                      should serialize their key formats into this byte format
     *                      in order to use this method. If you use VCSU and PCSU as
     *                      types, the hexadecimal translation of the byte buffer
     *                      would be the long key.
     * @return A Future that stores a handle to a direct byte buffer that contains
     *         the value that corresponds to the key in the specified
     *         subgroup/shard. The byte buffer would be empty if the key has not
     *         been put into cascade.
     */
    public QueryResults<CascadeObject> get(ByteBuffer key) {
        long res = getInternal(key);
        return new QueryResults<CascadeObject>(res, 1);
    }

    /**
     * Get the value corresponding to the key from cascade object pool.
     *
     * @param key The String key of the key-value pair, and it should also specify the object pool.
     *
     * @return A Future that stores a handle to a direct byte buffer that contains
     *         the value that corresponds to the key in the specified
     *         subgroup/shard. The byte buffer would be empty if the key has not
     *         been put into cascade.
     */
    public QueryResults<CascadeObject> get(String key) {
        ByteBuffer bbkey = str2ByteBuffer(key);
        long res = getInternal(bbkey);
        return new QueryResults<CascadeObject>(res, 1);
    }

    /**
     * Put a String key and its corresponding value into cascade object pool.
     *
     * The new object would replace the old object if a new key-value pair with the same key as
     * one put before is put.
     *
     * @param key           The string key of the key-value pair. Requires:
     *                      {@code key} should be non-negative.
     * @param val           A string holding the value of the key-value pair.
     * @return A Future that stores a handle to a Bundle object that contains the
     *         version and timestamp of the operation.
     */
    public QueryResults<Bundle> put(String key, String val) {
        ByteBuffer bbkey = str2ByteBuffer(key);
        ByteBuffer bbval = str2ByteBuffer(val);
        long res = putInternal(bbkey, bbval);
        return new QueryResults<Bundle>(res, 0);
    }

    /**
     * Put a String key and its corresponding value into cascade object pool.
     *
     * The new object would replace the old object if a new key-value pair with the same key as
     * one put before is put.
     *
     * @param key           The key of the key-value pair in a ByteBuffer. Requires:
     *                      {@code key} should specify object pool.
     * @param val           A Java direct byte buffer that holds the value of the
     *                      key-value pair. Requires: {@code val} should be a direct
     *                      byte buffer.
     * @return A Future that stores a handle to a Bundle object that contains the
     *         version and timestamp of the operation.
     */
    public QueryResults<Bundle> put(ByteBuffer key, ByteBuffer val) {
        long res = putInternal(key, val);
        return new QueryResults<Bundle>(res, 0);
    }

    /**
     * Put a String key and its corresponding value into cascade object pool using put and
     * forget mechanism.
     *
     * Put_and_forget differs from normal put in that we do not check if the put
     * is successful or not and hence there is no return value.
     *
     * @param key           The string key of the key-value pair. Requires:
     *                      {@code key} should be non-negative.
     * @param val           A String holding the value of the key-value pair.
     * @return None.
     */
    public void put_and_forget(String key, String val)
    {
        ByteBuffer bbkey = str2ByteBuffer(key);
        ByteBuffer bbval = str2ByteBuffer(val);
        putAndForgetInternal(bbkey, bbval);
        return;
    }

    /**
     * Put a String key and its corresponding value into cascade object pool using put and
     * forget mechanism.
     *
     * Put_and_forget differs from normal put in that we do not check if the put
     * is successful or not and hence there is no return value.
     *
     * @param key           The string key of the key-value pair in a ByteBuffer. Requires:
     *                      {@code key} should be non-negative.
     * @param val           A Java direct byte buffer that holds the value of the
     *                      key-value pair. Requires: {@code val} should be a direct
     *                      byte buffer.
     * @return None.
     */
    public void put_and_forget(ByteBuffer key, ByteBuffer val)
    {
        putAndForgetInternal(key, val);
        return;
    }

    /**
     * Remove a long key and its corresponding value from cascade.
     *
     * @param key           The String key of the key-value pair. Requires:
     *                      {@code key} should be non-negative.
     * @return A Future that stores a handle to a Bundle object that contains the
     *         version and timestamp of the operation.
     */
    public QueryResults<Bundle> remove(String key)
    {
        ByteBuffer bbkey = str2ByteBuffer(key);
        long res = removeInternal(bbkey);
        return new QueryResults<Bundle>(res, 0);
    }

    /**
     * Remove a long key and its corresponding value from cascade.
     *
     * @param key           The ByteBuffer key of the key-value pair. Requires:
     *                      {@code key} should be non-negative.
     * @return A Future that stores a handle to a Bundle object that contains the
     *         version and timestamp of the operation.
     */
    public QueryResults<Bundle> remove(ByteBuffer key)
    {
        long res = removeInternal(key);
        return new QueryResults<Bundle>(res, 0);
    }

    /**** INTERNAL FUNCTIONS ****/

    /**
     * Internal interface for put operation.
     *
     * @param type          The type of the subgroup.
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
     * @param type          The type of the subgroup.
     * @param subgroupIndex The index of the subgroup with type {@code type} to get
     *                      this key-value pair from.
     * @param shardIndex    The index of the shard within the subgroup with type
     *                      {@code type} and subgroup index {@code subgroupIndex} to
     *                      get this key-value pair from.
     * @param key           The byte buffer key of the key-value pair.
     * @param version       The version returned by the Future object of past put.
     *                      -1 if no version is available.
     * @param stable        get stable version or not.
     * @return A handle of the C++ future that stores the byte buffer for values.
     */
    private native long getInternal(ServiceType type, long subgroupIndex, long shardIndex, ByteBuffer key,
            long version, boolean stable);

    /**
     * Internal interface for get by time operation.
     *
     * @param type          The type of the subgroup.
     * @param subgroupIndex The index of the subgroup with type {@code type} to get
     *                      this key-value pair from.
     * @param shardIndex    The index of the shard within the subgroup with type
     *                      {@code type} and subgroup index {@code subgroupIndex} to
     *                      get this key-value pair from.
     * @param key           The byte buffer key of the key-value pair.
     * @param timestamp     The timestamp returned by the Future object of past put.
     * @param stable        get stable data or not.
     * @return A handle of the C++ future that stores the byte buffer for values.
     */
    private native long getInternalByTime(ServiceType type, long subgroupIndex, long shardIndex, ByteBuffer key,
            long timestamp, boolean stable);

    /**
     * Internal interface for get by time operation for object pool.
     *
     * @param key           The byte buffer key of the key-value pair.
     * @param timestamp     The timestamp returned by the Future object of past put.
     * @param stable        A boolean flag. If a stable version of the data is
     *                      needed, this should be set to true.
     * @return A handle of the C++ future that stores the byte buffer for values.
     */
    private native long getInternalByTime(ByteBuffer key, long timestamp, boolean stable);

    /**
     * Internal interface for remove operation.
     *
     * @param type          The type of the subgroup.
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

    /**
     * Internal interface for list key operation.
     *
     * @param type          The type of the subgroup.
     * @param version       The upper bound persistent version of all keys listed.
     *                      -1 if you want to list all keys.
     * @param stable        get stable data or not.
     * @param subgroupIndex The index of the subgroup with type {@code type} to
     *                      remove this key-value pair from.
     * @param shardIndex    The index of the shard within the subgroup with type
     *                      {@code type} and subgroup index {@code subgroupIndex} to
     *                      remove this key-value pair from.
     * @return A handle of the C++ future that stores a vector with all keys included.
     */
    private native long listKeysInternal(ServiceType type, long version, boolean stable, long subgroupIndex, long shardIndex);

    /**
     * Internal interface for list keys in an object pool.
     *
       @param path          The path of the object pool, stored in a ByteBuffer.
     * @param version       The upper bound persistent version of all keys listed.
     *                      -1 if you want to list all keys.
     * @param stable        get stable data or not.
     * @return A handle of the C++ future that stores a vector with all keys included.
     */
    private native long listKeysInternal(ByteBuffer path, long version, boolean stable);

    /**
     * Internal interface for listing the keys in an object pool.
     *
     * @param path          The path of the object pool.
     * @param timestamp     The upper bound timestamp of all keys listed.
     * @param stable        Get stable data or not.
     * @return A handle of the C++ future that stores a vector with all keys included.
     */
    private native long listKeysByTimeInternal(ByteBuffer path, long timestamp, boolean stable);

    /**
     * Internal interface for list key by time operation.
     *
     * @param type          The type of the subgroup.
     * @param timestamp     The upper bound timestamp of all keys listed.
     * @param stable        get stable data or not.
     * @param subgroupIndex The index of the subgroup with type {@code type} to
     *                      remove this key-value pair from.
     * @param shardIndex    The index of the shard within the subgroup with type
     *                      {@code type} and subgroup index {@code subgroupIndex} t
     *                      remove this key-value pair from.
     * @return A handle of the C++ future that stores a vector with all keys included.
     */
    private native long listKeysByTimeInternal(ServiceType type, long timestamp, boolean stable, long subgroupIndex, long shardIndex);

    /**
     * Internal interface for creating an object pool.
     *
     * @param pathname      The pathname of the objectpool
     * @param type          The type of the subgroup
     * @param subgroupIndex The index of the subgroup with type {@code type}
     * @param shardingPolicy    The sharding policy of this object pool
     * @param objectLocations   The specified ObjectLocations
     *
     * @return A handle of the C++ future
     */
    private native long createObjectPoolInternal(String pathname, ServiceType service_type,
            long subgroupIndex, ShardingPolicy shardingPolicy,
            Map<String,Integer> objectLocations);

    /**
     * Internal interface for creating an object pool using the default configuration.
     *
     * @param pathname      The pathname of the objectpool
     * @param type          The type of the subgroup
     * @param subgroupIndex The index of the subgroup with type {@code type}
     * @param shardingPolicy    The sharding policy of this object pool
     * @param objectLocations   The specified ObjectLocations
     *
     * @return A handle of the C++ future
     */
    private native long createObjectPoolDefaultInternal(String pathname, ServiceType service_type,
            long subgroupIndex);


    /**
     * Internal interface for put_and_forget operation.
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
     */
    private native void putAndForgetInternal(ServiceType type,
            long subgroupIndex, long shardIndex, ByteBuffer key, ByteBuffer val);

    /**
     * Internal interface for multi_get operation.
     *
     * @param type          The type of the subgroup.
     * @param subgroupIndex The index of the subgroup with type {@code type} to get
     *                      this key-value pair from.
     * @param shardIndex    The index of the shard within the subgroup with type
     *                      {@code type} and subgroup index {@code subgroupIndex} to
     *                      get this key-value pair from.
     * @param key           The byte buffer key of the key-value pair.
     * @return A handle of the C++ future that stores the byte buffer for values.
     */
    private native long multiGetInternal(ServiceType type, long subgroupIndex,
            long shardIndex, ByteBuffer key);

    /**
     * Internal interface for multi_get operation for object pool.
     *
     * @param type          The type of the subgroup.
     * @param key           The byte buffer key of the key-value pair.
     * @return A handle of the C++ future that stores the byte buffer for values.
     */
    private native long multiGetInternal(ByteBuffer key);

    /**
     * Internal interface for get operation for object pool.
     *
     * @param key           The byte buffer key of the key-value pair.
     * @return A handle of the C++ future that stores the byte buffer for values.
     */
    private native long getInternal(ByteBuffer key);

    /**
     * Internal interface for put operation for the object pool.
     *
     * @param key           The byte buffer key of the key-value pair. Object pool can be extracted
     *                      from the key.
     * @param val           The byte buffer value of the key-value pair.
     *
     * @return A handle of the C++ future that stores the version and timestamp of
     *         the operation.
     */
    private native long putInternal(ByteBuffer key, ByteBuffer val);

    /**
     * Internal interface for put_and_forget operation for the object pool.
     *
     * @param key           The byte buffer key of the key-value pair. Object pool can be extracted
     *                      from the key.
     * @param val           The byte buffer value of the key-value pair.
     * @return A handle of the C++ future that stores the version and timestamp of
     *         the operation.
     */
    private native void putAndForgetInternal(ByteBuffer key, ByteBuffer val);

    /**
     * Internal interface for remove operation for object pool.
     *
     * @param key           The byte buffer key of the key-value pair. Object pool can be extracted
     *                      from the key.
     * @return A handle of the C++ future that stores the version and timestamp of
     *         the operation.
     */
    private native long removeInternal(ByteBuffer key);

    /**
     * Internal interface for get size operation.
     *
     * @param type          The type of the subgroup.
     * @param subgroupIndex The index of the subgroup with type {@code type} to get
     *                      this key-value pair from.
     * @param shardIndex    The index of the shard within the subgroup with type
     *                      {@code type} and subgroup index {@code subgroupIndex} to
     *                      get this key-value pair from.
     * @param key           The byte buffer key of the key-value pair.
     * @param version       The version returned by the Future object of past put.
     *                      -1 if no version is available.
     * @param stable        get stable version or not.
     * @return A handle of the C++ future that stores the byte buffer for values.
     */
    private native long getSizeInternal(ServiceType type, long subgroupIndex, long shardIndex,
        ByteBuffer key, long version, boolean stable);

    /**
     * Internal interface for get size operation in an object pool.
     *
     * @param path          A ByteBuffer containing the path of the object pool.
     * @param key           The byte buffer key of the key-value pair.
     * @param version       The version returned by the Future object of past put.
     *                      -1 if no version is available.
     * @param stable        get stable version or not.
     * @return A handle of the C++ future that stores the byte buffer for values.
     */
    private native long getSizeInternal(ByteBuffer path, ByteBuffer key, long version,
        boolean stable);
}
