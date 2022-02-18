package io.cascade.stream;

import io.cascade.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.nio.ByteBuffer;
import java.util.function.*;

/**
 * A supplier to be used to create a Java Stream that can iterate over all values 
 * in a Cascade shard that have smaller versions than the specified version. 
 * Notice: one supplier can only be fed once to any stream.
 */
public class ShardSupplier implements Supplier<ByteBuffer>{
    
    /** The client API used by the supplier. */
    protected Client client;

    /** The service type of this supplier. */
    protected ServiceType type;

    /** The subgroup index of the shard. */
    protected long subgroupIndex;
    
    /** The shard index of the shard. */
    protected long shardIndex;

    /** The upper bound of all the versions to explore. */
    private long version;

    /** The list of all keys. */
    protected List<ByteBuffer> keyList;

    /** The pointer indicating the current position of the supplier. */
    protected int ptr = 0;

    /** Constructor. */
    public ShardSupplier(Client client, ServiceType type, long subgroupIndex, long shardIndex, long version){
        this.client = client;
        this.subgroupIndex = subgroupIndex;
        this.shardIndex = shardIndex;
        this.version = version;
        this.type = type;
    }

    /** Build the supplier. 
     * @return true if the supplier is built successfully, false otherwise.
     */
    public boolean build(){
        try (QueryResults<List<ByteBuffer>> queryResults = client.listKeys(type, version, subgroupIndex, shardIndex)) {
            Map<Integer, List<ByteBuffer>> replyMap = queryResults.get();
            if (replyMap != null){
                keyList = new ArrayList<>();
                replyMap.values().forEach(keyList::addAll);
            }
            return this.keyList != null;
        }
    }

    @Override
    public ByteBuffer get(){
        if (ptr >= keyList.size()) return null;
        Map<Integer, CascadeObject> getResults = client.get(type, keyList.get(ptr++), version, subgroupIndex, shardIndex).get();
        // System.out.println("reply map:" + getResults);
        return getResults.values().iterator().next().object;
    }

    /** Get the size of this supplier. */
    public long size(){
        return keyList.size();
    }

    /** Get the list of keys for the supplier. */
    public List<ByteBuffer> keyList(){
        return this.keyList;
    }
}
