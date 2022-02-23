package io.cascade.stream;

import io.cascade.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.nio.ByteBuffer;
import java.util.function.*;

/**
 * A supplier to be used to create a Java Stream that can iterate over all values 
 * in a Cascade shard that have earlier timestamps than the specified timestamp.
 * Notice: one supplier can only be fed once to any stream.
 */
public class ShardTimeSupplier extends ShardSupplier implements Supplier<ByteBuffer>{

    /** The upper bound of all the timestamps to explore. */
    private long timestamp;

    /** Constructor. */
    public ShardTimeSupplier(Client client, ServiceType type, long subgroupIndex, long shardIndex, long timestamp){
        super(client, type, subgroupIndex, shardIndex, -1);
        this.timestamp = timestamp;
    }

    /** Build the supplier. 
     * @return true if the supplier is built successfully, false otherwise.
     */
    public boolean build(){
        try (QueryResults<List<ByteBuffer>> queryResults = client.listKeysByTime(type, timestamp, subgroupIndex, shardIndex)) {
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
        Map<Integer, CascadeObject> getResults = client.getByTime(type, keyList.get(ptr++), timestamp, true/*always get stable data*/, subgroupIndex, shardIndex).get();
        System.out.println("reply map:" + getResults);
        return getResults.values().iterator().next().object;
    }

    /** Get the size of this supplier. */
    public long size(){
        return keyList.size();
    }

}
