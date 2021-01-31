package io.cascade.stream;

import io.cascade.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.nio.ByteBuffer;
import java.util.function.*;

public class ShardTimeSupplier extends ShardSupplier implements Supplier<ByteBuffer>{
    private long timestamp;

    public ShardTimeSupplier(Client client, ServiceType type, long subgroupIndex, long shardIndex, long timestamp){
        super(client, type, subgroupIndex, shardIndex, -1);
        this.timestamp = timestamp;
    }

    public boolean build(){
        QueryResults<List<ByteBuffer>> queryResults = client.listKeysByTime(type, timestamp, subgroupIndex, shardIndex);
        Map<Integer, List<ByteBuffer>> replyMap = queryResults.get();
        if (replyMap != null){
            keyList = new ArrayList<>();
            replyMap.values().forEach(keyList::addAll);
        }
        return this.keyList != null;
    }

    public ByteBuffer get(){
        if (ptr >= keyList.size()) return null;
        Map<Integer, ByteBuffer> getResults = client.getByTime(type, keyList.get(ptr++), timestamp, subgroupIndex, shardIndex).get();
        System.out.println("reply map:" + getResults);
        return getResults.values().iterator().next();
    }

}