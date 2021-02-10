package io.cascade.stream;

import io.cascade.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.nio.ByteBuffer;
import java.util.function.*;

public class ShardSupplier implements Supplier<ByteBuffer>{
    protected Client client;
    protected long subgroupIndex;
    protected long shardIndex;
    private long version;
    protected List<ByteBuffer> keyList;
    protected ServiceType type;
    protected int ptr = 0;

    public ShardSupplier(Client client, ServiceType type, long subgroupIndex, long shardIndex, long version){
        this.client = client;
        this.subgroupIndex = subgroupIndex;
        this.shardIndex = shardIndex;
        this.version = version;
        this.type = type;
        
    }

    public boolean build(){
        QueryResults<List<ByteBuffer>> queryResults = client.listKeys(type, version, subgroupIndex, shardIndex);
        Map<Integer, List<ByteBuffer>> replyMap = queryResults.get();
        if (replyMap != null){
            keyList = new ArrayList<>();
            replyMap.values().forEach(keyList::addAll);
        }
        return this.keyList != null;
    }

    public ByteBuffer get(){
        if (ptr >= keyList.size()) return null;
        Map<Integer, CascadeObject> getResults = client.get(type, keyList.get(ptr++), version, subgroupIndex, shardIndex).get();
        // System.out.println("reply map:" + getResults);
        return getResults.values().iterator().next().object;
    }

    public void clear(){
        ptr = 0;
    }

    public long size(){
        return keyList.size();
    }

    public List<ByteBuffer> keyList(){
        return this.keyList;
    }
}