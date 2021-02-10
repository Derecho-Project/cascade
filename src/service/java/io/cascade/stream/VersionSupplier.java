package io.cascade.stream;

import io.cascade.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.nio.ByteBuffer;
import java.util.function.*;

public class VersionSupplier implements Supplier<ByteBuffer>{
    private Client client;
    private ServiceType type;
    private long subgroupIndex;
    private long shardIndex;
    private ByteBuffer key;
    private long version;
    private boolean end = false;
    
    public VersionSupplier(Client client, ServiceType type, long subgroupIndex, long shardIndex, ByteBuffer key, long version){
        this.client = client;
        this.type = type;
        this.subgroupIndex = subgroupIndex;
        this.shardIndex = shardIndex;
        this.key = key;
        this.version = version;
    }

    public VersionSupplier(Client client, ServiceType type, long subgroupIndex, long shardIndex, long key, long version){
        this.client = client;
        this.type = type;
        this.subgroupIndex = subgroupIndex;
        this.shardIndex = shardIndex;
        String str = Long.toString(key);
        byte[] arr = str.getBytes();
        ByteBuffer bbkey = ByteBuffer.allocateDirect(arr.length);
        bbkey.put(arr);
        this.key = bbkey;
        this.version = version;
    }

    public ByteBuffer get(){
        if (end) return null;
        Map<Integer, CascadeObject> getResults = client.get(type, key, version, subgroupIndex, shardIndex).get();
        CascadeObject nxtObj = getResults.values().iterator().next();
        if (nxtObj == null) {
            end = true;
            return null;
        }
        this.version = nxtObj.previousVersionByKey;
        return nxtObj.object;
    }

}