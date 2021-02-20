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
    private ArrayList<ByteBuffer> values;
    private int ptr = 0;
    
    public VersionSupplier(Client client, ServiceType type, long subgroupIndex, long shardIndex, ByteBuffer key, long version){
        this.client = client;
        this.type = type;
        this.subgroupIndex = subgroupIndex;
        this.shardIndex = shardIndex;
        this.key = key;
        this.version = version;
        this.values = setValues();
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
        this.values = setValues();
    }

    private ArrayList<ByteBuffer> setValues(){
        long tempVersion = this.version;
        boolean justGetStarted = true;
        ArrayList<ByteBuffer> values = new ArrayList<>();
        while (justGetStarted || tempVersion != -1){
            // System.out.println("start setting values! temp version: " + tempVersion);
            Map<Integer, CascadeObject> getResults = client.get(type, key, tempVersion, subgroupIndex, shardIndex).get();
            CascadeObject nxtObj = getResults.values().iterator().next();
            if (nxtObj == null) {
                return values;
            }
            tempVersion = nxtObj.previousVersionByKey;
            values.add(nxtObj.object);
            justGetStarted = false;
        }
        return values;
    }

    public ByteBuffer get(){
        if (ptr < values.size()){
            return values.get(ptr++);
        }
        return null;
    }

    public long size(){
        return values.size();
    }

}