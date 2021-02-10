package io.cascade.stream;

import io.cascade.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.nio.ByteBuffer;
import java.util.function.*;

public class SubgroupSupplier implements Supplier<ByteBuffer>{
    private Client client;
    private ServiceType type;
    private long subgroupIndex;
    private long version;
    private boolean end = false;
    private List<ShardSupplier> shardSupplierList;
    private int ptr;
    
    public SubgroupSupplier(Client client, ServiceType type, long subgroupIndex, long version){
        this.client = client;
        this.type = type;
        this.subgroupIndex = subgroupIndex;
        this.version = version;
        this.shardSupplierList = new ArrayList<>();
        this.ptr = 0;
    }

    public void build(){
        long numShards = client.getNumberOfShards(type, subgroupIndex);
        System.out.println("num shards:" + numShards);
        for (long i = 0; i < numShards; ++i){
            ShardSupplier ss = new ShardSupplier(client, type, subgroupIndex, i, version);
            while (!ss.build()){
                try{
                    Thread.sleep(1000);
                    System.out.println("slept for 1 sec...");
                }catch(InterruptedException e){
                    // do nothing
                }
            }
            shardSupplierList.add(ss);
        }
        
    }

    public ByteBuffer get(){
        ShardSupplier ss = null;
        while (ptr < shardSupplierList.size()){
            ss = shardSupplierList.get(ptr);
            ByteBuffer bb = ss.get();
            if (bb != null) 
                return bb;
            ptr++;
        }
        return null;
    }

    public long size(){
        long acc = 0;
        for (ShardSupplier ss: shardSupplierList){
            acc += ss.size();
        }
        return acc;
    }

}