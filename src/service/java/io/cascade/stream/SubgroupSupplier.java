package io.cascade.stream;

import io.cascade.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.nio.ByteBuffer;
import java.util.function.*;

/**
 * A supplier to be used to create a Java Stream that can iterate over all values 
 * in a Cascade subgroup that have smaller versions than the specified version.
 * Notice: one supplier can only be fed once to any stream.
 */
public class SubgroupSupplier implements Supplier<ByteBuffer>{

    /** The client API used by the supplier. */
    private Client client;

    /** The service type of this supplier. */
    private ServiceType type;

    /** The subgroup index of the shard. */
    private long subgroupIndex;

    /** The upper bound of all the versions to explore. */
    private long version;

    /** List of all shard suppliers to iterate over. */
    private List<ShardSupplier> shardSupplierList;

    /** The pointer indicating the current position of the supplier. */
    private int ptr = 0;
    
    /** Constructor. */
    public SubgroupSupplier(Client client, ServiceType type, long subgroupIndex, long version){
        this.client = client;
        this.type = type;
        this.subgroupIndex = subgroupIndex;
        this.version = version;
        this.shardSupplierList = new ArrayList<>();
        this.ptr = 0;
    }

    /** Build the supplier. 
     * Will block until the supplier is successfully built.
     */
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

    @Override
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

    /** Get the size of this supplier. */
    public long size(){
        long acc = 0;
        for (ShardSupplier ss: shardSupplierList){
            acc += ss.size();
        }
        return acc;
    }

}