package io.cascade.stream;

import io.cascade.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.nio.ByteBuffer;
import java.util.function.*;

/**
 * A supplier to be used to create a Java Stream that can iterate over all versions 
 * of a particular key in a Cascade shard that have smaller versions than the 
 * specified version.
 * Notice: one supplier can only be fed once to any stream.
 */
public class VersionSupplier implements Supplier<ByteBuffer>{

    /** The client API used by the supplier. */
    private Client client;

    /** The service type of this supplier. */
    private ServiceType type;

    /** The subgroup index of the shard. */
    private long subgroupIndex;

    /** The shard index of the shard. */
    private long shardIndex;

    /** The key to iterate over. */
    private ByteBuffer key;

    /** The upper bound of all the versions to explore. */
    private long version;

    /** The list of all different versions of value corresponding to the key. */
    private ArrayList<ByteBuffer> values;

    /** The pointer indicating the current position of the supplier. */
    private int ptr = 0;
    
    /** Constructor for byte buffer keys. */
    public VersionSupplier(Client client, ServiceType type, long subgroupIndex, long shardIndex, ByteBuffer key, long version){
        this.client = client;
        this.type = type;
        this.subgroupIndex = subgroupIndex;
        this.shardIndex = shardIndex;
        this.key = key;
        this.version = version;
        this.values = setValues();
    }

    /** Constructor for long keys. */
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

    /** Helper function to build the list of values with different versions corresponding
     *  to a same key.
     *  @return an empty list if build fails, or the list of values with different versions of 
     *  a same key, as specified.
     */
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

    @Override
    public ByteBuffer get(){
        if (ptr < values.size()){
            return values.get(ptr++);
        }
        return null;
    }

    /** Get the size of this supplier. */
    public long size(){
        return values.size();
    }

}