package io.cascade;

import java.util.List;
import java.nio.ByteBuffer;
import java.util.function.*;

public class ShardSupplier implements Supplier<ByteBuffer>{
    private Client client;
    private long subgroupIndex;
    private long shardIndex;
    private long version;

    public ShardSupplier(Client client, long subgroupIndex, long shardIndex, long version){
        this.client = client;
        this.subgroupIndex = subgroupIndex;
        this.shardIndex = shardIndex;
        this.version = version;
    }

    public ByteBuffer get(){
        return null;
    }
}