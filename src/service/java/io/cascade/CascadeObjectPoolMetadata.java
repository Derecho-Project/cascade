package io.cascade;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.HashMap;

/**
 * The data structure that stores the object pool metadata information
 */
public class CascadeObjectPoolMetadata {
    public long version;
    public long timestamp;
    public long previousVersion;
    public long previousVersionByKey;
    public String pathname;
    public ServiceType serviceType;
    public int subgroupIndex;
    public ShardingPolicy shardingPolicy;
    public Map<String,Integer> objectLocations;
    public boolean deleted;
}
