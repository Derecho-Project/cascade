package io.cascade;
import java.nio.ByteBuffer;

/**
 * The data structure that stores the version-timestamp pair to return to the
 * client of put() and remove() APIs.
 */
public class CascadeObject {
    public long version;
    public long timestamp;
    public long previousVersion;
    public long previousVersionByKey;
    // the messageId is for Evaluation purpose only.
    public long messageId;
    public ByteBuffer object;

    /**
     * Constructor of CascadeObject objects.
     * 
     * @param version   The version of the key-value pair, returned by C++ side.
     * @param timestamp The timestamp of the key-value pair, returned by C++ side.
     */
    public CascadeObject(long version, long timestamp, long previousVersion, long previousVersionByKey, ByteBuffer bb) {
        this.version = version;
        this.timestamp = timestamp;
        this.previousVersion = previousVersion;
        this.previousVersionByKey = previousVersionByKey;
        this.object = bb;
    }

    @Override
    public String toString() {
        return "version: " + version + 
               "; timestamp: " + timestamp + 
               "; previousVersion: " + previousVersion +
               "; previousVersionByKey: " + previousVersionByKey + 
               "; messageId: " + messageId +
               "; bytebuffer: " + object;
    }
}
