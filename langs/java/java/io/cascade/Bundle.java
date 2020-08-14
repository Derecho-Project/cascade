package io.cascade;

/**
 * The data structure that stores the version-timestamp pair to return to the
 * client of put() and remove() APIs.
 */
public class Bundle {
    long version;
    long timestamp;

    /**
     * Constructor of bundle objects.
     * 
     * @param version   The version of the key-value pair, returned by C++ side.
     * @param timestamp The timestamp of the key-value pair, returned by C++ side.
     */
    public Bundle(long version, long timestamp) {
        this.version = version;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "version: " + version + "; timestamp: " + timestamp;
    }
}