package io.cascade;

/**
 * The data structure that stores the version-timestamp pair to return to the
 * client of put() and remove() APIs.
 */
public class Bundle {
    long version;
    long previous_version;
    long previous_version_by_key;
    long timestamp;

    /**
     * Constructor of bundle objects.
     * 
     * @param version   The version of the key-value pair, returned by C++ side.
     * @param previous_version  The previous version of the key-value pair, returned by C++ side.
     * @param previous_version_by_key  The previous version of the key-value pair by key, returned by C++ side.
     * @param timestamp The timestamp of the key-value pair, returned by C++ side.
     */
    public Bundle(long version, long previous_version, long previous_version_by_key, long timestamp) {
        this.version = version;
        this.previous_version = previous_version;
        this.previous_version_by_key = previous_version_by_key;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "version: " + version + 
               "; previous_version: " + previous_version +
               "; previous_version_by_key: " + previous_version_by_key +
               "; timestamp: " + timestamp;
    }
}
