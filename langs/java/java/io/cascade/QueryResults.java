package io.cascade;

import java.util.HashMap;
import java.util.Map;

/**
 * The handle that stores the future of Java put(), get(), and remove() natives.
 * Java side should call get() in this class to get the objects returned.
 */
public class QueryResults<T> {

    /**
     * The <node ID, T> map that stores <T>, the return objects from cascade futures
     * in each node with <node ID>.
     */
    Map<Integer, T> replyMap;
    /** The indicator of whether the objects are updated. */
    boolean updated;
    /**
     * The handle that stores the C++ memory address of the query results future.
     */
    long handle;

    /**
     * The mode of the future. 0 for Bundle type (put, remove), 1 for ByteBuffer
     * type (get, get by time).
     */
    public int mode;

    /**
     * The type of the subgroup. VCSU, PCSU, VCSS, or PCSS.
     */
    public ServiceType type;

    /**
     * The constructor for QueryResults future.
     *
     * @param h           the handle to store the C++ memory address of the query
     *                    results future.
     * @param m           The mode of delivery. 0 for bundle types, 1 for byte
     *                    buffer types.
     * @param serviceType the service type of the future.
     */
    public QueryResults(long h, int m, ServiceType serviceType) {

        replyMap = new HashMap<>();
        updated = false;
        handle = h;
        mode = m;
        type = serviceType;
    }

    /**
     * Get the results of the query when the future that stores the results of
     * calling Java functions is available. This function will block when the future
     * is not available in C++ side.
     *
     * @return The [node ID, T] map that stores [T], the return objects from cascade
     *         natives in each node with [node ID].
     */
    public Map<Integer, T> get() {

        replyMap = this.getReplyMap(handle);
        updated = true;

        return replyMap;
    }

    /**
     * The internal interface that communicates with C++ side to get reply map.
     * 
     * @param handle the memory address of the Java reply map.
     * @return The [node ID, T] map that stores [T], the return objects from cascade
     *         natives in each node with [node ID].
     */
    private native Map<Integer, T> getReplyMap(long handle);

}
