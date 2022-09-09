package io.cascade.test;

import io.cascade.*;
import io.cascade.stream.*;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.stream.*;
import java.nio.charset.StandardCharsets;
import java.nio.CharBuffer;
import java.util.Comparator;


/**
 * Test the use of the Java Stream API with the given suppliers.
 */
public class StreamTest {

    /** Helper function to convert a byte buffer to a string. */
    public static String byteBufferToString(ByteBuffer bb){
        byte b[] = new byte[bb.capacity()];
        for (int i = 0;i < bb.capacity(); i++){
            b[i] = bb.get(i);
        }
        String ret = new String(b);
        
        System.out.println(ret + " " + ret.length());
        return ret;
    }

    /** Testing the use of ShardSupplier. Will block until the results are ready.
     *  Users can use Stream.generate(Supplier ss) to create a stream and access 
     *  the rich functionalities of a stream, as shown here.
     */
    public static void main1(){
        try (Client client = new Client()) {
            ShardSupplier supplier = new ShardSupplier(client, ServiceType.PersistentCascadeStoreWithStringKey, 0, 0, -1);
            while (!supplier.build()){
                try{
                    Thread.sleep(1000);
                    System.out.println("slept for 1 sec...");
                }catch(InterruptedException e){
                    // do nothing
                }
            }
            
            System.out.println("finished construction, printing...");
            
    
            // Stream.generate(supplier)
            //       .limit(supplier.size())
            //       .map(StreamTest::byteBufferToString)
            //       .sorted((s1, s2) -> s2.compareTo(s1))
            //       .filter(s -> s.length() > 1)
            //       .forEachOrdered(System.out::println);
    
            String onestr = Stream.generate(supplier)
                                  .limit(supplier.size())
                                  .map(StreamTest::byteBufferToString)
                                  .filter(s -> s.length() > 1)
                                  .findFirst()
                                  .orElse(null);
            System.out.println("the string we get!" + onestr);
        }
    }

    /**
     * Translate strings to their service types.
     * 
     * @param str The string to translate.
     * @return The corresponding subgroup type of cascade.
     */
    public static ServiceType stringToType(String str) {
        switch (str) {
            case "VolatileCascadeStoreWithStringKey":
                return ServiceType.VolatileCascadeStoreWithStringKey;
            case "PersistentCascadeStoreWithStringKey":
                return ServiceType.PersistentCascadeStoreWithStringKey;
            case "TriggerCascadeNoStoreWithStringKey":
                return ServiceType.TriggerCascadeNoStoreWithStringKey;
            default:
                return null;
        }
    }

    /** Testing the use of SubgroupSupplier. */
    public static void main2(){
        try (Client client = new Client()) {
            SubgroupSupplier supplier = new SubgroupSupplier(client, ServiceType.PersistentCascadeStoreWithStringKey, 0, -1);
            supplier.build();
            
            Stream.generate(supplier)
                  .limit(supplier.size())
                  .map(StreamTest::byteBufferToString)
                  .forEachOrdered(System.out::println);
        }
    }

    /** Testing the use of VersionSupplier. */
    public static void main3(){
        try (Client client = new Client()) {
            String str = "34";
            byte[] arr = str.getBytes();
            ByteBuffer bb = ByteBuffer.allocateDirect(arr.length);
            bb.put(arr);
            VersionSupplier supplier = new VersionSupplier(client, ServiceType.PersistentCascadeStoreWithStringKey, 1, 0, bb, -1);
            Stream.generate(supplier)
                  .limit(supplier.size())
                  .map(StreamTest::byteBufferToString)
                  .forEachOrdered(System.out::println);
        }
    }

    /** Testing the use of ShardTimeSupplier. */
    public static void main4(){
        try (Client client = new Client()) {
            ShardTimeSupplier supplier = new ShardTimeSupplier(client, ServiceType.PersistentCascadeStoreWithStringKey, 0, 0, 1612579232912819L);
            supplier.build();
            
            Stream.generate(supplier)
                  .limit(supplier.size())
                  .map(StreamTest::byteBufferToString)
                  .forEachOrdered(System.out::println);
        }
    }

    public static final void main(String[] args) {
        System.out.println("Here is stream test!");
        main1();
        // main2();
        // main3();
        // main4();
    }
}
