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
 * Test the use of client.
 */
public class StreamTest {

    public static String byteBufferToString(ByteBuffer bb){
        byte b[] = new byte[bb.capacity()];
        for (int i = 0;i < bb.capacity(); i++){
            b[i] = bb.get(i);
        }
        String ret = new String(b);
        
        System.out.println(ret + " " + ret.length());
        return ret;
    }

    public static void main1(){
        Client client = new Client();
        ShardSupplier supplier = new ShardSupplier(client, ServiceType.PCSU, 0, 0, -1);
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

    /**
     * Translate strings to their service types.
     * 
     * @param str The string to translate.
     * @return The corresponding subgroup type of cascade.
     */
    public static ServiceType stringToType(String str) {
        switch (str) {
            case "VCSU":
                return ServiceType.VCSU;
            case "PCSU":
                return ServiceType.PCSU;
            case "VCSS":
                return ServiceType.VCSS;
            case "PCSS":
                return ServiceType.PCSS;
            default:
                return null;
        }
    }

    public static void main2(){
        Client client = new Client();
        SubgroupSupplier supplier = new SubgroupSupplier(client, ServiceType.PCSU, 0, -1);
        supplier.build();
        
        Stream.generate(supplier)
              .limit(supplier.size())
              .map(StreamTest::byteBufferToString)
              .forEachOrdered(System.out::println);
        
    }

    public static void main3(){
        Client client = new Client();
        String str = "34";
        byte[] arr = str.getBytes();
        ByteBuffer bb = ByteBuffer.allocateDirect(arr.length);
        bb.put(arr);
        VersionSupplier supplier = new VersionSupplier(client, ServiceType.PCSU, 1, 0, bb, -1);
        Stream.generate(supplier)
              .limit(6)
              .map(StreamTest::byteBufferToString)
              .forEachOrdered(System.out::println);
    }

    public static void main4(){
        Client client = new Client();
        ShardTimeSupplier supplier = new ShardTimeSupplier(client, ServiceType.PCSU, 0, 0, 1612579232912819L);
        supplier.build();
        
        Stream.generate(supplier)
              .limit(supplier.size())
              .map(StreamTest::byteBufferToString)
              .forEachOrdered(System.out::println);
        
    }


    public static final void main(String[] args) {
        System.out.println("Here is stream test!");
        main1();
        // main2();
        // main3();
        // main4();
    }
}
