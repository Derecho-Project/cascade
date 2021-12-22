package io.cascade.test;

import io.cascade.*;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/** A thread to receive responses and collect data. */
class PollingThread extends Thread {
    PerfTestClient client;

    public PollingThread(PerfTestClient client) {
        this.client = client;
    }

    /**
        Run the polling thread.
     */
    public void run() {
        this.setName("JavaPollResults");
        int futureCounter = 0;
        
        while (futureCounter != client.numMessages) {
            // System.out.println("Running! ***** ");
            List<QueryResults<Bundle>> myFutureQueue = new ArrayList<QueryResults<Bundle>>();
            client.futureQueueLock.lock();
            while (client.futureQueue.isEmpty()) {
                try {
                    client.futureQueueCV.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            // collect stuff from the future queue
            // System.out.println(client.futureQueue.size());
            myFutureQueue.addAll(client.futureQueue);
            client.futureQueue.clear(); 
            client.futureQueueLock.unlock();
            

            for (QueryResults<Bundle> qr : myFutureQueue) {
                Map<Integer, Bundle> replyMap = qr.get();

                for (Map.Entry<Integer, Bundle> e : replyMap.entrySet()) {
                    int i = e.getKey();
                    Bundle b = e.getValue();
                    // System.out.println("" + i + " " + b);
                }

                // record the release time
                client.recvTss[futureCounter++] = System.nanoTime() / 1000;

                // release idle tx slots
                if (client.maxPendingOps > 0) {
                    client.idleTxSlotLock.lock();
                    client.idleTxSlotCnt.getAndIncrement();
                    client.idleTxSlotCV.signalAll();
                    client.idleTxSlotLock.unlock();
                }
            }

            if (futureCounter == client.numMessages) {
                break;
            }
        }

        // System.out.println("Polling thread shuts down!");

    }
}

/**
 * Performance Test!
 */
public class PerfTestClient {

    // 1. transmittion depth for throttling the sender
    // 0 for unlimited.
    int maxPendingOps;

    // 2. message traffic
    int numMessages;
    int messageSize;

    // 3. tx semaphore
    Lock idleTxSlotLock;
    Condition idleTxSlotCV;
    AtomicInteger idleTxSlotCnt;

    // 3. tx semaphore
    List<QueryResults<Bundle>> futureQueue;
    Condition futureQueueCV;
    Lock futureQueueLock;

    // 5. timestamps log for statistics
    long sendTss[], recvTss[];

    // 6. thread
    PollingThread thread;

    public PerfTestClient(int maxPendingOps, int numMessages, int messageSize) {
        this.maxPendingOps = maxPendingOps;
        this.numMessages = numMessages;
        this.messageSize = messageSize;
        this.idleTxSlotCnt = new AtomicInteger(maxPendingOps);
        idleTxSlotLock = new ReentrantLock();
        idleTxSlotCV = idleTxSlotLock.newCondition();
        futureQueue = new ArrayList<QueryResults<Bundle>>();
        futureQueueLock = new ReentrantLock();
        futureQueueCV = futureQueueLock.newCondition();
        sendTss = new long[numMessages];
        recvTss = new long[numMessages];
        thread = new PollingThread(this);

    }

    /** A function that randomizes the key and always returns a >= 0 key.
     */
    static long randomize_key(long in) {
        long random_seed = System.nanoTime();
        long x = (in ^ random_seed);
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        return x < 0? -x : x;
    }

    /**
     * Send the put request to the server. 
     */
    void send(ServiceType type, int msgCnt, Client client, String key, ByteBuffer val) {
        // occupy an idle tx slot
        if (maxPendingOps > 0) {
            idleTxSlotLock.lock();
            while (idleTxSlotCnt.get() <= 0) {
                try {
                    idleTxSlotCV.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            idleTxSlotCnt.getAndDecrement();
            idleTxSlotLock.unlock();
        }
        // record the send time
        sendTss[msgCnt] = System.nanoTime() / 1000;

        // execute the function
        // System.out.println("Start put!");

        // send!
        QueryResults<Bundle> qr = client.put(type, key, val, 0, 0);
        // System.out.println("Finished put!");

        // change the future queue
        futureQueueLock.lock();
        futureQueue.add(qr);
        futureQueueCV.signalAll();
        futureQueueLock.unlock();
        // System.out.println("Finished send!");
    }

    /**
     * Print the statistics of the results.
     */
    void printStatistics() {

        // for (int i = 0; i < numMessages; ++i){
        //     System.out.println("" + sendTss[i] + ", " + recvTss[i] + ", " + (long)(recvTss[i] - sendTss[i]));
        // }
        long totalKBytes = numMessages * (messageSize / 1024);
	//System.out.println("Total KB: " + totalKBytes);
        long timespan = recvTss[numMessages - 1] - sendTss[0];
        double thp_MBps = totalKBytes / 1024.0 * 1000000.0 / timespan;
        double thp_ops = numMessages * 1000000.0 / timespan;

        // bandwidth
        long sum = 0;
        for (int i = 0; i < numMessages; ++i) {
            sum += recvTss[i] - sendTss[i];
            if (recvTss[i] - sendTss[i] < 0){
                System.out.println("Error! I am here!");
            }
        }

        // latency
        double avgLatency = sum * 1.0 / numMessages;
        double ssum = 0.0;
        for (int i = 0; i < numMessages; ++i) {
            ssum += (recvTss[i] - sendTss[i] - avgLatency) * (recvTss[i] - sendTss[i] - avgLatency);
        }
        double stdLatency = Math.sqrt(ssum / (1 + numMessages));

        //System.out.println("Message size (KiB):" + messageSize / 1024.0);
        //System.out.println("Throughput (MiB/s):" + thp_MBps);
        //System.out.println("Throughput (Ops/s):" + thp_ops);
        //System.out.println("Average-Latency (us):" + avgLatency);
        //System.out.println("Latency-std (us):" + stdLatency);
        System.out.println("" + thp_MBps + " " + avgLatency);

    }

    public static final void main(String[] args) {
        if (args.length < 4) {
            System.out.println(
                    "USAGE: java -jar perf_test.jar <test_type> <num_messages> <is_persistent> <msg_size> [max_pending_ops]");
            System.out.println(
                    "        max_pending_ops is the maximum number of pending operations allowed. Default is unlimited.");
            return;
        }
        int maxDistinctObjects = 4096;
        String testType = args[0];
        int numMessages = Integer.parseInt(args[1]);
        int isPersistent = Integer.parseInt(args[2]);
        int messageSize = Integer.parseInt(args[3]);
        int maxPendingOps = args.length >= 5 ? Integer.parseInt(args[4]) : 0;

        if (!testType.equals("put")) {
            System.out.println("TODO: " + testType + " not supported");
            return;
        }

        Client client = new Client();
        // System.out.println("Created client!");

        if (isPersistent > 0) {
            // System.out.println("start persistent!");

            PerfTestClient cs = new PerfTestClient(maxPendingOps, numMessages, messageSize);

            byte[] arr = new byte[messageSize];
            for (int i = 0; i < messageSize; ++i){
                arr[i] = (byte)i;
            }
            ByteBuffer bb = ByteBuffer.allocateDirect(messageSize).put(arr);

            // starting the polling thread
            cs.thread.start();

            // send!
            for (int i = 0; i < numMessages; ++i) {
                String key = "k" + (randomize_key(i) % maxDistinctObjects);
                // System.out.println("key: "+key);
                cs.send(ServiceType.PCSS, i, client, key, bb);
            }


            // wait for the polling thread to finish
            try {
                // System.out.println("waiting for thread to join!");
                cs.thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            // print the statistics
            cs.printStatistics();
        } else {
            // System.out.println("start volatile!");

            PerfTestClient cs = new PerfTestClient(maxPendingOps, numMessages, messageSize);

            byte[] arr = new byte[messageSize];
            for (int i = 0; i < messageSize; ++i){
                arr[i] = (byte)i;
            }
            ByteBuffer bb = ByteBuffer.allocateDirect(messageSize).put(arr);

            // starting the polling thread

            cs.thread.start();

            // send!
            for (int i = 0; i < numMessages; ++i) {
                String key = "" + (randomize_key(i) % maxDistinctObjects);
                // System.out.println("key: "+key);
                cs.send(ServiceType.VCSS, i, client, key, bb);
            }

            // wait for the polling thread to finish
            try {
                // System.out.println("waiting for thread to join!");
                cs.thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            // print the statistics
            cs.printStatistics();
        }
        // System.out.println("Finished performance test!");
    }

}
