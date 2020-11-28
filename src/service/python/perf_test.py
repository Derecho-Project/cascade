#!/usr/bin/env python3

import cascade_py
import threading
import time
import math
import sys
import cProfile, pstats, io

class AtomicInteger():
    def __init__(self, value=0):
        self._value = value
        self._lock = threading.Lock()

    def inc(self):
        with self._lock:
            self._value += 1
            return self._value

    def dec(self):
        with self._lock:
            self._value -= 1
            return self._value


    @property
    def value(self):
        with self._lock:
            return self._value

    @value.setter
    def value(self, v):
        with self._lock:
            self._value = v
            return self._value

class client_states:
    # 1. transmittion depth for throttling the sender
    # 0 for unlimited.
    max_pending_ops = 0

    # 2. message traffic
    num_messages = 0
    message_size = 0

    # 3. tx semaphore
    idle_tx_slot_cv = None
    idle_tx_slot_cnt = None
    idle_tx_slot_mutex = None

    # 4. future queue semaphore
    future_queue = []
    future_queue_cv = None
    future_queue_mutex = None

    # 5. timestamps log for statistics
    send_tss = []
    recv_tss = []

    # 6. thread
    poll_thread = None

    def __init__(self, _max_pending_ops, _num_messages, _message_size):
        self.num_messages = _num_messages
        self.max_pending_ops = _max_pending_ops
        self.message_size = _message_size

        self.idle_tx_slot_cnt = AtomicInteger(_max_pending_ops)
        self.idle_tx_slot_mutex = threading.RLock()
        self.idle_tx_slot_cv = threading.Condition(self.idle_tx_slot_mutex)

        self.future_queue_mutex = threading.RLock()
        self.future_queue_cv = threading.Condition(self.future_queue_mutex)

        self.client = cascade_py.ServiceClientAPI()
        self.poll_thread = threading.Thread(target = self.poll_results)

        self.send_tss = [0 for i in range(0,_num_messages)]
        self.recv_tss = [0 for i in range(0,_num_messages)]

    def poll_results(self):

        future_counter = 0
        while(future_counter != self.num_messages):
            my_future_queue = []
            
            with self.future_queue_mutex:
                while(len(self.future_queue) == 0):
                    try:
                        self.future_queue_cv.wait()
                    except:
                        print("Oh No!")
                
                my_future_queue, self.future_queue = self.future_queue, my_future_queue
            
            for qr in my_future_queue:
                store = qr.get_result()

                self.recv_tss[future_counter] = time.time() * (10**6)
                future_counter+=1

                if(self.max_pending_ops > 0):
                    with self.idle_tx_slot_mutex:
                        self.idle_tx_slot_cnt.inc()
                        self.idle_tx_slot_cv.notifyAll()

            if(future_counter == self.num_messages):
                break
        
        #print("polling thread shuts down!!")

    def wait_poll_all():
        self.poll_thread.join()

    def do_send(self, msg_cnt, typ, key, val):
        # wait for tx slot semaphore
        if(self.max_pending_ops > 0):
            with self.idle_tx_slot_mutex:
                while(self.idle_tx_slot_cnt.value <= 0):
                    try:
                        self.idle_tx_slot_cv.wait()
                    except:
                        print("Oh No but here")
                self.idle_tx_slot_cnt.dec()
            
        # record the send time
        self.send_tss[msg_cnt] = time.time() * (10**6)

        #print("Start put")

        qr = self.client.put(typ, key, val, 0, 0)

        with self.future_queue_mutex:
            self.future_queue.append(qr)
            self.future_queue_cv.notifyAll()
        
        #print("Finished send")

    def print_statistics(self):

        for i in range(0, self.num_messages):
            print(self.send_tss[i],self.recv_tss[i], (self.recv_tss[i] - self.send_tss[i]))
        

        total_bytes = self.num_messages * self.message_size
        timespan_us = self.recv_tss[self.num_messages-1] - self.send_tss[0]
        thp_MBps = total_bytes * 1000000.0 / 1048576 / timespan_us
        thp_ops = self.num_messages*1000000.0/timespan_us

        s = 0.0
        for i in range(0,self.num_messages):
            s += self.recv_tss[i]-self.send_tss[i]
        avg_latency_us = s/self.num_messages

        ssum = 0.0
        for i in range(0,self.num_messages):
            ssum += (self.recv_tss[i]-self.send_tss[i]-avg_latency_us) * (self.recv_tss[i]-self.send_tss[i]-avg_latency_us)
    
        std_latency_us = math.sqrt(ssum/(self.num_messages + 1))

        print("Message size (KiB):", self.message_size / 1024.0)
        print("Throughput (MiB/s):", thp_MBps)
        print("Throughput (Ops/s):", thp_ops)
        print("Average-Latency (us):", avg_latency_us)
        print("Latency-std (us):", std_latency_us)

def randomize_key(i):
    random_seed = int(time.time() * (10**6))
    x = i ^ random_seed
    x ^= x << 13
    x ^= x >> 7
    x ^= x << 17
    return x

def main():
    if(len(sys.argv[1:]) < 4):
        print("USAGE: python3 perf_test.py <test_type> <num_messages> <is_persistent> <msg_size> [max_pending_ops]")
        print()
        print("max_pending_ops is the maximum number of pending operations allowed. Default is unlimited.")

    max_distinct_objects = 4096
    typ = sys.argv[1]
    num_messages = int(sys.argv[2])
    is_persistent = int(sys.argv[3])
    message_size = int(sys.argv[4])
    max_pending_ops = -1
    if(len(sys.argv[1:]) >= 5):
        max_pending_ops = int(sys.argv[5])
    
    if(typ != "put"):
        print("Sorry not support method")
        sys.exit()
    
    if(is_persistent > 0):
        print("starting persistant")

        pers_client = client_states(max_pending_ops, num_messages, message_size)

        bb = bytes(message_size)

        
        #start thread
        pers_client.poll_thread.start()

        #sending
        for i in range(0,num_messages):
            key = str(randomize_key(i) % max_distinct_objects)
            #print("key",key)
            pers_client.do_send(i, "PCSU", key, bb)

        try:
            pers_client.poll_thread.join()
        except:
            print("Persistant Thread Fail")
        
        pers_client.print_statistics()
    else:
        print("starting volatile")

        vol_client = client_states(max_pending_ops, num_messages, message_size)

        bb = bytes(message_size)

        #start thread
        vol_client.poll_thread.start()

        #sending
        for i in range(0,num_messages):
            key = str(randomize_key(i) % max_distinct_objects)
            print("key",key)
            vol_client.do_send(i, "VCSU", key, bb)

        try:
            vol_client.poll_thread.join()
        except:
            print("Volatile Thread Fail")
        
        vol_client.print_statistics()

    print("Done with Performance test")

#print stats
def main2():
    pr = cProfile.Profile()
    pr.enable()
    main()
    s = io.StringIO()
    ps = pstats.Stats(pr)
    ps.print_stats()
    print(s.getvalue())

    

if __name__ == '__main__':
    main()
