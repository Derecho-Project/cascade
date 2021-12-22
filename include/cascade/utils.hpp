#pragma once

#include <functional>
#include <memory>
#include <map>
#include <tuple>
#include <vector>
#include <mutex>
#include <condition_variable>
#include <time.h>
#include <thread>
#include <cascade/config.h>

namespace derecho {
namespace cascade {

#define debug_enter_func_with_args(format,...) \
    dbg_default_debug("Entering {} with parameter:" #format ".", __func__, __VA_ARGS__)
#define debug_leave_func_with_value(format,...) \
    dbg_default_debug("Leaving {} with " #format "." , __func__, __VA_ARGS__)
#define debug_enter_func() dbg_default_debug("Entering {}.", __func__)
#define debug_leave_func() dbg_default_debug("Leaving {}.", __func__)

inline uint64_t get_time_us(bool use_wall_clock = true) {
    struct timespec tv;
    clock_gettime(use_wall_clock?CLOCK_REALTIME:CLOCK_MONOTONIC,&tv);
    return (tv.tv_sec*1000000 + tv.tv_nsec/1000);
}

/**
 * decompose the prefix into tokens. Please note that the token after the last separator is not considered a part of
 * the prefix and hence dropped if the "prefix_only" is true
 * For example, if prefix_only == true:
 * A/B/C  --> A,B
 * A/B/C/ --> A,B,C
 * Other wise;
 * A/B/C  --> A,B,C
 * A/B/C/ --> A,B,C
 *
 * @param str
 * @param prefix_only   - the flag controlling if the component after the last separator is included.
 * @param separator
 *
 * @return tokens
 */
inline std::vector<std::string> str_tokenizer(const std::string& str, bool prefix_only=false, char separator=PATH_SEPARATOR) {
    std::vector<std::string> components;
    std::string::size_type pos=0, spos=0;
    while (pos != std::string::npos) {
        pos = str.find(separator,pos);
        if (pos == std::string::npos) {
            if (!prefix_only && (spos < str.length())) {
                components.emplace_back(str.substr(spos));
            }
            continue;
        }
        // skip leading and consecutive '/'s.
        if (pos != spos) {
            components.emplace_back(str.substr(spos,(pos-spos)));
        }
        pos = pos + 1;
        spos = pos;
    } while (pos != std::string::npos);
    return components;
}

/**
 * the client collect open loop latencies
 */
class OpenLoopLatencyCollectorClient {
public:
    /**
     * Acknowledge type and id. This message is sent to Collector through UDP packet
     *
     * @param type      The type of event
     * @param id        The event id for corresponding event.
     * @param use_local_ts  Using loca timestamp.
     *
     * @return          N/A
     */
    virtual void ack(uint32_t type, uint32_t id, bool use_local_ts = false) = 0;

    /**
     * Create an open loop latency collector client
     * @param hostname      The hostname
     * @param udp_port      The port number for collecting ACKs, default to 54321
     * @return a unique pointer to OpenLoopLatencyCollectorClient
     */
    static std::unique_ptr<OpenLoopLatencyCollectorClient> create_client(
        const std::string& hostname,
        uint16_t udp_port = 54321);
};

/**
 * the server for collecting open loop latencies
 */
class OpenLoopLatencyCollector: public OpenLoopLatencyCollectorClient {
private:
    std::map<uint32_t,std::vector<uint64_t>> timestamps_in_us;
    std::map<uint32_t,uint32_t> counters;
    bool stop;
    mutable std::mutex stop_mutex;
    mutable std::condition_variable stop_cv;
    std::function<bool(const std::map<uint32_t,uint32_t>&)> udp_acks_collected_predicate;
    const uint16_t port;
    std::thread server_thread;
public:
    /**
     * Server Constructor
     */
    OpenLoopLatencyCollector(
        uint32_t max_ids,
        const std::vector<uint32_t>& type_set,
        const std::function<bool(const std::map<uint32_t,uint32_t>&)>& udp_acks_collected,
        uint16_t udp_port);

    /**
     * wait at most 'nsec' seconds for all results.
     * @param nsec      The maximum number secconds for wait
     *
     * @return          true if udp_acks_collected is satisfied, otherwise, it's false.
     */
    bool wait(uint32_t nsec);

    /**
     * override
     */
    virtual void ack(uint32_t type, uint32_t id, bool use_local_ts) override;

    /**
     * Report the latency statistics. The latecy is calculated between events of 'from_type' to events of 'to_type' for
     * each of the event id.
     *
     * @param from_type     the start point
     *
     * @return          a 3-tuple for average latency (us), standard deviation (us), and count.
     */
    std::tuple<double,double,uint32_t> report(uint32_t from_type,uint32_t to_type);

    /**
     * Create an open loop latency collector server
     * @param max_ids       The maximum id will be used in this server
     * @param type_set      The set of type ids
     * @param udp_acks_collected   The lambda function to determine if all data has been collected.
     * @param udp_port      The port number for collecting ACKs, default to 54321
     * @return a unique pointer to OpenLoopLatencyCollector
     */
    static std::unique_ptr<OpenLoopLatencyCollector> create_server(
        uint32_t max_ids,
        const std::vector<uint32_t>& type_set,
        const std::function<bool(const std::map<uint32_t,uint32_t>&)>& udp_acks_collected,
        uint16_t udp_port = 54321);

};

#ifdef ENABLE_EVALUATION
// time logger tags (TLTs)
#define TLT_READY_TO_SEND                   (0)
#define TLT_EC_SENT                         (1)
#define TLT_VOLATILE_PUT_START              (2)
#define TLT_VOLATILE_PUT_END                (3)
#define TLT_VOLATILE_PUT_AND_FORGET_START   (4)
#define TLT_VOLATILE_PUT_AND_FORGET_END     (5)
#define TLT_VOLATILE_ORDERED_PUT_START      (6)
#define TLT_VOLATILE_ORDERED_PUT_END        (7)
#define TLT_VOLATILE_ORDERED_PUT_AND_FORGET_START      (8)
#define TLT_VOLATILE_ORDERED_PUT_AND_FORGET_END        (9)
#define TLT_TRIGGER_PUT_START               (10)
#define TLT_TRIGGER_PUT_END                 (11)
#define TLT_PERSISTENT_PUT_START            (12)
#define TLT_PERSISTENT_PUT_END              (13)
#define TLT_PERSISTENT_PUT_AND_FORGET_START (14)
#define TLT_PERSISTENT_PUT_AND_FORGET_END   (15)
#define TLT_PERSISTENT_ORDERED_PUT_START    (16)
#define TLT_PERSISTENT_ORDERED_PUT_END      (17)
#define TLT_PERSISTENT_ORDERED_PUT_AND_FORGET_START    (18)
#define TLT_PERSISTENT_ORDERED_PUT_AND_FORGET_END      (19)
#define TLT_P2P_TRIGGERED                   (20)
#define TLT_ORDERED_TRIGGERED               (21)
#define TLT_PERSISTED                       (22)

#define TLT_PIPELINE(x)                     (10000 + (x))
#define TLT_DAIRYFARMDEMO(x)                (20000 + (x))

class TimestampLogger {
private:
    std::vector<std::tuple<uint64_t,uint64_t,uint64_t,uint64_t,uint64_t>> _log;
    pthread_spinlock_t lck;
public:
    /**
     * Constructor
     */
    TimestampLogger();
    /**
     * Log the timestamp
     * @param tag       timestamp tag
     * @param node_id   node id
     * @param msg_id    message id
     * @param ts_ns     timestamp in nanoseconds
     */
    void log(uint64_t tag, uint64_t node_id, uint64_t msg_id, uint64_t ts_ns, uint64_t extra=0ull);
    /**
     * Flush log to file
     * @param filename  filename
     * @param clear     True for clear the log after flush
     */
    void flush(const std::string& filename, bool clear = true);
    /**
     * Clear the log
     */
    void clear();
};

extern TimestampLogger global_timestamp_logger;

#endif

}
}
