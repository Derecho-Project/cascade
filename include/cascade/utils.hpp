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
#include <unordered_set>
#include <derecho/utils/time.h>
#include <cascade/config.h>

namespace derecho {
namespace cascade {

#define debug_enter_func_with_args(format,...) \
    dbg_default_debug("Entering {} with parameter:" #format ".", __func__, __VA_ARGS__)
#define debug_leave_func_with_value(format,...) \
    dbg_default_debug("Leaving {} with " #format "." , __func__, __VA_ARGS__)
#define debug_enter_func() dbg_default_debug("Entering {}.", __func__)
#define debug_leave_func() dbg_default_debug("Leaving {}.", __func__)

inline uint64_t get_time_ns(bool use_wall_clock = true) {
    struct timespec tv;
    clock_gettime(use_wall_clock?CLOCK_REALTIME:CLOCK_MONOTONIC,&tv);
    return (tv.tv_sec*INT64_1E9 + tv.tv_nsec);
}

inline uint64_t get_time_us(bool use_wall_clock = true) {
    return get_time_ns(use_wall_clock)/INT64_1E3;
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
     * @param[in]   from_type   the start point
     * @param[in]   to_type     the end point
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
/*
 * time logger tags (TLTs)
 * 
 * We support a wide range of timestamps in cascade for performance tests.
 * For Service Client (Please note that the END time is not logged because the return clause should be included.
 * The callers should measure it by themselves):
 * ::put():
 *      TLT_SERVICE_CLIENT_PUT_START
 * ::put_and_forget():
 *      TLT_SERVICE_CLIENT_PUT_AND_FORGET_START
 * ::trigger_put():
 *      TLT_SERVICE_CLIENT_TRIGGER_PUT_START
 * ::collective_trigger_put():
 *      TLT_SERVICE_CLIENT_COLLECTIVE_TRIGGER_PUT_START
 * ::remove():
 *      TLT_SERVICE_CLIENT_REMOVE_START     # no message id
 * ::get():
 *      TLT_SERVICE_CLIENT_GET_START        # no message id
 * ::multi_get():
 *      TLT_SERVICE_CLIENT_MULTI_GET_START  # no message id
 * ::list_keys():
 *      TLT_SERVICE_CLIENT_LIST_KEYS_START  # no message id
 * ::multi_list_keys():
 *      TLT_SERVICE_CLIENT_MULTI_LIST_KEYS_START    # no message id
 * ::get_size():
 *      TLT_SERVICE_CLIENT_GET_SIZE_START           # no message id
 * ::multi_get_size():
 *      TLT_SERVICE_CLIENT_MULTI_GET_SIZE_START     # no message id
 */
#define TLT_SERVICE_CLIENT_PUT_START                (1001)
#define TLT_SERVICE_CLIENT_PUT_AND_FORGET_START     (1002)
#define TLT_SERVICE_CLIENT_TRIGGER_PUT_START        (1003)
#define TLT_SERVICE_CLIENT_COLLECTIVE_TRIGGER_PUT_START \
                                                    (1004)
#define TLT_SERVICE_CLIENT_REMOVE_START             (1005)
#define TLT_SERVICE_CLIENT_GET_START                (1006)
#define TLT_SERVICE_CLIENT_MULTI_GET_START          (1007)
#define TLT_SERVICE_CLIENT_LIST_KEYS_START          (1008)
#define TLT_SERVICE_CLIENT_MULTI_LIST_KEYS_START    (1009)
#define TLT_SERVICE_CLIENT_GET_SIZE_START           (1010)
#define TLT_SERVICE_CLIENT_MULTI_GET_SIZE_START     (1011)

/* For VolatileCascadeStore:
 * ::put():
 *      TLT_VOLATILE_PUT_START
 *      TLT_VOLATILE_ORDERED_PUT_START
 *      TLT_VOLATILE_ORDERED_PUT_END
 *      TLT_VOLATILE_PUT_END
 * ::put_and_forget():
 *      TLT_VOLATILE_PUT_AND_FORGET_START
 *      TLT_VOLATILE_ORDERED_PUT_AND_FORGET_START
 *      TLT_VOLATILE_ORDERED_PUT_AND_FORGET_END
 *      TLT_VOLATILE_PUT_AND_FORGET_END
 * ::trigger_put():
 *      TLT_VOLATILE_TRIGGER_PUT_START
 *      TLT_VOLATILE_TRIGGER_PUT_END
 * ::remove():
 *      TLT_VOLATILE_REMOVE_START
 *      TLT_VOLATILE_ORDERED_REMOVE_START
 *      TLT_VOLATILE_ORDERED_REMOVE_END
 *      TLT_VOLATILE_REMOVE_END
 * ::get():
 *      TLT_VOLATILE_GET_START
 *      TLT_VOLATILE_GET_END
 * ::multi_get():
 *      TLT_VOLATILE_MULTI_GET_START
 *      TLT_VOLATILE_ORDERED_GET_START
 *      TLT_VOLATILE_ORDERED_GET_END
 *      TLT_VOLATILE_MULTI_GET_END
 * ::list_keys():
 *      TLT_VOLATILE_LIST_KEYS_START
 *      TLT_VOLATILE_LIST_KEYS_END
 * ::multi_list_keys():
 *      TLT_VOLATILE_MULTI_LIST_KEYS_START
 *      TLT_VOLATILE_ORDERED_LIST_KEYS_START
 *      TLT_VOLATILE_ORDERED_LIST_KEYS_END
 *      TLT_VOLATILE_MULTI_LIST_KEYS_END
 * ::get_size():
 *      TLT_VOLATILE_GET_SIZE_START
 *      TLT_VOLATILE_GET_SIZE_END
 * ::multi_get_size():
 *      TLT_VOLATILE_MULTI_GET_SIZE_START
 *      TLT_VOLATILE_ORDERED_GET_SIZE_START
 *      TLT_VOLATILE_ORDERED_GET_SIZE_END
 *      TLT_VOLATILE_MULTI_GET_SIZE_END
 */
#define TLT_VOLATILE_PUT_START                      (2001)
#define TLT_VOLATILE_ORDERED_PUT_START              (2002)
#define TLT_VOLATILE_ORDERED_PUT_END                (2003)
#define TLT_VOLATILE_PUT_END                        (2004)

#define TLT_VOLATILE_PUT_AND_FORGET_START           (2011)
#define TLT_VOLATILE_ORDERED_PUT_AND_FORGET_START   (2012)
#define TLT_VOLATILE_ORDERED_PUT_AND_FORGET_END     (2013)
#define TLT_VOLATILE_PUT_AND_FORGET_END             (2014)

#define TLT_VOLATILE_TRIGGER_PUT_START              (2021)
#define TLT_VOLATILE_TRIGGER_PUT_END                (2022)

#define TLT_VOLATILE_REMOVE_START                   (2031)
#define TLT_VOLATILE_ORDERED_REMOVE_START           (2032)
#define TLT_VOLATILE_ORDERED_REMOVE_END             (2033)
#define TLT_VOLATILE_REMOVE_END                     (2034)

#define TLT_VOLATILE_GET_START                      (2041)
#define TLT_VOLATILE_GET_END                        (2042)

#define TLT_VOLATILE_MULTI_GET_START                (2051)
#define TLT_VOLATILE_ORDERED_GET_START              (2052)
#define TLT_VOLATILE_ORDERED_GET_END                (2053)
#define TLT_VOLATILE_MULTI_GET_END                  (2054)

#define TLT_VOLATILE_LIST_KEYS_START                (2061)
#define TLT_VOLATILE_LIST_KEYS_END                  (2062)

#define TLT_VOLATILE_MULTI_LIST_KEYS_START          (2071)
#define TLT_VOLATILE_ORDERED_LIST_KEYS_START        (2072)
#define TLT_VOLATILE_ORDERED_LIST_KEYS_END          (2073)
#define TLT_VOLATILE_MULTI_LIST_KEYS_END            (2074)

#define TLT_VOLATILE_GET_SIZE_START                 (2081)
#define TLT_VOLATILE_GET_SIZE_END                   (2082)

#define TLT_VOLATILE_MULTI_GET_SIZE_START           (2091)
#define TLT_VOLATILE_ORDERED_GET_SIZE_START         (2092)
#define TLT_VOLATILE_ORDERED_GET_SIZE_END           (2093)
#define TLT_VOLATILE_MULTI_GET_SIZE_END             (2094)

/* For PersistentCascadeStore:
 * ::put():
 *      TLT_PERSISTENT_PUT_START
 *      TLT_PERSISTENT_ORDERED_PUT_START
 *      TLT_PERSISTENT_ORDERED_PUT_END
 *      TLT_PERSISTENT_PUT_END
 * ::put_and_forget():
 *      TLT_PERSISTENT_PUT_AND_FORGET_START
 *      TLT_PERSISTENT_ORDERED_PUT_AND_FORGET_START
 *      TLT_PERSISTENT_ORDERED_PUT_AND_FORGET_END
 *      TLT_PERSISTENT_PUT_AND_FORGET_END
 * ::trigger_put():
 *      TLT_PERSISTENT_TRIGGER_PUT_START
 *      TLT_PERSISTENT_TRIGGER_PUT_END
 * ::remove():
 *      TLT_PERSISTENT_REMOVE_START
 *      TLT_PERSISTENT_ORDERED_REMOVE_START
 *      TLT_PERSISTENT_ORDERED_REMOVE_END
 *      TLT_PERSISTENT_REMOVE_END
 * ::get():
 *      TLT_PERSISTENT_GET_START
 *      TLT_PERSISTENT_GET_END
 * ::multi_get():
 *      TLT_PERSISTENT_MULTI_GET_START
 *      TLT_PERSISTENT_ORDERED_GET_START
 *      TLT_PERSISTENT_ORDERED_GET_END
 *      TLT_PERSISTENT_MULTI_GET_END
 * ::list_keys():
 *      TLT_PERSISTENT_LIST_KEYS_START
 *      TLT_PERSISTENT_LIST_KEYS_END
 * ::multi_list_keys():
 *      TLT_PERSISTENT_MULTI_LIST_KEYS_START
 *      TLT_PERSISTENT_ORDERED_LIST_KEYS_START
 *      TLT_PERSISTENT_ORDERED_LIST_KEYS_END
 *      TLT_PERSISTENT_MULTI_LIST_KEYS_END
 * ::get_size():
 *      TLT_PERSISTENT_GET_SIZE_START
 *      TLT_PERSISTENT_GET_SIZE_END
 * ::multi_get_size():
 *      TLT_PERSISTENT_MULTI_GET_SIZE_START
 *      TLT_PERSISTENT_ORDERED_GET_SIZE_START
 *      TLT_PERSISTENT_ORDERED_GET_SIZE_END
 *      TLT_PERSISTENT_MULTI_GET_SIZE_END
 */
#define TLT_PERSISTENT_PUT_START                    (3001)
#define TLT_PERSISTENT_ORDERED_PUT_START            (3002)
#define TLT_PERSISTENT_ORDERED_PUT_END              (3003)
#define TLT_PERSISTENT_PUT_END                      (3004)

#define TLT_PERSISTENT_PUT_AND_FORGET_START         (3011)
#define TLT_PERSISTENT_ORDERED_PUT_AND_FORGET_START (3012)
#define TLT_PERSISTENT_ORDERED_PUT_AND_FORGET_END   (3013)
#define TLT_PERSISTENT_PUT_AND_FORGET_END           (3014)

#define TLT_PERSISTENT_TRIGGER_PUT_START            (3021)
#define TLT_PERSISTENT_TRIGGER_PUT_END              (3022)

#define TLT_PERSISTENT_REMOVE_START                 (3031)
#define TLT_PERSISTENT_ORDERED_REMOVE_START         (3032)
#define TLT_PERSISTENT_ORDERED_REMOVE_END           (3033)
#define TLT_PERSISTENT_REMOVE_END                   (3034)

#define TLT_PERSISTENT_GET_START                    (3041)
#define TLT_PERSISTENT_GET_END                      (3042)

#define TLT_PERSISTENT_MULTI_GET_START              (3051)
#define TLT_PERSISTENT_ORDERED_GET_START            (3052)
#define TLT_PERSISTENT_ORDERED_GET_END              (3053)
#define TLT_PERSISTENT_MULTI_GET_END                (3054)

#define TLT_PERSISTENT_LIST_KEYS_START              (3061)
#define TLT_PERSISTENT_LIST_KEYS_END                (3062)

#define TLT_PERSISTENT_MULTI_LIST_KEYS_START        (3071)
#define TLT_PERSISTENT_ORDERED_LIST_KEYS_START      (3072)
#define TLT_PERSISTENT_ORDERED_LIST_KEYS_END        (3073)
#define TLT_PERSISTENT_MULTI_LIST_KEYS_END          (3074)

#define TLT_PERSISTENT_GET_SIZE_START               (3081)
#define TLT_PERSISTENT_GET_SIZE_END                 (3082)

#define TLT_PERSISTENT_MULTI_GET_SIZE_START         (3091)
#define TLT_PERSISTENT_ORDERED_GET_SIZE_START       (3092)
#define TLT_PERSISTENT_ORDERED_GET_SIZE_END         (3093)
#define TLT_PERSISTENT_MULTI_GET_SIZE_END           (3094)

/* For TriggerCascadeNoStore:
 * ::trigger_put():
 *      TLT_TRIGGER_PUT_START
 *      TLT_TRIGGER_PUT_END
 */
#define TLT_TRIGGER_PUT_START                       (4001)
#define TLT_TRIGGER_PUT_END                         (4002)

/*
 * For Persistent:
 *      TLT_PERSISTED
 */
#define TLT_PERSISTED                               (5001)

/*
 * For UDLs:
 *      TLT_ACTION_POST     The time when action is inserted into an action_queue for off critical data path processing.
 *                          The extra info is defined as follows:
 *                          struct {
 *                              uint8_t trigger;    // 0 - for ordered_put; 1 - for trigger_put
 *                              uint8_t stateful;   // Following DataFlowGRaph::Statefulness enumerate
 *                              uint8_t rsv8_0;
 *                              uint8_t rsv8_1;
 *                              uint32_t rsv32_0;
 *                          } info
 *      TLT_ACTION_FIRE     The time when action is fired by a worker thread. The extra info is defined as follows:
 *                          struct {
 *                              uint32_t worker_id; // the id of the worker thread
 *                              uint32_t rsv;
 *                          }
 */

typedef union __attribute__((packed,aligned(8))) action_post_extra_info {
    struct {
        uint8_t is_trigger;
        uint8_t stateful;
        uint8_t rsv8_0;
        uint8_t rsv8_1;
        uint32_t rsv32_0;
    }           info;
    uint64_t    uint64_val;
} ActionPostExtraInfo;
#define TLT_ACTION_POST_START                       (6001)
#define TLT_ACTION_POST_END                         (6002)

/*
 * TODO: add the following timestamps. I haven't done it yet was because Action.fire() has not type information so
 * that it cannot access the local id. Fix it later.
 */
typedef union __attribute__((packed,aligned(8))) action_fire_extra_info {
    struct {
        uint32_t worker_id;
        uint32_t rsv;
    }           info;
    uint64_t    uint64_val;
} ActionFireExtraInfo;
#define TLT_ACTION_FIRE_START                       (6003)
#define TLT_ACTION_FIRE_END                         (6004)

#define CASCADE_TIMESTAMP_TAG_FILTER        "CASCADE/timestamp_tag_enabler"

class TimestampLogger {
private:
    std::vector<std::tuple<uint64_t,uint64_t,uint64_t,uint64_t,uint64_t>> _log;
    pthread_spinlock_t lck;
    std::unordered_set<uint64_t> tag_enabler;
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
    void instance_log(uint64_t tag, uint64_t node_id, uint64_t msg_id, uint64_t ts_ns, uint64_t extra=0ull);
    /**
     * Flush log to file
     * @param filename  filename
     * @param clear     True for clear the log after flush
     */
    void instance_flush(const std::string& filename, bool clear = true);
    /**
     * Clear the log
     */
    void instance_clear();

    /** singleton */
    static TimestampLogger _tl;

public:
    /**
     * Log the timestamp
     * @param tag       timestamp tag
     * @param node_id   node id
     * @param msg_id    message id
     * @param ts_ns     timestamp in nanoseconds
     */
    static inline void log(uint64_t tag, uint64_t node_id, uint64_t msg_id, uint64_t ts_ns=get_time_ns(), uint64_t extra=0ull) {
        _tl.instance_log(tag,node_id,msg_id,ts_ns,extra);
    }
    /**
     * Flush log to file
     * @param filename  filename
     * @param clear     True for clear the log after flush
     */
    static inline void flush(const std::string& filename, bool clear = true) {
        _tl.instance_flush(filename,clear);
    }
    /**
     * Clear the log
     */
    static inline void clear() {
        _tl.instance_clear();
    }
};

#endif

/**
 * Evaluate arithmetic expression
 * @param   expression  The arithmetic expression
 *
 * @return  return value
 */
int64_t evaluate_arithmetic_expression(const std::string& expression);
}
}
