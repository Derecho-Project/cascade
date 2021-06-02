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

inline uint64_t get_time_us(bool use_wall_clock = true) {
    struct timespec tv;
    clock_gettime(use_wall_clock?CLOCK_REALTIME:CLOCK_MONOTONIC,&tv);
    return (tv.tv_sec*1000000 + tv.tv_nsec/1000);
}

/**
 * decompose a string to tokens.
 */
inline std::vector<std::string> str_tokenizer(const std::string& str, char separator) {
    std::vector<std::string> components;
    std::string::size_type pos=0, spos=0;
    while (pos != std::string::npos) {
        pos = str.find(separator,pos);
        if (pos == std::string::npos) {
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

}
}
