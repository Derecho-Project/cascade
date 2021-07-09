#pragma once
#include <cascade/service_client_api.hpp>
#include <rpc/server.h>
#include <rpc/client.h>
#include <vector>
#include <derecho/utils/logger.hpp>

namespace derecho {
namespace cascade {

#define PERFTEST_PORT               (18720)
#define NUMBER_OF_DISTINCT_OBJECTS  (4096)

class PerfTestServer {
private:
    ServiceClientAPI&   capi;
    ::rpc::server       server;
    std::vector<ObjectWithStringKey> objects;
public:
    /**
     * Constructor
     * @param capi
     */
    PerfTestServer(ServiceClientAPI& capi, uint16_t port = PERFTEST_PORT);

    /**
     * Destructor
     */
    virtual ~PerfTestServer();

};

/**
 * 'ExternalClientToCascadeServerMapping' defines how a set of external clients connecting to a shard pick the member.
 * FIXED: an extern client talk to a fixed member. The clients are allocated to the servers as evenly as possible.
 * RANDOM: All external client pick a server randomly, using the "Random" ShardMemberSelectionPolicy
 * ROUNDROBIN: All external client pick the server in a round-robin way, using the "RoundRobin"
 * ShardMemberSelectionPolicy.
 */
enum ExternalClientToCascadeServerMapping {
    FIXED,
    RANDOM,
    ROUNDROBIN
};

class PerfTestClient {
private:
    std::map<std::pair<std::string,uint16_t>,std::unique_ptr<::rpc::client>> connections;
    ServiceClientAPI& capi;

public:
    /**
     * Constructor
     */
    PerfTestClient(ServiceClientAPI& capi);

    /**
     * add a perf test server
     */
    void add_or_update_server(const std::string& host, uint16_t port);

    /**
     * list perf test servers
     */
    std::vector<std::pair<std::string,uint16_t>> get_connections();

    /**
     * remote a test server
     */
    void remove_server(const std::string& host, uint16_t port);

    /**
     * Object Pool Performance testing
     * @param object_pool_pathname
     * @param ec2cs
     *        Mapping from external client to a shard member.
     * @param read_write_ratio
     * @param ops_threshold
     * @param duration_secs
     * @param output_file
     *        The log in "message id, version, send_ts_us, acked_ts_us" will be written in the output file.
     */
    template<typename SubgroupType>
    void perf(const std::string&    object_pool_pathname,
              ExternalClientToCascadeServerMapping ec2cs,
              double                read_write_ratio,
              uint64_t              ops_threshold,
              uint64_t              duration_secs,
              const std::string&    output_file);
    /**
     * Single Shard Performance testing
     * @param subgroup_index
     * @param subgroup_shard
     * @param ec2cs
     *        Mapping from external client to a shard member.
     * @param read_write_ratio
     * @param ops_threshold
     * @param duration_secs
     * @param output_file
     *        The log in "message id, version, send_ts_us, acked_ts_us" will be written in the output file.
     */
    template<typename SubgroupType>
    void perf(uint32_t  subgroup_index,
              uint32_t  shard_index,
              ExternalClientToCascadeServerMapping ec2cs,
              double    read_write_ratio,
              uint64_t  ops_threshold,
              uint64_t  duration_secs,
              const std::string&    output_file);
    /**
     * Destructor
     */
    virtual ~PerfTestClient();
};

template<typename SubgroupType>
void PerfTestClient::perf(const std::string&    object_pool_pathname,
                          ExternalClientToCascadeServerMapping ec2cs,
                          double                read_write_ratio,
                          uint64_t              ops_threshold,
                          uint64_t              duration_secs,
                          const std::string&    output_filename) {
    debug_enter_func_with_args(object_pool_pathname,static_cast<uint32_t>(ec2cs),read_write_ratio,ops_threshold,duration_secs,output_filename);
    // 1 - decides on shard membership policy for the "policy" and "user_specified_node_ids" argument for rpc calls.
    ShardMemberSelectionPolicy policy;
    auto object_pool = capi.find_object_pool(object_pool_pathname);
    uint32_t number_of_shards = capi.get_number_of_shards<SubgroupType>(object_pool.subgroup_index);
    std::map<std::pair<std::string,uint16_t>,std::vector<node_id_t>> user_specified_node_ids;
    for (const auto& kv:connections) {
        user_specified_node_ids.emplace(kv.first,std::vector<node_id_t>{number_of_shards});
    }
    switch(ec2cs) {
    case ExternalClientToCascadeServerMapping::FIXED:
        policy = ShardMemberSelectionPolicy::UserSpecified;
        for(uint32_t shard_index=0;shard_index<number_of_shards;shard_index++) {
            auto shard_members = capi.template get_shard_members<SubgroupType>(object_pool.subgroup_index,shard_index);
            uint32_t connection_index = 0;
            for(const auto& kv:connections) {
                user_specified_node_ids[kv.first].at(shard_index) = shard_members.at(connection_index%shard_members.size());
                connection_index ++;
            }
        }
        break;
    case ExternalClientToCascadeServerMapping::RANDOM:
        policy = ShardMemberSelectionPolicy::Random;
        break;
    case ExternalClientToCascadeServerMapping::ROUNDROBIN:
        policy = ShardMemberSelectionPolicy::RoundRobin;
        break;
    };
    // 2 - send requests and wait for response
    std::map<std::pair<std::string,uint16_t>,std::future<RPCLIB_MSGPACK::object_handle>> futures;
    for (auto& kv: connections) {
        futures.emplace(kv.first,kv.second->async_call("perf",object_pool_pathname,static_cast<uint32_t>(policy),
                        user_specified_node_ids.at(kv.first),read_write_ratio,ops_threshold,duration_secs,output_filename));
    }

    for(auto& kv:futures) {
        bool result = kv.second.get().as<bool>();
        dbg_default_info("perfserver {}:{} finished with {}.",kv.first.first,kv.first.second,result);
    }
    debug_leave_func();
}

template <typename SubgroupType>
void PerfTestClient::perf(uint32_t  subgroup_index,
                          uint32_t  shard_index,
                          ExternalClientToCascadeServerMapping ec2cs,
                          double    read_write_ratio,
                          uint64_t  ops_threshold,
                          uint64_t  duration_secs,
                          const std::string&    output_file) {
    //TODO:
}

}
}
