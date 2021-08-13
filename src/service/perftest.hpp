#pragma once
#include <cascade/service_client_api.hpp>
#include <limits>
#include <rpc/server.h>
#include <rpc/client.h>
#include <rpc/rpc_error.h>
#include <vector>
#include <derecho/utils/logger.hpp>

namespace derecho {
namespace cascade {

#define PERFTEST_PORT               (18720)
#define NUMBER_OF_DISTINCT_OBJECTS  (4096)
#define INVALID_SUBGROUP_INDEX      (std::numeric_limits<uint32_t>::max())
#define INVALID_SHARD_INDEX         (std::numeric_limits<uint32_t>::max())

class PerfTestServer {
    using put_and_forget_perf_log_t = struct {
        uint64_t first_send_ns;
        uint64_t last_send_ns;
        uint64_t ack_ns;
        uint64_t num_messages;
    };
private:
    ServiceClientAPI&   capi;
    ::rpc::server       server;
    std::vector<ObjectWithStringKey> objects;

    /**
     * Create the workload for test. Please note that the old data in objects will be removed.
     *
     * @param payload_size      Expected size of the serialized object byte array. We are going to calculate a adequate
     *                          object size of the workload.
     * @param key_prefix        The key prefix for the objects created.
     */
    void make_workload(uint32_t payload_size,const std::string& key_prefix);

    /**
     * evaluating put operation
     *
     * @param timestamp_log         Caller provided timestamp_log
     * @param max_operation_per_second  max message rate
     * @param duration_secs         experiment duration in seconds
     * @param subgroup_type_index
     * @param subgroup_index        If subgroup_index and shard_index are both valid, the test will use object pool API.
     * @param shard_index           If subgroup_index and shard_index are both valid, the test will use object pool API.
     *
     * @return true/false
     */
    bool eval_put(std::vector<std::tuple<uint64_t,uint64_t,uint64_t>>& timestamp_log,
                  uint64_t max_operation_per_second,
                  uint64_t duration_secs,
                  uint32_t subgroup_type_index,
                  uint32_t subgroup_index=INVALID_SUBGROUP_INDEX,
                  uint32_t shard_index=INVALID_SHARD_INDEX);

    /**
     * evaluating put_and_forget operation
     *
     * @param timestamp_log         Caller provided timestamp_log
     * @param max_operation_per_second  max message rate
     * @param duration_secs         experiment duration in seconds
     * @param subgroup_type_index
     * @param subgroup_index        If subgroup_index and shard_index are both valid, the test will use object pool API.
     * @param shard_index           If subgroup_index and shard_index are both valid, the test will use object pool API.
     *
     * @return true/false
     */
    bool eval_put_and_forget(put_and_forget_perf_log_t& timestamp_log,
                             uint64_t max_operation_per_second,
                             uint64_t duration_secs,
                             uint32_t subgroup_type_index,
                             uint32_t subgroup_index=INVALID_SUBGROUP_INDEX,
                             uint32_t shard_index=INVALID_SHARD_INDEX);

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
    // helpers
    /** wait for the futures from perftest servers.*/
    bool check_rpc_futures(std::map<std::pair<std::string,uint16_t>,std::future<RPCLIB_MSGPACK::object_handle>>&& futures);
    /** download a file from the perftest server */
    bool download_file(const std::string& filename);

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
     * Object Pool Performance testing, using put with return
     * @param use_put_and_forget if true, use put and forget to keep the pipeline busy.
     * @param object_pool_pathname
     * @param ec2cs
     *        Mapping from external client to a shard member.
     * @param read_write_ratio
     * @param ops_threshold
     * @param duration_secs
     * @param output_file
     *        The log in "message id, version, send_ts_us, acked_ts_us" will be written in the output file.
     * @return true for a successful run, false for a failed run.
     */
    template<typename SubgroupType>
    bool perf_put(bool                  use_put_and_forget,
                  const std::string&    object_pool_pathname,
                  ExternalClientToCascadeServerMapping ec2cs,
                  double                read_write_ratio,
                  uint64_t              ops_threshold,
                  uint64_t              duration_secs,
                  const std::string&    output_file);
    /**
     * Single Shard Performance testing, using put with return
     * @param use_put_and_forget if true, use put and forget to keep the pipeline busy.
     * @param subgroup_index
     * @param subgroup_shard
     * @param ec2cs
     *        Mapping from external client to a shard member.
     * @param read_write_ratio
     * @param ops_threshold
     * @param duration_secs
     * @param output_file
     *        The log in "message id, version, send_ts_us, acked_ts_us" will be written in the output file.
     * @return true for a successful run, false for a failed run.
     */
    template<typename SubgroupType>
    bool perf_put(bool      use_put_and_forget,
                  uint32_t  subgroup_index,
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
    bool PerfTestClient::perf_put(bool                  use_put_and_forget,
                                  const std::string&    object_pool_pathname,
                                  ExternalClientToCascadeServerMapping ec2cs,
                                  double                read_write_ratio,
                                  uint64_t              ops_threshold,
                                  uint64_t              duration_secs,
                                  const std::string&    output_filename) {
    debug_enter_func_with_args("object_pool_pathname={},ec2cs={},read_write_ratio={},ops_threshold={},duration_secs={},output_filename={}",
                               object_pool_pathname,static_cast<uint32_t>(ec2cs),read_write_ratio,ops_threshold,duration_secs,output_filename);
    bool ret = true;
    // 1 - decides on shard membership policy for the "policy" and "user_specified_node_ids" argument for rpc calls.
    ShardMemberSelectionPolicy policy;
    auto object_pool = capi.find_object_pool(object_pool_pathname);
    if (!object_pool.is_valid() || object_pool.is_null()) {
        throw derecho::derecho_exception("Cannot find object pool:" + object_pool_pathname);
    }
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
        futures.emplace(kv.first,kv.second->async_call(use_put_and_forget?"perf_put_and_forget_to_objectpool":"perf_put_to_objectpool",object_pool_pathname,static_cast<uint32_t>(policy),
                        user_specified_node_ids.at(kv.first),read_write_ratio,ops_threshold,duration_secs,output_filename));
    }

    ret = check_rpc_futures(std::move(futures));

    // 3 - download
    if (ret) {
        ret = download_file(output_filename);
    }

    debug_leave_func();
    return ret;
}

template <typename SubgroupType>
bool PerfTestClient::perf_put(bool      use_put_and_forget,
                              uint32_t  subgroup_index,
                              uint32_t  shard_index,
                              ExternalClientToCascadeServerMapping ec2cs,
                              double    read_write_ratio,
                              uint64_t  ops_threshold,
                              uint64_t  duration_secs,
                              const std::string& output_filename) {
    debug_enter_func_with_args("subgroup_index={}, shard_index={}, ec2cs={},read_write_ratio={},ops_threshold={},duration_secs={},output_filename={}",
                               subgroup_index,shard_index,static_cast<uint32_t>(ec2cs),read_write_ratio,ops_threshold,duration_secs,output_filename);
    bool ret = true;
    // 1 - decides on shard membership policy for the "policy" and "user_specified_node_ids" argument for rpc calls.
    ShardMemberSelectionPolicy policy;
    std::map<std::pair<std::string,uint16_t>,node_id_t> user_specified_node_ids;
    switch(ec2cs) {
    case ExternalClientToCascadeServerMapping::FIXED:
        policy = ShardMemberSelectionPolicy::UserSpecified;
        {
            auto shard_members = capi.template get_shard_members<SubgroupType>(subgroup_index,shard_index);
            uint32_t connection_index = 0;
            for(const auto& kv:connections) {
                user_specified_node_ids[kv.first] = shard_members.at(connection_index%shard_members.size());
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
        futures.emplace(kv.first,kv.second->async_call(use_put_and_forget?"perf_put_and_forget_to_shard":"perf_put_to_shard",
                                                       capi.template get_subgroup_type_index<SubgroupType>(),
                                                       subgroup_index,shard_index,static_cast<uint32_t>(policy),
                                                       user_specified_node_ids.at(kv.first),read_write_ratio,
                                                       ops_threshold,duration_secs,output_filename));
    }
    ret = check_rpc_futures(std::move(futures));

    // 3 - download
    if (ret) {
        ret = download_file(output_filename);
    }

    debug_leave_func();
    return ret;
}

}
}
