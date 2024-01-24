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
#define INVALID_SUBGROUP_INDEX      (std::numeric_limits<uint32_t>::max())
#define INVALID_SHARD_INDEX         (std::numeric_limits<uint32_t>::max())

class PerfTestServer {
public:
    /**
     * The maximum number of distinct test objects to create for test workloads.
     * Fewer will be created if the total size of the objects would be greater than
     * max_workload_memory.
     */
    static const uint32_t max_num_distinct_objects = 4096;
    /**
     * The maximum number of bytes that should be used to create workload objects.
     * This prevents the client from running out of RAM creating large objects.
     */
    static constexpr uint64_t max_workload_memory = 16 * 1073741824L;
    /**
     * The number of milliseconds between put() updates in the setup phase of
     * the get_by_time tests. This determines how many log entries (updates)
     * correspond to one second of timestamps.
     */
    static const uint32_t get_by_time_put_interval = 10;
private:
    ServiceClientAPI&   capi;
    ::rpc::server       server;
    std::vector<ObjectWithStringKey> objects;

    /**
     * evaluating put operation
     *
     * @param max_operation_per_second  max message rate
     * @param duration_secs         experiment duration in seconds
     * @param subgroup_type_index
     * @param subgroup_index        If subgroup_index and shard_index are both valid, the test will use object pool API.
     * @param shard_index           If subgroup_index and shard_index are both valid, the test will use object pool API.
     *
     * @return true/false
     */
    bool eval_put(uint64_t max_operation_per_second,
                  uint64_t duration_secs,
                  uint32_t subgroup_type_index,
                  uint32_t subgroup_index=INVALID_SUBGROUP_INDEX,
                  uint32_t shard_index=INVALID_SHARD_INDEX);

    /**
     * evaluating put_and_forget operation
     *
     * @param max_operation_per_second  max message rate
     * @param duration_secs         experiment duration in seconds
     * @param subgroup_type_index
     * @param subgroup_index        If subgroup_index and shard_index are both valid, the test will use object pool API.
     * @param shard_index           If subgroup_index and shard_index are both valid, the test will use object pool API.
     *
     * @return true/false
     */
    bool eval_put_and_forget(uint64_t max_operation_per_second,
                             uint64_t duration_secs,
                             uint32_t subgroup_type_index,
                             uint32_t subgroup_index=INVALID_SUBGROUP_INDEX,
                             uint32_t shard_index=INVALID_SHARD_INDEX);

    /**
     * evaluating trigger put operation
     *
     * @param max_operation_per_second  max message rate
     * @param duration_secs         experiment duration in seconds
     * @param subgroup_type_index
     * @param subgroup_index        If subgroup_index and shard_index are both valid, the test will use object pool API.
     * @param shard_index           If subgroup_index and shard_index are both valid, the test will use object pool API.
     *
     * @return true/false
     */
    bool eval_trigger_put(uint64_t max_operation_per_second,
                          uint64_t duration_secs,
                          uint32_t subgroup_type_index,
                          uint32_t subgroup_index=INVALID_SUBGROUP_INDEX,
                          uint32_t shard_index=INVALID_SHARD_INDEX);


    /**
     * Evaluate get operations. This function will first call put() a sufficient
     * number of times to create a log of the requested depth for each key, then
     * measure the duration of a sequence of get() operations.
     *
     * @param log_depth The number of versions in the past to request on each read.
     * The put() operations that run before starting the experiment will ensure
     * there are at least this many versions in the log. 0 means to request the
     * current version, while -1 means to call multi_get() (which does a multicast
     * to ensure it gets the latest version) rather than get(). Log depths greater
     * than 0 are not supported when the subgroup type is "volatile."
     * @param max_operations_per_second Maximum message rate
     * @param duration_secs Experiment duration in seconds
     * @param subgroup_type_index Index of the targeted subgroup type within the Cascade template parameters
     * @param subgroup_index Specific subgroup to target. If subgroup_index and shard_index are both invalid, the test will use the object pool API.
     * @param shard_index Specific shard to target. If subgroup_index and shard_index are both invalid, the test will use the object pool API.
     * @return true if experiment completes successfully, false on error
     */
    bool eval_get(int32_t log_depth,
                  uint64_t max_operations_per_second,
                  uint64_t duration_secs,
                  uint32_t subgroup_type_index,
                  uint32_t subgroup_index=INVALID_SUBGROUP_INDEX,
                  uint32_t shard_index=INVALID_SHARD_INDEX);

    /**
     * Evaluate get_by_time operations. This function will first call put() at
     * a steady rate to give each test object a history of at least the
     * requested number of milliseconds, then call get_by_time() for the oldest
     * timestamp associated with each key.
     *
     * @param ms_in_past The number of milliseconds in the past, relative to
     * the time at which the setup phase finishes, to request in each
     * get_by_time() call.
     * @param max_operations_per_second Maximum rate at which to call
     * get_by_time() during the experiment
     * @param duration_secs Experiment duration in seconds
     * @param subgroup_type_index Index of the targeted subgroup type within the Cascade template parameters
     * @param subgroup_index Specific subgroup to target. If subgroup_index and shard_index are both invalid, the test will use the object pool API.
     * @param shard_index Specific shard to target. If subgroup_index and shard_index are both invalid, the test will use the object pool API.
     * @return true if experiment completes successfully, false on error
     */
    bool eval_get_by_time(uint64_t ms_in_past,
                          uint64_t max_operations_per_second,
                          uint64_t duration_secs,
                          uint32_t subgroup_type_index,
                          uint32_t subgroup_index = INVALID_SUBGROUP_INDEX,
                          uint32_t shard_index = INVALID_SHARD_INDEX);

public:
    /**
     * Constructor
     * @param[in]   capi
     * @param[in]   port
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

/*
 * The put type is for the perf_put() function
 */
enum PutType {
    PUT,            // normal put
    PUT_AND_FORGET, // put and forget
    TRIGGER_PUT     // trigger put
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
     * @param put_type
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
    bool perf_put(PutType               put_type,
                  const std::string&    object_pool_pathname,
                  ExternalClientToCascadeServerMapping ec2cs,
                  double                read_write_ratio,
                  uint64_t              ops_threshold,
                  uint64_t              duration_secs,
                  const std::string&    output_file);

    /**
     * Object Pool performance test using only gets
     *
     * @tparam SubgroupType The type of the subgroup containing the object pool
     * @param object_pool_pathname
     *        The object pool to test
     * @param client_server_mapping
     *        The policy for mapping external clients to shard members
     * @param log_depth
     *        The number of versions in the past to request in each get request.
     *        0 means to request the current version, while -1 means to call
     *        multi_get() to get the latest version. Depths greater than 0 are
     *        not supported when SubgroupType is not PersistentCascadeStore.
     * @param ops_threshold
     *        The maximum number of operations per second to submit from each client
     * @param duration_secs
     *        How long each client should run the test for
     * @param output_filename
     *        The output file to write timestamps to after running the test
     * @return true for a successful run,
     * @return false for a failed run
     */
    template<typename SubgroupType>
    bool perf_get(const std::string& object_pool_pathname,
                  ExternalClientToCascadeServerMapping client_server_mapping,
                  int32_t log_depth,
                  uint64_t ops_threshold,
                  uint64_t duration_secs,
                  const std::string& output_filename);

    /**
     * Object Pool get_by_time performance test. This can only be called on
     * PersistentCascadeStore, since that's the only type that supports get_by_time.
     *
     * @tparam SubgroupType The type of subgroup to test. Must be PersistentCascadeStore.
     * @param object_pool_pathname
     *        The object pool to test
     * @param client_server_mapping
     *        The policy for mapping external clients to shard members
     * @param ms_in_past
     *        The number of milliseconds in the past, relative to the time at the
     *        start of the experiment, to use as the timestamp to request for each
     *        get_by_time().
     * @param ops_threshold
     *        The maximum number of operations per second to submit from each client
     * @param duration_secs
     *        How long each client should run the test for
     * @param output_filename
     *        The output file to write timestamps to after running the test
     * @return true for a successful run, false for a failed run
     */
    template <typename SubgroupType>
    bool perf_get_by_time(const std::string& object_pool_pathname,
                          ExternalClientToCascadeServerMapping client_server_mapping,
                          uint64_t ms_in_past,
                          uint64_t ops_threshold,
                          uint64_t duration_secs,
                          const std::string& output_filename);
    /**
     * Single Shard Performance testing, using put with return
     * @param put_type
     * @param subgroup_index
     * @param shard_index
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
    bool perf_put(PutType   put_type,
                  uint32_t  subgroup_index,
                  uint32_t  shard_index,
                  ExternalClientToCascadeServerMapping ec2cs,
                  double    read_write_ratio,
                  uint64_t  ops_threshold,
                  uint64_t  duration_secs,
                  const std::string&    output_file);

    /**
     * Single Shard performance test, using only gets
     *
     * @tparam SubgroupType The type of the subgroup to test
     * @param subgroup_index
     * @param shard_index
     * @param client_server_mapping
     *        The policy for mapping external clients to shard members
     * @param log_depth
     *        The number of versions in the past to request in each get request.
     *        0 means to request the current version, while -1 means to call
     *        multi_get() to get the latest version. Depths greater than 0 are
     *        not supported when SubgroupType is not PersistentCascadeStore.
     * @param ops_threshold
     *        The maximum number of operations per second to submit from each client
     * @param duration_secs
     *        How long each client should run the test for
     * @param output_filename
     *        The output file to write timestamps to after running the test
     * @return true
     * @return false
     */
    template<typename SubgroupType>
    bool perf_get(uint32_t  subgroup_index,
                  uint32_t  shard_index,
                  ExternalClientToCascadeServerMapping client_server_mapping,
                  int32_t log_depth,
                  uint64_t ops_threshold,
                  uint64_t duration_secs,
                  const std::string& output_filename);

    /**
     * Single shard performance test of get_by_time. This can only be called on
     * PersistentCascadeStore, since it's the only one that supports that method.
     *
     * @tparam SubgroupType The type of subgroup to test. Must be PersistentCascadeStore.
     * @param subgroup_index
     * @param shard_index
     * @param client_server_mapping
     *        The policy for mapping external clients to shard members
     * @param ms_in_past
     *        The number of milliseconds in the past, relative to the time at the
     *        start of the experiment, to use as the timestamp to request for each
     *        get_by_time().
     * @param ops_threshold
     *        The maximum number of operations per second to submit from each client
     * @param duration_secs
     *        How long each client should run the test for
     * @param output_filename
     *        The output file to write timestamps to after running the test
     * @return true for a successful run, false for a failed run
     */
    template <typename SubgroupType>
    bool perf_get_by_time(uint32_t subgroup_index,
                          uint32_t shard_index,
                          ExternalClientToCascadeServerMapping client_server_mapping,
                          uint64_t ms_in_past,
                          uint64_t ops_threshold,
                          uint64_t duration_secs,
                          const std::string& output_filename);
    /**
     * Destructor
     */
    virtual ~PerfTestClient();
};

template<typename SubgroupType>
bool PerfTestClient::perf_put(PutType               put_type,
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
    ShardMemberSelectionPolicy policy = ShardMemberSelectionPolicy::Random;
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

    std::string rpc_cmd{};
    switch(put_type){
    case PutType::PUT:
        rpc_cmd = "perf_put_to_objectpool";
        break;
    case PutType::PUT_AND_FORGET:
        rpc_cmd = "perf_put_and_forget_to_objectpool";
        break;
    case PutType::TRIGGER_PUT:
        rpc_cmd = "perf_trigger_put_to_objectpool";
        break;
    }
    int64_t start_sec = static_cast<int64_t>(get_walltime())/INT64_1E9 + 5; // wait for 5 second so that the rpc servers are started.

    std::map<std::pair<std::string,uint16_t>,std::future<RPCLIB_MSGPACK::object_handle>> futures;
    for (auto& kv: connections) {
        futures.emplace(kv.first,kv.second->async_call(rpc_cmd,
                                                       object_pool_pathname,
                                                       static_cast<uint32_t>(policy),
                                                       user_specified_node_ids.at(kv.first),
                                                       read_write_ratio,
                                                       ops_threshold,
                                                       start_sec,
                                                       duration_secs,
                                                       output_filename));
    }

    ret = check_rpc_futures(std::move(futures));

    // 3 - flush server timestamps
    capi.template dump_timestamp(output_filename,object_pool_pathname);

    debug_leave_func();
    return ret;
}

template <typename SubgroupType>
bool PerfTestClient::perf_get(const std::string& object_pool_pathname,
                              ExternalClientToCascadeServerMapping client_server_mapping,
                              int32_t log_depth,
                              uint64_t ops_threshold,
                              uint64_t duration_secs,
                              const std::string& output_filename) {
    debug_enter_func_with_args("object_pool_pathname={},ec2cs={},log_depth={},ops_threshold={},duration_secs={},output_filename={}",
                               object_pool_pathname, static_cast<uint32_t>(client_server_mapping), log_depth, ops_threshold, duration_secs, output_filename);
    bool ret = true;
    // 1 - decides on shard membership policy for the "policy" and "user_specified_node_ids" argument for rpc calls.
    ShardMemberSelectionPolicy policy = ShardMemberSelectionPolicy::Random;
    auto object_pool = capi.find_object_pool(object_pool_pathname);
    if(!object_pool.is_valid() || object_pool.is_null()) {
        throw derecho::derecho_exception("Cannot find object pool:" + object_pool_pathname);
    }
    uint32_t number_of_shards = capi.get_number_of_shards<SubgroupType>(object_pool.subgroup_index);
    std::map<std::pair<std::string, uint16_t>, std::vector<node_id_t>> user_specified_node_ids;
    for(const auto& kv : connections) {
        user_specified_node_ids.emplace(kv.first, std::vector<node_id_t>{number_of_shards});
    }
    switch(client_server_mapping) {
        case ExternalClientToCascadeServerMapping::FIXED:
            policy = ShardMemberSelectionPolicy::UserSpecified;
            for(uint32_t shard_index = 0; shard_index < number_of_shards; shard_index++) {
                auto shard_members = capi.template get_shard_members<SubgroupType>(object_pool.subgroup_index, shard_index);
                uint32_t connection_index = 0;
                for(const auto& kv : connections) {
                    user_specified_node_ids[kv.first].at(shard_index) = shard_members.at(connection_index % shard_members.size());
                    connection_index++;
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

    std::string rpc_cmd = "perf_get_to_objectpool";
    int64_t start_sec = static_cast<int64_t>(get_walltime()) / INT64_1E9 + 5;  // wait for 5 second so that the rpc servers are started.

    std::map<std::pair<std::string, uint16_t>, std::future<RPCLIB_MSGPACK::object_handle>> futures;
    for(auto& kv : connections) {
        futures.emplace(kv.first, kv.second->async_call(rpc_cmd,
                                                        object_pool_pathname,
                                                        static_cast<uint32_t>(policy),
                                                        user_specified_node_ids.at(kv.first),
                                                        log_depth,
                                                        ops_threshold,
                                                        start_sec,
                                                        duration_secs,
                                                        output_filename));
    }

    ret = check_rpc_futures(std::move(futures));

    // 3 - flush server timestamps
    capi.template dump_timestamp(output_filename, object_pool_pathname);

    debug_leave_func();
    return ret;
}

template <typename SubgroupType>
bool PerfTestClient::perf_get_by_time(const std::string& object_pool_pathname,
                                      ExternalClientToCascadeServerMapping client_server_mapping,
                                      uint64_t ms_in_past,
                                      uint64_t ops_threshold,
                                      uint64_t duration_secs,
                                      const std::string& output_filename) {
    debug_enter_func_with_args("object_pool_pathname={},ec2cs={},ms_in_past={},ops_threshold={},duration_secs={},output_filename={}",
                               object_pool_pathname, static_cast<uint32_t>(client_server_mapping), ms_in_past, ops_threshold, duration_secs, output_filename);
    bool ret = true;
    // 1 - decide on shard membership policy for the "policy" and "user_specified_node_ids" argument for rpc calls
    ShardMemberSelectionPolicy policy = ShardMemberSelectionPolicy::Random;
    auto object_pool = capi.find_object_pool(object_pool_pathname);
    if(!object_pool.is_valid() || object_pool.is_null()) {
        throw derecho::derecho_exception("Cannot find object pool:" + object_pool_pathname);
    }
    uint32_t number_of_shards = capi.get_number_of_shards<SubgroupType>(object_pool.subgroup_index);
    std::map<std::pair<std::string, uint16_t>, std::vector<node_id_t>> user_specified_node_ids;
    for(const auto& kv : connections) {
        user_specified_node_ids.emplace(kv.first, std::vector<node_id_t>{number_of_shards});
    }
    switch(client_server_mapping) {
        case ExternalClientToCascadeServerMapping::FIXED:
            policy = ShardMemberSelectionPolicy::UserSpecified;
            for(uint32_t shard_index = 0; shard_index < number_of_shards; shard_index++) {
                auto shard_members = capi.template get_shard_members<SubgroupType>(object_pool.subgroup_index, shard_index);
                uint32_t connection_index = 0;
                for(const auto& kv : connections) {
                    user_specified_node_ids[kv.first].at(shard_index) = shard_members.at(connection_index % shard_members.size());
                    connection_index++;
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
    std::string rpc_cmd = "perf_get_by_time_to_objectpool";
    int64_t start_sec = static_cast<int64_t>(get_walltime()) / INT64_1E9 + 5;  // wait for 5 second so that the rpc servers are started.

    std::map<std::pair<std::string, uint16_t>, std::future<RPCLIB_MSGPACK::object_handle>> futures;
    for(auto& kv : connections) {
        futures.emplace(kv.first, kv.second->async_call(rpc_cmd,
                                                        object_pool_pathname,
                                                        static_cast<uint32_t>(policy),
                                                        user_specified_node_ids.at(kv.first),
                                                        ms_in_past,
                                                        ops_threshold,
                                                        start_sec,
                                                        duration_secs,
                                                        output_filename));
    }

    ret = check_rpc_futures(std::move(futures));

    // 3 - flush server timestamps
    capi.template dump_timestamp(output_filename, object_pool_pathname);

    debug_leave_func();
    return ret;
}

template <typename SubgroupType>
bool PerfTestClient::perf_put(PutType   put_type,
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
    ShardMemberSelectionPolicy policy = ShardMemberSelectionPolicy::Random;
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

    std::string rpc_cmd{};
    switch(put_type) {
    case PutType::PUT:
        rpc_cmd = "perf_put_to_shard";
        break;
    case PutType::PUT_AND_FORGET:
        rpc_cmd = "perf_put_and_forget_to_shard";
        break;
    case PutType::TRIGGER_PUT:
        rpc_cmd = "perf_trigger_put_to_shard";
        break;
    }

    int64_t start_sec = static_cast<int64_t>(get_walltime())/INT64_1E9 + 5; // wait for 5 second so that the rpc servers are started.

    for (auto& kv: connections) {
        futures.emplace(kv.first,kv.second->async_call(rpc_cmd,
                                                       capi.template get_subgroup_type_index<SubgroupType>(),
                                                       subgroup_index,shard_index,static_cast<uint32_t>(policy),
                                                       user_specified_node_ids.at(kv.first),read_write_ratio,
                                                       ops_threshold,
                                                       start_sec, duration_secs,output_filename));
    }
    ret = check_rpc_futures(std::move(futures));

    // 3 - flush server timestamp
    auto qr = capi.template dump_timestamp<SubgroupType>(output_filename,subgroup_index,shard_index);
    qr.get();

    debug_leave_func();
    return ret;
}

template <typename SubgroupType>
bool PerfTestClient::perf_get(uint32_t subgroup_index,
                              uint32_t shard_index,
                              ExternalClientToCascadeServerMapping client_server_mapping,
                              int32_t log_depth,
                              uint64_t ops_threshold,
                              uint64_t duration_secs,
                              const std::string& output_filename) {
    debug_enter_func_with_args("subgroup_index={},shard_index={},ec2cs={},log_depth={},ops_threshold={},duration_secs={},output_filename={}",
                               subgroup_index, shard_index, static_cast<uint32_t>(client_server_mapping), log_depth, ops_threshold, duration_secs, output_filename);
    bool ret = true;
    // 1 - decides on shard membership policy for the "policy" and "user_specified_node_ids" argument for rpc calls.
    ShardMemberSelectionPolicy policy = ShardMemberSelectionPolicy::Random;
    std::map<std::pair<std::string, uint16_t>, node_id_t> user_specified_node_ids;
    switch(client_server_mapping) {
        case ExternalClientToCascadeServerMapping::FIXED: {
            policy = ShardMemberSelectionPolicy::UserSpecified;
            auto shard_members = capi.template get_shard_members<SubgroupType>(subgroup_index, shard_index);
            uint32_t connection_index = 0;
            for(const auto& kv : connections) {
                user_specified_node_ids[kv.first] = shard_members.at(connection_index % shard_members.size());
                connection_index++;
            }
        } break;
        case ExternalClientToCascadeServerMapping::RANDOM:
            policy = ShardMemberSelectionPolicy::Random;
            break;
        case ExternalClientToCascadeServerMapping::ROUNDROBIN:
            policy = ShardMemberSelectionPolicy::RoundRobin;
            break;
    };
    // 2 - send requests and wait for response

    std::string rpc_cmd = "perf_get_to_shard";
    int64_t start_sec = static_cast<int64_t>(get_walltime()) / INT64_1E9 + 5;  // wait for 5 second so that the rpc servers are started.

    std::map<std::pair<std::string, uint16_t>, std::future<RPCLIB_MSGPACK::object_handle>> futures;
    for(auto& kv : connections) {
        futures.emplace(kv.first, kv.second->async_call(rpc_cmd,
                                                        capi.template get_subgroup_type_index<SubgroupType>(),
                                                        subgroup_index,
                                                        shard_index,
                                                        static_cast<uint32_t>(policy),
                                                        user_specified_node_ids.at(kv.first),
                                                        log_depth,
                                                        ops_threshold,
                                                        start_sec,
                                                        duration_secs,
                                                        output_filename));
    }

    ret = check_rpc_futures(std::move(futures));

    // 3 - flush server timestamps
    capi.template dump_timestamp<SubgroupType>(output_filename, subgroup_index, shard_index);

    debug_leave_func();
    return ret;
}

template <typename SubgroupType>
bool PerfTestClient::perf_get_by_time(uint32_t subgroup_index,
                                      uint32_t shard_index,
                                      ExternalClientToCascadeServerMapping client_server_mapping,
                                      uint64_t ms_in_past,
                                      uint64_t ops_threshold,
                                      uint64_t duration_secs,
                                      const std::string& output_filename) {
    bool ret = true;
    // 1 - decides on shard membership policy for the "policy" and "user_specified_node_ids" argument for rpc calls.
    ShardMemberSelectionPolicy policy = ShardMemberSelectionPolicy::Random;
    std::map<std::pair<std::string, uint16_t>, node_id_t> user_specified_node_ids;
    switch(client_server_mapping) {
        case ExternalClientToCascadeServerMapping::FIXED: {
            policy = ShardMemberSelectionPolicy::UserSpecified;
            auto shard_members = capi.template get_shard_members<SubgroupType>(subgroup_index, shard_index);
            uint32_t connection_index = 0;
            for(const auto& kv : connections) {
                user_specified_node_ids[kv.first] = shard_members.at(connection_index % shard_members.size());
                connection_index++;
            }
        } break;
        case ExternalClientToCascadeServerMapping::RANDOM:
            policy = ShardMemberSelectionPolicy::Random;
            break;
        case ExternalClientToCascadeServerMapping::ROUNDROBIN:
            policy = ShardMemberSelectionPolicy::RoundRobin;
            break;
    };
    // 2 - send requests and wait for response
    std::string rpc_cmd = "perf_get_by_time_to_shard";
    int64_t start_sec = static_cast<int64_t>(get_walltime()) / INT64_1E9 + 5;  // wait for 5 second so that the rpc servers are started.

    std::map<std::pair<std::string, uint16_t>, std::future<RPCLIB_MSGPACK::object_handle>> futures;
    for(auto& kv : connections) {
        futures.emplace(kv.first, kv.second->async_call(rpc_cmd,
                                                        capi.template get_subgroup_type_index<SubgroupType>(),
                                                        subgroup_index,
                                                        shard_index,
                                                        static_cast<uint32_t>(policy),
                                                        user_specified_node_ids.at(kv.first),
                                                        ms_in_past,
                                                        ops_threshold,
                                                        start_sec,
                                                        duration_secs,
                                                        output_filename));
    }

    ret = check_rpc_futures(std::move(futures));

    // 3 - flush server timestamps
    capi.template dump_timestamp<SubgroupType>(output_filename, subgroup_index, shard_index);

    debug_leave_func();
    return ret;
}
}
}
