#include "perftest.hpp"
#include <derecho/conf/conf.hpp>
#include <derecho/core/detail/rpc_utils.hpp>
#include <type_traits>
#include <optional>
#include <queue>
#include <derecho/utils/time.h>
#include <unistd.h>
#include <fstream>

namespace derecho {
namespace cascade {

//////////////////////////////////////
// PerfTestClient implementation    //
//////////////////////////////////////

#define on_subgroup_type_index(tindex, func, ...) \
    if (std::type_index(typeid(VolatileCascadeStoreWithStringKey)) == tindex) { \
        func <VolatileCascadeStoreWithStringKey>(__VA_ARGS__); \
    } else if (std::type_index(typeid(PersistentCascadeStoreWithStringKey)) == tindex) { \
        func <PersistentCascadeStoreWithStringKey>(__VA_ARGS__); \
    } else if (std::type_index(typeid(TriggerCascadeNoStoreWithStringKey)) == tindex) { \
        func <TriggerCascadeNoStoreWithStringKey>(__VA_ARGS__); \
    } else { \
        throw derecho::derecho_exception(std::string("Unknown type_index:") + tindex.name()); \
    }

#define on_subgroup_type_index_with_return(tindex, result_handler, func, ...) \
    if (std::type_index(typeid(VolatileCascadeStoreWithStringKey)) == tindex) { \
        result_handler(func <VolatileCascadeStoreWithStringKey>(__VA_ARGS__)); \
    } else if (std::type_index(typeid(PersistentCascadeStoreWithStringKey)) == tindex) { \
        result_handler(func <PersistentCascadeStoreWithStringKey>(__VA_ARGS__)); \
    } else if (std::type_index(typeid(TriggerCascadeNoStoreWithStringKey)) == tindex) { \
        result_handler(func <TriggerCascadeNoStoreWithStringKey>(__VA_ARGS__)); \
    } else { \
        throw derecho::derecho_exception(std::string("Unknown type_index:") + tindex.name()); \
    }

#define on_subgroup_type_index_no_trigger(tindex, func, ...) \
    if (std::type_index(typeid(VolatileCascadeStoreWithStringKey)) == tindex) { \
        func <VolatileCascadeStoreWithStringKey>(__VA_ARGS__); \
    } else if (std::type_index(typeid(PersistentCascadeStoreWithStringKey)) == tindex) { \
        func <PersistentCascadeStoreWithStringKey>(__VA_ARGS__); \
    } else { \
        throw derecho::derecho_exception(std::string("Unknown type_index:") + tindex.name()); \
    }

#define on_subgroup_type_index_with_return_no_trigger(tindex, result_handler, func, ...) \
    if (std::type_index(typeid(VolatileCascadeStoreWithStringKey)) == tindex) { \
        result_handler(func <VolatileCascadeStoreWithStringKey>(__VA_ARGS__)); \
    } else if (std::type_index(typeid(PersistentCascadeStoreWithStringKey)) == tindex) { \
        result_handler(func <PersistentCascadeStoreWithStringKey>(__VA_ARGS__)); \
    } else { \
        throw derecho::derecho_exception(std::string("Unknown type_index:") + tindex.name()); \
    }

PerfTestServer::PerfTestServer(ServiceClientAPI& capi, uint16_t port):
    capi(capi),
    server(port) {
    // Initialize objects
    const uint32_t buf_size = derecho::getConfUInt32(CONF_DERECHO_MAX_P2P_REQUEST_PAYLOAD_SIZE) - 256;
    char *buf = (char*)malloc(buf_size);
    memset(buf,'A',buf_size);
    for (uint32_t i=0;i<NUMBER_OF_DISTINCT_OBJECTS;i++) {
        objects.emplace_back("key_"+std::to_string(i),buf,buf_size);
    }
    // API: run perf
    server.bind("perf",[this](
        const std::string&  object_pool_pathname,
        uint32_t            policy,
        std::vector<node_id_t>  user_specified_node_ids, // one per shard
        double              read_write_ratio,
        uint64_t            max_operation_per_second,
        uint64_t            duration_secs,
        const std::string&  output_filename) {

        auto object_pool = this->capi.find_object_pool(object_pool_pathname);

        uint32_t number_of_shards;
        // STEP 1 - set up the shard member selection policy
        on_subgroup_type_index_no_trigger(std::decay_t<decltype(capi)>::subgroup_type_order.at(object_pool.subgroup_type_index),
            number_of_shards = this->capi.template get_number_of_shards, object_pool.subgroup_index);
        if (user_specified_node_ids.size() < number_of_shards) {
            throw derecho::derecho_exception(std::string("The size of 'user_specified_node_ids' argument does not match shard number."));
        }
        for (uint32_t shard_index = 0; shard_index < number_of_shards; shard_index ++) {
            on_subgroup_type_index_no_trigger(std::decay_t<decltype(capi)>::subgroup_type_order.at(object_pool.subgroup_type_index),
                this->capi.template set_member_selection_policy, object_pool.subgroup_index, shard_index, static_cast<ShardMemberSelectionPolicy>(policy), user_specified_node_ids.at(shard_index));
        }
        // STEP 2 - workload and log
        std::vector<std::tuple<uint64_t,uint64_t,uint64_t>> timestamp_log; // version,send_timestamp_ns,reply_timestamp_ns
        uint32_t window_size = derecho::getConfUInt32(CONF_DERECHO_P2P_WINDOW_SIZE);
        // std::queue<std::optional<derecho::QueryResults<std::tuple<persistent::version_t,uint64_t>>>> futures;
        std::queue<std::pair<uint64_t,derecho::QueryResults<std::tuple<persistent::version_t,uint64_t>>>> futures;
        //TODO: control read_write_ratio
        uint64_t interval_ns = static_cast<uint64_t>(1e9/max_operation_per_second);
        uint64_t next_ns = get_walltime();
        uint64_t end_ns = next_ns + duration_secs*1000000000;
        while(true) {
            uint64_t now_ns = get_walltime();
            if (now_ns > end_ns) {
                break;
            }
            if (now_ns + 1000 > next_ns) {
                usleep(now_ns - next_ns + 1000);
            }
            if (futures.size() >= window_size) {
                auto& replies = futures.front().second.get();
                for (auto& reply: replies) {
                    auto version = std::get<0>(reply.second.get());
                    uint64_t reply_timestamp_ns = get_walltime();
                    uint64_t send_timestamp_ns = futures.front().first;
                    timestamp_log.emplace_back(version, send_timestamp_ns, reply_timestamp_ns);
                    break;
                }
                futures.pop();
            }
            while (futures.size() < window_size && ((get_walltime() + 1000) > next_ns)) {
                next_ns += interval_ns;
                std::function<void(QueryResults<std::tuple<persistent::version_t,uint64_t>>)> future_appender = 
                    [&futures](QueryResults<std::tuple<persistent::version_t,uint64_t>>&& query_results){
                        futures.emplace(get_walltime(),std::move(query_results));
                    };
                on_subgroup_type_index_with_return_no_trigger(
                    std::decay_t<decltype(capi)>::subgroup_type_order.at(object_pool.subgroup_type_index),
                    future_appender,
                    this->capi.template put, objects.at(get_walltime()%NUMBER_OF_DISTINCT_OBJECTS));
            }
            // extract ready futures.
            while (futures.size() > 0) {
                if (!futures.front().second.is_ready()) {
                    break;
                }
                auto& replies = futures.front().second.get();
                for (auto& reply: replies) {
                    auto version = std::get<0>(reply.second.get());
                    uint64_t reply_timestamp_ns = get_walltime();
                    uint64_t send_timestamp_ns = futures.front().first;
                    timestamp_log.emplace_back(version, send_timestamp_ns, reply_timestamp_ns);
                    break;
                }
                futures.pop();
            }
        }
        // wait for all pending futures.
        while (futures.size() > 0) {
            auto& replies = futures.front().second.get();
            for (auto& reply: replies) {
                auto version = std::get<0>(reply.second.get());
                uint64_t reply_timestamp_ns = get_walltime();
                uint64_t send_timestamp_ns = futures.front().first;
                timestamp_log.emplace_back(version, send_timestamp_ns, reply_timestamp_ns);
                break;
            }
            futures.pop();
        }
        // write output to file
        std::ofstream outfile(output_filename);
        outfile << "#version,send_ts_us,acked_ts_us" << std::endl;
        for (const auto& le:timestamp_log) {
            outfile << std::get<0>(le) << "," << (std::get<1>(le)/1000) << "," << (std::get<2>(le)/1000) << std::endl;
        }
        outfile.close();
        return true;
    });
    // start the worker thread asynchronously
    server.async_run(1);
}

PerfTestServer::~PerfTestServer() {
    server.stop();
}

//////////////////////////////////////
// PerfTestClient implementation    //
//////////////////////////////////////

PerfTestClient::PerfTestClient(ServiceClientAPI& capi):capi(capi) {}

void PerfTestClient::add_or_update_server(const std::string& host, uint16_t port) {
    auto key = std::make_pair(host,port);
    if (connections.find(key) != connections.end()) {
        connections.erase(key);
    }
    connections.emplace(key,std::make_unique<::rpc::client>(host,port));
}

std::vector<std::pair<std::string,uint16_t>> PerfTestClient::get_connections() {
    std::vector<std::pair<std::string,uint16_t>> result;
    for (auto& kv:connections) {
        result.emplace_back(kv.first);
    }
    return result;
}

void PerfTestClient::remove_server(const std::string& host, uint16_t port) {
    auto key = std::make_pair(host,port);
    if (connections.find(key) != connections.end()) {
        connections.erase(key);
    }
}

PerfTestClient::~PerfTestClient() {}

}
}
