#include <cascade/config.h>
#ifdef ENABLE_EVALUATION
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

/////////////////////////////////////////////////////
// PerfTestClient/PerfTestServer implementation    //
/////////////////////////////////////////////////////

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

void PerfTestServer::make_workload(uint32_t payload_size, const std::string& key_prefix) {
    const uint32_t buf_size = payload_size - 128 - key_prefix.size();
    char *buf = (char*)malloc(buf_size);
    memset(buf,'A',buf_size);
    for (uint32_t i=0;i<NUMBER_OF_DISTINCT_OBJECTS;i++) {
        objects.emplace_back(key_prefix+std::to_string(i),buf,buf_size);
    }
    free(buf);
}

bool PerfTestServer::eval_put(std::vector<std::tuple<uint64_t,uint64_t,uint64_t>>& timestamp_log,
                              uint64_t max_operation_per_second,
                              uint64_t duration_secs,
                              uint32_t subgroup_type_index,
                              uint32_t subgroup_index,
                              uint32_t shard_index) {
        // synchronization data structures
        // 1 - version,send_timestamp_ns,reply_timestamp_ns
        // are now from timestamp_log
        // 2 - sending window and future queue
        uint32_t                window_size = derecho::getConfUInt32(CONF_DERECHO_P2P_WINDOW_SIZE);
        uint32_t                window_slots = window_size*2;
        std::mutex              window_slots_mutex;
        std::condition_variable window_slots_cv;
        std::queue<std::pair<uint64_t,derecho::QueryResults<std::tuple<persistent::version_t,uint64_t>>>> futures;
        std::mutex                                                                                        futures_mutex;
        std::condition_variable                                                                           futures_cv;
        std::condition_variable                                                                           window_cv;
        // 3 - all sent flag
        std::atomic<bool>                                   all_sent(false);
        // 4 - query thread
        std::thread                                         query_thread(
            [&timestamp_log,&window_slots,&window_slots_mutex,&window_slots_cv,&futures,&futures_mutex,&futures_cv,&all_sent](){
                std::unique_lock<std::mutex> futures_lck{futures_mutex};
                while(!all_sent || (futures.size()>0)) {
                    // pick pending futures
                    using namespace std::chrono_literals;
                    while(!futures_cv.wait_for(futures_lck,500ms,[&futures,&all_sent]{return (futures.size() > 0) || all_sent;}));
                    std::decay_t<decltype(futures)> pending_futures;
                    futures.swap(pending_futures);
                    futures_lck.unlock();
                    //+---------------------------------------+
                    //|             QUEUE UNLOCKED            |
                    // waiting for futures with lock released.
                    while (pending_futures.size() > 0) {
                        auto& replies = pending_futures.front().second.get();
                        for (auto& reply: replies) {
                            auto version = std::get<0>(reply.second.get());
                            uint64_t reply_timestamp_ns = get_walltime();
                            uint64_t send_timestamp_ns = pending_futures.front().first;
                            timestamp_log.emplace_back(version, send_timestamp_ns, reply_timestamp_ns);
                            break;
                        }
                        pending_futures.pop();
                        {
                            std::lock_guard<std::mutex> window_slots_lock{window_slots_mutex};
                            window_slots ++;
                        }
                        window_slots_cv.notify_one();
                    }
                    //|            QUEUE UNLOCKED             |
                    //+---------------------------------------+
                    // Acquire LOCK
                    futures_lck.lock();
                }
            }
        );

        //TODO: control read_write_ratio
        uint64_t interval_ns = (max_operation_per_second==0)?0:static_cast<uint64_t>(1e9/max_operation_per_second);
        uint64_t next_ns = get_walltime();
        uint64_t end_ns = next_ns + duration_secs*1000000000;
        while(true) {
            uint64_t now_ns = get_walltime();
            if (now_ns > end_ns) {
                all_sent.store(true);
                break;
            }
            // we leave 500 ns for loop overhead.
            if (now_ns + 500 < next_ns) {
                usleep((next_ns - now_ns - 500)/1000); // sleep in microseconds.
            }
            {
                std::unique_lock<std::mutex> window_slots_lock{window_slots_mutex};
                window_slots_cv.wait(window_slots_lock,[&window_slots]{return (window_slots > 0);});
                window_slots --;
            }
            next_ns += interval_ns;
            std::function<void(QueryResults<std::tuple<persistent::version_t,uint64_t>>&&)> future_appender = 
                [&futures,&futures_mutex,&futures_cv](QueryResults<std::tuple<persistent::version_t,uint64_t>>&& query_results){
                    std::unique_lock<std::mutex> lock{futures_mutex};
                    uint64_t timestamp_ns = get_walltime();
                    futures.emplace(timestamp_ns,std::move(query_results));
                    lock.unlock();
                    futures_cv.notify_one();
                };
            if (subgroup_index == INVALID_SUBGROUP_INDEX ||
                shard_index == INVALID_SHARD_INDEX) {
                on_subgroup_type_index_with_return_no_trigger(
                    std::decay_t<decltype(capi)>::subgroup_type_order.at(subgroup_type_index),
                    future_appender,
                    this->capi.template put, objects.at(now_ns%NUMBER_OF_DISTINCT_OBJECTS));
            } else {
                on_subgroup_type_index_with_return_no_trigger(
                    std::decay_t<decltype(capi)>::subgroup_type_order.at(subgroup_type_index),
                    future_appender,
                    this->capi.template put, objects.at(now_ns%NUMBER_OF_DISTINCT_OBJECTS), subgroup_index, shard_index);
            }
        }
        // wait for all pending futures.
        query_thread.join();
        return true;
}

bool PerfTestServer::eval_put_and_forget(put_and_forget_perf_log_t& timestamp_log,
                                         uint64_t max_operation_per_second,
                                         uint64_t duration_secs,
                                         uint32_t subgroup_type_index,
                                         uint32_t subgroup_index,
                                         uint32_t shard_index) {
        uint64_t interval_ns = (max_operation_per_second==0)?0:static_cast<uint64_t>(1e9/max_operation_per_second);
        uint64_t next_ns = get_walltime();
        uint64_t end_ns = next_ns + duration_secs*1000000000;
        timestamp_log.first_send_ns = next_ns;
        timestamp_log.num_messages = 0;
        // control read_write_ratio
        while(true) {
            uint64_t now_ns = get_walltime();
            if (now_ns > end_ns) {
                break;
            }
            // we leave 500 ns for loop overhead.
            if (now_ns + 500 < next_ns) {
                usleep((next_ns - now_ns - 500)/1000); // sleep in microseconds.
            }
            next_ns += interval_ns;
            if (subgroup_index == INVALID_SUBGROUP_INDEX || shard_index == INVALID_SHARD_INDEX) {
                on_subgroup_type_index_no_trigger(std::decay_t<decltype(capi)>::subgroup_type_order.at(subgroup_type_index),
                        this->capi.template put_and_forget, objects.at(now_ns%NUMBER_OF_DISTINCT_OBJECTS));
            } else {
                on_subgroup_type_index_no_trigger(std::decay_t<decltype(capi)>::subgroup_type_order.at(subgroup_type_index),
                        this->capi.template put_and_forget, objects.at(now_ns%NUMBER_OF_DISTINCT_OBJECTS), subgroup_index, shard_index);
            }
            timestamp_log.num_messages ++;
        }
        // send a normal put
        timestamp_log.last_send_ns = get_walltime();
        std::function<void(QueryResults<std::tuple<persistent::version_t,uint64_t>>&&)> future_appender = 
            [&timestamp_log](QueryResults<std::tuple<persistent::version_t,uint64_t>>&& query_results){
                auto& replies = query_results.get();
                for (auto& reply: replies) {
                    reply.second.get();
                }
                timestamp_log.ack_ns = get_walltime();
                timestamp_log.num_messages ++;
            };
        if (subgroup_index == INVALID_SUBGROUP_INDEX ||
            shard_index == INVALID_SHARD_INDEX) {
            on_subgroup_type_index_with_return_no_trigger(
                std::decay_t<decltype(capi)>::subgroup_type_order.at(subgroup_type_index),
                future_appender,
                this->capi.template put, objects.at(0));
        } else {
            on_subgroup_type_index_with_return_no_trigger(
                std::decay_t<decltype(capi)>::subgroup_type_order.at(subgroup_type_index),
                future_appender,
                this->capi.template put, objects.at(0), subgroup_index, shard_index);
        }
        return true;
}

PerfTestServer::PerfTestServer(ServiceClientAPI& capi, uint16_t port):
    capi(capi),
    server(port) {
    // Initialize objects

    // API 1 : run shard perf
    //
    // @param subgroup_type_index
    // @param subgroup_index
    // @param shard_index
    // @param policy
    // @param user_specified_node_id
    // @param read_write_ratio
    // @param max_operation_per_second
    // @param duration_Secs
    // @param output_filename
    //
    // @return true/false indicating if the RPC call is successful.
    server.bind("perf_put_to_shard",[this](
        uint32_t            subgroup_type_index,
        uint32_t            subgroup_index,
        uint32_t            shard_index,
        uint32_t            policy,
        uint32_t            user_specified_node_id,
        double              read_write_ratio,
        uint64_t            max_operation_per_second,
        uint64_t            duration_secs,
        const std::string&  output_filename) {
        // STEP 1 - set up the shard member selection policy
        on_subgroup_type_index_no_trigger(std::decay_t<decltype(capi)>::subgroup_type_order.at(subgroup_type_index),
            this->capi.template set_member_selection_policy,
            subgroup_index,
            shard_index,
            static_cast<ShardMemberSelectionPolicy>(policy),
            user_specified_node_id);
        // STEP 2 - prepare workload
        make_workload(derecho::getConfUInt32(CONF_DERECHO_MAX_P2P_REQUEST_PAYLOAD_SIZE),"raw_key_");
        // STEP 3 - start experiment and log
        std::vector<std::tuple<uint64_t,uint64_t,uint64_t>> timestamp_log;
        timestamp_log.reserve(65536);
        if (this->eval_put(timestamp_log,max_operation_per_second,duration_secs,subgroup_type_index,subgroup_index,shard_index)) {
            std::ofstream outfile(output_filename);
            outfile << "#version send_ts_us acked_ts_us" << std::endl;
            for (const auto& le:timestamp_log) {
                outfile << std::get<0>(le) << " " << (std::get<1>(le)/1000) << " " << (std::get<2>(le)/1000) << std::endl;
            }
            outfile.close();
            // flush server_side timestamp log to file
            std::function<void(derecho::rpc::QueryResults<void>&&)> future_handler = [](derecho::rpc::QueryResults<void>&& qr) {
                qr.get();
            };
            on_subgroup_type_index_with_return_no_trigger(
                std::decay_t<decltype(capi)>::subgroup_type_order.at(subgroup_type_index),
                future_handler,
                this->capi.template dump_timestamp, output_filename, subgroup_index, shard_index);
            return true;
        } else {
            return false;
        }
    });
    // API 1.5 : run shard perf with put_and_forget
    //
    // @param subgroup_type_index
    // @param subgroup_index
    // @param shard_index
    // @param policy
    // @param user_specified_node_id
    // @param read_write_ratio
    // @param max_operation_per_second
    // @param duration_Secs
    // @param output_filename
    //
    // @return true/false indicating if the RPC call is successful.
    server.bind("perf_put_and_forget_to_shard",[this](
        uint32_t            subgroup_type_index,
        uint32_t            subgroup_index,
        uint32_t            shard_index,
        uint32_t            policy,
        uint32_t            user_specified_node_id,
        double              read_write_ratio,
        uint64_t            max_operation_per_second,
        uint64_t            duration_secs,
        const std::string&  output_filename) {
        // STEP 1 - set up the shard member selection policy
        on_subgroup_type_index_no_trigger(std::decay_t<decltype(capi)>::subgroup_type_order.at(subgroup_type_index),
            this->capi.template set_member_selection_policy,
            subgroup_index,
            shard_index,
            static_cast<ShardMemberSelectionPolicy>(policy),
            user_specified_node_id);
        // STEP 2 - prepare workload
        make_workload(derecho::getConfUInt32(CONF_DERECHO_MAX_P2P_REQUEST_PAYLOAD_SIZE),"raw_key_");
        // STEP 3 - start experiment and log
        put_and_forget_perf_log_t timestamp_log;
        if (this->eval_put_and_forget(timestamp_log,max_operation_per_second,duration_secs,subgroup_type_index,subgroup_index,shard_index)) {
            std::ofstream outfile(output_filename);
            outfile << "#first_send_us last_send_us acked_ts_us num_messages" << std::endl;
            outfile << timestamp_log.first_send_ns/1000 << " "
                    << timestamp_log.last_send_ns/1000 << " "
                    << timestamp_log.ack_ns/1000 << " "
                    << timestamp_log.num_messages << std::endl;
            outfile.close();
            // flush server_side timestamp log to file
            std::function<void(derecho::rpc::QueryResults<void>&&)> future_handler = [](derecho::rpc::QueryResults<void>&& qr) {
                qr.get();
            };
            on_subgroup_type_index_with_return_no_trigger(
                std::decay_t<decltype(capi)>::subgroup_type_order.at(subgroup_type_index),
                future_handler,
                this->capi.template dump_timestamp, output_filename, subgroup_index, shard_index);
            return true;
        } else {
            return false;
        }
    });
    // API 2 : run object pool perf
    //
    // @param object_pool_pathname
    // @param policy
    // @param user_specified_node_ids
    // @param read_write_ratio
    // @param max_operation_per_second
    // @param duration_Secs
    // @param output_filename
    //
    // @return true/false indicating if the RPC call is successful.
    server.bind("perf_put_to_objectpool",[this](
        const std::string&  object_pool_pathname,
        uint32_t            policy,
        const std::vector<node_id_t>&  user_specified_node_ids, // one per shard
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
            throw derecho::derecho_exception(std::string("the size of 'user_specified_node_ids' argument does not match shard number."));
        }
        for (uint32_t shard_index = 0; shard_index < number_of_shards; shard_index ++) {
            on_subgroup_type_index_no_trigger(std::decay_t<decltype(capi)>::subgroup_type_order.at(object_pool.subgroup_type_index),
                this->capi.template set_member_selection_policy, object_pool.subgroup_index, shard_index, static_cast<ShardMemberSelectionPolicy>(policy), user_specified_node_ids.at(shard_index));
        }
        // STEP 2 - prepare workload
        make_workload(derecho::getConfUInt32(CONF_DERECHO_MAX_P2P_REQUEST_PAYLOAD_SIZE),object_pool_pathname+"/key_");
        // STEP 3 - start experiment and log
        std::vector<std::tuple<uint64_t,uint64_t,uint64_t>> timestamp_log;
        timestamp_log.reserve(65536);
        if (this->eval_put(timestamp_log,max_operation_per_second,duration_secs,object_pool.subgroup_type_index)) {
            std::ofstream outfile(output_filename);
            outfile << "#version send_ts_us acked_ts_us" << std::endl;
            for (const auto& le:timestamp_log) {
                outfile << std::get<0>(le) << " " << (std::get<1>(le)/1000) << " " << (std::get<2>(le)/1000) << std::endl;
            }
            outfile.close();
            // flush server_side timestamp log to file
            std::function<void(std::vector<std::unique_ptr<derecho::rpc::QueryResults<void>>>&&)> future_handler = [](std::vector<std::unique_ptr<derecho::rpc::QueryResults<void>>>&& qrs) {
                for (auto& qr:qrs){
                    qr.get();
                }
            };
            on_subgroup_type_index_with_return_no_trigger(
                std::decay_t<decltype(capi)>::subgroup_type_order.at(object_pool.subgroup_type_index),
                future_handler,
                this->capi.template dump_timestamp, output_filename, object_pool_pathname);
            return true;
        } else {
            return false;
        }
    });
    // API 2.5 : run shard perf with put_and_forget
    //
    // @param object_pool_pathname
    // @param policy
    // @param user_specified_node_id
    // @param read_write_ratio
    // @param max_operation_per_second
    // @param duration_Secs
    // @param output_filename
    //
    // @return true/false indicating if the RPC call is successful.
    server.bind("perf_put_and_forget_to_objectpool",[this](
        const std::string&  object_pool_pathname,
        uint32_t            policy,
        const std::vector<node_id_t>&  user_specified_node_ids, // one per shard
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
            throw derecho::derecho_exception(std::string("the size of 'user_specified_node_ids' argument does not match shard number."));
        }
        for (uint32_t shard_index = 0; shard_index < number_of_shards; shard_index ++) {
            on_subgroup_type_index_no_trigger(std::decay_t<decltype(capi)>::subgroup_type_order.at(object_pool.subgroup_type_index),
                this->capi.template set_member_selection_policy, object_pool.subgroup_index, shard_index, static_cast<ShardMemberSelectionPolicy>(policy), user_specified_node_ids.at(shard_index));
        }
        // STEP 2 - prepare workload
        make_workload(derecho::getConfUInt32(CONF_DERECHO_MAX_P2P_REQUEST_PAYLOAD_SIZE),"raw_key_");
        // STEP 3 - start experiment and log
        put_and_forget_perf_log_t timestamp_log;
        if (this->eval_put_and_forget(timestamp_log,max_operation_per_second,duration_secs,object_pool.subgroup_type_index)) {
            std::ofstream outfile(output_filename);
            outfile << "#first_send_us last_send_us acked_ts_us num_messages" << std::endl;
            outfile << timestamp_log.first_send_ns/1000 << " "
                    << timestamp_log.last_send_ns/1000 << " "
                    << timestamp_log.ack_ns/1000 << " "
                    << timestamp_log.num_messages << std::endl;
            outfile.close();
            // flush server_side timestamp log to file
            std::function<void(std::vector<std::unique_ptr<derecho::rpc::QueryResults<void>>>&&)> future_handler = [](std::vector<std::unique_ptr<derecho::rpc::QueryResults<void>>>&& qrs) {
                for (auto& qr:qrs){
                    qr.get();
                }
            };
            on_subgroup_type_index_with_return_no_trigger(
                std::decay_t<decltype(capi)>::subgroup_type_order.at(object_pool.subgroup_type_index),
                future_handler,
                this->capi.template dump_timestamp, output_filename, object_pool_pathname);
            return true;
        } else {
            return false;
        }
    });
    // API 3: read file
    server.bind("download",[](const std::string& filename){
        char buf[1024];
        std::stringstream ss;
        std::ifstream infile(filename);
        while(infile.getline(buf,1024)) {
            ss << buf << '\n';
        }
        infile.close();
        return ss.str();
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

bool PerfTestClient::check_rpc_futures(std::map<std::pair<std::string,uint16_t>,std::future<RPCLIB_MSGPACK::object_handle>>&& futures) {
    bool ret = true;
    for(auto& kv:futures) {
        try {
            bool result = kv.second.get().as<bool>();
            dbg_default_trace("perfserver {}:{} finished with {}.",kv.first.first,kv.first.second,result);
        } catch (::rpc::rpc_error& rpce) {
            dbg_default_warn("perfserver {}:{} throws an exception. function:{}, error:{}",
                             kv.first.first,
                             kv.first.second,
                             rpce.get_function_name(),
                             rpce.get_error().as<std::string>());
            ret = false;
        } catch (...) {
            dbg_default_warn("perfserver {}:{} throws unknown exception.",
                             kv.first.first, kv.first.second);
            ret = false;
        }
    }
    return ret;
}

bool PerfTestClient::download_file(const std::string& filename) {
    bool ret = true;
    for(auto& kv:connections) {
        try {
            auto output = kv.second->call("download",filename);
            std::ofstream outfile{filename+"-"+kv.first.first+":"+std::to_string(kv.first.second)};
            outfile << output.as<std::string>();
        } catch (::rpc::rpc_error& rpce) {
            dbg_default_warn("perfserver {}:{} throws an exception. function:{}, error:{}",
                             kv.first.first,
                             kv.first.second,
                             rpce.get_function_name(),
                             rpce.get_error().as<std::string>());
            ret = false;
        } catch (...) {
            dbg_default_warn("perfserver {}:{} throws unknown exception.",
                             kv.first.first, kv.first.second);
            ret = false;
        }
    }
    return ret;
}

}
}

#endif//ENABLE_EVALUATION
