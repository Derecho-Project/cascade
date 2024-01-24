#include <cascade/config.h>
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


#ifdef ENABLE_EVALUATION
#define TLT_READY_TO_SEND       (11000)
#define TLT_EC_SENT             (12000)
#define TLT_EC_GET_FINISHED     (12042)
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

bool PerfTestServer::eval_put(uint64_t max_operation_per_second,
                              uint64_t duration_secs,
                              uint32_t subgroup_type_index,
                              uint32_t subgroup_index,
                              uint32_t shard_index) {
        // synchronization data structures
        // 1 - sending window and future queue
        uint32_t                window_size = derecho::getConfUInt32(derecho::Conf::DERECHO_P2P_WINDOW_SIZE);
        uint32_t                window_slots = window_size*2;
        std::mutex              window_slots_mutex;
        std::condition_variable window_slots_cv;
        std::queue<std::pair<uint64_t,derecho::QueryResults<derecho::cascade::version_tuple>>> futures;
        std::mutex                                                                             futures_mutex;
        std::condition_variable                                                                futures_cv;
        std::condition_variable                                                                window_cv;
        // 3 - all sent flag
        std::atomic<bool>                                   all_sent(false);
        // 4 - query thread
        std::thread                                         query_thread(
            [&window_slots,&window_slots_mutex,&window_slots_cv,&futures,&futures_mutex,&futures_cv,&all_sent](){
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
                            std::get<0>(reply.second.get());
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
        uint64_t interval_ns = (max_operation_per_second==0)?0:static_cast<uint64_t>(INT64_1E9/max_operation_per_second);
        uint64_t next_ns = get_walltime();
        uint64_t end_ns = next_ns + duration_secs*1000000000ull;
        uint64_t message_id = this->capi.get_my_id()*1000000000ull;
        const uint32_t num_distinct_objects = objects.size();
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
            std::function<void(QueryResults<derecho::cascade::version_tuple>&&)> future_appender =
                [&futures,&futures_mutex,&futures_cv](QueryResults<derecho::cascade::version_tuple>&& query_results){
                    std::unique_lock<std::mutex> lock{futures_mutex};
                    uint64_t timestamp_ns = get_walltime();
                    futures.emplace(timestamp_ns,std::move(query_results));
                    lock.unlock();
                    futures_cv.notify_one();
                };
            // set message id.
            // constexpr does not work in non-template functions.
            if (std::is_base_of<IHasMessageID,std::decay_t<decltype(objects[0])>>::value) {
                dynamic_cast<IHasMessageID*>(&objects.at(now_ns%num_distinct_objects))->set_message_id(message_id);
            } else {
                throw derecho_exception{"Evaluation requests an object to support IHasMessageID interface."};
            }
            TimestampLogger::log(TLT_READY_TO_SEND,this->capi.get_my_id(),message_id,get_walltime());
            if (subgroup_index == INVALID_SUBGROUP_INDEX ||
                shard_index == INVALID_SHARD_INDEX) {
                future_appender(this->capi.put(objects.at(now_ns%num_distinct_objects)));
            } else {
                on_subgroup_type_index_with_return(
                    std::decay_t<decltype(capi)>::subgroup_type_order.at(subgroup_type_index),
                    future_appender,
                    this->capi.template put, objects.at(now_ns%num_distinct_objects), subgroup_index, shard_index);
            }
            TimestampLogger::log(TLT_EC_SENT,this->capi.get_my_id(),message_id,get_walltime());
            message_id ++;
        }
        // wait for all pending futures.
        query_thread.join();
        return true;
}

bool PerfTestServer::eval_put_and_forget(uint64_t max_operation_per_second,
                                         uint64_t duration_secs,
                                         uint32_t subgroup_type_index,
                                         uint32_t subgroup_index,
                                         uint32_t shard_index) {
    uint64_t interval_ns = (max_operation_per_second==0)?0:static_cast<uint64_t>(INT64_1E9/max_operation_per_second);
    uint64_t next_ns = get_walltime();
    uint64_t end_ns = next_ns + duration_secs*1000000000ull;
    uint64_t message_id = this->capi.get_my_id()*1000000000ull;
    const uint32_t num_distinct_objects = objects.size();
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
        // set message id.
        // constexpr does not work in non-template function, obviously
        if (std::is_base_of<IHasMessageID, std::decay_t<decltype(objects[0])>>::value) {
            dynamic_cast<IHasMessageID*>(&objects.at(now_ns%num_distinct_objects))->set_message_id(message_id);
        } else {
            throw derecho_exception{"Evaluation requests an object to support IHasMessageID interface."};
        }
        // log time.
        TimestampLogger::log(TLT_READY_TO_SEND,this->capi.get_my_id(),message_id,get_walltime());
        // send it
        if (subgroup_index == INVALID_SUBGROUP_INDEX || shard_index == INVALID_SHARD_INDEX) {
            this->capi.put_and_forget(objects.at(now_ns%num_distinct_objects));
        } else {
            on_subgroup_type_index(std::decay_t<decltype(capi)>::subgroup_type_order.at(subgroup_type_index),
                    this->capi.template put_and_forget, objects.at(now_ns%num_distinct_objects), subgroup_index, shard_index);
        }
        // log time.
        TimestampLogger::log(TLT_EC_SENT,this->capi.get_my_id(),message_id,get_walltime());
        message_id ++;
    }
    return true;
}

bool PerfTestServer::eval_trigger_put(uint64_t max_operation_per_second,
                                      uint64_t duration_secs,
                                      uint32_t subgroup_type_index,
                                      uint32_t subgroup_index,
                                      uint32_t shard_index) {
    uint64_t interval_ns = (max_operation_per_second==0)?0:static_cast<uint64_t>(INT64_1E9/max_operation_per_second);
    uint64_t next_ns = get_walltime();
    uint64_t end_ns = next_ns + duration_secs*1000000000ull;
    uint64_t message_id = this->capi.get_my_id()*1000000000ull;
    const uint32_t num_distinct_objects = objects.size();
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
        // set message id.
        // constexpr does not work here.
        if (std::is_base_of<IHasMessageID,std::decay_t<decltype(objects[0])>>::value) {
            dynamic_cast<IHasMessageID*>(&objects.at(now_ns%num_distinct_objects))->set_message_id(message_id);
        } else {
            throw derecho_exception{"Evaluation requests an object to support IHasMessageID interface."};
        }
        // log time.
        TimestampLogger::log(TLT_READY_TO_SEND,this->capi.get_my_id(),message_id,get_walltime());
        if (subgroup_index == INVALID_SUBGROUP_INDEX || shard_index == INVALID_SHARD_INDEX) {
            this->capi.trigger_put(objects.at(now_ns%num_distinct_objects));
        } else {
            on_subgroup_type_index(std::decay_t<decltype(capi)>::subgroup_type_order.at(subgroup_type_index),
                    this->capi.template trigger_put, objects.at(now_ns%num_distinct_objects), subgroup_index, shard_index);
        }
        TimestampLogger::log(TLT_EC_SENT,this->capi.get_my_id(),message_id,get_walltime());
        message_id ++;
    }

    return true;
}

bool PerfTestServer::eval_get(int32_t log_depth,
                              uint64_t max_operations_per_second,
                              uint64_t duration_secs,
                              uint32_t subgroup_type_index,
                              uint32_t subgroup_index,
                              uint32_t shard_index) {
    debug_enter_func_with_args("log_depth={},max_ops={},duration={},subgroup_type_index={},subgroup_index={},shard_index={}",
                               log_depth, max_operations_per_second, duration_secs, subgroup_type_index, subgroup_index, shard_index);
    // In case the test objects ever change type, use an alias for whatever type is in the objects vector
    using ObjectType = std::decay_t<decltype(objects[0])>;
    // Sending window variables
    uint32_t window_size = derecho::getConfUInt32(derecho::Conf::DERECHO_P2P_WINDOW_SIZE);
    uint32_t window_slots = window_size * 2;
    std::mutex window_slots_mutex;
    std::condition_variable window_slots_cv;
    // Result future queue, which pairs message IDs with a future for that message
    std::queue<std::pair<uint64_t, derecho::QueryResults<const ObjectType>>> futures;
    std::mutex futures_mutex;
    std::condition_variable futures_cv;
    std::condition_variable window_cv;
    // All sent flag
    std::atomic<bool> all_sent(false);
    // Node ID, used for logger calls
    const node_id_t my_node_id = this->capi.get_my_id();
    // Future consuming thread
    std::thread query_thread(
            [&]() {
                std::unique_lock<std::mutex> futures_lck{futures_mutex};
                while(!all_sent || (futures.size() > 0)) {
                    // Wait for the futures queue to be non-empty, then swap it with pending_futures
                    using namespace std::chrono_literals;
                    while(!futures_cv.wait_for(futures_lck, 500ms, [&futures, &all_sent] { return (futures.size() > 0) || all_sent; }))
                        ;
                    std::decay_t<decltype(futures)> pending_futures;
                    futures.swap(pending_futures);
                    futures_lck.unlock();
                    //+---------------------------------------+
                    //|             QUEUE UNLOCKED            |
                    // wait for each future in pending_futures, leaving futures unlocked
                    while(pending_futures.size() > 0) {
                        auto& replies = pending_futures.front().second.get();
                        uint64_t message_id = pending_futures.front().first;
                        // Get only the first reply
                        for(auto& reply : replies) {
                            reply.second.get();
                            // This might not be an accurate time for when the query completed, depending on how long the thread waited to acquire the queue lock
                            TimestampLogger::log(TLT_EC_GET_FINISHED, my_node_id, message_id, get_walltime());
                            break;
                        }
                        pending_futures.pop();
                        {
                            std::lock_guard<std::mutex> window_slots_lock{window_slots_mutex};
                            window_slots++;
                        }
                        window_slots_cv.notify_one();
                    }
                    //|            QUEUE UNLOCKED             |
                    //+---------------------------------------+
                    // Acquire lock on futures queue
                    futures_lck.lock();
                }
            });
    // Put test objects in the target subgroup/object pool, and record which version is log_depth back in the log
    // NOTE: This only works if there is a single client! If there are multiple clients there will be num_clients * log_depth versions
    std::vector<persistent::version_t> oldest_object_versions;
    for(const auto& object : objects) {
        using namespace derecho;
        // Call put with either the object pool interface or the shard interface, and store the future here
        // Use a unique_ptr to work around the fact that QueryResults has a move constructor but no default constructor
        std::unique_ptr<QueryResults<cascade::version_tuple>> put_result_future;
        if(subgroup_index == INVALID_SUBGROUP_INDEX || shard_index == INVALID_SHARD_INDEX) {
            // put returns the QueryResults by value, so we have to move-construct it into a unique_ptr
            put_result_future = std::make_unique<QueryResults<cascade::version_tuple>>(std::move(this->capi.put(object)));
        } else {
            // Manual copy of on_subgroup_type_index macro so I can use make_unique
            std::type_index tindex = std::decay_t<decltype(capi)>::subgroup_type_order.at(subgroup_type_index);
            if(std::type_index(typeid(VolatileCascadeStoreWithStringKey)) == tindex) {
                put_result_future = std::make_unique<QueryResults<cascade::version_tuple>>(
                    std::move(this->capi.template put<VolatileCascadeStoreWithStringKey>(object, subgroup_index, shard_index)));
            } else if(std::type_index(typeid(PersistentCascadeStoreWithStringKey)) == tindex) {
                put_result_future = std::make_unique<QueryResults<cascade::version_tuple>>(
                    std::move(this->capi.template put<PersistentCascadeStoreWithStringKey>(object, subgroup_index, shard_index)));
            } else if(std::type_index(typeid(TriggerCascadeNoStoreWithStringKey)) == tindex) {
                put_result_future = std::make_unique<QueryResults<cascade::version_tuple>>(
                    std::move(this->capi.template put<TriggerCascadeNoStoreWithStringKey>(object, subgroup_index, shard_index)));
            } else {
                throw derecho::derecho_exception(std::string("Unknown type_index:") + tindex.name());
            }
        }
        // Save the version assigned to this version of the object, which will become the oldest version in the log
        auto& replies = put_result_future->get();
        derecho::cascade::version_tuple object_version_tuple = replies.begin()->second.get();
        oldest_object_versions.emplace_back(std::get<0>(object_version_tuple));
        dbg_default_debug("eval_get: Object {} got version {}, putting {} more versions in front of it", object.get_key_ref(), std::get<0>(object_version_tuple), log_depth);
        // Call put_and_forget repeatedly on the same object to give it multiple newer versions in the log, up to the depth needed
        for(int32_t i = 0; i < log_depth; ++i) {
            if(subgroup_index == INVALID_SUBGROUP_INDEX || shard_index == INVALID_SHARD_INDEX) {
                this->capi.put_and_forget(object);
            } else {
                on_subgroup_type_index(std::decay_t<decltype(capi)>::subgroup_type_order.at(subgroup_type_index),
                                       this->capi.template put_and_forget, object, subgroup_index, shard_index);
            }
        }
    }

    dbg_default_info("eval_get: Puts complete, ready to start experiment");

    // Timing control variables
    uint64_t interval_ns = (max_operations_per_second == 0) ? 0 : static_cast<uint64_t>(INT64_1E9 / max_operations_per_second);
    uint64_t next_ns = get_walltime();
    uint64_t end_ns = next_ns + duration_secs * INT64_1E9;
    uint64_t message_id = this->capi.get_my_id() * 1000000000ull;
    const uint32_t num_distinct_objects = objects.size();
    while(true) {
        uint64_t now_ns = get_walltime();
        if(now_ns > end_ns) {
            all_sent.store(true);
            break;
        }
        // we leave 500 ns for loop overhead.
        if(now_ns + 500 < next_ns) {
            usleep((next_ns - now_ns - 500) / 1000);  // sleep in microseconds.
        }
        {
            std::unique_lock<std::mutex> window_slots_lock{window_slots_mutex};
            window_slots_cv.wait(window_slots_lock, [&window_slots] { return (window_slots > 0); });
            window_slots--;
        }
        next_ns += interval_ns;
        // Since each loop iteration creates its own future_appender, capture the message_id by copy
        std::function<void(QueryResults<const ObjectType>&&)> future_appender =
                [&futures, &futures_mutex, &futures_cv, message_id](
                        QueryResults<const ObjectType>&& query_results) {
                    std::unique_lock<std::mutex> lock{futures_mutex};
                    futures.emplace(message_id, std::move(query_results));
                    lock.unlock();
                    futures_cv.notify_one();
                };
        std::size_t cur_object_index = now_ns % num_distinct_objects;
        // NOTE: Setting the message ID on the object won't do anything because we're doing a Get, not a Put
        TimestampLogger::log(TLT_READY_TO_SEND, my_node_id, message_id, get_walltime());
        // With either the object pool interface or the shard interface, further decide whether to request the current version or an old version
        if(subgroup_index == INVALID_SUBGROUP_INDEX || shard_index == INVALID_SHARD_INDEX) {
            if(log_depth == -1) {
                future_appender(this->capi.multi_get(objects.at(cur_object_index).get_key_ref()));
            } else if(log_depth == 0) {
                future_appender(this->capi.get(objects.at(cur_object_index).get_key_ref(), CURRENT_VERSION));
            } else {
                future_appender(this->capi.get(objects.at(cur_object_index).get_key_ref(), oldest_object_versions.at(cur_object_index)));
            }
        } else {
            if(log_depth == -1) {
                on_subgroup_type_index_with_return(
                        std::decay_t<decltype(capi)>::subgroup_type_order.at(subgroup_type_index),
                        future_appender,
                        this->capi.template multi_get, objects.at(cur_object_index).get_key_ref(), subgroup_index, shard_index);
            } else if(log_depth == 0) {
                on_subgroup_type_index_with_return(
                        std::decay_t<decltype(capi)>::subgroup_type_order.at(subgroup_type_index),
                        future_appender,
                        this->capi.template get, objects.at(cur_object_index).get_key_ref(), CURRENT_VERSION, true, subgroup_index, shard_index);
            } else {
                on_subgroup_type_index_with_return(
                        std::decay_t<decltype(capi)>::subgroup_type_order.at(subgroup_type_index),
                        future_appender,
                        this->capi.template get, objects.at(cur_object_index).get_key_ref(), oldest_object_versions.at(cur_object_index), true, subgroup_index, shard_index);
            }
        }
        TimestampLogger::log(TLT_EC_SENT, my_node_id, message_id, get_walltime());
        message_id++;
    }
    dbg_default_info("eval_get: All messages sent, waiting for queries to complete");
    // wait for all pending futures.
    query_thread.join();
    return true;
}

bool PerfTestServer::eval_get_by_time(uint64_t ms_in_past,
                                      uint64_t max_operations_per_second,
                                      uint64_t duration_secs,
                                      uint32_t subgroup_type_index,
                                      uint32_t subgroup_index,
                                      uint32_t shard_index) {
    debug_enter_func_with_args("ms_in_past={},max_ops={},duration={},subgroup_type_index={},subgroup_index={},shard_index={}",
                               ms_in_past, max_operations_per_second, duration_secs, subgroup_type_index, subgroup_index, shard_index);
    // In case the test objects ever change type, use an alias for whatever type is in the objects vector
    using ObjectType = std::decay_t<decltype(objects[0])>;
    // Sending window variables
    uint32_t window_size = derecho::getConfUInt32(derecho::Conf::DERECHO_P2P_WINDOW_SIZE);
    uint32_t window_slots = window_size * 2;
    std::mutex window_slots_mutex;
    std::condition_variable window_slots_cv;
    // Result future queue, which pairs message IDs with a future for that message
    std::queue<std::pair<uint64_t, derecho::QueryResults<const ObjectType>>> futures;
    std::mutex futures_mutex;
    std::condition_variable futures_cv;
    std::condition_variable window_cv;
    // All sent flag
    std::atomic<bool> all_sent(false);
    // Node ID, used for logger calls
    const node_id_t my_node_id = this->capi.get_my_id();
    // Future consuming thread
    std::thread query_thread(
            [&]() {
                std::unique_lock<std::mutex> futures_lck{futures_mutex};
                while(!all_sent || (futures.size() > 0)) {
                    // Wait for the futures queue to be non-empty, then swap it with pending_futures
                    using namespace std::chrono_literals;
                    while(!futures_cv.wait_for(futures_lck, 500ms, [&futures, &all_sent] { return (futures.size() > 0) || all_sent; }))
                        ;
                    std::decay_t<decltype(futures)> pending_futures;
                    futures.swap(pending_futures);
                    futures_lck.unlock();
                    //+---------------------------------------+
                    //|             QUEUE UNLOCKED            |
                    // wait for each future in pending_futures, leaving futures unlocked
                    while(pending_futures.size() > 0) {
                        auto& replies = pending_futures.front().second.get();
                        uint64_t message_id = pending_futures.front().first;
                        // Get only the first reply
                        for(auto& reply : replies) {
                            reply.second.get();
                            TimestampLogger::log(TLT_EC_GET_FINISHED, my_node_id, message_id, get_walltime());
                            break;
                        }
                        pending_futures.pop();
                        {
                            std::lock_guard<std::mutex> window_slots_lock{window_slots_mutex};
                            window_slots++;
                        }
                        window_slots_cv.notify_one();
                    }
                    //|            QUEUE UNLOCKED             |
                    //+---------------------------------------+
                    // Acquire lock on futures queue
                    futures_lck.lock();
                }
            });

    const uint32_t num_distinct_objects = objects.size();
    // Put all the objects in the target subgroup once, so that the oldest timestamp isn't always just version 0
    for(const auto& object : objects) {
        if(subgroup_index == INVALID_SUBGROUP_INDEX || shard_index == INVALID_SHARD_INDEX) {
            this->capi.put_and_forget(object);
        } else {
            on_subgroup_type_index(std::decay_t<decltype(capi)>::subgroup_type_order.at(subgroup_type_index),
                                   this->capi.template put_and_forget, object, subgroup_index, shard_index);
        }
    }
    // Put test objects in the target subgroup/object pool for ms_in_past milliseconds, and record the oldest timestamp
    // For now use a fixed rate of 1 put() every 10ms (i.e. 100 op/s), but we might want to make this configurable in the future
    std::queue<std::pair<std::size_t, derecho::QueryResults<derecho::cascade::version_tuple>>> put_futures_queue;
    const uint64_t put_interval_ns = get_by_time_put_interval * INT64_1E6;
    // Offset to add to each timestamp received in reply from put() before using as the timestamp to request. This accounts for slight differences in timestamps assigned at each replica
    const uint64_t timestamp_offset_us = 100;
    uint64_t next_put_ns = get_walltime();
    uint64_t put_end_ns = get_walltime() + ms_in_past * INT64_1E9;
    if(put_end_ns < next_put_ns + put_interval_ns * num_distinct_objects) {
        dbg_default_warn("eval_get_by_time: Requested ms_in_past ({}) is shorter than minimum time needed to put all objects once ({}). Increasing it to the minimum.", ms_in_past, (put_interval_ns * num_distinct_objects) / INT64_1E6);
        put_end_ns = get_walltime() + (put_interval_ns * num_distinct_objects);
    }
    std::size_t current_object = 0;
    while(true) {
        uint64_t now_ns = get_walltime();
        if(now_ns > put_end_ns) {
            break;
        }
        if(now_ns + 500 < next_put_ns) {
            usleep((next_put_ns - now_ns - 500) / 1000);
        }
        next_put_ns += put_interval_ns;
        // Note: No need to wait on a send window slot because this loop runs at a fixed rate (not user-supplied)
        // that is always slow enough to not exhaust the P2P send window size

        // Put the QueryResults on a queue to collect later, but no need to do it in a parallel thread
        std::function<void(QueryResults<derecho::cascade::version_tuple>&&)> future_appender =
                [&put_futures_queue, current_object](QueryResults<derecho::cascade::version_tuple>&& query_results) {
                    put_futures_queue.emplace(current_object, std::move(query_results));
                };
        if(subgroup_index == INVALID_SUBGROUP_INDEX || shard_index == INVALID_SHARD_INDEX) {
            future_appender(this->capi.put(objects.at(current_object)));
        } else {
            on_subgroup_type_index_with_return(
                    std::decay_t<decltype(capi)>::subgroup_type_order.at(subgroup_type_index),
                    future_appender,
                    this->capi.template put, objects.at(current_object), subgroup_index, shard_index);
        }
        current_object = (current_object + 1) % num_distinct_objects;
    }
    dbg_default_info("eval_get_by_time: Finished all puts, collecting QueryResults");
    // The first object to complete a put() is the one we will request on every get() since it is the right number of milliseconds in the past
    uint64_t timestamp_to_request;
    std::size_t object_to_request;
    auto& first_put_replies = put_futures_queue.front().second.get();
    object_to_request = put_futures_queue.front().first;
    derecho::cascade::version_tuple first_object_version_tuple = first_put_replies.begin()->second.get();
    dbg_default_debug("Object {} ms in the past is key {} with timestamp {}", ms_in_past, objects.at(object_to_request).get_key_ref(), std::get<1>(first_object_version_tuple));
    timestamp_to_request = std::get<1>(first_object_version_tuple) + timestamp_offset_us;
    put_futures_queue.pop();
    // Wait for the other puts to complete
    persistent::version_t last_object_version;
    std::size_t last_object_put;
    while(put_futures_queue.size() > 0) {
        auto& replies = put_futures_queue.front().second.get();
        std::size_t object_index = put_futures_queue.front().first;
        derecho::cascade::version_tuple object_version_tuple = replies.begin()->second.get();
        dbg_default_debug("Put complete for {}, assigned timestamp was {}", objects.at(object_index).get_key_ref(), std::get<1>(object_version_tuple));
        if(put_futures_queue.size() == 1) {
            last_object_put = object_index;
            last_object_version = std::get<0>(object_version_tuple);
        }
        put_futures_queue.pop();
    }
    dbg_default_info("eval_get_by_time: Puts complete, performing a stable get for version {} to wait for persistence", last_object_version);

    // Do a stable get() of the version associated with the last object, and wait for the reply, to ensure all the puts have finished persisting
    if(subgroup_index == INVALID_SUBGROUP_INDEX || shard_index == INVALID_SHARD_INDEX) {
        auto query_results = this->capi.get(objects.at(last_object_put).get_key_ref(), last_object_version, true);
        query_results.get().begin()->second.get();
    } else {
        // No real need to do on_subgroup_type_index because get_by_time only works with a single subgroup type anyway
        auto query_results = this->capi.template get<PersistentCascadeStoreWithStringKey>(
                objects.at(last_object_put).get_key_ref(), last_object_version, true, subgroup_index, shard_index);
        query_results.get().begin()->second.get();
    }

    dbg_default_info("eval_get_by_time: Target version is stable, ready to start experiment");

    // Timing control variables for the get loop
    uint64_t interval_ns = (max_operations_per_second == 0) ? 0 : static_cast<uint64_t>(INT64_1E9 / max_operations_per_second);
    uint64_t next_ns = get_walltime();
    uint64_t end_ns = next_ns + duration_secs * 1000000000ull;
    uint64_t message_id = this->capi.get_my_id() * 1000000000ull;

    while(true) {
        uint64_t now_ns = get_walltime();
        if(now_ns > end_ns) {
            all_sent.store(true);
            break;
        }
        // we leave 500 ns for loop overhead.
        if(now_ns + 500 < next_ns) {
            usleep((next_ns - now_ns - 500) / 1000);
        }
        {
            std::unique_lock<std::mutex> window_slots_lock{window_slots_mutex};
            window_slots_cv.wait(window_slots_lock, [&window_slots] { return (window_slots > 0); });
            window_slots--;
        }
        next_ns += interval_ns;
        // Since each loop iteration creates its own future_appender, capture the message_id by copy
        std::function<void(QueryResults<const ObjectType>&&)> future_appender =
                [&futures, &futures_mutex, &futures_cv, message_id](
                        QueryResults<const ObjectType>&& query_results) {
                    std::unique_lock<std::mutex> lock{futures_mutex};
                    futures.emplace(message_id, std::move(query_results));
                    lock.unlock();
                    futures_cv.notify_one();
                };
        // NOTE: Setting the message ID on the object won't do anything because we're doing a Get, not a Put
        TimestampLogger::log(TLT_READY_TO_SEND, my_node_id, message_id, get_walltime());
        if(subgroup_index == INVALID_SUBGROUP_INDEX || shard_index == INVALID_SHARD_INDEX) {
            future_appender(this->capi.get_by_time(objects.at(object_to_request).get_key_ref(), timestamp_to_request));
        } else {
            on_subgroup_type_index_with_return(
                    std::decay_t<decltype(capi)>::subgroup_type_order.at(subgroup_type_index),
                    future_appender,
                    this->capi.template get_by_time, objects.at(object_to_request).get_key_ref(), timestamp_to_request, true, subgroup_index, shard_index);
        }
        TimestampLogger::log(TLT_EC_SENT, my_node_id, message_id, get_walltime());
        message_id++;
    }
    dbg_default_info("eval_get: All messages sent, waiting for queries to complete");
    // wait for all pending futures.
    query_thread.join();
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
        int64_t             start_sec,
        uint64_t            duration_secs,
        const std::string&  output_filename) {
        // STEP 1 - set up the shard member selection policy
        on_subgroup_type_index(std::decay_t<decltype(capi)>::subgroup_type_order.at(subgroup_type_index),
            this->capi.template set_member_selection_policy,
            subgroup_index,
            shard_index,
            static_cast<ShardMemberSelectionPolicy>(policy),
            user_specified_node_id);
        // STEP 2 - prepare workload
        objects.clear();
        uint32_t object_size = derecho::getConfUInt32(derecho::Conf::DERECHO_MAX_P2P_REQUEST_PAYLOAD_SIZE);
        uint32_t num_distinct_objects = std::min(static_cast<uint64_t>(max_num_distinct_objects), max_workload_memory / object_size);
        make_workload<std::string, ObjectWithStringKey>(object_size, num_distinct_objects, "raw_key_", objects);
        // STEP 3 - start experiment and log
        int64_t sleep_us = (start_sec*INT64_1E9 - static_cast<int64_t>(get_walltime()))/INT64_1E3;
        if (sleep_us > 1) {
            usleep(sleep_us);
        }
        if (this->eval_put(max_operation_per_second,duration_secs,subgroup_type_index,subgroup_index,shard_index)) {
            TimestampLogger::flush(output_filename);
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
        int64_t             start_sec,
        uint64_t            duration_secs,
        const std::string&  output_filename) {
        // STEP 1 - set up the shard member selection policy
        on_subgroup_type_index(std::decay_t<decltype(capi)>::subgroup_type_order.at(subgroup_type_index),
            this->capi.template set_member_selection_policy,
            subgroup_index,
            shard_index,
            static_cast<ShardMemberSelectionPolicy>(policy),
            user_specified_node_id);
        // STEP 2 - prepare workload
        objects.clear();
        uint32_t object_size = derecho::getConfUInt32(derecho::Conf::DERECHO_MAX_P2P_REQUEST_PAYLOAD_SIZE);
        uint32_t num_distinct_objects = std::min(static_cast<uint64_t>(max_num_distinct_objects), max_workload_memory / object_size);
        make_workload<std::string, ObjectWithStringKey>(object_size, num_distinct_objects, "raw_key_", objects);
        // STEP 3 - start experiment and log
        int64_t sleep_us = (start_sec*INT64_1E9 - static_cast<int64_t>(get_walltime()))/INT64_1E3;
        if (sleep_us > 1) {
            usleep(sleep_us);
        }
        if (this->eval_put_and_forget(max_operation_per_second,duration_secs,subgroup_type_index,subgroup_index,shard_index)) {
            TimestampLogger::flush(output_filename);
            return true;
        } else {
            return false;
        }
    });
    // API 1.6 : run shard perf with trigger_put
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
    server.bind("perf_trigger_put_to_shard",[this](
        uint32_t            subgroup_type_index,
        uint32_t            subgroup_index,
        uint32_t            shard_index,
        uint32_t            policy,
        uint32_t            user_specified_node_id,
        double              read_write_ratio,
        uint64_t            max_operation_per_second,
        int64_t             start_sec,
        uint64_t            duration_secs,
        const std::string&  output_filename) {
        // STEP 1 - set up the shard member selection policy
        on_subgroup_type_index(std::decay_t<decltype(capi)>::subgroup_type_order.at(subgroup_type_index),
            this->capi.template set_member_selection_policy,
            subgroup_index,
            shard_index,
            static_cast<ShardMemberSelectionPolicy>(policy),
            user_specified_node_id);
        // STEP 2 - prepare workload
        objects.clear();
        uint32_t object_size = derecho::getConfUInt32(derecho::Conf::DERECHO_MAX_P2P_REQUEST_PAYLOAD_SIZE);
        uint32_t num_distinct_objects = std::min(static_cast<uint64_t>(max_num_distinct_objects), max_workload_memory / object_size);
        make_workload<std::string, ObjectWithStringKey>(object_size, num_distinct_objects, "raw_key_", objects);
        int64_t sleep_us = (start_sec * INT64_1E9 - static_cast<int64_t>(get_walltime())) / INT64_1E3;
        if (sleep_us > 1) {
            usleep(sleep_us);
        }
        // STEP 3 - start experiment and log
        if (this->eval_trigger_put(max_operation_per_second,duration_secs,subgroup_type_index,subgroup_index,shard_index)) {
            TimestampLogger::flush(output_filename);
            return true;
        } else {
            return false;
        }
    });

    /**
     * RPC function that runs perf_get on a specific shard
     *
     * @param subgroup_type_index
     * @param subgroup_index
     * @param shard_index
     * @param member_selection_policy
     * @param user_specified_node_id
     * @param log_depth
     * @param max_operations_per_second
     * @param start_sec
     * @param duration_secs
     * @param output_filename
     * @return true if experiment completed successfully, false if there was an error
     */
    server.bind("perf_get_to_shard", [this](uint32_t subgroup_type_index,
                                            uint32_t subgroup_index,
                                            uint32_t shard_index,
                                            uint32_t member_selection_policy,
                                            uint32_t user_specified_node_id,
                                            uint32_t log_depth,
                                            uint64_t max_operations_per_second,
                                            int64_t start_sec,
                                            uint64_t duration_secs,
                                            const std::string& output_filename) {
        // Set up the shard member selection policy
        on_subgroup_type_index(std::decay_t<decltype(capi)>::subgroup_type_order.at(subgroup_type_index),
                               this->capi.template set_member_selection_policy,
                               subgroup_index,
                               shard_index,
                               static_cast<ShardMemberSelectionPolicy>(member_selection_policy),
                               user_specified_node_id);
        // Create workload objects
        objects.clear();
        uint32_t object_size = derecho::getConfUInt32(derecho::Conf::DERECHO_MAX_P2P_REQUEST_PAYLOAD_SIZE);
        uint32_t num_distinct_objects = std::min(static_cast<uint64_t>(max_num_distinct_objects), max_workload_memory / object_size);
        // Ensure adding log_depth versions to all the workload objects won't run out of log space, in case log_depth is large
        num_distinct_objects = std::min(num_distinct_objects, derecho::getConfUInt32(derecho::Conf::PERS_MAX_LOG_ENTRY) / (log_depth + 1));
        num_distinct_objects = std::min(static_cast<uint64_t>(num_distinct_objects),
                                        derecho::getConfUInt64(derecho::Conf::PERS_MAX_DATA_SIZE) / (object_size * (log_depth + 1)));
        make_workload<std::string, ObjectWithStringKey>(object_size, num_distinct_objects, "raw_key_", objects);
        // Wait for start time
        int64_t sleep_us = (start_sec * INT64_1E9 - static_cast<int64_t>(get_walltime())) / INT64_1E3;
        if(sleep_us > 1) {
            usleep(sleep_us);
        }
        // Run experiment, then log timestamps
        try {
            if(this->eval_get(log_depth, max_operations_per_second, duration_secs, subgroup_type_index, subgroup_index, shard_index)) {
                TimestampLogger::flush(output_filename);
                return true;
            } else {
                return false;
            }
        } catch(const std::exception& e) {
            std::cerr << "eval_get failed with exception: " << typeid(e).name() << ": " << e.what() << std::endl;
            return false;
        }
    });

    /**
     * RPC function that runs perf_get_by_time on a specific shard
     * @return true if the experiment completed successfully, false if there was an error
     */
    server.bind("perf_get_by_time_to_shard", [this](uint32_t subgroup_type_index,
                                                    uint32_t subgroup_index,
                                                    uint32_t shard_index,
                                                    uint32_t member_selection_policy,
                                                    uint32_t user_specified_node_id,
                                                    uint64_t ms_in_past,
                                                    uint64_t max_operations_per_second,
                                                    int64_t start_sec,
                                                    uint64_t duration_secs,
                                                    const std::string& output_filename) {
        // Set up the shard member selection policy
        on_subgroup_type_index(std::decay_t<decltype(capi)>::subgroup_type_order.at(subgroup_type_index),
                               this->capi.template set_member_selection_policy,
                               subgroup_index,
                               shard_index,
                               static_cast<ShardMemberSelectionPolicy>(member_selection_policy),
                               user_specified_node_id);
        // Create workload objects
        objects.clear();
        uint32_t object_size = derecho::getConfUInt32(derecho::Conf::DERECHO_MAX_P2P_REQUEST_PAYLOAD_SIZE);
        uint32_t num_distinct_objects = std::min(static_cast<uint64_t>(max_num_distinct_objects), max_workload_memory / object_size);
        // Ensure it's possible to do a put() to each object once in ms_in_past milliseconds at a rate of 10 ms per put
        num_distinct_objects = std::min(static_cast<uint64_t>(num_distinct_objects), ms_in_past / get_by_time_put_interval);
        make_workload<std::string, ObjectWithStringKey>(object_size, num_distinct_objects, "raw_key_", objects);
        // Wait for start time
        int64_t sleep_us = (start_sec * INT64_1E9 - static_cast<int64_t>(get_walltime())) / INT64_1E3;
        if(sleep_us > 1) {
            usleep(sleep_us);
        }
        // Run experiment, then log timestamps
        try {
            if(this->eval_get_by_time(ms_in_past, max_operations_per_second, duration_secs, subgroup_type_index, subgroup_index, shard_index)) {
                TimestampLogger::flush(output_filename);
                return true;
            } else {
                return false;
            }
        } catch(const std::exception& e) {
            std::cerr << "eval_get failed with exception: " << typeid(e).name() << ": " << e.what() << std::endl;
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
        int64_t             start_sec,
        uint64_t            duration_secs,
        const std::string&  output_filename) {

        auto object_pool = this->capi.find_object_pool(object_pool_pathname);

        uint32_t number_of_shards;
        // STEP 1 - set up the shard member selection policy
        on_subgroup_type_index(std::decay_t<decltype(capi)>::subgroup_type_order.at(object_pool.subgroup_type_index),
            number_of_shards = this->capi.template get_number_of_shards, object_pool.subgroup_index);
        if (user_specified_node_ids.size() < number_of_shards) {
            throw derecho::derecho_exception(std::string("the size of 'user_specified_node_ids' argument does not match shard number."));
        }
        for (uint32_t shard_index = 0; shard_index < number_of_shards; shard_index ++) {
            on_subgroup_type_index(std::decay_t<decltype(capi)>::subgroup_type_order.at(object_pool.subgroup_type_index),
                this->capi.template set_member_selection_policy, object_pool.subgroup_index, shard_index, static_cast<ShardMemberSelectionPolicy>(policy), user_specified_node_ids.at(shard_index));
        }
        // STEP 2 - prepare workload
        objects.clear();
        uint32_t object_size = derecho::getConfUInt32(derecho::Conf::DERECHO_MAX_P2P_REQUEST_PAYLOAD_SIZE);
        uint32_t num_distinct_objects = std::min(static_cast<uint64_t>(max_num_distinct_objects), max_workload_memory / object_size);
        make_workload<std::string, ObjectWithStringKey>(object_size, num_distinct_objects, object_pool_pathname + "/key_", objects);
        int64_t sleep_us = (start_sec*INT64_1E9 - static_cast<int64_t>(get_walltime()))/INT64_1E3;
        if (sleep_us > 1) {
            usleep(sleep_us);
        }
        // STEP 3 - start experiment and log
        if (this->eval_put(max_operation_per_second,duration_secs,object_pool.subgroup_type_index)) {
            TimestampLogger::flush(output_filename);
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
        int64_t             start_sec,
        uint64_t            duration_secs,
        const std::string&  output_filename) {

        auto object_pool = this->capi.find_object_pool(object_pool_pathname);

        uint32_t number_of_shards;
        // STEP 1 - set up the shard member selection policy
        on_subgroup_type_index(std::decay_t<decltype(capi)>::subgroup_type_order.at(object_pool.subgroup_type_index),
            number_of_shards = this->capi.template get_number_of_shards, object_pool.subgroup_index);
        if (user_specified_node_ids.size() < number_of_shards) {
            throw derecho::derecho_exception(std::string("the size of 'user_specified_node_ids' argument does not match shard number."));
        }
        for (uint32_t shard_index = 0; shard_index < number_of_shards; shard_index ++) {
            on_subgroup_type_index(std::decay_t<decltype(capi)>::subgroup_type_order.at(object_pool.subgroup_type_index),
                this->capi.template set_member_selection_policy, object_pool.subgroup_index, shard_index, static_cast<ShardMemberSelectionPolicy>(policy), user_specified_node_ids.at(shard_index));
        }
        // STEP 2 - prepare workload
        objects.clear();
        uint32_t object_size = derecho::getConfUInt32(derecho::Conf::DERECHO_MAX_P2P_REQUEST_PAYLOAD_SIZE);
        uint32_t num_distinct_objects = std::min(static_cast<uint64_t>(max_num_distinct_objects), max_workload_memory / object_size);
        make_workload<std::string, ObjectWithStringKey>(object_size, num_distinct_objects, object_pool_pathname+"/key_", objects);
        int64_t sleep_us = (start_sec*INT64_1E9 - static_cast<int64_t>(get_walltime()))/INT64_1E3;
        if (sleep_us > 1) {
            usleep(sleep_us);
        }
        // STEP 3 - start experiment and log
        if (this->eval_put_and_forget(max_operation_per_second,duration_secs,object_pool.subgroup_type_index)) {
            TimestampLogger::flush(output_filename);
            return true;
        } else {
            return false;
        }
    });
    // API 2.6 : run object perf with trigger_put
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
    server.bind("perf_trigger_put_to_objectpool", [this](
                                                          const std::string& object_pool_pathname,
                                                          uint32_t policy,
                                                          const std::vector<node_id_t>& user_specified_node_ids,  // one per shard
                                                          double read_write_ratio,
                                                          uint64_t max_operation_per_second,
                                                          int64_t start_sec,
                                                          uint64_t duration_secs,
                                                          const std::string& output_filename) {

        auto object_pool = this->capi.find_object_pool(object_pool_pathname);

        uint32_t number_of_shards;
        // STEP 1 - set up the shard member selection policy
        on_subgroup_type_index(std::decay_t<decltype(capi)>::subgroup_type_order.at(object_pool.subgroup_type_index),
            number_of_shards = this->capi.template get_number_of_shards, object_pool.subgroup_index);
        if (user_specified_node_ids.size() < number_of_shards) {
            throw derecho::derecho_exception(std::string("the size of 'user_specified_node_ids' argument does not match shard number."));
        }
        for (uint32_t shard_index = 0; shard_index < number_of_shards; shard_index ++) {
            on_subgroup_type_index(std::decay_t<decltype(capi)>::subgroup_type_order.at(object_pool.subgroup_type_index),
                this->capi.template set_member_selection_policy, object_pool.subgroup_index, shard_index, static_cast<ShardMemberSelectionPolicy>(policy), user_specified_node_ids.at(shard_index));
        }
        // STEP 2 - prepare workload
        objects.clear();
        uint32_t object_size = derecho::getConfUInt32(derecho::Conf::DERECHO_MAX_P2P_REQUEST_PAYLOAD_SIZE);
        uint32_t num_distinct_objects = std::min(static_cast<uint64_t>(max_num_distinct_objects), max_workload_memory / object_size);
        make_workload<std::string, ObjectWithStringKey>(object_size, num_distinct_objects, object_pool_pathname+"/key_", objects);
        int64_t sleep_us = (start_sec*INT64_1E9 - static_cast<int64_t>(get_walltime()))/INT64_1E3;
        if (sleep_us > 1) {
            usleep(sleep_us);
        }
        // STEP 3 - start experiment and log
        if (this->eval_trigger_put(max_operation_per_second,duration_secs,object_pool.subgroup_type_index)) {
            TimestampLogger::flush(output_filename);
            return true;
        } else {
            return false;
        }
    });
    /**
     * RPC function that runs perf_get using the object pool interface
     *
     * @param object_pool_pathname
     * @param member_selection_policy
     * @param user_specified_node_ids
     * @param log_depth
     * @param max_operations_per_second
     * @param start_sec
     * @param duration_secs
     * @param output_filename
     * @return true if experiment completed successfully, false if there was an error
     */
    server.bind("perf_get_to_objectpool", [this](const std::string& object_pool_pathname,
                                                 uint32_t member_selection_policy,
                                                 const std::vector<node_id_t>& user_specified_node_ids,
                                                 int32_t log_depth,
                                                 uint64_t max_operations_per_second,
                                                 int64_t start_sec,
                                                 uint64_t duration_secs,
                                                 const std::string& output_filename) {
        auto object_pool = this->capi.find_object_pool(object_pool_pathname);
        uint32_t number_of_shards;
        // Set up the shard member selection policy
        std::type_index object_pool_type_index = std::decay_t<decltype(capi)>::subgroup_type_order.at(object_pool.subgroup_type_index);
        on_subgroup_type_index(object_pool_type_index,
                               number_of_shards = this->capi.template get_number_of_shards, object_pool.subgroup_index);
        if(user_specified_node_ids.size() < number_of_shards) {
            throw derecho::derecho_exception(std::string("the size of 'user_specified_node_ids' argument does not match shard number."));
        }
        for(uint32_t shard_index = 0; shard_index < number_of_shards; shard_index++) {
            on_subgroup_type_index(object_pool_type_index,
                                   this->capi.template set_member_selection_policy, object_pool.subgroup_index, shard_index, static_cast<ShardMemberSelectionPolicy>(member_selection_policy), user_specified_node_ids.at(shard_index));
        }
        // Create workload objects
        objects.clear();
        // Ensure the workload objects will fit in memory (for now, 16GB)
        uint32_t object_size = derecho::getConfUInt32(derecho::Conf::DERECHO_MAX_P2P_REQUEST_PAYLOAD_SIZE);
        // Result of min will never be larger than max_num_distinct_objects, so assigning it to uint32_t is safe
        uint32_t num_distinct_objects = std::min(static_cast<uint64_t>(max_num_distinct_objects), max_workload_memory / object_size);
        // Ensure adding log_depth versions to all the workload objects won't run out of log space, in case log_depth is large
        num_distinct_objects = std::min(num_distinct_objects, derecho::getConfUInt32(derecho::Conf::PERS_MAX_LOG_ENTRY) / (log_depth + 1));
        num_distinct_objects = std::min(static_cast<uint64_t>(num_distinct_objects),
                                        derecho::getConfUInt64(derecho::Conf::PERS_MAX_DATA_SIZE) / (object_size * (log_depth + 1)));
        make_workload<std::string, ObjectWithStringKey>(object_size, num_distinct_objects,
                                                        object_pool_pathname + "/key_", objects);
        // Wait for start time
        int64_t sleep_us = (start_sec * INT64_1E9 - static_cast<int64_t>(get_walltime())) / INT64_1E3;
        if(sleep_us > 1) {
            usleep(sleep_us);
        }
        // Run experiment, then log timestamps
        try {
            if(this->eval_get(log_depth, max_operations_per_second, duration_secs, object_pool.subgroup_type_index)) {
                TimestampLogger::flush(output_filename);
                return true;
            } else {
                return false;
            }
        } catch(const std::exception& e) {
            std::cerr << "eval_get failed with exception: " << typeid(e).name() << ": " << e.what() << std::endl;
            return false;
        }
    });

    server.bind("perf_get_by_time_to_objectpool", [this](const std::string& object_pool_pathname,
                                                         uint32_t member_selection_policy,
                                                         const std::vector<node_id_t>& user_specified_node_ids,
                                                         uint64_t ms_in_past,
                                                         uint64_t max_operations_per_second,
                                                         int64_t start_sec,
                                                         uint64_t duration_secs,
                                                         const std::string& output_filename) {
        auto object_pool = this->capi.find_object_pool(object_pool_pathname);
        uint32_t number_of_shards;
        // Set up the shard member selection policy
        std::type_index object_pool_type_index = std::decay_t<decltype(capi)>::subgroup_type_order.at(object_pool.subgroup_type_index);
        on_subgroup_type_index(object_pool_type_index,
                               number_of_shards = this->capi.template get_number_of_shards, object_pool.subgroup_index);
        if(user_specified_node_ids.size() < number_of_shards) {
            throw derecho::derecho_exception(std::string("the size of 'user_specified_node_ids' argument does not match shard number."));
        }
        for(uint32_t shard_index = 0; shard_index < number_of_shards; shard_index++) {
            on_subgroup_type_index(object_pool_type_index,
                                   this->capi.template set_member_selection_policy, object_pool.subgroup_index, shard_index, static_cast<ShardMemberSelectionPolicy>(member_selection_policy), user_specified_node_ids.at(shard_index));
        }
        // Create workload objects
        objects.clear();
        uint32_t object_size = derecho::getConfUInt32(derecho::Conf::DERECHO_MAX_P2P_REQUEST_PAYLOAD_SIZE);
        uint32_t num_distinct_objects = std::min(static_cast<uint64_t>(max_num_distinct_objects), max_workload_memory / object_size);
        // Ensure it's possible to do a put() to each object once in ms_in_past milliseconds at a rate of 10 ms per put
        num_distinct_objects = std::min(static_cast<uint64_t>(num_distinct_objects), ms_in_past / get_by_time_put_interval);
        make_workload<std::string, ObjectWithStringKey>(object_size, num_distinct_objects,
                                                        object_pool_pathname + "/key_", objects);
        // Wait for start time
        int64_t sleep_us = (start_sec * INT64_1E9 - static_cast<int64_t>(get_walltime())) / INT64_1E3;
        if(sleep_us > 1) {
            usleep(sleep_us);
        }
        // Run experiment, then log timestamps
        try {
            if(this->eval_get_by_time(ms_in_past, max_operations_per_second, duration_secs, object_pool.subgroup_type_index)) {
                TimestampLogger::flush(output_filename);
                return true;
            } else {
                return false;
            }
        } catch(const std::exception& e) {
            std::cerr << "eval_get_by_time failed with exception: " << typeid(e).name() << ": " << e.what() << std::endl;
            return false;
        }
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

}
}

#endif//ENABLE_EVALUATION
