#pragma once
#include "../persistent_store.hpp"

#include "cascade/config.h"
#include "cascade/utils.hpp"
#include "debug_util.hpp"
#include "delta_store_core.hpp"

#include <derecho/conf/conf.hpp>
#include <derecho/persistent/PersistentInterface.hpp>
#include <derecho/persistent/detail/PersistLog.hpp>
#include <fstream>
#include <map>
#include <memory>
#include <string>
#include <tuple>
#include <type_traits>

#ifdef ENABLE_EVALUATION
#include <derecho/utils/time.h>
#endif

namespace derecho {
namespace cascade {

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
version_tuple PersistentCascadeStore<KT, VT, IK, IV, ST>::put(const VT& value) const {
    debug_enter_func_with_args("value.get_key_ref()={}", value.get_key_ref());
    LOG_TIMESTAMP_BY_TAG(TLT_PERSISTENT_PUT_START, group, value);

    derecho::Replicated<PersistentCascadeStore>& subgroup_handle = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index);
    auto results = subgroup_handle.template ordered_send<RPC_NAME(ordered_put)>(value);
    auto& replies = results.get();
    version_tuple ret{CURRENT_VERSION, 0};
    // TODO: verfiy consistency ?
    for(auto& reply_pair : replies) {
        ret = reply_pair.second.get();
    }

    LOG_TIMESTAMP_BY_TAG(TLT_PERSISTENT_PUT_END, group, value);
    debug_leave_func_with_value("version=0x{:x},timestamp={}us", std::get<0>(ret), std::get<1>(ret));
    return ret;
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
std::pair<transaction_id,transaction_status_t> PersistentCascadeStore<KT, VT, IK, IV, ST>::put_objects(
        const std::vector<VT>& write_objects,
        const std::unordered_map<uint32_t,std::vector<std::size_t>>& write_objects_per_shard,
        const std::vector<std::tuple<KT,persistent::version_t,persistent::version_t,persistent::version_t>>& read_objects,
        const std::unordered_map<uint32_t,std::vector<std::size_t>>& read_objects_per_shard,
        const std::vector<uint32_t>& shard_list) const {
    debug_enter_func_with_args("write_objects.size={},read_objects.size={},shard_list.size={}", write_objects.size(),read_objects.size(),shard_list.size());

    transaction_id txid = {0,0,persistent::INVALID_VERSION};
    transaction_status_t status = transaction_status_t::ABORT;

    if(!write_objects.empty()){
        derecho::Replicated<PersistentCascadeStore>& subgroup_handle = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index);
        auto results = subgroup_handle.template ordered_send<RPC_NAME(ordered_put_objects)>(write_objects,write_objects_per_shard,read_objects,read_objects_per_shard,shard_list);
        auto& replies = results.get();

        for(auto& reply_pair : replies) {
            auto ret = reply_pair.second.get();
            txid = ret.first;
            status = ret.second;
        }
    }

    debug_leave_func_with_value("txid=({},{},{}),status={}",std::get<0>(txid),std::get<1>(txid),std::get<2>(txid),status);
    return std::make_pair(txid,status);
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
void PersistentCascadeStore<KT, VT, IK, IV, ST>::put_objects_forward(
        const transaction_id& txid,
        const std::vector<VT>& write_objects,
        const std::unordered_map<uint32_t,std::vector<std::size_t>>& write_objects_per_shard,
        const std::vector<std::tuple<KT,persistent::version_t,persistent::version_t,persistent::version_t>>& read_objects,
        const std::unordered_map<uint32_t,std::vector<std::size_t>>& read_objects_per_shard,
        const std::vector<uint32_t>& shard_list) const {
    debug_enter_func_with_args("write_objects.size={},read_objects.size={},shard_list.size={}", write_objects.size(),read_objects.size(),shard_list.size());

    derecho::Replicated<PersistentCascadeStore>& subgroup_handle = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index);
    auto results = subgroup_handle.template ordered_send<RPC_NAME(ordered_put_objects_forward)>(txid,write_objects,write_objects_per_shard,read_objects,read_objects_per_shard,shard_list);
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
void PersistentCascadeStore<KT, VT, IK, IV, ST>::put_objects_backward(const transaction_id& txid,const transaction_status_t& status) const {
    debug_enter_func_with_args("txid=({},{},{}),status={}",std::get<0>(txid),std::get<1>(txid),std::get<2>(txid),status);

    derecho::Replicated<PersistentCascadeStore>& subgroup_handle = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index);
    auto results = subgroup_handle.template ordered_send<RPC_NAME(ordered_put_objects_backward)>(txid,status);
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
void PersistentCascadeStore<KT, VT, IK, IV, ST>::put_and_forget(const VT& value) const {
    debug_enter_func_with_args("value.get_key_ref()={}", value.get_key_ref());
    LOG_TIMESTAMP_BY_TAG(TLT_PERSISTENT_PUT_AND_FORGET_START, group, value);

    derecho::Replicated<PersistentCascadeStore>& subgroup_handle = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index);
    subgroup_handle.template ordered_send<RPC_NAME(ordered_put_and_forget)>(value);

    LOG_TIMESTAMP_BY_TAG(TLT_PERSISTENT_PUT_AND_FORGET_END, group, value);
    debug_leave_func();
}

#ifdef ENABLE_EVALUATION
template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
double PersistentCascadeStore<KT, VT, IK, IV, ST>::perf_put(const uint32_t max_payload_size, const uint64_t duration_sec) const {
    debug_enter_func_with_args("max_payload_size={},duration_sec={}", max_payload_size, duration_sec);
    derecho::Replicated<PersistentCascadeStore>& subgroup_handle = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index);
    double ops = internal_perf_put(subgroup_handle, max_payload_size, duration_sec);
    debug_leave_func_with_value("{} ops.", ops);
    return ops;
}
#endif  // ENABLE_EVALUATION

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
version_tuple PersistentCascadeStore<KT, VT, IK, IV, ST>::remove(const KT& key) const {
    debug_enter_func_with_args("key={}", key);
    LOG_TIMESTAMP_BY_TAG(TLT_PERSISTENT_REMOVE_START, group,*IV);

    derecho::Replicated<PersistentCascadeStore>& subgroup_handle = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index);
    auto results = subgroup_handle.template ordered_send<RPC_NAME(ordered_remove)>(key);
    auto& replies = results.get();
    version_tuple ret(CURRENT_VERSION, 0);
    // TODO: verify consistency ?
    for(auto& reply_pair : replies) {
        ret = reply_pair.second.get();
    }

    LOG_TIMESTAMP_BY_TAG(TLT_PERSISTENT_REMOVE_END, group, *IV);
    debug_leave_func_with_value("version=0x{:x},timestamp={}us", std::get<0>(ret), std::get<1>(ret));
    return ret;
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
const VT PersistentCascadeStore<KT, VT, IK, IV, ST>::get(const KT& key, const persistent::version_t& ver, bool stable, bool exact) const {
    debug_enter_func_with_args("key={},ver=0x{:x},stable={},exact={}", key, ver, stable, exact);
#if __cplusplus > 201703L
    LOG_TIMESTAMP_BY_TAG(TLT_PERSISTENT_GET_START, group,*IV,ver);
#else
    LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_PERSISTENT_GET_START, group,*IV,ver);
#endif

    persistent::version_t requested_version = ver;

    // adjust version if stable is requested.
    if(stable) {
        derecho::Replicated<PersistentCascadeStore>& subgroup_handle = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index);
        requested_version = ver;
        if(requested_version == CURRENT_VERSION) {
            requested_version = subgroup_handle.get_global_persistence_frontier();
        } else {
            // The first condition test if requested_version is beyond the active latest atomic broadcast version.
            // However, that could be true for a valid requested version for a new started setup, where the active
            // latest atomic broadcast version is INVALID_VERSION(-1) since there is no atomic broadcast yet. In such a
            // case, we need also check if requested_version is beyond the local latest version. If both are true, we
            // determine the requested_version is invalid: it asks a version in the future.
            if(!subgroup_handle.wait_for_global_persistence_frontier(requested_version) && requested_version > persistent_core.getLatestVersion()) {
                // INVALID version
                dbg_default_debug("{}: requested version:{:x} is beyond the latest atomic broadcast version.", __PRETTY_FUNCTION__, requested_version);
#if __cplusplus > 201703L
                LOG_TIMESTAMP_BY_TAG(TLT_PERSISTENT_GET_END, group,*IV,ver);
#else
                LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_PERSISTENT_GET_END, group,*IV,ver);
#endif
                return *IV;
            }
        }
    }

    if(requested_version == CURRENT_VERSION) {
        // return the unstable question
        debug_leave_func_with_value("lockless_get({})", key);
#if __cplusplus > 201703L
        LOG_TIMESTAMP_BY_TAG(TLT_PERSISTENT_GET_END, group,*IV,ver);
#else
        LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_PERSISTENT_GET_END, group,*IV,ver);
#endif
        return persistent_core->lockless_get(key);
    } else {
        return persistent_core.template getDelta<std::vector<VT>>(requested_version, exact, [this, key, requested_version, exact, ver](const std::vector<VT>& vv) {
            for (const auto& v:vv) {
                if(key == v.get_key_ref()) {
                    debug_leave_func_with_value("key:{} is found at version:0x{:x}", key, requested_version);
#if __cplusplus > 201703L
                    LOG_TIMESTAMP_BY_TAG(TLT_PERSISTENT_GET_END, group,*IV,ver);
#else
                    LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_PERSISTENT_GET_END, group,*IV,ver);
#endif
                    return v;
                }
            }

            if(exact) {
                // return invalid object for EXACT search.
                debug_leave_func_with_value("No data found for key:{} at version:0x{:x}", key, requested_version);
#if __cplusplus > 201703L
                LOG_TIMESTAMP_BY_TAG(TLT_PERSISTENT_GET_END, group,*IV,ver);
#else
                LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_PERSISTENT_GET_END, group,*IV,ver);
#endif
                return *IV;
            } else {
                // fall back to the slow path.
                // following the backward chain until its version is behind requested_version.
                // TODO: We can introduce a per-key version index to achieve a better performance
                //       with a 64bit per log entry memory overhead.
                VT o = persistent_core->lockless_get(key);
                persistent::version_t target_version = o.version;
                while (target_version > requested_version) {
                    target_version = 
                        persistent_core.template getDelta<std::vector<VT>>(target_version,true,
                            [&key](const std::vector<VT>& vv){
                                for (const auto& v:vv) {
                                    if (key == v.get_key_ref()) {
                                        return v.previous_version_by_key;
                                    }
                                }
                                return persistent::INVALID_VERSION;
                            });
                }
                if (target_version == persistent::INVALID_VERSION) {
#if __cplusplus > 201703L
                    LOG_TIMESTAMP_BY_TAG(TLT_PERSISTENT_GET_END, group,*IV,ver);
#else
                    LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_PERSISTENT_GET_END, group,*IV,ver);
#endif
                    debug_leave_func_with_value("No data found for key:{} before version:0x{:x}", key, requested_version);
                    return *IV;
                } else {
#if __cplusplus > 201703L
                    LOG_TIMESTAMP_BY_TAG(TLT_PERSISTENT_GET_END, group,*IV,ver);
#else
                    LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_PERSISTENT_GET_END, group,*IV,ver);
#endif
                    return persistent_core.template getDelta<std::vector<VT>>(target_version,true,
                            [&key](const std::vector<VT>& vv){
                                for (const auto& v:vv) {
                                    if (key == v.get_key_ref()) {
                                        return v;
                                    }
                                }
                                return *IV;
                            });
                }
            }
        });
    }
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
transaction_status_t PersistentCascadeStore<KT, VT, IK, IV, ST>::get_transaction_status(const transaction_id& txid, const bool stable) const {
    /*
     * TODO careful: we should rethink this once we have a persistent implementation of transaction_database and pending_transactions.
     * Currently, we save the version the TX was committed in memory.
     * Ideally, the version should come from persistent_core.
     * Furthermore, this code will currently fail in a CascadeChain replica site, since other sites will not have any registry of txid.
     */

    /*
     * We start by getting the actual TX.
     *
     * An out_of_range exception can be thrown even if 'txid' exists in
     * transaction_database. Since std::map is not thread-safe, and there is another
     * thread modifying kv_map concurrently, the internal data structure can
     * be changed while this thread is inside transaction_database.at(txid). Therefore, we
     * keep trying until it is possible to get the status.
     */
    CascadeTransactionInternal* tx;
    transaction_status_t status;
    while(true) {
        try {
            if(transaction_database.count(txid) == 0){
                return transaction_status_t::INVALID;
            }

            tx = transaction_database.at(txid);
            status = tx->status;
            break;
        } catch (const std::out_of_range&) {
            dbg_default_debug("{}: out_of_range exception thrown while trying to get transacion", __PRETTY_FUNCTION__);
        }
    }
    
    // wait for stability: check next shards in the chain and use tx->commit_version to check stability in this shard
    if(stable && (tx->status == transaction_status_t::COMMIT)){
        transaction_status_t next_status = transaction_status_t::COMMIT;
        
        /*
         * TODO commenting this part for now: is it necessary?
        
        auto shard_index = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index).get_shard_num();
        std::pair<uint32_t,uint32_t> shard_id(this->subgroup_index,shard_index);
        
        // check next shard if this is not the last
        if(shard_id != tx->shard_list.back()){
            // get next shard
            auto it = std::next(std::find(tx->shard_list.begin(),tx->shard_list.end(),shard_id));
            auto next_subgroup_index = (*it).first;
            auto next_shard_index = (*it).second;

            // target shard is in the same subgroup
            if(this->subgroup_index == next_subgroup_index){
                // TODO should we have other policies to pick the next node?
                std::vector<std::vector<node_id_t>> subgroup_members = group->template get_subgroup_members<PersistentCascadeStore>(this->subgroup_index);
                node_id_t next_node_id = subgroup_members[next_shard_index][0];

                // p2p_send to the next shard
                auto& subgroup_handle = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index);
                auto results = subgroup_handle.template p2p_send<RPC_NAME(get_transaction_status)>(next_node_id,txid,true);
                auto& replies = results.get();
                for(auto& reply_pair : replies) {
                    next_status = reply_pair.second.get();
                }
            } else {
                // TODO should we have other policies to pick the next node?
                std::vector<std::vector<node_id_t>> next_subgroup_members = group->template get_subgroup_members<PersistentCascadeStore>(next_subgroup_index);
                node_id_t next_node_id = next_subgroup_members[next_shard_index][0];

                // p2p_send to the next shard
                auto& subgroup_handle = group->template get_nonmember_subgroup<PersistentCascadeStore>(next_subgroup_index);
                auto results = subgroup_handle.template p2p_send<RPC_NAME(get_transaction_status)>(next_node_id,txid,true);
                auto& replies = results.get();
                for(auto& reply_pair : replies) {
                    next_status = reply_pair.second.get();
                }
            }
        }*/
        
        // check for this shard
        if(next_status == transaction_status_t::COMMIT){
            if(tx->write_objects.size() > 0){
                persistent::version_t requested_version = tx->commit_version;

                derecho::Replicated<PersistentCascadeStore>& subgroup_handle = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index);

                // The first condition test if requested_version is beyond the active latest atomic broadcast version.
                // However, that could be true for a valid requested version for a new started setup, where the active
                // latest atomic broadcast version is INVALID_VERSION(-1) since there is no atomic broadcast yet. In such a
                // case, we need also check if requested_version is beyond the local latest version. If both are true, we
                // determine the requested_version is invalid: it asks a version in the future.
                if(!subgroup_handle.wait_for_global_persistence_frontier(requested_version) && requested_version > persistent_core.getLatestVersion()) {
                    // INVALID version
                    dbg_default_debug("{}: requested version:{:x} is beyond the latest atomic broadcast version.", __PRETTY_FUNCTION__, requested_version);
                    return transaction_status_t::INVALID;
                }
            }
        } 
        
        status = next_status;
    }
    
    return status;
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
const VT PersistentCascadeStore<KT, VT, IK, IV, ST>::multi_get(const KT& key) const {
    debug_enter_func_with_args("key={}", key);
    LOG_TIMESTAMP_BY_TAG(TLT_PERSISTENT_MULTI_GET_START, group,*IV);

    derecho::Replicated<PersistentCascadeStore>& subgroup_handle = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index);
    auto results = subgroup_handle.template ordered_send<RPC_NAME(ordered_get)>(key);
    auto& replies = results.get();
    //  TODO: verify consistency ?
    for(auto& reply_pair : replies) {
        reply_pair.second.wait();
    }

    LOG_TIMESTAMP_BY_TAG(TLT_PERSISTENT_MULTI_GET_END, group,*IV);
    debug_leave_func();
    return replies.begin()->second.get();
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
const VT PersistentCascadeStore<KT, VT, IK, IV, ST>::get_by_time(const KT& key, const uint64_t& ts_us, const bool stable) const {
    debug_enter_func_with_args("key={},ts_us={},stable={}", key, ts_us, stable);
    const HLC hlc(ts_us, 0ull);

    derecho::Replicated<PersistentCascadeStore>& subgroup_handle = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index);

    if(ts_us > get_time_us()) {
        dbg_default_warn("Cannot get data at a time in the future.");
        return *IV;
    }

    // get_global_stability_frontier return nano seconds.
    if(stable && (ts_us > (subgroup_handle.compute_global_stability_frontier() / 1000))) {
        dbg_default_debug("Stability frontier is {} but requested timestamp is {}", subgroup_handle.compute_global_stability_frontier() / 1000, ts_us);
        dbg_default_warn("Cannot get data at a time in the future.");
        return *IV;
    }

    persistent::version_t ver = persistent_core.getVersionAtTime({ts_us, 0});
    if(ver == persistent::INVALID_VERSION) {
        return *IV;
    }

    debug_leave_func();
    return get(key, ver, stable, false);
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
uint64_t PersistentCascadeStore<KT, VT, IK, IV, ST>::multi_get_size(const KT& key) const {
    debug_enter_func_with_args("key={}", key);
    LOG_TIMESTAMP_BY_TAG(TLT_PERSISTENT_MULTI_GET_SIZE_START,group,*IV);

    derecho::Replicated<PersistentCascadeStore>& subgroup_handle = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index);
    auto results = subgroup_handle.template ordered_send<RPC_NAME(ordered_get_size)>(key);
    auto& replies = results.get();
    // TODO: verify consistency ?
    // for (auto& reply_pair : replies) {
    //     ret = reply_pair.second.get();
    // }
    uint64_t size = replies.begin()->second.get();
    LOG_TIMESTAMP_BY_TAG(TLT_PERSISTENT_MULTI_GET_SIZE_END,group,*IV);
    debug_leave_func();
    return size;
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
uint64_t PersistentCascadeStore<KT, VT, IK, IV, ST>::get_size(const KT& key, const persistent::version_t& ver, const bool stable, const bool exact) const {
    debug_enter_func_with_args("key={},ver=0x{:x},stable={},exact={}", key, ver, stable, exact);
#if __cplusplus > 201703L
    LOG_TIMESTAMP_BY_TAG(TLT_PERSISTENT_GET_SIZE_START, group,*IV,ver);
#else
    LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_PERSISTENT_GET_SIZE_START, group,*IV,ver);
#endif

    persistent::version_t requested_version = ver;

    // adjust version if stable is requested.
    if(stable) {
        derecho::Replicated<PersistentCascadeStore>& subgroup_handle = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index);
        requested_version = ver;
        if(requested_version == CURRENT_VERSION) {
            requested_version = subgroup_handle.get_global_persistence_frontier();
        } else {
            // The first condition test if requested_version is beyond the active latest atomic broadcast version.
            // However, that could be true for a valid requested version for a new started setup, where the active
            // latest atomic broadcast version is INVALID_VERSION(-1) since there is no atomic broadcast yet. In such a
            // case, we need also check if requested_version is beyond the local latest version. If both are true, we
            // determine the requested_version is invalid: it asks a version in the future.
            if(!subgroup_handle.wait_for_global_persistence_frontier(requested_version) && requested_version > persistent_core.getLatestVersion()) {
                // INVALID version
                dbg_default_debug("{}: requested version:{:x} is beyond the latest atomic broadcast version.", __PRETTY_FUNCTION__, requested_version);
#if __cplusplus > 201703L
                LOG_TIMESTAMP_BY_TAG(TLT_PERSISTENT_GET_SIZE_END, group,*IV,ver);
#else
                LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_PERSISTENT_GET_SIZE_END, group,*IV,ver);
#endif

                return 0ull;
            }
        }
    }

    if(requested_version == CURRENT_VERSION) {
        // return the unstable question
        debug_leave_func_with_value("lockless_get_size({})", key);
        auto rvo_val = persistent_core->lockless_get_size(key);
#if __cplusplus > 201703L
        LOG_TIMESTAMP_BY_TAG(TLT_PERSISTENT_GET_SIZE_END, group,*IV,ver);
#else
        LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_PERSISTENT_GET_SIZE_END, group,*IV,ver);
#endif
         return rvo_val;
    } else {
        return persistent_core.template getDelta<std::vector<VT>>(requested_version, exact, [this, key, requested_version, exact, ver](const std::vector<VT>& vv) -> uint64_t {
            for (const auto& v: vv) {
                if(key == v.get_key_ref()) {
                    debug_leave_func_with_value("key:{} is found at version:0x{:x}", key, requested_version);
                    uint64_t size = mutils::bytes_size(v);
#if __cplusplus > 201703L
                    LOG_TIMESTAMP_BY_TAG(TLT_PERSISTENT_GET_SIZE_END, group,*IV,ver);
#else
                    LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_PERSISTENT_GET_SIZE_END, group,*IV,ver);
#endif
                    return size;
                }
            }

            if(exact) {
                // return invalid object for EXACT search.
                debug_leave_func_with_value("No data found for key:{} at version:0x{:x}", key, requested_version);
#if __cplusplus > 201703L
                LOG_TIMESTAMP_BY_TAG(TLT_PERSISTENT_GET_SIZE_END, group,*IV,ver);
#else
                LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_PERSISTENT_GET_SIZE_END, group,*IV,ver);
#endif
                return 0ull;
            } else {
                // fall back to the slow path.
                // following the backward chain until its version is behind requested_version.
                // TODO: We can introduce a per-key version index to achieve a better performance
                //       with a 64bit per log entry memory overhead.
                VT o = persistent_core->lockless_get(key);
                persistent::version_t target_version = o.version;
                while (target_version > requested_version) {
                    target_version = 
                        persistent_core.template getDelta<std::vector<VT>>(target_version,true,
                            [&key](const std::vector<VT>& vv){
                                for (const auto& v:vv) {
                                    if (key == v.get_key_ref()) {
                                        return v.previous_version_by_key;
                                    }
                                }
                                return persistent::INVALID_VERSION;
                            });
                }
                if (target_version == persistent::INVALID_VERSION) {
#if __cplusplus > 201703L
                    LOG_TIMESTAMP_BY_TAG(TLT_PERSISTENT_GET_SIZE_END, group,*IV,ver);
#else
                    LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_PERSISTENT_GET_SIZE_END, group,*IV,ver);
#endif
                    debug_leave_func_with_value("No data found for key:{} before version:0x{:x}", key, requested_version);
                    return 0ull;
                } else {
#if __cplusplus > 201703L
                    LOG_TIMESTAMP_BY_TAG(TLT_PERSISTENT_GET_SIZE_END, group,*IV,ver);
#else
                    LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_PERSISTENT_GET_SIZE_END, group,*IV,ver);
#endif
                    return persistent_core.template getDelta<std::vector<VT>>(target_version,true,
                            [&key](const std::vector<VT>& vv){
                                for (const auto& v:vv) {
                                    if (key == v.get_key_ref()) {
                                        return static_cast<uint64_t>(mutils::bytes_size(v));
                                    }
                                }
                                return static_cast<uint64_t>(0ull);
                            });
                }
            }
        });
    }
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
uint64_t PersistentCascadeStore<KT, VT, IK, IV, ST>::get_size_by_time(const KT& key, const uint64_t& ts_us, const bool stable) const {
    debug_enter_func_with_args("key={},ts_us={},stable={}", key, ts_us, stable);
    const HLC hlc(ts_us, 0ull);

    derecho::Replicated<PersistentCascadeStore>& subgroup_handle = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index);

    // get_global_stability_frontier return nano seconds.
    if ((ts_us > get_time_us()) ||
        (stable && (ts_us > (subgroup_handle.compute_global_stability_frontier() / 1000)))) {
        dbg_default_warn("Cannot get data at a time in the future.");
        return 0;
    }

    persistent::version_t ver = persistent_core.getVersionAtTime({ts_us, 0});
    if(ver == persistent::INVALID_VERSION) {
        return 0;
    }

    debug_leave_func();

    return get_size(key, ver, stable);
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
std::vector<KT> PersistentCascadeStore<KT, VT, IK, IV, ST>::multi_list_keys(const std::string& prefix) const {
    debug_enter_func_with_args("prefix={}.", prefix);
    LOG_TIMESTAMP_BY_TAG(TLT_PERSISTENT_MULTI_LIST_KEYS_START,group,*IV);

    derecho::Replicated<PersistentCascadeStore>& subgroup_handle = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index);
    auto results = subgroup_handle.template ordered_send<RPC_NAME(ordered_list_keys)>(prefix);
    auto& replies = results.get();
    // TODO: verify consistency ?
    // TODO: make shoudl nrvo works here!!!
    auto rvo_val = replies.begin()->second.get();
    LOG_TIMESTAMP_BY_TAG(TLT_PERSISTENT_MULTI_LIST_KEYS_END,group,*IV);
    debug_leave_func();
    return rvo_val;
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
std::vector<KT> PersistentCascadeStore<KT, VT, IK, IV, ST>::list_keys(const std::string& prefix, const persistent::version_t& ver, const bool stable) const {
    debug_enter_func_with_args("prefix={}, ver=0x{:x}, stable={}", prefix, ver, stable);
#if __cplusplus > 201703L
    LOG_TIMESTAMP_BY_TAG(TLT_PERSISTENT_LIST_KEYS_START, group,*IV,ver);
#else
    LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_PERSISTENT_LIST_KEYS_START, group,*IV,ver);
#endif

    persistent::version_t requested_version = ver;

    if(stable) {
        derecho::Replicated<PersistentCascadeStore>& subgroup_handle = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index);
        requested_version = ver;
        if(requested_version == CURRENT_VERSION) {
            requested_version = subgroup_handle.get_global_persistence_frontier();
        } else {
            // The first condition test if requested_version is beyond the active latest atomic broadcast version.
            // However, that could be true for a valid requested version for a new started setup, where the active
            // latest atomic broadcast version is INVALID_VERSION(-1) since there is no atomic broadcast yet. In such a
            // case, we need also check if requested_version is beyond the local latest version. If both are true, we
            // determine the requested_version is invalid: it asks a version in the future.
            if(!subgroup_handle.wait_for_global_persistence_frontier(requested_version) && requested_version > persistent_core.getLatestVersion()) {
                // INVALID version
#if __cplusplus > 201703L
                LOG_TIMESTAMP_BY_TAG(TLT_PERSISTENT_LIST_KEYS_END, group,*IV,ver);
#else
                LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_PERSISTENT_LIST_KEYS_END, group,*IV,ver);
#endif
                dbg_default_debug("{}: requested version:{:x} is beyond the latest atomic broadcast version.", __PRETTY_FUNCTION__, requested_version);
                return {};
            }
        }
    }

    if(requested_version == CURRENT_VERSION) {
        // return the unstable question
        debug_leave_func_with_value("lockless_list_prefix({})", prefix);
        // TODO: make sure NRVO works here!!!
        auto rvo_val = persistent_core->lockless_list_keys(prefix);
#if __cplusplus > 201703L
        LOG_TIMESTAMP_BY_TAG(TLT_PERSISTENT_LIST_KEYS_END, group,*IV,ver);
#else
        LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_PERSISTENT_LIST_KEYS_END, group,*IV,ver);
#endif
        return rvo_val;
    } else {
        std::vector<KT> keys;
        persistent_core.get(requested_version, [&keys, &prefix](const DeltaCascadeStoreCore<KT, VT, IK, IV>& pers_core) {
            for(const auto& kv : pers_core.kv_map) {
                if(get_pathname<KT>(kv.first).find(prefix) == 0) {
                    keys.push_back(kv.first);
                }
            }
        });
#if __cplusplus > 201703L
        LOG_TIMESTAMP_BY_TAG(TLT_PERSISTENT_LIST_KEYS_END, group,*IV,ver);
#else
        LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_PERSISTENT_LIST_KEYS_END, group,*IV,ver);
#endif
        return keys;
    }
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
std::vector<KT> PersistentCascadeStore<KT, VT, IK, IV, ST>::list_keys_by_time(const std::string& prefix, const uint64_t& ts_us, const bool stable) const {
    debug_enter_func_with_args("ts_us={}", ts_us);
    const HLC hlc(ts_us, 0ull);

    derecho::Replicated<PersistentCascadeStore>& subgroup_handle = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index);

    // get_global_stability_frontier return nano seconds.
    if((ts_us > get_time_us()) ||
       (stable && (ts_us > (subgroup_handle.compute_global_stability_frontier() / 1000)))) {
        dbg_default_warn("Cannot get data at a time in the future.");
        return {};
    }

    persistent::version_t ver = persistent_core.getVersionAtTime({ts_us, 0});
    if(ver == persistent::INVALID_VERSION) {
        return {};
    }

    return list_keys(prefix, ver, stable);
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
version_tuple PersistentCascadeStore<KT, VT, IK, IV, ST>::ordered_put(const VT& value) {
    debug_enter_func_with_args("key={}", value.get_key_ref());
    auto version_and_hlc = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index).get_current_version();
#if __cplusplus > 201703L
    LOG_TIMESTAMP_BY_TAG(TLT_PERSISTENT_ORDERED_PUT_START, group, value, std::get<0>(version_and_hlc));
#else
    LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_PERSISTENT_ORDERED_PUT_START, group, value, std::get<0>(version_and_hlc));
#endif
    version_tuple version_and_timestamp{persistent::INVALID_VERSION,0};
    
    // fail if there is a pending TX conflicting with this object
    if(!has_conflict(value)){
        if(this->internal_ordered_put(value) == true) {
            version_and_timestamp = {std::get<0>(version_and_hlc),std::get<1>(version_and_hlc).m_rtc_us};
        }
    }

#if __cplusplus > 201703L
    LOG_TIMESTAMP_BY_TAG(TLT_PERSISTENT_ORDERED_PUT_END, group, value, std::get<0>(version_and_hlc));
#else
    LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_PERSISTENT_ORDERED_PUT_END, group, value, std::get<0>(version_and_hlc));
#endif
    debug_leave_func_with_value("version=0x{:x},timestamp={}us",
            std::get<0>(version_and_hlc),
            std::get<1>(version_and_hlc).m_rtc_us);

    return version_and_timestamp;
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
std::pair<transaction_id,transaction_status_t> PersistentCascadeStore<KT, VT, IK, IV, ST>::ordered_put_objects(
        const std::vector<VT>& write_objects,
        const std::unordered_map<uint32_t,std::vector<std::size_t>>& write_objects_per_shard,
        const std::vector<std::tuple<KT,persistent::version_t,persistent::version_t,persistent::version_t>>& read_objects,
        const std::unordered_map<uint32_t,std::vector<std::size_t>>& read_objects_per_shard,
        const std::vector<uint32_t>& shard_list) {
    debug_enter_func_with_args("write_objects.size={},read_objects.size={},shard_list.size={}", write_objects.size(),read_objects.size(),shard_list.size());
    
    transaction_id txid = {0,0,persistent::INVALID_VERSION};
    transaction_status_t status = transaction_status_t::ABORT;

    if(!write_objects.empty()){
        // get an ID and add the TX to the internal structures
        auto shard_index = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index).get_shard_num();
        CascadeTransactionInternal* tx = new CascadeTransactionInternal(new_transaction_id(),write_objects,write_objects_per_shard,read_objects,read_objects_per_shard,shard_list,shard_index);
        enqueue_transaction(tx);

        // check if there is a conflicting TX in the pending list
        // if not, we can start processing it
        if(!has_conflict(tx)){
            // check previous versions for this shard
            if(check_previous_versions(tx)) {
                // check if this is the last shard
                if(shard_index == tx->shard_list.back()){
                    // this is the last and only shard, we can commit and remove from the pending list
                    commit_transaction(tx);
                    tx->status = transaction_status_t::COMMIT;
                
                    // no need to check conflicts: we know at this point that there is no conflict
                    dequeue_transaction(tx);
                } else {
                    send_tx_forward(tx,write_objects,write_objects_per_shard,read_objects,read_objects_per_shard,shard_list);
                }
            } else {
                // this is the first shard, so we can just ABORT and remove the tx from the pending list, no need to send the result backwards to the previous shard
                tx->status = transaction_status_t::ABORT;
                dequeue_transaction(tx);
            }
            
            versions_checked.at(tx) = true;
        }

        txid = tx->txid;
        status = tx->status;
    }
    
    debug_leave_func_with_value("txid=({},{},{}),status={}",std::get<0>(txid),std::get<1>(txid),std::get<2>(txid),status);
    return std::make_pair(txid,status);
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
void PersistentCascadeStore<KT, VT, IK, IV, ST>::ordered_put_objects_forward(
        const transaction_id& txid,
        const std::vector<VT>& write_objects,
        const std::unordered_map<uint32_t,std::vector<std::size_t>>& write_objects_per_shard,
        const std::vector<std::tuple<KT,persistent::version_t,persistent::version_t,persistent::version_t>>& read_objects,
        const std::unordered_map<uint32_t,std::vector<std::size_t>>& read_objects_per_shard,
        const std::vector<uint32_t>& shard_list) {
    debug_enter_func_with_args("txid=({},{},{}),write_objects.size={},read_objects.size={},shard_list.size={}", std::get<0>(txid),std::get<1>(txid),std::get<2>(txid),write_objects.size(),read_objects.size(),shard_list.size());
    // if it is a transaction that we already have, do nothing (it is being resent by a recovering node)
    if(transaction_database.count(txid) > 0){
        return;
    }

    // register the new tx
    auto shard_index = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index).get_shard_num();
    CascadeTransactionInternal* tx = new CascadeTransactionInternal(txid,write_objects,write_objects_per_shard,read_objects,read_objects_per_shard,shard_list,shard_index);
    enqueue_transaction(tx);
    
    // check if there is a conflicting TX in the pending list
    // if not, we can start processing it
    if(!has_conflict(tx)){
        // check previous versions for this shard
        if(check_previous_versions(tx)) {
            // check if this is the last shard
            if(shard_index == tx->shard_list.back()){
                // this is the last, but not the only: we commit and send the result backwards
                commit_transaction(tx);
                tx->status = transaction_status_t::COMMIT;
                send_tx_status_backward(tx);
                
                // clean queue only after sending to the previous/next shard
                // no need to check conflicts: we know at this point that there is no conflict
                dequeue_transaction(tx);
            } else {
                send_tx_forward(tx,write_objects,write_objects_per_shard,read_objects,read_objects_per_shard,shard_list);
            }
        } else {
            // send ABORT to previous shard
            tx->status = transaction_status_t::ABORT;
            send_tx_status_backward(tx);
            
            // clean queue only after sending to the previous/next shard
            // no need to check conflicts: we know at this point that there is no conflict
            dequeue_transaction(tx);
        }

        versions_checked.at(tx) = true;
    }
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
void PersistentCascadeStore<KT, VT, IK, IV, ST>::ordered_put_objects_backward(const transaction_id& txid,const transaction_status_t& status) {
    debug_enter_func_with_args("txid=({},{},{})",std::get<0>(txid),std::get<1>(txid),std::get<2>(txid));

    // if we do not have the transaction, something went wrong
    if(transaction_database.count(txid) == 0){
        dbg_default_debug("{}: received an unknown transaction ({},{},{})", __PRETTY_FUNCTION__, std::get<0>(txid),std::get<1>(txid),std::get<2>(txid));
        return;
    }
    
    CascadeTransactionInternal* tx = transaction_database.at(txid);
   
    // if this was already processed, do nothing (it is being resent by a recovering node)
    if(tx->status != transaction_status_t::PENDING){
        return;
    }

    // if version was not checked, it could not have been forwarded in the first place: panic
    assert(versions_checked.at(tx) == true);
    
    // abort or commit TXs that were waiting for this one
    // this procedure is recursive since the change in status of TXs may trigger a chain of aborts, commits, and forwards
    tx->status = status;
    if(status == transaction_status_t::COMMIT){
        tx_committed_recursive(tx);
    } else {
        tx_aborted_recursive(tx);
    }
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
void PersistentCascadeStore<KT, VT, IK, IV, ST>::ordered_put_and_forget(const VT& value) {
    debug_enter_func_with_args("key={}", value.get_key_ref());
#ifdef ENABLE_EVALUATION
    auto version_and_hlc = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index).get_current_version();
#endif

#if __cplusplus > 201703L
    LOG_TIMESTAMP_BY_TAG(TLT_PERSISTENT_ORDERED_PUT_AND_FORGET_START, group, value, std::get<0>(version_and_hlc));
#else
    LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_PERSISTENT_ORDERED_PUT_AND_FORGET_START, group, value, std::get<0>(version_and_hlc));
#endif
    
    // fail if there is a pending TX conflicting with this object
    if(!has_conflict(value)){
        this->internal_ordered_put(value);
    }

#if __cplusplus > 201703L
    LOG_TIMESTAMP_BY_TAG(TLT_PERSISTENT_ORDERED_PUT_AND_FORGET_END, group, value, std::get<0>(version_and_hlc));
#else
    LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_PERSISTENT_ORDERED_PUT_AND_FORGET_END, group, value, std::get<0>(version_and_hlc));
#endif

#ifdef ENABLE_EVALUATION
    // avoid unused variable warning.
    if constexpr(!std::is_base_of<IHasMessageID, VT>::value) {
        version_and_hlc = version_and_hlc;
    }
#endif
    debug_leave_func();
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
bool PersistentCascadeStore<KT, VT, IK, IV, ST>::internal_ordered_put(const VT& value) {
    auto version_and_hlc = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index).get_current_version();
    if constexpr(std::is_base_of<IKeepVersion, VT>::value) {
        value.set_version(std::get<0>(version_and_hlc));
    }
    if constexpr(std::is_base_of<IKeepTimestamp, VT>::value) {
        value.set_timestamp(std::get<1>(version_and_hlc).m_rtc_us);
    }
    if(this->persistent_core->ordered_put(value, this->persistent_core.getLatestVersion()) == false) {
        // verification failed. S we return invalid versions.
        debug_leave_func_with_value("version=0x{:x},timestamp={}us",
                std::get<0>(version_and_hlc),
                std::get<1>(version_and_hlc).m_rtc_us);
        return false;
    }
    if(cascade_watcher_ptr) {
        (*cascade_watcher_ptr)(
                // group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index).get_subgroup_id(), // this is subgroup id
                this->subgroup_index,
                group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index).get_shard_num(),
                group->get_rpc_caller_id(),
                value.get_key_ref(), value, cascade_context_ptr);
    }
    return true;
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
version_tuple PersistentCascadeStore<KT, VT, IK, IV, ST>::ordered_remove(const KT& key) {
    debug_enter_func_with_args("key={}", key);
    auto version_and_hlc = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index).get_current_version();
    version_tuple version_and_timestamp{persistent::INVALID_VERSION,0};

#if __cplusplus > 201703L
    LOG_TIMESTAMP_BY_TAG(TLT_PERSISTENT_ORDERED_REMOVE_START, group, *IV, std::get<0>(version_and_hlc));
#else
    LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_PERSISTENT_ORDERED_REMOVE_START, group, *IV, std::get<0>(version_and_hlc));
#endif

    auto value = create_null_object_cb<KT, VT, IK, IV>(key);
    if constexpr(std::is_base_of<IKeepVersion, VT>::value) {
        value.set_version(std::get<0>(version_and_hlc));
    }
    if constexpr(std::is_base_of<IKeepTimestamp, VT>::value) {
        value.set_timestamp(std::get<1>(version_and_hlc).m_rtc_us);
    }

    // fail if there is a pending TX conflicting with this object
    if(!has_conflict(value)){
        if(this->persistent_core->ordered_remove(value, this->persistent_core.getLatestVersion())) {
            if(cascade_watcher_ptr) {
                (*cascade_watcher_ptr)(
                        // group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index).get_subgroup_id(), // this is subgroup id
                        this->subgroup_index,
                        group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index).get_shard_num(),
                        group->get_rpc_caller_id(),
                        key, value, cascade_context_ptr);
            }
        }
        version_and_timestamp = {std::get<0>(version_and_hlc),std::get<1>(version_and_hlc).m_rtc_us};
    }

#if __cplusplus > 201703L
    LOG_TIMESTAMP_BY_TAG(TLT_PERSISTENT_ORDERED_REMOVE_END, group, *IV, std::get<0>(version_and_hlc));
#else
    LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_PERSISTENT_ORDERED_REMOVE_END, group, *IV, std::get<0>(version_and_hlc));
#endif
    debug_leave_func_with_value("version=0x{:x},timestamp={}us",
            std::get<0>(version_and_hlc),
            std::get<1>(version_and_hlc).m_rtc_us);

    return version_and_timestamp;
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
const VT PersistentCascadeStore<KT, VT, IK, IV, ST>::ordered_get(const KT& key) {
    debug_enter_func_with_args("key={}", key);
#ifdef ENABLE_EVALUATION
    auto version_and_hlc = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index).get_current_version();
#endif

#if __cplusplus > 201703L
    LOG_TIMESTAMP_BY_TAG(TLT_PERSISTENT_ORDERED_GET_START, group, *IV, std::get<0>(version_and_hlc));
#else
    LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_PERSISTENT_ORDERED_GET_START, group, *IV, std::get<0>(version_and_hlc));
#endif

    //TODO: double check if Named Return Value Optimization (NRVO) works here!!!
    auto rvo_val = this->persistent_core->ordered_get(key);

#if __cplusplus > 201703L
    LOG_TIMESTAMP_BY_TAG(TLT_PERSISTENT_ORDERED_GET_END, group, *IV, std::get<0>(version_and_hlc));
#else
    LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_PERSISTENT_ORDERED_GET_END, group, *IV, std::get<0>(version_and_hlc));
#endif

    debug_leave_func();
    return rvo_val;
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
uint64_t PersistentCascadeStore<KT, VT, IK, IV, ST>::ordered_get_size(const KT& key) {
    debug_enter_func_with_args("key={}", key);
#ifdef ENABLE_EVALUATION
    auto version_and_hlc = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index).get_current_version();
#endif

#if __cplusplus > 201703L
    LOG_TIMESTAMP_BY_TAG(TLT_PERSISTENT_ORDERED_GET_SIZE_START, group, *IV, std::get<0>(version_and_hlc));
#else
    LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_PERSISTENT_ORDERED_GET_SIZE_START, group, *IV, std::get<0>(version_and_hlc));
#endif

    uint64_t size = this->persistent_core->ordered_get_size(key);

#if __cplusplus > 201703L
    LOG_TIMESTAMP_BY_TAG(TLT_PERSISTENT_ORDERED_GET_SIZE_END, group, *IV, std::get<0>(version_and_hlc));
#else
    LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_PERSISTENT_ORDERED_GET_SIZE_END, group, *IV, std::get<0>(version_and_hlc));
#endif

    debug_leave_func();
    return size;
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
void PersistentCascadeStore<KT, VT, IK, IV, ST>::trigger_put(const VT& value) const {
    debug_enter_func_with_args("key={}", value.get_key_ref());
    LOG_TIMESTAMP_BY_TAG(TLT_PERSISTENT_TRIGGER_PUT_START, group, value);

    if(cascade_watcher_ptr) {
        (*cascade_watcher_ptr)(
                this->subgroup_index,
                group->template get_subgroup<PersistentCascadeStore<KT, VT, IK, IV, ST>>(this->subgroup_index).get_shard_num(),
                group->get_rpc_caller_id(),
                value.get_key_ref(), value, cascade_context_ptr, true);
    }

    LOG_TIMESTAMP_BY_TAG(TLT_PERSISTENT_TRIGGER_PUT_END, group, value);
    debug_leave_func();
}

#ifdef ENABLE_EVALUATION
template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
void PersistentCascadeStore<KT, VT, IK, IV, ST>::dump_timestamp_log(const std::string& filename) const {
    debug_enter_func_with_args("filename={}", filename);
    derecho::Replicated<PersistentCascadeStore>& subgroup_handle = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index);
    auto result = subgroup_handle.template ordered_send<RPC_NAME(ordered_dump_timestamp_log)>(filename);
    auto& replies = result.get();
    for(auto r : replies) {
        volatile uint32_t _ = r;
        _ = _;
    }
    debug_leave_func();
    return;
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
void PersistentCascadeStore<KT, VT, IK, IV, ST>::ordered_dump_timestamp_log(const std::string& filename) {
    debug_enter_func_with_args("filename={}", filename);
    TimestampLogger::flush(filename);
    debug_leave_func();
}

#ifdef DUMP_TIMESTAMP_WORKAROUND
template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
void PersistentCascadeStore<KT, VT, IK, IV, ST>::dump_timestamp_log_workaround(const std::string& filename) const {
    debug_enter_func_with_args("filename={}", filename);
    TimestampLogger::flush(filename);
    debug_leave_func();
}
#endif
#endif  // ENABLE_EVALUATION

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
std::vector<KT> PersistentCascadeStore<KT, VT, IK, IV, ST>::ordered_list_keys(const std::string& prefix) {
    debug_enter_func();

#ifdef ENABLE_EVALUATION
    auto version_and_hlc = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index).get_current_version();
#endif
#if __cplusplus > 201703L
    LOG_TIMESTAMP_BY_TAG(TLT_PERSISTENT_ORDERED_LIST_KEYS_START, group, *IV, std::get<0>(version_and_hlc));
#else
    LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_PERSISTENT_ORDERED_LIST_KEYS_START, group, *IV, std::get<0>(version_and_hlc));
#endif

    // TODO: make sure NRVO works here!!!
    auto rvo_val = this->persistent_core->ordered_list_keys(prefix);

#if __cplusplus > 201703L
    LOG_TIMESTAMP_BY_TAG(TLT_PERSISTENT_ORDERED_LIST_KEYS_END, group, *IV, std::get<0>(version_and_hlc));
#else
    LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_PERSISTENT_ORDERED_LIST_KEYS_END, group, *IV, std::get<0>(version_and_hlc));
#endif
    debug_leave_func();
    return rvo_val;
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
std::unique_ptr<PersistentCascadeStore<KT, VT, IK, IV, ST>> PersistentCascadeStore<KT, VT, IK, IV, ST>::from_bytes(mutils::DeserializationManager* dsm, uint8_t const* buf) {
    auto persistent_core_ptr = mutils::from_bytes<persistent::Persistent<DeltaCascadeStoreCore<KT, VT, IK, IV>, ST>>(dsm, buf);
    auto persistent_cascade_store_ptr = std::make_unique<PersistentCascadeStore>(std::move(*persistent_core_ptr),
                                                                                 dsm->registered<CriticalDataPathObserver<PersistentCascadeStore<KT, VT, IK, IV>>>() ? &(dsm->mgr<CriticalDataPathObserver<PersistentCascadeStore<KT, VT, IK, IV>>>()) : nullptr,
                                                                                 dsm->registered<ICascadeContext>() ? &(dsm->mgr<ICascadeContext>()) : nullptr);
    return persistent_cascade_store_ptr;
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
PersistentCascadeStore<KT, VT, IK, IV, ST>::PersistentCascadeStore(
        persistent::PersistentRegistry* pr,
        CriticalDataPathObserver<PersistentCascadeStore<KT, VT, IK, IV>>* cw,
        ICascadeContext* cc) : persistent_core([]() {
                                   return std::make_unique<DeltaCascadeStoreCore<KT, VT, IK, IV>>();
                               },
                                               nullptr, pr),
                               cascade_watcher_ptr(cw),
                               cascade_context_ptr(cc) {
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
PersistentCascadeStore<KT, VT, IK, IV, ST>::PersistentCascadeStore(
        persistent::Persistent<DeltaCascadeStoreCore<KT, VT, IK, IV>, ST>&&
                _persistent_core,
        CriticalDataPathObserver<PersistentCascadeStore<KT, VT, IK, IV>>* cw,
        ICascadeContext* cc) : persistent_core(std::move(_persistent_core)),
                               cascade_watcher_ptr(cw),
                               cascade_context_ptr(cc) {
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
PersistentCascadeStore<KT, VT, IK, IV, ST>::PersistentCascadeStore() : persistent_core(
        []() {
            return std::make_unique<DeltaCascadeStoreCore<KT, VT, IK, IV>>();
        },
        nullptr,
        nullptr),
                                                                       cascade_watcher_ptr(nullptr),
                                                                       cascade_context_ptr(nullptr) {
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
PersistentCascadeStore<KT, VT, IK, IV, ST>::~PersistentCascadeStore() {}

// ========= transaction support code =========

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
PersistentCascadeStore<KT, VT, IK, IV, ST>::CascadeTransactionInternal::CascadeTransactionInternal(
                const transaction_id& txid,
                const std::vector<VT>& write_objects,
                const std::unordered_map<uint32_t,std::vector<std::size_t>>& write_objects_per_shard,
                const std::vector<std::tuple<KT,persistent::version_t,persistent::version_t,persistent::version_t>>& read_objects,
                const std::unordered_map<uint32_t,std::vector<std::size_t>>& read_objects_per_shard,
                const std::vector<uint32_t>& shard_list,
                uint32_t shard_index){
    this->txid = txid; 
    this->shard_list = shard_list;
    this->this_shard_it = std::find(this->shard_list.begin(),this->shard_list.end(),shard_index);

    std::size_t idx = 0;
    for(std::size_t i=0;i<write_objects.size();i++){
        auto& obj = write_objects[i];

        this->write_keys.emplace(obj.get_key_ref());

        if(i == write_objects_per_shard.at(shard_index).at(idx)){
            this->write_objects.emplace_back(obj);
            idx++;
        }
    }
    
    idx = 0;
    for(std::size_t i=0;i<read_objects.size();i++){
        auto& obj = read_objects[i];

        this->read_keys.emplace(std::get<0>(obj));

        if(i == read_objects_per_shard.at(shard_index).at(idx)){
            this->read_objects.emplace_back(obj);
            idx++;
        }
    }
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
transaction_id PersistentCascadeStore<KT, VT, IK, IV, ST>::new_transaction_id(){
    auto shard_index = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index).get_shard_num();
    auto version_and_hlc = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index).get_current_version();
    persistent::version_t current_version = std::get<0>(version_and_hlc);
    return std::make_tuple(this->subgroup_index,shard_index,current_version);
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
void PersistentCascadeStore<KT, VT, IK, IV, ST>::enqueue_transaction(CascadeTransactionInternal* tx){
    transaction_database.emplace(tx->txid,tx);
    versions_checked.emplace(tx,false);
    pending_transactions.push_back(tx);
    tx->queue_it = std::prev(pending_transactions.end());

    // check conflicts with the current queue
    for(auto it=pending_transactions.begin();it != tx->queue_it; it++){
        CascadeTransactionInternal* pending_tx = *it;
        if(tx->conflicts(pending_tx)){
            forward_conflicts[pending_tx].push_back(tx);
            backward_conflicts[tx].push_back(pending_tx);
        }
    }
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
void PersistentCascadeStore<KT, VT, IK, IV, ST>::dequeue_transaction(CascadeTransactionInternal* tx){
    pending_transactions.erase(tx->queue_it); 
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
bool PersistentCascadeStore<KT, VT, IK, IV, ST>::CascadeTransactionInternal::conflicts(CascadeTransactionInternal* other){
    // write keys
    for(auto& key : other->write_keys){
        if((this->write_keys.count(key) > 0) || (this->read_keys.count(key) > 0)){
            return true;
        }
    }

    // read keys 
    for(auto& key : other->read_keys){
        if(this->write_keys.count(key) > 0){
            return true;
        }
    }

    return false;
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
bool PersistentCascadeStore<KT, VT, IK, IV, ST>::CascadeTransactionInternal::conflicts(const KT& key){
    return ((this->write_keys.count(key) > 0) || (this->read_keys.count(key) > 0));
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
bool PersistentCascadeStore<KT, VT, IK, IV, ST>::has_conflict(CascadeTransactionInternal* tx){
    return !((backward_conflicts.count(tx) == 0) || (backward_conflicts[tx].size() == 0));
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
bool PersistentCascadeStore<KT, VT, IK, IV, ST>::has_conflict(const VT& value){
    auto& key = value.get_key_ref();
    for(CascadeTransactionInternal* tx : pending_transactions){
        if(tx->conflicts(key)){
            return true;
        }
    }
    return false;
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
bool PersistentCascadeStore<KT, VT, IK, IV, ST>::check_previous_versions(CascadeTransactionInternal* tx){
    // check versions of to-write objects
    if(!tx->write_objects.empty()){
        if(!this->persistent_core->ordered_check_previous_versions(tx->write_objects, this->persistent_core.getLatestVersion())){
            return false;
        }
    }

    // check versions of read-only objects
    if(!tx->read_objects.empty()){
        if(!this->persistent_core->ordered_check_current_versions(tx->read_objects, this->persistent_core.getLatestVersion())){
            return false;
        }
    }

    return true;
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
void PersistentCascadeStore<KT, VT, IK, IV, ST>::commit_transaction(CascadeTransactionInternal* tx){
    if(tx->write_objects.empty()){
        return;
    }
    
    auto version_and_hlc = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index).get_current_version();
    tx->commit_version = std::get<0>(version_and_hlc);

    // set new version
    for(const VT& value : tx->write_objects){
        if constexpr(std::is_base_of<IKeepVersion, VT>::value) {
            value.set_version(std::get<0>(version_and_hlc));
        }
        if constexpr(std::is_base_of<IKeepTimestamp, VT>::value) {
            value.set_timestamp(std::get<1>(version_and_hlc).m_rtc_us);
        }
    }

    // perform update
    this->persistent_core->ordered_put_objects(tx->write_objects, this->persistent_core.getLatestVersion());

    for(const VT& value : tx->write_objects){
        if(cascade_watcher_ptr) {
            (*cascade_watcher_ptr)(
                    // group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index).get_subgroup_id(), // this is subgroup id
                    this->subgroup_index,
                    group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index).get_shard_num(),
                    group->get_rpc_caller_id(),
                    value.get_key_ref(), value, cascade_context_ptr);
        }
    }
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
void PersistentCascadeStore<KT, VT, IK, IV, ST>::send_tx_forward(
        CascadeTransactionInternal* tx,
        const std::vector<VT>& write_objects,
        const std::unordered_map<uint32_t,std::vector<std::size_t>>& write_objects_per_shard,
        const std::vector<std::tuple<KT,persistent::version_t,persistent::version_t,persistent::version_t>>& read_objects,
        const std::unordered_map<uint32_t,std::vector<std::size_t>>& read_objects_per_shard,
        const std::vector<uint32_t>& shard_list){
    // only one node in the shard passes the TX forward
    auto shard_index = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index).get_shard_num();
    std::vector<std::vector<node_id_t>> subgroup_members = group->template get_subgroup_members<PersistentCascadeStore>(this->subgroup_index);
    std::vector<node_id_t>& shard_members = subgroup_members[shard_index];

    // node with lowest ID forwards the transaction
    if(group->get_my_id() == shard_members[0]){
        // get next shard
        auto it = std::next(tx->this_shard_it);
        auto next_shard_index = *it;

        // TODO should we have other policies to pick the next node?
        node_id_t next_node_id = subgroup_members[next_shard_index][0];

        // p2p_send to the next shard
        auto& subgroup_handle = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index);
        subgroup_handle.template p2p_send<RPC_NAME(put_objects_forward)>(next_node_id,tx->txid,write_objects,write_objects_per_shard,read_objects,read_objects_per_shard,shard_list);
    }
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
void PersistentCascadeStore<KT, VT, IK, IV, ST>::send_tx_status_backward(CascadeTransactionInternal* tx) {
    // only one node in the shard passes the TX forward
    auto shard_index = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index).get_shard_num();
    std::vector<std::vector<node_id_t>> subgroup_members = group->template get_subgroup_members<PersistentCascadeStore>(this->subgroup_index);
    std::vector<node_id_t>& shard_members = subgroup_members[shard_index];

    // node with lowest ID send the transaction backwards
    if(group->get_my_id() == shard_members[0]){
        // get previous shard
        auto it = std::prev(tx->this_shard_it);
        auto next_shard_index = *it;
           
        // TODO should we have other policies to pick the next node?
        node_id_t next_node_id = subgroup_members[next_shard_index][0];

        // p2p_send to the next shard
        auto& subgroup_handle = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index);
        subgroup_handle.template p2p_send<RPC_NAME(put_objects_backward)>(next_node_id,tx->txid,tx->status);
    }
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
void PersistentCascadeStore<KT, VT, IK, IV, ST>::tx_committed_recursive(CascadeTransactionInternal* tx) {
    tx->status = transaction_status_t::COMMIT;
    commit_transaction(tx);
    
    // send status backward if this is not the first shard
    auto shard_index = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index).get_shard_num();
    if(shard_index != tx->shard_list.front()){
        send_tx_status_backward(tx);
    }

    dequeue_transaction(tx);

    // tx was comitted, so we need to abort the TXs that were waiting for it
    for(CascadeTransactionInternal* ahead_tx : forward_conflicts.at(tx)){
        tx_aborted_recursive(ahead_tx);
    }

    forward_conflicts.erase(tx);
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
void PersistentCascadeStore<KT, VT, IK, IV, ST>::tx_aborted_recursive(CascadeTransactionInternal* tx) {
    tx->status = transaction_status_t::ABORT;
    
    // send backwards if this is not the first shard
    auto shard_index = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index).get_shard_num();
    if(shard_index != tx->shard_list.front()){
        send_tx_status_backward(tx);
    }

    dequeue_transaction(tx);

    // tx was aborted, so we need to remove it from conflict and check if a tx can run
    backward_conflicts.erase(tx);
    for(CascadeTransactionInternal* ahead_tx : forward_conflicts.at(tx)){
        // clear conflict
        auto& clist = backward_conflicts.at(ahead_tx);
        clist.erase(std::find(clist.begin(),clist.end(),tx));

        // check if ahead_tx can run
        if(clist.empty()){
            backward_conflicts.erase(ahead_tx);
            tx_run_recursive(ahead_tx);
        }
    }
    
    forward_conflicts.erase(tx);
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
void PersistentCascadeStore<KT, VT, IK, IV, ST>::tx_run_recursive(CascadeTransactionInternal* tx) {
    auto shard_index = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index).get_shard_num();

    // run a tx: check versions and send it forwards or backwards
    if(check_previous_versions(tx)){
        // if this is the last shard, commit
        if(shard_index == tx->shard_list.back()){
            tx_committed_recursive(tx);
        } else {
            // this is not the last, send it forward
            // TODO send_tx_forward(tx->txid,write_objects,write_objects_per_shard,read_objects,read_objects_per_shard,tx->shard_list);
        }
    } else {
        // abort
        tx_aborted_recursive(tx);
    }

    versions_checked.at(tx) = true;
}

}  // namespace cascade
}  // namespace derecho

