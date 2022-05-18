#pragma once

#include "../signature_store.hpp"
#include "cascade/config.h"
#include "cascade/utils.hpp"
#include "debug_util.hpp"
#include "delta_store_core.hpp"

#include <derecho/core/derecho.hpp>

#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <tuple>
#include <vector>

namespace derecho {
namespace cascade {

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
std::tuple<persistent::version_t, uint64_t> SignatureCascadeStore<KT, VT, IK, IV, ST>::put(const VT& value) const {
    // Somehow ensure that only one replica does the UDL here
    debug_enter_func_with_args("value.get_key_ref()={}", value.get_key_ref());
    LOG_TIMESTAMP_BY_TAG(TLT_PERSISTENT_PUT_START, group, value);

    derecho::Replicated<SignatureCascadeStore>& subgroup_handle = group->template get_subgroup<SignatureCascadeStore>(this->subgroup_index);
    auto results = subgroup_handle.template ordered_send<RPC_NAME(ordered_put)>(value);
    auto& replies = results.get();
    std::tuple<persistent::version_t, uint64_t> ret(CURRENT_VERSION, 0);
    for(auto& reply_pair : replies) {
        ret = reply_pair.second.get();
    }

    LOG_TIMESTAMP_BY_TAG(TLT_PERSISTENT_PUT_END, group, value);
    debug_leave_func_with_value("version=0x{:x},timestamp={}", std::get<0>(ret), std::get<1>(ret));
    return ret;
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
void SignatureCascadeStore<KT, VT, IK, IV, ST>::put_and_forget(const VT& value) const {
    debug_enter_func_with_args("value.get_key_ref()={}", value.get_key_ref());
    LOG_TIMESTAMP_BY_TAG(TLT_PERSISTENT_PUT_AND_FORGET_START, group, value);
    derecho::Replicated<SignatureCascadeStore>& subgroup_handle = group->template get_subgroup<SignatureCascadeStore>(this->subgroup_index);
    subgroup_handle.template ordered_send<RPC_NAME(ordered_put_and_forget)>(value);
    LOG_TIMESTAMP_BY_TAG(TLT_PERSISTENT_PUT_AND_FORGET_END, group, value);
    debug_leave_func();
}

#ifdef ENABLE_EVALUATION
template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
double SignatureCascadeStore<KT, VT, IK, IV, ST>::perf_put(const uint32_t max_payload_size, const uint64_t duration_sec) const {
    debug_enter_func_with_args("max_payload_size={},duration_sec={}", max_payload_size, duration_sec);
    derecho::Replicated<SignatureCascadeStore>& subgroup_handle = group->template get_subgroup<SignatureCascadeStore>(this->subgroup_index);
    double ops = internal_perf_put(subgroup_handle, max_payload_size, duration_sec);
    debug_leave_func_with_value("{} ops.", ops);
    return ops;
}
#endif  // ENABLE_EVALUATION

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
std::tuple<persistent::version_t, uint64_t> SignatureCascadeStore<KT, VT, IK, IV, ST>::remove(const KT& key) const {
    debug_enter_func_with_args("key={}", key);
    derecho::Replicated<SignatureCascadeStore>& subgroup_handle = group->template get_subgroup<SignatureCascadeStore>(this->subgroup_index);
    auto results = subgroup_handle.template ordered_send<RPC_NAME(ordered_remove)>(key);
    auto& replies = results.get();
    std::tuple<persistent::version_t, uint64_t> ret(CURRENT_VERSION, 0);
    // TODO: verify consistency ?
    for(auto& reply_pair : replies) {
        ret = reply_pair.second.get();
    }
    debug_leave_func_with_value("version=0x{:x},timestamp={}", std::get<0>(ret), std::get<1>(ret));
    return ret;
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
const VT SignatureCascadeStore<KT, VT, IK, IV, ST>::get(const KT& key, const persistent::version_t& ver, const bool stable, bool exact) const {
    debug_enter_func_with_args("key={},ver=0x{:x},stable={},exact={}", key, ver, stable, exact);
    persistent::version_t requested_version = ver;

    // adjust version if stable is requested.
    if(stable) {
        // Fix: Do not support get(CURRENT_VERSION) on SignatureStore at all. This doesn't make sense anyway.

        // This feature might not be possible to provide: The requested version ver is a data-object
        // version, and subgroup_handle.get_global_persistence_frontier() will return the latest
        // stable *signature object* version
        derecho::Replicated<SignatureCascadeStore>& subgroup_handle = group->template get_subgroup<SignatureCascadeStore>(this->subgroup_index);
        if(requested_version == CURRENT_VERSION) {
            requested_version = subgroup_handle.get_global_persistence_frontier();
        } else {
            // This will wait for the persistence frontier based on the wrong version (a signature version, not a data version)
            // Fix: Translate it first
            if(!subgroup_handle.wait_for_global_persistence_frontier(requested_version) && requested_version > persistent_core.getLatestVersion()) {
                // INVALID version
                dbg_default_debug("{}: requested version:{:x} is beyond the latest atomic broadcast version.", __PRETTY_FUNCTION__, requested_version);
                return *IV;
            }
        }
    }

    if(requested_version == CURRENT_VERSION) {
        debug_leave_func_with_value("lockless_get({})", key);
        return persistent_core->lockless_get(key);
    } else {
        // Translate ver from a data-object version to its corresponding signature-object version
        persistent::version_t hash_version;
        {
            std::lock_guard<std::mutex> map_lock(version_map_mutex);
            auto version_map_search = (*data_to_hash_version).upper_bound(ver);
            if(version_map_search != (*data_to_hash_version).begin()) {
                version_map_search--;
                // The search iterator now points to the largest version <= ver, which is what we want
                if(version_map_search->first == requested_version || !exact) {
                    hash_version = version_map_search->second;
                } else {
                    debug_leave_func_with_value("invalid object; version 0x{:x} did not match with exact search", version_map_search->first);
                    return *IV;
                }
            } else {
                // The map is empty, so no objects have yet been stored here
                debug_leave_func();
                return *IV;
            }
        }
        debug_leave_func_with_value("corresponding hash ver=0x{:x}", hash_version);
        return persistent_core.template getDelta<VT>(hash_version, exact, [&key, hash_version, exact, this](const VT& v) {
            if(key == v.get_key_ref()) {
                return v;
            } else {
                if(exact) {
                    // return invalid object for EXACT search.
                    return *IV;
                } else {
                    // fall back to the slow path.
                    auto versioned_state_ptr = persistent_core.get(hash_version);
                    if(versioned_state_ptr->kv_map.find(key) != versioned_state_ptr->kv_map.end()) {
                        return versioned_state_ptr->kv_map.at(key);
                    }
                    return *IV;
                }
            }
        });
    }
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
const VT SignatureCascadeStore<KT, VT, IK, IV, ST>::multi_get(const KT& key) const {
    debug_enter_func_with_args("key={}", key);
    derecho::Replicated<SignatureCascadeStore>& subgroup_handle = group->template get_subgroup<SignatureCascadeStore>(this->subgroup_index);
    auto results = subgroup_handle.template ordered_send<RPC_NAME(ordered_get)>(key);
    auto& replies = results.get();
    for(auto& reply_pair : replies) {
        reply_pair.second.wait();
    }
    debug_leave_func();
    return replies.begin()->second.get();
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
const VT SignatureCascadeStore<KT, VT, IK, IV, ST>::get_by_time(const KT& key, const uint64_t& ts_us, const bool stable) const {
    debug_enter_func_with_args("key={},ts_us={}", key, ts_us);
    const HLC hlc(ts_us, 0ull);
    derecho::Replicated<SignatureCascadeStore>& subgroup_handle = group->template get_subgroup<SignatureCascadeStore>(this->subgroup_index);
    // get_global_stability_frontier return nano seconds.
    if(ts_us > subgroup_handle.compute_global_stability_frontier() / 1000) {
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
uint64_t SignatureCascadeStore<KT, VT, IK, IV, ST>::multi_get_size(const KT& key) const {
    debug_enter_func_with_args("key={}", key);
    derecho::Replicated<SignatureCascadeStore>& subgroup_handle = group->template get_subgroup<SignatureCascadeStore>(this->subgroup_index);
    auto results = subgroup_handle.template ordered_send<RPC_NAME(ordered_get_size)>(key);
    auto& replies = results.get();
    // TODO: verify consistency ?
    // for (auto& reply_pair : replies) {
    //     ret = reply_pair.second.get();
    // }
    debug_leave_func();
    return replies.begin()->second.get();
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
uint64_t SignatureCascadeStore<KT, VT, IK, IV, ST>::get_size(const KT& key, const persistent::version_t& ver, const bool stable, bool exact) const {
    debug_enter_func_with_args("key={},ver=0x{:x},stable={},exact={}", key, ver, stable, exact);
    persistent::version_t requested_version = ver;
    // adjust version if stable is requested.
    if(stable) {
        derecho::Replicated<SignatureCascadeStore>& subgroup_handle = group->template get_subgroup<SignatureCascadeStore>(this->subgroup_index);
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
                return 0ull;
            }
        }
    }
    if(requested_version == CURRENT_VERSION) {
        // return the unstable query
        debug_leave_func_with_value("lockless_get_size({})", key);
        return persistent_core->lockless_get_size(key);
    } else {
        return persistent_core.template getDelta<VT>(requested_version, exact, [this, key, requested_version, exact](const VT& v) -> uint64_t {
            if(key == v.get_key_ref()) {
                debug_leave_func_with_value("key:{} is found at version:0x{:x}", key, requested_version);
                return mutils::bytes_size(v);
            } else {
                if(exact) {
                    // return invalid object for EXACT search.
                    debug_leave_func_with_value("No data found for key:{} at version:0x{:x}", key, requested_version);
                    return 0ull;
                } else {
                    // fall back to the slow path.
                    auto versioned_state_ptr = persistent_core.get(requested_version);
                    if(versioned_state_ptr->kv_map.find(key) != versioned_state_ptr->kv_map.end()) {
                        debug_leave_func_with_value("Reconstructed version:0x{:x} for key:{}", requested_version, key);
                        return mutils::bytes_size(versioned_state_ptr->kv_map.at(key));
                    }
                    debug_leave_func_with_value("No data found for key:{} before version:0x{:x}", key, requested_version);
                    return 0ull;
                }
            }
        });
    }
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
uint64_t SignatureCascadeStore<KT, VT, IK, IV, ST>::get_size_by_time(const KT& key, const uint64_t& ts_us, const bool stable) const {
    debug_enter_func_with_args("key={},ts_us={},stable={}", key, ts_us, stable);
    const HLC hlc(ts_us, 0ull);

    derecho::Replicated<SignatureCascadeStore>& subgroup_handle = group->template get_subgroup<SignatureCascadeStore>(this->subgroup_index);

    // get_global_stability_frontier return nano seconds.
    if(ts_us > subgroup_handle.compute_global_stability_frontier() / 1000) {
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
std::vector<KT> SignatureCascadeStore<KT, VT, IK, IV, ST>::multi_list_keys(const std::string& prefix) const {
    debug_enter_func_with_args("prefix={}.", prefix);
    derecho::Replicated<SignatureCascadeStore>& subgroup_handle = group->template get_subgroup<SignatureCascadeStore>(this->subgroup_index);
    auto results = subgroup_handle.template ordered_send<RPC_NAME(ordered_list_keys)>(prefix);
    auto& replies = results.get();
    // TODO: verify consistency ?
    debug_leave_func();
    return replies.begin()->second.get();
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
std::vector<KT> SignatureCascadeStore<KT, VT, IK, IV, ST>::list_keys(const std::string& prefix, const persistent::version_t& ver, const bool stable) const {
    debug_enter_func_with_args("prefix={}, ver=0x{:x}, stable={}", prefix, ver, stable);
    persistent::version_t requested_version = ver;
    if(stable) {
        derecho::Replicated<SignatureCascadeStore>& subgroup_handle = group->template get_subgroup<SignatureCascadeStore>(this->subgroup_index);
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
                return {};
            }
        }
    }

    if(requested_version == CURRENT_VERSION) {
        // return the unstable query
        debug_leave_func_with_value("lockless_list_prefix({})", prefix);
        return persistent_core->lockless_list_keys(prefix);
    } else {
        std::vector<KT> keys;
        persistent_core.get(requested_version, [&keys, &prefix](const DeltaCascadeStoreCore<KT, VT, IK, IV>& pers_core) {
            for(const auto& kv : pers_core.kv_map) {
                if(get_pathname<KT>(kv.first).find(prefix) == 0) {
                    keys.push_back(kv.first);
                }
            }
        });
        return keys;
    }
}
template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
std::vector<KT> SignatureCascadeStore<KT, VT, IK, IV, ST>::list_keys_by_time(const std::string& prefix, const uint64_t& ts_us, const bool stable) const {
    debug_enter_func_with_args("ts_us={}", ts_us);
    const HLC hlc(ts_us, 0ull);

    derecho::Replicated<SignatureCascadeStore>& subgroup_handle = group->template get_subgroup<SignatureCascadeStore>(this->subgroup_index);

    // get_global_stability_frontier return nano seconds.
    if(ts_us > subgroup_handle.compute_global_stability_frontier() / 1000) {
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
std::tuple<persistent::version_t, uint64_t> SignatureCascadeStore<KT, VT, IK, IV, ST>::ordered_put(const VT& value) {
    debug_enter_func_with_args("key={}", value.get_key_ref());

    std::tuple<persistent::version_t, uint64_t> version_and_timestamp = group->template get_subgroup<SignatureCascadeStore>(this->subgroup_index).get_current_version();
#if __cplusplus > 201703L
    LOG_TIMESTAMP_BY_TAG(TLT_PERSISTENT_ORDERED_PUT_START, group, value, std::get<0>(version_and_timestamp));
#else
    LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_PERSISTENT_ORDERED_PUT_START, group, value, std::get<0>(version_and_timestamp));
#endif
    if(this->internal_ordered_put(value) == false) {
        version_and_timestamp = {persistent::INVALID_VERSION, 0};
    }

#if __cplusplus > 201703L
    LOG_TIMESTAMP_BY_TAG(TLT_PERSISTENT_ORDERED_PUT_END, group, value, std::get<0>(version_and_timestamp));
#else
    LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_PERSISTENT_ORDERED_PUT_END, group, value, std::get<0>(version_and_timestamp));
#endif
    debug_leave_func_with_value("version=0x{:x},timestamp={}", std::get<0>(version_and_timestamp), std::get<1>(version_and_timestamp));

    return version_and_timestamp;
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
void SignatureCascadeStore<KT, VT, IK, IV, ST>::ordered_put_and_forget(const VT& value) {
    debug_enter_func_with_args("key={}", value.get_key_ref());
#ifdef ENABLE_EVALUATION
    std::tuple<persistent::version_t, uint64_t> version_and_timestamp = group->template get_subgroup<SignatureCascadeStore>(this->subgroup_index).get_current_version();
#endif

#if __cplusplus > 201703L
    LOG_TIMESTAMP_BY_TAG(TLT_PERSISTENT_ORDERED_PUT_AND_FORGET_START, group, value, std::get<0>(version_and_timestamp));
#else
    LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_PERSISTENT_ORDERED_PUT_AND_FORGET_START, group, value, std::get<0>(version_and_timestamp));
#endif

    this->internal_ordered_put(value);

#if __cplusplus > 201703L
    LOG_TIMESTAMP_BY_TAG(TLT_PERSISTENT_ORDERED_PUT_AND_FORGET_END, group, value, std::get<0>(version_and_timestamp));
#else
    LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_PERSISTENT_ORDERED_PUT_AND_FORGET_END, group, value, std::get<0>(version_and_timestamp));
#endif

#ifdef ENABLE_EVALUATION
    // avoid unused variable warning.
    if constexpr(!std::is_base_of<IHasMessageID, VT>::value) {
        version_and_timestamp = version_and_timestamp;
    }
#endif
    debug_leave_func();
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
bool SignatureCascadeStore<KT, VT, IK, IV, ST>::internal_ordered_put(const VT& value) {
    static_assert(std::is_base_of_v<IKeepVersion, VT>, "SignatureCascadeStore can only be used with values that implement IKeepVersion");
    std::tuple<persistent::version_t, uint64_t> hash_object_version_and_timestamp
            = group->template get_subgroup<SignatureCascadeStore>(this->subgroup_index).get_current_version();
    // Assume the input object's version field is currently set to its corresponding data object's version
    persistent::version_t data_object_version = value.get_version();
    value.set_version(std::get<0>(hash_object_version_and_timestamp));
    if constexpr(std::is_base_of<IKeepTimestamp, VT>::value) {
        value.set_timestamp(std::get<1>(hash_object_version_and_timestamp));
    }
    // Store the mapping
    {
        std::lock_guard<std::mutex> map_lock(version_map_mutex);
        data_to_hash_version->emplace(data_object_version, std::get<0>(hash_object_version_and_timestamp));
    }
    if(this->persistent_core->ordered_put(value, this->persistent_core.getLatestVersion()) == false) {
        // verification failed. So we return invalid versions.
        debug_leave_func_with_value("version=0x{:x},timestamp={}", persistent::INVALID_VERSION, 0);
        return false;
    }
    if(cascade_watcher_ptr) {
        (*cascade_watcher_ptr)(
                this->subgroup_index,
                group->template get_subgroup<SignatureCascadeStore>(this->subgroup_index).get_shard_num(),
                group->get_rpc_caller_id(),
                value.get_key_ref(), value,
                cascade_context_ptr);
    }
    debug_leave_func_with_value("version=0x{:x},timestamp={}", std::get<0>(hash_object_version_and_timestamp), std::get<1>(hash_object_version_and_timestamp));
    return true;
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
std::tuple<persistent::version_t, uint64_t> SignatureCascadeStore<KT, VT, IK, IV, ST>::ordered_remove(const KT& key) {
    debug_enter_func_with_args("key={}", key);
    std::tuple<persistent::version_t, uint64_t> version_and_timestamp = group->template get_subgroup<SignatureCascadeStore>(this->subgroup_index).get_current_version();
    auto value = create_null_object_cb<KT, VT, IK, IV>(key);
    if constexpr(std::is_base_of<IKeepVersion, VT>::value) {
        value.set_version(std::get<0>(version_and_timestamp));
    }
    if constexpr(std::is_base_of<IKeepTimestamp, VT>::value) {
        value.set_timestamp(std::get<1>(version_and_timestamp));
    }
    if(this->persistent_core->ordered_remove(value, this->persistent_core.getLatestVersion())) {
        if(cascade_watcher_ptr) {
            (*cascade_watcher_ptr)(
                    this->subgroup_index,
                    group->template get_subgroup<SignatureCascadeStore>(this->subgroup_index).get_shard_num(),
                    group->get_rpc_caller_id(),
                    key, value,
                    cascade_context_ptr);
        }
    }

    debug_leave_func_with_value("version=0x{:x},timestamp={}", std::get<0>(version_and_timestamp), std::get<1>(version_and_timestamp));

    return version_and_timestamp;
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
const VT SignatureCascadeStore<KT, VT, IK, IV, ST>::ordered_get(const KT& key) {
    debug_enter_func_with_args("key={}", key);

    debug_leave_func();

    return this->persistent_core->ordered_get(key);
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
uint64_t SignatureCascadeStore<KT, VT, IK, IV, ST>::ordered_get_size(const KT& key) {
    debug_enter_func_with_args("key={}", key);

    debug_leave_func();

    return this->persistent_core->ordered_get_size(key);
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
void SignatureCascadeStore<KT, VT, IK, IV, ST>::trigger_put(const VT& value) const {
    debug_enter_func_with_args("key={}", value.get_key_ref());

    if(cascade_watcher_ptr) {
        (*cascade_watcher_ptr)(
                this->subgroup_index,
                group->template get_subgroup<SignatureCascadeStore<KT, VT, IK, IV, ST>>(this->subgroup_index).get_shard_num(),
                group->get_rpc_caller_id(),
                value.get_key_ref(), value,
                cascade_context_ptr, true);
    }

    debug_leave_func();
}

#ifdef ENABLE_EVALUATION
template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
void SignatureCascadeStore<KT, VT, IK, IV, ST>::dump_timestamp_log(const std::string& filename) const {
    derecho::Replicated<SignatureCascadeStore>& subgroup_handle = group->template get_subgroup<SignatureCascadeStore>(this->subgroup_index);
    auto result = subgroup_handle.template ordered_send<RPC_NAME(ordered_dump_timestamp_log)>(filename);
    auto& replies = result.get();
    for(auto r : replies) {
        volatile uint32_t _ = r;
        _ = _;
    }
    return;
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
void SignatureCascadeStore<KT, VT, IK, IV, ST>::ordered_dump_timestamp_log(const std::string& filename) {
    global_timestamp_logger.flush(filename);
}
#ifdef DUMP_TIMESTAMP_WORKAROUND
template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
void SignatureCascadeStore<KT, VT, IK, IV, ST>::dump_timestamp_log_workaround(const std::string& filename) const {
    debug_enter_func_with_args("filename={}", filename);
    global_timestamp_logger.flush(filename);
    debug_leave_func();
}
#endif  // DUMP_TIMESTAMP_WORKAROUND
#endif  // ENABLE_EVALUATION

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
std::vector<KT> SignatureCascadeStore<KT, VT, IK, IV, ST>::ordered_list_keys(const std::string& prefix) {
    debug_enter_func();

    debug_leave_func();

    return this->persistent_core->ordered_list_keys(prefix);
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
std::unique_ptr<SignatureCascadeStore<KT, VT, IK, IV, ST>> SignatureCascadeStore<KT, VT, IK, IV, ST>::from_bytes(mutils::DeserializationManager* dsm, uint8_t const* buf) {
    auto persistent_core_ptr = mutils::from_bytes<persistent::Persistent<DeltaCascadeStoreCore<KT, VT, IK, IV>, ST>>(dsm, buf);
    std::size_t persistent_core_size = mutils::bytes_size(*persistent_core_ptr);
    auto version_map_ptr = mutils::from_bytes<persistent::Persistent<std::map<persistent::version_t, const persistent::version_t>>>(dsm, buf + persistent_core_size);
    auto persistent_cascade_store_ptr
            = std::make_unique<SignatureCascadeStore>(std::move(*persistent_core_ptr),
                                                      std::move(*version_map_ptr),
                                                      dsm->registered<CriticalDataPathObserver<SignatureCascadeStore<KT, VT, IK, IV>>>() ? &(dsm->mgr<CriticalDataPathObserver<SignatureCascadeStore<KT, VT, IK, IV>>>()) : nullptr,
                                                      dsm->registered<ICascadeContext>() ? &(dsm->mgr<ICascadeContext>()) : nullptr);
    return persistent_cascade_store_ptr;
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
SignatureCascadeStore<KT, VT, IK, IV, ST>::SignatureCascadeStore(
        persistent::PersistentRegistry* pr,
        CriticalDataPathObserver<SignatureCascadeStore<KT, VT, IK, IV>>* cw,
        ICascadeContext* cc)
        : persistent_core(pr, true),  // enable signatures
          data_to_hash_version(pr, false),
          cascade_watcher_ptr(cw),
          cascade_context_ptr(cc) {}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
SignatureCascadeStore<KT, VT, IK, IV, ST>::SignatureCascadeStore(
        persistent::Persistent<DeltaCascadeStoreCore<KT, VT, IK, IV>, ST>&& deserialized_persistent_core,
        persistent::Persistent<std::map<persistent::version_t, const persistent::version_t>>&& deserialized_data_to_hash_version,
        CriticalDataPathObserver<SignatureCascadeStore<KT, VT, IK, IV>>* cw,
        ICascadeContext* cc)
        : persistent_core(std::move(deserialized_persistent_core)),
          data_to_hash_version(std::move(deserialized_data_to_hash_version)),
          cascade_watcher_ptr(cw),
          cascade_context_ptr(cc) {}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
SignatureCascadeStore<KT, VT, IK, IV, ST>::SignatureCascadeStore()
        : persistent_core(
                []() {
                    return std::make_unique<DeltaCascadeStoreCore<KT, VT, IK, IV>>();
                },
                nullptr,
                nullptr),  // I'm guessing the dummy version doesn't need to enable signatures on persistent_core
          data_to_hash_version(nullptr),
          cascade_watcher_ptr(nullptr),
          cascade_context_ptr(nullptr) {}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
SignatureCascadeStore<KT, VT, IK, IV, ST>::~SignatureCascadeStore() {}

/* --- New methods only in SignatureCascadeStore, not copied from PersistentCascadeStore --- */

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
std::tuple<std::vector<uint8_t>, persistent::version_t> SignatureCascadeStore<KT, VT, IK, IV, ST>::get_signature(
        const KT& key,
        const persistent::version_t& ver,
        bool exact) const {
    debug_enter_func_with_args("key={},ver=0x{:x}", key, ver);
    if(ver != CURRENT_VERSION) {
        // Translate ver from a data-object version to its corresponding signature-object version
        persistent::version_t hash_version;
        {
            std::lock_guard<std::mutex> map_lock(version_map_mutex);
            auto version_map_search = (*data_to_hash_version).upper_bound(ver);
            if(version_map_search != (*data_to_hash_version).begin()) {
                version_map_search--;
                // The search iterator now points to the largest version <= ver, which is what we want
                if(version_map_search->first == ver || !exact) {
                    hash_version = version_map_search->second;
                } else {
                    debug_leave_func_with_value("invalid signature; version 0x{:x} did not match with exact search", version_map_search->first);
                    return {std::vector<uint8_t>{}, persistent::INVALID_VERSION};
                }
            } else {
                // The map is empty, so no objects have yet been stored here
                debug_leave_func();
                return {std::vector<uint8_t>{}, persistent::INVALID_VERSION};
            }
        }
        std::vector<uint8_t> signature(persistent_core.getSignatureSize());
        persistent::version_t previous_signed_version;
        // Hopefully, the user kept track of which log version corresponded to the "put" for this key,
        // and the entry at the requested version is an object with the correct key
        bool signature_found = persistent_core.template getDeltaSignature<VT>(
                hash_version, [&key](const VT& deltaEntry) {
                    return deltaEntry.get_key_ref() == key;
                },
                signature.data(), previous_signed_version);
        // If an inexact match is requested, we need to search backward until we find the newest entry
        // prior to hash_version that contains the requested key. This is slow, but I can't think of a better way.
        if(!signature_found && !exact) {
            dbg_default_debug("get_signature: Inexact match requested, searching for {} at version 0x{:x}", key, hash_version);
            persistent::version_t search_ver = hash_version - 1;
            while(search_ver > 0 && !signature_found) {
                signature_found = persistent_core.template getDeltaSignature<VT>(
                        search_ver, [&key](const VT& deltaEntry) {
                            return deltaEntry.get_key_ref() == key;
                        },
                        signature.data(), previous_signed_version);
                search_ver--;
            }
        }
        debug_leave_func();
        if(signature_found) {
            return {signature, previous_signed_version};
        } else {
            return {std::vector<uint8_t>{}, persistent::INVALID_VERSION};
        }
    }
    derecho::Replicated<SignatureCascadeStore>& subgroup_handle = group->template get_subgroup<SignatureCascadeStore>(this->subgroup_index);
    auto results = subgroup_handle.template ordered_send<RPC_NAME(ordered_get_signature)>(key);
    auto& replies = results.get();
    debug_leave_func();
    return replies.begin()->second.get();
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
std::tuple<std::vector<uint8_t>, persistent::version_t> SignatureCascadeStore<KT, VT, IK, IV, ST>::get_signature_by_version(
        const persistent::version_t& ver) const {
    debug_enter_func_with_args("ver=0x{:x}", ver);
    if(ver == CURRENT_VERSION) {
        debug_leave_func_with_value("get_signature_by_version does not yet support CURRENT_VERSION ({})", CURRENT_VERSION);
        return {std::vector<uint8_t>{}, persistent::INVALID_VERSION};
    }
    std::vector<uint8_t> signature(persistent_core.getSignatureSize());
    persistent::version_t previous_signed_version;
    if(persistent_core.getSignature(ver, signature.data(), previous_signed_version)) {
        debug_leave_func_with_value("signature found, previous_signed_version=0x{:x}", previous_signed_version);
        return {signature, previous_signed_version};
    } else {
        debug_leave_func();
        return {std::vector<uint8_t>{}, persistent::INVALID_VERSION};
    }
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
std::tuple<std::vector<uint8_t>, persistent::version_t> SignatureCascadeStore<KT, VT, IK, IV, ST>::ordered_get_signature(
        const KT& key) {
    debug_enter_func_with_args("key={}", key);
    // If the requested key isn't in the map, return an empty signature
    if(persistent_core->kv_map.find(key) == persistent_core->kv_map.end()) {
        debug_leave_func();
        return {std::vector<uint8_t>{}, persistent::INVALID_VERSION};
    }

    persistent::version_t current_signed_version = persistent_core.getLastPersistedVersion();
    // The latest entry in the log might not relate to the key we are looking for, so
    // we need to traverse backward until we find the newest entry that is a "put" for that key
    std::vector<uint8_t> signature(persistent_core.getSignatureSize());
    persistent::version_t previous_signed_version = persistent::INVALID_VERSION;
    bool signature_found = false;
    while(!signature_found) {
        // This must work eventually, since the key is in the map
        dbg_default_debug("ordered_get_signature: Looking for signature at version 0x{:x}", current_signed_version);
        signature_found = persistent_core.template getDeltaSignature<VT>(
                current_signed_version,
                [&key](const VT& deltaEntry) {
                    return deltaEntry.get_key_ref() == key;
                },
                signature.data(), previous_signed_version);
        current_signed_version--;
    }

    debug_leave_func();
    return {signature, previous_signed_version};
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
void SignatureCascadeStore<KT, VT, IK, IV, ST>::request_notification(node_id_t external_client_id, persistent::version_t ver) const {
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
void SignatureCascadeStore<KT, VT, IK, IV, ST>::subscribe_to_notifications(node_id_t external_client_id, const KT& key) const {
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
void SignatureCascadeStore<KT, VT, IK, IV, ST>::subscribe_to_all_notifications(node_id_t external_client_id) const {
}

}  // namespace cascade
}  // namespace derecho