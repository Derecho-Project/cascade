#pragma once

#include "../signature_store.hpp"
#include "cascade/cascade_notification_message.hpp"
#include "cascade/config.h"
#include "cascade/utils.hpp"
#include "debug_util.hpp"
#include "delta_store_core.hpp"

#include <derecho/core/derecho.hpp>
#include <derecho/mutils-serialization/SerializationSupport.hpp>

#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <tuple>
#include <vector>

namespace derecho {
namespace cascade {

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
version_tuple SignatureCascadeStore<KT, VT, IK, IV, ST>::put(const VT& value) const {
    // Somehow ensure that only one replica does the UDL here
    debug_enter_func_with_args("value.get_key_ref()={}", value.get_key_ref());
    LOG_TIMESTAMP_BY_TAG(TLT_SIGNATURE_PUT_START, group, value);

    derecho::Replicated<SignatureCascadeStore>& subgroup_handle = group->template get_subgroup<SignatureCascadeStore>(this->subgroup_index);
    auto results = subgroup_handle.template ordered_send<RPC_NAME(ordered_put)>(value);
    auto& replies = results.get();
    version_tuple ret(CURRENT_VERSION, 0, CURRENT_VERSION, CURRENT_VERSION);
    for(auto& reply_pair : replies) {
        ret = reply_pair.second.get();
    }

    LOG_TIMESTAMP_BY_TAG(TLT_SIGNATURE_PUT_END, group, value);
    debug_leave_func_with_value("version=0x{:x},timestamp={},previous_version=0x{:x},previous_version_by_key=0x{:x}",
                                std::get<0>(ret), std::get<1>(ret), std::get<2>(ret), std::get<3>(ret));
    return ret;
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
void SignatureCascadeStore<KT, VT, IK, IV, ST>::put_and_forget(const VT& value) const {
    debug_enter_func_with_args("value.get_key_ref()={}", value.get_key_ref());
    LOG_TIMESTAMP_BY_TAG(TLT_SIGNATURE_PUT_AND_FORGET_START, group, value);
    derecho::Replicated<SignatureCascadeStore>& subgroup_handle = group->template get_subgroup<SignatureCascadeStore>(this->subgroup_index);
    subgroup_handle.template ordered_send<RPC_NAME(ordered_put_and_forget)>(value);
    LOG_TIMESTAMP_BY_TAG(TLT_SIGNATURE_PUT_AND_FORGET_END, group, value);
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
version_tuple SignatureCascadeStore<KT, VT, IK, IV, ST>::remove(const KT& key) const {
    debug_enter_func_with_args("key={}", key);
    LOG_TIMESTAMP_BY_TAG(TLT_SIGNATURE_REMOVE_START, group, *IV);

    derecho::Replicated<SignatureCascadeStore>& subgroup_handle = group->template get_subgroup<SignatureCascadeStore>(this->subgroup_index);
    auto results = subgroup_handle.template ordered_send<RPC_NAME(ordered_remove)>(key);
    auto& replies = results.get();
    version_tuple ret(CURRENT_VERSION, 0, CURRENT_VERSION, CURRENT_VERSION);
    // TODO: verify consistency ?
    for(auto& reply_pair : replies) {
        ret = reply_pair.second.get();
    }
    LOG_TIMESTAMP_BY_TAG(TLT_SIGNATURE_REMOVE_END, group, *IV);
    debug_leave_func_with_value("version=0x{:x},timestamp={},previous_version=0x{:x},previous_version_by_key=0x{:x}",
                                std::get<0>(ret), std::get<1>(ret), std::get<2>(ret), std::get<3>(ret));
    return ret;
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
const VT SignatureCascadeStore<KT, VT, IK, IV, ST>::get(const KT& key, const persistent::version_t& ver, const bool stable, bool exact) const {
    debug_enter_func_with_args("key={},ver=0x{:x},stable={},exact={}", key, ver, stable, exact);
#if __cplusplus > 201703L
    LOG_TIMESTAMP_BY_TAG(TLT_SIGNATURE_GET_START, group, *IV, ver);
#else
    LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_SIGNATURE_GET_START, group, *IV, ver);
#endif

    if(ver == CURRENT_VERSION) {
        dbg_default_error("SignatureCascadeStore only supports get() with specific version, not CURRENT_VERSION");
        debug_leave_func();
        return *IV;
    }
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
                debug_leave_func_with_value("invalid object; version 0x{:x} did not match with exact search", version_map_search->first);
#if __cplusplus > 201703L
                LOG_TIMESTAMP_BY_TAG(TLT_SIGNATURE_GET_END, group, *IV, ver);
#else
                LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_SIGNATURE_GET_END, group, *IV, ver);
#endif
                return *IV;
            }
        } else {
            // The map is empty, so no objects have yet been stored here
            debug_leave_func();
            return *IV;
        }
    }

    if(stable) {
        derecho::Replicated<SignatureCascadeStore>& subgroup_handle = group->template get_subgroup<SignatureCascadeStore>(this->subgroup_index);
        // Wait for the requested signature object to be globally persisted
        if(!subgroup_handle.wait_for_global_persistence_frontier(hash_version)
           && hash_version > persistent_core.getLatestVersion()) {
            // INVALID version
            dbg_default_debug("{}: requested version:{:x} is beyond the latest atomic broadcast version.", __PRETTY_FUNCTION__, hash_version);
#if __cplusplus > 201703L
            LOG_TIMESTAMP_BY_TAG(TLT_SIGNATURE_GET_END, group, *IV, ver);
#else
            LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_SIGNATURE_GET_END, group, *IV, ver);
#endif
            return *IV;
        }
    }
    dbg_default_debug("corresponding hash ver=0x{:x}", hash_version);
    return persistent_core.template getDelta<VT>(hash_version, exact, [&key, ver, hash_version, exact, this](const VT& v) {
        if(key == v.get_key_ref()) {
            return v;
        } else {
            if(exact) {
                // return invalid object for EXACT search.
                debug_leave_func_with_value("No hash object found for key {} at version 0x{:x}", key, hash_version);
#if __cplusplus > 201703L
                LOG_TIMESTAMP_BY_TAG(TLT_SIGNATURE_GET_END, group, *IV, ver);
#else
                LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_SIGNATURE_GET_END, group, *IV, ver);
#endif
                return *IV;
            } else {
                // fall back to the slow path.
                auto versioned_state_ptr = persistent_core.get(hash_version);
                if(versioned_state_ptr->kv_map.find(key) != versioned_state_ptr->kv_map.end()) {
                    debug_leave_func_with_value("Reconstructed version 0x{:x} for hash object with key {}", hash_version, key);
#if __cplusplus > 201703L
                    LOG_TIMESTAMP_BY_TAG(TLT_SIGNATURE_GET_END, group, *IV, ver);
#else
                    LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_SIGNATURE_GET_END, group, *IV, ver);
#endif

                    return versioned_state_ptr->kv_map.at(key);
                }
                debug_leave_func_with_value("No hash object found for key {} before version 0x{:x}", key, hash_version);
                return *IV;
            }
        }
    });
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
const VT SignatureCascadeStore<KT, VT, IK, IV, ST>::multi_get(const KT& key) const {
    debug_enter_func_with_args("key={}", key);
    LOG_TIMESTAMP_BY_TAG(TLT_SIGNATURE_MULTI_GET_START, group, *IV);
    dbg_default_warn("WARNING: multi_get({}) called on SignatureCascadeStore. This will return the current version of the signed hash object, which may not correspond to the current version of the data object in PersistentCascadeStore", key);
    derecho::Replicated<SignatureCascadeStore>& subgroup_handle = group->template get_subgroup<SignatureCascadeStore>(this->subgroup_index);
    auto results = subgroup_handle.template ordered_send<RPC_NAME(ordered_get)>(key);
    auto& replies = results.get();
    for(auto& reply_pair : replies) {
        reply_pair.second.wait();
    }
    LOG_TIMESTAMP_BY_TAG(TLT_SIGNATURE_MULTI_GET_END, group, *IV);
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
#if __cplusplus > 201703L
    LOG_TIMESTAMP_BY_TAG(TLT_SIGNATURE_GET_SIZE_START, group, *IV, ver);
#else
    LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_SIGNATURE_GET_SIZE_START, group, *IV, ver);
#endif
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
        auto rvo_val = persistent_core->lockless_get_size(key);
#if __cplusplus > 201703L
        LOG_TIMESTAMP_BY_TAG(TLT_SIGNATURE_GET_SIZE_END, group, *IV, ver);
#else
        LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_SIGNATURE_GET_SIZE_END, group, *IV, ver);
#endif
        return rvo_val;
    } else {
        return persistent_core.template getDelta<VT>(requested_version, exact, [this, key, ver, requested_version, exact](const VT& v) -> uint64_t {
            if(key == v.get_key_ref()) {
                debug_leave_func_with_value("key:{} is found at version:0x{:x}", key, requested_version);
                uint64_t size = mutils::bytes_size(v);
#if __cplusplus > 201703L
                LOG_TIMESTAMP_BY_TAG(TLT_SIGNATURE_GET_SIZE_END, group, *IV, ver);
#else
                LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_SIGNATURE_GET_SIZE_END, group, *IV, ver);
#endif
                return size;
            } else {
                if(exact) {
                    // return invalid object for EXACT search.
                    debug_leave_func_with_value("No data found for key:{} at version:0x{:x}", key, requested_version);
#if __cplusplus > 201703L
                    LOG_TIMESTAMP_BY_TAG(TLT_SIGNATURE_GET_SIZE_END, group, *IV, ver);
#else
                    LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_SIGNATURE_GET_SIZE_END, group, *IV, ver);
#endif
                    return 0ull;
                } else {
                    // fall back to the slow path.
                    auto versioned_state_ptr = persistent_core.get(requested_version);
                    if(versioned_state_ptr->kv_map.find(key) != versioned_state_ptr->kv_map.end()) {
                        debug_leave_func_with_value("Reconstructed version:0x{:x} for key:{}", requested_version, key);
                        uint64_t size = mutils::bytes_size(versioned_state_ptr->kv_map.at(key));
#if __cplusplus > 201703L
                        LOG_TIMESTAMP_BY_TAG(TLT_SIGNATURE_GET_SIZE_END, group, *IV, ver);
#else
                        LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_SIGNATURE_GET_SIZE_END, group, *IV, ver);
#endif
                        return size;
                    }
                    debug_leave_func_with_value("No data found for key:{} before version:0x{:x}", key, requested_version);
#if __cplusplus > 201703L
                    LOG_TIMESTAMP_BY_TAG(TLT_SIGNATURE_GET_SIZE_END, group, *IV, ver);
#else
                    LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_SIGNATURE_GET_SIZE_END, group, *IV, ver);
#endif
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
    LOG_TIMESTAMP_BY_TAG(TLT_SIGNATURE_MULTI_LIST_KEYS_START, group, *IV);

    derecho::Replicated<SignatureCascadeStore>& subgroup_handle = group->template get_subgroup<SignatureCascadeStore>(this->subgroup_index);
    auto results = subgroup_handle.template ordered_send<RPC_NAME(ordered_list_keys)>(prefix);
    auto& replies = results.get();
    // TODO: verify consistency ?
    auto rvo_val = replies.begin()->second.get();
    LOG_TIMESTAMP_BY_TAG(TLT_SIGNATURE_MULTI_LIST_KEYS_END, group, *IV);
    debug_leave_func();
    return rvo_val;
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
std::vector<KT> SignatureCascadeStore<KT, VT, IK, IV, ST>::list_keys(const std::string& prefix, const persistent::version_t& ver, const bool stable) const {
    debug_enter_func_with_args("prefix={}, ver=0x{:x}, stable={}", prefix, ver, stable);
#if __cplusplus > 201703L
    LOG_TIMESTAMP_BY_TAG(TLT_SIGNATURE_LIST_KEYS_START, group, *IV, ver);
#else
    LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_SIGNATURE_LIST_KEYS_START, group, *IV, ver);
#endif
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
#if __cplusplus > 201703L
                LOG_TIMESTAMP_BY_TAG(TLT_SIGNATURE_LIST_KEYS_END, group, *IV, ver);
#else
                LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_SIGNATURE_LIST_KEYS_END, group, *IV, ver);
#endif
                dbg_default_debug("{}: requested version:{:x} is beyond the latest atomic broadcast version.", __PRETTY_FUNCTION__, requested_version);
                return {};
            }
        }
    }

    if(requested_version == CURRENT_VERSION) {
        // return the unstable query
        debug_leave_func_with_value("lockless_list_prefix({})", prefix);
        auto rvo_val = persistent_core->lockless_list_keys(prefix);
#if __cplusplus > 201703L
        LOG_TIMESTAMP_BY_TAG(TLT_SIGNATURE_LIST_KEYS_END, group, *IV, ver);
#else
        LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_SIGNATURE_LIST_KEYS_END, group, *IV, ver);
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
        LOG_TIMESTAMP_BY_TAG(TLT_SIGNATURE_LIST_KEYS_END, group, *IV, ver);
#else
        LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_SIGNATURE_LIST_KEYS_END, group, *IV, ver);
#endif
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
version_tuple SignatureCascadeStore<KT, VT, IK, IV, ST>::ordered_put(const VT& value) {
    debug_enter_func_with_args("key={}", value.get_key_ref());

    std::tuple<persistent::version_t, uint64_t> version_and_timestamp = group->template get_subgroup<SignatureCascadeStore>(this->subgroup_index).get_current_version();
#if __cplusplus > 201703L
    LOG_TIMESTAMP_BY_TAG(TLT_SIGNATURE_ORDERED_PUT_START, group, value, std::get<0>(version_and_timestamp));
#else
    LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_SIGNATURE_ORDERED_PUT_START, group, value, std::get<0>(version_and_timestamp));
#endif
    auto ret = this->internal_ordered_put(value);

#if __cplusplus > 201703L
    LOG_TIMESTAMP_BY_TAG(TLT_SIGNATURE_ORDERED_PUT_END, group, value, std::get<0>(version_and_timestamp));
#else
    LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_SIGNATURE_ORDERED_PUT_END, group, value, std::get<0>(version_and_timestamp));
#endif
    debug_leave_func_with_value("version=0x{:x},timestamp={},previous_version=0x{:x},previous_version_by_key=0x{:x}",
                                std::get<0>(ret), std::get<1>(ret), std::get<2>(ret), std::get<3>(ret));
#ifdef ENABLE_EVALUATION
    // avoid unused variable warning.
    if constexpr(!std::is_base_of<IHasMessageID, VT>::value) {
        version_and_timestamp = version_and_timestamp;
    }
#endif
    return ret;
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
void SignatureCascadeStore<KT, VT, IK, IV, ST>::ordered_put_and_forget(const VT& value) {
    debug_enter_func_with_args("key={}", value.get_key_ref());
#ifdef ENABLE_EVALUATION
    std::tuple<persistent::version_t, uint64_t> version_and_timestamp = group->template get_subgroup<SignatureCascadeStore>(this->subgroup_index).get_current_version();
#endif

#if __cplusplus > 201703L
    LOG_TIMESTAMP_BY_TAG(TLT_SIGNATURE_ORDERED_PUT_AND_FORGET_START, group, value, std::get<0>(version_and_timestamp));
#else
    LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_SIGNATURE_ORDERED_PUT_AND_FORGET_START, group, value, std::get<0>(version_and_timestamp));
#endif

    this->internal_ordered_put(value);

#if __cplusplus > 201703L
    LOG_TIMESTAMP_BY_TAG(TLT_SIGNATURE_ORDERED_PUT_AND_FORGET_END, group, value, std::get<0>(version_and_timestamp));
#else
    LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_SIGNATURE_ORDERED_PUT_AND_FORGET_END, group, value, std::get<0>(version_and_timestamp));
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
version_tuple SignatureCascadeStore<KT, VT, IK, IV, ST>::internal_ordered_put(const VT& value) {
    static_assert(std::is_base_of_v<IKeepVersion, VT>, "SignatureCascadeStore can only be used with values that implement IKeepVersion");
    debug_enter_func_with_args("key={}", value.get_key_ref());
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
        dbg_default_debug("internal_ordered_put: Storing mapping from data ver=0x{:x} -> hash ver=0x{:x}", data_object_version, std::get<0>(hash_object_version_and_timestamp));
        data_to_hash_version->emplace(data_object_version, std::get<0>(hash_object_version_and_timestamp));
    }
    persistent::version_t previous_version = this->persistent_core.getLatestVersion();
    persistent::version_t previous_version_by_key;
    try {
        previous_version_by_key = this->persistent_core->ordered_put(value, previous_version);
    } catch(cascade_exception& ex) {
        // verification failed. So we return invalid versions.
        debug_leave_func_with_value("Failed with exception: {}", ex.what());
        return {persistent::INVALID_VERSION, persistent::INVALID_VERSION, persistent::INVALID_VERSION, 0};
    }
    subgroup_id_t my_subgroup_id = group->template get_subgroup<SignatureCascadeStore>(this->subgroup_index).get_subgroup_id();
    // Register a signature notification action for all subscribed clients
    // The key must be copied into the lambdas, since value.get_key_ref() won't work once this method ends
    KT copy_of_key = value.get_key_ref();
    uint64_t message_id = 0;
#ifdef ENABLE_EVALUATION
    // Similarly, the message ID for timestamp logging must be copied in, if enabled
    if constexpr(std::is_base_of_v<IHasMessageID, VT>) {
        message_id = value.get_message_id();
    }
#endif
    for(const node_id_t client_id : subscribed_clients[value.get_key_ref()]) {
        dbg_default_debug("internal_ordered_put: Registering notify action for client {}, version 0x{:x}", client_id, std::get<0>(hash_object_version_and_timestamp));
        cascade_context_ptr->get_persistence_observer().register_persistence_action(
                my_subgroup_id, std::get<0>(hash_object_version_and_timestamp), true,
                [=]() {
                    send_client_notification(client_id, copy_of_key, std::get<0>(hash_object_version_and_timestamp), data_object_version, message_id);
                });
    }
    for(const node_id_t client_id : subscribed_clients[*IK]) {
        dbg_default_debug("internal_ordered_put: Registering notify action for client {}, version 0x{:x}", client_id, std::get<0>(hash_object_version_and_timestamp));
        cascade_context_ptr->get_persistence_observer().register_persistence_action(
                my_subgroup_id, std::get<0>(hash_object_version_and_timestamp), true,
                [=]() {
                    send_client_notification(client_id, copy_of_key, std::get<0>(hash_object_version_and_timestamp), data_object_version, message_id);
                });
    }
#ifdef ENABLE_EVALUATION
    // For evaluation, register an additional action to record a timestamp log entry when the signature is finished
    if constexpr(std::is_base_of_v<IHasMessageID, VT>) {
        node_id_t my_id = this->group->get_my_id();
        cascade_context_ptr->get_persistence_observer().register_persistence_action(
                my_subgroup_id, std::get<0>(hash_object_version_and_timestamp), true,
                [=]() {
                    TimestampLogger::log(TLT_SIGNATURE_PERSISTED, my_id, message_id, get_walltime(), std::get<0>(hash_object_version_and_timestamp));
                });
    }
#endif
    // Register an action to send the signed object to the WanAgent once the signature is finished
    if(backup_enabled && is_primary_site) {
        cascade_context_ptr->get_persistence_observer().register_persistence_action(
                my_subgroup_id, std::get<0>(hash_object_version_and_timestamp), true,
                [=]() {
                    send_to_wan_agent(std::get<0>(hash_object_version_and_timestamp), data_object_version);
                });
    }
    if(cascade_watcher_ptr) {
        (*cascade_watcher_ptr)(
                this->subgroup_index,
                group->template get_subgroup<SignatureCascadeStore>(this->subgroup_index).get_shard_num(),
                group->get_rpc_caller_id(),
                value.get_key_ref(), value,
                cascade_context_ptr);
    }
    debug_leave_func_with_value("version=0x{:x},timestamp={},previous_version=0x{:x},previous_version_by_key=0x{:x}",
                                std::get<0>(hash_object_version_and_timestamp), std::get<1>(hash_object_version_and_timestamp), previous_version, previous_version_by_key);
    return {std::get<0>(hash_object_version_and_timestamp), std::get<1>(hash_object_version_and_timestamp), previous_version, previous_version_by_key};
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
version_tuple SignatureCascadeStore<KT, VT, IK, IV, ST>::ordered_remove(const KT& key) {
    debug_enter_func_with_args("key={}", key);
    std::tuple<persistent::version_t, uint64_t> version_and_timestamp = group->template get_subgroup<SignatureCascadeStore>(this->subgroup_index).get_current_version();
#if __cplusplus > 201703L
    LOG_TIMESTAMP_BY_TAG(TLT_SIGNATURE_ORDERED_REMOVE_START, group, *IV, std::get<0>(version_and_timestamp));
#else
    LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_SIGNATURE_ORDERED_REMOVE_START, group, *IV, std::get<0>(version_and_timestamp));
#endif
    auto value = create_null_object_cb<KT, VT, IK, IV>(key);
    if constexpr(std::is_base_of<IKeepVersion, VT>::value) {
        value.set_version(std::get<0>(version_and_timestamp));
    }
    if constexpr(std::is_base_of<IKeepTimestamp, VT>::value) {
        value.set_timestamp(std::get<1>(version_and_timestamp));
    }
    try {
        auto previous_version = this->persistent_core.getLatestVersion();
        auto previous_version_by_key = this->persistent_core->ordered_remove(value, this->persistent_core.getLatestVersion());

        if(cascade_watcher_ptr) {
            (*cascade_watcher_ptr)(
                    this->subgroup_index,
                    group->template get_subgroup<SignatureCascadeStore>(this->subgroup_index).get_shard_num(),
                    group->get_rpc_caller_id(),
                    key, value,
                    cascade_context_ptr);
        }
#if __cplusplus > 201703L
        LOG_TIMESTAMP_BY_TAG(TLT_SIGNATURE_ORDERED_REMOVE_END, group, *IV, std::get<0>(version_and_timestamp));
#else
        LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_SIGNATURE_ORDERED_REMOVE_END, group, *IV, std::get<0>(version_and_timestamp));
#endif
        debug_leave_func_with_value("version=0x{:x},previous_version=0x{:x},previous_version_by_key=0x{:x},timestamp={}",
                                    std::get<0>(version_and_timestamp),
                                    std::get<1>(version_and_timestamp),
                                    previous_version,
                                    previous_version_by_key);

        return {std::get<0>(version_and_timestamp),
                std::get<1>(version_and_timestamp),
                previous_version,
                previous_version_by_key};
    } catch(cascade_exception& ex) {
        debug_leave_func_with_value("Failed with exception:{}", ex.what());
        return {persistent::INVALID_VERSION, persistent::INVALID_VERSION, persistent::INVALID_VERSION, 0};
    }
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
const VT SignatureCascadeStore<KT, VT, IK, IV, ST>::ordered_get(const KT& key) {
    debug_enter_func_with_args("key={}", key);
#ifdef ENABLE_EVALUATION
    std::tuple<persistent::version_t, uint64_t> version_and_timestamp = group->template get_subgroup<SignatureCascadeStore>(this->subgroup_index).get_current_version();
#endif

#if __cplusplus > 201703L
    LOG_TIMESTAMP_BY_TAG(TLT_SIGNATURE_ORDERED_GET_START, group, *IV, std::get<0>(version_and_timestamp));
#else
    LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_SIGNATURE_ORDERED_GET_START, group, *IV, std::get<0>(version_and_timestamp));
#endif

    // TODO: double check if Named Return Value Optimization (NRVO) works here!!!
    auto rvo_val = this->persistent_core->ordered_get(key);
#if __cplusplus > 201703L
    LOG_TIMESTAMP_BY_TAG(TLT_SIGNATURE_ORDERED_GET_END, group, *IV, std::get<0>(version_and_timestamp));
#else
    LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_SIGNATURE_ORDERED_GET_END, group, *IV, std::get<0>(version_and_timestamp));
#endif
    debug_leave_func();
    return rvo_val;
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
uint64_t SignatureCascadeStore<KT, VT, IK, IV, ST>::ordered_get_size(const KT& key) {
    debug_enter_func_with_args("key={}", key);
#ifdef ENABLE_EVALUATION
    std::tuple<persistent::version_t, uint64_t> version_and_timestamp = group->template get_subgroup<SignatureCascadeStore>(this->subgroup_index).get_current_version();
#endif
#if __cplusplus > 201703L
    LOG_TIMESTAMP_BY_TAG(TLT_SIGNATURE_ORDERED_GET_SIZE_START, group, *IV, std::get<0>(version_and_timestamp));
#else
    LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_SIGNATURE_ORDERED_GET_SIZE_START, group, *IV, std::get<0>(version_and_timestamp));
#endif

    uint64_t size = this->persistent_core->ordered_get_size(key);

#if __cplusplus > 201703L
    LOG_TIMESTAMP_BY_TAG(TLT_SIGNATURE_ORDERED_GET_SIZE_END, group, *IV, std::get<0>(version_and_timestamp));
#else
    LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_SIGNATURE_ORDERED_GET_SIZE_END, group, *IV, std::get<0>(version_and_timestamp));
#endif
    debug_leave_func_with_value("size={}", size);
    return size;
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
void SignatureCascadeStore<KT, VT, IK, IV, ST>::trigger_put(const VT& value) const {
    debug_enter_func_with_args("key={}", value.get_key_ref());
    LOG_TIMESTAMP_BY_TAG(TLT_SIGNATURE_TRIGGER_PUT_START, group, value);

    if(cascade_watcher_ptr) {
        (*cascade_watcher_ptr)(
                this->subgroup_index,
                group->template get_subgroup<SignatureCascadeStore<KT, VT, IK, IV, ST>>(this->subgroup_index).get_shard_num(),
                group->get_rpc_caller_id(),
                value.get_key_ref(), value,
                cascade_context_ptr, true);
    }

    LOG_TIMESTAMP_BY_TAG(TLT_SIGNATURE_TRIGGER_PUT_END, group, value);
    debug_leave_func();
}

#ifdef ENABLE_EVALUATION
template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
void SignatureCascadeStore<KT, VT, IK, IV, ST>::dump_timestamp_log(const std::string& filename) const {
    debug_enter_func_with_args("filename={}", filename);
    derecho::Replicated<SignatureCascadeStore>& subgroup_handle = group->template get_subgroup<SignatureCascadeStore>(this->subgroup_index);
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
void SignatureCascadeStore<KT, VT, IK, IV, ST>::ordered_dump_timestamp_log(const std::string& filename) {
    TimestampLogger::flush(filename);
}
#ifdef DUMP_TIMESTAMP_WORKAROUND
template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
void SignatureCascadeStore<KT, VT, IK, IV, ST>::dump_timestamp_log_workaround(const std::string& filename) const {
    debug_enter_func_with_args("filename={}", filename);
    TimestampLogger::flush(filename);
    debug_leave_func();
}
#endif  // DUMP_TIMESTAMP_WORKAROUND
#endif  // ENABLE_EVALUATION

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
std::vector<KT> SignatureCascadeStore<KT, VT, IK, IV, ST>::ordered_list_keys(const std::string& prefix) {
#ifdef ENABLE_EVALUATION
    std::tuple<persistent::version_t, uint64_t> version_and_timestamp = group->template get_subgroup<SignatureCascadeStore>(this->subgroup_index).get_current_version();
#endif
#if __cplusplus > 201703L
    LOG_TIMESTAMP_BY_TAG(TLT_SIGNATURE_ORDERED_LIST_KEYS_START, group, *IV, std::get<0>(version_and_timestamp));
#else
    LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_SIGNATURE_ORDERED_LIST_KEYS_START, group, *IV, std::get<0>(version_and_timestamp));
#endif

    // TODO: make sure NRVO works here!!!
    auto rvo_val = this->persistent_core->ordered_list_keys(prefix);

#if __cplusplus > 201703L
    LOG_TIMESTAMP_BY_TAG(TLT_SIGNATURE_ORDERED_LIST_KEYS_END, group, *IV, std::get<0>(version_and_timestamp));
#else
    LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_SIGNATURE_ORDERED_LIST_KEYS_END, group, *IV, std::get<0>(version_and_timestamp));
#endif
    debug_leave_func();
    return rvo_val;
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
std::unique_ptr<SignatureCascadeStore<KT, VT, IK, IV, ST>> SignatureCascadeStore<KT, VT, IK, IV, ST>::from_bytes(mutils::DeserializationManager* dsm, uint8_t const* buf) {
    auto subgroup_id_ptr = mutils::from_bytes<derecho::subgroup_id_t>(dsm, buf);
    std::size_t offset = mutils::bytes_size(*subgroup_id_ptr);
    auto backup_enabled_ptr = mutils::from_bytes<bool>(dsm, buf + offset);
    offset += mutils::bytes_size(*backup_enabled_ptr);
    auto is_primary_ptr = mutils::from_bytes<bool>(dsm, buf + offset);
    offset += mutils::bytes_size(*is_primary_ptr);
    auto persistent_core_ptr = mutils::from_bytes<persistent::Persistent<DeltaCascadeStoreCore<KT, VT, IK, IV>, ST>>(dsm, buf + offset);
    offset += mutils::bytes_size(*persistent_core_ptr);
    auto version_map_ptr = mutils::from_bytes<persistent::Persistent<std::map<persistent::version_t, const persistent::version_t>>>(dsm, buf + offset);
    offset += mutils::bytes_size(*version_map_ptr);
    auto ack_table_ptr = mutils::from_bytes<std::map<wan_agent::site_id_t, uint64_t>>(dsm, buf + offset);
    offset += mutils::bytes_size(*ack_table_ptr);
    auto message_table_ptr = mutils::from_bytes<std::map<uint64_t, std::tuple<KT, persistent::version_t, persistent::version_t>>>(dsm, buf + offset);
    auto persistent_cascade_store_ptr
            = std::make_unique<SignatureCascadeStore>(*subgroup_id_ptr,
                                                      *backup_enabled_ptr,
                                                      *is_primary_ptr,
                                                      std::move(*persistent_core_ptr),
                                                      std::move(*version_map_ptr),
                                                      std::move(*ack_table_ptr),
                                                      std::move(*message_table_ptr),
                                                      dsm->registered<CriticalDataPathObserver<SignatureCascadeStore<KT, VT, IK, IV>>>() ? &(dsm->mgr<CriticalDataPathObserver<SignatureCascadeStore<KT, VT, IK, IV>>>()) : nullptr,
                                                      dsm->registered<ICascadeContext>() ? &(dsm->mgr<ICascadeContext>()) : nullptr);
    return persistent_cascade_store_ptr;
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
SignatureCascadeStore<KT, VT, IK, IV, ST>::SignatureCascadeStore(
        persistent::PersistentRegistry* pr,
        derecho::subgroup_id_t subgroup_id,
        CriticalDataPathObserver<SignatureCascadeStore<KT, VT, IK, IV>>* cw,
        ICascadeContext* cc)
        : subgroup_id(subgroup_id),
          backup_enabled(getConfWithDefault(CASCADE_ENABLE_WANAGENT, true)),
          is_primary_site(getConfWithDefault(CASCADE_IS_PRIMARY_SITE, true)),
          persistent_core(pr, true),  // enable signatures
          data_to_hash_version(pr, false),
          cascade_watcher_ptr(cw),
          cascade_context_ptr(cc) {}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
SignatureCascadeStore<KT, VT, IK, IV, ST>::SignatureCascadeStore(
        derecho::subgroup_id_t deserialized_subgroup_id,
        bool backup_enabled,
        bool is_primary_site,
        persistent::Persistent<DeltaCascadeStoreCore<KT, VT, IK, IV>, ST>&& deserialized_persistent_core,
        persistent::Persistent<std::map<persistent::version_t, const persistent::version_t>>&& deserialized_data_to_hash_version,
        std::map<wan_agent::site_id_t, uint64_t>&& deserialized_ack_table,
        std::map<uint64_t, std::tuple<KT, persistent::version_t, persistent::version_t>>&& deserialized_wanagent_message_ids,
        CriticalDataPathObserver<SignatureCascadeStore<KT, VT, IK, IV>>* cw,
        ICascadeContext* cc)
        : subgroup_id(deserialized_subgroup_id),
          backup_enabled(backup_enabled),
          is_primary_site(is_primary_site),
          persistent_core(std::move(deserialized_persistent_core)),
          data_to_hash_version(std::move(deserialized_data_to_hash_version)),
          backup_ack_table(std::move(deserialized_ack_table)),
          wanagent_message_ids(std::move(deserialized_wanagent_message_ids)),
          cascade_watcher_ptr(cw),
          cascade_context_ptr(cc) {}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
SignatureCascadeStore<KT, VT, IK, IV, ST>::SignatureCascadeStore()
        : subgroup_id(0),
          backup_enabled(false),
          is_primary_site(false),
          persistent_core(
                  []() {
                      return std::make_unique<DeltaCascadeStoreCore<KT, VT, IK, IV>>();
                  },
                  nullptr,
                  nullptr),  // I'm guessing the dummy version doesn't need to enable signatures on persistent_core
          data_to_hash_version(nullptr),
          wanagent(nullptr),
          cascade_watcher_ptr(nullptr),
          cascade_context_ptr(nullptr) {}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
SignatureCascadeStore<KT, VT, IK, IV, ST>::~SignatureCascadeStore() {}

/* --- New methods only in SignatureCascadeStore, not copied from PersistentCascadeStore --- */

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
void SignatureCascadeStore<KT, VT, IK, IV, ST>::new_view_callback(const View& new_view) {
    if(!backup_enabled) {
        return;
    }
    uint32_t my_shard_num = new_view.my_subgroups.at(subgroup_id);
    const SubView& my_shard_view = new_view.subgroup_shard_views.at(subgroup_id).at(my_shard_num);
    // Use the "wanagent_port_offset" config option to derive the WanAgent port for the shard leader from its Derecho port
    int wanagent_port_offset = getConfWithDefault(CASCADE_WANAGENT_PORT_OFFSET, 1000);
    uint16_t wanagent_leader_port = my_shard_view.member_ips_and_ports[0].gms_port + wanagent_port_offset;
    // If this is the very first new-view callback, the WanAgent hasn't been constructed yet and needs to be set up
    if(!wanagent) {
        std::string agent_config_location = getConfWithDefault(CASCADE_WANAGENT_CONFIG_FILE, std::string("wanagent.json"));
        nlohmann::json wan_agent_config = nlohmann::json::parse(std::ifstream(agent_config_location));
        wan_agent::site_id_t my_site_id = wan_agent_config[WAN_AGENT_CONF_LOCAL_SITE_ID];
        // Find the sites entry for the local site, and ensure local_initial_leader is set to the index matching
        // this subgroup/shard's actual leader in the current view
        ip_addr_t shard_leader_ip = my_shard_view.member_ips_and_ports[0].ip_address;
        for(const auto& site_object : wan_agent_config[WAN_AGENT_CONF_SITES]) {
            if(site_object[WAN_AGENT_CONF_SITES_ID] == my_site_id) {
                for(std::size_t replica_index = 0; replica_index < site_object[WAN_AGENT_CONF_SITES_IP].size(); ++replica_index) {
                    if(site_object[WAN_AGENT_CONF_SITES_IP][replica_index] == shard_leader_ip
                       && site_object[WAN_AGENT_CONF_SITES_PORT][replica_index] == wanagent_leader_port) {
                        wan_agent_config[WAN_AGENT_CONF_LOCAL_LEADER] = replica_index;
                        break;
                    }
                }
                break;
            }
        }
        wanagent = wan_agent::WanAgent::create(
                wan_agent_config,
                [this](const std::map<wan_agent::site_id_t, uint64_t>& ack_table) { wan_stability_callback(ack_table); },
                [this](uint32_t sender, const uint8_t* msg, size_t size) { wan_message_callback(sender, msg, size); });
        return;
    }
    // Otherwise, update WanAgent's leader (to equal the shard leader) and determine if this node just became the leader
    bool became_leader = !wanagent->is_site_leader() && my_shard_view.members[0] == group->get_my_id();
    wanagent->set_site_leader(wan_agent::TcpEndpoint{my_shard_view.member_ips_and_ports[0].ip_address, wanagent_leader_port});
    if(became_leader) {
        // Determine from the ack table which updates were still pending and need to be resent
        uint64_t max_acked_id = 0;
        uint64_t min_acked_id = static_cast<uint64_t>(-1);
        bool max_at_least_0 = false;
        for(const auto& site_entry : backup_ack_table) {
            if(site_entry.second < min_acked_id || site_entry.second == static_cast<uint64_t>(-1)) {
                min_acked_id = site_entry.second;
            }
            if(site_entry.second >= max_acked_id && site_entry.second != static_cast<uint64_t>(-1)) {
                max_at_least_0 = true;
                max_acked_id = site_entry.second;
            }
        }
        if(!max_at_least_0) {
            max_acked_id = static_cast<uint64_t>(-1);
        }
        // Re-create backup objects and messages from min_acked_id + 1 up to max_acked_id
        std::vector<std::unique_ptr<uint8_t[]>> message_buffers;
        for(uint64_t message_id = min_acked_id + 1; message_id <= max_acked_id; message_id++) {
            auto [key, hash_version, data_version] = wanagent_message_ids.at(message_id);
            VT backup_object = make_backup_object(hash_version, data_version);
            std::size_t object_size = mutils::bytes_size(backup_object);
            // Manually re-create the WanAgent message format, which puts the payload size at the beginning
            std::unique_ptr<uint8_t[]> message_buffer = std::make_unique<uint8_t[]>(object_size + sizeof(object_size));
            std::memcpy(message_buffer.get(), &object_size, sizeof(object_size));
            mutils::to_bytes(backup_object, message_buffer.get() + sizeof(object_size));
            message_buffers.emplace_back(std::move(message_buffer));
        }
        wanagent->initialize_new_leader(backup_ack_table, message_buffers);
        wanagent->await_connections_ready();
    }
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
std::tuple<std::vector<uint8_t>, persistent::version_t> SignatureCascadeStore<KT, VT, IK, IV, ST>::get_signature(
        const KT& key,
        const persistent::version_t& ver,
        bool stable,
        bool exact) const {
    debug_enter_func_with_args("key={},ver=0x{:x}", key, ver);

    if(ver == CURRENT_VERSION) {
        dbg_default_error("SignatureCascadeStore only supports get_signature() with specific version, not CURRENT_VERSION");
        debug_leave_func();
        return {std::vector<uint8_t>{}, persistent::INVALID_VERSION};
    }

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
    if(stable) {
        derecho::Replicated<SignatureCascadeStore>& subgroup_handle = group->template get_subgroup<SignatureCascadeStore>(this->subgroup_index);
        // Wait for the requested signature object to be globally persisted
        if(!subgroup_handle.wait_for_global_persistence_frontier(hash_version)
           && hash_version > persistent_core.getLatestVersion()) {
            // INVALID version
            dbg_default_debug("{}: requested version:{:x} is beyond the latest atomic broadcast version.", __PRETTY_FUNCTION__, hash_version);
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
    if(signature_found) {
        debug_leave_func_with_value("signature found with hash ver=0x{:x} and previous_signed_version=0x{:x}", hash_version, previous_signed_version);
        return {signature, previous_signed_version};
    } else {
        debug_leave_func_with_value("signature not found for hash ver=0x{:x}", hash_version);
        return {std::vector<uint8_t>{}, persistent::INVALID_VERSION};
    }
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
std::tuple<std::vector<uint8_t>, persistent::version_t> SignatureCascadeStore<KT, VT, IK, IV, ST>::get_signature_by_version(
        const persistent::version_t& ver) const {
    debug_enter_func_with_args("ver=0x{:x}", ver);
    if(ver == CURRENT_VERSION) {
        dbg_default_error("get_signature_by_version must be called with a specific version, not CURRENT_VERSION");
        debug_leave_func_with_value("get_signature_by_version does not support CURRENT_VERSION ({})", CURRENT_VERSION);
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
VT SignatureCascadeStore<KT, VT, IK, IV, ST>::make_backup_object(persistent::version_t hash_object_version,
                                                                 persistent::version_t data_object_version) {
    debug_enter_func_with_args("hash version = {}, data version = {}", hash_object_version, data_object_version);
    // Construct a fake "object" containing the signature and the corresponding data object version in addition to the hash
    VT object_plus_signature;
    // Initialize by copying the hash object
    // We know the version will be an exact match because this method is only called internally on known versions
    object_plus_signature.copy_from(*persistent_core.template getDelta<VT>(hash_object_version, true));
    // Copy the object's body to a new blob and add the additional "header fields" to the beginning
    // This only works if VT has a member called "blob" that is a Blob; I hope this is safe
    std::size_t new_body_size = object_plus_signature.blob.size
                                + persistent_core.getSignatureSize()
                                + sizeof(data_object_version);
    uint8_t* new_body_buffer = new uint8_t[new_body_size];
    std::memcpy(new_body_buffer, reinterpret_cast<uint8_t*>(&data_object_version), sizeof(data_object_version));
    std::size_t bytes_written = sizeof(data_object_version);
    persistent::version_t previous_signed_version = persistent::INVALID_VERSION;
    bool signature_found = persistent_core.getSignature(hash_object_version,
                                                        new_body_buffer + bytes_written,
                                                        previous_signed_version);
    if(!signature_found) {
        dbg_default_error("Signature not found for version {}, even though persistence has finished", hash_object_version);
    }
    bytes_written += persistent_core.getSignatureSize();
    std::memcpy(new_body_buffer + bytes_written, object_plus_signature.blob.bytes, object_plus_signature.blob.size);
    // Unnecessary copy because Blob can't take ownership of a buffer; it always copies it in
    object_plus_signature.blob = Blob(new_body_buffer, new_body_size);
    delete[] new_body_buffer;
    debug_leave_func_with_value("{}", object_plus_signature);
    return object_plus_signature;
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
void SignatureCascadeStore<KT, VT, IK, IV, ST>::send_to_wan_agent(persistent::version_t hash_object_version,
                                                                  persistent::version_t data_object_version) {
    debug_enter_func_with_args("hash version = {}, data version = {}", hash_object_version, data_object_version);
    if(!wanagent->is_site_leader()) {
        dbg_default_debug("Skipping send_to_wan_agent since this node is not the shard leader");
        debug_leave_func();
        return;
    }
    VT object_plus_signature = make_backup_object(hash_object_version, data_object_version);
    std::vector<uint8_t> serialized_object(mutils::bytes_size(object_plus_signature));
    mutils::to_bytes(object_plus_signature, serialized_object.data());
    uint64_t message_num = wanagent->send(serialized_object.data(), serialized_object.size());
    // Send an ordered update to the other replicas to record the message number
    derecho::Replicated<SignatureCascadeStore>& subgroup_handle = group->template get_subgroup<SignatureCascadeStore>(this->subgroup_index);
    subgroup_handle.template ordered_send<RPC_NAME(record_wan_message_id)>(
            message_num, object_plus_signature.get_key_ref(), hash_object_version, data_object_version);
    debug_leave_func();
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
void SignatureCascadeStore<KT, VT, IK, IV, ST>::send_client_notification(
        node_id_t external_client_id, const KT& key, persistent::version_t hash_object_version,
        persistent::version_t data_object_version, uint64_t evaluation_message_id) const {
    debug_enter_func_with_args("key={}, hash_object_version={}, data_object_version={}", key, hash_object_version, data_object_version);
    // Retrieve the signature, which must exist by now since persistence is finished, as well as the previous signature it encapsulates
    persistent::version_t previous_signed_version = persistent::INVALID_VERSION;
    persistent::version_t dummy;
    std::vector<uint8_t> signature(persistent_core.getSignatureSize());
    std::vector<uint8_t> previous_signature(persistent_core.getSignatureSize());
    bool signature_found = persistent_core.getSignature(hash_object_version, signature.data(), previous_signed_version);
    if(!signature_found) {
        dbg_default_error("Signature not found for version {}, even though persistence has finished", hash_object_version);
    }
    // For the very first version, previous_signed_version is -1 and there is no previous signature to retrieve
    if(previous_signed_version != persistent::INVALID_VERSION) {
        signature_found = persistent_core.getSignature(previous_signed_version, previous_signature.data(), dummy);
        if(!signature_found) {
            dbg_default_error("Signature not found for version {}, even though persistence has finished", hash_object_version);
        }
    }

    derecho::ExternalClientCallback<SignatureCascadeStore>& client_caller
            = group->template get_client_callback<SignatureCascadeStore>(this->subgroup_index);
    std::size_t message_size = mutils::bytes_size(data_object_version)
#ifdef ENABLE_EVALUATION
                               + mutils::bytes_size(evaluation_message_id)
#endif
                               + mutils::bytes_size(hash_object_version)
                               + mutils::bytes_size(signature)
                               + mutils::bytes_size(previous_signed_version)
                               + mutils::bytes_size(previous_signature);
    // Message format: [message id], data version, hash version, signature data, previous signed version, previous signature
    // Problem: Blob's data buffer can't be modified, so I have to copy the bytes into a temporary buffer, then copy them again into Blob
    uint8_t* temp_buffer_for_blob = new uint8_t[message_size];
    std::size_t body_offset = 0;
#ifdef ENABLE_EVALUATION
    body_offset += mutils::to_bytes(evaluation_message_id, temp_buffer_for_blob + body_offset);
#endif
    body_offset += mutils::to_bytes(data_object_version, temp_buffer_for_blob + body_offset);
    body_offset += mutils::to_bytes(hash_object_version, temp_buffer_for_blob + body_offset);
    body_offset += mutils::to_bytes(signature, temp_buffer_for_blob + body_offset);
    body_offset += mutils::to_bytes(previous_signed_version, temp_buffer_for_blob + body_offset);
    body_offset += mutils::to_bytes(previous_signature, temp_buffer_for_blob + body_offset);
    Blob message_body(temp_buffer_for_blob, message_size);
    delete[] temp_buffer_for_blob;
    // Construct and send a CascadeNotificationMessage in the same way as ServiceClient::notify
    CascadeNotificationMessage cascade_message(get_pathname<KT>(key), message_body);
    // TODO: redesign to avoid memory copies.
    NotificationMessage derecho_message(CascadeNotificationMessageType::SignatureNotification,
                                        mutils::bytes_size(cascade_message));
    mutils::to_bytes(cascade_message, derecho_message.body);
    try {
        client_caller.template p2p_send<derecho::rpc::hash_cstr("notify")>(external_client_id, derecho_message);
    } catch (const derecho::node_removed_from_group_exception& e) {
        dbg_default_debug("Notification not sent, client has disconnected");
    }
    debug_leave_func();
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
void SignatureCascadeStore<KT, VT, IK, IV, ST>::request_notification(node_id_t external_client_id, persistent::version_t ver) const {
    debug_enter_func_with_args("external_client_id={}, ver={}", external_client_id, ver);
    // Translate ver from a data-object version to its corresponding signature-object version
    // This function will only be called after the data object has been put in the PersistentStore
    // (which will forward it to the SignatureStore), so the mapping should exist by now
    persistent::version_t hash_version;
    {
        std::lock_guard<std::mutex> map_lock(version_map_mutex);
        auto version_map_search = (*data_to_hash_version).upper_bound(ver);
        if(version_map_search != (*data_to_hash_version).begin()) {
            version_map_search--;
            // The search iterator now points to the largest version <= ver, which is what we want
            hash_version = version_map_search->second;
        } else {
            // The map is empty, so no objects have yet been stored here
            debug_leave_func();
            return;
        }
    }

    // Figure out which key is stored at this version, so it can be used to construct a notification message
    KT key = persistent_core.template getDelta<VT>(hash_version, false, [](const VT& value) {
        return value.get_key_ref();
    });
    dbg_default_debug("request_notification: Registering notify action for key {}, version {}", key, hash_version);
    derecho::Replicated<SignatureCascadeStore>& subgroup_handle = group->template get_subgroup<SignatureCascadeStore>(this->subgroup_index);
    subgroup_id_t my_subgroup_id = subgroup_handle.get_subgroup_id();
    cascade_context_ptr->get_persistence_observer().register_persistence_action(
            my_subgroup_id, hash_version, true,
            [=]() { send_client_notification(external_client_id, key, hash_version, ver); });
    debug_leave_func();
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
void SignatureCascadeStore<KT, VT, IK, IV, ST>::subscribe_to_notifications(node_id_t external_client_id, const KT& key) const {
    debug_enter_func_with_args("external_client_id={}, key={}", external_client_id, key);
    subscribed_clients[key].emplace_back(external_client_id);
    debug_leave_func();
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
void SignatureCascadeStore<KT, VT, IK, IV, ST>::subscribe_to_all_notifications(node_id_t external_client_id) const {
    debug_enter_func_with_args("external_client_id={}", external_client_id);
    subscribed_clients[*IK].emplace_back(external_client_id);
    debug_leave_func();
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
void SignatureCascadeStore<KT, VT, IK, IV, ST>::wan_stability_callback(const std::map<wan_agent::site_id_t, uint64_t>& ack_table) {
    debug_enter_func();
    derecho::Replicated<SignatureCascadeStore>& subgroup_handle = group->template get_subgroup<SignatureCascadeStore>(this->subgroup_index);
    subgroup_handle.template ordered_send<RPC_NAME(update_ack_table)>(ack_table);
    debug_leave_func();
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
void SignatureCascadeStore<KT, VT, IK, IV, ST>::wan_message_callback(wan_agent::site_id_t sender, const uint8_t* msg_buf, size_t msg_size) {
    debug_enter_func_with_args("sender={}", sender);
    if(is_primary_site) {
        dbg_default_warn("Received a WanAgent remote message, but this is the primary site! Ignoring it.");
        return;
    }
    std::unique_ptr<VT> object_from_remote = mutils::from_bytes<VT>(nullptr, msg_buf);
    derecho::Replicated<SignatureCascadeStore>& subgroup_handle = group->template get_subgroup<SignatureCascadeStore>(this->subgroup_index);
    subgroup_handle.template ordered_send<RPC_NAME(ordered_put)>(*object_from_remote);
    debug_leave_func();
}

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
void SignatureCascadeStore<KT, VT, IK, IV, ST>::update_ack_table(const std::map<wan_agent::site_id_t, uint64_t>& ack_table) {
    debug_enter_func_with_args("ack_table={}", ack_table);
    backup_ack_table = ack_table;
    uint64_t min_acked_id = static_cast<uint64_t>(-1);
    for(const auto& site_entry : backup_ack_table) {
        if(site_entry.second < min_acked_id || site_entry.second == static_cast<uint64_t>(-1)) {
            min_acked_id = site_entry.second;
        }
    }
    // STILL TODO:
    // If the new ACK means that a message has finished being backed up, we may need to notify a client.
    // The notification should be done from this ordered-callable function, not the wan_stability_callback,
    // since the client could have requested a notification from a shard member that is not the WanAgent leader

    // Garbage-collect wanagent_message_ids: Any message older than min_acked_id is stable and has been notified
    auto min_id_iter = wanagent_message_ids.find(min_acked_id);
    if(min_id_iter != wanagent_message_ids.end()) {
        wanagent_message_ids.erase(wanagent_message_ids.begin(), min_id_iter);
    }
    debug_leave_func();
}
template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
void SignatureCascadeStore<KT, VT, IK, IV, ST>::record_wan_message_id(uint64_t message_id, const KT& object_key,
                                                                      persistent::version_t object_version,
                                                                      persistent::version_t data_object_version) {
    debug_enter_func_with_args("message_id={}, key={}, object_version={}, data_version={}", message_id, object_key, object_version, data_object_version);
    wanagent_message_ids.emplace(message_id, std::make_tuple(object_key, object_version, data_object_version));
    debug_leave_func();
}

}  // namespace cascade
}  // namespace derecho
