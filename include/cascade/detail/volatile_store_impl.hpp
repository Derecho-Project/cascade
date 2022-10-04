#pragma once
#include "../volatile_store.hpp"

#include "cascade/config.h"
#include "cascade/utils.hpp"
#include "debug_util.hpp"

#include <derecho/conf/conf.hpp>
#include <derecho/persistent/PersistentInterface.hpp>
#include <derecho/persistent/detail/PersistLog.hpp>
#include <map>
#include <memory>
#include <string>
#include <type_traits>

namespace derecho {
namespace cascade {

template <typename KT, typename VT, KT* IK, VT* IV>
std::tuple<persistent::version_t, uint64_t> VolatileCascadeStore<KT, VT, IK, IV>::put(const VT& value) const {
    debug_enter_func_with_args("value.get_key_ref={}", value.get_key_ref());
    LOG_TIMESTAMP_BY_TAG(TLT_VOLATILE_PUT_START, group, value);

    derecho::Replicated<VolatileCascadeStore>& subgroup_handle = group->template get_subgroup<VolatileCascadeStore>(this->subgroup_index);
    auto results = subgroup_handle.template ordered_send<RPC_NAME(ordered_put)>(value);
    auto& replies = results.get();
    std::tuple<persistent::version_t, uint64_t> ret(CURRENT_VERSION, 0);
    // TODO: verfiy consistency ?
    for(auto& reply_pair : replies) {
        ret = reply_pair.second.get();
    }

    LOG_TIMESTAMP_BY_TAG(TLT_VOLATILE_PUT_END, group, value);
    debug_leave_func_with_value("version=0x{:x},timestamp={}", std::get<0>(ret), std::get<1>(ret));
    return ret;
}

template <typename KT, typename VT, KT* IK, VT* IV>
void VolatileCascadeStore<KT, VT, IK, IV>::put_and_forget(const VT& value) const {
    debug_enter_func_with_args("value.get_key_ref={}", value.get_key_ref());
    LOG_TIMESTAMP_BY_TAG(TLT_VOLATILE_PUT_AND_FORGET_START, group, value);

    derecho::Replicated<VolatileCascadeStore>& subgroup_handle = group->template get_subgroup<VolatileCascadeStore>(this->subgroup_index);
    subgroup_handle.template ordered_send<RPC_NAME(ordered_put_and_forget)>(value);

    LOG_TIMESTAMP_BY_TAG(TLT_VOLATILE_PUT_AND_FORGET_END, group, value);
    debug_leave_func();
}

#ifdef ENABLE_EVALUATION

template <typename CascadeType>
double internal_perf_put(derecho::Replicated<CascadeType>& subgroup_handle, const uint64_t max_payload_size, const uint64_t duration_sec) {
    uint64_t num_messages_sent = 0;
    // make workload
    std::vector<typename CascadeType::ObjectType> objects;
    if constexpr(std::is_convertible_v<typename CascadeType::KeyType, std::string>) {
        make_workload<typename CascadeType::KeyType, typename CascadeType::ObjectType>(max_payload_size, "raw_key_", objects);
    } else if constexpr(std::is_integral_v<typename CascadeType::KeyType>) {
        make_workload<typename CascadeType::KeyType, typename CascadeType::ObjectType>(max_payload_size, 10000, objects);
    } else {
        dbg_default_error("{} see unknown Key Type:{}", __PRETTY_FUNCTION__, typeid(typename CascadeType::KeyType).name());
        return 0;
    }
    uint64_t now_ns = get_walltime();
    uint64_t start_ns = now_ns;
    uint64_t end_ns = now_ns + duration_sec * 1000000000;
    while(end_ns > now_ns) {
        subgroup_handle.template ordered_send<RPC_NAME(ordered_put_and_forget)>(objects.at(now_ns % NUMBER_OF_DISTINCT_OBJECTS));
        now_ns = get_walltime();
        num_messages_sent++;
    }
    // send a normal put
    auto results = subgroup_handle.template ordered_send<RPC_NAME(ordered_put)>(objects.at(now_ns % NUMBER_OF_DISTINCT_OBJECTS));
    auto& replies = results.get();
    std::tuple<persistent::version_t, uint64_t> ret(CURRENT_VERSION, 0);
    // TODO: verfiy consistency ?
    for(auto& reply_pair : replies) {
        ret = reply_pair.second.get();
    }
    now_ns = get_walltime();
    num_messages_sent++;

    return (num_messages_sent)*1e9 / (now_ns - start_ns);
}

template <typename KT, typename VT, KT* IK, VT* IV>
double VolatileCascadeStore<KT, VT, IK, IV>::perf_put(const uint32_t max_payload_size, const uint64_t duration_sec) const {
    debug_enter_func_with_args("max_payload_size={},duration_sec={}", max_payload_size, duration_sec);
    derecho::Replicated<VolatileCascadeStore>& subgroup_handle = group->template get_subgroup<VolatileCascadeStore>(this->subgroup_index);
    double ops = internal_perf_put(subgroup_handle, max_payload_size, duration_sec);
    debug_leave_func_with_value("{} ops.", ops);
    return ops;
}

#endif

template <typename KT, typename VT, KT* IK, VT* IV>
std::tuple<persistent::version_t, uint64_t> VolatileCascadeStore<KT, VT, IK, IV>::remove(const KT& key) const {
    debug_enter_func_with_args("key={}", key);
    derecho::Replicated<VolatileCascadeStore>& subgroup_handle = group->template get_subgroup<VolatileCascadeStore>(this->subgroup_index);
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

// both stable and exact are ignored for VolatileCascadeStore
template <typename KT, typename VT, KT* IK, VT* IV>
const VT VolatileCascadeStore<KT, VT, IK, IV>::get(const KT& key, const persistent::version_t& ver, bool, bool) const {
    debug_enter_func_with_args("key={},ver=0x{:x}", key, ver);
    if(ver != CURRENT_VERSION) {
        debug_leave_func_with_value("Cannot support versioned get, ver=0x{:x}", ver);
        return *IV;
    }

    // copy data out
    persistent::version_t v1, v2;
    static thread_local VT copied_out;
    do {
        // This only for TSO memory reordering.
        v2 = this->lockless_v2.load(std::memory_order_relaxed);
        // compiler reordering barrier
#ifdef __GNUC__
        asm volatile("" ::
                             : "memory");
#else
#error Lockless support is currently for GCC only
#endif
        if(this->kv_map.find(key) != this->kv_map.end()) {
            copied_out.copy_from(this->kv_map.at(key));
        } else {
            copied_out.copy_from(*IV);
        }
        // compiler reordering barrier
#ifdef __GNUC__
        asm volatile("" ::
                             : "memory");
#else
#error Lockless support is currently for GCC only
#endif
        v1 = this->lockless_v1.load(std::memory_order_relaxed);
        // busy sleep
        std::this_thread::yield();
    } while(v1 != v2);
    return copied_out;
}

template <typename KT, typename VT, KT* IK, VT* IV>
const VT VolatileCascadeStore<KT, VT, IK, IV>::multi_get(const KT& key) const {
    debug_enter_func_with_args("key={}", key);

    derecho::Replicated<VolatileCascadeStore>& subgroup_handle = group->template get_subgroup<VolatileCascadeStore>(this->subgroup_index);
    auto results = subgroup_handle.template ordered_send<RPC_NAME(ordered_get)>(key);
    auto& replies = results.get();
    // TODO: verify consistency ?
    for(auto& reply_pair : replies) {
        reply_pair.second.wait();
    }
    debug_leave_func();
    return replies.begin()->second.get();
}

template <typename KT, typename VT, KT* IK, VT* IV>
const VT VolatileCascadeStore<KT, VT, IK, IV>::get_by_time(const KT& key, const uint64_t& ts_us, const bool stable) const {
    // VolatileCascadeStore does not support this.
    debug_enter_func();
    debug_leave_func();

    return *IV;
}

template <typename KT, typename VT, KT* IK, VT* IV>
std::vector<KT> VolatileCascadeStore<KT, VT, IK, IV>::multi_list_keys(const std::string& prefix) const {
    debug_enter_func_with_args("prefix={}", prefix);
    derecho::Replicated<VolatileCascadeStore>& subgroup_handle = group->template get_subgroup<VolatileCascadeStore>(this->subgroup_index);
    auto results = subgroup_handle.template ordered_send<RPC_NAME(ordered_list_keys)>(prefix);
    auto& replies = results.get();
    std::vector<KT> ret;
    // TODO: verfity consistency ?
    for(auto& reply_pair : replies) {
        ret = reply_pair.second.get();
    }
    debug_leave_func();
    return ret;
}

template <typename KT, typename VT, KT* IK, VT* IV>
std::vector<KT> VolatileCascadeStore<KT, VT, IK, IV>::list_keys(const std::string& prefix, const persistent::version_t& ver, const bool) const {
    if(ver != CURRENT_VERSION) {
        debug_leave_func_with_value("Cannot support versioned list_keys, ver=0x{:x}", ver);
        return {};
    }

    return multi_list_keys(prefix);
}

template <typename KT, typename VT, KT* IK, VT* IV>
std::vector<KT> VolatileCascadeStore<KT, VT, IK, IV>::list_keys_by_time(const std::string& prefix, const uint64_t& ts_us, const bool) const {
    // VolatileCascadeStore does not support this.
    debug_enter_func_with_args("ts_us=0x{:x}", ts_us);
    debug_leave_func();
    return {};
}

template <typename KT, typename VT, KT* IK, VT* IV>
uint64_t VolatileCascadeStore<KT, VT, IK, IV>::multi_get_size(const KT& key) const {
    debug_enter_func_with_args("key={}", key);
    derecho::Replicated<VolatileCascadeStore>& subgroup_handle = group->template get_subgroup<VolatileCascadeStore>(this->subgroup_index);
    auto results = subgroup_handle.template ordered_send<RPC_NAME(ordered_get_size)>(key);
    auto& replies = results.get();
    // TODO: verify consistency ?
    // for (auto& reply_pair : replies) {
    //     ret = reply_pair.second.get();
    // }
    debug_leave_func();
    return replies.begin()->second.get();
}

template <typename KT, typename VT, KT* IK, VT* IV>
uint64_t VolatileCascadeStore<KT, VT, IK, IV>::get_size(const KT& key, const persistent::version_t& ver, const bool, const bool) const {
    debug_enter_func_with_args("key={},ver=0x{:x}", key, ver);
    if(ver != CURRENT_VERSION) {
        debug_leave_func_with_value("Cannot support versioned get, ver=0x{:x}", ver);
        return 0;
    }
    debug_leave_func();

    // copy data out
    persistent::version_t v1, v2;
    uint64_t size = 0ull;
    do {
        // This only for TSO memory reordering.
        v2 = this->lockless_v2.load(std::memory_order_relaxed);
        // compiler reordering barrier
#ifdef __GNUC__
        asm volatile("" ::
                             : "memory");
#else
#error Lockless support is currently for GCC only
#endif
        if(this->kv_map.find(key) != this->kv_map.end()) {
            size = mutils::bytes_size(this->kv_map.at(key));
        }
        // compiler reordering barrier
#ifdef __GNUC__
        asm volatile("" ::
                             : "memory");
#else
#error Lockless support is currently for GCC only
#endif
        v1 = this->lockless_v1.load(std::memory_order_relaxed);
        // busy sleep
        std::this_thread::yield();
    } while(v1 != v2);
    return size;
}

template <typename KT, typename VT, KT* IK, VT* IV>
uint64_t VolatileCascadeStore<KT, VT, IK, IV>::get_size_by_time(const KT&, const uint64_t&, const bool) const {
    // VolatileCascadeStore does not support this.
    debug_enter_func();

    debug_leave_func();
    return 0;
}

template <typename KT, typename VT, KT* IK, VT* IV>
std::vector<KT> VolatileCascadeStore<KT, VT, IK, IV>::ordered_list_keys(const std::string& prefix) {
    std::vector<KT> key_list;
    debug_enter_func();
    for(auto kv : this->kv_map) {
        if(get_pathname<KT>(kv.first).find(prefix) == 0) {
            key_list.push_back(kv.first);
        }
    }

    debug_leave_func();
    return key_list;
}

template <typename KT, typename VT, KT* IK, VT* IV>
std::tuple<persistent::version_t, uint64_t> VolatileCascadeStore<KT, VT, IK, IV>::ordered_put(const VT& value) {
    debug_enter_func_with_args("key={}", value.get_key_ref());

    std::tuple<persistent::version_t, uint64_t> version_and_timestamp = group->template get_subgroup<VolatileCascadeStore>(this->subgroup_index).get_current_version();

    if(this->internal_ordered_put(value) == false) {
        version_and_timestamp = {persistent::INVALID_VERSION, 0};
    }

    debug_leave_func_with_value("version=0x{:x},timestamp={}", std::get<0>(version_and_timestamp), std::get<1>(version_and_timestamp));

    return version_and_timestamp;
}

template <typename KT, typename VT, KT* IK, VT* IV>
void VolatileCascadeStore<KT, VT, IK, IV>::ordered_put_and_forget(const VT& value) {
    debug_enter_func_with_args("key={}", value.get_key_ref());
    LOG_TIMESTAMP_BY_TAG(TLT_VOLATILE_ORDERED_PUT_AND_FORGET_START, group, value);
    internal_ordered_put(value);
    LOG_TIMESTAMP_BY_TAG(TLT_VOLATILE_ORDERED_PUT_AND_FORGET_END, group, value);
    debug_leave_func();
}

template <typename KT, typename VT, KT* IK, VT* IV>
bool VolatileCascadeStore<KT, VT, IK, IV>::internal_ordered_put(const VT& value) {
    std::tuple<persistent::version_t, uint64_t> version_and_timestamp = group->template get_subgroup<VolatileCascadeStore>(this->subgroup_index).get_current_version();

    if constexpr(std::is_base_of<IKeepVersion, VT>::value) {
        value.set_version(std::get<0>(version_and_timestamp));
    }
    if constexpr(std::is_base_of<IKeepTimestamp, VT>::value) {
        value.set_timestamp(std::get<1>(version_and_timestamp));
    }

    // validator
    if constexpr(std::is_base_of<IValidator<KT, VT>, VT>::value) {
        if(!value.validate(this->kv_map)) {
            return false;
        }
    }

    // Verify previous version MUST happen before update previous versions.
    if constexpr(std::is_base_of<IVerifyPreviousVersion, VT>::value) {
        bool verify_result;
        if(this->kv_map.find(value.get_key_ref()) != this->kv_map.end()) {
            verify_result = value.verify_previous_version(this->update_version, this->kv_map.at(value.get_key_ref()).get_version());
        } else {
            verify_result = value.verify_previous_version(this->update_version, persistent::INVALID_VERSION);
        }
        if(!verify_result) {
            // reject the update by returning an invalid version and timestamp
            return false;
        }
    }
    if constexpr(std::is_base_of<IKeepPreviousVersion, VT>::value) {
        if(this->kv_map.find(value.get_key_ref()) != this->kv_map.end()) {
            value.set_previous_version(this->update_version, this->kv_map.at(value.get_key_ref()).get_version());
        } else {
            value.set_previous_version(this->update_version, persistent::INVALID_VERSION);
        }
    }

    // for lockless check
    this->lockless_v1.store(std::get<0>(version_and_timestamp), std::memory_order_relaxed);
    // compiler reordering barrier
#ifdef __GNUC__
    asm volatile("" ::
                         : "memory");
#else
#error Lockless support is currently for GCC only
#endif

    this->kv_map.erase(value.get_key_ref());           // remove
    this->kv_map.emplace(value.get_key_ref(), value);  // copy constructor
    this->update_version = std::get<0>(version_and_timestamp);

    // for lockless check
    // compiler reordering barrier
#ifdef __GNUC__
    asm volatile("" ::
                         : "memory");
#else
#error Lockless support is currently for GCC only
#endif
    this->lockless_v2.store(std::get<0>(version_and_timestamp), std::memory_order_relaxed);

    if(cascade_watcher_ptr) {
        (*cascade_watcher_ptr)(
                // group->template get_subgroup<VolatileCascadeStore>(this->subgroup_index).get_subgroup_id(), // this is subgroup id
                this->subgroup_index,  // this is subgroup index
                group->template get_subgroup<VolatileCascadeStore>(this->subgroup_index).get_shard_num(),
                group->get_rpc_caller_id(),
                value.get_key_ref(), value, cascade_context_ptr);
    }

    return true;
}

template <typename KT, typename VT, KT* IK, VT* IV>
std::tuple<persistent::version_t, uint64_t> VolatileCascadeStore<KT, VT, IK, IV>::ordered_remove(const KT& key) {
    debug_enter_func_with_args("key={}", key);

    std::tuple<persistent::version_t, uint64_t> version_and_timestamp = group->template get_subgroup<VolatileCascadeStore>(this->subgroup_index).get_current_version();

    if(this->kv_map.find(key) == this->kv_map.end()) {
        debug_leave_func_with_value("version=0x{:x},timestamp={}", std::get<0>(version_and_timestamp), std::get<1>(version_and_timestamp));
        return version_and_timestamp;
    }

    auto value = create_null_object_cb<KT, VT, IK, IV>(key);

    if constexpr(std::is_base_of<IKeepVersion, VT>::value) {
        value.set_version(std::get<0>(version_and_timestamp));
    }
    if constexpr(std::is_base_of<IKeepTimestamp, VT>::value) {
        value.set_timestamp(std::get<1>(version_and_timestamp));
    }
    if constexpr(std::is_base_of<IKeepPreviousVersion, VT>::value) {
        if(this->kv_map.find(key) != this->kv_map.end()) {
            value.set_previous_version(this->update_version, this->kv_map.at(key).get_version());
        } else {
            value.set_previous_version(this->update_version, persistent::INVALID_VERSION);
        }
    }

    // for lockless check
    this->lockless_v1.store(std::get<0>(version_and_timestamp), std::memory_order_relaxed);
    // compiler reordering barrier
#ifdef __GNUC__
    asm volatile("" ::
                         : "memory");
#else
#error Lockless support is currently for GCC only
#endif

    this->kv_map.erase(key);  // remove
    this->kv_map.emplace(key, value);
    this->update_version = std::get<0>(version_and_timestamp);

    // for lockless check
    // compiler reordering barrier
#ifdef __GNUC__
    asm volatile("" ::
                         : "memory");
#else
#error Lockless support is currently for GCC only
#endif
    this->lockless_v2.store(std::get<0>(version_and_timestamp), std::memory_order_relaxed);

    if(cascade_watcher_ptr) {
        (*cascade_watcher_ptr)(
                // group->template get_subgroup<VolatileCascadeStore>(this->subgroup_index).get_subgroup_id(), // this is subgroup id
                this->subgroup_index,
                group->template get_subgroup<VolatileCascadeStore>(this->subgroup_index).get_shard_num(),
                group->get_rpc_caller_id(),
                key, value, cascade_context_ptr);
    }

    debug_leave_func_with_value("version=0x{:x},timestamp={}", std::get<0>(version_and_timestamp), std::get<1>(version_and_timestamp));

    return version_and_timestamp;
}

template <typename KT, typename VT, KT* IK, VT* IV>
const VT VolatileCascadeStore<KT, VT, IK, IV>::ordered_get(const KT& key) {
    debug_enter_func_with_args("key={}", key);

    if(this->kv_map.find(key) != this->kv_map.end()) {
        debug_leave_func_with_value("key={}", key);
        return this->kv_map.at(key);
    } else {
        debug_leave_func();
        return *IV;
    }
}

template <typename KT, typename VT, KT* IK, VT* IV>
uint64_t VolatileCascadeStore<KT, VT, IK, IV>::ordered_get_size(const KT& key) {
    debug_enter_func_with_args("key={}", key);

    if(this->kv_map.find(key) != this->kv_map.end()) {
        return mutils::bytes_size(this->kv_map.at(key));
    } else {
        debug_leave_func();
        return 0;
    }
}

template <typename KT, typename VT, KT* IK, VT* IV>
void VolatileCascadeStore<KT, VT, IK, IV>::trigger_put(const VT& value) const {
    debug_enter_func_with_args("key={}", value.get_key_ref());

    if(cascade_watcher_ptr) {
        (*cascade_watcher_ptr)(
                this->subgroup_index,
                group->template get_subgroup<VolatileCascadeStore<KT, VT, IK, IV>>(this->subgroup_index).get_shard_num(),
                group->get_rpc_caller_id(),
                value.get_key_ref(), value, cascade_context_ptr, true);
    }

    debug_leave_func();
}

#ifdef ENABLE_EVALUATION
template <typename KT, typename VT, KT* IK, VT* IV>
void VolatileCascadeStore<KT, VT, IK, IV>::dump_timestamp_log(const std::string& filename) const {
    debug_enter_func_with_args("filename={}", filename);
    derecho::Replicated<VolatileCascadeStore>& subgroup_handle = group->template get_subgroup<VolatileCascadeStore>(this->subgroup_index);
    auto result = subgroup_handle.template ordered_send<RPC_NAME(ordered_dump_timestamp_log)>(filename);
    auto& replies = result.get();
    for(auto r : replies) {
        volatile uint32_t _ = r;
        _ = _;
    }
    debug_leave_func();
    return;
}

template <typename KT, typename VT, KT* IK, VT* IV>
void VolatileCascadeStore<KT, VT, IK, IV>::ordered_dump_timestamp_log(const std::string& filename) {
    debug_enter_func_with_args("filename={}", filename);
    global_timestamp_logger.flush(filename);
    debug_leave_func();
}
#ifdef DUMP_TIMESTAMP_WORKAROUND
template <typename KT, typename VT, KT* IK, VT* IV>
void VolatileCascadeStore<KT, VT, IK, IV>::dump_timestamp_log_workaround(const std::string& filename) const {
    debug_enter_func_with_args("filename={}", filename);
    global_timestamp_logger.flush(filename);
    debug_leave_func();
}
#endif
#endif  // ENABLE_EVALUATION

template <typename KT, typename VT, KT* IK, VT* IV>
std::unique_ptr<VolatileCascadeStore<KT, VT, IK, IV>> VolatileCascadeStore<KT, VT, IK, IV>::from_bytes(
        mutils::DeserializationManager* dsm,
        uint8_t const* buf) {
    auto kv_map_ptr = mutils::from_bytes<std::map<KT, VT>>(dsm, buf);
    auto update_version_ptr = mutils::from_bytes<persistent::version_t>(dsm, buf + mutils::bytes_size(*kv_map_ptr));
    auto volatile_cascade_store_ptr = std::make_unique<VolatileCascadeStore>(std::move(*kv_map_ptr),
                                                                             *update_version_ptr,
                                                                             dsm->registered<CriticalDataPathObserver<VolatileCascadeStore<KT, VT, IK, IV>>>() ? &(dsm->mgr<CriticalDataPathObserver<VolatileCascadeStore<KT, VT, IK, IV>>>()) : nullptr,
                                                                             dsm->registered<ICascadeContext>() ? &(dsm->mgr<ICascadeContext>()) : nullptr);
    return volatile_cascade_store_ptr;
}

template <typename KT, typename VT, KT* IK, VT* IV>
VolatileCascadeStore<KT, VT, IK, IV>::VolatileCascadeStore(
        CriticalDataPathObserver<VolatileCascadeStore<KT, VT, IK, IV>>* cw,
        ICascadeContext* cc) : lockless_v1(persistent::INVALID_VERSION),
                               lockless_v2(persistent::INVALID_VERSION),
                               update_version(persistent::INVALID_VERSION),
                               cascade_watcher_ptr(cw),
                               cascade_context_ptr(cc) {
    debug_enter_func();
    debug_leave_func();
}

template <typename KT, typename VT, KT* IK, VT* IV>
VolatileCascadeStore<KT, VT, IK, IV>::VolatileCascadeStore(
        const std::map<KT, VT>& _kvm,
        persistent::version_t _uv,
        CriticalDataPathObserver<VolatileCascadeStore<KT, VT, IK, IV>>* cw,
        ICascadeContext* cc) : lockless_v1(_uv),
                               lockless_v2(_uv),
                               kv_map(_kvm),
                               update_version(_uv),
                               cascade_watcher_ptr(cw),
                               cascade_context_ptr(cc) {
    debug_enter_func_with_args("copy to kv_map, size={}", kv_map.size());
    debug_leave_func();
}

template <typename KT, typename VT, KT* IK, VT* IV>
VolatileCascadeStore<KT, VT, IK, IV>::VolatileCascadeStore(
        std::map<KT, VT>&& _kvm,
        persistent::version_t _uv,
        CriticalDataPathObserver<VolatileCascadeStore<KT, VT, IK, IV>>* cw,
        ICascadeContext* cc) : lockless_v1(_uv),
                               lockless_v2(_uv),
                               kv_map(std::move(_kvm)),
                               update_version(_uv),
                               cascade_watcher_ptr(cw),
                               cascade_context_ptr(cc) {
    debug_enter_func_with_args("move to kv_map, size={}", kv_map.size());
    debug_leave_func();
}
}  // namespace cascade
}  // namespace derecho
