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
version_tuple VolatileCascadeStore<KT, VT, IK, IV>::put(const VT& value, bool as_trigger) const {
    debug_enter_func_with_args("value.get_key_ref={}", value.get_key_ref());
    LOG_TIMESTAMP_BY_TAG(TLT_VOLATILE_PUT_START, group, value);

    derecho::Replicated<VolatileCascadeStore>& subgroup_handle = group->template get_subgroup<VolatileCascadeStore>(this->subgroup_index);
    auto results = subgroup_handle.template ordered_send<RPC_NAME(ordered_put)>(value,as_trigger);
    auto& replies = results.get();
    version_tuple ret{CURRENT_VERSION, 0};
    // TODO: verfiy consistency ?
    for(auto& reply_pair : replies) {
        ret = reply_pair.second.get();
    }

    LOG_TIMESTAMP_BY_TAG(TLT_VOLATILE_PUT_END, group, value);
    debug_leave_func_with_value("version=0x{:x},timestamp={}us", std::get<0>(ret), std::get<1>(ret));
    return ret;
}

template <typename KT, typename VT, KT* IK, VT* IV>
void VolatileCascadeStore<KT, VT, IK, IV>::put_and_forget(const VT& value, bool as_trigger) const {
    debug_enter_func_with_args("value.get_key_ref={}", value.get_key_ref());
    LOG_TIMESTAMP_BY_TAG(TLT_VOLATILE_PUT_AND_FORGET_START, group, value);

    derecho::Replicated<VolatileCascadeStore>& subgroup_handle = group->template get_subgroup<VolatileCascadeStore>(this->subgroup_index);
    subgroup_handle.template ordered_send<RPC_NAME(ordered_put_and_forget)>(value,as_trigger);

    LOG_TIMESTAMP_BY_TAG(TLT_VOLATILE_PUT_AND_FORGET_END, group, value);
    debug_leave_func();
}

#ifdef ENABLE_EVALUATION

template <typename CascadeType>
double internal_perf_put(derecho::Replicated<CascadeType>& subgroup_handle, const uint64_t max_payload_size, const uint64_t duration_sec) {
    uint64_t num_messages_sent = 0;
    // make workload
    const uint32_t num_distinct_objects = 4096;
    std::vector<typename CascadeType::ObjectType> objects;
    if constexpr(std::is_convertible_v<typename CascadeType::KeyType, std::string>) {
        make_workload<typename CascadeType::KeyType, typename CascadeType::ObjectType>(max_payload_size, num_distinct_objects, "raw_key_", objects);
    } else if constexpr(std::is_integral_v<typename CascadeType::KeyType>) {
        make_workload<typename CascadeType::KeyType, typename CascadeType::ObjectType>(max_payload_size, num_distinct_objects, 10000, objects);
    } else {
        dbg_default_error("{} see unknown Key Type:{}", __PRETTY_FUNCTION__, typeid(typename CascadeType::KeyType).name());
        return 0;
    }
    uint64_t now_ns = get_walltime();
    uint64_t start_ns = now_ns;
    uint64_t end_ns = now_ns + duration_sec * INT64_1E9;
    while(end_ns > now_ns) {
        subgroup_handle.template ordered_send<RPC_NAME(ordered_put_and_forget)>(objects.at(now_ns % num_distinct_objects),false);
        now_ns = get_walltime();
        num_messages_sent++;
    }
    // send a normal put
    auto results = subgroup_handle.template ordered_send<RPC_NAME(ordered_put)>(objects.at(now_ns % num_distinct_objects),false);
    auto& replies = results.get();
    version_tuple ret(CURRENT_VERSION, 0);
    // TODO: verfiy consistency ?
    for(auto& reply_pair : replies) {
        ret = reply_pair.second.get();
    }
    now_ns = get_walltime();
    num_messages_sent++;

    return (num_messages_sent)*1e9 / (now_ns - start_ns);
}

template <typename KT, typename VT, KT* IK, VT* IV>
bool VolatileCascadeStore<KT, VT, IK, IV>::oob_send(uint64_t data_addr, uint64_t gpu_addr, uint64_t rkey, size_t size) const{
    // STEP 2 - do RDMA write to send the OOB data
    auto& subgroup_handle = group->template get_subgroup<VolatileCascadeStore>(this->subgroup_index);
        struct iovec iov;
	iov.iov_base    = reinterpret_cast<void*>(data_addr);                        iov.iov_len     = static_cast<size_t>(size);
       subgroup_handle.oob_remote_write(group->get_rpc_caller_id(),&iov,1,gpu_addr,rkey,size);
       subgroup_handle.wait_for_oob_op(group->get_rpc_caller_id(),OOB_OP_WRITE,1000);
       return true;

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
version_tuple VolatileCascadeStore<KT, VT, IK, IV>::remove(const KT& key) const {
    debug_enter_func_with_args("key={}", key);
    LOG_TIMESTAMP_BY_TAG(TLT_VOLATILE_REMOVE_START, group, *IV);
    derecho::Replicated<VolatileCascadeStore>& subgroup_handle = group->template get_subgroup<VolatileCascadeStore>(this->subgroup_index);
    auto results = subgroup_handle.template ordered_send<RPC_NAME(ordered_remove)>(key);
    auto& replies = results.get();
    version_tuple ret(CURRENT_VERSION, 0);
    // TODO: verify consistency ?
    for(auto& reply_pair : replies) {
        ret = reply_pair.second.get();
    }
    LOG_TIMESTAMP_BY_TAG(TLT_VOLATILE_REMOVE_END, group, *IV);
    debug_leave_func_with_value("version=0x{:x},timestamp={}us", std::get<0>(ret), std::get<1>(ret));
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
    LOG_TIMESTAMP_BY_TAG(TLT_VOLATILE_GET_START, group, *IV);

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
        /* 
         * An out_of_range exception can be thrown even if 'key' exists in
         * kv_map. Since std::map is not thread-safe, and there is another
         * thread modifying kv_map concurrently, the internal data structure can
         * be changed while this thread is inside kv_map.at(key). Therefore, we
         * keep trying until it is possible to copy either the object we are
         * looking for, or the invalid object.
         */
        while(true) {
            try {
                if(this->kv_map.find(key) != this->kv_map.end()) {
                    copied_out.copy_from(this->kv_map.at(key));
                } else {
                    copied_out.copy_from(*IV);
                }

                break;
            } catch (const std::out_of_range&) {
                dbg_default_debug("{}: out_of_range exception thrown while trying to get key {}", __PRETTY_FUNCTION__, key);
            }
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
    LOG_TIMESTAMP_BY_TAG(TLT_VOLATILE_GET_END, group, *IV);
    return copied_out;
}

template <typename KT, typename VT, KT* IK, VT* IV>
const VT VolatileCascadeStore<KT, VT, IK, IV>::multi_get(const KT& key) const {
    debug_enter_func_with_args("key={}", key);
    LOG_TIMESTAMP_BY_TAG(TLT_VOLATILE_MULTI_GET_START, group, *IV);

    derecho::Replicated<VolatileCascadeStore>& subgroup_handle = group->template get_subgroup<VolatileCascadeStore>(this->subgroup_index);
    auto results = subgroup_handle.template ordered_send<RPC_NAME(ordered_get)>(key);
    auto& replies = results.get();
    // TODO: verify consistency ?
    for(auto& reply_pair : replies) {
        reply_pair.second.wait();
    }
    debug_leave_func();
    LOG_TIMESTAMP_BY_TAG(TLT_VOLATILE_MULTI_GET_END, group, *IV);
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

    LOG_TIMESTAMP_BY_TAG(TLT_VOLATILE_MULTI_LIST_KEYS_START, group, *IV);
    derecho::Replicated<VolatileCascadeStore>& subgroup_handle = group->template get_subgroup<VolatileCascadeStore>(this->subgroup_index);
    auto results = subgroup_handle.template ordered_send<RPC_NAME(ordered_list_keys)>(prefix);
    auto& replies = results.get();
    std::vector<KT> ret;
    // TODO: verfity consistency ?
    for(auto& reply_pair : replies) {
        ret = reply_pair.second.get();
    }
    LOG_TIMESTAMP_BY_TAG(TLT_VOLATILE_MULTI_LIST_KEYS_END, group, *IV);

    debug_leave_func();
    return ret;
}

template <typename KT, typename VT, KT* IK, VT* IV>
std::vector<KT> VolatileCascadeStore<KT, VT, IK, IV>::list_keys(const std::string& prefix, const persistent::version_t& ver, const bool) const {
    if(ver != CURRENT_VERSION) {
        debug_leave_func_with_value("Cannot support versioned list_keys, ver=0x{:x}", ver);
        return {};
    }

    LOG_TIMESTAMP_BY_TAG(TLT_VOLATILE_LIST_KEYS_START, group, *IV);
    // copy key list out
    std::vector<KT> key_list;
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
        for(auto kv : this->kv_map) {
            if(get_pathname<KT>(kv.first).find(prefix) == 0) {
                key_list.push_back(kv.first);
            }
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
    LOG_TIMESTAMP_BY_TAG(TLT_VOLATILE_LIST_KEYS_END, group, *IV);

    return key_list;
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

    LOG_TIMESTAMP_BY_TAG(TLT_VOLATILE_MULTI_GET_SIZE_START, group, *IV);
    derecho::Replicated<VolatileCascadeStore>& subgroup_handle = group->template get_subgroup<VolatileCascadeStore>(this->subgroup_index);
    auto results = subgroup_handle.template ordered_send<RPC_NAME(ordered_get_size)>(key);
    auto& replies = results.get();
    // TODO: verify consistency ?
    // for (auto& reply_pair : replies) {
    //     ret = reply_pair.second.get();
    // }
    uint64_t ret = replies.begin()->second.get();
    LOG_TIMESTAMP_BY_TAG(TLT_VOLATILE_MULTI_GET_SIZE_END, group, *IV);
    debug_leave_func();

    return ret;
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
    LOG_TIMESTAMP_BY_TAG(TLT_VOLATILE_GET_SIZE_START, group, *IV);
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
    LOG_TIMESTAMP_BY_TAG(TLT_VOLATILE_GET_SIZE_END, group, *IV);
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
    debug_enter_func();

#ifdef ENABLE_EVALUATION
    auto version_and_hlc = group->template get_subgroup<VolatileCascadeStore>(this->subgroup_index).get_current_version();
#endif

#if __cplusplus > 201703L
    LOG_TIMESTAMP_BY_TAG(TLT_VOLATILE_ORDERED_LIST_KEYS_START,group,*IV,std::get<0>(version_and_hlc));
#else
    LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_VOLATILE_ORDERED_LIST_KEYS_START,group,*IV,std::get<0>(version_and_hlc));
#endif
    std::vector<KT> key_list;
    for(auto kv : this->kv_map) {
        if(get_pathname<KT>(kv.first).find(prefix) == 0) {
            key_list.push_back(kv.first);
        }
    }
#if __cplusplus > 201703L
    LOG_TIMESTAMP_BY_TAG(TLT_VOLATILE_ORDERED_LIST_KEYS_END,group,*IV,std::get<0>(version_and_hlc));
#else
    LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_VOLATILE_ORDERED_LIST_KEYS_END,group,*IV,std::get<0>(version_and_hlc));
#endif

    debug_leave_func();
    return key_list;
}

template <typename KT, typename VT, KT* IK, VT* IV>
version_tuple VolatileCascadeStore<KT, VT, IK, IV>::ordered_put(const VT& value, bool as_trigger) {
    debug_enter_func_with_args("key={}", value.get_key_ref());

    auto version_and_hlc = group->template get_subgroup<VolatileCascadeStore>(this->subgroup_index).get_current_version();

#if __cplusplus > 201703L
    LOG_TIMESTAMP_BY_TAG(TLT_VOLATILE_ORDERED_PUT_START,group,value,std::get<0>(version_and_hlc));
#else
    LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_VOLATILE_ORDERED_PUT_START,group,value,std::get<0>(version_and_hlc));
#endif

    version_tuple version_and_timestamp{persistent::INVALID_VERSION, 0};

    if(this->internal_ordered_put(value,as_trigger) == true) {
        version_and_timestamp = {std::get<0>(version_and_hlc),std::get<1>(version_and_hlc).m_rtc_us};
    }

#if __cplusplus > 201703L
    LOG_TIMESTAMP_BY_TAG(TLT_VOLATILE_ORDERED_PUT_END,group,value,std::get<0>(version_and_hlc));
#else
    LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_VOLATILE_ORDERED_PUT_END,group,value,std::get<0>(version_and_hlc));
#endif

    debug_leave_func_with_value("version=0x{:x},timestamp={}us",
            std::get<0>(version_and_hlc),
            std::get<1>(version_and_hlc).m_rtc_us);

    return version_and_timestamp;
}

template <typename KT, typename VT, KT* IK, VT* IV>
void VolatileCascadeStore<KT, VT, IK, IV>::ordered_put_and_forget(const VT& value, bool as_trigger) {
    debug_enter_func_with_args("key={}", value.get_key_ref());
#ifdef ENABLE_EVALUATION
    auto version_and_hlc = group->template get_subgroup<VolatileCascadeStore>(this->subgroup_index).get_current_version();
#endif
#if __cplusplus > 201703L
    LOG_TIMESTAMP_BY_TAG(TLT_VOLATILE_ORDERED_PUT_AND_FORGET_START,group,value,std::get<0>(version_and_hlc));
#else
    LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_VOLATILE_ORDERED_PUT_AND_FORGET_START,group,value,std::get<0>(version_and_hlc));
#endif
    internal_ordered_put(value,as_trigger);
#if __cplusplus > 201703L
    LOG_TIMESTAMP_BY_TAG(TLT_VOLATILE_ORDERED_PUT_AND_FORGET_END,group,value,std::get<0>(version_and_hlc));
#else
    LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_VOLATILE_ORDERED_PUT_AND_FORGET_END,group,value,std::get<0>(version_and_hlc));
#endif
    debug_leave_func();
}

template <typename KT, typename VT, KT* IK, VT* IV>
bool VolatileCascadeStore<KT, VT, IK, IV>::internal_ordered_put(const VT& value, bool as_trigger) {
    auto version_and_hlc = group->template get_subgroup<VolatileCascadeStore>(this->subgroup_index).get_current_version();

    if constexpr(std::is_base_of<IKeepVersion, VT>::value) {
        value.set_version(std::get<0>(version_and_hlc));
    }
    if constexpr(std::is_base_of<IKeepTimestamp, VT>::value) {
        value.set_timestamp(std::get<1>(version_and_hlc).m_rtc_us);
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

    if (!as_trigger) {
    // for lockless check
    this->lockless_v1.store(std::get<0>(version_and_hlc), std::memory_order_relaxed);
    // compiler reordering barrier
#ifdef __GNUC__
        asm volatile("" ::
                         : "memory");
#else
#error Lockless support is currently for GCC only
#endif

        this->kv_map.erase(value.get_key_ref());           // remove
        this->kv_map.emplace(value.get_key_ref(), value);  // copy constructor
        this->update_version = std::get<0>(version_and_hlc);

        // for lockless check
        // compiler reordering barrier
#ifdef __GNUC__
        asm volatile("" ::
                         : "memory");
#else
#error Lockless support is currently for GCC only
#endif
        this->lockless_v2.store(std::get<0>(version_and_hlc), std::memory_order_relaxed);
    }

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
version_tuple VolatileCascadeStore<KT, VT, IK, IV>::ordered_remove(const KT& key) {
    debug_enter_func_with_args("key={}", key);

    auto version_and_hlc = group->template get_subgroup<VolatileCascadeStore>(this->subgroup_index).get_current_version();

#if __cplusplus > 201703L
    LOG_TIMESTAMP_BY_TAG(TLT_VOLATILE_ORDERED_REMOVE_START,group,*IV,std::get<0>(version_and_hlc));
#else
    LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_VOLATILE_ORDERED_REMOVE_START,group,*IV,std::get<0>(version_and_hlc));
#endif

    if(this->kv_map.find(key) == this->kv_map.end()) {
        debug_leave_func_with_value("version=0x{:x},timestamp={}us",
                std::get<0>(version_and_hlc), 
                std::get<1>(version_and_hlc).m_rtc_us);
        return {std::get<0>(version_and_hlc),
                std::get<1>(version_and_hlc).m_rtc_us};
    }

    auto value = create_null_object_cb<KT, VT, IK, IV>(key);

    if constexpr(std::is_base_of<IKeepVersion, VT>::value) {
        value.set_version(std::get<0>(version_and_hlc));
    }
    if constexpr(std::is_base_of<IKeepTimestamp, VT>::value) {
        value.set_timestamp(std::get<1>(version_and_hlc).m_rtc_us);
    }
    if constexpr(std::is_base_of<IKeepPreviousVersion, VT>::value) {
        if(this->kv_map.find(key) != this->kv_map.end()) {
            value.set_previous_version(this->update_version, this->kv_map.at(key).get_version());
        } else {
            value.set_previous_version(this->update_version, persistent::INVALID_VERSION);
        }
    }

    // for lockless check
    this->lockless_v1.store(std::get<0>(version_and_hlc), std::memory_order_relaxed);
    // compiler reordering barrier
#ifdef __GNUC__
    asm volatile("" ::
                         : "memory");
#else
#error Lockless support is currently for GCC only
#endif

    this->kv_map.erase(key);  // remove
    this->kv_map.emplace(key, value);
    this->update_version = std::get<0>(version_and_hlc);

    // for lockless check
    // compiler reordering barrier
#ifdef __GNUC__
    asm volatile("" ::
                         : "memory");
#else
#error Lockless support is currently for GCC only
#endif
    this->lockless_v2.store(std::get<0>(version_and_hlc), std::memory_order_relaxed);

    if(cascade_watcher_ptr) {
        (*cascade_watcher_ptr)(
                // group->template get_subgroup<VolatileCascadeStore>(this->subgroup_index).get_subgroup_id(), // this is subgroup id
                this->subgroup_index,
                group->template get_subgroup<VolatileCascadeStore>(this->subgroup_index).get_shard_num(),
                group->get_rpc_caller_id(),
                key, value, cascade_context_ptr);
    }

#if __cplusplus > 201703L
    LOG_TIMESTAMP_BY_TAG(TLT_VOLATILE_ORDERED_REMOVE_END,group,*IV,std::get<0>(version_and_hlc));
#else
    LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_VOLATILE_ORDERED_REMOVE_END,group,*IV,std::get<0>(version_and_hlc));
#endif

    debug_leave_func_with_value("version=0x{:x},timestamp={}us",
            std::get<0>(version_and_hlc),
            std::get<1>(version_and_hlc).m_rtc_us);

    return {std::get<0>(version_and_hlc),
            std::get<1>(version_and_hlc).m_rtc_us};
}

template <typename KT, typename VT, KT* IK, VT* IV>
const VT VolatileCascadeStore<KT, VT, IK, IV>::ordered_get(const KT& key) {
    debug_enter_func_with_args("key={}", key);
#ifdef ENABLE_EVALUATION
    auto version_and_hlc = group->template get_subgroup<VolatileCascadeStore>(this->subgroup_index).get_current_version();
#endif

#if __cplusplus > 201703L
    LOG_TIMESTAMP_BY_TAG(TLT_VOLATILE_ORDERED_GET_START,group,*IV,std::get<0>(version_and_hlc));
#else
    LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_VOLATILE_ORDERED_GET_START,group,*IV,std::get<0>(version_and_hlc));
#endif

    if(this->kv_map.find(key) != this->kv_map.end()) {
        debug_leave_func_with_value("key={}", key);
#if __cplusplus > 201703L
    LOG_TIMESTAMP_BY_TAG(TLT_VOLATILE_ORDERED_GET_END,group,*IV,std::get<0>(version_and_hlc));
#else
    LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_VOLATILE_ORDERED_GET_END,group,*IV,std::get<0>(version_and_hlc));
#endif
        return this->kv_map.at(key);
    } else {
#if __cplusplus > 201703L
    LOG_TIMESTAMP_BY_TAG(TLT_VOLATILE_ORDERED_GET_END,group,*IV,std::get<0>(version_and_hlc));
#else
    LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_VOLATILE_ORDERED_GET_END,group,*IV,std::get<0>(version_and_hlc));
#endif
        debug_leave_func();
        return *IV;
    }
}

template <typename KT, typename VT, KT* IK, VT* IV>
uint64_t VolatileCascadeStore<KT, VT, IK, IV>::ordered_get_size(const KT& key) {
    debug_enter_func_with_args("key={}", key);

#ifdef ENABLE_EVALUATION
    auto version_and_hlc = group->template get_subgroup<VolatileCascadeStore>(this->subgroup_index).get_current_version();
#endif

#if __cplusplus > 201703L
    LOG_TIMESTAMP_BY_TAG(TLT_VOLATILE_ORDERED_GET_SIZE_START,group,*IV,std::get<0>(version_and_hlc));
#else
    LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_VOLATILE_ORDERED_GET_SIZE_START,group,*IV,std::get<0>(version_and_hlc));
#endif

    if(this->kv_map.find(key) != this->kv_map.end()) {
#if __cplusplus > 201703L
    LOG_TIMESTAMP_BY_TAG(TLT_VOLATILE_ORDERED_GET_SIZE_END,group,*IV,std::get<0>(version_and_hlc));
#else
    LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_VOLATILE_ORDERED_GET_SIZE_END,group,*IV,std::get<0>(version_and_hlc));
#endif
        return mutils::bytes_size(this->kv_map.at(key));
    } else {
#if __cplusplus > 201703L
    LOG_TIMESTAMP_BY_TAG(TLT_VOLATILE_ORDERED_GET_SIZE_END,group,*IV,std::get<0>(version_and_hlc));
#else
    LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_VOLATILE_ORDERED_GET_SIZE_END,group,*IV,std::get<0>(version_and_hlc));
#endif
        debug_leave_func();
        return 0;
    }
}

template <typename KT, typename VT, KT* IK, VT* IV>
void VolatileCascadeStore<KT, VT, IK, IV>::trigger_put(const VT& value) const {
    debug_enter_func_with_args("key={}", value.get_key_ref());

    LOG_TIMESTAMP_BY_TAG(TLT_VOLATILE_TRIGGER_PUT_START, group, value);
    if(cascade_watcher_ptr) {
        (*cascade_watcher_ptr)(
                this->subgroup_index,
                group->template get_subgroup<VolatileCascadeStore<KT, VT, IK, IV>>(this->subgroup_index).get_shard_num(),
                group->get_rpc_caller_id(),
                value.get_key_ref(), value, cascade_context_ptr, true);
    }
    LOG_TIMESTAMP_BY_TAG(TLT_VOLATILE_TRIGGER_PUT_END, group, value);

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
    TimestampLogger::flush(filename);
    debug_leave_func();
}
#ifdef DUMP_TIMESTAMP_WORKAROUND
template <typename KT, typename VT, KT* IK, VT* IV>
void VolatileCascadeStore<KT, VT, IK, IV>::dump_timestamp_log_workaround(const std::string& filename) const {
    debug_enter_func_with_args("filename={}", filename);
    TimestampLogger::flush(filename);
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
