#pragma once
#include <derecho/conf/conf.hpp>
#include <derecho/persistent/PersistentInterface.hpp>
#include <derecho/persistent/detail/PersistLog.hpp>
#include <memory>
#include <map>
#include <type_traits>
#include <fstream>
#include <cascade/utils.hpp>

#ifdef ENABLE_EVALUATION
#include <derecho/utils/time.h>
#endif

namespace derecho {
namespace cascade {

#ifdef ENABLE_EVALUATION
#define NUMBER_OF_DISTINCT_OBJECTS (4096)

/**
 * This is a hidden API.
 * TestValueTypeConstructor is to check if the ValueType has a public constructor support such a call:
 * VT(KT,uint8_t*,uint32_t)
 */
template <typename KT,typename VT>
struct TestVTConstructor {
    template <class X,class = decltype(X(KT{},static_cast<uint8_t*>(nullptr),0))>
        static std::true_type test(X*);
    template <class X>
        static std::false_type test(...);

    static constexpr bool value = decltype(test<VT>(0))::value;
};

template<typename KT, typename VT>
void make_workload(uint32_t payload_size, const KT& key_prefix, std::vector<VT>& objects) {
    if constexpr (TestVTConstructor<KT,VT>::value) {
        const uint32_t buf_size = payload_size - 128 - sizeof(key_prefix);
        uint8_t* buf = (uint8_t*)malloc(buf_size);
        memset(buf,'A',buf_size);
        for (uint32_t i=0;i<NUMBER_OF_DISTINCT_OBJECTS;i++) {
            if constexpr (std::is_convertible_v<KT,std::string>) {
                objects.emplace_back(key_prefix+std::to_string(i),buf,buf_size);
            } else if constexpr (std::is_integral_v<KT>) {
                objects.emplace_back(key_prefix+i,buf,buf_size);
            } else {
                dbg_default_error("Cannot make workload for key type:{}",typeid(KT).name());
                break;
            }
        }
        free(buf);
    } else {
        dbg_default_error("Cannot make workload for key type:{} and value type:{}, because it does not support constructor:VT(KT,uint8_t*,uint32_t)",typeid(KT).name(),typeid(VT).name());
    }
}

#if __cplusplus > 201703L
// C++ 20
#define LOG_TIMESTAMP_BY_TAG(t,g,v,...) \
    if constexpr (std::is_base_of<IHasMessageID, std::decay_t<decltype(v)>>::value) { \
        global_timestamp_logger.log(t, \
                                    g->get_my_id(), \
                                    dynamic_cast<const IHasMessageID*>(&(v))->get_message_id(), \
                                    get_walltime() \
                                    __VA_OPT__(,) __VA_ARGS__); \
    }
#else
// C++ 17
#define LOG_TIMESTAMP_BY_TAG(t,g,v) \
    if constexpr (std::is_base_of<IHasMessageID, std::decay_t<decltype(v)>>::value) { \
        global_timestamp_logger.log(t, \
                                    g->get_my_id(), \
                                    dynamic_cast<const IHasMessageID*>(&(v))->get_message_id(), \
                                    get_walltime()); \
    }

#define LOG_TIMESTAMP_BY_TAG_EXTRA(t,g,v,e) \
    if constexpr (std::is_base_of<IHasMessageID, std::decay_t<decltype(v)>>::value) { \
        global_timestamp_logger.log(t, \
                                    g->get_my_id(), \
                                    dynamic_cast<const IHasMessageID*>(&(v))->get_message_id(), \
                                    get_walltime(),e); \
    }

#endif//__cplusplus > 201703L

#else

#if __cplusplus > 201703L
#define LOG_TIMESTAMP_BY_TAG(t,g,v,...)
#else
#define LOG_TIMESTAMP_BY_TAG(t,g,v)
#define LOG_TIMESTAMP_BY_TAG_EXTRA(t,g,v,e)
#endif

#endif//ENABLE_EVALUATION


template<typename KeyType>
std::string get_pathname(const std::enable_if_t<std::is_convertible<KeyType,std::string>::value,std::string>& key) {
    const std::string* pstr = dynamic_cast<const std::string*>(&key);
    size_t pos = pstr->rfind(PATH_SEPARATOR);
    if (pos != std::string::npos) {
        return pstr->substr(0,pos);
    }
    return "";
}

template<typename KeyType>
std::string get_pathname(const std::enable_if_t<!std::is_convertible<KeyType,std::string>::value,KeyType>& key) {
    return "";
}


///////////////////////////////////////////////////////////////////////////////
// 1 - Volatile Cascade Store Implementation
///////////////////////////////////////////////////////////////////////////////

template<typename KT, typename VT, KT* IK, VT* IV>
std::tuple<persistent::version_t,uint64_t> VolatileCascadeStore<KT,VT,IK,IV>::put(const VT& value) const {
    debug_enter_func_with_args("value.get_key_ref={}",value.get_key_ref());
    LOG_TIMESTAMP_BY_TAG(TLT_VOLATILE_PUT_START,group,value);

    derecho::Replicated<VolatileCascadeStore>& subgroup_handle = group->template get_subgroup<VolatileCascadeStore>(this->subgroup_index);
    auto results = subgroup_handle.template ordered_send<RPC_NAME(ordered_put)>(value);
    auto& replies = results.get();
    std::tuple<persistent::version_t,uint64_t> ret(CURRENT_VERSION,0);
    // TODO: verfiy consistency ?
    for (auto& reply_pair : replies) {
        ret = reply_pair.second.get();
    }

    LOG_TIMESTAMP_BY_TAG(TLT_VOLATILE_PUT_END,group,value);
    debug_leave_func_with_value("version=0x{:x},timestamp={}",std::get<0>(ret),std::get<1>(ret));
    return ret;
}

template<typename KT, typename VT, KT* IK, VT* IV>
void VolatileCascadeStore<KT,VT,IK,IV>::put_and_forget(const VT& value) const {
    debug_enter_func_with_args("value.get_key_ref={}",value.get_key_ref());
    LOG_TIMESTAMP_BY_TAG(TLT_VOLATILE_PUT_AND_FORGET_START,group,value);

    derecho::Replicated<VolatileCascadeStore>& subgroup_handle = group->template get_subgroup<VolatileCascadeStore>(this->subgroup_index);
    subgroup_handle.template ordered_send<RPC_NAME(ordered_put_and_forget)>(value);

    LOG_TIMESTAMP_BY_TAG(TLT_VOLATILE_PUT_AND_FORGET_END,group,value);
    debug_leave_func();
}

#ifdef ENABLE_EVALUATION

template <typename CascadeType>
double internal_perf_put(derecho::Replicated<CascadeType>& subgroup_handle, const uint64_t max_payload_size, const uint64_t duration_sec) {
    uint64_t num_messages_sent = 0;
    // make workload
    std::vector<typename CascadeType::ObjectType> objects;
    if constexpr (std::is_convertible_v<typename CascadeType::KeyType,std::string>) {
        make_workload<typename CascadeType::KeyType,typename CascadeType::ObjectType>(max_payload_size,"raw_key_",objects);
    } else if constexpr (std::is_integral_v<typename CascadeType::KeyType>) {
        make_workload<typename CascadeType::KeyType,typename CascadeType::ObjectType>(max_payload_size,10000,objects);
    } else {
        dbg_default_error("{} see unknown Key Type:{}",__PRETTY_FUNCTION__,typeid(typename CascadeType::KeyType).name());
        return 0;
    }
    uint64_t now_ns = get_walltime();
    uint64_t start_ns = now_ns;
    uint64_t end_ns = now_ns + duration_sec*1000000000;
    while(end_ns > now_ns) {
        subgroup_handle.template ordered_send<RPC_NAME(ordered_put_and_forget)>(objects.at(now_ns%NUMBER_OF_DISTINCT_OBJECTS));
        now_ns = get_walltime();
        num_messages_sent ++;
    }
    // send a normal put
    auto results = subgroup_handle.template ordered_send<RPC_NAME(ordered_put)>(objects.at(now_ns%NUMBER_OF_DISTINCT_OBJECTS));
    auto& replies = results.get();
    std::tuple<persistent::version_t,uint64_t> ret(CURRENT_VERSION,0);
    // TODO: verfiy consistency ?
    for (auto& reply_pair : replies) {
        ret = reply_pair.second.get();
    }
    now_ns = get_walltime();
    num_messages_sent ++;

    return (num_messages_sent)*1e9/(now_ns-start_ns);
}

template<typename KT, typename VT, KT* IK, VT* IV>
double VolatileCascadeStore<KT,VT,IK,IV>::perf_put(const uint32_t max_payload_size, const uint64_t duration_sec) const {
    debug_enter_func_with_args("max_payload_size={},duration_sec={}",max_payload_size,duration_sec);
    derecho::Replicated<VolatileCascadeStore>& subgroup_handle = group->template get_subgroup<VolatileCascadeStore>(this->subgroup_index);
    double ops = internal_perf_put(subgroup_handle,max_payload_size,duration_sec);
    debug_leave_func_with_value("{} ops.",ops);
    return ops;
}

#endif

template<typename KT, typename VT, KT* IK, VT* IV>
std::tuple<persistent::version_t,uint64_t> VolatileCascadeStore<KT,VT,IK,IV>::remove(const KT& key) const {
    debug_enter_func_with_args("key={}",key);
    derecho::Replicated<VolatileCascadeStore>& subgroup_handle = group->template get_subgroup<VolatileCascadeStore>(this->subgroup_index);
    auto results = subgroup_handle.template ordered_send<RPC_NAME(ordered_remove)>(key);
    auto& replies = results.get();
    std::tuple<persistent::version_t,uint64_t> ret(CURRENT_VERSION,0);
    // TODO: verify consistency ?
    for (auto& reply_pair : replies) {
        ret = reply_pair.second.get();
    }
    debug_leave_func_with_value("version=0x{:x},timestamp={}",std::get<0>(ret),std::get<1>(ret));
    return ret;
}

// both stable and exact are ignored for VolatileCascadeStore
template<typename KT, typename VT, KT* IK, VT* IV>
const VT VolatileCascadeStore<KT,VT,IK,IV>::get(const KT& key, const persistent::version_t& ver, bool, bool) const {
    debug_enter_func_with_args("key={},ver=0x{:x}",key,ver);
    if (ver != CURRENT_VERSION) {
        debug_leave_func_with_value("Cannot support versioned get, ver=0x{:x}", ver);
        return *IV;
    }

    // copy data out
    persistent::version_t v1,v2;
    static thread_local VT copied_out;
    do {
        // This only for TSO memory reordering.
        v2 = this->lockless_v2.load(std::memory_order_relaxed);
        // compiler reordering barrier
#ifdef __GNUC__
        asm volatile("" ::: "memory");
#else
#error Lockless support is currently for GCC only
#endif
        if (this->kv_map.find(key) != this->kv_map.end()) {
            copied_out.copy_from(this->kv_map.at(key));
        } else {
            copied_out.copy_from(*IV);
        }
        // compiler reordering barrier
#ifdef __GNUC__
        asm volatile("" ::: "memory");
#else
#error Lockless support is currently for GCC only
#endif
        v1 = this->lockless_v1.load(std::memory_order_relaxed);
        // busy sleep
        std::this_thread::yield();
    } while(v1!=v2);
    return copied_out;
}

template<typename KT, typename VT, KT* IK, VT* IV>
const VT VolatileCascadeStore<KT,VT,IK,IV>::multi_get(const KT& key) const {
    debug_enter_func_with_args("key={}",key);

    derecho::Replicated<VolatileCascadeStore>& subgroup_handle = group->template get_subgroup<VolatileCascadeStore>(this->subgroup_index);
    auto results = subgroup_handle.template ordered_send<RPC_NAME(ordered_get)>(key);
    auto& replies = results.get();
    // TODO: verify consistency ?
    for (auto& reply_pair : replies) {
        reply_pair.second.wait();
    }
    debug_leave_func();
    return replies.begin()->second.get();
}

template<typename KT, typename VT, KT* IK, VT* IV>
const VT VolatileCascadeStore<KT,VT,IK,IV>::get_by_time(const KT& key, const uint64_t& ts_us, const bool stable) const {
    // VolatileCascadeStore does not support this.
    debug_enter_func();
    debug_leave_func();

    return *IV;
}

template<typename KT, typename VT, KT* IK, VT* IV>
std::vector<KT> VolatileCascadeStore<KT,VT,IK,IV>::multi_list_keys(const std::string& prefix) const {
    debug_enter_func_with_args("prefix={}",prefix);
    derecho::Replicated<VolatileCascadeStore>& subgroup_handle = group->template get_subgroup<VolatileCascadeStore>(this->subgroup_index);
    auto results = subgroup_handle.template ordered_send<RPC_NAME(ordered_list_keys)>(prefix);
    auto& replies = results.get();
    std::vector<KT> ret;
    // TODO: verfity consistency ?
    for (auto& reply_pair : replies) {
        ret = reply_pair.second.get();
    }
    debug_leave_func();
    return ret;
}

template<typename KT, typename VT, KT* IK, VT* IV>
std::vector<KT> VolatileCascadeStore<KT,VT,IK,IV>::list_keys(const std::string& prefix, const persistent::version_t& ver, const bool) const {
    if (ver != CURRENT_VERSION) {
        debug_leave_func_with_value("Cannot support versioned list_keys, ver=0x{:x}", ver);
        return {};
    }

    return multi_list_keys(prefix);
}

template<typename KT, typename VT, KT* IK, VT* IV>
std::vector<KT> VolatileCascadeStore<KT,VT,IK,IV>::list_keys_by_time(const std::string& prefix, const uint64_t& ts_us, const bool) const {
    // VolatileCascadeStore does not support this.
    debug_enter_func_with_args("ts_us=0x{:x}", ts_us);
    debug_leave_func();
    return {};
}

template<typename KT, typename VT, KT* IK, VT* IV>
uint64_t VolatileCascadeStore<KT,VT,IK,IV>::multi_get_size(const KT& key) const {
    debug_enter_func_with_args("key={}",key);
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


template<typename KT, typename VT, KT* IK, VT* IV>
uint64_t VolatileCascadeStore<KT,VT,IK,IV>::get_size(const KT& key, const persistent::version_t& ver, const bool, const bool) const {
    debug_enter_func_with_args("key={},ver=0x{:x}",key,ver);
    if (ver != CURRENT_VERSION) {
        debug_leave_func_with_value("Cannot support versioned get, ver=0x{:x}", ver);
        return 0;
    }
    debug_leave_func();

    // copy data out
    persistent::version_t v1,v2;
    uint64_t size = 0ull;
    do {
        // This only for TSO memory reordering.
        v2 = this->lockless_v2.load(std::memory_order_relaxed);
        // compiler reordering barrier
#ifdef __GNUC__
        asm volatile("" ::: "memory");
#else
#error Lockless support is currently for GCC only
#endif
        if (this->kv_map.find(key) != this->kv_map.end()) {
            size = mutils::bytes_size(this->kv_map.at(key));
        }
        // compiler reordering barrier
#ifdef __GNUC__
        asm volatile("" ::: "memory");
#else
#error Lockless support is currently for GCC only
#endif
        v1 = this->lockless_v1.load(std::memory_order_relaxed);
        // busy sleep
        std::this_thread::yield();
    } while(v1!=v2);
    return size;
}

template<typename KT, typename VT, KT* IK, VT* IV>
uint64_t VolatileCascadeStore<KT,VT,IK,IV>::get_size_by_time(const KT&, const uint64_t&, const bool) const {
    // VolatileCascadeStore does not support this.
    debug_enter_func();

    debug_leave_func();
    return 0;
}

template<typename KT, typename VT, KT* IK, VT* IV>
std::vector<KT> VolatileCascadeStore<KT,VT,IK,IV>::ordered_list_keys(const std::string& prefix) {
    std::vector<KT> key_list;
    debug_enter_func();
    for(auto kv: this->kv_map) {
        if (get_pathname<KT>(kv.first).find(prefix) == 0) {
            key_list.push_back(kv.first);
        }
    }

    debug_leave_func();
    return key_list;
}

template<typename KT, typename VT, KT* IK, VT* IV>
std::tuple<persistent::version_t,uint64_t> VolatileCascadeStore<KT,VT,IK,IV>::ordered_put(const VT& value) {
    debug_enter_func_with_args("key={}",value.get_key_ref());

    std::tuple<persistent::version_t,uint64_t> version_and_timestamp = group->template get_subgroup<VolatileCascadeStore>(this->subgroup_index).get_current_version();

    if(this->internal_ordered_put(value)==false) {
        version_and_timestamp = {persistent::INVALID_VERSION,0};
    }

    debug_leave_func_with_value("version=0x{:x},timestamp={}",std::get<0>(version_and_timestamp), std::get<1>(version_and_timestamp));

    return version_and_timestamp;
}

template<typename KT, typename VT, KT* IK, VT* IV>
void VolatileCascadeStore<KT,VT,IK,IV>::ordered_put_and_forget(const VT& value) {
    debug_enter_func_with_args("key={}",value.get_key_ref());
    LOG_TIMESTAMP_BY_TAG(TLT_VOLATILE_ORDERED_PUT_AND_FORGET_START,group,value);
    internal_ordered_put(value);
    LOG_TIMESTAMP_BY_TAG(TLT_VOLATILE_ORDERED_PUT_AND_FORGET_END,group,value);
    debug_leave_func();
}

template<typename KT, typename VT, KT* IK, VT* IV>
bool VolatileCascadeStore<KT,VT,IK,IV>::internal_ordered_put(const VT& value) {
    std::tuple<persistent::version_t,uint64_t> version_and_timestamp = group->template get_subgroup<VolatileCascadeStore>(this->subgroup_index).get_current_version();

    if constexpr (std::is_base_of<IKeepVersion,VT>::value) {
        value.set_version(std::get<0>(version_and_timestamp));
    }
    if constexpr (std::is_base_of<IKeepTimestamp,VT>::value) {
        value.set_timestamp(std::get<1>(version_and_timestamp));
    }

    // validator
    if constexpr (std::is_base_of<IValidator<KT,VT>,VT>::value) {
        if(!value.validate(this->kv_map)) {
            return false;
        }
    }

    // Verify previous version MUST happen before update previous versions.
    if constexpr (std::is_base_of<IVerifyPreviousVersion,VT>::value) {
        bool verify_result;
        if (this->kv_map.find(value.get_key_ref())!=this->kv_map.end()) {
            verify_result = value.verify_previous_version(this->update_version,this->kv_map.at(value.get_key_ref()).get_version());
        } else {
            verify_result = value.verify_previous_version(this->update_version,persistent::INVALID_VERSION);
        }
        if (!verify_result) {
            // reject the update by returning an invalid version and timestamp
            return false;
        }
    }
    if constexpr (std::is_base_of<IKeepPreviousVersion,VT>::value) {
        if (this->kv_map.find(value.get_key_ref())!=this->kv_map.end()) {
            value.set_previous_version(this->update_version,this->kv_map.at(value.get_key_ref()).get_version());
        } else {
            value.set_previous_version(this->update_version,persistent::INVALID_VERSION);
        }
    }

    // for lockless check
    this->lockless_v1.store(std::get<0>(version_and_timestamp),std::memory_order_relaxed);
    // compiler reordering barrier
#ifdef __GNUC__
    asm volatile("" ::: "memory");
#else
#error Lockless support is currently for GCC only
#endif

    this->kv_map.erase(value.get_key_ref()); // remove
    this->kv_map.emplace(value.get_key_ref(), value); // copy constructor
    this->update_version = std::get<0>(version_and_timestamp);

    // for lockless check
    // compiler reordering barrier
#ifdef __GNUC__
    asm volatile("" ::: "memory");
#else
#error Lockless support is currently for GCC only
#endif
    this->lockless_v2.store(std::get<0>(version_and_timestamp),std::memory_order_relaxed);

    if (cascade_watcher_ptr) {
        (*cascade_watcher_ptr)(
            // group->template get_subgroup<VolatileCascadeStore>(this->subgroup_index).get_subgroup_id(), // this is subgroup id
            this->subgroup_index, // this is subgroup index
            group->template get_subgroup<VolatileCascadeStore>(this->subgroup_index).get_shard_num(),
            group->get_rpc_caller_id(),
            value.get_key_ref(), value, cascade_context_ptr);
    }

    return true;
}


template<typename KT, typename VT, KT* IK, VT* IV>
std::tuple<persistent::version_t,uint64_t> VolatileCascadeStore<KT,VT,IK,IV>::ordered_remove(const KT& key) {
    debug_enter_func_with_args("key={}",key);

    std::tuple<persistent::version_t,uint64_t> version_and_timestamp = group->template get_subgroup<VolatileCascadeStore>(this->subgroup_index).get_current_version();

    if (this->kv_map.find(key)==this->kv_map.end()) {
        debug_leave_func_with_value("version=0x{:x},timestamp={}",std::get<0>(version_and_timestamp), std::get<1>(version_and_timestamp));
        return version_and_timestamp;
    }

    auto value = create_null_object_cb<KT,VT,IK,IV>(key);

    if constexpr (std::is_base_of<IKeepVersion,VT>::value) {
        value.set_version(std::get<0>(version_and_timestamp));
    }
    if constexpr (std::is_base_of<IKeepTimestamp,VT>::value) {
        value.set_timestamp(std::get<1>(version_and_timestamp));
    }
    if constexpr (std::is_base_of<IKeepPreviousVersion,VT>::value) {
        if (this->kv_map.find(key)!=this->kv_map.end()) {
            value.set_previous_version(this->update_version,this->kv_map.at(key).get_version());
        } else {
            value.set_previous_version(this->update_version,persistent::INVALID_VERSION);
        }
    }

    // for lockless check
    this->lockless_v1.store(std::get<0>(version_and_timestamp),std::memory_order_relaxed);
    // compiler reordering barrier
#ifdef __GNUC__
    asm volatile("" ::: "memory");
#else
#error Lockless support is currently for GCC only
#endif

    this->kv_map.erase(key); // remove
    this->kv_map.emplace(key, value);
    this->update_version = std::get<0>(version_and_timestamp);

    // for lockless check
    // compiler reordering barrier
#ifdef __GNUC__
    asm volatile("" ::: "memory");
#else
#error Lockless support is currently for GCC only
#endif
    this->lockless_v2.store(std::get<0>(version_and_timestamp),std::memory_order_relaxed);
    
    if (cascade_watcher_ptr) {
        (*cascade_watcher_ptr)(
            // group->template get_subgroup<VolatileCascadeStore>(this->subgroup_index).get_subgroup_id(), // this is subgroup id
            this->subgroup_index,
            group->template get_subgroup<VolatileCascadeStore>(this->subgroup_index).get_shard_num(),
            group->get_rpc_caller_id(),
            key, value,cascade_context_ptr);
    }

    debug_leave_func_with_value("version=0x{:x},timestamp={}",std::get<0>(version_and_timestamp), std::get<1>(version_and_timestamp));
    
    return version_and_timestamp;
}

template<typename KT, typename VT, KT* IK, VT* IV>
const VT VolatileCascadeStore<KT,VT,IK,IV>::ordered_get(const KT& key) {
    debug_enter_func_with_args("key={}",key);

    if (this->kv_map.find(key) != this->kv_map.end()) {
        debug_leave_func_with_value("key={}",key);
        return this->kv_map.at(key);
    } else {
        debug_leave_func();
        return *IV;
    }
}

template<typename KT, typename VT, KT* IK, VT* IV>
uint64_t VolatileCascadeStore<KT,VT,IK,IV>::ordered_get_size(const KT& key) {
    debug_enter_func_with_args("key={}",key);

    if (this->kv_map.find(key) != this->kv_map.end()) {
        return mutils::bytes_size(this->kv_map.at(key));
    } else {
        debug_leave_func();
        return 0;
    }
}

template<typename KT, typename VT, KT* IK, VT* IV>
void VolatileCascadeStore<KT,VT,IK,IV>::trigger_put(const VT& value) const {
    debug_enter_func_with_args("key={}",value.get_key_ref());

    if (cascade_watcher_ptr) {
        (*cascade_watcher_ptr)(
            this->subgroup_index, 
            group->template get_subgroup<VolatileCascadeStore<KT,VT,IK,IV>>(this->subgroup_index).get_shard_num(),
            group->get_rpc_caller_id(),
            value.get_key_ref(), value, cascade_context_ptr, true);
    }

    debug_leave_func();
}

#ifdef ENABLE_EVALUATION
template<typename KT, typename VT, KT* IK, VT* IV>
void VolatileCascadeStore<KT,VT,IK,IV>::dump_timestamp_log(const std::string& filename) const {
    debug_enter_func_with_args("filename={}",filename);
    derecho::Replicated<VolatileCascadeStore>& subgroup_handle = group->template get_subgroup<VolatileCascadeStore>(this->subgroup_index);
    auto result = subgroup_handle.template ordered_send<RPC_NAME(ordered_dump_timestamp_log)>(filename);
    auto& replies = result.get();
    for (auto r:replies) {
        volatile uint32_t _ = r;
        _ = _;
    }
    debug_leave_func();
    return;
}

template<typename KT, typename VT, KT* IK, VT* IV>
void VolatileCascadeStore<KT,VT,IK,IV>::ordered_dump_timestamp_log(const std::string& filename) {
    debug_enter_func_with_args("filename={}",filename);
    global_timestamp_logger.flush(filename);
    debug_leave_func();
}
#ifdef DUMP_TIMESTAMP_WORKAROUND
template<typename KT, typename VT, KT* IK, VT* IV>
void VolatileCascadeStore<KT,VT,IK,IV>::dump_timestamp_log_workaround(const std::string& filename) const {
    debug_enter_func_with_args("filename={}",filename);
    global_timestamp_logger.flush(filename);
    debug_leave_func();
}
#endif
#endif//ENABLE_EVALUATION

template<typename KT, typename VT, KT* IK, VT* IV>
std::unique_ptr<VolatileCascadeStore<KT,VT,IK,IV>> VolatileCascadeStore<KT,VT,IK,IV>::from_bytes(
    mutils::DeserializationManager* dsm, 
    uint8_t const* buf) {
    auto kv_map_ptr = mutils::from_bytes<std::map<KT,VT>>(dsm,buf);
    auto update_version_ptr = mutils::from_bytes<persistent::version_t>(dsm,buf+mutils::bytes_size(*kv_map_ptr));
    auto volatile_cascade_store_ptr =
        std::make_unique<VolatileCascadeStore>(std::move(*kv_map_ptr),
                                               *update_version_ptr,
                                               dsm->registered<CriticalDataPathObserver<VolatileCascadeStore<KT,VT,IK,IV>>>()?&(dsm->mgr<CriticalDataPathObserver<VolatileCascadeStore<KT,VT,IK,IV>>>()):nullptr,
                                               dsm->registered<ICascadeContext>()?&(dsm->mgr<ICascadeContext>()):nullptr);
    return volatile_cascade_store_ptr;
}

template<typename KT, typename VT, KT* IK, VT* IV>
VolatileCascadeStore<KT,VT,IK,IV>::VolatileCascadeStore(
    CriticalDataPathObserver<VolatileCascadeStore<KT,VT,IK,IV>>* cw,
    ICascadeContext* cc):
    lockless_v1(persistent::INVALID_VERSION),
    lockless_v2(persistent::INVALID_VERSION),
    update_version(persistent::INVALID_VERSION),
    cascade_watcher_ptr(cw),
    cascade_context_ptr(cc) {
    debug_enter_func();
    debug_leave_func();
}

template<typename KT, typename VT, KT* IK, VT* IV>
VolatileCascadeStore<KT,VT,IK,IV>::VolatileCascadeStore(
    const std::map<KT,VT>& _kvm,
    persistent::version_t _uv,
    CriticalDataPathObserver<VolatileCascadeStore<KT,VT,IK,IV>>* cw,
    ICascadeContext* cc):
    lockless_v1(_uv),
    lockless_v2(_uv),
    kv_map(_kvm),
    update_version(_uv),
    cascade_watcher_ptr(cw),
    cascade_context_ptr(cc) {
    debug_enter_func_with_args("copy to kv_map, size={}",kv_map.size());
    debug_leave_func();
}

template<typename KT, typename VT, KT* IK, VT* IV>
VolatileCascadeStore<KT,VT,IK,IV>::VolatileCascadeStore(
    std::map<KT,VT>&& _kvm,
    persistent::version_t _uv,
    CriticalDataPathObserver<VolatileCascadeStore<KT,VT,IK,IV>>* cw,
    ICascadeContext* cc):
    lockless_v1(_uv),
    lockless_v2(_uv),
    kv_map(std::move(_kvm)),
    update_version(_uv),
    cascade_watcher_ptr(cw),
    cascade_context_ptr(cc) {
    debug_enter_func_with_args("move to kv_map, size={}",kv_map.size());
    debug_leave_func();
}

///////////////////////////////////////////////////////////////////////////////
// 2 - Persistent Cascade Store Implementation
///////////////////////////////////////////////////////////////////////////////
template <typename KT, typename VT, KT* IK, VT* IV>
void DeltaCascadeStoreCore<KT,VT,IK,IV>::_Delta::set_data_len(const size_t& dlen) {
    assert(capacity >= dlen);
    this->len = dlen;
}

template <typename KT, typename VT, KT* IK, VT* IV>
uint8_t* DeltaCascadeStoreCore<KT,VT,IK,IV>::_Delta::data_ptr() {
    assert(buffer != nullptr);
    return buffer;
}

template <typename KT, typename VT, KT* IK, VT *IV>
void DeltaCascadeStoreCore<KT,VT,IK,IV>::_Delta::calibrate(const size_t& dlen) {
    size_t new_cap = dlen;
    if(this->capacity >= new_cap) {
        return;
    }
    // calculate new capacity
    int width = sizeof(size_t) << 3;
    int right_shift_bits = 1;
    new_cap--;
    while(right_shift_bits < width) {
        new_cap |= new_cap >> right_shift_bits;
        right_shift_bits = right_shift_bits << 1;
    }
    new_cap++;
    // resize
    this->buffer = (uint8_t*)realloc(buffer, new_cap);
    if(this->buffer == nullptr) {
        dbg_default_crit("{}:{} Failed to allocate delta buffer. errno={}", __FILE__, __LINE__, errno);
        throw derecho::derecho_exception("Failed to allocate delta buffer.");
    } else {
        this->capacity = new_cap;
    }
}

template <typename KT, typename VT, KT* IK, VT *IV>
bool DeltaCascadeStoreCore<KT,VT,IK,IV>::_Delta::is_empty() {
    return (this->len == 0);
}

template <typename KT, typename VT, KT* IK, VT *IV>
void DeltaCascadeStoreCore<KT,VT,IK,IV>::_Delta::clean() {
    this->len = 0;
}

template <typename KT, typename VT, KT* IK, VT *IV>
void DeltaCascadeStoreCore<KT,VT,IK,IV>::_Delta::destroy() {
    if(this->capacity > 0) {
        free(this->buffer);
    }
}

template <typename KT, typename VT, KT* IK, VT *IV>
void DeltaCascadeStoreCore<KT,VT,IK,IV>::initialize_delta() {
    delta.buffer = (uint8_t*)malloc(DEFAULT_DELTA_BUFFER_CAPACITY);
    if (delta.buffer == nullptr) {
        dbg_default_crit("{}:{} Failed to allocate delta buffer. errno={}", __FILE__, __LINE__, errno);
        throw derecho::derecho_exception("Failed to allocate delta buffer.");
    }
    delta.capacity = DEFAULT_DELTA_BUFFER_CAPACITY;
    delta.len = 0;
}

template <typename KT, typename VT, KT* IK, VT *IV>
void DeltaCascadeStoreCore<KT,VT,IK,IV>::finalizeCurrentDelta(const persistent::DeltaFinalizer& df) {
    df(this->delta.buffer, this->delta.len);
    this->delta.clean();
}

template <typename KT, typename VT, KT* IK, VT *IV>
void DeltaCascadeStoreCore<KT,VT,IK,IV>::applyDelta(uint8_t const* const delta) {
    apply_ordered_put(*mutils::from_bytes<VT>(nullptr,delta));
    mutils::deserialize_and_run(nullptr,delta,[this](const VT& value){
        this->apply_ordered_put(value);
    });
}

template <typename KT, typename VT, KT* IK, VT *IV>
void DeltaCascadeStoreCore<KT,VT,IK,IV>::apply_ordered_put(const VT& value) {
    // for lockless check
    this->lockless_v1.store(value.get_version(),std::memory_order_relaxed);
    // compiler reordering barrier
#ifdef __GNUC__
    asm volatile("" ::: "memory");
#else
#error Lockless support is currently for GCC only
#endif

    this->kv_map.erase(value.get_key_ref());
    this->kv_map.emplace(value.get_key_ref(),value);

    // compiler reordering barrier
#ifdef __GNUC__
    asm volatile("" ::: "memory");
#else
#error Lockless support is currently for GCC only
#endif

    // for lockless check
    this->lockless_v2.store(value.get_version(),std::memory_order_relaxed);
}

template <typename KT, typename VT, KT* IK, VT *IV>
std::unique_ptr<DeltaCascadeStoreCore<KT,VT,IK,IV>> DeltaCascadeStoreCore<KT,VT,IK,IV>::create(mutils::DeserializationManager* dm) {
    if (dm != nullptr) {
        try {
            return std::make_unique<DeltaCascadeStoreCore<KT,VT,IK,IV>>();
        } catch (...) {
        }
    }
    return std::make_unique<DeltaCascadeStoreCore<KT,VT,IK,IV>>();
}

template <typename KT, typename VT, KT* IK, VT *IV>
bool DeltaCascadeStoreCore<KT,VT,IK,IV>::ordered_put(const VT& value, persistent::version_t prev_ver) {
    // call validator
    if constexpr (std::is_base_of<IValidator<KT,VT>,VT>::value) {
        if(!value.validate(this->kv_map)) {
            return false;
        }
    }

    // verify version MUST happen before updating it's previous versions (prev_ver,prev_ver_by_key).
    if constexpr (std::is_base_of<IVerifyPreviousVersion,VT>::value) {
        bool verify_result;
        if (kv_map.find(value.get_key_ref())!=this->kv_map.end()) {
            verify_result = value.verify_previous_version(prev_ver,this->kv_map.at(value.get_key_ref()).get_version());
        } else {
            verify_result = value.verify_previous_version(prev_ver,persistent::INVALID_VERSION);
        }
        if (!verify_result) {
            // reject the package if verify failed.
            return false;
        }
    }
    if constexpr (std::is_base_of<IKeepPreviousVersion,VT>::value) {
        persistent::version_t prev_ver_by_key = persistent::INVALID_VERSION;
        if (kv_map.find(value.get_key_ref()) != kv_map.end()) {
            prev_ver_by_key = kv_map.at(value.get_key_ref()).get_version();
        }
        value.set_previous_version(prev_ver,prev_ver_by_key);
    }
    // create delta.
    assert(this->delta.is_empty());
    this->delta.calibrate(mutils::bytes_size(value));
    mutils::to_bytes(value,this->delta.data_ptr());
    this->delta.set_data_len(mutils::bytes_size(value));
    // apply_ordered_put
    apply_ordered_put(value);
    return true;
}

template <typename KT, typename VT, KT* IK, VT *IV>
bool DeltaCascadeStoreCore<KT,VT,IK,IV>::ordered_remove(const VT& value, persistent::version_t prev_ver) {
    auto& key = value.get_key_ref();
    // test if key exists
    if  (kv_map.find(key) == kv_map.end()) {
        // skip it when no such key.
        return false;
    } else if (kv_map.at(key).is_null()) {
        // and skip the keys has been deleted already.
        return false;
    }

    if constexpr (std::is_base_of<IKeepPreviousVersion,VT>::value) {
        value.set_previous_version(prev_ver,kv_map.at(key).get_version());
    }
    // create delta.
    assert(this->delta.is_empty());
    this->delta.calibrate(mutils::bytes_size(value));
    mutils::to_bytes(value,this->delta.data_ptr());
    this->delta.set_data_len(mutils::bytes_size(value));
    // apply_ordered_put
    apply_ordered_put(value);
    return true;
}

template <typename KT, typename VT, KT* IK, VT* IV>
const VT DeltaCascadeStoreCore<KT,VT,IK,IV>::ordered_get(const KT& key) const {
    if (kv_map.find(key) != kv_map.end()) {
        return kv_map.at(key);
    } else {
        return *IV;
    }
}

template <typename KT, typename VT, KT* IK, VT* IV>
const VT DeltaCascadeStoreCore<KT,VT,IK,IV>::lockless_get(const KT& key) const {
    persistent::version_t v1,v2;
    static thread_local VT copied_out;
    do {
        // This only for TSO memory reordering.
        v2 = this->lockless_v2.load(std::memory_order_relaxed);
        // compiler reordering barrier
#ifdef __GNUC__
        asm volatile("" ::: "memory");
#else
#error Lockless support is currently for GCC only
#endif
        if (this->kv_map.find(key) != this->kv_map.end()) {
            copied_out.copy_from(this->kv_map.at(key));
        } else {
            copied_out.copy_from(*IV);
        }
        // compiler reordering barrier
#ifdef __GNUC__
        asm volatile("" ::: "memory");
#else
#error Lockless support is currently for GCC only
#endif
        v1 = this->lockless_v1.load(std::memory_order_relaxed);
        // busy sleep
        std::this_thread::yield();
    } while(v1!=v2);
    return copied_out;
}

template <typename KT, typename VT, KT* IK, VT* IV>
std::vector<KT> DeltaCascadeStoreCore<KT,VT,IK,IV>::lockless_list_keys(const std::string& prefix) const {
    persistent::version_t v1,v2;
    std::vector<KT> key_list;
    do {
        // This only for TSO memory reordering.
        v2 = this->lockless_v2.load(std::memory_order_relaxed);
        // compiler reordering barrier
#ifdef __GNUC__
        asm volatile("" ::: "memory");
#else
#error Lockless support is currently for GCC only
#endif
        for (const auto& kv:kv_map) {
            if (get_pathname<KT>(kv.first).find(prefix)==0) {
                key_list.push_back(kv.first);
            }
        }
        // compiler reordering barrier
#ifdef __GNUC__
        asm volatile("" ::: "memory");
#else
#error Lockless support is currently for GCC only
#endif
        v1 = this->lockless_v1.load(std::memory_order_relaxed);
        // busy sleep
        std::this_thread::yield();
    } while(v1!=v2);
    return key_list;
}

template <typename KT, typename VT, KT* IK, VT* IV>
std::vector<KT> DeltaCascadeStoreCore<KT,VT,IK,IV>::ordered_list_keys(const std::string& prefix) {
    std::vector<KT> key_list;
    for (const auto& kv: kv_map) {
        if (get_pathname<KT>(kv.first).find(prefix)==0) {
            key_list.push_back(kv.first);
        }
    }
    return key_list;
}

template <typename KT, typename VT, KT* IK, VT* IV>
uint64_t DeltaCascadeStoreCore<KT,VT,IK,IV>::ordered_get_size(const KT& key) {
    if (kv_map.find(key) != kv_map.end()) {
        return mutils::bytes_size(kv_map.at(key));
    } else {
        return 0;
    }
}

template <typename KT, typename VT, KT* IK, VT* IV>
uint64_t DeltaCascadeStoreCore<KT,VT,IK,IV>::lockless_get_size(const KT& key) const {
    persistent::version_t v1,v2;
    uint64_t size = 0ull;

    do {
        // This only for TSO memory reordering.
        v2 = this->lockless_v2.load(std::memory_order_relaxed);
        // compiler reordering barrier
#ifdef __GNUC__
        asm volatile("" ::: "memory");
#else
#error Lockless support is currently for GCC only
#endif
        if (this->kv_map.find(key) != this->kv_map.end()) {
            size = mutils::bytes_size(this->kv_map.at(key));
        }
        // compiler reordering barrier
#ifdef __GNUC__
        asm volatile("" ::: "memory");
#else
#error Lockless support is currently for GCC only
#endif
        v1 = this->lockless_v1.load(std::memory_order_relaxed);
        // busy sleep
        std::this_thread::yield();
    } while(v1!=v2);
    return size;
}

template <typename KT, typename VT, KT* IK, VT* IV>
DeltaCascadeStoreCore<KT,VT,IK,IV>::DeltaCascadeStoreCore():
    lockless_v1(persistent::INVALID_VERSION),
    lockless_v2(persistent::INVALID_VERSION) {
    initialize_delta();
}

template <typename KT, typename VT, KT* IK, VT* IV>
DeltaCascadeStoreCore<KT,VT,IK,IV>::DeltaCascadeStoreCore(const std::map<KT,VT>& _kv_map):
    lockless_v1(persistent::INVALID_VERSION),
    lockless_v2(persistent::INVALID_VERSION),
    kv_map(_kv_map) {
    initialize_delta();
}

template <typename KT, typename VT, KT* IK, VT* IV>
DeltaCascadeStoreCore<KT,VT,IK,IV>::DeltaCascadeStoreCore(std::map<KT,VT>&& _kv_map):
    lockless_v1(persistent::INVALID_VERSION),
    lockless_v2(persistent::INVALID_VERSION),
    kv_map(std::move(_kv_map)) {
    initialize_delta();
}

template<typename KT, typename VT, KT* IK, VT* IV>
DeltaCascadeStoreCore<KT,VT,IK,IV>::~DeltaCascadeStoreCore() {
    if (this->delta.buffer != nullptr) {
        free(this->delta.buffer);
    }
}

template<typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
std::tuple<persistent::version_t,uint64_t> PersistentCascadeStore<KT,VT,IK,IV,ST>::put(const VT& value) const {
    debug_enter_func_with_args("value.get_key_ref()={}",value.get_key_ref());
    LOG_TIMESTAMP_BY_TAG(TLT_PERSISTENT_PUT_START,group,value);

    derecho::Replicated<PersistentCascadeStore>& subgroup_handle = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index);
    auto results = subgroup_handle.template ordered_send<RPC_NAME(ordered_put)>(value);
    auto& replies = results.get();
    std::tuple<persistent::version_t,uint64_t> ret(CURRENT_VERSION,0);
    // TODO: verfiy consistency ?
    for (auto& reply_pair : replies) {
        ret = reply_pair.second.get();
    }

    LOG_TIMESTAMP_BY_TAG(TLT_PERSISTENT_PUT_END,group,value);
    debug_leave_func_with_value("version=0x{:x},timestamp={}",std::get<0>(ret),std::get<1>(ret));
    return ret;
}

template<typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
void PersistentCascadeStore<KT,VT,IK,IV,ST>::put_and_forget(const VT& value) const {
    debug_enter_func_with_args("value.get_key_ref()={}",value.get_key_ref());
    LOG_TIMESTAMP_BY_TAG(TLT_PERSISTENT_PUT_AND_FORGET_START,group,value);
    derecho::Replicated<PersistentCascadeStore>& subgroup_handle = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index);
    subgroup_handle.template ordered_send<RPC_NAME(ordered_put_and_forget)>(value);
    LOG_TIMESTAMP_BY_TAG(TLT_PERSISTENT_PUT_AND_FORGET_END,group,value);
    debug_leave_func();
}

#ifdef ENABLE_EVALUATION
template<typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
double PersistentCascadeStore<KT,VT,IK,IV,ST>::perf_put(const uint32_t max_payload_size, const uint64_t duration_sec) const {
    debug_enter_func_with_args("max_payload_size={},duration_sec={}",max_payload_size,duration_sec);
    derecho::Replicated<PersistentCascadeStore>& subgroup_handle = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index);
    double ops = internal_perf_put(subgroup_handle,max_payload_size,duration_sec);
    debug_leave_func_with_value("{} ops.",ops);
    return ops;
}
#endif//ENABLE_EVALUATION

template<typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
std::tuple<persistent::version_t,uint64_t> PersistentCascadeStore<KT,VT,IK,IV,ST>::remove(const KT& key) const {
    debug_enter_func_with_args("key={}",key);
    derecho::Replicated<PersistentCascadeStore>& subgroup_handle = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index);
    auto results = subgroup_handle.template ordered_send<RPC_NAME(ordered_remove)>(key);
    auto& replies = results.get();
    std::tuple<persistent::version_t,uint64_t> ret(CURRENT_VERSION,0);
    // TODO: verify consistency ?
    for (auto& reply_pair : replies) {
        ret = reply_pair.second.get();
    }
    debug_leave_func_with_value("version=0x{:x},timestamp={}",std::get<0>(ret),std::get<1>(ret));
    return ret;
}

template<typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
const VT PersistentCascadeStore<KT,VT,IK,IV,ST>::get(const KT& key, const persistent::version_t& ver, bool stable, bool exact) const {
    debug_enter_func_with_args("key={},ver=0x{:x},stable={},exact={}",key,ver,stable,exact);
    persistent::version_t requested_version = ver;

    // adjust version if stable is requested.
    if (stable) {
        derecho::Replicated<PersistentCascadeStore>& subgroup_handle = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index);
        requested_version = ver;
        if (requested_version == CURRENT_VERSION) {
            requested_version = subgroup_handle.get_global_persistence_frontier();
        } else {
            // The first condition test if requested_version is beyond the active latest atomic broadcast version.
            // However, that could be true for a valid requested version for a new started setup, where the active
            // latest atomic broadcast version is INVALID_VERSION(-1) since there is no atomic broadcast yet. In such a
            // case, we need also check if requested_version is beyond the local latest version. If both are true, we
            // determine the requested_version is invalid: it asks a version in the future.
            if(!subgroup_handle.wait_for_global_persistence_frontier(requested_version) && 
               requested_version > persistent_core.getLatestVersion()) {
                // INVALID version
                dbg_default_debug("{}: requested version:{:x} is beyond the latest atomic broadcast version.",__PRETTY_FUNCTION__,requested_version);
                return *IV;
            }
        }

    }

    if (requested_version == CURRENT_VERSION) {
        // return the unstable question
        debug_leave_func_with_value("lockless_get({})",key);
        return persistent_core->lockless_get(key);
    } else {
        return persistent_core.template getDelta<VT>(requested_version, exact, [this,key,requested_version,exact](const VT& v){
                if (key == v.get_key_ref()) {
                    debug_leave_func_with_value("key:{} is found at version:0x{:x}", key, requested_version);
                    return v;
                } else {
                    if (exact) {
                        // return invalid object for EXACT search.
                        debug_leave_func_with_value("No data found for key:{} at version:0x{:x}", key, requested_version);
                        return *IV;
                    } else {
                        // fall back to the slow path.
                        auto versioned_state_ptr = persistent_core.get(requested_version);
                        if (versioned_state_ptr->kv_map.find(key) != versioned_state_ptr->kv_map.end()) {
                            debug_leave_func_with_value("Reconstructed version:0x{:x} for key:{}",requested_version,key);
                            return versioned_state_ptr->kv_map.at(key);
                        }
                        debug_leave_func_with_value("No data found for key:{} before version:0x{:x}", key, requested_version);
                        return *IV;
                    }
                }
            });
    }
}

template<typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
const VT PersistentCascadeStore<KT,VT,IK,IV,ST>::multi_get(const KT& key) const {
    debug_enter_func_with_args("key={}",key);
    derecho::Replicated<PersistentCascadeStore>& subgroup_handle = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index);
    auto results = subgroup_handle.template ordered_send<RPC_NAME(ordered_get)>(key);
    auto& replies = results.get();
    //  TODO: verify consistency ?
    for (auto& reply_pair : replies) {
        reply_pair.second.wait();
    }
    debug_leave_func();
    return replies.begin()->second.get();
}


template<typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
const VT PersistentCascadeStore<KT,VT,IK,IV,ST>::get_by_time(const KT& key, const uint64_t& ts_us, const bool stable) const {
    debug_enter_func_with_args("key={},ts_us={},stable={}",key,ts_us,stable);
    const HLC hlc(ts_us,0ull);

    derecho::Replicated<PersistentCascadeStore>& subgroup_handle = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index);

    // get_global_stability_frontier return nano seconds.
    if ( ts_us > subgroup_handle.compute_global_stability_frontier()/1000 ) {
        dbg_default_warn("Cannot get data at a time in the future.");
        return *IV;
    }

    persistent::version_t ver = persistent_core.getVersionAtTime({ts_us,0});
    if (ver == persistent::INVALID_VERSION) {
        return *IV;
    }

    debug_leave_func();
    return get(key,ver,stable,false);
}

template<typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
uint64_t PersistentCascadeStore<KT,VT,IK,IV,ST>::multi_get_size(const KT& key) const {
    debug_enter_func_with_args("key={}",key);
    derecho::Replicated<PersistentCascadeStore>& subgroup_handle = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index);
    auto results = subgroup_handle.template ordered_send<RPC_NAME(ordered_get_size)>(key);
    auto& replies = results.get();
    // TODO: verify consistency ?
    // for (auto& reply_pair : replies) {
    //     ret = reply_pair.second.get();
    // }
    debug_leave_func();
    return replies.begin()->second.get();
}

template<typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
uint64_t PersistentCascadeStore<KT,VT,IK,IV,ST>::get_size(const KT& key, const persistent::version_t& ver, const bool stable, const bool exact) const {
    debug_enter_func_with_args("key={},ver=0x{:x},stable={},exact={}",key,ver,stable,exact);
    persistent::version_t requested_version = ver;

    // adjust version if stable is requested.
    if (stable) {
        derecho::Replicated<PersistentCascadeStore>& subgroup_handle = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index);
        requested_version = ver;
        if (requested_version == CURRENT_VERSION) {
            requested_version = subgroup_handle.get_global_persistence_frontier();
        } else {
            // The first condition test if requested_version is beyond the active latest atomic broadcast version.
            // However, that could be true for a valid requested version for a new started setup, where the active
            // latest atomic broadcast version is INVALID_VERSION(-1) since there is no atomic broadcast yet. In such a
            // case, we need also check if requested_version is beyond the local latest version. If both are true, we
            // determine the requested_version is invalid: it asks a version in the future.
            if(!subgroup_handle.wait_for_global_persistence_frontier(requested_version) && 
               requested_version > persistent_core.getLatestVersion()) {
                // INVALID version
                dbg_default_debug("{}: requested version:{:x} is beyond the latest atomic broadcast version.",__PRETTY_FUNCTION__,requested_version);
                return 0ull;
            }
        }

    }

    if (requested_version == CURRENT_VERSION) {
        // return the unstable question
        debug_leave_func_with_value("lockless_get_size({})",key);
        return persistent_core->lockless_get_size(key);
    } else {
        return persistent_core.template getDelta<VT>(requested_version, exact, [this,key,requested_version,exact](const VT& v)->uint64_t{
                if (key == v.get_key_ref()) {
                    debug_leave_func_with_value("key:{} is found at version:0x{:x}", key, requested_version);
                    return mutils::bytes_size(v);
                } else {
                    if (exact) {
                        // return invalid object for EXACT search.
                        debug_leave_func_with_value("No data found for key:{} at version:0x{:x}", key, requested_version);
                        return 0ull;
                    } else {
                        // fall back to the slow path.
                        auto versioned_state_ptr = persistent_core.get(requested_version);
                        if (versioned_state_ptr->kv_map.find(key) != versioned_state_ptr->kv_map.end()) {
                            debug_leave_func_with_value("Reconstructed version:0x{:x} for key:{}",requested_version,key);
                            return mutils::bytes_size(versioned_state_ptr->kv_map.at(key));
                        }
                        debug_leave_func_with_value("No data found for key:{} before version:0x{:x}", key, requested_version);
                        return 0ull;
                    }
                }
            });
    }
}

template<typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
uint64_t PersistentCascadeStore<KT,VT,IK,IV,ST>::get_size_by_time(const KT& key, const uint64_t& ts_us, const bool stable) const {
    debug_enter_func_with_args("key={},ts_us={},stable={}",key,ts_us,stable);
    const HLC hlc(ts_us,0ull);

    derecho::Replicated<PersistentCascadeStore>& subgroup_handle = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index);

    // get_global_stability_frontier return nano seconds.
    if ( ts_us > subgroup_handle.compute_global_stability_frontier()/1000 ) {
        dbg_default_warn("Cannot get data at a time in the future.");
        return 0;
    }

    persistent::version_t ver = persistent_core.getVersionAtTime({ts_us,0});
    if (ver == persistent::INVALID_VERSION) {
        return 0;
    }

    debug_leave_func();

    return get_size(key,ver,stable);
}

template<typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
std::vector<KT> PersistentCascadeStore<KT,VT,IK,IV,ST>::multi_list_keys(const std::string& prefix) const {
    debug_enter_func_with_args("prefix={}.",prefix);
    derecho::Replicated<PersistentCascadeStore>& subgroup_handle = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index);
    auto results = subgroup_handle.template ordered_send<RPC_NAME(ordered_list_keys)>(prefix);
    auto& replies = results.get();
    // TODO: verify consistency ?
    debug_leave_func();
    return replies.begin()->second.get();
}

template<typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
std::vector<KT> PersistentCascadeStore<KT,VT,IK,IV,ST>::list_keys(const std::string& prefix, const persistent::version_t& ver, const bool stable) const {
    debug_enter_func_with_args("prefix={}, ver=0x{:x}, stable={}",prefix,ver,stable);

    persistent::version_t requested_version = ver;

    if (stable) {
        derecho::Replicated<PersistentCascadeStore>& subgroup_handle = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index);
        requested_version = ver;
        if (requested_version == CURRENT_VERSION) {
            requested_version = subgroup_handle.get_global_persistence_frontier();
        } else {
            // The first condition test if requested_version is beyond the active latest atomic broadcast version.
            // However, that could be true for a valid requested version for a new started setup, where the active
            // latest atomic broadcast version is INVALID_VERSION(-1) since there is no atomic broadcast yet. In such a
            // case, we need also check if requested_version is beyond the local latest version. If both are true, we
            // determine the requested_version is invalid: it asks a version in the future.
            if(!subgroup_handle.wait_for_global_persistence_frontier(requested_version) && 
               requested_version > persistent_core.getLatestVersion()) {
                // INVALID version
                dbg_default_debug("{}: requested version:{:x} is beyond the latest atomic broadcast version.",__PRETTY_FUNCTION__,requested_version);
                return {};
            }
        }

    }

    if (requested_version == CURRENT_VERSION) {
        // return the unstable question
        debug_leave_func_with_value("lockless_list_prefix({})",prefix);
        return persistent_core->lockless_list_keys(prefix);
    } else {
        std::vector<KT> keys;
        persistent_core.get(requested_version, [&keys,&prefix](const DeltaCascadeStoreCore<KT,VT,IK,IV>& pers_core){
                for (const auto& kv:pers_core.kv_map) {
                    if (get_pathname<KT>(kv.first).find(prefix) == 0) {
                        keys.push_back(kv.first);
                    }
                }
            });
        return keys;
    }
}

template<typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
std::vector<KT> PersistentCascadeStore<KT,VT,IK,IV,ST>::list_keys_by_time(const std::string& prefix, const uint64_t& ts_us, const bool stable) const {
    debug_enter_func_with_args("ts_us={}",ts_us);
    const HLC hlc(ts_us,0ull);

    derecho::Replicated<PersistentCascadeStore>& subgroup_handle = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index);

    // get_global_stability_frontier return nano seconds.
    if ( ts_us > subgroup_handle.compute_global_stability_frontier()/1000 ) {
        dbg_default_warn("Cannot get data at a time in the future.");
        return {};
    }

    persistent::version_t ver = persistent_core.getVersionAtTime({ts_us,0});
    if (ver == persistent::INVALID_VERSION) {
        return {};
    }

    return list_keys(prefix,ver,stable);
}

template<typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
std::tuple<persistent::version_t,uint64_t> PersistentCascadeStore<KT,VT,IK,IV,ST>::ordered_put(const VT& value) {
    debug_enter_func_with_args("key={}",value.get_key_ref());

    std::tuple<persistent::version_t,uint64_t> version_and_timestamp = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index).get_current_version();
#if __cplusplus > 201703L
    LOG_TIMESTAMP_BY_TAG(TLT_PERSISTENT_ORDERED_PUT_START,group,value,std::get<0>(version_and_timestamp));
#else
    LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_PERSISTENT_ORDERED_PUT_START,group,value,std::get<0>(version_and_timestamp));
#endif
    if(this->internal_ordered_put(value) == false) {
        version_and_timestamp = {persistent::INVALID_VERSION,0};
    }

#if __cplusplus > 201703L
    LOG_TIMESTAMP_BY_TAG(TLT_PERSISTENT_ORDERED_PUT_END,group,value,std::get<0>(version_and_timestamp));
#else
    LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_PERSISTENT_ORDERED_PUT_END,group,value,std::get<0>(version_and_timestamp));
#endif
    debug_leave_func_with_value("version=0x{:x},timestamp={}",std::get<0>(version_and_timestamp), std::get<1>(version_and_timestamp));

    return version_and_timestamp;
}

template<typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
void PersistentCascadeStore<KT,VT,IK,IV,ST>::ordered_put_and_forget(const VT& value) {
    debug_enter_func_with_args("key={}",value.get_key_ref());
#ifdef ENABLE_EVALUATION
    std::tuple<persistent::version_t,uint64_t> version_and_timestamp = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index).get_current_version();
#endif

#if __cplusplus > 201703L
    LOG_TIMESTAMP_BY_TAG(TLT_PERSISTENT_ORDERED_PUT_AND_FORGET_START,group,value,std::get<0>(version_and_timestamp));
#else
    LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_PERSISTENT_ORDERED_PUT_AND_FORGET_START,group,value,std::get<0>(version_and_timestamp));
#endif

    this->internal_ordered_put(value);

#if __cplusplus > 201703L
    LOG_TIMESTAMP_BY_TAG(TLT_PERSISTENT_ORDERED_PUT_AND_FORGET_END,group,value,std::get<0>(version_and_timestamp));
#else
    LOG_TIMESTAMP_BY_TAG_EXTRA(TLT_PERSISTENT_ORDERED_PUT_AND_FORGET_END,group,value,std::get<0>(version_and_timestamp));
#endif

#ifdef ENABLE_EVALUATION
    // avoid unused variable warning.
    if constexpr (!std::is_base_of<IHasMessageID,VT>::value) {
        version_and_timestamp = version_and_timestamp;
    }
#endif
    debug_leave_func();
}

template<typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
bool PersistentCascadeStore<KT,VT,IK,IV,ST>::internal_ordered_put(const VT& value) {
    std::tuple<persistent::version_t,uint64_t> version_and_timestamp = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index).get_current_version();
    if constexpr (std::is_base_of<IKeepVersion,VT>::value) {
        value.set_version(std::get<0>(version_and_timestamp));
    }
    if constexpr (std::is_base_of<IKeepTimestamp,VT>::value) {
        value.set_timestamp(std::get<1>(version_and_timestamp));
    }
    if (this->persistent_core->ordered_put(value,this->persistent_core.getLatestVersion()) == false) {
        // verification failed. S we return invalid versions.
        debug_leave_func_with_value("version=0x{:x},timestamp={}",std::get<0>(version_and_timestamp), std::get<1>(version_and_timestamp));
        return false;
    }
    if (cascade_watcher_ptr) {
        (*cascade_watcher_ptr)(
            // group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index).get_subgroup_id(), // this is subgroup id
            this->subgroup_index,
            group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index).get_shard_num(),
            group->get_rpc_caller_id(),
            value.get_key_ref(), value, cascade_context_ptr);
    }
    return true;
}

template<typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
std::tuple<persistent::version_t,uint64_t> PersistentCascadeStore<KT,VT,IK,IV,ST>::ordered_remove(const KT& key) {
    debug_enter_func_with_args("key={}",key);
    std::tuple<persistent::version_t,uint64_t> version_and_timestamp = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index).get_current_version();
    auto value = create_null_object_cb<KT,VT,IK,IV>(key);
    if constexpr (std::is_base_of<IKeepVersion,VT>::value) {
        value.set_version(std::get<0>(version_and_timestamp));
    }
    if constexpr (std::is_base_of<IKeepTimestamp,VT>::value) {
        value.set_timestamp(std::get<1>(version_and_timestamp));
    }
    if(this->persistent_core->ordered_remove(value,this->persistent_core.getLatestVersion())) {
        if (cascade_watcher_ptr) {
            (*cascade_watcher_ptr)(
                // group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index).get_subgroup_id(), // this is subgroup id
                this->subgroup_index,
                group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index).get_shard_num(),
                group->get_rpc_caller_id(),
                key, value, cascade_context_ptr);
        }
    }

    debug_leave_func_with_value("version=0x{:x},timestamp={}",std::get<0>(version_and_timestamp), std::get<1>(version_and_timestamp));

    return version_and_timestamp;
}

template<typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
const VT PersistentCascadeStore<KT,VT,IK,IV,ST>::ordered_get(const KT& key) {
    debug_enter_func_with_args("key={}",key);

    debug_leave_func();

    return this->persistent_core->ordered_get(key);
}

template<typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
uint64_t PersistentCascadeStore<KT,VT,IK,IV,ST>::ordered_get_size(const KT& key) {
    debug_enter_func_with_args("key={}",key);

    debug_leave_func();

    return this->persistent_core->ordered_get_size(key);
}

template<typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
void PersistentCascadeStore<KT,VT,IK,IV,ST>::trigger_put(const VT& value) const {
    debug_enter_func_with_args("key={}",value.get_key_ref());

    if (cascade_watcher_ptr) {
        (*cascade_watcher_ptr)(
            this->subgroup_index, 
            group->template get_subgroup<PersistentCascadeStore<KT,VT,IK,IV,ST>>(this->subgroup_index).get_shard_num(),
            group->get_rpc_caller_id(),
            value.get_key_ref(), value, cascade_context_ptr, true);
    }

    debug_leave_func();
}

#ifdef ENABLE_EVALUATION
template<typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
void PersistentCascadeStore<KT,VT,IK,IV,ST>::dump_timestamp_log(const std::string& filename) const {
    debug_enter_func_with_args("filename={}",filename);
    derecho::Replicated<PersistentCascadeStore>& subgroup_handle = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index);
    auto result = subgroup_handle.template ordered_send<RPC_NAME(ordered_dump_timestamp_log)>(filename);
    auto& replies = result.get();
    for (auto r:replies) {
        volatile uint32_t _ = r;
        _ = _;
    }
    debug_leave_func();
    return;
}

template<typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
void PersistentCascadeStore<KT,VT,IK,IV,ST>::ordered_dump_timestamp_log(const std::string& filename) {
    debug_enter_func_with_args("filename={}",filename);
    global_timestamp_logger.flush(filename);
    debug_leave_func();
}

#ifdef DUMP_TIMESTAMP_WORKAROUND
template<typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
void PersistentCascadeStore<KT,VT,IK,IV,ST>::dump_timestamp_log_workaround(const std::string& filename) const {
    debug_enter_func_with_args("filename={}",filename);
    global_timestamp_logger.flush(filename);
    debug_leave_func();
}
#endif
#endif//ENABLE_EVALUATION

template<typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
std::vector<KT> PersistentCascadeStore<KT,VT,IK,IV,ST>::ordered_list_keys(const std::string& prefix) {
    debug_enter_func();

    debug_leave_func();

    return this->persistent_core->ordered_list_keys(prefix);
}


template<typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
std::unique_ptr<PersistentCascadeStore<KT,VT,IK,IV,ST>> PersistentCascadeStore<KT,VT,IK,IV,ST>::from_bytes(mutils::DeserializationManager* dsm, uint8_t const* buf) {
    auto persistent_core_ptr = mutils::from_bytes<persistent::Persistent<DeltaCascadeStoreCore<KT,VT,IK,IV>,ST>>(dsm,buf);
    auto persistent_cascade_store_ptr =
        std::make_unique<PersistentCascadeStore>(std::move(*persistent_core_ptr),
                                                 dsm->registered<CriticalDataPathObserver<PersistentCascadeStore<KT,VT,IK,IV>>>()?&(dsm->mgr<CriticalDataPathObserver<PersistentCascadeStore<KT,VT,IK,IV>>>()):nullptr,
                                                 dsm->registered<ICascadeContext>()?&(dsm->mgr<ICascadeContext>()):nullptr);
    return persistent_cascade_store_ptr;
}

template<typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
PersistentCascadeStore<KT,VT,IK,IV,ST>::PersistentCascadeStore(
                                               persistent::PersistentRegistry* pr,
                                               CriticalDataPathObserver<PersistentCascadeStore<KT,VT,IK,IV>>* cw,
                                               ICascadeContext* cc):
                                               persistent_core(
                                                   [](){
                                                       return std::make_unique<DeltaCascadeStoreCore<KT,VT,IK,IV>>();
                                                   },
                                                   nullptr,
                                                   pr),
                                               cascade_watcher_ptr(cw),
                                               cascade_context_ptr(cc) {
}

template<typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
PersistentCascadeStore<KT,VT,IK,IV,ST>::PersistentCascadeStore(
                                               persistent::Persistent<DeltaCascadeStoreCore<KT,VT,IK,IV>,ST>&&
                                               _persistent_core,
                                               CriticalDataPathObserver<PersistentCascadeStore<KT,VT,IK,IV>>* cw,
                                               ICascadeContext* cc):
                                               persistent_core(std::move(_persistent_core)),
                                               cascade_watcher_ptr(cw),
                                               cascade_context_ptr(cc) {
}

template<typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
PersistentCascadeStore<KT,VT,IK,IV,ST>::PersistentCascadeStore():
                                               persistent_core(
                                                   [](){
                                                       return std::make_unique<DeltaCascadeStoreCore<KT,VT,IK,IV>>();
                                                   },
                                                   nullptr,
                                                   nullptr),
                                               cascade_watcher_ptr(nullptr),
                                               cascade_context_ptr(nullptr) {
}

template<typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
PersistentCascadeStore<KT,VT,IK,IV,ST>::~PersistentCascadeStore() {}

template<typename KT, typename VT, KT* IK, VT* IV>
std::tuple<persistent::version_t,uint64_t> TriggerCascadeNoStore<KT,VT,IK,IV>::put(const VT& value) const {
    dbg_default_warn("Calling unsupported func:{}",__PRETTY_FUNCTION__);
    return {persistent::INVALID_VERSION,0};
}

template<typename KT, typename VT, KT* IK, VT* IV>
void TriggerCascadeNoStore<KT,VT,IK,IV>::put_and_forget(const VT& value) const {
    dbg_default_warn("Calling unsupported func:{}",__PRETTY_FUNCTION__);
}

#ifdef ENABLE_EVALUATION
template<typename KT, typename VT, KT* IK, VT* IV>
double TriggerCascadeNoStore<KT,VT,IK,IV>::perf_put(const uint32_t max_payload_size, const uint64_t duration_sec) const {
    dbg_default_warn("Calling unsupported func:{}",__PRETTY_FUNCTION__);
    return 0.0;
}
#endif//ENABLE_EVALUATION

template<typename KT, typename VT, KT* IK, VT* IV>
std::tuple<persistent::version_t,uint64_t> TriggerCascadeNoStore<KT,VT,IK,IV>::remove(const KT& key) const {
    dbg_default_warn("Calling unsupported func:{}",__PRETTY_FUNCTION__);
    return {persistent::INVALID_VERSION,0};
}

template<typename KT, typename VT, KT* IK, VT* IV>
const VT TriggerCascadeNoStore<KT,VT,IK,IV>::get(const KT& key, const persistent::version_t& ver,bool, bool) const {
    dbg_default_warn("Calling unsupported func:{}",__PRETTY_FUNCTION__);
    return *IV;
}

template<typename KT, typename VT, KT* IK, VT* IV>
const VT TriggerCascadeNoStore<KT,VT,IK,IV>::multi_get(const KT& key) const {
    dbg_default_warn("Calling unsupported func:{}",__PRETTY_FUNCTION__);
    return *IV;
}

template<typename KT, typename VT, KT* IK, VT* IV>
const VT TriggerCascadeNoStore<KT,VT,IK,IV>::get_by_time(const KT& key, const uint64_t& ts_us, const bool stable) const {
    dbg_default_warn("Calling unsupported func:{}",__PRETTY_FUNCTION__);
    return *IV;
}

template<typename KT, typename VT, KT* IK, VT* IV>
std::vector<KT> TriggerCascadeNoStore<KT,VT,IK,IV>::multi_list_keys(const std::string& prefix) const {
    dbg_default_warn("Calling unsupported func:{}",__PRETTY_FUNCTION__);
    return {};
}

template<typename KT, typename VT, KT* IK, VT* IV>
std::vector<KT> TriggerCascadeNoStore<KT,VT,IK,IV>::list_keys(const std::string& prefix, const persistent::version_t& ver, const bool stable) const {
    dbg_default_warn("Calling unsupported func:{}",__PRETTY_FUNCTION__);
    return {};
}

template<typename KT, typename VT, KT* IK, VT* IV>
std::vector<KT> TriggerCascadeNoStore<KT,VT,IK,IV>::list_keys_by_time(const std::string& prefix, const uint64_t& ts_us, const bool stable) const {
    dbg_default_warn("Calling unsupported func:{}",__PRETTY_FUNCTION__);
    return {};
}

template<typename KT, typename VT, KT* IK, VT* IV>
uint64_t TriggerCascadeNoStore<KT,VT,IK,IV>::multi_get_size(const KT& key) const {
    dbg_default_warn("Calling unsupported func:{}",__PRETTY_FUNCTION__);
    return 0; 
}

template<typename KT, typename VT, KT* IK, VT* IV>
uint64_t TriggerCascadeNoStore<KT,VT,IK,IV>::get_size(const KT& key, const persistent::version_t& ver, const bool stable, bool extract) const {
    dbg_default_warn("Calling unsupported func:{}",__PRETTY_FUNCTION__);
    return 0; 
}

template<typename KT, typename VT, KT* IK, VT* IV>
uint64_t TriggerCascadeNoStore<KT,VT,IK,IV>::get_size_by_time(const KT& key, const uint64_t& ts_us, const bool stable) const {
    dbg_default_warn("Calling unsupported func:{}",__PRETTY_FUNCTION__);
    return 0;
}

template<typename KT, typename VT, KT* IK, VT* IV>
std::vector<KT> TriggerCascadeNoStore<KT,VT,IK,IV>::ordered_list_keys(const std::string& prefix) {
    dbg_default_warn("Calling unsupported func:{}",__PRETTY_FUNCTION__);
    return {};
}

template<typename KT, typename VT, KT* IK, VT* IV>
std::tuple<persistent::version_t,uint64_t> TriggerCascadeNoStore<KT,VT,IK,IV>::ordered_put(const VT& value) {
    dbg_default_warn("Calling unsupported func:{}",__PRETTY_FUNCTION__);
    return {};
}

template<typename KT, typename VT, KT* IK, VT* IV>
void TriggerCascadeNoStore<KT,VT,IK,IV>::ordered_put_and_forget(const VT& value) {
    dbg_default_warn("Calling unsupported func:{}",__PRETTY_FUNCTION__);
}


template<typename KT, typename VT, KT* IK, VT* IV>
std::tuple<persistent::version_t,uint64_t> TriggerCascadeNoStore<KT,VT,IK,IV>::ordered_remove(const KT& key) {
    dbg_default_warn("Calling unsupported func:{}",__PRETTY_FUNCTION__);
    return {};
}

template<typename KT, typename VT, KT* IK, VT* IV>
const VT TriggerCascadeNoStore<KT,VT,IK,IV>::ordered_get(const KT& key) {
    dbg_default_warn("Calling unsupported func:{}",__PRETTY_FUNCTION__);
    return *IV;
}

template<typename KT, typename VT, KT* IK, VT* IV>
uint64_t TriggerCascadeNoStore<KT,VT,IK,IV>::ordered_get_size(const KT& key) {
    dbg_default_warn("Calling unsupported func:{}",__PRETTY_FUNCTION__);
    return 0;
}

template<typename KT, typename VT, KT* IK, VT* IV>
void TriggerCascadeNoStore<KT,VT,IK,IV>::trigger_put(const VT& value) const {
    debug_enter_func_with_args("key={}",value.get_key_ref());
    LOG_TIMESTAMP_BY_TAG(TLT_TRIGGER_PUT_START,group,value);
    

    if (cascade_watcher_ptr) {
        (*cascade_watcher_ptr)(
            this->subgroup_index, 
            group->template get_subgroup<TriggerCascadeNoStore<KT,VT,IK,IV>>(this->subgroup_index).get_shard_num(),
            group->get_rpc_caller_id(),
            value.get_key_ref(), value, cascade_context_ptr, true);
    }

    LOG_TIMESTAMP_BY_TAG(TLT_TRIGGER_PUT_END,group,value);
    debug_leave_func();
}

#ifdef ENABLE_EVALUATION

template<typename KT, typename VT, KT* IK, VT* IV>
void TriggerCascadeNoStore<KT,VT,IK,IV>::dump_timestamp_log(const std::string& filename) const {
    debug_enter_func_with_args("filename={}",filename);
    derecho::Replicated<TriggerCascadeNoStore>& subgroup_handle = group->template get_subgroup<TriggerCascadeNoStore>(this->subgroup_index);
    auto result = subgroup_handle.template ordered_send<RPC_NAME(ordered_dump_timestamp_log)>(filename);
    auto& replies = result.get();
    for (auto r:replies) {
        volatile uint32_t _ = r;
        _ = _;
    }
    debug_leave_func();
    return;
}

template<typename KT, typename VT, KT* IK, VT* IV>
void TriggerCascadeNoStore<KT,VT,IK,IV>::ordered_dump_timestamp_log(const std::string& filename) {
    debug_enter_func_with_args("filename={}",filename);
    global_timestamp_logger.flush(filename);
    debug_leave_func();
}
#ifdef DUMP_TIMESTAMP_WORKAROUND
template<typename KT, typename VT, KT* IK, VT* IV>
void TriggerCascadeNoStore<KT,VT,IK,IV>::dump_timestamp_log_workaround(const std::string& filename) const {
    debug_enter_func_with_args("filename={}",filename);
    global_timestamp_logger.flush(filename);
    debug_leave_func();
}
#endif
#endif//ENABLE_EVALUATION

template<typename KT, typename VT, KT* IK, VT* IV>
std::unique_ptr<TriggerCascadeNoStore<KT,VT,IK,IV>> TriggerCascadeNoStore<KT,VT,IK,IV>::from_bytes(mutils::DeserializationManager* dsm, uint8_t const* buf) {
    return std::make_unique<TriggerCascadeNoStore<KT,VT,IK,IV>>(
                                                 dsm->registered<CriticalDataPathObserver<TriggerCascadeNoStore<KT,VT,IK,IV>>>()?&(dsm->mgr<CriticalDataPathObserver<TriggerCascadeNoStore<KT,VT,IK,IV>>>()):nullptr,
                                                 dsm->registered<ICascadeContext>()?&(dsm->mgr<ICascadeContext>()):nullptr);
}

template<typename KT, typename VT, KT* IK, VT* IV>
mutils::context_ptr<TriggerCascadeNoStore<KT,VT,IK,IV>> TriggerCascadeNoStore<KT,VT,IK,IV>::from_bytes_noalloc(mutils::DeserializationManager* dsm, uint8_t const* buf) {
    return mutils::context_ptr<TriggerCascadeNoStore>(from_bytes(dsm,buf));
}

template<typename KT, typename VT, KT* IK, VT* IV>
TriggerCascadeNoStore<KT,VT,IK,IV>::TriggerCascadeNoStore(CriticalDataPathObserver<TriggerCascadeNoStore<KT,VT,IK,IV>>* cw,
                                            ICascadeContext* cc):
                                            cascade_watcher_ptr(cw),
                                            cascade_context_ptr(cc) {}

}//namespace cascade
}//namespace derecho
