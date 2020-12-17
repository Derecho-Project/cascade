#pragma once
#include <memory>
#include <map>

namespace derecho {
namespace cascade {

#define debug_enter_func_with_args(format,...) \
    dbg_default_debug("Entering {} with parameter:" #format ".", __func__, __VA_ARGS__)
#define debug_leave_func_with_value(format,...) \
    dbg_default_debug("Leaving {} with " #format "." , __func__, __VA_ARGS__)
#define debug_enter_func() dbg_default_debug("Entering {}.")
#define debug_leave_func() dbg_default_debug("Leaving {}.")

///////////////////////////////////////////////////////////////////////////////
// 1 - Volatile Cascade Store Implementation
///////////////////////////////////////////////////////////////////////////////

template<typename KT, typename VT, KT* IK, VT* IV>
std::tuple<persistent::version_t,uint64_t> VolatileCascadeStore<KT,VT,IK,IV>::put(const VT& value) const {
    debug_enter_func_with_args("value.get_key_ref={}",value.get_key_ref());
    derecho::Replicated<VolatileCascadeStore>& subgroup_handle = group->template get_subgroup<VolatileCascadeStore>(this->subgroup_index);
    auto results = subgroup_handle.template ordered_send<RPC_NAME(ordered_put)>(value);
    auto& replies = results.get();
    std::tuple<persistent::version_t,uint64_t> ret(CURRENT_VERSION,0);
    // TODO: verfiy consistency ?
    for (auto& reply_pair : replies) {
        ret = reply_pair.second.get();
    }
    debug_leave_func_with_value("version=0x{:x},timestamp={}",std::get<0>(ret),std::get<1>(ret));
    return ret;
}

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

template<typename KT, typename VT, KT* IK, VT* IV>
const VT VolatileCascadeStore<KT,VT,IK,IV>::get(const KT& key, const persistent::version_t& ver, bool) const {
    debug_enter_func_with_args("key={},ver=0x{:x}",key,ver);
    if (ver != CURRENT_VERSION) {
        debug_leave_func_with_value("Cannot support versioned get, ver=0x{:x}", ver);
        return *IV;
    }
    derecho::Replicated<VolatileCascadeStore>& subgroup_handle = group->template get_subgroup<VolatileCascadeStore>(this->subgroup_index);
    auto results = subgroup_handle.template ordered_send<RPC_NAME(ordered_get)>(key);
    auto& replies = results.get();
    // TODO: verify consistency ?
    // for (auto& reply_pair : replies) {
    //     ret = reply_pair.second.get();
    // }
    debug_leave_func();
    return replies.begin()->second.get();
}

template<typename KT, typename VT, KT* IK, VT* IV>
const VT VolatileCascadeStore<KT,VT,IK,IV>::get_by_time(const KT& key, const uint64_t& ts_us) const {
    // VolatileCascadeStore does not support this.
    debug_enter_func();
    debug_leave_func();

    return *IV;
}

template<typename KT, typename VT, KT* IK, VT* IV>
std::vector<KT> VolatileCascadeStore<KT,VT,IK,IV>::list_keys(const persistent::version_t& ver) const {
    debug_enter_func_with_args("ver=0x{:x}",ver);
    if (ver != CURRENT_VERSION) {
        debug_leave_func_with_value("Cannot support versioned list_keys, ver=0x{:x}", ver);
        return {};
    }
    derecho::Replicated<VolatileCascadeStore>& subgroup_handle = group->template get_subgroup<VolatileCascadeStore>(this->subgroup_index);
    auto results = subgroup_handle.template ordered_send<RPC_NAME(ordered_list_keys)>();
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
std::vector<KT> VolatileCascadeStore<KT,VT,IK,IV>::list_keys_by_time(const uint64_t& ts_us) const {
    // VolatileCascadeStore does not support this.
    debug_enter_func_with_args("ts_us=0x{:x}", ts_us);
    debug_leave_func();
    return {};
}

template<typename KT, typename VT, KT* IK, VT* IV>
uint64_t VolatileCascadeStore<KT,VT,IK,IV>::get_size(const KT& key, const persistent::version_t& ver, bool) const {
    debug_enter_func_with_args("key={},ver=0x{:x}",key,ver);
    if (ver != CURRENT_VERSION) {
        debug_leave_func_with_value("Cannot support versioned get, ver=0x{:x}", ver);
        return 0;
    }
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
uint64_t VolatileCascadeStore<KT,VT,IK,IV>::get_size_by_time(const KT& key, const uint64_t& ts_us) const {
    // VolatileCascadeStore does not support this.
    debug_enter_func();

    debug_leave_func();
    return 0;
}

template<typename KT, typename VT, KT* IK, VT* IV>
std::vector<KT> VolatileCascadeStore<KT,VT,IK,IV>::ordered_list_keys() {
    std::vector<KT> key_list;
    debug_enter_func();
    for(auto kv: this->kv_map) {
        key_list.push_back(kv.first);
    }
    debug_leave_func();
    return key_list;
}

template<typename KT, typename VT, KT* IK, VT* IV>
std::tuple<persistent::version_t,uint64_t> VolatileCascadeStore<KT,VT,IK,IV>::ordered_put(const VT& value) {
    debug_enter_func_with_args("key={}",value.get_key_ref());

    std::tuple<persistent::version_t,uint64_t> version_and_timestamp = group->template get_subgroup<VolatileCascadeStore>(this->subgroup_index).get_next_version();

    if constexpr (std::is_base_of<IKeepVersion,VT>::value) {
        value.set_version(std::get<0>(version_and_timestamp));
    }
    if constexpr (std::is_base_of<IKeepTimestamp,VT>::value) {
        value.set_timestamp(std::get<1>(version_and_timestamp));
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
            return {persistent::INVALID_VERSION,0};
        }
    }
    if constexpr (std::is_base_of<IKeepPreviousVersion,VT>::value) {
        if (this->kv_map.find(value.get_key_ref())!=this->kv_map.end()) {
            value.set_previous_version(this->update_version,this->kv_map.at(value.get_key_ref()).get_version());
        } else {
            value.set_previous_version(this->update_version,persistent::INVALID_VERSION);
        }
    }
    this->kv_map.erase(value.get_key_ref()); // remove
    this->kv_map.emplace(value.get_key_ref(), value); // copy constructor
    this->update_version = std::get<0>(version_and_timestamp);

    if (cascade_watcher_ptr) {
        (*cascade_watcher_ptr)(
            // group->template get_subgroup<VolatileCascadeStore>(this->subgroup_index).get_subgroup_id(), // this is subgroup id
            this->subgroup_index, // this is subgroup index
            group->template get_subgroup<VolatileCascadeStore>(this->subgroup_index).get_shard_num(),
            value.get_key_ref(), value, cascade_context_ptr);
    }

    debug_leave_func_with_value("version=0x{:x},timestamp={}",std::get<0>(version_and_timestamp), std::get<1>(version_and_timestamp));

    return version_and_timestamp;
}

template<typename KT, typename VT, KT* IK, VT* IV>
std::tuple<persistent::version_t,uint64_t> VolatileCascadeStore<KT,VT,IK,IV>::ordered_remove(const KT& key) {
    debug_enter_func_with_args("key={}",key);

    std::tuple<persistent::version_t,uint64_t> version_and_timestamp = group->template get_subgroup<VolatileCascadeStore>(this->subgroup_index).get_next_version();

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
    this->kv_map.erase(key); // remove
    this->kv_map.emplace(key, value);
    this->update_version = std::get<0>(version_and_timestamp);

    if (cascade_watcher_ptr) {
        (*cascade_watcher_ptr)(
            // group->template get_subgroup<VolatileCascadeStore>(this->subgroup_index).get_subgroup_id(), // this is subgroup id
            this->subgroup_index,
            group->template get_subgroup<VolatileCascadeStore>(this->subgroup_index).get_shard_num(),
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
std::unique_ptr<VolatileCascadeStore<KT,VT,IK,IV>> VolatileCascadeStore<KT,VT,IK,IV>::from_bytes(
    mutils::DeserializationManager* dsm, 
    char const* buf) {
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
char* DeltaCascadeStoreCore<KT,VT,IK,IV>::_Delta::data_ptr() {
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
    this->buffer = (char*)realloc(buffer, new_cap);
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
    delta.buffer = (char*)malloc(DEFAULT_DELTA_BUFFER_CAPACITY);
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
void DeltaCascadeStoreCore<KT,VT,IK,IV>::applyDelta(char const* const delta) {
    apply_ordered_put(*mutils::from_bytes<VT>(nullptr,delta));
    mutils::deserialize_and_run(nullptr,delta,[this](const VT& value){
        this->apply_ordered_put(value);
    });
}

template <typename KT, typename VT, KT* IK, VT *IV>
void DeltaCascadeStoreCore<KT,VT,IK,IV>::apply_ordered_put(const VT& value) {
    this->kv_map.erase(value.get_key_ref());
    this->kv_map.emplace(value.get_key_ref(),value);
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
const VT DeltaCascadeStoreCore<KT,VT,IK,IV>::ordered_get(const KT& key) {
    if (kv_map.find(key) != kv_map.end()) {
        return kv_map.at(key);
    } else {
        return *IV;
    }
}

template <typename KT, typename VT, KT* IK, VT* IV>
std::vector<KT> DeltaCascadeStoreCore<KT,VT,IK,IV>::ordered_list_keys() {
    std::vector<KT> key_list;
    for (auto& kv: kv_map) {
        key_list.push_back(kv.first);
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
DeltaCascadeStoreCore<KT,VT,IK,IV>::DeltaCascadeStoreCore() {
    initialize_delta();
}

template <typename KT, typename VT, KT* IK, VT* IV>
DeltaCascadeStoreCore<KT,VT,IK,IV>::DeltaCascadeStoreCore(const std::map<KT,VT>& _kv_map): kv_map(_kv_map) {
    initialize_delta();
}

template <typename KT, typename VT, KT* IK, VT* IV>
DeltaCascadeStoreCore<KT,VT,IK,IV>::DeltaCascadeStoreCore(std::map<KT,VT>&& _kv_map): kv_map(_kv_map) {
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
    derecho::Replicated<PersistentCascadeStore>& subgroup_handle = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index);
    auto results = subgroup_handle.template ordered_send<RPC_NAME(ordered_put)>(value);
    auto& replies = results.get();
    std::tuple<persistent::version_t,uint64_t> ret(CURRENT_VERSION,0);
    // TODO: verfiy consistency ?
    for (auto& reply_pair : replies) {
        ret = reply_pair.second.get();
    }
    debug_leave_func_with_value("version=0x{:x},timestamp={}",std::get<0>(ret),std::get<1>(ret));
    return ret;
}

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
const VT PersistentCascadeStore<KT,VT,IK,IV,ST>::get(const KT& key, const persistent::version_t& ver, bool exact) const {
    debug_enter_func_with_args("key={},ver=0x{:x}",key,ver);
    if (ver != CURRENT_VERSION) {
        debug_leave_func();
        return persistent_core.template getDelta<VT>(ver, [&key,ver,exact,this](const VT& v){
                if (key == v.get_key_ref()) {
                    return v;
                } else {
                    if (exact) {
                        // return invalid object for EXACT search.
                        return *IV;
                    } else {
                        // fall back to the slow path.
                        auto versioned_state_ptr = persistent_core.get(ver);
                        if (versioned_state_ptr->kv_map.find(key) != versioned_state_ptr->kv_map.end()) {
                            return versioned_state_ptr->kv_map.at(key);
                        }
                        return *IV;
                    }
                }
            });
    }
    derecho::Replicated<PersistentCascadeStore>& subgroup_handle = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index);
    auto results = subgroup_handle.template ordered_send<RPC_NAME(ordered_get)>(key);
    auto& replies = results.get();
    // TODO: verify consistency ?
    // for (auto& reply_pair : replies) {
    //     ret = reply_pair.second.get();
    // }
    debug_leave_func();
    return replies.begin()->second.get();
}

template<typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
const VT PersistentCascadeStore<KT,VT,IK,IV,ST>::get_by_time(const KT& key, const uint64_t& ts_us) const {
    debug_enter_func_with_args("key={},ts_us={}",key,ts_us);
    const HLC hlc(ts_us,0ull);
    try {
        debug_leave_func();
        uint64_t idx = persistent_core.getIndexAtTime({ts_us,0});
        if (idx == persistent::INVALID_INDEX) {
            return *IV;
        } else {
            // Reconstructing the state is extremely slow!!!
            // TODO: get the version at time ts_us, and go back from there.
            auto versioned_state_ptr = persistent_core.get(hlc);
            if (versioned_state_ptr->kv_map.find(key) != versioned_state_ptr->kv_map.end()) {
                return versioned_state_ptr->kv_map.at(key);
            }
            return *IV;
        }
    } catch (const int64_t &ex) {
        dbg_default_warn("temporal query throws exception:0x{:x}. key={}, ts={}", ex, key, ts_us);
    } catch (...) {
        dbg_default_warn("temporal query throws unknown exception. key={}, ts={}", key, ts_us);
    }
    debug_leave_func();
    return *IV;
}

template<typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
uint64_t PersistentCascadeStore<KT,VT,IK,IV,ST>::get_size(const KT& key, const persistent::version_t& ver, bool exact) const {
    debug_enter_func_with_args("key={},ver=0x{:x}",key,ver);
    if (ver != CURRENT_VERSION) {
        if (exact) {
            return persistent_core.template getDelta<VT>(ver,[](const VT& value){return mutils::bytes_size(value);});
        } else {
            return mutils::bytes_size(persistent_core.get(ver)->kv_map.at(key));
        }
    }
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
uint64_t PersistentCascadeStore<KT,VT,IK,IV,ST>::get_size_by_time(const KT& key, const uint64_t& ts_us) const {
    debug_enter_func_with_args("key={},ts_us={}",key,ts_us);
    const HLC hlc(ts_us,0ull);
    try {
        debug_leave_func();
        return mutils::bytes_size(persistent_core.get(hlc)->kv_map.at(key));
    } catch (const int64_t &ex) {
        dbg_default_warn("temporal query throws exception:0x{:x}. key={}, ts={}", ex, key, ts_us);
    } catch (...) {
        dbg_default_warn("temporal query throws unknown exception. key={}, ts={}", key, ts_us);
    }
    debug_leave_func();
    return 0;
}

template<typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
std::vector<KT> PersistentCascadeStore<KT,VT,IK,IV,ST>::list_keys(const persistent::version_t& ver) const {
    debug_enter_func_with_args("ver=0x{:x}.",ver);
    if (ver != CURRENT_VERSION) {
        std::vector<KT> key_list;
        auto kv_map = persistent_core.get(ver)->kv_map;
        for (auto& kv:kv_map) {
            key_list.push_back(kv.first);
        }
        debug_leave_func();
        return key_list;
    }
    derecho::Replicated<PersistentCascadeStore>& subgroup_handle = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index);
    auto results = subgroup_handle.template ordered_send<RPC_NAME(ordered_list_keys)>();
    auto& replies = results.get();
    // TODO: verify consistency ?
    debug_leave_func();
    return replies.begin()->second.get();
}

template<typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
std::vector<KT> PersistentCascadeStore<KT,VT,IK,IV,ST>::list_keys_by_time(const uint64_t& ts_us) const {
    debug_enter_func_with_args("ts_us={}",ts_us);
    const HLC hlc(ts_us,0ull);
    try {
        auto kv_map = persistent_core.get(hlc)->kv_map;
        std::vector<KT> key_list;
        for(auto& kv:kv_map) {
            key_list.push_back(kv.first);
        }
        debug_leave_func();
        return key_list;
    } catch (const int64_t& ex) {
        dbg_default_warn("temporal query throws exception:0x{:x]. ts={}", ex, ts_us);
    } catch (...) {
        dbg_default_warn("temporal query throws unknown exception. ts={}",  ts_us);
    }
    debug_leave_func();
    return {};
}

template<typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
std::tuple<persistent::version_t,uint64_t> PersistentCascadeStore<KT,VT,IK,IV,ST>::ordered_put(const VT& value) {
    debug_enter_func_with_args("key={}",value.get_key_ref());
    std::tuple<persistent::version_t,uint64_t> version_and_timestamp = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index).get_next_version();
    if constexpr (std::is_base_of<IKeepVersion,VT>::value) {
        value.set_version(std::get<0>(version_and_timestamp));
    }
    if constexpr (std::is_base_of<IKeepTimestamp,VT>::value) {
        value.set_timestamp(std::get<1>(version_and_timestamp));
    }
    if (this->persistent_core->ordered_put(value,this->persistent_core.getLatestVersion()) == false) {
        // verification failed. S we return invalid versions.
        debug_leave_func_with_value("version=0x{:x},timestamp={}",std::get<0>(version_and_timestamp), std::get<1>(version_and_timestamp));
        return {persistent::INVALID_VERSION,0};
    }
    if (cascade_watcher_ptr) {
        (*cascade_watcher_ptr)(
            // group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index).get_subgroup_id(), // this is subgroup id
            this->subgroup_index,
            group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index).get_shard_num(),
            value.get_key_ref(), value, cascade_context_ptr);
    }

    debug_leave_func_with_value("version=0x{:x},timestamp={}",std::get<0>(version_and_timestamp), std::get<1>(version_and_timestamp));

    return version_and_timestamp;
}

template<typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
std::tuple<persistent::version_t,uint64_t> PersistentCascadeStore<KT,VT,IK,IV,ST>::ordered_remove(const KT& key) {
    debug_enter_func_with_args("key={}",key);
    std::tuple<persistent::version_t,uint64_t> version_and_timestamp = group->template get_subgroup<PersistentCascadeStore>(this->subgroup_index).get_next_version();
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
std::vector<KT> PersistentCascadeStore<KT,VT,IK,IV,ST>::ordered_list_keys() {
    debug_enter_func();

    debug_leave_func();

    return this->persistent_core->ordered_list_keys();
}


template<typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
std::unique_ptr<PersistentCascadeStore<KT,VT,IK,IV,ST>> PersistentCascadeStore<KT,VT,IK,IV,ST>::from_bytes(mutils::DeserializationManager* dsm, char const* buf) {
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
                                               cascade_context_ptr(cc) {}


template<typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
PersistentCascadeStore<KT,VT,IK,IV,ST>::PersistentCascadeStore(
                                               persistent::Persistent<DeltaCascadeStoreCore<KT,VT,IK,IV>,ST>&&
                                               _persistent_core,
                                               CriticalDataPathObserver<PersistentCascadeStore<KT,VT,IK,IV>>* cw,
                                               ICascadeContext* cc):
                                               persistent_core(std::move(_persistent_core)),
                                               cascade_watcher_ptr(cw),
                                               cascade_context_ptr(cc) {}

template<typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
PersistentCascadeStore<KT,VT,IK,IV,ST>::~PersistentCascadeStore() {}

}//namespace cascade
}//namespace derecho
