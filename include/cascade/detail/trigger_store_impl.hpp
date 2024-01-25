#pragma once
#include "../trigger_store.hpp"

#include "cascade/config.h"
#include "cascade/utils.hpp"
#include "debug_util.hpp"

#include <cstdint>
#include <memory>
#include <string>
#include <tuple>

#ifdef ENABLE_EVALUATION
#include <derecho/utils/time.h>
#endif

namespace derecho {
namespace cascade {

template <typename KT, typename VT, KT* IK, VT* IV>
version_tuple TriggerCascadeNoStore<KT, VT, IK, IV>::put(const VT& value) const {
    dbg_default_warn("Calling unsupported func:{}", __PRETTY_FUNCTION__);
    return {persistent::INVALID_VERSION, 0};
}

template <typename KT, typename VT, KT* IK, VT* IV>
void TriggerCascadeNoStore<KT, VT, IK, IV>::put_and_forget(const VT& value) const {
    dbg_default_warn("Calling unsupported func:{}", __PRETTY_FUNCTION__);
}

#ifdef ENABLE_EVALUATION
template <typename KT, typename VT, KT* IK, VT* IV>
double TriggerCascadeNoStore<KT, VT, IK, IV>::perf_put(const uint32_t max_payload_size, const uint64_t duration_sec) const {
    dbg_default_warn("Calling unsupported func:{}", __PRETTY_FUNCTION__);
    return 0.0;
}
#endif  // ENABLE_EVALUATION

template <typename KT, typename VT, KT* IK, VT* IV>
version_tuple TriggerCascadeNoStore<KT, VT, IK, IV>::remove(const KT& key) const {
    dbg_default_warn("Calling unsupported func:{}", __PRETTY_FUNCTION__);
    return {persistent::INVALID_VERSION, 0};
}

template <typename KT, typename VT, KT* IK, VT* IV>
const VT TriggerCascadeNoStore<KT, VT, IK, IV>::get(const KT& key, const persistent::version_t& ver, bool, bool) const {
    dbg_default_warn("Calling unsupported func:{}", __PRETTY_FUNCTION__);
    return *IV;
}

template <typename KT, typename VT, KT* IK, VT* IV>
const VT TriggerCascadeNoStore<KT, VT, IK, IV>::multi_get(const KT& key) const {
    dbg_default_warn("Calling unsupported func:{}", __PRETTY_FUNCTION__);
    return *IV;
}

template <typename KT, typename VT, KT* IK, VT* IV>
const VT TriggerCascadeNoStore<KT, VT, IK, IV>::get_by_time(const KT& key, const uint64_t& ts_us, const bool stable) const {
    dbg_default_warn("Calling unsupported func:{}", __PRETTY_FUNCTION__);
    return *IV;
}

template <typename KT, typename VT, KT* IK, VT* IV>
std::vector<KT> TriggerCascadeNoStore<KT, VT, IK, IV>::multi_list_keys(const std::string& prefix) const {
    dbg_default_warn("Calling unsupported func:{}", __PRETTY_FUNCTION__);
    return {};
}

template <typename KT, typename VT, KT* IK, VT* IV>
std::vector<KT> TriggerCascadeNoStore<KT, VT, IK, IV>::list_keys(const std::string& prefix, const persistent::version_t& ver, const bool stable) const {
    dbg_default_warn("Calling unsupported func:{}", __PRETTY_FUNCTION__);
    return {};
}

template <typename KT, typename VT, KT* IK, VT* IV>
std::vector<KT> TriggerCascadeNoStore<KT, VT, IK, IV>::list_keys_by_time(const std::string& prefix, const uint64_t& ts_us, const bool stable) const {
    dbg_default_warn("Calling unsupported func:{}", __PRETTY_FUNCTION__);
    return {};
}

template <typename KT, typename VT, KT* IK, VT* IV>
uint64_t TriggerCascadeNoStore<KT, VT, IK, IV>::multi_get_size(const KT& key) const {
    dbg_default_warn("Calling unsupported func:{}", __PRETTY_FUNCTION__);
    return 0;
}

template <typename KT, typename VT, KT* IK, VT* IV>
uint64_t TriggerCascadeNoStore<KT, VT, IK, IV>::get_size(const KT& key, const persistent::version_t& ver, const bool stable, bool extract) const {
    dbg_default_warn("Calling unsupported func:{}", __PRETTY_FUNCTION__);
    return 0;
}

template <typename KT, typename VT, KT* IK, VT* IV>
uint64_t TriggerCascadeNoStore<KT, VT, IK, IV>::get_size_by_time(const KT& key, const uint64_t& ts_us, const bool stable) const {
    dbg_default_warn("Calling unsupported func:{}", __PRETTY_FUNCTION__);
    return 0;
}

template <typename KT, typename VT, KT* IK, VT* IV>
std::vector<KT> TriggerCascadeNoStore<KT, VT, IK, IV>::ordered_list_keys(const std::string& prefix) {
    dbg_default_warn("Calling unsupported func:{}", __PRETTY_FUNCTION__);
    return {};
}

template <typename KT, typename VT, KT* IK, VT* IV>
version_tuple TriggerCascadeNoStore<KT, VT, IK, IV>::ordered_put(const VT& value) {
    dbg_default_warn("Calling unsupported func:{}", __PRETTY_FUNCTION__);
    return {persistent::INVALID_VERSION, 0};
}

template <typename KT, typename VT, KT* IK, VT* IV>
void TriggerCascadeNoStore<KT, VT, IK, IV>::ordered_put_and_forget(const VT& value) {
    dbg_default_warn("Calling unsupported func:{}", __PRETTY_FUNCTION__);
}

template <typename KT, typename VT, KT* IK, VT* IV>
version_tuple TriggerCascadeNoStore<KT, VT, IK, IV>::ordered_remove(const KT& key) {
    dbg_default_warn("Calling unsupported func:{}", __PRETTY_FUNCTION__);
    return {persistent::INVALID_VERSION, 0};
}

template <typename KT, typename VT, KT* IK, VT* IV>
const VT TriggerCascadeNoStore<KT, VT, IK, IV>::ordered_get(const KT& key) {
    dbg_default_warn("Calling unsupported func:{}", __PRETTY_FUNCTION__);
    return *IV;
}

template <typename KT, typename VT, KT* IK, VT* IV>
uint64_t TriggerCascadeNoStore<KT, VT, IK, IV>::ordered_get_size(const KT& key) {
    dbg_default_warn("Calling unsupported func:{}", __PRETTY_FUNCTION__);
    return 0;
}

template <typename KT, typename VT, KT* IK, VT* IV>
void TriggerCascadeNoStore<KT, VT, IK, IV>::trigger_put(const VT& value) const {
    debug_enter_func_with_args("key={}", value.get_key_ref());
    LOG_TIMESTAMP_BY_TAG(TLT_TRIGGER_PUT_START, group, value);

    if(cascade_watcher_ptr) {
        (*cascade_watcher_ptr)(
                this->subgroup_index,
                group->template get_subgroup<TriggerCascadeNoStore<KT, VT, IK, IV>>(this->subgroup_index).get_shard_num(),
                group->get_rpc_caller_id(),
                value.get_key_ref(), value, cascade_context_ptr, true);
    }

    LOG_TIMESTAMP_BY_TAG(TLT_TRIGGER_PUT_END, group, value);
    debug_leave_func();
}

#ifdef ENABLE_EVALUATION

template <typename KT, typename VT, KT* IK, VT* IV>
void TriggerCascadeNoStore<KT, VT, IK, IV>::dump_timestamp_log(const std::string& filename) const {
    debug_enter_func_with_args("filename={}", filename);
    derecho::Replicated<TriggerCascadeNoStore>& subgroup_handle = group->template get_subgroup<TriggerCascadeNoStore>(this->subgroup_index);
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
void TriggerCascadeNoStore<KT, VT, IK, IV>::ordered_dump_timestamp_log(const std::string& filename) {
    debug_enter_func_with_args("filename={}", filename);
    TimestampLogger::flush(filename);
    debug_leave_func();
}
#ifdef DUMP_TIMESTAMP_WORKAROUND
template <typename KT, typename VT, KT* IK, VT* IV>
void TriggerCascadeNoStore<KT, VT, IK, IV>::dump_timestamp_log_workaround(const std::string& filename) const {
    debug_enter_func_with_args("filename={}", filename);
    TimestampLogger::flush(filename);
    debug_leave_func();
}
#endif
#endif  // ENABLE_EVALUATION

template <typename KT, typename VT, KT* IK, VT* IV>
std::unique_ptr<TriggerCascadeNoStore<KT, VT, IK, IV>> TriggerCascadeNoStore<KT, VT, IK, IV>::from_bytes(mutils::DeserializationManager* dsm, uint8_t const* buf) {
    return std::make_unique<TriggerCascadeNoStore<KT, VT, IK, IV>>(
            dsm->registered<CriticalDataPathObserver<TriggerCascadeNoStore<KT, VT, IK, IV>>>() ? &(dsm->mgr<CriticalDataPathObserver<TriggerCascadeNoStore<KT, VT, IK, IV>>>()) : nullptr,
            dsm->registered<ICascadeContext>() ? &(dsm->mgr<ICascadeContext>()) : nullptr);
}

template <typename KT, typename VT, KT* IK, VT* IV>
mutils::context_ptr<TriggerCascadeNoStore<KT, VT, IK, IV>> TriggerCascadeNoStore<KT, VT, IK, IV>::from_bytes_noalloc(mutils::DeserializationManager* dsm, uint8_t const* buf) {
    return mutils::context_ptr<TriggerCascadeNoStore>(from_bytes(dsm, buf));
}

template <typename KT, typename VT, KT* IK, VT* IV>
TriggerCascadeNoStore<KT, VT, IK, IV>::TriggerCascadeNoStore(CriticalDataPathObserver<TriggerCascadeNoStore<KT, VT, IK, IV>>* cw,
                                                             ICascadeContext* cc) : cascade_watcher_ptr(cw),
                                                                                    cascade_context_ptr(cc) {}
}  // namespace cascade
}  // namespace derecho
