#pragma once

#include "cascade/config.h"
#include "cascade_interface.hpp"

#include <derecho/core/derecho.hpp>
#include <derecho/mutils-serialization/SerializationSupport.hpp>
#include <derecho/persistent/Persistent.hpp>

#include <map>
#include <atomic>
#include <vector>

namespace derecho {
namespace cascade {
/**
 * template volatile cascade stores.
 *
 * VolatileCascadeStore is highly efficient by manage all the data only in the memory without implementing the heavy
 * log mechanism. Reading by version or time will always return invlaid value.
 */
template <typename KT, typename VT, KT* IK, VT* IV>
class VolatileCascadeStore : public ICascadeStore<KT, VT, IK, IV>,
                             public mutils::ByteRepresentable,
                             public derecho::GroupReference,
                             public derecho::NotificationSupport {
private:
    bool internal_ordered_put(const VT& value, bool as_trigger);
    void*   oob_mr_ptr;
    size_t  oob_mr_size;
#if defined(__i386__) || defined(__x86_64__) || defined(_M_AMD64) || defined(_M_IX86)
    mutable std::atomic<persistent::version_t> lockless_v1;
    mutable std::atomic<persistent::version_t> lockless_v2;
#else
#error The lockless reader/writer works only with TSO memory reordering. Please check https://en.wikipedia.org/wiki/Memory_ordering
#endif
public:
    /* group reference */
    using derecho::GroupReference::group;
    /* volatile cascade store in memory */
    std::map<KT, VT> kv_map;
    /* record the version of latest update */
    persistent::version_t update_version;
    /* watcher */
    CriticalDataPathObserver<VolatileCascadeStore<KT, VT, IK, IV>>* cascade_watcher_ptr;
    /* cascade context */
    ICascadeContext* cascade_context_ptr;

    REGISTER_RPC_FUNCTIONS_WITH_NOTIFICATION(VolatileCascadeStore,
                                             P2P_TARGETS(
                                                     put,
                                                     put_and_forget,
#ifdef ENABLE_EVALUATION
                                                     perf_put,
#endif
                                                     remove,
                                                     get,
                                                     multi_get,
                                                     get_by_time,
                                                     multi_list_keys,
                                                     list_keys,
                                                     list_keys_by_time,
                                                     multi_get_size,
                                                     get_size,
                                                     get_size_by_time,
                                                     trigger_put.
						     oob_send
#ifdef ENABLE_EVALUATION
                                                     ,
                                                     dump_timestamp_log
#ifdef DUMP_TIMESTAMP_WORKAROUND
                                                     ,
                                                     dump_timestamp_log_workaround
#endif
#endif  // ENABLE_EVALUATION
                                                     ),
                                             ORDERED_TARGETS(
                                                     ordered_put,
                                                     ordered_put_and_forget,
                                                     ordered_remove,
                                                     ordered_get,
                                                     ordered_list_keys,
                                                     ordered_get_size
#ifdef ENABLE_EVALUATION
                                                     ,
                                                     ordered_dump_timestamp_log
#endif  // ENABLE_EVALUATION
                                                     ));
#ifdef ENABLE_EVALUATION
    virtual void dump_timestamp_log(const std::string& filename) const override;
#ifdef DUMP_TIMESTAMP_WORKAROUND
    virtual void dump_timestamp_log_workaround(const std::string& filename) const override;
#endif
#endif  // ENABLE_EVALUATION
    virtual void trigger_put(const VT& value) const override;
    virtual version_tuple put(const VT& value, bool as_trigger) const override;
    virtual bool oob_send(uint64_t data_addr, uint64_t gpu_addr, uint64_t rkey, size_t size) const override;
    void oob_reg_mem(void* addr, size_t size);
    void oob_dereg_mem(void* addr);
#ifdef ENABLE_EVALUATION
    virtual double perf_put(const uint32_t max_payload_size, const uint64_t duration_sec) const override;
#endif  // ENABLE_EVALUATION
    virtual void put_and_forget(const VT& value, bool as_trigger) const override;
    virtual version_tuple remove(const KT& key) const override;
    virtual const VT get(const KT& key, const persistent::version_t& ver, const bool stable, bool exact = false) const override;
    virtual const VT multi_get(const KT& key) const override;
    virtual const VT get_by_time(const KT& key, const uint64_t& ts_us, const bool stable) const override;
    virtual std::vector<KT> multi_list_keys(const std::string& prefix) const override;
    virtual std::vector<KT> list_keys(const std::string& prefix, const persistent::version_t& ver, const bool stable) const override;
    virtual std::vector<KT> list_keys_by_time(const std::string& prefix, const uint64_t& ts_us, const bool stable) const override;
    virtual uint64_t multi_get_size(const KT& key) const override;
    virtual uint64_t get_size(const KT& key, const persistent::version_t& ver, const bool stable, bool exact = false) const override;
    virtual uint64_t get_size_by_time(const KT& key, const uint64_t& ts_us, const bool stable) const override;
    virtual version_tuple ordered_put(const VT& value, bool as_trigger) override;
    virtual void ordered_put_and_forget(const VT& value, bool as_trigger) override;
    virtual version_tuple ordered_remove(const KT& key) override;
    virtual const VT ordered_get(const KT& key) override;
    virtual std::vector<KT> ordered_list_keys(const std::string& prefix) override;
    virtual uint64_t ordered_get_size(const KT& key) override;
#ifdef ENABLE_EVALUATION
    virtual void ordered_dump_timestamp_log(const std::string& filename) override;
#endif  // ENABLE_EVALUATION

    // serialization support
    DEFAULT_SERIALIZE(kv_map, update_version);

    static std::unique_ptr<VolatileCascadeStore> from_bytes(mutils::DeserializationManager* dsm, uint8_t const* buf);

    DEFAULT_DESERIALIZE_NOALLOC(VolatileCascadeStore);

    void ensure_registered(mutils::DeserializationManager&) {}

    /* constructors */
    VolatileCascadeStore(CriticalDataPathObserver<VolatileCascadeStore<KT, VT, IK, IV>>* cw = nullptr,
                         ICascadeContext* cc = nullptr);
    VolatileCascadeStore(const std::map<KT, VT>& _kvm,
                         persistent::version_t _uv,
                         CriticalDataPathObserver<VolatileCascadeStore<KT, VT, IK, IV>>* cw = nullptr,
                         ICascadeContext* cc = nullptr);  // copy kv_map
    VolatileCascadeStore(std::map<KT, VT>&& _kvm,
                         persistent::version_t _uv,
                         CriticalDataPathObserver<VolatileCascadeStore<KT, VT, IK, IV>>* cw = nullptr,
                         ICascadeContext* cc = nullptr);  // move kv_map
};
}  // namespace cascade
}  // namespace derecho

#include "detail/volatile_store_impl.hpp"
