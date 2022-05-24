#pragma once

#include "cascade_interface.hpp"
#include "detail/delta_store_core.hpp"

#include <derecho/core/derecho.hpp>
#include <derecho/mutils-serialization/SerializationSupport.hpp>

#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <tuple>
#include <vector>

namespace derecho {
namespace cascade {

/**
 * template for persistent cascade stores.
 *
 * PersistentCascadeStore is full-fledged implementation with log mechansim. Data can be stored in different
 * persistent devices including file system(persistent::ST_FILE) or SPDK(persistent::ST_SPDK). Please note that the
 * data is cached in memory too.
 */
template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST = persistent::ST_FILE>
class PersistentCascadeStore : public ICascadeStore<KT, VT, IK, IV>,
                               public mutils::ByteRepresentable,
                               public derecho::PersistsFields,
                               public derecho::GroupReference,
                               public derecho::NotificationSupport {
private:
    bool internal_ordered_put(const VT& value);

public:
    using derecho::GroupReference::group;
    persistent::Persistent<DeltaCascadeStoreCore<KT, VT, IK, IV>, ST> persistent_core;
    CriticalDataPathObserver<PersistentCascadeStore<KT, VT, IK, IV>>* cascade_watcher_ptr;
    /* cascade context */
    ICascadeContext* cascade_context_ptr;

    REGISTER_RPC_FUNCTIONS_WITH_NOTIFICATION(PersistentCascadeStore,
                                             P2P_TARGETS(
                                                     put,
                                                     put_and_forget,
#ifdef ENABLE_EVALUATION
                                                     perf_put,
#endif  // ENABLE_EVALUATION
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
                                                     trigger_put
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
    virtual std::tuple<persistent::version_t, uint64_t> put(const VT& value) const override;
    virtual void put_and_forget(const VT& value) const override;
#ifdef ENABLE_EVALUATION
    virtual double perf_put(const uint32_t max_payload_size, const uint64_t duration_sec) const override;
#endif  // ENABLE_EVALUATION
    virtual std::tuple<persistent::version_t, uint64_t> remove(const KT& key) const override;
    virtual const VT get(const KT& key, const persistent::version_t& ver, const bool stable, bool exact = false) const override;
    virtual const VT multi_get(const KT& key) const override;
    virtual const VT get_by_time(const KT& key, const uint64_t& ts_us, const bool stable) const override;
    virtual std::vector<KT> multi_list_keys(const std::string& prefix) const override;
    virtual std::vector<KT> list_keys(const std::string& prefix, const persistent::version_t& ver, const bool stable) const override;
    virtual std::vector<KT> list_keys_by_time(const std::string& prefix, const uint64_t& ts_us, const bool stable) const override;
    virtual uint64_t multi_get_size(const KT& key) const override;
    virtual uint64_t get_size(const KT& key, const persistent::version_t& ver, const bool stable, bool exact = false) const override;
    virtual uint64_t get_size_by_time(const KT& key, const uint64_t& ts_us, const bool stable) const override;
    virtual std::tuple<persistent::version_t, uint64_t> ordered_put(const VT& value) override;
    virtual void ordered_put_and_forget(const VT& value) override;
    virtual std::tuple<persistent::version_t, uint64_t> ordered_remove(const KT& key) override;
    virtual const VT ordered_get(const KT& key) override;
    virtual std::vector<KT> ordered_list_keys(const std::string& prefix) override;
    virtual uint64_t ordered_get_size(const KT& key) override;
#ifdef ENABLE_EVALUATION
    virtual void ordered_dump_timestamp_log(const std::string& filename) override;
#endif  // ENABLE_EVALUATION

    // serialization support
    DEFAULT_SERIALIZE(persistent_core);

    static std::unique_ptr<PersistentCascadeStore> from_bytes(mutils::DeserializationManager* dsm, uint8_t const* buf);

    DEFAULT_DESERIALIZE_NOALLOC(PersistentCascadeStore);

    void ensure_registered(mutils::DeserializationManager&) {}

    // constructors
    PersistentCascadeStore(persistent::PersistentRegistry* pr,
                           CriticalDataPathObserver<PersistentCascadeStore<KT, VT, IK, IV>>* cw = nullptr,
                           ICascadeContext* cc = nullptr);
    PersistentCascadeStore(persistent::Persistent<DeltaCascadeStoreCore<KT, VT, IK, IV>, ST>&& _persistent_core,
                           CriticalDataPathObserver<PersistentCascadeStore<KT, VT, IK, IV>>* cw = nullptr,
                           ICascadeContext* cc = nullptr);  // move persistent_core
    PersistentCascadeStore();

    // destructor
    virtual ~PersistentCascadeStore();
};
}  // namespace cascade
}  // namespace derecho

#include "detail/persistent_store_impl.hpp"
