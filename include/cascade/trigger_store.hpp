#pragma once

#include "cascade_interface.hpp"

#include "cascade/config.h"

#include <derecho/core/derecho.hpp>
#include <derecho/mutils-serialization/SerializationSupport.hpp>

#include <memory>
#include <string>
#include <tuple>
#include <vector>

namespace derecho {
namespace cascade {

/**
 * Template for cascade trigger store
 *
 * @tparam KT   key type
 * @tparam VT   value type
 * @tparam IK   a pointer to invalid key
 * @tparam IV   a pointer to invalid value
 */
template <typename KT, typename VT, KT* IK, VT* IV>
class TriggerCascadeNoStore : public ICascadeStore<KT, VT, IK, IV>,
                              public mutils::ByteRepresentable,
                              public derecho::GroupReference,
                              public derecho::NotificationSupport {
public:
    using derecho::GroupReference::group;
    CriticalDataPathObserver<TriggerCascadeNoStore<KT, VT, IK, IV>>* cascade_watcher_ptr;
    /* cascade context */
    ICascadeContext* cascade_context_ptr;

    REGISTER_RPC_FUNCTIONS_WITH_NOTIFICATION(TriggerCascadeNoStore,
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
                                                     trigger_put
#ifdef ENABLE_EVALUATION
                                                     ,
                                                     dump_timestamp_log
#ifdef DUMP_TIMESTAMP_WORKAROUND
                                                     ,
                                                     dump_timestamp_log_workaround
#endif
#endif
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
#endif
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
    virtual std::size_t to_bytes(uint8_t*) const override { return 0; }
    virtual void post_object(const std::function<void(uint8_t const* const, std::size_t)>&) const override {}
    virtual std::size_t bytes_size() const { return 0; }
    static std::unique_ptr<TriggerCascadeNoStore<KT, VT, IK, IV>> from_bytes(mutils::DeserializationManager*, const uint8_t*);
    static mutils::context_ptr<TriggerCascadeNoStore<KT, VT, IK, IV>> from_bytes_noalloc(mutils::DeserializationManager*, uint8_t const*);
    void ensure_registered(mutils::DeserializationManager&) {}

    // constructors
    TriggerCascadeNoStore(CriticalDataPathObserver<TriggerCascadeNoStore<KT, VT, IK, IV>>* cw = nullptr,
                          ICascadeContext* cc = nullptr);
};

/**
 * get_pathname(): retrieve the pathname, a.k.a prefix from a key.
 * A pathname identifies the object pool this object belongs to.
 *
 * @tparam KeyType - Type of the Key
 * @param  key     - key
 *
 * @return pathname. An empty string returns for invalid key types and invalid keys.
 */
template <typename KeyType>
inline std::string get_pathname(const KeyType& key);

}  // namespace cascade
}  // namespace derecho

#include "detail/trigger_store_impl.hpp"
