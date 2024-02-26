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
#include <utility>

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
    /*
     * Transaction support
     *
     * This is a temporary implementation for convenience, see below.
     *
     * TODO For now, this does not persist the list of pending transactions or the transactions themselves. Thus it is impossible to recover from failures.
     *
     * TODO For now, this lives only here for PersistentCascadeStore. After this is working and tested, we may consider turning this into a more general (and improved) implementation.
     *
     * TODO For now, everything is copied on instantiation. Ideally, in the future we should only copy the objects when the transaction is committed.
     *
     */
    class CascadeTransaction {
    public:
        transaction_id txid;
        transaction_status_t status = transaction_status_t::PENDING;
        std::map<std::pair<uint32_t,uint32_t>,std::vector<VT>> mapped_objects;
        std::map<std::pair<uint32_t,uint32_t>,std::vector<std::tuple<KT,persistent::version_t,persistent::version_t,persistent::version_t>>> mapped_readonly_keys;
        std::vector<std::pair<uint32_t,uint32_t>> shard_list;

        // copy constructor 
        CascadeTransaction(
                const transaction_id& txid,
                const std::map<std::pair<uint32_t,uint32_t>,std::vector<VT>>& mapped_objects,
                const std::map<std::pair<uint32_t,uint32_t>,std::vector<std::tuple<KT,persistent::version_t,persistent::version_t,persistent::version_t>>>& mapped_readonly_keys,
                const std::vector<std::pair<uint32_t,uint32_t>>& shard_list):
            txid(txid),
            mapped_objects(mapped_objects), // TODO check if the Blobs are actually getting copied
            mapped_readonly_keys(mapped_readonly_keys),
            shard_list(shard_list) {}

        /* 
         * Checks if this tx conflicts with another in the given shard. It is assumed that the lists of objects are all sorted in both transactions.
         */
        bool conflicts(const CascadeTransaction* other,const std::pair<uint32_t,uint32_t>& shard_id);
        
        // check if a single object conflicts with this tx in the given shard
        bool conflicts(const VT& object,const std::pair<uint32_t,uint32_t>& shard_id);
    };

    std::map<transaction_id,CascadeTransaction*> transaction_database;
    std::vector<transaction_id> pending_transactions;
    std::map<transaction_id,bool> versions_checked;

    transaction_id new_transaction_id();
    bool has_conflict(const CascadeTransaction* tx,const std::pair<uint32_t,uint32_t>& shard_id);
    bool has_conflict(const VT& other,const std::pair<uint32_t,uint32_t>& shard_id);
    bool check_previous_versions(const CascadeTransaction* tx,const std::pair<uint32_t,uint32_t>& shard_id);
    void commit_transaction(const CascadeTransaction* tx,const std::pair<uint32_t,uint32_t>& shard_id);

    // ======= end of new transactional code =======

    // internal helpers
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
                                                     put_objects,
                                                     put_objects_forward,
                                                     put_objects_backward,
                                                     put_and_forget,
#ifdef ENABLE_EVALUATION
                                                     perf_put,
#endif  // ENABLE_EVALUATION
                                                     remove,
                                                     get,
                                                     get_transaction_status,
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
                                                     ordered_put_objects,
                                                     ordered_put_objects_forward,
                                                     ordered_put_objects_backward,
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
    virtual version_tuple put(const VT& value) const override;
    virtual std::pair<transaction_id,transaction_status_t> put_objects(
            const std::map<std::pair<uint32_t,uint32_t>,std::vector<VT>>& mapped_objects,
            const std::map<std::pair<uint32_t,uint32_t>,std::vector<std::tuple<KT,persistent::version_t,persistent::version_t,persistent::version_t>>>& mapped_readonly_keys,
            const std::vector<std::pair<uint32_t,uint32_t>>& shard_list) const override;
    virtual void put_objects_forward(
            const transaction_id& txid,
            const std::map<std::pair<uint32_t,uint32_t>,std::vector<VT>>& mapped_objects,
            const std::map<std::pair<uint32_t,uint32_t>,std::vector<std::tuple<KT,persistent::version_t,persistent::version_t,persistent::version_t>>>& mapped_readonly_keys,
            const std::vector<std::pair<uint32_t,uint32_t>>& shard_list) const override;
    virtual void put_objects_backward(const transaction_id& txid,const transaction_status_t& status) const override;
    virtual void put_and_forget(const VT& value) const override;
#ifdef ENABLE_EVALUATION
    virtual double perf_put(const uint32_t max_payload_size, const uint64_t duration_sec) const override;
#endif  // ENABLE_EVALUATION
    virtual version_tuple remove(const KT& key) const override;
    virtual const VT get(const KT& key, const persistent::version_t& ver, const bool stable, bool exact = false) const override;
    virtual transaction_status_t get_transaction_status(const transaction_id& txid) const override;
    virtual const VT multi_get(const KT& key) const override;
    virtual const VT get_by_time(const KT& key, const uint64_t& ts_us, const bool stable) const override;
    virtual std::vector<KT> multi_list_keys(const std::string& prefix) const override;
    virtual std::vector<KT> list_keys(const std::string& prefix, const persistent::version_t& ver, const bool stable) const override;
    virtual std::vector<KT> list_keys_by_time(const std::string& prefix, const uint64_t& ts_us, const bool stable) const override;
    virtual uint64_t multi_get_size(const KT& key) const override;
    virtual uint64_t get_size(const KT& key, const persistent::version_t& ver, const bool stable, bool exact = false) const override;
    virtual uint64_t get_size_by_time(const KT& key, const uint64_t& ts_us, const bool stable) const override;
    virtual version_tuple ordered_put(const VT& value) override;
    virtual std::pair<transaction_id,transaction_status_t> ordered_put_objects(
            const std::map<std::pair<uint32_t,uint32_t>,std::vector<VT>>& mapped_objects,
            const std::map<std::pair<uint32_t,uint32_t>,std::vector<std::tuple<KT,persistent::version_t,persistent::version_t,persistent::version_t>>>& mapped_readonly_keys,
            const std::vector<std::pair<uint32_t,uint32_t>>& shard_list) override;
    virtual void ordered_put_objects_forward(
            const transaction_id& txid,
            const std::map<std::pair<uint32_t,uint32_t>,std::vector<VT>>& mapped_objects,
            const std::map<std::pair<uint32_t,uint32_t>,std::vector<std::tuple<KT,persistent::version_t,persistent::version_t,persistent::version_t>>>& mapped_readonly_keys,
            const std::vector<std::pair<uint32_t,uint32_t>>& shard_list) override;
    virtual void ordered_put_objects_backward(const transaction_id& txid,const transaction_status_t& status) override;
    virtual void ordered_put_and_forget(const VT& value) override;
    virtual version_tuple ordered_remove(const KT& key) override;
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
