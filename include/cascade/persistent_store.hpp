#pragma once

#include "cascade_interface.hpp"
#include "detail/delta_store_core.hpp"

#include <derecho/core/derecho.hpp>
#include <derecho/mutils-serialization/SerializationSupport.hpp>

#include <atomic>
#include <cstdint>
#include <memory>
#include <tuple>
#include <vector>
#include <list>
#include <utility>
#include <unordered_set>
#include <unordered_map>
#include "boost/functional/hash.hpp"

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
     * This is a temporary implementation
     *
     * TODO For now, this does not persist the list of pending transactions or the transactions themselves. Thus it is impossible to recover from failures.
     *
     * TODO For now, this lives only here for PersistentCascadeStore. After this is working and tested, we may consider turning this into a more general (and improved) implementation.
     *
     * TODO For now, everything is copied on instantiation. Ideally, in the future we should only copy the objects when the transaction is committed.
     *
     */
    class CascadeTransactionInternal {
    public:
        transaction_id txid;
        transaction_status_t status = transaction_status_t::PENDING;
        persistent::version_t commit_version = persistent::INVALID_VERSION;
        std::vector<uint32_t>::iterator this_shard_it;
        typename std::list<CascadeTransactionInternal*>::iterator queue_it;

        std::vector<VT> write_objects;
        std::vector<std::tuple<KT,persistent::version_t,persistent::version_t,persistent::version_t>> read_objects;
        std::unordered_set<KT> write_keys;
        std::unordered_set<KT> read_keys;
        std::vector<uint32_t> shard_list;

        // copy constructor 
        CascadeTransactionInternal(
                const transaction_id& txid,
                const std::vector<VT>& write_objects,
                const std::unordered_map<uint32_t,std::vector<std::size_t>>& write_objects_per_shard,
                const std::vector<std::tuple<KT,persistent::version_t,persistent::version_t,persistent::version_t>>& read_objects,
                const std::unordered_map<uint32_t,std::vector<std::size_t>>& read_objects_per_shard,
                const std::vector<uint32_t>& shard_list,
                uint32_t shard_index);

        bool conflicts(CascadeTransactionInternal* other);
        bool conflicts(const KT& key);
    };

    struct TxidHash {
        std::size_t operator()(const transaction_id& key) const {
            return boost::hash_value(key);
        }
    };

    std::list<CascadeTransactionInternal*> pending_transactions;
    std::unordered_map<transaction_id,CascadeTransactionInternal*,TxidHash> transaction_database;
    std::unordered_map<CascadeTransactionInternal*,bool> versions_checked;
    std::unordered_map<CascadeTransactionInternal*,std::list<CascadeTransactionInternal*>> forward_conflicts;
    std::unordered_map<CascadeTransactionInternal*,std::list<CascadeTransactionInternal*>> backward_conflicts;

    transaction_id new_transaction_id();
    void enqueue_transaction(CascadeTransactionInternal* tx);
    void dequeue_transaction(CascadeTransactionInternal* tx);
    bool has_conflict(CascadeTransactionInternal* tx);
    bool has_conflict(const VT& value);
    bool check_previous_versions(CascadeTransactionInternal* tx);
    void commit_transaction(CascadeTransactionInternal* tx);

    // helpers
    void send_tx_forward(
            CascadeTransactionInternal* tx,
            const std::vector<VT>& write_objects,
            const std::unordered_map<uint32_t,std::vector<std::size_t>>& write_objects_per_shard,
            const std::vector<std::tuple<KT,persistent::version_t,persistent::version_t,persistent::version_t>>& read_objects,
            const std::unordered_map<uint32_t,std::vector<std::size_t>>& read_objects_per_shard,
            const std::vector<uint32_t>& shard_list);
    void send_tx_status_backward(CascadeTransactionInternal* tx);
    void tx_committed_recursive(CascadeTransactionInternal* tx);
    void tx_aborted_recursive(CascadeTransactionInternal* tx);
    void tx_run_recursive(CascadeTransactionInternal* tx);

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
            const std::vector<VT>& write_objects,
            const std::unordered_map<uint32_t,std::vector<std::size_t>>& write_objects_per_shard,
            const std::vector<std::tuple<KT,persistent::version_t,persistent::version_t,persistent::version_t>>& read_objects,
            const std::unordered_map<uint32_t,std::vector<std::size_t>>& read_objects_per_shard,
            const std::vector<uint32_t>& shard_list) const override;
    virtual void put_objects_forward(
            const transaction_id& txid,
            const std::vector<VT>& write_objects,
            const std::unordered_map<uint32_t,std::vector<std::size_t>>& write_objects_per_shard,
            const std::vector<std::tuple<KT,persistent::version_t,persistent::version_t,persistent::version_t>>& read_objects,
            const std::unordered_map<uint32_t,std::vector<std::size_t>>& read_objects_per_shard,
            const std::vector<uint32_t>& shard_list) const override;
    virtual void put_objects_backward(const transaction_id& txid,const transaction_status_t& status) const override;
    virtual void put_and_forget(const VT& value) const override;
#ifdef ENABLE_EVALUATION
    virtual double perf_put(const uint32_t max_payload_size, const uint64_t duration_sec) const override;
#endif  // ENABLE_EVALUATION
    virtual version_tuple remove(const KT& key) const override;
    virtual const VT get(const KT& key, const persistent::version_t& ver, const bool stable, bool exact = false) const override;
    virtual transaction_status_t get_transaction_status(const transaction_id& txid, const bool stable) const override;
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
            const std::vector<VT>& write_objects,
            const std::unordered_map<uint32_t,std::vector<std::size_t>>& write_objects_per_shard,
            const std::vector<std::tuple<KT,persistent::version_t,persistent::version_t,persistent::version_t>>& read_objects,
            const std::unordered_map<uint32_t,std::vector<std::size_t>>& read_objects_per_shard,
            const std::vector<uint32_t>& shard_list) override;
    virtual void ordered_put_objects_forward(
            const transaction_id& txid,
            const std::vector<VT>& write_objects,
            const std::unordered_map<uint32_t,std::vector<std::size_t>>& write_objects_per_shard,
            const std::vector<std::tuple<KT,persistent::version_t,persistent::version_t,persistent::version_t>>& read_objects,
            const std::unordered_map<uint32_t,std::vector<std::size_t>>& read_objects_per_shard,
            const std::vector<uint32_t>& shard_list) override;
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
