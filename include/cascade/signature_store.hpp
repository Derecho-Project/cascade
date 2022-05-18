#pragma once

#include "cascade_interface.hpp"
#include "detail/delta_store_core.hpp"

#include <derecho/core/derecho.hpp>

#include <cstdint>
#include <map>
#include <mutex>
#include <string>
#include <vector>

namespace derecho {
namespace cascade {

/**
 * This subgroup type creates a node that stores signed hashes in a persistent log.
 * It is expected that the key type KT matches the key type for a PersistentCascadeStore
 * that stores the actual data, and the value type VT is some kind of byte array that
 * can hold a hash (e.g. std::array<uint8_t, 32>.
 *
 * This is mostly a copy of PersistentCascadeStore that just inherits SignedPersistentFields
 * to enable signatures on the persistent key-value map. It would be nice if I could reuse the
 * code for all the put/get methods without copying and pasting it.
 */
template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST = persistent::ST_FILE>
class SignatureCascadeStore : public ICascadeStore<KT, VT, IK, IV>,
                              public mutils::ByteRepresentable,
                              public derecho::SignedPersistentFields,
                              public derecho::NotificationSupport,
                              public derecho::GroupReference {
private:
    /** Derecho group reference */
    using derecho::GroupReference::group;
    /**
     * Persistent core that stores hashes, which will be signed because Persistent<T> is
     * constructed with signatures=true.
     */
    persistent::Persistent<DeltaCascadeStoreCore<KT, VT, IK, IV>, ST> persistent_core;
    /**
     * Map from data versions (version associated with an object stored in PersistentCascadeStore)
     * to hash versions (version of the hash of that object stored in this SignatureCascadeStore).
     * A new key-value entry is added each time a hash object is added with put(), but entries do not
     * change once added.
     */
    persistent::Persistent<std::map<persistent::version_t, const persistent::version_t>> data_to_hash_version;
    /**
     * A mutex to control access to data_to_hash_version, since it may be simultaneously accessed by
     * both a P2P get (to read) and an ordered put (to write). This wouldn't be necessary if we had a
     * thread-safe list, since version mappings are append-only.
     */
    mutable std::mutex version_map_mutex;
    /** Watcher */
    CriticalDataPathObserver<SignatureCascadeStore<KT, VT, IK, IV>>* cascade_watcher_ptr;
    /** Cascade context (off-critical-path manager) */
    ICascadeContext* cascade_context_ptr;
    bool internal_ordered_put(const VT& value);

public:
    /* Specific to SignatureStore, not part of the Cascade interface */

    /**
     * Retrieves the signature and the previous signed version that is logged
     * with the object identified by key at version ver, where ver is the version
     * of its corresponding *data object* (i.e. object with the same key suffix but
     * an object-pool prefix located on PersistentCascadeStore). Note that the
     * version of the corresponding hash object stored here will be different.
     * Returns an empty signature and an invalid version if there is no version ver,
     * or if an exact match is requested and version ver does not correspond to an
     * update to the requested key.
     *
     * @param key The key identifying the (hash) object to retrieve a signature for
     * @param ver The version of the data object that is associated with the desired hash object
     * @param exact True if the version requested must be an exact match, false if the
     * method should return the signature on the nearest version (before ver) that
     * contains an update to the specified key.
     * @return A pair of values: the signature, and the previous persistent version
     * included in this signature.
     */
    std::tuple<std::vector<uint8_t>, persistent::version_t> get_signature(const KT& key,
                                                                          const persistent::version_t& ver,
                                                                          bool exact = false) const;

    /**
     * Retrieves the signature and the previous signed version that is in the log at
     * version ver, where ver is the version of the *hash object* stored in this
     * subgroup. This is used to get a "previous signed version" (to validate a signature),
     * since the previous signed version will always be the previous version in the
     * SignatureCascadeStore log, not the previous version of the corresponding data object.
     *
     * @param ver The version (of some hash object in the SignatureCascadeStore) to get a signature for
     * @return A pair of values: the signature, and the previous persistent version
     * included in this signature.
     */
    std::tuple<std::vector<uint8_t>, persistent::version_t> get_signature_by_version(const persistent::version_t& ver) const;

    /**
     * Ordered (subgroup-internal) version of get_signature, which is called by get_signature
     * if the caller requested the "current version" of the object rather than a specific
     * past version. This will return the signature on the latest signed version, not
     * necessarily the latest in-memory version, so clients should wait until the latest
     * version has finished persisting if they want to get the "current" signature.
     *
     * @param key The key identifying the object to retrieve a signature for
     * @return The signature, and the previous persistent version included in the signature
     */
    std::tuple<std::vector<uint8_t>, persistent::version_t> ordered_get_signature(const KT&);

    /* CascadeStore interface */

#ifdef ENABLE_EVALUATION
    virtual void dump_timestamp_log(const std::string& filename) const override;
#ifdef DUMP_TIMESTAMP_WORKAROUND
    virtual void dump_timestamp_log_workaround(const std::string& filename) const override;
#endif  // DUMP_TIMESTAMP_WORKAROUND
#endif  // ENABLE_EVALUATION
    virtual void trigger_put(const VT& value) const override;
    virtual std::tuple<persistent::version_t, uint64_t> put(const VT& value) const override;
    virtual void put_and_forget(const VT& value) const override;
#ifdef ENABLE_EVALUATION
    virtual double perf_put(const uint32_t max_payload_size, const uint64_t duration_sec) const override;
#endif  // ENABLE_EVALUATION
    virtual std::tuple<persistent::version_t, uint64_t> remove(const KT& key) const override;
    /**
     * Gets the object that stores a hash of the data object matching key "key" (i.e.
     * its key has the same suffix but a different object-pool prefix), at a specific
     * version "ver" of the *data object*. The version of the hash object will not
     * necessarily be equal to this version.
     *
     * @param key The key of the hash object to retrieve from this SignatureCascadeStore
     * @param ver The version of the data object that is associated with the desired hash object
     * @param exact True if the data-object version must match exactly, false if the hash
     * of the closest-known version of the data object can be returned instead
     */
    virtual const VT get(const KT& key, const persistent::version_t& ver, const bool stable, bool exact = false) const override;
    virtual const VT multi_get(const KT& key) const override;
    virtual const VT get_by_time(const KT& key, const uint64_t& ts_us, const bool stable) const override;
    virtual std::vector<KT> list_keys(const std::string& prefix, const persistent::version_t& ver, const bool stable) const override;
    virtual std::vector<KT> multi_list_keys(const std::string& prefix) const override;
    // virtual std::vector<KT> op_list_keys(const persistent::version_t& ver, const std::string& op_path) const override;
    virtual std::vector<KT> list_keys_by_time(const std::string& prefix, const uint64_t& ts_us, const bool stable) const override;
    // virtual std::vector<KT> op_list_keys_by_time(const uint64_t& ts_us, const std::string& op_path) const override;
    virtual uint64_t get_size(const KT& key, const persistent::version_t& ver, const bool stable, bool exact = false) const override;
    virtual uint64_t multi_get_size(const KT& key) const override;
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

    /* Notification support */
    void notify(const derecho::NotificationMessage& msg) const { derecho::NotificationSupport::notify(msg); }

    /**
     * Asks this node to send a notification to an external client whenever any
     * object has finished being signed.
     * @param external_client_id The ID of the external client to notify
     */
    void subscribe_to_all_notifications(node_id_t external_client_id) const;
    /**
     * Asks this node to send a notification to an external client whenever a new
     * signature is generated for the key identified by key.
     * @param external_client_id The ID of the external client to notify
     * @param key The key (of a hash object stored here) the client is interested in
     */
    void subscribe_to_notifications(node_id_t external_client_id, const KT& key) const;
    /**
     * Asks this node to send a notification to an external client when a specific version
     * of a data object has finished being signed. This will only generate one notification,
     * rather than subscribing the client to updates.
     * @param external_client_id The ID of the external client to notify
     * @param ver The version of a data object that the client wants to be notified about.
     * Since this version uniquely identifies a single data object, it's not necessary to
     * also provide a key.
     */
    void request_notification(node_id_t external_client_id, persistent::version_t ver) const;

    static const uint64_t SIGNATURE_FINISHED_MESSAGE = 1000;

    REGISTER_RPC_FUNCTIONS(SignatureCascadeStore,
                           P2P_TARGETS(
                                   put,
                                   put_and_forget,
#ifdef ENABLE_EVALUATION
                                   perf_put,
#endif  // ENABLE_EVALUATION
                                   get_signature,
                                   get_signature_by_version,
                                   remove,
                                   get,
                                   multi_get,
                                   get_by_time,
                                   list_keys,
                                   multi_list_keys,
                                   list_keys_by_time,
                                   get_size,
                                   multi_get_size,
                                   get_size_by_time,
                                   trigger_put,
                                   notify,
                                   subscribe_to_notifications,
                                   subscribe_to_all_notifications,
                                   request_notification
#ifdef ENABLE_EVALUATION
                                   ,
                                   dump_timestamp_log
#endif  // ENABLE_EVALUATION
                                   ),
                           ORDERED_TARGETS(
                                   ordered_put,
                                   ordered_put_and_forget,
                                   ordered_remove,
                                   ordered_get,
                                   ordered_get_signature,
                                   ordered_list_keys,
                                   ordered_get_size
#ifdef ENABLE_EVALUATION
                                   ,
                                   ordered_dump_timestamp_log
#endif  // ENABLE_EVALUATION
                                   ));

    /* Serialization support, with a custom deserializer to get the context pointers from the registry */
    DEFAULT_SERIALIZE(persistent_core, data_to_hash_version);

    DEFAULT_DESERIALIZE_NOALLOC(SignatureCascadeStore);

    static std::unique_ptr<SignatureCascadeStore> from_bytes(mutils::DeserializationManager* dsm, uint8_t const* buf);

    /* Constructors */
    // Initial constructor, creates Persistent objects
    SignatureCascadeStore(persistent::PersistentRegistry* persistent_registry,
                          CriticalDataPathObserver<SignatureCascadeStore<KT, VT, IK, IV>>* watcher = nullptr,
                          ICascadeContext* context = nullptr);
    // Deserialization constructor, moves Persistent objects
    SignatureCascadeStore(persistent::Persistent<DeltaCascadeStoreCore<KT, VT, IK, IV>, ST>&& deserialized_persistent_core,
                          persistent::Persistent<std::map<persistent::version_t, const persistent::version_t>>&& deserialized_data_to_hash_version,
                          CriticalDataPathObserver<SignatureCascadeStore<KT, VT, IK, IV>>* watcher = nullptr,
                          ICascadeContext* context = nullptr);
    // Dummy constructor needed by client_stub_factory
    SignatureCascadeStore();

    virtual ~SignatureCascadeStore();
};

/**
 * Provides a member constant value equal to true if the template parameter
 * matches SignatureCascadeStore, equal to false otherwise.
 */
template <typename>
struct is_signature_store : std::false_type {};

template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST>
struct is_signature_store<SignatureCascadeStore<KT, VT, IK, IV, ST>> : std::true_type {};

}  // namespace cascade
}  // namespace derecho

#include "detail/signature_store_impl.hpp"