#pragma once

#include "cascade_interface.hpp"
#include "detail/delta_store_core.hpp"

#include <derecho/core/derecho.hpp>
#include <wan_agent.hpp>

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
                              public derecho::GetsViewChangeCallback,
                              public derecho::GroupReference {
private:
    /** Derecho group reference */
    using derecho::GroupReference::group;
    /**
     * Subgroup ID of this node's subgroup, set in the constructor called by the Derecho factory.
     */
    derecho::subgroup_id_t subgroup_id;
    /**
     * True if this node is on the CascadeChain primary site, false if it is on a backup site.
     * Initialized from the Derecho config file in the constructor.
     */
    bool is_primary_site;
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
    /**
     * Map from keys to the IDs of external clients who have subscribed to signature notifications
     * for those keys. This is not part of the replicated state of SignatureCascadeStore; an external
     * client subscribes to notifications from only one replica, and will need to re-subscribe if that
     * replica goes down.
     *
     * The entry for the invalid key *IK contains the list of clients who have subscribed to
     * notifications for all keys.
     */
    mutable std::map<KT, std::list<node_id_t>> subscribed_clients;
    /**
     * The WanAgent instance running on this replica of the subgroup, used to send signed updates to
     * the backup site once they have persisted locally. This is not part of the replicated state of
     * SignatureCascadeStore, since each replica needs its own WanAgent instance. Note that it is
     * initially null and does not get constructed until the first new-view callback, which should
     * happen before the SignatureCascadeStore starts receiving messages.
     */
    mutable std::unique_ptr<wan_agent::WanAgent> wanagent;
    /**
     * A copy of the acknowledgement table reported by WanAgent, which maps each backup site ID
     * to the most recent message number acknowledged by that site. This is needed to supply to WanAgent's
     * initialize_new_leader() function if this replica becomes the subgroup leader.
     */
    std::map<wan_agent::site_id_t, uint64_t> backup_ack_table;
    /**
     * Maps each WanAgent message ID to the identifying tuple (key, hash version,
     * data version) for the object sent in that WanAgent message. Used to
     * determine which object corresponds to a WanAgent message, both for
     * notifying the client that the backup is finished when a WanAgent message
     * is acknowledged, and for determining which objects need to be re-sent to
     * WanAgent after a View change.
     */
    std::map<uint64_t, std::tuple<KT, persistent::version_t, persistent::version_t>> wanagent_message_ids;

    /** Watcher */
    CriticalDataPathObserver<SignatureCascadeStore<KT, VT, IK, IV>>* cascade_watcher_ptr;
    /** Cascade context (off-critical-path manager) */
    ICascadeContext* cascade_context_ptr;

    version_tuple internal_ordered_put(const VT& value);
    /**
     * Sends an external client a notification indicating that the specified
     * hash object version has reached global persistence and been signed. This
     * function is used as the callback action for PersistenceObserver.
     *
     * @param client_id The external client to notify
     * @param key The key identifying the hash object
     * @param hash_object_version The version of a hash object stored in this
     * SignatureCascadeStore that should have reached global persistence when
     * this function is called
     * @param data_object_version The version of the corresponding data object
     * in PersistentCascadeStore that generated the hash object
     */
    void send_client_notification(node_id_t client_id, const KT& key, persistent::version_t hash_object_version,
                                  persistent::version_t data_object_version) const;

    /**
     * Sends a message to the backup sites via WanAgent that includes a hash
     * object plus its signature and corresponding data object version, if this
     * node is the shard leader (i.e. WanAgent leader). Does nothing if this
     * node is not the shard leader.
     *
     * @param hash_object_version The version identifying the hash object that has
     * finished being signed. This is used to retrieve it from the DeltaStoreCore.
     * @param data_object_version The data-object version corresponding to this hash
     * object. This will be needed by the remote WanAgent to match up this signature
     * with the data object it receives from the PersistentCascadeStore.
     */
    void send_to_wan_agent(persistent::version_t hash_object_version, persistent::version_t data_object_version);

    /**
     * Creates an object to send to the backup sites via WanAgent that contains
     * a hash object, its signature, and the corresponding data object version.
     * Specifically, the object's headers are the same as the hash object with
     * the requested version, and its body consists of the hash concatenated
     * with the signature and data object version.
     *
     * @param hash_object_version The version identifying the hash object that
     * will be backed up. This is used to retrieve it from the DeltaStoreCore.
     * @param data_object_version The data-object version corresponding to this
     * hash object.
     * @return An object that is mostly a copy of the hash object, but with the
     * data object version and signature appended to its body.
     */
    VT make_backup_object(persistent::version_t hash_object_version, persistent::version_t data_object_version);

    /**
     * Callback function for WanAgent to use as its PredicateLambda, which is called
     * when there is a new ACK from a remote site.
     */
    void wan_stability_callback(const std::map<wan_agent::site_id_t, uint64_t>& ack_table);

    /**
     * Callback function for WanAgent to use as its RemoteMessageCallback, which is
     * called when it receives a new message from a remote site. This only has an
     * effect when running on a backup site, since the primary site should never
     * receive WanAgent messages.
     *
     * @param sender The WanAgent site ID of the message's sender
     * @param msg_buf A pointer to the byte buffer containing the message
     * @param msg_size The size of the message in bytes
     */
    void wan_message_callback(wan_agent::site_id_t sender, const uint8_t* msg_buf, size_t msg_size);

    /**
     * An ordered-callable function that sets the value of the backup_ack_table variable
     * on all replicas. Called from the WanAgent's stability callback (which only runs on
     * the leader) to replicate the cached ack table to the rest of the subgroup
     *
     * @param ack_table The new value of backup_ack_table
     */
    void update_ack_table(const std::map<wan_agent::site_id_t, uint64_t>& ack_table);

    /**
     * An ordered-callable function that adds a new entry to the wanagent_message_ids
     * table, mapping a WanAgent message ID to the key and version of the object that
     * was sent via WanAgent in that message. Called by the subgroup leader to update
     * the other replicas when it submits a message to WanAgent.
     *
     * @param message_id The WanAgent message ID for a hash object that has been sent
     * to the backup sites
     * @param object_key The key identifying the hash object
     * @param object_version The version of the hash object that was sent
     * @param data_object_version The data-object version corresponding to the hash object
     */
    void record_wan_message_id(uint64_t message_id, const KT& object_key,
                               persistent::version_t object_version, persistent::version_t data_object_version);

public:

    /* Specific to SignatureStore, not part of the Cascade interface */

    /**
     * Handler function for new view callbacks delivered by the Derecho Group;
     * reconfigures the WanAgent to have a new site leader if the SignatureCascadeStore
     * shard leader changed in the new view. In the very first new-view callback,
     * this also creates the WanAgent (the WanAgent can't be created until we know
     * the initial view).
     *
     * @param new_view The View installed by Derecho
     */
    void new_view_callback(const View& new_view);

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
     * @param stable True if the server should wait for the hash object to be globally
     * stable (persisted) before returning its signature, false if the function can return right away
     * @param exact True if the version requested must be an exact match, false if the
     * method should return the signature on the nearest version (before ver) that
     * contains an update to the specified key.
     * @return A pair of values: the signature, and the previous persistent version
     * included in this signature.
     */
    std::tuple<std::vector<uint8_t>, persistent::version_t> get_signature(const KT& key,
                                                                          const persistent::version_t& ver,
                                                                          bool stable,
                                                                          bool exact = false) const;

    /**
     * Retrieves the signature and the previous signed version that is in the log at
     * version ver, where ver is the version of the *hash object* stored in this
     * subgroup. No key argument is required since the version uniquely identifies a
     * single log entry. This is used to get a "previous signed version" (to validate a
     * signature), since the "previous signed version" will always be the previous
     * version in the SignatureCascadeStore log, not the previous version of the
     * corresponding data object.
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
    virtual version_tuple put(const VT& value) const override;
    virtual void put_and_forget(const VT& value) const override;
#ifdef ENABLE_EVALUATION
    virtual double perf_put(const uint32_t max_payload_size, const uint64_t duration_sec) const override;
#endif  // ENABLE_EVALUATION
    virtual version_tuple remove(const KT& key) const override;
    /**
     * Gets the object that stores a hash of the data object matching key "key" (i.e.
     * its key has the same suffix but a different object-pool prefix), at a specific
     * version "ver" of the *data object*. The version of the hash object will not
     * necessarily be equal to this version.
     *
     * @param key The key of the hash object to retrieve from this SignatureCascadeStore
     * @param ver The version of the data object that is associated with the desired hash object
     * @param stable True if the server should wait for the hash object to be globally
     * stable (persisted) before returning it, false if the function can return right away
     * @param exact True if the data-object version must match exactly, false if the hash
     * of the closest-known version of the data object can be returned instead
     */
    virtual const VT get(const KT& key, const persistent::version_t& ver, const bool stable, bool exact = false) const override;
    /**
     * Gets the current version of the hash object with the specified key. This function
     * should not be used because there is no guarantee that the current version of the hash
     * object corresponds to the current version of the data object in the PersistentCascadeStore.
     * Clients should use get() to request the hash object with the correct version corresponding
     * to the data object.
     */
    virtual const VT multi_get(const KT& key) const override;
    virtual const VT get_by_time(const KT& key, const uint64_t& ts_us, const bool stable) const override;
    virtual std::vector<KT> list_keys(const std::string& prefix, const persistent::version_t& ver, const bool stable) const override;
    virtual std::vector<KT> multi_list_keys(const std::string& prefix) const override;
    virtual std::vector<KT> list_keys_by_time(const std::string& prefix, const uint64_t& ts_us, const bool stable) const override;
    /**
     * Gets the size of a hash object at a specific version of that hash object. Unlike
     * get(), this function does not assume the version is for a corresponding data object
     * and translate it to the hash object version. Clients will rarely need to call this
     * function anyway, since all objects in the SignatureCascadeStore are the same size
     * regardless of their corresponding data object (they store SHA256 hashes).
     */
    virtual uint64_t get_size(const KT& key, const persistent::version_t& ver, const bool stable, bool exact = false) const override;
    virtual uint64_t multi_get_size(const KT& key) const override;
    virtual uint64_t get_size_by_time(const KT& key, const uint64_t& ts_us, const bool stable) const override;
    virtual version_tuple ordered_put(const VT& value) override;
    virtual void ordered_put_and_forget(const VT& value) override;
    virtual version_tuple ordered_remove(const KT& key) override;
    virtual const VT ordered_get(const KT& key) override;
    virtual std::vector<KT> ordered_list_keys(const std::string& prefix) override;
    virtual uint64_t ordered_get_size(const KT& key) override;
#ifdef ENABLE_EVALUATION
    virtual void ordered_dump_timestamp_log(const std::string& filename) override;
#endif  // ENABLE_EVALUATION

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

    REGISTER_RPC_FUNCTIONS_WITH_NOTIFICATION(SignatureCascadeStore,
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
                                                     ordered_get_size,
                                                     update_ack_table,
                                                     record_wan_message_id
#ifdef ENABLE_EVALUATION
                                                     ,
                                                     ordered_dump_timestamp_log
#endif  // ENABLE_EVALUATION
                                                     ));

    /* Serialization support, with a custom deserializer to get the context pointers from the registry */
    DEFAULT_SERIALIZE(subgroup_id, is_primary_site, persistent_core, data_to_hash_version, backup_ack_table, wanagent_message_ids);

    DEFAULT_DESERIALIZE_NOALLOC(SignatureCascadeStore);

    static std::unique_ptr<SignatureCascadeStore> from_bytes(mutils::DeserializationManager* dsm, uint8_t const* buf);

    /* Constructors */
    // Initial constructor, creates Persistent objects
    SignatureCascadeStore(persistent::PersistentRegistry* persistent_registry,
                          derecho::subgroup_id_t subgroup_id,
                          CriticalDataPathObserver<SignatureCascadeStore<KT, VT, IK, IV>>* watcher = nullptr,
                          ICascadeContext* context = nullptr);
    // Deserialization constructor, moves Persistent objects
    SignatureCascadeStore(derecho::subgroup_id_t deserialized_subgroup_id,
                          bool is_primary_site,
                          persistent::Persistent<DeltaCascadeStoreCore<KT, VT, IK, IV>, ST>&& deserialized_persistent_core,
                          persistent::Persistent<std::map<persistent::version_t, const persistent::version_t>>&& deserialized_data_to_hash_version,
                          std::map<wan_agent::site_id_t, uint64_t>&& deserialized_ack_table,
                          std::map<uint64_t, std::tuple<KT, persistent::version_t, persistent::version_t>>&& deserialized_wanagent_message_ids,
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
