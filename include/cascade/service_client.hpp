#pragma once

#include "object.hpp"
#include "object_pool_metadata.hpp"
#include "service.hpp"
#include <cascade/config.h>

#include <cstdint>
#include <derecho/core/notification.hpp>
#include <derecho/mutils-serialization/SerializationSupport.hpp>
#include <derecho/persistent/PersistentInterface.hpp>
#include <derecho/utils/time.h>
#include <functional>
#include <hs/hs.h>
#include <memory>
#include <mutex>
#include <mutils-containers/KindMap.hpp>
#include <optional>
#include <shared_mutex>
#include <string>
#include <tuple>
#include <unordered_map>
#include <vector>

namespace derecho {
namespace cascade {
/** The notification handler type */
using cascade_notification_handler_t = std::function<void(const Blob&)>;

/** The CascadeNotificationMessage type */
#define CASCADE_NOTIFICATION_MESSAGE_TYPE (0x100000000ull)
struct CascadeNotificationMessage : public mutils::ByteRepresentable {
    /** The object pool pathname, empty string for raw cascade notification message */
    std::string object_pool_pathname;
    /** data */
    Blob blob;

    /** TODO: the default serialization support macro might contain unnecessary copies. Check it!!! */
    DEFAULT_SERIALIZATION_SUPPORT(CascadeNotificationMessage, object_pool_pathname, blob);

    /** constructors */
    CascadeNotificationMessage() : object_pool_pathname(),
                                   blob() {}
    CascadeNotificationMessage(CascadeNotificationMessage&& other) : object_pool_pathname(other.object_pool_pathname),
                                                                     blob(std::move(other.blob)) {}
    CascadeNotificationMessage(const CascadeNotificationMessage& other) : object_pool_pathname(other.object_pool_pathname),
                                                                          blob(other.blob) {}
    CascadeNotificationMessage(const std::string& _object_pool_pathname,
                               const Blob& _blob) : object_pool_pathname(_object_pool_pathname),
                                                    blob(_blob) {}
};

/**
 * This is the structure for the server side notification handlers
 */
template <typename SubgroupType>
struct SubgroupNotificationHandler {
    /**
     * key: object_pool_pathname
     * value: an optional std::function for the handler
     * The handler for "" key is the default handler, which will always be triggered.
     */
    std::unordered_map<std::string, std::optional<cascade_notification_handler_t>> object_pool_notification_handlers;
    mutable std::unique_ptr<std::mutex> object_pool_notification_handlers_mutex;

    SubgroupNotificationHandler() : object_pool_notification_handlers_mutex(std::make_unique<std::mutex>()) {}

    template <typename T>
    void initialize(derecho::ExternalClientCaller<SubgroupType, T>& subgroup_caller);

    void operator()(const derecho::NotificationMessage& msg);
};

template <typename SubgroupType>
using per_type_notification_handler_registry_t = std::unordered_map<uint32_t, SubgroupNotificationHandler<SubgroupType>>;
/**
 * The ServiceClient template class contains all APIs needed to read/write data. The four core APIs are put, remove,
 * get, and get_by_time. We also provide a set of helper APIs for the client to get the group topology. The core APIs
 * target a specific subgroup and shard of the service, or a specific object pool (which maps to a subgroup).
 * The client uses a ShardMemberSelectionPolicy to determine which member of that subgroup/shard to communicate with.
 */
template <typename... CascadeTypes>
class ServiceClient {
    static_assert(have_same_object_type<CascadeTypes...>());

private:
    // default caller as an external client.
    std::unique_ptr<derecho::ExternalGroupClient<CascadeMetadataService<CascadeTypes...>, CascadeTypes...>> external_group_ptr;
    mutable std::mutex external_group_ptr_mutex;
    // caller as a group member.
    derecho::Group<CascadeMetadataService<CascadeTypes...>, CascadeTypes...>* group_ptr;
    mutable std::mutex group_ptr_mutex;
    // cascade server side notification handler registry.
    mutable mutils::KindMap<per_type_notification_handler_registry_t, CascadeTypes...> notification_handler_registry;
    mutable std::mutex notification_handler_registry_mutex;
    /**
     * 'member_selection_policies' is a map from derecho shard to its member selection policy.
     * We use a 3-tuple consisting of subgroup type index, subgroup index, and shard index to identify a shard. And
     * the policy is defined by a 2-tuple with the ShardMemberSelectionPolicy enum and a user specified node id, in
     * case of ShardMemorySelectionPolicy::UserSpecified. The user specified node id is used as member index if the
     * policy is ShardMemberSelectionPolicy::RoundRobin
     *
     * The default member selection policy is defined as DEFAULT_SHARD_MEMBER_SELECTION_POLICY.
     */
    std::unordered_map<
            std::tuple<std::type_index, uint32_t, uint32_t>,
            std::tuple<ShardMemberSelectionPolicy, node_id_t>,
            do_hash<std::tuple<std::type_index, uint32_t, uint32_t>>>
            member_selection_policies;
    mutable std::shared_mutex member_selection_policies_mutex;
    /**
     * 'member_cache' is a map from derecho shard to its member list. This cache is used to accelerate the member
     * choices process. If the client cannot connect to the cached member (after a couple of retries), it will refresh
     * the corresponding cache entry.
     */
    std::unordered_map<
            std::tuple<std::type_index, uint32_t, uint32_t>,
            std::vector<node_id_t>,
            do_hash<std::tuple<std::type_index, uint32_t, uint32_t>>>
            member_cache;
    mutable std::shared_mutex member_cache_mutex;
    // config clock skew delta (in microseconds) for time consistency
    const uint64_t server_clock_skew_delta_us;
    /**
     * 'object_pool_info_cache' is a local cache for object pool metadata. This cache is used to accelerate the
     * object access process. If an object pool does not exists, it will be loaded from metadata service.
     *
     * Each entry of the object_pool_info_cache is an object of type ObjectPoolMetadataCacheEntry. Such an object
     * caches an object pool metadata object (opm) along with the affinity set regex processing data structures.
     */
    class ObjectPoolMetadataCacheEntry {
    public:
        ObjectPoolMetadata<CascadeTypes...> opm;
        /**
         * The constructor
         * @param[in] _opm object pool metadata
         */
        ObjectPoolMetadataCacheEntry(const ObjectPoolMetadata<CascadeTypes...>& _opm);

        /**
         * The destructor
         */
        virtual ~ObjectPoolMetadataCacheEntry();

        /**
         * Convert a key string to corresponding affinity set string.
         * @param[in] key_string
         *
         * @return affinity set string
         */
        inline std::string to_affinity_set(const std::string& key_string);

    private:
        /* the database storing compiled regex */
        hs_database_t* database;
        /* the scratch for the regex */
        thread_local static hs_scratch_t* scratch;
    };

    std::unordered_map<
            std::string,
            ObjectPoolMetadataCacheEntry>
            object_pool_metadata_cache;
    mutable std::shared_mutex object_pool_metadata_cache_mutex;

    /**
     * Pick a member by a given a policy.
     * @param[in] subgroup_index
     * @param[in] shard_index
     * @param[in] key_for_hashing   - only for KeyHashing policy, ignored otherwise.
     * @param[in] retry             - if true, refresh the member_cache.
     */
    template <typename SubgroupType, typename KeyTypeForHashing>
    node_id_t pick_member_by_policy(uint32_t subgroup_index,
                                    uint32_t shard_index,
                                    const KeyTypeForHashing& key_for_hashing,
                                    bool retry = false);

    /**
     * Refresh(or fill) a member cache entry.
     * @param[in] subgroup_index
     * @param[in] shard_index
     */
    template <typename SubgroupType>
    void refresh_member_cache_entry(uint32_t subgroup_index, uint32_t shard_index);

    /**
     * Metadata API Helper: turn a string key to subgroup type index, subgroup index, and shard index.
     */
    template <typename KeyType>
    std::tuple<uint32_t, uint32_t, uint32_t> key_to_shard(
            const KeyType& key, bool check_object_location = true);

    /**
     * The Constructor
     * We prevent calling the constructor explicitly, because the ServiceClient is a singleton.
     * @param[in] _group_ptr The caller can pass a pointer pointing to a derecho group object. If the pointer is
     *                   valid, the implementation will reply on the group object instead of creating an external
     *                   client to communicate with group members.
     */
    ServiceClient(derecho::Group<CascadeMetadataService<CascadeTypes...>, CascadeTypes...>* _group_ptr = nullptr);

public:
    /**
     * ServiceClient can be an external client or a cascade server. is_external_client() test this condition.
     * The external client implementation is based on ExternalGroupClient<> while the cascade node implementation is
     * based on Group<>.
     *
     * @return true for external client; other wise false.
     */
    inline bool is_external_client() const;

    /**
     * Derecho group helpers: They derive the API in derecho::ExternalClient.
     * - get_my_id                  return my local node id.
     * - get_members                returns all members in the top-level Derecho group.
     * - get_subgroup_members       returns a vector of vectors of node ids: [[node ids in shard 0],[node ids in shard 1],...]
     * - get_shard_members          returns the members in a shard specified by subgroup id(or subgroup type/index pair) and
     *                              shard index.
     * - get_number_of_subgroups    returns the number of subgroups of a given type
     * - get_number_of_shards       returns the number of shards of a given subgroup
     * - get_my_shard               returns the shard number that this node is a member of in the specific
     *                              subgroup (by subgroup type and index), or -1 if this node is not a member
     *                              of any shard in the specified subgroup.
     * During view change, the Client might experience failure if the member is gone. In such a case, the client needs
     * refresh its local member cache by calling get_shard_members.
     */
    node_id_t get_my_id() const;

    std::vector<node_id_t> get_members() const;

    template <typename SubgroupType>
    std::vector<std::vector<node_id_t>> get_subgroup_members(uint32_t subgroup_index) const;

protected:
    template <typename FirstType, typename SecondType, typename... RestTypes>
    std::vector<std::vector<node_id_t>> type_recursive_get_subgroup_members(uint32_t type_index, uint32_t subgroup_index) const;
    template <typename LastType>
    std::vector<std::vector<node_id_t>> type_recursive_get_subgroup_members(uint32_t type_index, uint32_t subgroup_index) const;

public:
    std::vector<std::vector<node_id_t>> get_subgroup_members(const std::string& object_pool_pathname);

    template <typename SubgroupType>
    std::vector<node_id_t> get_shard_members(uint32_t subgroup_index, uint32_t shard_index) const;

protected:
    template <typename FirstType, typename SecondType, typename... RestTypes>
    std::vector<node_id_t> type_recursive_get_shard_members(uint32_t type_index,
                                                            uint32_t subgroup_index, uint32_t shard_index) const;
    template <typename LastType>
    std::vector<node_id_t> type_recursive_get_shard_members(uint32_t type_index,
                                                            uint32_t subgroup_index, uint32_t shard_index) const;

public:
    std::vector<node_id_t> get_shard_members(const std::string& object_pool_pathname, uint32_t shard_index);

    template <typename SubgroupType>
    uint32_t get_number_of_subgroups() const;

    template <typename SubgroupType>
    uint32_t get_number_of_shards(uint32_t subgroup_index) const;

    // type recursive helpers for get_number_of_shards
protected:
    template <typename FirstType, typename SecondType, typename... RestTypes>
    uint32_t type_recursive_get_number_of_shards(uint32_t type_index, uint32_t subgroup_index) const;
    template <typename LastType>
    uint32_t type_recursive_get_number_of_shards(uint32_t type_index, uint32_t subgroup_index) const;

public:
    /**
     * This get_number_of_shards() overload the typed version.
     * @param[in] subgroup_type_index   - the type index of the subrgoup type.
     * @param[in] subgroup_index        - the subgroup index in the given type.
     */
    uint32_t get_number_of_shards(uint32_t subgroup_type_index, uint32_t subgroup_index) const;

    /**
     * This get_number_of_shards(), pick subgroup using object pool pathname.
     * @param[in] object_pool_pathname  - the object pool name
     */
    uint32_t get_number_of_shards(const std::string& object_pool_pathname);

    template <typename SubgroupType>
    int32_t get_my_shard(uint32_t subgroup_index) const;

protected:
    template <typename FirstType, typename SecondType, typename... RestTypes>
    int32_t type_recursive_get_my_shard(uint32_t type_index, uint32_t subgroup_index) const;
    template <typename LastType>
    int32_t type_recursive_get_my_shard(uint32_t type_index, uint32_t subgroup_index) const;

public:
    /**
     * @fn int32_t get_my_shard(uint32_t subgroup_type_index, uint32_t subgroup_index) const
     * @brief find the shard I belong to, given the subgroup specified by type and index.
     * @param[in]   subgroup_type_index     - the type index of the subgroup type.
     * @param[in]   subgroup_index          - the subgroup index in the given type.
     * @return  The number of the shard, or -1 if current node is not in the specified subgroup.
     */
    int32_t get_my_shard(uint32_t subgroup_type_index, uint32_t subgroup_index) const;

    /**
     * @fn int32_t get_my_shard(const std::string& object_pool_pathname)
     * @brief find the shard I belong to, given the object pool specified by object pool path name.
     * @param[in]   object_pool_pathname    - the object pool path name.
     * @return  The number of the shard, or -1 if current node is not in the specified subgroup.
     */
    int32_t get_my_shard(const std::string& object_pool_pathname);

    /**
     * Updates the member selection policy for a shard.
     *
     * @tparam SubgroupType The Cascade subgroup type the shard is in
     * @param[in] subgroup_index
     * @param[in] shard_index
     * @param[in] policy - the new policy
     * @param[in] user_specified_node_id - optional, the node ID to contact if the policy is UserSpecified
     */
    template <typename SubgroupType>
    void set_member_selection_policy(uint32_t subgroup_index, uint32_t shard_index,
                                     ShardMemberSelectionPolicy policy, node_id_t user_specified_node_id = INVALID_NODE_ID);

    /**
     * Reads the member selection policy for a shard.
     *
     * @tparam SubgroupType The Cascade subgroup type the shard is in
     * @param[in] subgroup_index
     * @param[in] shard_index
     * @return a 2-tuple of policy and user_specified_node_id.
     */
    template <typename SubgroupType>
    std::tuple<ShardMemberSelectionPolicy, node_id_t> get_member_selection_policy(
            uint32_t subgroup_index, uint32_t shard_index) const;

    /**
     * "put" writes an object to a given subgroup/shard.
     *
     * @param[in] object            the object to write.
     *                          User provided SubgroupType::ObjectType must have the following two members:
     *                          - SubgroupType::ObjectType::key of SubgroupType::KeyType, which must be set to a
     *                            valid key.
     *                          - SubgroupType::ObjectType::ver of std::tuple<persistent::version_t, uint64_t>.
     *                            Similar to the return object, this member is a two tuple with the first member
     *                            for a version and the second for a timestamp. A caller of put can specify either
     *                            of the version and timestamp meaning what is the latest version/timestamp the caller
     *                            has seen. Cascade will reject the write if the corresponding key has been updated
     *                            already. TODO: should we make it an optional feature?
     * @param[in] subgroup_index    the subgroup index of CascadeType
     * @param[in] shard_index       the shard index.
     * @param[in] as_trigger        If true, the object will NOT apply to the K/V store. The object will only be
     *                              used to update the state.
     *
     * @return a future to the version and timestamp of the put operation.
     * TODO: check if the user application is responsible for reclaim the future by reading it sometime.
     */
    template <typename SubgroupType>
    derecho::rpc::QueryResults<version_tuple> put(const typename SubgroupType::ObjectType& object,
                                                  uint32_t subgroup_index, uint32_t shard_index, bool as_trigger = false);
    /**
     * "type_recursive_put" is a helper function for internal use only.
     * @param[in]   type_index  the index of the subgroup type in the CascadeTypes... list. And the FirstType,
     *                          SecondType, ..., RestTypes should be in the same order.
     * @param[in]   object      the object to write
     * @param[in]   subgroup_index
     *                          the subgroup index in the subgroup type designated by type_index
     * @param[in]   shard_index the shard index
     * @param[in]   as_trigger  If true, the object will NOT apply to the K/V store. The object will only be
     *                          used to update the state.
     *
     * @return a future to the version and timestamp of the put operation.
     */
protected:
    template <typename ObjectType, typename FirstType, typename SecondType, typename... RestTypes>
    derecho::rpc::QueryResults<version_tuple> type_recursive_put(
            uint32_t type_index,
            const ObjectType& object,
            uint32_t subgroup_index,
            uint32_t shard_index,
            bool as_trigger = false);

    template <typename ObjectType, typename LastType>
    derecho::rpc::QueryResults<version_tuple> type_recursive_put(
            uint32_t type_index,
            const ObjectType& object,
            uint32_t subgroup_index,
            uint32_t shard_index,
            bool as_trigger = false);

public:
    /**
     * object pool version of "put"
     * @param[in] object            the object to write, the object pool is extracted from the object key.
     * @param[in] as_trigger        If true, the object will NOT apply to the K/V store. The object will only be
     *                              used to update the state.
     *
     * @return a future to the version and timestamp of the put operation.
     */
    template <typename ObjectType>
    derecho::rpc::QueryResults<version_tuple> put(const ObjectType& object, bool as_trigger = false);

    /**
     * "put_and_forget" writes an object to a given subgroup/shard, but no return value.
     *
     * @param[in] object            the object to write.
     *                          User provided SubgroupType::ObjectType must have the following two members:
     *                          - SubgroupType::ObjectType::key of SubgroupType::KeyType, which must be set to a
     *                            valid key.
     *                          - SubgroupType::ObjectType::ver of std::tuple<persistent::version_t, uint64_t>.
     *                            Similar to the return object, this member is a two tuple with the first member
     *                            for a version and the second for a timestamp. A caller of put can specify either
     *                            of the version and timestamp meaning what is the latest version/timestamp the caller
     *                            has seen. Cascade will reject the write if the corresponding key has been updated
     *                            already. TODO: should we make it an optional feature?
     * @param[in] subgroup_index   the subgroup index of CascadeType
     * @param[in] shard_index       the shard index.
     * @param[in] as_trigger        If true, the object will NOT apply to the K/V store. The object will only be
     *                              used to update the state.
     */
    template <typename SubgroupType>
    void put_and_forget(const typename SubgroupType::ObjectType& object,
                        uint32_t subgroup_index, uint32_t shard_index, bool as_trigger = false);

protected:
    /**
     * "type_recursive_put_and_forget" is a helper function for internal use only.
     * @param[in] type_index    the index of the subgroup type in the CascadeTypes... list. and the FirstType,
     *                          SecondType, .../ RestTypes should be in the same order.
     * @param[in] object        the object to write
     * @param[in] subgroup_index
     *                          the subgroup index in the subgroup type designated by type_index
     * @param[in] shard_index   the shard index
     * @param[in] as_trigger    If true, the object will NOT apply to the K/V store. The object will only be
     *                          used to update the state.
     */
    template <typename ObjectType, typename FirstType, typename SecondType, typename... RestTypes>
    void type_recursive_put_and_forget(
            uint32_t type_index,
            const ObjectType& object,
            uint32_t subgroup_index,
            uint32_t shard_index,
            bool as_trigger = false);

    template <typename ObjectType, typename LastType>
    void type_recursive_put_and_forget(
            uint32_t type_index,
            const ObjectType& object,
            uint32_t subgroup_index,
            uint32_t shard_index,
            bool as_trigger = false);

public:
    /**
     * object pool version of "put_and_forget"
     * @param[in] object        the object to write, the object pool is extracted from the object key.
     * @param[in] as_trigger    If true, the object will NOT apply to the K/V store. The object will only be
     *                          used to update the state.
     */
    template <typename ObjectType>
    void put_and_forget(const ObjectType& object, bool as_trigger = false);

    /**
     * "trigger_put" writes an object to a given subgroup/shard.
     *
     * @param[in] object            the object to write.
     * @param[in] subgroup_index   the subgroup index of CascadeType
     * @param[in] shard_index       the shard index.
     *
     * @return a void future.
     */
    template <typename SubgroupType>
    derecho::rpc::QueryResults<void> trigger_put(const typename SubgroupType::ObjectType& object,
                                                 uint32_t subgroup_index, uint32_t shard_index);

protected:
    /**
     * "type_recursive_trigger_put" is a helper function for internal use only.
     * @param[in]   type_index  the index of the subgroup type in the CascadeTypes... list. and the FirstType,
     *                          SecondType, .../ RestTypes should be in the same order.
     * @param[in]   object      the object to write
     * @param[in]   subgroup_index
     *                          the subgroup index in the subgroup type designated by type_index
     * @param[in] shard_index   the shard index
     *
     * @return future
     */
    template <typename ObjectType, typename FirstType, typename SecondType, typename... RestTypes>
    derecho::rpc::QueryResults<void> type_recursive_trigger_put(
            uint32_t type_index,
            const ObjectType& object,
            uint32_t subgroup_index,
            uint32_t shard_index);

    template <typename ObjectType, typename LastType>
    derecho::rpc::QueryResults<void> type_recursive_trigger_put(
            uint32_t type_index,
            const ObjectType& object,
            uint32_t subgroup_index,
            uint32_t shard_index);

public:
    /**
     * object pool version of "trigger_put"
     * @param[in] object    the object to write, the object pool is extracted from the object key.
     */
    template <typename ObjectType>
    derecho::rpc::QueryResults<void> trigger_put(const ObjectType& object);
    /**
     * "collective_trigger_put" writes an object to a set of nodes.
     *
     * Please notice that returning from QueryResults<void>::get() only means that the message has been sent by the
     * sender. It does NOT guarantee that the message is/will be successfully processed by the remote side. However,
     * we agree that QueryResults<void> should reflect exceptions or errors either on local or remote side, which is
     * not enabled so far. TODO: Track exception in derecho::rpc::QueryResults<void>
     *
     * @param[in] object            the object to write.
     * @param[in] subgroup_index    the subgroup index of CascadeType
     * @param[in] nodes_and_futures map from node ids to futures.
     */
    template <typename SubgroupType>
    void collective_trigger_put(const typename SubgroupType::ObjectType& object,
                                uint32_t subgroup_index,
                                std::unordered_map<node_id_t, std::unique_ptr<derecho::rpc::QueryResults<void>>>& nodes_and_futures);
    /**
     * "put_by_time" writes an object to a given subgroup/shard with a custom timestamp.
     *
     * @param[in] object            the object to write.
     * @param[in] timestamp_us      the timestamp in microseconds to use for the message header.
     *                              The request will be rejected if timestamp_us < get_walltime() - Delta.
     * @param[in] as_trigger        If true, the object will NOT apply to the K/V store. The object will only be
     *                              used to update the state.
     */
    template <typename ObjectType>
    derecho::rpc::QueryResults<version_tuple> put_by_time(const ObjectType& object, const uint64_t& timestamp_us, bool as_trigger = false);

    /**
     * "put_by_time" writes an object to a given subgroup/shard with a custom timestamp.
     *
     * @param[in] object            the object to write.
     * @param[in] timestamp_us      the timestamp in microseconds to use for the message header.
     *                              The request will be rejected if timestamp_us < get_walltime() - Delta.
     * @param[in] subgroup_index    the subgroup index of CascadeType
     * @param[in] shard_index       the shard index.
     * @param[in] as_trigger        If true, the object will NOT apply to the K/V store. The object will only be
     *                              used to update the state.
     */
    template <typename SubgroupType>
    derecho::rpc::QueryResults<version_tuple> put_by_time(const typename SubgroupType::ObjectType& object,
          const uint64_t& timestamp_us, uint32_t subgroup_index, uint32_t shard_index, bool as_trigger = false);

protected:
    /**
     * "type_recursive_put" is a helper function for internal use only.
     * @param[in]   type_index  the index of the subgroup type in the CascadeTypes... list. And the FirstType,
     *                          SecondType, ..., RestTypes should be in the same order.
     * @param[in]   object      the object to write
     * @param[in]   timestamp_us the timestamp in microseconds to use for the message header.
     * @param[in]   subgroup_index
     *                          the subgroup index in the subgroup type designated by type_index
     * @param[in]   shard_index the shard index
     * @param[in]   as_trigger  If true, the object will NOT apply to the K/V store. The object will only be
     *                          used to update the state.
     *
     * @return a future to the version and timestamp of the put operation.
     */
    template <typename ObjectType, typename FirstType, typename SecondType, typename... RestTypes>
    derecho::rpc::QueryResults<version_tuple> type_recursive_put_by_time(
            uint32_t type_index,
            const ObjectType& object,
            const uint64_t& timestamp_us,
            uint32_t subgroup_index,
            uint32_t shard_index,
            bool as_trigger = false);

    template <typename ObjectType, typename LastType>
    derecho::rpc::QueryResults<version_tuple> type_recursive_put_by_time(
            uint32_t type_index,
            const ObjectType& object,
            const uint64_t& timestamp_us,
            uint32_t subgroup_index,
            uint32_t shard_index,
            bool as_trigger = false);
public:
    /**
     * "remove" deletes an object with the given key.
     *
     * @param[in] key               the object key
     * @param[in] subgroup_index   the subgroup index of CascadeType
     * @param[in] shard_index       the shard index.
     *
     * @return a future to the version and timestamp of the put operation.
     * TODO: check if the user application is responsible for reclaim the future by reading it sometime.
     */
    template <typename SubgroupType>
    derecho::rpc::QueryResults<version_tuple> remove(const typename SubgroupType::KeyType& key,
                                                     uint32_t subgroup_index, uint32_t shard_index);

protected:
    /**
     * "type_recursive_remove" is a helper function for internal use only.
     * @param[in]   type_index              the index of the subgroup type in the CascadeTypes... list. and the FirstType,
     *                          SecondType, .../ RestTypes should be in the same order.
     * @param[in]   key                     the key
     * @param[in]   subgroup_index          the subgroup index in the subgroup type designated by type_index
     * @param[in]   shard_index             the shard index
     *
     * @return a future to the version and timestamp of the put operation.
     */
    template <typename KeyType, typename FirstType, typename SecondType, typename... RestTypes>
    derecho::rpc::QueryResults<version_tuple> type_recursive_remove(
            uint32_t type_index,
            const KeyType& key,
            uint32_t subgroup_index,
            uint32_t shard_index);

    template <typename KeyType, typename LastType>
    derecho::rpc::QueryResults<version_tuple> type_recursive_remove(
            uint32_t type_index,
            const KeyType& key,
            uint32_t subgroup_index,
            uint32_t shard_index);

public:
    /**
     * object pool version of "remove"
     * @param[in]   key             the object key
     *
     * @return  returns a future
     */
    template <typename KeyType>
    derecho::rpc::QueryResults<version_tuple> remove(const KeyType& key);

    /**
     * "get" retrieve the object of a given key
     *
     * @param[in] key               the object key
     * @param[in] version           the version of the object to read. If equal to CURRENT_VERSION, get will either
     *                              read the current object from memory of the replica that handles the get request
     *                              (if stable is false), or read the latest stable version that is persisted (if
     *                              stable is true). Note that in any case "get" will contact only a single replica;
     *                              to use an atomic multicast to get the latest version that is present on all replicas,
     *                              use multi_get.
     * @param[in] stable            if true, get will wait until the requested version's persistent data is safe, meaning the
     *                              persistent data is persisted on all replicas.
     * @param[in] subgroup_index    the subgroup index of CascadeType
     * @param[in] shard_index       the shard index.
     *
     * @return a future to the retrieved object.
     * TODO: check if the user application is responsible for reclaim the future by reading it sometime.
     */
    template <typename SubgroupType>
    derecho::rpc::QueryResults<const typename SubgroupType::ObjectType> get(
            const typename SubgroupType::KeyType& key,
            const persistent::version_t& version = CURRENT_VERSION,
            bool stable = true,
            uint32_t subgroup_index = 0,
            uint32_t shard_index = 0);

protected:
    /**
     * "type_recursive_get" is a helper function for internal use only.
     * @param[in] type_index        the index of the subgroup type in the CascadeTypes... list. and the FirstType,
     *                          SecondType, .../ RestTypes should be in the same order.
     * @param[in] key               the key
     * @param[in] version           the version
     * @param[in] stable            stable or not?
     * @param[in] subgroup_index    the subgroup index in the subgroup type designated by type_index
     * @param[in] shard_index       the shard index
     *
     * @return a future for the object.
     */
    template <typename KeyType, typename FirstType, typename SecondType, typename... RestTypes>
    auto type_recursive_get(
            uint32_t type_index,
            const KeyType& key,
            const persistent::version_t& version,
            bool stable,
            uint32_t subgroup_index,
            uint32_t shard_index);

    template <typename KeyType, typename LastType>
    auto type_recursive_get(
            uint32_t type_index,
            const KeyType& key,
            const persistent::version_t& version,
            bool stable,
            uint32_t subgroup_index,
            uint32_t shard_index);

public:
    /**
     * object pool version of "get"
     *
     * @param[in] key               the object key; the object pool is extracted from this key
     * @param[in] version           the version of the object to read. If equal to CURRENT_VERSION, get will either
     *                              read the current object from memory of the replica that handles the get request
     *                              (if stable is false), or read the latest stable version that is persisted (if
     *                              stable is true). Note that in any case "get" will contact only a single replica;
     *                              to use an atomic multicast to get the latest version that is present on all replicas,
     *                              use multi_get.
     * @param[in] stable            if true, get will wait until the requested version's persistent data is safe, meaning the
     *                              persistent data is persisted on all replicas.
     * @return a future to the retrieved object.
     */
    template <typename KeyType>
    auto get(
            const KeyType& key,
            const persistent::version_t& version = CURRENT_VERSION,
            bool stable = true);

    /**
     * "multi_get" retrieves the latest version of the object for a given key using an atomic broadcast.
     * This ensures that the get request is mutually exclusive and linearizable with any concurrent put
     * requests to the same key.
     *
     * @param[in] key               the object key
     * @param[in] subgroup_index    the subgroup index of CascadeType
     * @param[in] shard_index       the shard index.
     *
     * @return a future to the retrieved object, including a response from each replica in the shard.
     */
    template <typename SubgroupType>
    derecho::rpc::QueryResults<const typename SubgroupType::ObjectType> multi_get(const typename SubgroupType::KeyType& key,
                                                                                  uint32_t subgroup_index, uint32_t shard_index);

    /**
     * "type_recursive_multi_get"   is a helper function for internal use only.
     * @param[in]   type_index      the index of the subgroup type in the CascadeTypes... list. and the FirstType,
     *                              SecondType, .../ RestTypes should be in the same order.
     * @param[in]   key             the key
     * @param[in]   subgroup_index  the subgroup index in the subgroup type designated by type_index
     * @param[in]   shard_index     the shard index
     *
     * @return a future for the object.
     */
protected:
    template <typename KeyType, typename FirstType, typename SecondType, typename... RestTypes>
    auto type_recursive_multi_get(
            uint32_t type_index,
            const KeyType& key,
            uint32_t subgroup_index,
            uint32_t shard_index);

    template <typename KeyType, typename LastType>
    auto type_recursive_multi_get(
            uint32_t type_index,
            const KeyType& key,
            uint32_t subgroup_index,
            uint32_t shard_index);

public:
    /**
     * object pool version of "multi_get"
     *
     * @param[in] key               the object key; the object pool is extracted from this key
     *
     * @return a future for the retrieved object, including a response from each replica in the object pool
     */
    template <typename KeyType>
    auto multi_get(const KeyType& key);

    /**
     * "get_by_time" retrieve the object of a given key
     *
     * @param[in] key               the object key
     * @param[in] ts_us             Wall clock time in microseconds.
     * @param[in] stable            stable get or not
     * @param[in] subgroup_index   the subgroup index of CascadeType
     * @param[in] shard_index       the shard index.
     *
     * @return a future to the retrieved object.
     * TODO: check if the user application is responsible for reclaim the future by reading it sometime.
     */
    template <typename SubgroupType>
    derecho::rpc::QueryResults<const typename SubgroupType::ObjectType> get_by_time(
            const typename SubgroupType::KeyType& key,
            const uint64_t& ts_us,
            const bool stable = true,
            uint32_t subgroup_index = 0,
            uint32_t shard_index = 0);

    /**
     * "type_recursive_get_by_time" is a helper function for internal use only.
     * @param[in] type_index        the index of the subgroup type in the CascadeTypes... list. and the FirstType,
     *                          SecondType, .../ RestTypes should be in the same order.
     * @param[in] key               the key
     * @param[in] ts_us             Wall clock time in microseconds.
     * @param[in] stable            stable get or not
     * @param[in] subgroup_index    the subgroup index in the subgroup type designated by type_index
     * @param[in] shard_index       the shard index
     *
     * @return a future for the object.
     */
protected:
    template <typename KeyType, typename FirstType, typename SecondType, typename... RestTypes>
    auto type_recursive_get_by_time(
            uint32_t type_index,
            const KeyType& key,
            const uint64_t& ts_us,
            const bool stable,
            uint32_t subgroup_index,
            uint32_t shard_index);

    template <typename KeyType, typename LastType>
    auto type_recursive_get_by_time(
            uint32_t type_index,
            const KeyType& key,
            const uint64_t& ts_us,
            const bool stable,
            uint32_t subgroup_index,
            uint32_t shard_index);

public:
    /**
     * object pool version of "get_by_time"
     *
     * @param[in] key               the object key; the object pool is extracted from this key
     * @param[in] ts_us             Wall clock time in microseconds.
     * @param[in] stable            stable get or not
     *
     * @return a future for the retrieved object.
     */
    template <typename KeyType>
    auto get_by_time(
            const KeyType& key,
            const uint64_t& ts_us,
            const bool stable = true);

    /**
     * "get_size" retrieve size of the object of a given key
     *
     * @param[in] key               the object key
     * @param[in] version           the version of the object to read. If equal to CURRENT_VERSION, this will either
     *                              get the size of the current object from memory of the replica that handles the request
     *                              (if stable is false), or get the size of the latest stable version that is persisted
     *                              (if stable is true). Note that in any case "get_size" will contact only a single replica;
     *                              to use an atomic multicast to check the latest version that is present on all replicas,
     *                              use multi_get_size.
     * @param[in] stable            stable get or not
     * @param[in] subgroup_index    the subgroup index of CascadeType
     * @param[in] shard_index       the shard index.
     *
     * @return a future to the retrieved size.
     * TODO: check if the user application is responsible for reclaim the future by reading it sometime.
     */
    template <typename SubgroupType>
    derecho::rpc::QueryResults<uint64_t> get_size(
            const typename SubgroupType::KeyType& key,
            const persistent::version_t& version,
            const bool stable = true,
            uint32_t subgroup_index = 0,
            uint32_t shard_index = 0);

protected:
    /**
     * "type_recursive_get_size" is a helper function for internal use only.
     * @param[in] type_index        the index of the subgroup type in the CascadeTypes... list. and the FirstType,
     *                          SecondType, .../ RestTypes should be in the same order.
     * @param[in] key               the key
     * @param[in] version           version
     * @param[in] stable            stable get size or not
     * @param[in] subgroup_index    the subgroup index in the subgroup type designated by type_index
     * @param[in] shard_index       the shard index
     *
     * @return a future for the retrieved size.
     */
    template <typename KeyType, typename FirstType, typename SecondType, typename... RestTypes>
    derecho::rpc::QueryResults<uint64_t> type_recursive_get_size(
            uint32_t type_index,
            const KeyType& key,
            const persistent::version_t& version,
            const bool stable,
            uint32_t subgroup_index,
            uint32_t shard_index);

    template <typename KeyType, typename LastType>
    derecho::rpc::QueryResults<uint64_t> type_recursive_get_size(
            uint32_t type_index,
            const KeyType& key,
            const persistent::version_t& version,
            const bool stable,
            uint32_t subgroup_index,
            uint32_t shard_index);

public:
    /**
     * object pool version of "get_size"
     * @param[in] key               the object key
     * @param[in] version           the version of the object to read. If equal to CURRENT_VERSION, this will either
     *                              get the size of the current object from memory of the replica that handles the request
     *                              (if stable is false), or get the size of the latest stable version that is persisted
     *                              (if stable is true). Note that in any case "get_size" will contact only a single replica;
     *                              to use an atomic multicast to check the latest version that is present on all replicas,
     *                              use multi_get_size.
     * @param[in] stable            stable get or not
     * @return a future for the retrieved size.
     */
    template <typename KeyType>
    derecho::rpc::QueryResults<uint64_t> get_size(
            const KeyType& key,
            const persistent::version_t& version,
            const bool stable = true);

    /**
     * "multi_get_size" retrieves the size of the current version of the object with a given key, using an atomic broadcast.
     *
     * @param[in] key               the object key
     * @param[in] subgroup_index    the subgroup index of CascadeType
     * @param[in] shard_index       the shard index.
     *
     * @return a future to the retrieved size.
     */
    template <typename SubgroupType>
    derecho::rpc::QueryResults<uint64_t> multi_get_size(
            const typename SubgroupType::KeyType& key,
            uint32_t subgroup_index, uint32_t shard_index);

    /**
     * "type_recursive_multi_get_size" is a helper function for internal use only.
     * @param[in] type_index        the index of the subgroup type in the CascadeTypes... list. and the FirstType,
     *                          SecondType, .../ RestTypes should be in the same order.
     * @param[in] key               the key
     * @param[in] subgroup_index    the subgroup index in the subgroup type designated by type_index
     * @param[in] shard_index       the shard index
     *
     * @return a future for the object.
     */
protected:
    template <typename KeyType, typename FirstType, typename SecondType, typename... RestTypes>
    derecho::rpc::QueryResults<uint64_t> type_recursive_multi_get_size(
            uint32_t type_index,
            const KeyType& key,
            uint32_t subgroup_index,
            uint32_t shard_index);

    template <typename KeyType, typename LastType>
    derecho::rpc::QueryResults<uint64_t> type_recursive_multi_get_size(
            uint32_t type_index,
            const KeyType& key,
            uint32_t subgroup_index,
            uint32_t shard_index);

public:
    /**
     * object pool version of "multi_get_size"
     *
     * @param[in] key               the object key; the object pool is extracted from this key
     *
     * @return a future for the retrieved size.
     */
    template <typename KeyType>
    derecho::rpc::QueryResults<uint64_t> multi_get_size(const KeyType& key);

    /**
     * "get_size_by_time" retrieve size of the object of a given key
     *
     * @param[in] key               the object key
     * @param[in] ts_us             Wall clock time in microseconds.
     * @param[in] stable            stable get or not
     * @param[in] subgroup_index   the subgroup index of CascadeType
     * @param[in] shard_index       the shard index.
     *
     * @return a future to the retrieved size.
     * TODO: check if the user application is responsible for reclaim the future by reading it sometime.
     */
    template <typename SubgroupType>
    derecho::rpc::QueryResults<uint64_t> get_size_by_time(
            const typename SubgroupType::KeyType& key,
            const uint64_t& ts_us,
            const bool stable = true,
            uint32_t subgroup_index = 0,
            uint32_t shard_index = 0);

protected:
    /**
     * "type_recursive_get_size" is a helper function for internal use only.
     * @param[in] type_index        the index of the subgroup type in the CascadeTypes... list. and the FirstType,
     *                          SecondType, .../ RestTypes should be in the same order.
     * @param[in] key               the key
     * @param[in] ts_us             Wall clock time in microseconds.
     * @param[in] stable            stable get or not
     * @param[in] subgroup_index    the subgroup index in the subgroup type designated by type_index
     * @param[in] shard_index       the shard index
     *
     * @return a future for the object.
     */
    template <typename KeyType, typename FirstType, typename SecondType, typename... RestTypes>
    derecho::rpc::QueryResults<uint64_t> type_recursive_get_size_by_time(
            uint32_t type_index,
            const KeyType& key,
            const uint64_t& ts_us,
            const bool stable,
            uint32_t subgroup_index,
            uint32_t shard_index);

    template <typename KeyType, typename LastType>
    derecho::rpc::QueryResults<uint64_t> type_recursive_get_size_by_time(
            uint32_t type_index,
            const KeyType& key,
            const uint64_t& ts_us,
            const bool stable,
            uint32_t subgroup_index,
            uint32_t shard_index);

public:
    /**
     * object pool version of "get_size_by_time"
     *
     * @param[in] key               the object key
     * @param[in] ts_us             Wall clock time in microseconds.
     * @param[in] stable            stable get or not
     *
     * @return a future for the retrieved size.
     */
    template <typename KeyType>
    derecho::rpc::QueryResults<uint64_t> get_size_by_time(
            const KeyType& key,
            const uint64_t& ts_us,
            const bool stable = true);

    /**
     * "list_keys" retrieve the list of keys in a shard
     *
     * @param[in] version           the version at which to list the keys; all keys that existed at or before this version
     *                              will be included. If equal to CURRENT_VERSION, list_keys will list all keys currently
     *                              in memory at the replica that handles the request.
     * @param[in] stable            if true, list_keys will wait until the requested version's persistent data is safe, meaning the
     *                              persistent data is persisted on all replicas, before listing the keys present at that version.
     * @param[in] subgroup_index    the subgroup index of CascadeType
     * @param[in] shard_index       the shard index.
     *
     * @return a future for the vector of keys
     * TODO: check if the user application is responsible for reclaim the future by reading it sometime.
     */
    template <typename SubgroupType>
    derecho::rpc::QueryResults<std::vector<typename SubgroupType::KeyType>> list_keys(
            const persistent::version_t& version,
            const bool stable = true,
            uint32_t subgroup_index = 0,
            uint32_t shard_index = 0);

protected:
    template <typename FirstType, typename SecondType, typename... RestTypes>
    auto type_recursive_list_keys(
            uint32_t type_index,
            const persistent::version_t& version,
            const bool stable,
            const std::string& object_pool_pathname);
    template <typename LastType>
    auto type_recursive_list_keys(
            uint32_t type_index,
            const persistent::version_t& version,
            const bool stable,
            const std::string& object_pool_pathname);
    template <typename SubgroupType>
    std::vector<std::unique_ptr<derecho::rpc::QueryResults<std::vector<typename SubgroupType::KeyType>>>>
    __list_keys(const persistent::version_t& version, const bool stable, const std::string& object_pool_pathname);

public:
    /**
     * @brief object pool version of "list_keys"; lists all keys in an object pool
     *
     * @param[in] version               the version at which to list the keys; all keys that existed at or before this version
     *                                  will be included. If equal to CURRENT_VERSION, list_keys will list all keys currently
     *                                  in memory at the replica that handles the request.
     * @param[in] stable                if true, list_keys will wait until the requested version's persistent data is safe, meaning the
     *                                  persistent data is persisted on all replicas, before listing the keys present at that version.
     * @param[in] object_pool_pathname  the object pathname
     *
     * @return a vector of futures for key lists, with one key list for each shard in the object pool.
     * The return value's type will look like vector<unique_ptr<QueryResults<vector<KeyType>>>>, where KeyType is either string or uint64_t
     */
    auto list_keys(const persistent::version_t& version, const bool stable, const std::string& object_pool_pathname);

    /**
     * A function that helps unpack the return value of list_keys (object-pool version). Iterates through the
     * vector of QueryResults and waits for each one to complete, then combines the resulting vectors of keys
     * into a single vector of keys (across all shards in the object pool).
     *
     * @tparam KeyType The type of a key in this object pool
     * @param future The vector-of-futures returned by list_keys or multi_list_keys
     * @return a vector of keys in the object pool
     */
    template <typename KeyType>
    std::vector<KeyType> wait_list_keys(
            std::vector<std::unique_ptr<derecho::rpc::QueryResults<std::vector<KeyType>>>>& future);

    /**
     * "multi_list_keys" retrieves the list of keys in a shard at the current version, using an atomic multicast
     *
     * @param[in] subgroup_index   the subgroup index of CascadeType
     * @param[in] shard_index       the shard index.
     *
     * @return a future for the list of keys.
     */
    template <typename SubgroupType>
    derecho::rpc::QueryResults<std::vector<typename SubgroupType::KeyType>> multi_list_keys(
            uint32_t subgroup_index,
            uint32_t shard_index);

protected:
    template <typename FirstType, typename SecondType, typename... RestTypes>
    auto type_recursive_multi_list_keys(
            uint32_t type_index,
            const std::string& object_pool_pathname);
    template <typename LastType>
    auto type_recursive_multi_list_keys(
            uint32_t type_index,
            const std::string& object_pool_pathname);
    template <typename SubgroupType>
    std::vector<std::unique_ptr<derecho::rpc::QueryResults<std::vector<typename SubgroupType::KeyType>>>>
    __multi_list_keys(const std::string& object_pool_pathname);

public:
    /**
     * object pool version of "multi_list_keys"
     *
     * @param[in] object_pool_pathname  the object pathname
     *
     * @return a vector of futures for key lists, with one key list for each shard in the object pool.
     * The return value's type will look like vector<unique_ptr<QueryResults<vector<KeyType>>>>, where KeyType is either string or uint64_t
     */
    auto multi_list_keys(const std::string& object_pool_pathname);

    /**
     * "list_keys_by_time" retrieves the list of keys in a shard at a specific time
     *
     * @param[in] ts_us             Wall clock time in microseconds.
     * @param[in] stable            stable get or not
     * @param[in] subgroup_index    the subgroup index of CascadeType
     * @param[in] shard_index       the shard index.
     *
     * @return a future for the list of keys
     */
    template <typename SubgroupType>
    derecho::rpc::QueryResults<std::vector<typename SubgroupType::KeyType>> list_keys_by_time(
            const uint64_t& ts_us,
            const bool stable = true,
            uint32_t subgroup_index = 0,
            uint32_t shard_index = 0);

protected:
    template <typename FirstType, typename SecondType, typename... RestTypes>
    auto type_recursive_list_keys_by_time(
            uint32_t type_index,
            const uint64_t& ts_us,
            const bool stable,
            const std::string& object_pool_pathname);
    template <typename LastType>
    auto type_recursive_list_keys_by_time(
            uint32_t type_index,
            const uint64_t& ts_us,
            const bool stable,
            const std::string& object_pool_pathname);
    template <typename SubgroupType>
    std::vector<std::unique_ptr<derecho::rpc::QueryResults<std::vector<typename SubgroupType::KeyType>>>>
    __list_keys_by_time(const uint64_t& ts_us, const bool stable, const std::string& object_pool_pathname);

public:
    /**
     * object pool version of "list_keys_by_time"
     *
     * @param[in] ts_us                  timestamp
     * @param[in] stable                 stable flag
     * @param[in] object_pool_pathname   the object pathname
     *
     * @return a vector of futures for key lists, with one key list for each shard in the object pool.
     * The return value's type will look like vector<unique_ptr<QueryResults<vector<KeyType>>>>, where KeyType is either string or uint64_t
     */
    auto list_keys_by_time(const uint64_t& ts_us, const bool stable, const std::string& object_pool_pathname);

    /**
     * Object Pool Management API: refresh object pool cache
     * We load 'unstable' (committed but maybe not persisted) metadata here.
     */
    void refresh_object_pool_metadata_cache();

    /**
     * Object Pool Management API: create object pool
     *
     * @tparam SubgroupType     Type of the subgroup for the created object pool
     * @param[in]  pathname         Object pool's pathname as identifier.
     * @param[in]  subgroup_index   Index of the subgroup
     * @param[in]  sharding_policy  The default sharding policy for this object pool
     * @param[in]  object_locations The set of special object locations.
     * @param[in]  affinity_set_regex
     *                          The affinity set regex.
     *
     * @return a future to the version and timestamp of the put operation.
     */
    template <typename SubgroupType>
    derecho::rpc::QueryResults<version_tuple> create_object_pool(
            const std::string& pathname, const uint32_t subgroup_index,
            const sharding_policy_t sharding_policy = HASH,
            const std::unordered_map<std::string, uint32_t>& object_locations = {},
            const std::string& affinity_set_regex = "");

    /**
     * ObjectPoolManagement API: remove object pool
     *
     * @param[in]  pathname         Object pool pathname
     *
     * @return a future to the version and timestamp of the put operation.
     */
    derecho::rpc::QueryResults<version_tuple> remove_object_pool(const std::string& pathname);

private:
    /**
     * ObjectPoolManagement API: find object pool
     *
     * @param[in]  pathname         Object pool pathname
     * @param[in]  rlck             shared lock, which needs to be hold.
     *
     * @return the object pool metadata
     */
    ObjectPoolMetadata<CascadeTypes...> internal_find_object_pool(const std::string& pathname,
                                                                  std::shared_lock<std::shared_mutex>& rlck);

public:
    /**
     * ObjectPoolManagement API: find object pool
     *
     * @param[in]  pathname         Object pool pathname
     *
     * @return the object pool metadata
     */
    ObjectPoolMetadata<CascadeTypes...> find_object_pool(const std::string& pathname);

    /**
     * ObjectPoolManagement API: find object pool and affinity_set from key
     *
     * @param[in]  key              The key of an object.
     *
     * @return the object pool metadata along with the affinity set string
     */
    template <typename KeyType>
    std::pair<ObjectPoolMetadata<CascadeTypes...>, std::string>
    find_object_pool_and_affinity_set_by_key(const KeyType& key);

    /**
     * ObjectPoolManagement API: list all the object pools by pathnames
     *
     * @param[in] include_deleted   show deleted pools with an exclaimation point(!).
     * @param[in] refresh           false for cached object ids, true for refreshed ids.
     *
     * @return the pool ids.
     */
    std::vector<std::string> list_object_pools(bool include_deleted, bool refresh = false);

    /**
     * Register an notification handler to a subgroup. If such a handler has been registered, it will be replaced
     * by the new one.
     *
     * @tparam SubgroupType     The Subgroup Type
     * @param[in] handler           The handler to reigster
     * @param[in] subgroup_index    Index of the subgroup
     *
     * @return true if a previous notification handler is replaced.
     */
    template <typename SubgroupType>
    bool register_notification_handler(
            const cascade_notification_handler_t& handler,
            const uint32_t subgroup_index = 0);

protected:
    template <typename SubgroupType>
    bool register_notification_handler(
            const cascade_notification_handler_t& handler,
            const std::string& object_pool_pathname,
            const uint32_t subgroup_index);
    template <typename FirstType, typename SecondType, typename... RestTypes>
    bool type_recursive_register_notification_handler(
            uint32_t type_index,
            const cascade_notification_handler_t& handler,
            const std::string& object_pool_pathname,
            const uint32_t subgroup_index);
    template <typename LastType>
    bool type_recursive_register_notification_handler(
            uint32_t type_index,
            const cascade_notification_handler_t& handler,
            const std::string& object_pool_pathname,
            const uint32_t subgroup_index);

public:
    /**
     * Register notification handler(object pool version). If such a handler has been registered, it will be
     * replaced by the new one.
     *
     * @tparam SubgroupType         The Subgroup Type
     * @param[in] handler               The handler to reigster
     * @param[in] object_pool_pathname  To with object pool is this handler registered.
     *
     * @return true if a previous notification handler is replaced.
     */
    bool register_notification_handler(
            const cascade_notification_handler_t& handler,
            const std::string& object_pool_pathname);

    /**
     * Send a notification message to an external client.
     *
     * @tparam SubgroupType     The Subgroup Type
     * @param[in] msg               The message to send
     * @param[in] subgroup_index    The subgroup index
     * @param[in] client_id         The node id of the external client to be notified
     */
    template <typename SubgroupType>
    void notify(const Blob& msg,
                const uint32_t subgroup_index,
                const node_id_t client_id) const;

protected:
    template <typename SubgroupType>
    void notify(const Blob& msg,
                const std::string& object_pool_pathname,
                const uint32_t subgroup_index,
                const node_id_t client_id) const;
    template <typename FirstType, typename SecondType, typename... RestTypes>
    void type_recursive_notify(
            uint32_t type_index,
            const Blob& msg,
            const std::string& object_pool_pathname,
            const uint32_t subgroup_index,
            const node_id_t client_id) const;
    template <typename LastType>
    void type_recursive_notify(
            uint32_t type_index,
            const Blob& msg,
            const std::string& object_pool_pathname,
            const uint32_t subgroup_index,
            const node_id_t client_id) const;

public:
    /**
     * Send a notification message to an external client.
     *
     * @param[in] msg                   The messgae to send
     * @param[in] object_pool_pathname  In which object_pool the notification is in.
     * @param[in] client_id             The client id
     */
    void notify(const Blob& msg,
                const std::string& object_pool_pathname,
                const node_id_t client_id);

#ifdef ENABLE_EVALUATION
    /**
     * Dump the timestamp log entries into a file on each of the nodes in a shard.
     *
     * @param[in] filename         - the output filename
     * @param[in] subgroup_index   - the subgroup index
     * @param[in] shard_index      - the shard index
     *
     * @return query results
     */
    template <typename SubgroupType>
    derecho::rpc::QueryResults<void> dump_timestamp(const std::string& filename, const uint32_t subgroup_index, const uint32_t shard_index);

    /**
     * The object store version:
     *
     * @param[in] filename             -   the filename
     * @param[in] object_pool_pathname -   the object pool pathname
     */
    void dump_timestamp(const std::string& filename, const std::string& object_pool_pathname);

    /**
     * Dump the timestamp log entries into a file on each of the nodes in a subgroup.
     *
     * @param[in] filename         - the output filename
     * @param[in] subgroup_index   - the subgroup index
     */
    template <typename SubgroupType>
    void dump_timestamp(const uint32_t subgroup_index, const std::string& filename);

protected:
    template <typename FirstType, typename SecondType, typename... RestTypes>
    void type_recursive_dump(uint32_t type_index, uint32_t subgroup_index, const std::string& filename);

    template <typename LastType>
    void type_recursive_dump(uint32_t type_index, uint32_t subgroup_index, const std::string& filename);

public:
#ifdef DUMP_TIMESTAMP_WORKAROUND
    /**
     * Dump the timestamp log entries into a file on a specific node.
     *
     * @param[in] filename         - the output filename
     * @param[in] subgroup_index   - the subgroup index
     * @param[in] shard_index      - the shard index
     * @param[in] node_id          - the given node id.
     *
     * @return a vector of query results.
     */
    template <typename SubgroupType>
    derecho::rpc::QueryResults<void> dump_timestamp_workaround(const std::string& filename, const uint32_t subgroup_index, const uint32_t shard_index, const node_id_t node_id);
#endif

    /**
     * Evaluate the ordered put performance inside a shard. Please note that those put does not involve the
     * external client data path.
     *
     * @param[in] message_size      - the message size for the shard. TODO: we should be able to retrieve the maximum
     *                            message size from SubgroupType, subgroup_index and shard_index. How?
     * @param[in] duration_sec      - the duration of the test in seconds.
     * @param[in] subgroup_index    - the subgroup index
     * @param[in] shard_index       - the shard index
     *
     * @return the value in ops.
     */
    template <typename SubgroupType>
    derecho::rpc::QueryResults<double> perf_put(const uint32_t message_size, const uint64_t duration_sec, const uint32_t subgroup_index, const uint32_t shard_index);
#endif  // ENABLE_EVALUATION

    const static std::vector<std::type_index> subgroup_type_order;
    const static uint32_t invalid_subgroup_type_index;
    /**
     * Get type index
     * @return the the subgroup type index
     */
    template <typename SubgroupType>
    static uint32_t get_subgroup_type_index();

    /* singleton */
private:
    static std::unique_ptr<ServiceClient> service_client_singleton_ptr;
    static std::mutex singleton_mutex;

public:
    /**
     * Initialize the service_client_single_ptr singleton with a cascade service. This can only be called once
     * before any get_service_client() is called.
     * @param[in] _group_ptr The caller can pass a pointer pointing to a derecho group object. If the pointer is
     *                   valid, the implementation will reply on the group object instead of creating an external
     *                   client to communicate with group members.
     */
    static void initialize(derecho::Group<CascadeMetadataService<CascadeTypes...>, CascadeTypes...>* _group_ptr);

    /**
     * Get the singleton ServiceClient API. If it does not exists, initialize it as an external client.
     */
    static ServiceClient& get_service_client();
};  // ServiceClient

}  // namespace cascade
}  // namespace derecho

#include "detail/service_client_impl.hpp"
