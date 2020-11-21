#pragma once
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <typeinfo>
#include <tuple>
#include <derecho/utils/time.h>
#include <nlohmann/json.hpp>
#include "cascade.hpp"

using json = nlohmann::json; 

/**
 * The cascade service templates
 * 
 * Type neutral templates components go here. Since the server binary and client library has to be type aware (because
 * they are pre-compiled), we separate the api and implementation of them in type-awared header files as follows:
 * - service_types.hpp contains the predefined types for derecho Subgroups, which are specialized from
 *   derecho::cascade::VolatileCascadeStore/PersistentCascadeStore templates.
 * - service_client_api.hpp contains the client API definition.
 * - service_server_api.hpp contains the server API definition. Huh, Server API??? YES! because the application need to
 *   specify their 'onData()' behaviours by implementing the APIs in service_server_api.hpp as a shared library. The
 *   server will load them on restart.
 */
namespace derecho {
namespace cascade {

#define CONF_ONDATA_LIBRARY     "CASCADE/ondata_library"
#define CONF_GROUP_LAYOUT       "CASCADE/group_layout"
#define JSON_CONF_TYPE_ALIAS    "type_alias"
#define JSON_CONF_LAYOUT        "layout"
/**
 * The service will start a cascade service node to serve the client.
 */
template <typename... CascadeTypes>
class Service {
public:
    /**
     * Constructor
     * The constructor will load the configuration, start the service thread.
     * @param layout TODO: explain layout
     * @param dsms TODO: explain it here
     * @param factories: explain it here
     */
    Service(const json& layout, const std::vector<DeserializationContext*>& dsms, derecho::Factory<CascadeTypes>... factories);
    /**
     * The workhorse
     */
    void run();
    /**
     * Stop the service
     */
    void stop(bool is_joining);
    /**
     * Join the service thread
     */
    void join();
    /**
     * Test if the service is running or stopped.
     */ 
    bool is_running();
private:
    /**
     * control synchronization members
     */
    std::mutex service_control_mutex;
    std::condition_variable service_control_cv;
    bool _is_running;
    std::thread service_thread;
    /**
     * The group
     */
    std::unique_ptr<derecho::Group<CascadeTypes...>> group;

    /**
     * Singleton pointer
     */
    static std::unique_ptr<Service<CascadeTypes...>> service_ptr;

public:
    /**
     * Start the singleton service
     * Please make sure only one thread call start. We do not defense such an incorrect usage.
     * @param layout TODO: explain layout
     * @param factories - the factories to create objects.
     */
    static void start(const json& layout, const std::vector<DeserializationContext*>& dsms, derecho::Factory<CascadeTypes>... factories);
    /**
     * Check if service is started or not.
     */
    static bool is_started();
    /**
     * shutdown the service
     */
    static void shutdown(bool is_joining=true);
    /**
     * wait on the service util it stop
     */
    static void wait();
};

/**
 * The Service Context
 */
template <typename... CascadeTypes>
class ServiceContext {
};

/**
 * Create the critical data path callback function.
 * Application should provide corresponding callbacks. The application MUST hold the ownership of the
 * callback objects and make sure its availability during service lifecycle.
 */
template <typename KT, typename VT, KT* IK, VT *IV>
std::shared_ptr<CascadeWatcher<KT,VT,IK,IV>> create_critical_data_path_callback();

/**
 * defining key strings used in the [CASCADE] section of configuration file.
 */
#define MIN_NODES_BY_SHARD      "min_nodes_by_shard"
#define MAX_NODES_BY_SHARD      "max_nodes_by_shard"
#define DELIVERY_MODES_BY_SHARD "delivery_modes_by_shard"
#define DELIVERY_MODE_ORDERED   "Ordered"
#define DELIVERY_MODE_RAW       "Raw"
#define PROFILES_BY_SHARD       "profiles_by_shard"

/**
 * The ServiceClient template class contains all APIs needed for read/write data. The four core APIs are put, remove,
 * get, and get_by_time. We also provide a set of helper APIs for the client to get the group topology. By default, the
 * core APIs are talking a random but fix member of the specified subgroup and shard. The client can override this
 * behaviour by specifying other member selection policy (ShardMemberSelectionPolicy).
 *
 * The default policy behaviour depends on the
 */
enum ShardMemberSelectionPolicy {
    FirstMember,    // use the first member in the list returned from get_shard_members(), this is the default behaviour.
    LastMember,     // use the last member in the list returned from get_shard_members()
    Random,         // use a random member in the shard for each operations(put/remove/get/get_by_time).
    FixedRandom,    // use a random member and stick to that for the following operations.
    RoundRobin,     // use a member in round-robin order.
    UserSpecified,  // user specify which member to contact.
    InvalidPolicy = -1
};
#define DEFAULT_SHARD_MEMBER_SELECTION_POLICY (ShardMemberSelectionPolicy::FirstMember)

template <typename T> struct do_hash {};

template <> struct do_hash<std::tuple<std::type_index,uint32_t,uint32_t>> {
    size_t operator()(const std::tuple<std::type_index,uint32_t,uint32_t>& t) const {
        return static_cast<size_t>(std::get<0>(t).hash_code() ^ ((std::get<1>(t)<<16) | std::get<2>(t)));
    }
};

template <typename... CascadeTypes>
class ServiceClient {
private:
    // caller
    derecho::ExternalGroup<CascadeTypes...> external_group;
    /**
     * 'member_selection_policies' is a map from derecho shard to its member selection policy.
     * We use a 3-tuple consisting of subgroup type index, subgroup index, and shard index to identify a shard. And
     * the policy is defined by a 2-tuple with the ShardMemberSelectionPolicy enum and a user specified node id, in
     * case of ShardMemorySelectionPolicy::UserSpecified. The user specified node id is used as member index if the
     * policy is ShardMemberSelectionPolicy::RoundRobin
     *
     * The default member selection policy is defined as SHARD_MEMBER_SELECTION_POLICY (ShardMemberSelectionPolicy::FirstMember).
     */
    std::unordered_map<
        std::tuple<std::type_index,uint32_t,uint32_t>,
        std::tuple<ShardMemberSelectionPolicy,node_id_t>,
        do_hash<std::tuple<std::type_index,uint32_t,uint32_t>>> member_selection_policies;
    std::shared_mutex member_selection_policies_mutex;
    /**
     * 'member_cache' is a map from derecho shard to its member list. This cache is used to accelerate the member
     * choices process. If the client cannot connect to the cached member (after a couple of retries), it will refresh
     * the corresponding cache entry.
     */
    std::unordered_map<
        std::tuple<std::type_index,uint32_t,uint32_t>,
        std::vector<node_id_t>,
        do_hash<std::tuple<std::type_index,uint32_t,uint32_t>>> member_cache;
    std::shared_mutex member_cache_mutex;

    /**
     * Pick a member by a given a policy.
     * @param subgroup_index
     * @param shard_index 
     * @param retry - if true, refresh the member_cache.
     */
    template <typename SubgroupType>
    node_id_t pick_member_by_policy(uint32_t subgroup_index,
                                             uint32_t shard_index,
                                             bool retry = false);

    /**
     * Refresh(or fill) a member cache entry.
     * @param subgroup_index
     * @param shard_index
     */
    template <typename SubgroupType>
    void refresh_member_cache_entry(uint32_t subgroup_index, uint32_t shard_index);
public:
    /**
     * Derecho group helpers: They derive the API in derecho::ExternalClient.
     * - get_members        returns all members in the top-level Derecho group.
     * - get_shard_members  returns the members in a shard specified by subgroup id(or subgroup type/index pair) and
     *   shard index.
     * - get_number_of_subgroups    returns the number of subgroups of a given type
     * - get_number_of_shards       returns the number of shards of a given subgroup
     * During view change, the Client might experience failure if the member is gone. In such a case, the client needs
     * refresh its local member cache by calling get_shard_members.
     */
    std::vector<node_id_t> get_members();
    std::vector<node_id_t> get_shard_members(derecho::subgroup_id_t subgroup_id,uint32_t shard_index);
    template <typename SubgroupType>
    std::vector<node_id_t> get_shard_members(uint32_t subgroup_index,uint32_t shard_index);
    template <typename SubgroupType>
    uint32_t get_number_of_subgroups();
    uint32_t get_number_of_shards(derecho::subgroup_id_t subgroup_id);
    template <typename SubgroupType>
    uint32_t get_number_of_shards(uint32_t subgroup_index);

    /**
     * Member selection policy control API.
     * - set_member_selection_policy updates the member selection policies.
     * - get_member_selection_policy read the member selection policies.
     * @param subgroup_index 
     * @param shard_index
     * @policy
     * @user_specified_node_id
     * @return get_member_selection_policy returns a 2-tuple of policy and user_specified_node_id.
     */
    template <typename SubgroupType>
    void set_member_selection_policy(uint32_t subgroup_index,uint32_t shard_index,
            ShardMemberSelectionPolicy policy,node_id_t user_specified_node_id=INVALID_NODE_ID);

    template <typename SubgroupType>
    std::tuple<ShardMemberSelectionPolicy,node_id_t> get_member_selection_policy(
            uint32_t subgroup_index, uint32_t shard_index);

    /**
     * "put" writes an object to a given subgroup/shard.
     *
     * @param object            the object to write.
     *                          User provided SubgroupType::ObjectType must have the following two members:
     *                          - SubgroupType::ObjectType::key of SubgroupType::KeyType, which must be set to a
     *                            valid key.
     *                          - SubgroupType::ObjectType::ver of std::tuple<persistent::version_t, uint64_t>.
     *                            Similar to the return object, this member is a two tuple with the first member
     *                            for a version and the second for a timestamp. A caller of put can specify either
     *                            of the version and timestamp meaning what is the latest version/timestamp the caller
     *                            has seen. Cascade will reject the write if the corresponding key has been updated
     *                            already. TODO: should we make it an optional feature?
     * @subugroup_index         the subgroup index of CascadeType
     * @shard_index             the shard index.
     *
     * @return a future to the version and timestamp of the put operation.
     * TODO: check if the user application is responsible for reclaim the future by reading it sometime.
     */
    template <typename SubgroupType>
    derecho::rpc::QueryResults<std::tuple<persistent::version_t,uint64_t>> put(const typename SubgroupType::ObjectType& object,
            uint32_t subgroup_index=0, uint32_t shard_index=0);

    /**
     * "remove" deletes an object with the given key.
     *
     * @param key               the object key
     * @subugroup_index         the subgroup index of CascadeType
     * @shard_index             the shard index.
     *
     * @return a future to the version and timestamp of the put operation.
     * TODO: check if the user application is responsible for reclaim the future by reading it sometime.
     */
    template <typename SubgroupType>
    derecho::rpc::QueryResults<std::tuple<persistent::version_t,uint64_t>> remove(const typename SubgroupType::KeyType& key,
            uint32_t subgroup_index=0, uint32_t shard_index=0);

    /**
     * "get" retrieve the object of a given key
     *
     * @param key               the object key
     * @param version           if version is CURRENT_VERSION, this "get" will fire a ordered send to get the latest
     *                          state of the key. Otherwise, it will try to read the key's state at version.
     * @subugroup_index         the subgroup index of CascadeType
     * @shard_index             the shard index.
     *
     * @return a future to the retrieved object.
     * TODO: check if the user application is responsible for reclaim the future by reading it sometime.
     */
    template <typename SubgroupType>
    derecho::rpc::QueryResults<const typename SubgroupType::ObjectType> get(const typename SubgroupType::KeyType& key, const persistent::version_t& version = CURRENT_VERSION,
            uint32_t subgroup_index=0, uint32_t shard_index=0);

    /**
     * "get_by_time" retrieve the object of a given key
     *
     * @param key               the object key
     * @param ts_us             Wall clock time in microseconds. 
     * @subugroup_index         the subgroup index of CascadeType
     * @shard_index             the shard index.
     *
     * @return a future to the retrieved object.
     * TODO: check if the user application is responsible for reclaim the future by reading it sometime.
     */
    template <typename SubgroupType>
    derecho::rpc::QueryResults<const typename SubgroupType::ObjectType> get_by_time(const typename SubgroupType::KeyType& key, const uint64_t& ts_us,
            uint32_t subgroup_index=0, uint32_t shard_index=0);

    /**
     * "get_size" retrieve size of the object of a given key
     *
     * @param key               the object key
     * @param version           if version is CURRENT_VERSION, this "get" will fire a ordered send to get the latest
     *                          state of the key. Otherwise, it will try to read the key's state at version.
     * @subugroup_index         the subgroup index of CascadeType
     * @shard_index             the shard index.
     *
     * @return a future to the retrieved size.
     * TODO: check if the user application is responsible for reclaim the future by reading it sometime.
     */
    template <typename SubgroupType>
    derecho::rpc::QueryResults<uint64_t> get_size(const typename SubgroupType::KeyType& key, const persistent::version_t& version = CURRENT_VERSION,
            uint32_t subgroup_index=0, uint32_t shard_index=0);

    /**
     * "get_size_by_time" retrieve size of the object of a given key
     *
     * @param key               the object key
     * @param ts_us             Wall clock time in microseconds. 
     * @subugroup_index         the subgroup index of CascadeType
     * @shard_index             the shard index.
     *
     * @return a future to the retrieved size.
     * TODO: check if the user application is responsible for reclaim the future by reading it sometime.
     */
    template <typename SubgroupType>
    derecho::rpc::QueryResults<uint64_t> get_size_by_time(const typename SubgroupType::KeyType& key, const uint64_t& ts_us,
            uint32_t subgroup_index=0, uint32_t shard_index=0);

    /**
     * "list_keys" retrieve the list of keys in a shard
     *
     * @param version           if version is CURRENT_VERSION, this "get" will fire a ordered send to get the latest
     *                          state of the key. Otherwise, it will try to read the key's state at version.
     * @subugroup_index         the subgroup index of CascadeType
     * @shard_index             the shard index.
     *
     * @return a future to the retrieved object.
     * TODO: check if the user application is responsible for reclaim the future by reading it sometime.
     */
    template <typename SubgroupType>
    derecho::rpc::QueryResults<std::vector<typename SubgroupType::KeyType>> list_keys(const persistent::version_t& version = CURRENT_VERSION,
            uint32_t subgroup_index=0, uint32_t shard_index=0);

    /**
     * "list_keys_by_time" retrieve the list of keys in a shard
     *
     * @param ts_us             Wall clock time in microseconds.
     * @subugroup_index         the subgroup index of CascadeType
     * @shard_index             the shard index.
     *
     * @return a future to the retrieved object.
     * TODO: check if the user application is responsible for reclaim the future by reading it sometime.
     */
    template <typename SubgroupType>
    derecho::rpc::QueryResults<std::vector<typename SubgroupType::KeyType>> list_keys_by_time(const uint64_t& ts_us,
            uint32_t subgroup_index=0, uint32_t shard_index=0);
    template <typename SubgroupType>
    void submit_predicate(const std::string& key, const std::string& predicate_str, const bool inplace,
            uint32_t subgroup_index=0, uint32_t shard_index=0);

    template <typename SubgroupType>
    void change_predicate(const std::string& key,
            uint32_t subgroup_index=0, uint32_t shard_index=0);
};

}// namespace cascade
}// namespace derecho

#include "detail/service_impl.hpp"
