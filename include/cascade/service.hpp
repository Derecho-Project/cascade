#pragma once
#include <cstdint>
#include <derecho/persistent/PersistentInterface.hpp>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <typeinfo>
#include <tuple>
#include <derecho/utils/time.h>
#include <list>
#include <condition_variable>
#include <thread>
#include <functional>
#include <iostream>
#include <unordered_map>
#include <unordered_set>
#include <derecho/conf/conf.hpp>
#include "cascade.hpp"
#include "object_pool_metadata.hpp"
#include "user_defined_logic_manager.hpp"
#include "data_flow_graph.hpp"
#include "detail/prefix_registry.hpp"

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
    /* Cascade Factory type*/
    template <typename CascadeType>
    using Factory = std::function<std::unique_ptr<CascadeType>(persistent::PersistentRegistry*, subgroup_id_t subgroup_id, ICascadeContext*)>;

    /* Cascade Metadata Service type*/
	template<typename...CascadeTypes>
	using CascadeMetadataService = PersistentCascadeStore<
    	std::remove_cv_t<std::remove_reference_t<decltype(((ObjectPoolMetadata<CascadeTypes...>*)nullptr)->get_key_ref())>>,
        ObjectPoolMetadata<CascadeTypes...>,
        &ObjectPoolMetadata<CascadeTypes...>::IK,
        &ObjectPoolMetadata<CascadeTypes...>::IV,
        ST_FILE>;
#define METADATA_SERVICE_SUBGROUP_INDEX (0)

    
    /* The cascade context to be defined later */
    template <typename... CascadeTypes>
    class CascadeContext;

    /* The Action to be defined later */
    struct Action;
    /**
     * The off-critical data path handler API
     */
    class OffCriticalDataPathObserver: public derecho::DeserializationContext {
    public:
        /**
         * This function has to be re-entrant/thread-safe.
         * @param key_string    The key string
         * @param prefix        The matching prefix length key_string.subtring(0,prefix) returns the prefix.
         *                      Please note that the trailing '/' is included.
         * @param version       The version of the key
         * @param value_ptr     The raw value pointer
         * @param ctxt          The CascadeContext
         * @param worker_id     The off critical data path worker id.
         */ 
        virtual void operator() (const std::string& key_string,
                                 const uint32_t prefix_length,
                                 persistent::version_t version,
                                 const mutils::ByteRepresentable* const value_ptr,
                                 const std::unordered_map<std::string,bool>& outputs,
                                 ICascadeContext* ctxt, 
                                 uint32_t worker_id) = 0;
    };
    /**
     * Action is an command passed from the on critical data path logic (cascade watcher) to the off critical data path
     * logic, a.k.a. workers, running in the cascade context thread pool.
     *
     * !!! IMPORTANT NOTES ON "ACTION" DESIGN !!!
     * Action carries the key string, version, prefix handler (ocdpo_raw_ptr), and the object value so that the prefix
     * handler have all the information to process in the worker thread. It is important to avoid unnecessary copies
     * because the object value is big sometime (for example, a high resolution video clip). Currently, we copied the
     * value data into a new allocated memory buffer pointed by a unique pointer in the critical data path because the
     * value in critical data path is in Derecho's managed RDMA buffer, which will not last beyond the lifetime of the
     * critical data path. However, even this copy can be avoided using a lock-less design.
     *
     * For example, we can pass the raw pointer to the value in VolatileCascadeStore or PersistentCascadeStore instead of
     * allocating new memory and copying data. But the critical data path keeps updating the value (actually, the old
     * value is removed from the map, and a new value is inserted). Dereferencing the raw pointer might crash with a
     * segmentation fault if the pointed value is reclaimed. Moreover, using lock is not efficient at all because the
     * off critical data path lock will block the critical data path, slowing down the whole system. An optimal solution
     * to this issue is to 
     * 1) keep a short history of all the versions in VolatileCascadeStore or PersistentCascadeStore in std::vector<>;
     * 2) enable concurrent access to the value. For example, we can allocate a lock for each of the slot of the history
     * and pass it to the critical data path so that the worker thread can lock the corresponding slot when it is
     * working on that. The number of slots in history should match the size of action buffer.
     *
     * This is a TODO work to be done later. So far, we stick to the extra copy for convenience.
     *
     */
#define ACTION_BUFFER_ENTRY_SIZE    (256)
#define ACTION_BUFFER_SIZE          (1024)
    struct Action {
        std::string                     key_string;
        uint32_t                        prefix_length;
        persistent::version_t           version;
        std::shared_ptr<OffCriticalDataPathObserver>   ocdpo_ptr;
        std::shared_ptr<mutils::ByteRepresentable>     value_ptr;
        std::unordered_map<std::string,bool>           outputs;
        /**
         * Move constructor
         * @param other     The input Action object
         */
        Action(Action&& other):
            key_string(other.key_string),
            prefix_length(other.prefix_length),
            version(other.version),
            ocdpo_ptr(std::move(other.ocdpo_ptr)),
            value_ptr(std::move(other.value_ptr)),
            outputs(std::move(other.outputs)) {}
        /**
         * Constructor
         * @param   _key_string
         * @param   _version
         * @param   _ocdpo_ptr const reference rvalue
         * @param   _value_ptr
         */
        Action(const std::string&           _key_string = "",
               const uint32_t               _prefix_length = 0,
               const persistent::version_t& _version = CURRENT_VERSION,
               const std::shared_ptr<OffCriticalDataPathObserver>&  _ocdpo_ptr = nullptr,
               const std::shared_ptr<mutils::ByteRepresentable>&    _value_ptr = nullptr,
               const std::unordered_map<std::string,bool>           _outputs = {}):
            key_string(_key_string),
            prefix_length(_prefix_length),
            version(_version),
            ocdpo_ptr(_ocdpo_ptr),
            value_ptr(_value_ptr),
            outputs(_outputs) {}
        Action(const Action&) = delete; // disable copy constructor
        /**
         * Assignment operators
         */
        Action& operator = (Action&&) = default;
        Action& operator = (const Action&) = delete;
        /**
         *  fire the action.
         *  @param ctxt
         *  @param worker_id
         */
        inline void fire(ICascadeContext* ctxt,uint32_t worker_id) {
            if (value_ptr && ocdpo_ptr) {
                dbg_default_trace("In {}: action is fired.", __PRETTY_FUNCTION__);
                (*ocdpo_ptr)(key_string,prefix_length,version,value_ptr.get(),outputs,ctxt,worker_id);
            }
        }
        inline explicit operator bool() const {
            return (bool)value_ptr;
        }
    };

    inline std::ostream& operator << (std::ostream& out, const Action& action) {
        out << "Action:\n"
            << "\tkey = " << action.key_string << "\n"
            << "\tprefix_length = " << action.prefix_length << "\n"
            << "\tversion = " << std::hex << action.version << "\n"
            << "\tocdpo_ptr = " << action.ocdpo_ptr.get() << "\n"
            << "\tvalue_ptr = " << action.value_ptr.get() << "\n"
            << "\toutput = ";
        for (auto& output:action.outputs) {
            out << output.first << (output.second? "[*]":"") << ";";
        }
        out << std::endl;

        return out;
    }
    
    /**
     * The service will start a cascade service node to serve the client.
     */
    template <typename... CascadeTypes>
    class Service {
    public:
        /**
         * Constructor
         * The constructor will load the configuration, start the service thread.
         * @param dsms deserialization managers
         * @param metadata_service_factory
         * @param factories: subgroup factories.
         */
        Service(const std::vector<DeserializationContext*>& dsms,
                derecho::cascade::Factory<CascadeMetadataService<CascadeTypes...>> metadata_service_factory,
                derecho::cascade::Factory<CascadeTypes>... factories);
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
        std::unique_ptr<derecho::Group<CascadeMetadataService<CascadeTypes...>,CascadeTypes...>> group;
        /**
         * The CascadeContext
         */
        std::unique_ptr<CascadeContext<CascadeTypes...>> context;
    
        /**
         * Singleton pointer
         */
        static std::unique_ptr<Service<CascadeTypes...>> service_ptr;
    
    public:
        /**
         * Start the singleton service
         * Please make sure only one thread call start. We do not defense such an incorrect usage.
         *
         * @param dsms
         * @param metadata_factory - factory for the metadata service.
         * @param factories - the factories to create objects.
         */
        static void start(const std::vector<DeserializationContext*>& dsms,
                          derecho::cascade::Factory<CascadeMetadataService<CascadeTypes...>> metadata_factory,
                          derecho::cascade::Factory<CascadeTypes>... factories);
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
     * Create the critical data path callback function.
     * Application should provide corresponding callbacks. The application MUST hold the ownership of the
     * callback objects and make sure its availability during service lifecycle.
     *
    template <typename KT, typename VT, KT* IK, VT *IV>
    std::shared_ptr<CascadeWatcher<KT,VT,IK,IV>> create_critical_data_path_callback();
     */

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
        // default caller as an external client.
        std::unique_ptr<derecho::ExternalGroup<CascadeMetadataService<CascadeTypes...>,CascadeTypes...>> external_group_ptr;
        mutable std::mutex external_group_ptr_mutex;
        // caller as a group member.
        derecho::Group<CascadeMetadataService<CascadeTypes...>, CascadeTypes...>* group_ptr;
        mutable std::mutex group_ptr_mutex;
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
        mutable std::shared_mutex member_selection_policies_mutex;
        /**
         * 'member_cache' is a map from derecho shard to its member list. This cache is used to accelerate the member
         * choices process. If the client cannot connect to the cached member (after a couple of retries), it will refresh
         * the corresponding cache entry.
         */
        std::unordered_map<
            std::tuple<std::type_index,uint32_t,uint32_t>,
            std::vector<node_id_t>,
            do_hash<std::tuple<std::type_index,uint32_t,uint32_t>>> member_cache;
        mutable std::shared_mutex member_cache_mutex;
        /**
         * 'object_pool_info_cache' is a local cache for object pool metadata. This cache is used to accelerate the
         * object access process. If an object pool does not exists, it will be loaded from metadata service.
         */
        std::unordered_map<
            std::string,
            ObjectPoolMetadata<CascadeTypes...>> object_pool_metadata_cache;
        mutable std::shared_mutex object_pool_metadata_cache_mutex;
    
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

        /**
         * Deprecated: Please use key_to_shard() instead
         *
         * Metadata API Helper: turn a string key to subgroup index and shard index
         *
        template <typename SubgroupType>
        std::pair<uint32_t,uint32_t> key_to_subgroup_index_and_shard_index(const typename SubgroupType::KeyType& key,
                bool check_object_location = true);
         */

        /**
         * Metadata API Helper: turn a string key to subgroup type index, subgroup index, and shard index.
         */
        template <typename KeyType>
        std::tuple<uint32_t,uint32_t,uint32_t> key_to_shard(
                const KeyType& key, bool check_object_location = true);

    public:
        /**
         * The Constructor
         * @param _group_ptr The caller can pass a pointer pointing to a derecho group object. If the pointer is
         *                   valid, the implementation will reply on the group object instead of creating an external
         *                   client to communicate with group members.
         */
        ServiceClient(derecho::Group<CascadeMetadataService<CascadeTypes...>, CascadeTypes...>* _group_ptr=nullptr);
        /**
         * Derecho group helpers: They derive the API in derecho::ExternalClient.
         * - get_my_id          return my local node id.
         * - get_members        returns all members in the top-level Derecho group.
         * - get_shard_members  returns the members in a shard specified by subgroup id(or subgroup type/index pair) and
         *   shard index.
         * - get_number_of_subgroups    returns the number of subgroups of a given type
         * - get_number_of_shards       returns the number of shards of a given subgroup
         * During view change, the Client might experience failure if the member is gone. In such a case, the client needs
         * refresh its local member cache by calling get_shard_members.
         */
        node_id_t get_my_id() const;

        std::vector<node_id_t> get_members() const;

        template <typename SubgroupType>
        std::vector<node_id_t> get_shard_members(uint32_t subgroup_index,uint32_t shard_index) const;

        template <typename SubgroupType>
        uint32_t get_number_of_shards(uint32_t subgroup_index) const;

        // type recursive helpers for get_number_of_shards
    protected:
        template <typename FirstType,typename SecondType, typename...RestTypes>
        uint32_t type_recursive_get_number_of_shards(uint32_t type_index, uint32_t subgroup_index) const;
        template <typename LastType>
        uint32_t type_recursive_get_number_of_shards(uint32_t type_index, uint32_t subgroup_index) const;
    public:
        /**
         * This get_number_of_shards() overload the typed version.
         * @param subgroup_type_index   - the type index of the subrgoup type.
         * @param subgroup_index        - the subgroup index in the given type.
         */
        uint32_t get_number_of_shards(uint32_t subgroup_type_index, uint32_t subgroup_index) const;

        /**
         * Member selection policy control API.
         * - set_member_selection_policy updates the member selection policies.
         * - get_member_selection_policy read the member selection policies.
         * @param subgroup_index 
         * @param shard_index
         * @param policy
         * @param user_specified_node_id
         * @return get_member_selection_policy returns a 2-tuple of policy and user_specified_node_id.
         */
        template <typename SubgroupType>
        void set_member_selection_policy(uint32_t subgroup_index,uint32_t shard_index,
                ShardMemberSelectionPolicy policy,node_id_t user_specified_node_id=INVALID_NODE_ID);
    
        template <typename SubgroupType>
        std::tuple<ShardMemberSelectionPolicy,node_id_t> get_member_selection_policy(
                uint32_t subgroup_index, uint32_t shard_index) const;
    
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
         * @param subugroup_index   the subgroup index of CascadeType
         * @param shard_index       the shard index.
         *
         * @return a future to the version and timestamp of the put operation.
         * TODO: check if the user application is responsible for reclaim the future by reading it sometime.
         */
        template <typename SubgroupType>
        derecho::rpc::QueryResults<std::tuple<persistent::version_t,uint64_t>> put(const typename SubgroupType::ObjectType& object,
                uint32_t subgroup_index, uint32_t shard_index);
        /**
         * "type_recursive_put" is a helper function for internal use only.
         * @type_index              the index of the subgroup type in the CascadeTypes... list. And the FirstType,
         *                          SecondType, ..., RestTypes should be in the same order.
         * @object                  the object to write
         * @subgroup_index          the subgroup index in the subgroup type designated by type_index
         * @shard_index             the shard index
         *
         * @return a future to the version and timestamp of the put operation.
         */
    protected:
        template <typename ObjectType, typename FirstType, typename SecondType, typename... RestTypes>
        derecho::rpc::QueryResults<std::tuple<persistent::version_t,uint64_t>> type_recursive_put(
                uint32_t type_index,
                const ObjectType& object,
                uint32_t subgroup_index,
                uint32_t shard_index);

        template <typename ObjectType, typename LastType>
        derecho::rpc::QueryResults<std::tuple<persistent::version_t,uint64_t>> type_recursive_put(
                uint32_t type_index,
                const ObjectType& object,
                uint32_t subgroup_index,
                uint32_t shard_index);
    public:
        /**
         * object pool version
         * @param object            the object to write, the object pool is extracted from the object key.
         *
         * @return a future to the version and timestamp of the put operation.
         */
        template <typename ObjectType>
        derecho::rpc::QueryResults<std::tuple<persistent::version_t,uint64_t>> put(const ObjectType& object);

        /**
         * "put_and_forget" writes an object to a given subgroup/shard, but no return value.
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
         * @param subugroup_index   the subgroup index of CascadeType
         * @param shard_index       the shard index.
         */
        template <typename SubgroupType>
        void put_and_forget(const typename SubgroupType::ObjectType& object,
                uint32_t subgroup_index, uint32_t shard_index);

        /**
         * "type_recursive_put_and_forget" is a helper function for internal use only.
         * @type_index              the index of the subgroup type in the CascadeTypes... list. and the FirstType,
         *                          SecondType, .../ RestTypes should be in the same order.
         * @object                  the object to write
         * @subgroup_index          the subgroup index in the subgroup type designated by type_index
         * @shard_index             the shard index
         */
    protected:
        template <typename ObjectType, typename FirstType, typename SecondType, typename... RestTypes>
        void type_recursive_put_and_forget(
                uint32_t type_index,
                const ObjectType& object,
                uint32_t subgroup_index,
                uint32_t shard_index);

        template <typename ObjectType, typename LastType>
        void type_recursive_put_and_forget(
                uint32_t type_index,
                const ObjectType& object,
                uint32_t subgroup_index,
                uint32_t shard_index);
    public:
        /**
         * object pool version
         * @param object    the object to write, the object pool is extracted from the object key.
         */
        template <typename ObjectType>
        void put_and_forget(const ObjectType& object);

        /**
         * "trigger_put" writes an object to a given subgroup/shard.
         *
         * @param object            the object to write.
         * @param subugroup_index   the subgroup index of CascadeType
         * @param shard_index       the shard index.
         *
         * @return a void future.
         */
        template <typename SubgroupType>
        derecho::rpc::QueryResults<void> trigger_put(const typename SubgroupType::ObjectType& object,
                uint32_t subgroup_index, uint32_t shard_index);

        /**
         * "type_recursive_trigger_put" is a helper function for internal use only.
         * @type_index              the index of the subgroup type in the CascadeTypes... list. and the FirstType,
         *                          SecondType, .../ RestTypes should be in the same order.
         * @object                  the object to write
         * @subgroup_index          the subgroup index in the subgroup type designated by type_index
         * @shard_index             the shard index
         */
    protected:
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
         * object pool version
         * @param object    the object to write, the object pool is extracted from the object key.
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
         * @param object            the object to write.
         * @param subugroup_index   the subgroup index of CascadeType
         * @param nodes             node ids for the set of nodes.
         *
         * @return an array of void futures, which length is nodes.size()
         */
        template <typename SubgroupType>
        void collective_trigger_put(const typename SubgroupType::ObjectType& object,
                uint32_t subgroup_index,
                std::unordered_map<node_id_t,std::unique_ptr<derecho::rpc::QueryResults<void>>>& nodes_and_futures);
    
        /**
         * "remove" deletes an object with the given key.
         *
         * @param key               the object key
         * @param subugroup_index   the subgroup index of CascadeType
         * @param shard_index       the shard index.
         *
         * @return a future to the version and timestamp of the put operation.
         * TODO: check if the user application is responsible for reclaim the future by reading it sometime.
         */
        template <typename SubgroupType>
        derecho::rpc::QueryResults<std::tuple<persistent::version_t,uint64_t>> remove(const typename SubgroupType::KeyType& key,
                uint32_t subgroup_index, uint32_t shard_index);

        /**
         * "type_recursive_remove" is a helper function for internal use only.
         * @type_index              the index of the subgroup type in the CascadeTypes... list. and the FirstType,
         *                          SecondType, .../ RestTypes should be in the same order.
         * @key                     the key
         * @subgroup_index          the subgroup index in the subgroup type designated by type_index
         * @shard_index             the shard index
         *
         * @return a future to the version and timestamp of the put operation.
         */
    protected:
        template <typename KeyType, typename FirstType, typename SecondType, typename... RestTypes>
        derecho::rpc::QueryResults<std::tuple<persistent::version_t,uint64_t>> type_recursive_remove(
                uint32_t type_index,
                const KeyType& key,
                uint32_t subgroup_index,
                uint32_t shard_index);

        template <typename KeyType, typename LastType>
        derecho::rpc::QueryResults<std::tuple<persistent::version_t,uint64_t>> type_recursive_remove(
                uint32_t type_index,
                const KeyType& key,
                uint32_t subgroup_index,
                uint32_t shard_index);
    public:
        /**
         * object pool version
         */
        template <typename KeyType>
        derecho::rpc::QueryResults<std::tuple<persistent::version_t,uint64_t>> remove(const KeyType& key);
    
        /**
         * "get" retrieve the object of a given key
         *
         * @param key               the object key
         * @param version           if version is CURRENT_VERSION, this "get" will fire a ordered send to get the latest
         *                          state of the key. Otherwise, it will try to read the key's state at version.
         * @param subugroup_index   the subgroup index of CascadeType
         * @param shard_index       the shard index.
         *
         * @return a future to the retrieved object.
         * TODO: check if the user application is responsible for reclaim the future by reading it sometime.
         */
        template <typename SubgroupType>
        derecho::rpc::QueryResults<const typename SubgroupType::ObjectType> get(const typename SubgroupType::KeyType& key, const persistent::version_t& version, 
                uint32_t subgroup_index, uint32_t shard_index);

        /**
         * "type_recursive_get" is a helper function for internal use only.
         * @type_index              the index of the subgroup type in the CascadeTypes... list. and the FirstType,
         *                          SecondType, .../ RestTypes should be in the same order.
         * @key                     the key
         * @version                 the version
         * @subgroup_index          the subgroup index in the subgroup type designated by type_index
         * @shard_index             the shard index
         *
         * @return a future for the object.
         */
    protected:
        template <typename KeyType, typename FirstType, typename SecondType, typename... RestTypes>
        auto type_recursive_get(
                uint32_t type_index,
                const KeyType& key,
                const persistent::version_t& version,
                uint32_t subgroup_index,
                uint32_t shard_index);

        template <typename KeyType, typename LastType>
        auto type_recursive_get(
                uint32_t type_index,
                const KeyType& key,
                const persistent::version_t& version,
                uint32_t subgroup_index,
                uint32_t shard_index);
    public:
        
        /**
         * object pool version
         */
        template <typename KeyType>
        auto get(
                const KeyType& key,
                const persistent::version_t& version = CURRENT_VERSION);
    
        /**
         * "get_by_time" retrieve the object of a given key
         *
         * @param key               the object key
         * @param ts_us             Wall clock time in microseconds. 
         * @param subugroup_index   the subgroup index of CascadeType
         * @param shard_index       the shard index.
         *
         * @return a future to the retrieved object.
         * TODO: check if the user application is responsible for reclaim the future by reading it sometime.
         */
        template <typename SubgroupType>
        derecho::rpc::QueryResults<const typename SubgroupType::ObjectType> get_by_time(const typename SubgroupType::KeyType& key, const uint64_t& ts_us,
                uint32_t subgroup_index, uint32_t shard_index);

        /**
         * "type_recursive_get_by_time" is a helper function for internal use only.
         * @param type_index        the index of the subgroup type in the CascadeTypes... list. and the FirstType,
         *                          SecondType, .../ RestTypes should be in the same order.
         * @param key               the key
         * @param ts_us             Wall clock time in microseconds. 
         * @param subgroup_index    the subgroup index in the subgroup type designated by type_index
         * @param shard_index       the shard index
         *
         * @return a future for the object.
         */
    protected:
        template <typename KeyType, typename FirstType, typename SecondType, typename... RestTypes>
        auto type_recursive_get_by_time(
                uint32_t type_index,
                const KeyType& key,
                const uint64_t& ts_us,
                uint32_t subgroup_index,
                uint32_t shard_index);

        template <typename KeyType, typename LastType>
        auto type_recursive_get_by_time(
                uint32_t type_index,
                const KeyType& key,
                const uint64_t& ts_us,
                uint32_t subgroup_index,
                uint32_t shard_index);
    public:
        
        /**
         * object pool version
         */
        template <typename KeyType>
        auto get_by_time(
                const KeyType& key,
                const uint64_t& ts_us);

        /**
         * "get_size" retrieve size of the object of a given key
         *
         * @param key               the object key
         * @param version           if version is CURRENT_VERSION, this "get" will fire a ordered send to get the latest
         *                          state of the key. Otherwise, it will try to read the key's state at version.
         * @param subugroup_index   the subgroup index of CascadeType
         * @param shard_index       the shard index.
         *
         * @return a future to the retrieved size.
         * TODO: check if the user application is responsible for reclaim the future by reading it sometime.
         */
        template <typename SubgroupType>
        derecho::rpc::QueryResults<uint64_t> get_size(const typename SubgroupType::KeyType& key, const persistent::version_t& version,
                uint32_t subgroup_index, uint32_t shard_index);

        /**
         * "type_recursive_get_size" is a helper function for internal use only.
         * @param type_index        the index of the subgroup type in the CascadeTypes... list. and the FirstType,
         *                          SecondType, .../ RestTypes should be in the same order.
         * @param key               the key
         * @param version           version
         * @param subgroup_index    the subgroup index in the subgroup type designated by type_index
         * @param shard_index       the shard index
         *
         * @return a future for the object.
         */
    protected:
        template <typename KeyType, typename FirstType, typename SecondType, typename... RestTypes>
        derecho::rpc::QueryResults<uint64_t> type_recursive_get_size(
                uint32_t type_index,
                const KeyType& key,
                const persistent::version_t& version,
                uint32_t subgroup_index,
                uint32_t shard_index);

        template <typename KeyType, typename LastType>
        derecho::rpc::QueryResults<uint64_t> type_recursive_get_size(
                uint32_t type_index,
                const KeyType& key,
                const persistent::version_t& version,
                uint32_t subgroup_index,
                uint32_t shard_index);
    public:
        
        /**
         * object pool version
         */
        template <typename KeyType>
        derecho::rpc::QueryResults<uint64_t> get_size(
                const KeyType& key,
                const persistent::version_t& version = CURRENT_VERSION);

        /**
         * "get_size_by_time" retrieve size of the object of a given key
         *
         * @param key               the object key
         * @param ts_us             Wall clock time in microseconds. 
         * @param subugroup_index   the subgroup index of CascadeType
         * @param shard_index       the shard index.
         *
         * @return a future to the retrieved size.
         * TODO: check if the user application is responsible for reclaim the future by reading it sometime.
         */
        template <typename SubgroupType>
        derecho::rpc::QueryResults<uint64_t> get_size_by_time(const typename SubgroupType::KeyType& key, const uint64_t& ts_us,
                uint32_t subgroup_index, uint32_t shard_index);

        /**
         * "type_recursive_get_size" is a helper function for internal use only.
         * @param type_index        the index of the subgroup type in the CascadeTypes... list. and the FirstType,
         *                          SecondType, .../ RestTypes should be in the same order.
         * @param key               the key
         * @param ts_us             Wall clock time in microseconds. 
         * @param subgroup_index    the subgroup index in the subgroup type designated by type_index
         * @param shard_index       the shard index
         *
         * @return a future for the object.
         */
    protected:
        template <typename KeyType, typename FirstType, typename SecondType, typename... RestTypes>
        derecho::rpc::QueryResults<uint64_t> type_recursive_get_size_by_time(
                uint32_t type_index,
                const KeyType& key,
                const uint64_t& ts_us,
                uint32_t subgroup_index,
                uint32_t shard_index);

        template <typename KeyType, typename LastType>
        derecho::rpc::QueryResults<uint64_t> type_recursive_get_size_by_time(
                uint32_t type_index,
                const KeyType& key,
                const uint64_t& ts_us,
                uint32_t subgroup_index,
                uint32_t shard_index);
    public:
        
        /**
         * object pool version
         */
        template <typename KeyType>
        derecho::rpc::QueryResults<uint64_t> get_size_by_time(
                const KeyType& key,
                const uint64_t& ts_us);

        /**
         * "list_keys" retrieve the list of keys in a shard
         *
         * @param version           if version is CURRENT_VERSION, this "get" will fire a ordered send to get the latest
         *                          state of the key. Otherwise, it will try to read the key's state at version.
         * @param subugroup_index   the subgroup index of CascadeType
         * @param shard_index       the shard index.
         *
         * @return a future to the retrieved object.
         * TODO: check if the user application is responsible for reclaim the future by reading it sometime.
         */
        template <typename SubgroupType>
        derecho::rpc::QueryResults<std::vector<typename SubgroupType::KeyType>> list_keys(const persistent::version_t& version,
                uint32_t subgroup_index, uint32_t shard_index);

    protected:
        template <typename FirstType, typename SecondType, typename... RestTypes>
        auto type_recursive_list_keys(
                uint32_t type_index,
                const persistent::version_t& version, 
                const std::string& object_pool_pathname);
        template <typename LastType> 
        auto type_recursive_list_keys(
                uint32_t type_index,
                const persistent::version_t& version,
                const std::string& object_pool_pathname);
        template <typename SubgroupType>
        std::vector<std::unique_ptr<derecho::rpc::QueryResults<std::vector<typename SubgroupType::KeyType>>>> 
            __list_keys(const persistent::version_t& version, const std::string& object_pool_pathname);
    public:
        /**
         * object pool version
         * @param version               if version is 
         * @param object_pool_pathname  the object pathname
         */
        auto list_keys(const persistent::version_t& version, const std::string& object_pool_pathname);

        template <typename KeyType>
        std::vector<KeyType> wait_list_keys(
                                std::vector<std::unique_ptr<derecho::rpc::QueryResults<std::vector<KeyType>>>>& future);
    
        /**
         * "list_keys_by_time" retrieve the list of keys in a shard
         *
         * @param ts_us             Wall clock time in microseconds.
         * @param subugroup_index   the subgroup index of CascadeType
         * @param shard_index       the shard index.
         *
         * @return a future to the retrieved object.
         * TODO: check if the user application is responsible for reclaim the future by reading it sometime.
         */
        template <typename SubgroupType>
        derecho::rpc::QueryResults<std::vector<typename SubgroupType::KeyType>> list_keys_by_time(const uint64_t& ts_us,
                uint32_t subgroup_index, uint32_t shard_index);
        
    protected:
        template <typename FirstType, typename SecondType, typename... RestTypes>
        auto type_recursive_list_keys_by_time(
                uint32_t type_index,
                const uint64_t& ts_us,
                const std::string& object_pool_pathname);
        template <typename LastType> 
        auto type_recursive_list_keys_by_time(
                uint32_t type_index,
                const uint64_t& ts_us,
                const std::string& object_pool_pathname);
        template <typename SubgroupType>
        std::vector<std::unique_ptr<derecho::rpc::QueryResults<std::vector<typename SubgroupType::KeyType>>>> 
            __list_keys_by_time(const uint64_t& ts_us, const std::string& object_pool_pathname);
    public:
        /**
        * object pool version
        * @param ts_us                  timestamp
        * @param object_pool_pathname   the object pathname
        */
        auto list_keys_by_time(const uint64_t& ts_us, const std::string& object_pool_pathname);

        /**
         * Object Pool Management API: refresh object pool cache
         */
        void refresh_object_pool_metadata_cache();

        /**
         * Object Pool Management API: create object pool
         *
         * @tparam SubgroupType     Type of the subgroup for the created object pool
         * @param  pathname         Object pool's pathname as identifier.
         * @param  subgroup_index   Index of the subgroup
         * @param  sharding_policy  The default sharding policy for this object pool
         * @param  object_locations The set of special object locations.
         *
         * @return a future to the version and timestamp of the put operation.
         */
        template <typename SubgroupType>
        derecho::rpc::QueryResults<std::tuple<persistent::version_t,uint64_t>> create_object_pool(
                const std::string& pathname, const uint32_t subgroup_index,
                const sharding_policy_t sharding_policy = HASH, const std::unordered_map<std::string,uint32_t>& object_locations = {});

        /**
         * ObjectPoolManagement API: remote object pool
         *
         * @param  pathname         Object pool pathname
         *
         * @return a future to the version and timestamp of the put operation.
         */
        derecho::rpc::QueryResults<std::tuple<persistent::version_t,uint64_t>> remove_object_pool(const std::string& pathname);

        /**
         * ObjectPoolManagement API: find object pool
         *
         * @param  pathname         Object pool pathname
         *
         * @return the object pool metadata
         */
        ObjectPoolMetadata<CascadeTypes...> find_object_pool(const std::string& pathname);

        /**
         * ObjectPoolManagement API: list all the object pools by pathnames
         *
         * @param refresh           false for cached object ids, true for refreshed ids.
         *
         * @return the pool ids.
         */
        std::vector<std::string> list_object_pools(bool refresh = false);

#ifdef ENABLE_EVALUATION
        /**
         * Dump the timestamp log entries into a file on each of the nodes in a subgroup.
         *
         * @param filename         - the output filename
         * @param subgroup_index   - the subgroup index
         * @param shard_index      - the shard index
         *
         * @return a vector of query results.
         */
        template <typename SubgroupType>
        derecho::rpc::QueryResults<void> dump_timestamp(const std::string& filename, const uint32_t subgroup_index, const uint32_t shard_index);

        /**
         * The object store version:
         *
         * @param filename             -   the filename version
         * @param object_pool_pathname -   the object pool version
         *
         * @return query results
         */
        template <typename SubgroupType>
        std::vector<std::unique_ptr<derecho::rpc::QueryResults<void>>> dump_timestamp(const std::string& filename, const std::string& object_pool_pathname);
        /**
         * Evaluate the ordered put performance inside a shard. Please note that those put does not involve the
         * external client data path.
         *
         * @param message_size      - the message size for the shard. TODO: we should be able to retrieve the maximum
         *                            message size from SubgroupType, subgroup_index and shard_index. How?
         * @param duration_sec      - the duration of the test in seconds.
         * @param subgroup_index    - the subgroup index
         * @param shard_index       - the shard index
         *
         * @return the value in ops.
         */
        template <typename SubgroupType>
        derecho::rpc::QueryResults<double> perf_put(const uint32_t message_size, const uint64_t duration_sec, const uint32_t subgroup_index, const uint32_t shard_index);
#endif//ENABLE_EVALUATION

        const static std::vector<std::type_index> subgroup_type_order;
        const static uint32_t invalid_subgroup_type_index;
        /**
         * Get type index
         * @return the the subgroup type index
         */
        template <typename SubgroupType>
        inline static uint32_t get_subgroup_type_index();
    };
    
    
    /**
     * configuration keys
     */
    #define CASCADE_CONTEXT_NUM_WORKERS_MULTICAST   "CASCADE/num_workers_for_multicast_ocdp"
    #define CASCADE_CONTEXT_NUM_WORKERS_P2P         "CASCADE/num_workers_for_p2p_ocdp"
    #define CASCADE_CONTEXT_CPU_CORES               "CASCADE/cpu_cores"
    #define CASCADE_CONTEXT_GPUS                    "CASCADE/gpus"
    #define CASCADE_CONTEXT_WORKER_CPU_AFFINITY     "CASCADE/worker_cpu_affinity"
    
    /**
     * A class describing the resources available in the Cascade context.
     */
    class ResourceDescriptor {
    public:
        /** cpu cores, loaded from configuration **/
        std::vector<uint32_t> cpu_cores;
        /** worker cpu aworker cpu ffinity, loaded from configuration **/
        std::map<uint32_t,std::vector<uint32_t>> multicast_ocdp_worker_to_cpu_cores;
        std::map<uint32_t,std::vector<uint32_t>> p2p_ocdp_worker_to_cpu_cores;
        /** gpu list**/
        std::vector<uint32_t> gpus;
        /** constructor **/
        ResourceDescriptor();
        /** destructor **/
        virtual ~ResourceDescriptor();
        /** dump **/
        void dump() const;
    };
   
    /**
     * The cascade context
     *
     * The cascade context manages computation resources like CPU cores, GPU, and memory. It works as the container for all
     * "off-critical" path logics. The main components of cascade context includes:
     * 1 - a thread pool for the off-critical path logics.
     * 2 - a prefix registry.
     * 3 - a bounded Action buffer.
     */
    using prefix_entry_t = 
                std::unordered_map<
                    std::string, // udl_id
                    std::tuple<
                        DataFlowGraph::VertexShardDispatcher,         // shard dispatcher
                        std::shared_ptr<OffCriticalDataPathObserver>, // ocdpo
                        std::unordered_map<std::string,bool>          // output map{prefix->bool}
                    >
                >;
    using match_results_t = std::unordered_map<std::string,prefix_entry_t>;
    template <typename... CascadeTypes>
    class CascadeContext: public ICascadeContext {
    private:
        struct action_queue {
            struct Action           action_buffer[ACTION_BUFFER_SIZE];
            std::atomic<size_t>     action_buffer_head;
            std::atomic<size_t>     action_buffer_tail;
            mutable std::mutex      action_buffer_slot_mutex;
            mutable std::mutex      action_buffer_data_mutex;
            mutable std::condition_variable action_buffer_slot_cv;
            mutable std::condition_variable action_buffer_data_cv;
            inline void initialize();
            inline void action_buffer_enqueue(Action&&);
            inline Action action_buffer_dequeue(std::atomic<bool>& is_running);
            inline void notify_all();
        };
        /** action (ring) buffer control */
        struct action_queue action_queue_for_multicast;
        struct action_queue action_queue_for_p2p;

        /** thread pool control */
        std::atomic<bool>       is_running;
        /** the prefix registries, one is active, the other is shadow 
         * prefix->{udl_id->{ocdpo,{prefix->trigger_put/put}}
         */
        std::shared_ptr<PrefixRegistry<prefix_entry_t,PATH_SEPARATOR>> prefix_registry_ptr;
        /** the data path logic loader */
        std::unique_ptr<UserDefinedLogicManager<CascadeTypes...>> user_defined_logic_manager;
        /** the off-critical data path worker thread pools */
        std::vector<std::thread> workhorses_for_multicast;
        std::vector<std::thread> workhorses_for_p2p;
        /** the service client: off critical data path logic use it to send data to a next tier. */
        std::unique_ptr<ServiceClient<CascadeTypes...>> service_client;
        /**
         * destroy the context, to be called in destructor 
         */
        void destroy();
        /**
         * off critical data path workhorse
         * @param _1 the task id, started from 0 to (OFF_CRITICAL_DATA_PATH_THREAD_POOL_SIZE-1)
         */
        void workhorse(uint32_t,struct action_queue&);
        
    public:
        /** Resources **/
        const ResourceDescriptor resource_descriptor;
        /**
         * Constructor
         */
        CascadeContext();
        /**
         * construct the resources from Derecho configuration.
         *
         * We enforce an explicit call to this initialization function to avoid heavily relying on the order of C++
         * global/static variables: CascadeContext relies on the global configuration from derecho implementation, which is
         * generally initialized with commandline parameters in main(). If we initialize the CascadeContext singleton in its
         * constructor, which happens before main(), it might miss extra configuration from commandline. Therefore,
         * CascadeContext singleton needs to be initialized in main() by calling CascadeContext::initialize(). Moreover, it
         * needs the off critical data path handler from main();
         * 
         * @param group_ptr                         The group handle
         */
        void construct(derecho::Group<CascadeMetadataService<CascadeTypes...>, CascadeTypes...>* group_ptr);
        /**
         * get the reference to encapsulated service client handle.
         * The reference is valid only after construct() is called.
         * 
         * @return a reference to service client.
         */
        ServiceClient<CascadeTypes...>& get_service_client_ref() const;
        /**
         * We give up the following on-demand loading mechanism:
         * ==============================================================================================================
         * The prefix registry management APIs
         *
         * We separate the prefix registration in two stages: preregistration and registration to support lazy loading
         * of the data path logic packages. During preregistration stage, we create an entry for the corresponding prefix
         * in the registry with an empty value. During registration stage, the prefix is filled.
         *
         * IMPORTANT: the prefix registry management API are designed for use ONLY in the critical data path. Since the
         * critical data path is a single thread, we don't use any lock for high performance. Please keep that in mind
         * and don't touch the following APIs in prefix handlers where you have access to all the CascadeContext APIs.
         *
         * - preregister_prefixes() allows batching preregistration of a set of prefixes, previous registered OCDPO will
         * be overwritten by the new prefixes.
         * - register_prefix() setup the OCDPO for the corresponding prefix. If the ocdpo_ptr is nullptr, the prefix is
         *   "preregister"ed.
         * - unregister_prefix() deletes a corresponding prefix from registry.
         * - get_prefix_handler() returns a raw pointer to the handler.
         *
         * @param prefixes  a list of vectors to pre-register.
         * @param prefix    a prefix to register.
         * @param ocdpo_ptr the data path observer, nullptr for preregistration.
         *
         * @return get_prefix_handler returns the OffCriticalDataPathObserver it holds for the corresponding prefix. If
         * the prefix is not registered, it will return nullptr.
         *
        virtual void preregister_prefixes(const std::vector<std::string>& prefixes);
        virtual void register_prefix(const std::string& prefix, const std::shared_ptr<OffCriticalDataPathObserver>& ocdpo_ptr = nullptr);
        virtual void unregister_prefix(const std::string& prefix);
        virtual OffCriticalDataPathObserver* get_prefix_handler(const std::string& prefix); 
         * =============================================================================================================
         * Now we agree on the new design that the prefix is assumed to be registered before the critical data path saw
         * some data coming. Without a lock guarding prefix registry in the critical data path, it's a little bit tricky
         * to support runtime update. 
         * 
         * IMPORTANT: Successful unregistration of a prefix does not guarantee the corresponding UDL is safe to be
         * released. Because a previous triggered off-critical data path might still working on the unregistered prefix.
         * TODO: find a mechanism to trigger safe UDL unloading.
         */

        /**
         * Register a set of prefixes
         *
         * @param prefixes              - the prefixes set
         * @param shard_dispatcher      - the shard dispatcher
         * @param user_defined_logic_id - the UDL id, presumably an UUID string
         * @param ocdpo_ptr             - the data path observer
         * @param outputs               - the outputs are a map from another prefix to put type (true for trigger put,
         *                                false for put).
         */
        virtual void register_prefixes(const std::unordered_set<std::string>& prefixes,
                                       const DataFlowGraph::VertexShardDispatcher shard_dispatcher,
                                       const std::string& user_defined_logic_id,
                                       const std::shared_ptr<OffCriticalDataPathObserver>& ocdpo_ptr,
                                       const std::unordered_map<std::string,bool>& outputs);
        /**
         * Unregister a set of prefixes
         * 
         * @param prefixes              - the prefixes set
         * @param user_defined_logic_id - the UDL id, presumably an UUID string
         * @param ocdpo_ptr             - the data path observer
         */
        virtual void unregister_prefixes(const std::unordered_set<std::string>& prefixes,
                                         const std::string& user_defined_logic_id);
        /**
         * Get the prefix handlers registered for a prefix
         *
         * @param prefix                - the prefix
         *
         * @return the unordered map of observers registered to this prefix.
         */
        virtual match_results_t get_prefix_handlers(const std::string& prefix); 
        /**
         * post an action to the Context for processing.
         *
         * @param action        The action
         * @param is_trigger    True for trigger, meaning the action will be processed in the workhorses for p2p send
         *
         * @return  true for a successful post, false for failure. The current only reason for failure is to post to a
         *          context already shut down.
         */
        virtual bool post(Action&& action, bool is_trigger = false);

        /**
         * Get the action queue length
         *
         * @return current queue_length
         */
        virtual size_t action_queue_length_p2p();
        virtual size_t action_queue_length_multicast();

        /**
         * Destructor
         */
        virtual ~CascadeContext();
    };
} // cascade
} // derecho

#include "detail/service_impl.hpp"
