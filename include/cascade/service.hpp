#pragma once
/**
 * @file    service.hpp
 *
 * @brief   This file includes the cascade service templates
 *
 * Type neutral templates components go here. Since the server binary and client library has to be type aware (because
 * they are pre-compiled), we separate the api and implementation of them in type-aware header files as follows:
 * - service_types.hpp contains the predefined types for derecho Subgroups, which are specialized from
 *   derecho::cascade::VolatileCascadeStore/PersistentCascadeStore templates, and the server API definition,
 *   which is a specialization of the Service template.
 * - service_client_api.hpp contains the client API definition, which is a specialization of ServiceClient.
 */

#include "cascade.hpp"
#include "data_flow_graph.hpp"
#include "detail/prefix_registry.hpp"
#include "object_pool_metadata.hpp"
#include "persistence_observer.hpp"
#include "user_defined_logic_manager.hpp"
#include "utils.hpp"

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <derecho/conf/conf.hpp>
#include <derecho/core/notification.hpp>
#include <derecho/persistent/PersistentInterface.hpp>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <thread>
#include <tuple>
#include <typeinfo>
#include <unordered_map>
#include <unordered_set>
#include <utility>

namespace derecho {
namespace cascade {
    /**
     * @fn constexpr bool have_same_object_type()
     * @tparam  CascadeType     Cascade Type
     * @return  true if CascadeType(s) has the same ObjectType, otherwise false.
     */
    template <typename CascadeType>
    constexpr bool have_same_object_type() {
        return true;
    }

    /**
     * @fn constexpr bool have_same_object_type()
     * @tparam  FirstCascadeType
     * @tparam  SecondCascadeType
     * @tparam  RestCascadeTypes
     * @return  true if CascadeType(s) has the same ObjectType, otherwise false.
     */
    template <typename FirstCascadeType, typename SecondCascadeType, typename ... RestCascadeTypes>
    constexpr bool have_same_object_type() {
        return std::is_same<typename FirstCascadeType::ObjectType, typename SecondCascadeType::ObjectType>::value &&
               have_same_object_type<SecondCascadeType,RestCascadeTypes...>();
    }

    /** Cascade Factory type*/
    template <typename CascadeType>
    using Factory = std::function<std::unique_ptr<CascadeType>(persistent::PersistentRegistry*, subgroup_id_t subgroup_id, ICascadeContext*)>;

    /* Cascade Metadata Service type*/
    template<typename...CascadeTypes>
    using CascadeMetadataService = PersistentCascadeStore<
        std::remove_cv_t<std::remove_reference_t<decltype(std::declval<ObjectPoolMetadata<CascadeTypes...>>().get_key_ref())>>,
        ObjectPoolMetadata<CascadeTypes...>,
        &ObjectPoolMetadata<CascadeTypes...>::IK,
        &ObjectPoolMetadata<CascadeTypes...>::IV,
        persistent::ST_FILE>;
#define METADATA_SERVICE_SUBGROUP_INDEX (0)


    /* The cascade execution engine to be defined later */
    template <typename... CascadeTypes>
    class ExecutionEngine;

    /**
     * The off-critical data path handler API
     */
    class OffCriticalDataPathObserver: public derecho::DeserializationContext {
    public:
        /**
         * This function has to be re-entrant/thread-safe.
         * @param[in] sender            The sender id
         * @param[in] full_key_string   The full key string
         * @param[in] prefix_length     The matching prefix length key_string.subtring(0,prefix_length) returns the prefix.
         *                          Please note that the trailing '/' is included.
         * @param[in] version           The version of the key
         * @param[in] value_ptr         The raw value pointer
         * @param[in] outputs           The object pool output should go
         * @param[in] ctxt              The CascadeContext
         * @param[in] worker_id         The off critical data path worker id.
         */
        virtual void operator() (const node_id_t sender,
                                 const std::string& full_key_string,
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
     * handler has all the information to process in the worker thread. It is important to avoid unnecessary copies
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
#define ACTION_BUFFER_SIZE          (8192)
// #define ACTION_BUFFER_SIZE          (1024)
    struct Action {
        node_id_t                       sender;
        std::string                     key_string;
        uint32_t                        prefix_length;
        persistent::version_t           version;
        std::shared_ptr<OffCriticalDataPathObserver>   ocdpo_ptr;
        std::shared_ptr<mutils::ByteRepresentable>     value_ptr;
        std::unordered_map<std::string,bool>           outputs;
        /**
         * Move constructor
         * @param[in] other     The input Action object
         */
        Action(Action&& other):
            sender(other.sender),
            key_string(other.key_string),
            prefix_length(other.prefix_length),
            version(other.version),
            ocdpo_ptr(std::move(other.ocdpo_ptr)),
            value_ptr(std::move(other.value_ptr)),
            outputs(std::move(other.outputs)) {}
        /**
         * Constructor
         * @param[in]   _sender
         * @param[in]   _key_string
         * @param[in]   _prefix_length
         * @param[in]   _version
         * @param[in]   _ocdpo_ptr const reference rvalue
         * @param[in]   _value_ptr
         * @param[in]   _outputs
         */
        Action(const node_id_t              _sender = INVALID_NODE_ID,
               const std::string&           _key_string = "",
               const uint32_t               _prefix_length = 0,
               const persistent::version_t& _version = CURRENT_VERSION,
               const std::shared_ptr<OffCriticalDataPathObserver>&  _ocdpo_ptr = nullptr,
               const std::shared_ptr<mutils::ByteRepresentable>&    _value_ptr = nullptr,
               const std::unordered_map<std::string,bool>           _outputs = {}):
            sender(_sender),
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
         *  @param[in] ctxt
         *  @param[in] worker_id
         */
        inline void fire(ICascadeContext* ctxt,uint32_t worker_id) {
            if (value_ptr && ocdpo_ptr) {
                TimestampLogger::log(TLT_ACTION_FIRE_START,
                                     0,
                                     dynamic_cast<const IHasMessageID*>(value_ptr.get())->get_message_id(),
                                     0);
                dbg_default_trace("In {}: [worker_id={}] action is fired.", __PRETTY_FUNCTION__, worker_id);
                (*ocdpo_ptr)(sender,key_string,prefix_length,version,value_ptr.get(),outputs,ctxt,worker_id);
            }
        }
        inline explicit operator bool() const {
            return (bool)value_ptr;
        }
    };

    inline std::ostream& operator << (std::ostream& out, const Action& action) {
        out << "Action:\n"
            << "\tsender = " << action.sender << "\n"
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

        static_assert(have_same_object_type<CascadeTypes...>());

        /**
         * Constructor
         * The constructor will load the configuration, start the service thread.
         * Constructor is hidden for singleton.
         *
         * @param[in] dsms deserialization managers
         * @param[in] metadata_service_factory
         * @param[in] factories: subgroup factories.
         */
        Service(const std::vector<DeserializationContext*>& dsms,
                derecho::cascade::Factory<CascadeMetadataService<CascadeTypes...>> metadata_service_factory,
                derecho::cascade::Factory<CascadeTypes>... factories);

    public:
        /**
         * The virtual Service destructor.
         */
        virtual ~Service();
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
        std::unique_ptr<ExecutionEngine<CascadeTypes...>> context;
        /**
         * Singleton pointer
         */
        static std::unique_ptr<Service<CascadeTypes...>> service_ptr;

    public:
        /**
         * Start the singleton service
         * Please make sure only one thread call start. We do not defense such an incorrect usage.
         *
         * @param[in] dsms
         * @param[in] metadata_factory - factory for the metadata service.
         * @param[in] factories - the factories to create objects.
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
        /**
         * Gets a pointer to the CascadeContext object for the singleton service, assuming
         * the service has been started. The CascadeContext can be used to interact with
         * the service by invoking requests as a client.
         *
         * @return A non-owning raw pointer to the CascadeContext owned by the singleton
         * service, or a null pointer if the service has not been started.
         */
        static CascadeContext<CascadeTypes...>* get_context();
    };

    /**
     * Options for the policy the ServiceClient will use to select which member of a shard to
     * communicate with when executing a put() or get() operation.
     */
    enum ShardMemberSelectionPolicy {
        FirstMember,    // use the first member in the list returned from get_shard_members()
        LastMember,     // use the last member in the list returned from get_shard_members()
        Random,         // use a random member in the shard for each operations(put/remove/get/get_by_time).
        FixedRandom,    // use a random member and stick to that for the following operations.
        RoundRobin,     // use a member in round-robin order.
        KeyHashing,     // use the key's hashing
        UserSpecified,  // user specify which member to contact.
        InvalidPolicy = -1
    };
    #define DEFAULT_SHARD_MEMBER_SELECTION_POLICY (ShardMemberSelectionPolicy::RoundRobin)

    std::ostream& operator<<(std::ostream& stream, const ShardMemberSelectionPolicy& policy);

    template <typename T> struct do_hash {};

    template <> struct do_hash<std::tuple<std::type_index,uint32_t,uint32_t>> {
        size_t operator()(const std::tuple<std::type_index,uint32_t,uint32_t>& t) const {
            return static_cast<size_t>(std::get<0>(t).hash_code() ^ ((std::get<1>(t)<<16) | std::get<2>(t)));
        }
    };

    /* Forward declaration of the ServiceClient, to be defined in service_client.hpp */
    template <typename... CascadeTypes>
    class ServiceClient;

    /**
     * configuration keys
     */
    static constexpr const char* CASCADE_CONTEXT_NUM_STATELESS_WORKERS_MULTICAST = "CASCADE/num_stateless_workers_for_multicast_ocdp";
    static constexpr const char* CASCADE_CONTEXT_NUM_STATELESS_WORKERS_P2P       = "CASCADE/num_stateless_workers_for_p2p_ocdp";
    static constexpr const char* CASCADE_CONTEXT_NUM_STATEFUL_WORKERS_MULTICAST  = "CASCADE/num_stateful_workers_for_multicast_ocdp";
    static constexpr const char* CASCADE_CONTEXT_NUM_STATEFUL_WORKERS_P2P        = "CASCADE/num_stateful_workers_for_p2p_ocdp";
    static constexpr const char* CASCADE_CONTEXT_CPU_CORES                       = "CASCADE/cpu_cores";
    static constexpr const char* CASCADE_CONTEXT_GPUS                            = "CASCADE/gpus";
    static constexpr const char* CASCADE_CONTEXT_WORKER_CPU_AFFINITY             = "CASCADE/worker_cpu_affinity";

    /**
     * A class describing the resources available in the Cascade context.
     */
    class ResourceDescriptor {
    public:
        /** cpu cores, loaded from configuration **/
        std::vector<uint32_t> cpu_cores;
        /** worker cpu affinity, loaded from configuration **/
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
     * @struct prefix_ocdpo_info_t
     * @brief   This is the information to live in the prefix tree.
     */
    using prefix_ocdpo_info_t = struct _prefix_ocdpo_info {
        std::string     udl_id;
        std::string     config_string;
        DataFlowGraph::VertexExecutionEnvironment       execution_environment;
        DataFlowGraph::VertexShardDispatcher            shard_dispatcher;
        DataFlowGraph::Statefulness                     statefulness;
        DataFlowGraph::VertexHook                       hook;
        std::shared_ptr<OffCriticalDataPathObserver>    ocdpo;
        std::unordered_map<std::string,bool>            output_map;
    };

    struct PrefixOCDPOInfoHash {
        size_t operator() (const prefix_ocdpo_info_t& info) const {
            return std::hash<std::string>{}(info.udl_id + info.config_string);
        }
    };

    struct PrefixOCDPOInfoCompare {
        bool operator() (const prefix_ocdpo_info_t& l, const prefix_ocdpo_info_t& r) const {
            return (l.udl_id == r.udl_id) &&
                   (l.config_string == r.config_string) &&
                   (l.execution_environment == r.execution_environment);
        }
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
    template <typename... CascadeTypes>
    class CascadeContext:public ICascadeContext {
    public:
        /**
         * get the reference to encapsulated service client handle.
         * The reference is valid only after construct() is called.
         *
         * @return a reference to service client.
         */
        virtual ServiceClient<CascadeTypes...>& get_service_client_ref() const = 0;
    };

    using prefix_ocdpo_info_set_t = std::unordered_set<prefix_ocdpo_info_t,PrefixOCDPOInfoHash,PrefixOCDPOInfoCompare>;
    using prefix_entry_t = std::unordered_map<
                                std::string, // dfg_id
                                prefix_ocdpo_info_set_t
                           >;
    using match_results_t = std::unordered_map<std::string,prefix_entry_t>;

    template <typename... CascadeTypes>
    class ExecutionEngine: public CascadeContext<CascadeTypes...> {
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
        std::vector<std::unique_ptr<struct action_queue>> stateful_action_queues_for_multicast;
        std::vector<std::unique_ptr<struct action_queue>> stateful_action_queues_for_p2p;
        struct action_queue single_threaded_action_queue_for_multicast;
        struct action_queue single_threaded_action_queue_for_p2p;
        struct action_queue stateless_action_queue_for_multicast;
        struct action_queue stateless_action_queue_for_p2p;

        /** thread pool control */
        std::atomic<bool>       is_running;
        /** the prefix registries, one is active, the other is shadow
         * prefix->{udl_id->{ocdpo,{prefix->trigger_put/put}}
         */
        std::shared_ptr<PrefixRegistry<prefix_entry_t,PATH_SEPARATOR>> prefix_registry_ptr;
        /** the data path logic loader */
        std::unique_ptr<UserDefinedLogicManager<CascadeTypes...>> user_defined_logic_manager;
        /** the off-critical data path worker thread pools */
        std::vector<std::thread> stateless_workhorses_for_multicast;
        std::vector<std::thread> stateless_workhorses_for_p2p;
        std::vector<std::thread> stateful_workhorses_for_multicast;
        std::vector<std::thread> stateful_workhorses_for_p2p;
        std::thread              single_threaded_workhorse_for_multicast;
        std::thread              single_threaded_workhorse_for_p2p;
        /** The persistence observer: lets CascadeStore objects register actions to fire when their data finishes persisting */
        std::unique_ptr<PersistenceObserver> persistence_observer;
        /**
         * destroy the context, to be called in destructor
         */
        void destroy();
        /**
         * off critical data path workhorse
         * @param[in] _1 The task id, started from 0 to (OFF_CRITICAL_DATA_PATH_THREAD_POOL_SIZE-1)
         * @param[in] _2 The action queue
         */
        void workhorse(uint32_t,struct action_queue&);

    public:
        /** Resources **/
        const ResourceDescriptor resource_descriptor;
        /**
         * Constructor
         */
        ExecutionEngine();
        /**
         * construct the resources from Derecho configuration.
         *
         * We enforce an explicit call to this initialization function to avoid heavily relying on the order of C++
         * global/static variables: CascadeContext relies on the global configuration from derecho implementation, which is
         * generally initialized with commandline parameters in main(). If we initialize the CascadeContext singleton in its
         * constructor, which happens before main(), it might miss extra configuration from commandline. Therefore,
         * CascadeContext singleton needs to be initialized in main() by calling CascadeContext::construct(). Moreover, it
         * needs the off critical data path handler from main();
         */
        void construct();
        /**
         * get the reference to encapsulated service client handle.
         * The reference is valid only after construct() is called.
         *
         * @return a reference to service client.
         */
        virtual ServiceClient<CascadeTypes...>& get_service_client_ref() const;

        /**
         * Gets a reference to the PersistenceObserver stored in the CascadeContext.
         * Cascade functions can use the PersistenceObserver to trigger actions when a
         * particular version of an object has finished persisting. This obviously won't
         * work on VolatileCascadeStore (or any other Cascade type that doesn't persist
         * its state).
         *
         * @return PersistenceObserver& A reference to a PersistenceObserver.
         */
        virtual PersistenceObserver& get_persistence_observer() const override;
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
         * Register a ocdpo of a given application designated by dfg uuid to a set of prefixes
         *
         * @param[in] dfg_uuid              - the dfg uuid
         * @param[in] prefixes              - the prefixes set
         * @param[in] shard_dispatcher      - the shard dispatcher
         * @param[in] execution_environment - the execution environment
         * @param[in] execution_environment_conf - the execution environment configuration
         * @param[in] stateful              - register a stateful udl
         * @param[in] hook                  - the hook for this ocdpo
         * @param[in] user_defined_logic_id - the UDL id, presumably an UUID string
         * @param[in] user_defined_logic_config
         *                              - the UDL configuration.
         * @param[in] ocdpo_ptr             - the data path observer
         * @param[in] outputs               - the outputs are a map from another prefix to put type (true for trigger put,
         *                                false for put).
         */
        virtual void register_prefixes(const std::string& dfg_uuid,
                                       const std::unordered_set<std::string>& prefixes,
                                       const DataFlowGraph::VertexShardDispatcher shard_dispatcher,
                                       const DataFlowGraph::VertexExecutionEnvironment execution_environment,
                                       const std::string& execution_environment_conf,
                                       const DataFlowGraph::Statefulness stateful,
                                       const DataFlowGraph::VertexHook hook,
                                       const std::string& user_defined_logic_id,
                                       const std::string& user_defined_logic_config,
                                       const std::shared_ptr<OffCriticalDataPathObserver>& ocdpo_ptr,
                                       const std::unordered_map<std::string,bool>& outputs);
        /**
         * Unregister all prefixes of an application
         *
         * @param[in] dfg_uuid              - the uuid of the dfg
         */
        virtual void unregister_prefixes(const std::string& dfg_uuid);
        /**
         * Get the prefix handlers registered for a prefix
         *
         * @param[in] prefix                - the prefix
         *
         * @return the unordered map of observers registered to this prefix.
         */
        virtual match_results_t get_prefix_handlers(const std::string& prefix);

        /**
         * post an action to the Context for processing.
         *
         * @param[in] action        The action
         * @param[in] stateful      If the action is stateful|stateless|singlethreaded
         * @param[in] is_trigger    True for trigger, meaning the action will be processed in the workhorses for p2p send
         *
         * @return  true for a successful post, false for failure. The current only reason for failure is to post to a
         *          context already shut down.
         */
        virtual bool post(Action&& action, DataFlowGraph::Statefulness stateful, bool is_trigger);

        /**
         * Get the stateless action queue length
         *
         * @return current queue_length
         */
        virtual size_t stateless_action_queue_length_p2p();
        virtual size_t stateless_action_queue_length_multicast();

        /**
         * Destructor
         */
        virtual ~ExecutionEngine();
    };//ExecutionEngine/
} // cascade
} // derecho

// Formatter boilerplate for the spdlog library
template <>
struct fmt::formatter<derecho::cascade::ShardMemberSelectionPolicy> : fmt::ostream_formatter {};

#include "detail/service_impl.hpp"
