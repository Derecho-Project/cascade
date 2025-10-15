#include <cascade/config.h>
#include <cascade/data_flow_graph.hpp>
#include <cascade/service_client.hpp>
#include <cascade/utils.hpp>
#include <chrono>
#include <derecho/core/derecho.hpp>
#include <string>
#include <thread>
#include <unordered_map>
#include <variant>
#include <vector>

using namespace std::chrono_literals;

#if __GLIBC__ == 2 && __GLIBC_MINOR__ < 30
#include <sys/syscall.h>
#define gettid() syscall(SYS_gettid)
#endif

namespace derecho{
namespace cascade{

using derecho::SubgroupAllocationPolicy;
using derecho::CrossProductPolicy;
using derecho::ShardAllocationPolicy;

template <typename CascadeType>
derecho::Factory<CascadeType> factory_wrapper(ICascadeContext* context_ptr, derecho::cascade::Factory<CascadeType> cascade_factory) {
    return [context_ptr,cascade_factory](persistent::PersistentRegistry *pr, subgroup_id_t subgroup_id) {
            return cascade_factory(pr,subgroup_id,context_ptr);
        };
}

template <typename... CascadeTypes>
Service<CascadeTypes...>::Service(const std::vector<DeserializationContext*>& dsms,
                                  derecho::cascade::Factory<CascadeMetadataService<CascadeTypes...>> metadata_service_factory,
                                  derecho::cascade::Factory<CascadeTypes>... factories) {
    // STEP 1 - load configuration
    derecho::SubgroupInfo si{derecho::make_subgroup_allocator<CascadeMetadataService<CascadeTypes...>,CascadeTypes...>()};
    // STEP 2 - setup cascade context
    context = std::make_unique<ExecutionEngine<CascadeTypes...>>();
    std::vector<DeserializationContext*> new_dsms(dsms);
    new_dsms.emplace_back(context.get());
    // STEP 3 - create derecho group
    group = std::make_unique<derecho::Group<CascadeMetadataService<CascadeTypes...>, CascadeTypes...>>(
            UserMessageCallbacks{
                    nullptr,
                    [this](subgroup_id_t subgroup_id, persistent::version_t version) {
                        context->get_persistence_observer().derecho_local_persistence_callback(subgroup_id, version);
                    },
                    // persistent
                    [this](subgroup_id_t subgroup_id, persistent::version_t version) {
#ifdef ENABLE_EVALUATION
                        TimestampLogger::log(TLT_PERSISTED, group->get_my_id(), 0, version);
#endif
                        context->get_persistence_observer().derecho_global_persistence_callback(subgroup_id, version);
                    },
                    nullptr},
            si,
            new_dsms,
            std::vector<derecho::view_upcall_t>{},
            factory_wrapper(context.get(), metadata_service_factory),
            factory_wrapper(context.get(), factories)...);
    dbg_default_trace("joined group.");
    // STEP 4 - construct context
    ServiceClient<CascadeTypes...>::initialize(group.get());
    context->construct();
    // STEP 5 - create service thread
    this->_is_running = true;
    service_thread = std::thread(&Service<CascadeTypes...>::run, this);
    dbg_default_trace("created daemon thread.");
}

template <typename... CascadeTypes>
Service<CascadeTypes...>::~Service() {
    dbg_default_trace("{}:{} Service destructor is called.", __FILE__,__LINE__);
}

template <typename... CascadeTypes>
void Service<CascadeTypes...>::run() {
    std::unique_lock<std::mutex> lck(this->service_control_mutex);
    this->service_control_cv.wait(lck, [this](){return !this->_is_running;});
    // stop gracefully
    group->barrier_sync();
    group->leave();
}

template <typename... CascadeTypes>
void Service<CascadeTypes...>::stop(bool is_joining) {
    std::unique_lock<std::mutex> lck(this->service_control_mutex);
    this->_is_running = false;
    lck.unlock();
    this->service_control_cv.notify_one();
    // wait until stopped.
    if (is_joining && this->service_thread.joinable()) {
        this->service_thread.join();
    }
}

template <typename... CascadeTypes>
void Service<CascadeTypes...>::join() {
    if (this->service_thread.joinable()) {
        this->service_thread.join();
    }
}

template <typename... CascadeTypes>
bool Service<CascadeTypes...>::is_running() {
    std::lock_guard<std::mutex> lck(this->service_control_mutex);
    return _is_running;
}

#ifndef __WITHOUT_SERVICE_SINGLETONS__
template <typename... CascadeTypes>
std::unique_ptr<Service<CascadeTypes...>> Service<CascadeTypes...>::service_ptr;

template <typename... CascadeTypes>
void Service<CascadeTypes...>::start(const std::vector<DeserializationContext*>& dsms,
        derecho::cascade::Factory<CascadeMetadataService<CascadeTypes...>> metadata_factory,
        derecho::cascade::Factory<CascadeTypes>... factories) {
    if (!service_ptr) {
        service_ptr = std::unique_ptr<Service<CascadeTypes...>>(new Service<CascadeTypes...>(dsms, metadata_factory, factories...));
    }
}

template <typename... CascadeTypes>
void Service<CascadeTypes...>::shutdown(bool is_joining) {
    if (service_ptr) {
        if (service_ptr->is_running()) {
            service_ptr->stop(is_joining);
        }
    }
}

template <typename... CascadeTypes>
void Service<CascadeTypes...>::wait() {
    if (service_ptr) {
        service_ptr->join();
    }
    service_ptr.reset();
}
#endif//__WITHOUT_SERVICE_SINGLETONS__

template <typename... CascadeTypes>
CascadeContext<CascadeTypes...>* Service<CascadeTypes...>::get_context() {
    if (service_ptr) {
        return service_ptr->context.get();
    } else {
        return nullptr;
    }
}


template <typename... CascadeTypes>
ExecutionEngine<CascadeTypes...>::ExecutionEngine()
        : persistence_observer(std::make_unique<PersistenceObserver>()) {
    stateless_action_queue_for_multicast.initialize();
    stateless_action_queue_for_p2p.initialize();
    prefix_registry_ptr = std::make_shared<PrefixRegistry<prefix_entry_t, PATH_SEPARATOR>>();
}

template <typename... CascadeTypes>
void ExecutionEngine<CascadeTypes...>::construct() {
    // 1 - create data path logic loader and register the prefixes. Ideally, this part should be done in the control
    // plane, where a centralized controller should issue the control messages to do load/unload.
    // TODO: implement the control plane.
    user_defined_logic_manager = UserDefinedLogicManager<CascadeTypes...>::create(this);
    auto dfgs = DataFlowGraph::get_data_flow_graphs();
    for (auto& dfg:dfgs) {
        for (auto& vertex:dfg.vertices) {
            for (uint32_t i=0; i<vertex.second.uuids.size(); i++) {
                if (vertex.second.execution_environment[i] == DataFlowGraph::VertexExecutionEnvironment::PTHREAD) {
                    // runs inside cascade address space: less secure but faster.
                    register_prefixes(
                        dfg.id,
                        {vertex.second.pathname},
                        vertex.second.shard_dispatchers[i],
                        vertex.second.execution_environment[i],
                        vertex.second.execution_environment_conf[i].dump(),
                        vertex.second.stateful[i],
                        vertex.second.hooks[i],
                        vertex.second.uuids[i],
                        vertex.second.configurations[i].dump(),
                        user_defined_logic_manager->get_observer(
                            vertex.second.uuids[i],
                            vertex.second.configurations[i]),
                        vertex.second.edges[i]);
                } else {
#ifdef ENABLE_MPROC
                    // runs inside a different address space: with a little overhead but more secure.
                    // TODO: hardwired UUID for prototyping. Use udl packaing/manager later.
                    register_prefixes(
                        dfg.id,
                        {vertex.second.pathname},
                        vertex.second.shard_dispatchers[i],
                        vertex.second.execution_environment[i],
                        vertex.second.execution_environment_conf[i].dump(),
                        vertex.second.stateful[i],
                        vertex.second.hooks[i],
                        "fb6458a8-60cb-11ee-b058-0242ac110003", //vertex.second.uuids[i],
                        vertex.second.configurations[i].dump(),
                        user_defined_logic_manager->get_observer(
                            "fb6458a8-60cb-11ee-b058-0242ac110003",
                            vertex.second.configurations[i]),
                        vertex.second.edges[i]);
#else
                    throw derecho_exception("MPROC is disabled, which is required by execution environment other than PTHREAD");
#endif
                }
            }
        }
    }
    // 2 - start the working threads
    is_running.store(true);
    uint32_t num_stateless_multicast_workers = 0;
    uint32_t num_stateless_p2p_workers = 0;
    // 2.1 - initialize stateless multicast workers.
    if (derecho::hasCustomizedConfKey(CASCADE_CONTEXT_NUM_STATELESS_WORKERS_MULTICAST) == false) {
        dbg_default_error("{} is not found, using 0...fix it, or posting to multicast off critical data path causes deadlock.", CASCADE_CONTEXT_NUM_STATELESS_WORKERS_MULTICAST);
    } else {
        num_stateless_multicast_workers = derecho::getConfUInt32(CASCADE_CONTEXT_NUM_STATELESS_WORKERS_MULTICAST);
    }
    for (uint32_t i=0;i<num_stateless_multicast_workers;i++) {
        // off_critical_data_path_thread_pool.emplace_back(std::thread(&ExecutionEngine<CascadeTypes...>::workhorse,this,i));
        stateless_workhorses_for_multicast.emplace_back(
            [this,i](){
                // set cpu affinity
                if (this->resource_descriptor.multicast_ocdp_worker_to_cpu_cores.find(i)!=
                    this->resource_descriptor.multicast_ocdp_worker_to_cpu_cores.end()) {
                    cpu_set_t cpuset{};
                    CPU_ZERO(&cpuset);
                    for (auto core: this->resource_descriptor.multicast_ocdp_worker_to_cpu_cores.at(i)) {
                        CPU_SET(core,&cpuset);
                    }
                    if(pthread_setaffinity_np(pthread_self(),sizeof(cpuset),&cpuset)!=0) {
                        dbg_default_warn("Failed to set affinity for cascade worker-{}", i);
                    }
                }
                // call workhorse
                this->workhorse(i,stateless_action_queue_for_multicast);
            });
    }
    // 2.2 -initialize stateless p2p workers.
    if (derecho::hasCustomizedConfKey(CASCADE_CONTEXT_NUM_STATELESS_WORKERS_P2P) == false) {
        dbg_default_error("{} is not found, using 0...fix it, or posting to multicast off critical data path causes deadlock.", CASCADE_CONTEXT_NUM_STATELESS_WORKERS_P2P);
    } else {
        num_stateless_p2p_workers = derecho::getConfUInt32(CASCADE_CONTEXT_NUM_STATELESS_WORKERS_P2P);
    }
    for (uint32_t i=0;i<num_stateless_p2p_workers;i++) {
        // off_critical_data_path_thread_pool.emplace_back(std::thread(&ExecutionEngine<CascadeTypes...>::workhorse,this,i));
        stateless_workhorses_for_p2p.emplace_back(
            [this,i](){
                // set cpu affinity
                if (this->resource_descriptor.p2p_ocdp_worker_to_cpu_cores.find(i)!=
                    this->resource_descriptor.p2p_ocdp_worker_to_cpu_cores.end()) {
                    cpu_set_t cpuset{};
                    CPU_ZERO(&cpuset);
                    for (auto core: this->resource_descriptor.p2p_ocdp_worker_to_cpu_cores.at(i)) {
                        CPU_SET(core,&cpuset);
                    }
                    if(pthread_setaffinity_np(pthread_self(),sizeof(cpuset),&cpuset)!=0) {
                        dbg_default_warn("Failed to set affinity for cascade worker-{}", i);
                    }
                }
                // call workhorse
                this->workhorse(i,stateless_action_queue_for_p2p);
            });
    }
    uint32_t num_stateful_multicast_workers = 0;
    uint32_t num_stateful_p2p_workers = 0;
    // 2.3 - initialize stateful multicast workers
    if (derecho::hasCustomizedConfKey(CASCADE_CONTEXT_NUM_STATEFUL_WORKERS_MULTICAST) == false) {
        dbg_default_error("{} is not found, using 0...fix it, or posting to multicast off critical data path causes deadlock.", CASCADE_CONTEXT_NUM_STATEFUL_WORKERS_MULTICAST);
    } else {
        num_stateful_multicast_workers = derecho::getConfUInt32(CASCADE_CONTEXT_NUM_STATEFUL_WORKERS_MULTICAST);
    }
    stateful_action_queues_for_multicast.resize(num_stateful_multicast_workers);
    for (uint32_t i=0;i<num_stateful_multicast_workers;i++) {
        // initialize local queue
        stateful_action_queues_for_multicast[i] = std::make_unique<struct action_queue>();
        stateful_action_queues_for_multicast.at(i)->initialize();
        stateful_workhorses_for_multicast.emplace_back(
            [this,i](){
                // set cpu affinity
                if (this->resource_descriptor.multicast_ocdp_worker_to_cpu_cores.find(i)!=
                    this->resource_descriptor.multicast_ocdp_worker_to_cpu_cores.end()) {
                    cpu_set_t cpuset{};
                    CPU_ZERO(&cpuset);
                    for (auto core: this->resource_descriptor.multicast_ocdp_worker_to_cpu_cores.at(i)) {
                        CPU_SET(core,&cpuset);
                    }
                    if(pthread_setaffinity_np(pthread_self(),sizeof(cpuset),&cpuset)!=0) {
                        dbg_default_warn("Failed to set affinity for cascade worker-{}", i);
                    }
                }
                // call workhorse
                this->workhorse(i,*stateful_action_queues_for_multicast.at(i));
            });
    }
    // 2.4 - initialize stateful p2p workers
    if (derecho::hasCustomizedConfKey(CASCADE_CONTEXT_NUM_STATEFUL_WORKERS_P2P) == false) {
        dbg_default_error("{} is not found, using 0...fix it, or posting to multicast off critical data path causes deadlock.", CASCADE_CONTEXT_NUM_STATEFUL_WORKERS_P2P);
    } else {
        num_stateful_p2p_workers = derecho::getConfUInt32(CASCADE_CONTEXT_NUM_STATEFUL_WORKERS_P2P);
    }
    stateful_action_queues_for_p2p.resize(num_stateful_p2p_workers);
    for (uint32_t i=0;i<num_stateful_p2p_workers;i++) {
        // initialize local queue
        stateful_action_queues_for_p2p[i] = std::make_unique<struct action_queue>();
        stateful_action_queues_for_p2p.at(i)->initialize();
        stateful_workhorses_for_p2p.emplace_back(
            [this,i](){
                // set cpu affinity
                if (this->resource_descriptor.p2p_ocdp_worker_to_cpu_cores.find(i)!=
                    this->resource_descriptor.p2p_ocdp_worker_to_cpu_cores.end()) {
                    cpu_set_t cpuset{};
                    CPU_ZERO(&cpuset);
                    for (auto core: this->resource_descriptor.p2p_ocdp_worker_to_cpu_cores.at(i)) {
                        CPU_SET(core,&cpuset);
                    }
                    if(pthread_setaffinity_np(pthread_self(),sizeof(cpuset),&cpuset)!=0) {
                        dbg_default_warn("Failed to set affinity for cascade worker-{}", i);
                    }
                }
                // call workhorse
                this->workhorse(i,*stateful_action_queues_for_p2p.at(i));
            });
    }
    // 2.5 - initialize single threaded workers
    single_threaded_action_queue_for_multicast.initialize();
    single_threaded_action_queue_for_p2p.initialize();
    single_threaded_workhorse_for_multicast = std::thread(
            [this](){
                // TODO:set cpu affinity
                // call workhorse
                // worker id 0xFFFFFFFF is reserved for single thread
                this->workhorse(0xFFFFFFFF,single_threaded_action_queue_for_multicast);
            });
    single_threaded_workhorse_for_p2p = std::thread(
            [this](){
                // TODO:set cpu affinity
                // call workhorse
                // worker id 0xFFFFFFFF is reserved for single thread
                this->workhorse(0xFFFFFFFF,single_threaded_action_queue_for_p2p);
            });
}

template <typename... CascadeTypes>
void ExecutionEngine<CascadeTypes...>::workhorse(uint32_t worker_id, struct action_queue& aq) {
    pthread_setname_np(pthread_self(), ("cs_ctxt_t" + std::to_string(worker_id)).c_str());
    dbg_default_trace("Cascade context workhorse[{}] started", worker_id);
    while(is_running) {
        // waiting for an action
        Action action = aq.action_buffer_dequeue(is_running);
        // if action_buffer_dequeue return with is_running == false, value_ptr is invalid(nullptr).
        action.fire(this,worker_id);

        if (!is_running) {
            do {
                action = std::move(aq.action_buffer_dequeue(is_running));
                if (!action) break; // end of queue
                action.fire(this,worker_id);
            } while(true);
        }
    }
    dbg_default_trace("Cascade context workhorse[{}] finished normally.", static_cast<uint64_t>(gettid()));
}

template <typename... CascadeTypes>
void ExecutionEngine<CascadeTypes...>::action_queue::initialize() {
    action_buffer_head.store(0);
    action_buffer_tail.store(0);
}
#define ACTION_BUFFER_IS_FULL   ((action_buffer_head) == ((action_buffer_tail+1)%ACTION_BUFFER_SIZE))
#define ACTION_BUFFER_IS_EMPTY  ((action_buffer_head) == (action_buffer_tail))
#define ACTION_BUFFER_DEQUEUE   ((action_buffer_head) = (action_buffer_head+1)%ACTION_BUFFER_SIZE)
#define ACTION_BUFFER_ENQUEUE   ((action_buffer_tail) = (action_buffer_tail+1)%ACTION_BUFFER_SIZE)
#define ACTION_BUFFER_HEAD      (action_buffer[action_buffer_head])
#define ACTION_BUFFER_NEXT_TAIL (action_buffer[(action_buffer_tail)%ACTION_BUFFER_SIZE])

/* There is only one thread that enqueues. */
template <typename... CascadeTypes>
void ExecutionEngine<CascadeTypes...>::action_queue::action_buffer_enqueue(Action&& action) {
    std::unique_lock<std::mutex> lck(action_buffer_slot_mutex);
    while (ACTION_BUFFER_IS_FULL) {
        dbg_default_warn("In {}: Critical data path waits for 10 ms. The action buffer is full! You are sending too fast or the UDL workers are too slow. This can cause a soft deadlock.", __PRETTY_FUNCTION__);
        action_buffer_slot_cv.wait_for(lck,10ms,[this]{return !ACTION_BUFFER_IS_EMPTY;});
    }

    ACTION_BUFFER_NEXT_TAIL = std::move(action);
    ACTION_BUFFER_ENQUEUE;
    action_buffer_data_cv.notify_one();
}

/* All worker threads dequeues. */
template <typename... CascadeTypes>
Action ExecutionEngine<CascadeTypes...>::action_queue::action_buffer_dequeue(std::atomic<bool>& is_running) {
    std::unique_lock<std::mutex> lck(action_buffer_data_mutex);
    while (ACTION_BUFFER_IS_EMPTY && is_running) {
        action_buffer_data_cv.wait_for(lck,10ms,[this,&is_running]{return (!ACTION_BUFFER_IS_EMPTY) || (!is_running);});
    }

    Action ret;
    if (!ACTION_BUFFER_IS_EMPTY) {
        ret = std::move(ACTION_BUFFER_HEAD);
        ACTION_BUFFER_DEQUEUE;
        action_buffer_slot_cv.notify_one();
    }

    return ret;
}

/* shutdown the action buffer */
template <typename... CascadeTypes>
void ExecutionEngine<CascadeTypes...>::action_queue::notify_all() {
    action_buffer_data_cv.notify_all();
    action_buffer_slot_cv.notify_all();
}

template <typename... CascadeTypes>
void ExecutionEngine<CascadeTypes...>::destroy() {
    dbg_default_trace("Destroying Cascade context@{:p}.",static_cast<void*>(this));
    is_running.store(false);
    stateless_action_queue_for_multicast.notify_all();
    stateless_action_queue_for_p2p.notify_all();
    for (auto& th:stateless_workhorses_for_multicast) {
        if (th.joinable()) {
            th.join();
        }
    }
    for (auto& th:stateless_workhorses_for_p2p) {
        if (th.joinable()) {
            th.join();
        }
    }
    stateless_workhorses_for_multicast.clear();
    stateless_workhorses_for_p2p.clear();
    for (auto& queue: stateful_action_queues_for_multicast) {
        queue->notify_all();
    }
    for (auto& queue: stateful_action_queues_for_p2p) {
        queue->notify_all();
    }
    for (auto& th: stateful_workhorses_for_multicast) {
        if (th.joinable()) {
            th.join();
        }
    }
    for (auto& th: stateful_workhorses_for_p2p) {
        if (th.joinable()) {
            th.join();
        }
    }
    stateful_workhorses_for_multicast.clear();
    stateful_workhorses_for_p2p.clear();
    if(single_threaded_workhorse_for_multicast.joinable()) {
        single_threaded_workhorse_for_multicast.join();
    }
    if(single_threaded_workhorse_for_p2p.joinable()) {
        single_threaded_workhorse_for_p2p.join();
    }
    dbg_default_trace("Cascade context@{:p} is destroyed.",static_cast<void*>(this));
}

template <typename... CascadeTypes>
ServiceClient<CascadeTypes...>& ExecutionEngine<CascadeTypes...>::get_service_client_ref() const {
    return ServiceClient<CascadeTypes...>::get_service_client();
}


template <typename... CascadeTypes>
PersistenceObserver& ExecutionEngine<CascadeTypes...>::get_persistence_observer() const {
    return *persistence_observer;
}

template <typename... CascadeTypes>
void ExecutionEngine<CascadeTypes...>::register_prefixes(
        const std::string&                                  dfg_uuid,
        const std::unordered_set<std::string>&              prefixes,
        const DataFlowGraph::VertexShardDispatcher          shard_dispatcher,
        const DataFlowGraph::VertexExecutionEnvironment     execution_environment,
        const std::string&                                  execution_environment_config,
        const DataFlowGraph::Statefulness                   stateful,
        const DataFlowGraph::VertexHook                     hook,
        const std::string&                                  user_defined_logic_id,
        const std::string&                                  user_defined_logic_config,
        const std::shared_ptr<OffCriticalDataPathObserver>& ocdpo_ptr,
        const std::unordered_map<std::string,bool>&         outputs) {
    for (const auto& prefix:prefixes) {
        prefix_registry_ptr->atomically_modify(prefix,
            [&dfg_uuid,&prefix,&execution_environment,&shard_dispatcher,&stateful,
             &hook,&user_defined_logic_id,&user_defined_logic_config,
             &ocdpo_ptr,&outputs] (const std::shared_ptr<prefix_entry_t>& entry){
                std::shared_ptr<prefix_entry_t> new_entry;
                if (entry) {
                    new_entry = std::make_shared<prefix_entry_t>(*entry);
                } else {
                    new_entry = std::make_shared<prefix_entry_t>(prefix_entry_t{});
                }

                // find application
                if (new_entry->find(dfg_uuid) == new_entry->end()) {
                    new_entry->emplace(dfg_uuid,prefix_ocdpo_info_set_t{});
                }
                // create prefix_ocdpo_info_t
                prefix_ocdpo_info_t ocdpo_info = {
                    .udl_id = user_defined_logic_id,
                    .config_string = user_defined_logic_config,
                    .execution_environment = execution_environment,
                    .shard_dispatcher = shard_dispatcher,
                    .statefulness = stateful,
                    .hook = hook,
                    .ocdpo = ocdpo_ptr,
                    .output_map = outputs};

                // insert it to new_entry
                (*new_entry)[dfg_uuid].erase(ocdpo_info);
                (*new_entry)[dfg_uuid].emplace(ocdpo_info);

                return new_entry;
            },true);
    }
}

template <typename... CascadeTypes>
void ExecutionEngine<CascadeTypes...>::unregister_prefixes(const std::string& dfg_uuid) {
    prefix_registry_ptr->atomically_traverse(
            [&dfg_uuid](const std::shared_ptr<prefix_entry_t>& entry) {
                if (entry->find(dfg_uuid) != entry->cend()) {
                    entry->erase(dfg_uuid);
                }
                return entry;
            });
}

/* Note: On the same hardware, copying a shared_ptr spends ~7.4ns, and copying a raw pointer spends ~1.8 ns*/
template <typename... CascadeTypes>
match_results_t ExecutionEngine<CascadeTypes...>::get_prefix_handlers(const std::string& path) {

    match_results_t handlers;

    prefix_registry_ptr->collect_values_for_prefixes(
            path,
            [&handlers](const std::string& prefix, const std::shared_ptr<prefix_entry_t>& entry) {
                // handlers[prefix].insert(entry->cbegin(),entry->cend());
                if (entry) {
                    handlers.emplace(prefix,*entry);
                }
            });

    return handlers;
}

template <typename... CascadeTypes>
bool ExecutionEngine<CascadeTypes...>::post(Action&& action, DataFlowGraph::Statefulness stateful, bool is_trigger) {
    static uint32_t trigger_rrcnt = 0;
    static uint32_t multicast_rrcnt = 0;
    dbg_default_trace("Posting an action to Cascade context@{:p}.", static_cast<void*>(this));
    if (is_running) {
        if (is_trigger) {
            switch(stateful) {
            case DataFlowGraph::Statefulness::STATEFUL:
                {
                    uint32_t thread_index = std::hash<std::string>{}(action.key_string) % stateful_action_queues_for_p2p.size();
                    stateful_action_queues_for_p2p[thread_index]->action_buffer_enqueue(std::move(action));
                }
                break;
            case DataFlowGraph::Statefulness::STATELESS:
            case DataFlowGraph::Statefulness::UNKNOWN_S: // default
                // stateless_action_queue_for_p2p.action_buffer_enqueue(std::move(action));
                stateful_action_queues_for_p2p[trigger_rrcnt++ % stateful_action_queues_for_p2p.size()]->action_buffer_enqueue(std::move(action));
                break;
            case DataFlowGraph::Statefulness::SINGLETHREADED:
                single_threaded_action_queue_for_p2p.action_buffer_enqueue(std::move(action));
                break;
            }
        } else {
            switch(stateful) {
            case DataFlowGraph::Statefulness::STATEFUL:
                {
                    uint32_t thread_index = std::hash<std::string>{}(action.key_string) % stateful_action_queues_for_multicast.size();
                    stateful_action_queues_for_multicast[thread_index]->action_buffer_enqueue(std::move(action));
                }
                break;
            case DataFlowGraph::Statefulness::STATELESS:
            case DataFlowGraph::Statefulness::UNKNOWN_S: // default
                // stateless_action_queue_for_multicast.action_buffer_enqueue(std::move(action));
                stateful_action_queues_for_multicast[multicast_rrcnt++ % stateful_action_queues_for_multicast.size()]->action_buffer_enqueue(std::move(action));
                break;
            case DataFlowGraph::Statefulness::SINGLETHREADED:
                single_threaded_action_queue_for_multicast.action_buffer_enqueue(std::move(action));
                break;
            }
        }
    } else {
        dbg_default_warn("Failed to post to Cascade context@{:p} because it is not running.", static_cast<void*>(this));
        return false;
    }
    dbg_default_trace("Action posted to Cascade context@{:p}.", static_cast<void*>(this));
    return true;
}

template <typename... CascadeTypes>
size_t ExecutionEngine<CascadeTypes...>::stateless_action_queue_length_p2p() {
    return (stateless_action_queue_for_p2p.action_buffer_tail - stateless_action_queue_for_multicast.action_buffer_head + ACTION_BUFFER_SIZE)%ACTION_BUFFER_SIZE;
}

template <typename... CascadeTypes>
size_t ExecutionEngine<CascadeTypes...>::stateless_action_queue_length_multicast() {
    return (stateless_action_queue_for_multicast.action_buffer_tail - stateless_action_queue_for_multicast.action_buffer_head + ACTION_BUFFER_SIZE)%ACTION_BUFFER_SIZE;
}

template <typename... CascadeTypes>
ExecutionEngine<CascadeTypes...>::~ExecutionEngine() {
    destroy();
}

}
}
