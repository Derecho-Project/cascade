#include <vector>
#include <map>
#include <typeindex>
#include <variant>
#include <derecho/core/derecho.hpp>
#include <cascade/data_flow_graph.hpp>
#include <chrono>

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
    context = std::make_unique<CascadeContext<CascadeTypes...>>();
    std::vector<DeserializationContext*> new_dsms(dsms);
    new_dsms.emplace_back(context.get());
    // STEP 3 - create derecho group
    group = std::make_unique<derecho::Group<CascadeMetadataService<CascadeTypes...>,CascadeTypes...>>(
                UserMessageCallbacks{},
                si,
                new_dsms,
                std::vector<derecho::view_upcall_t>{},
                factory_wrapper(context.get(),metadata_service_factory),
                factory_wrapper(context.get(),factories)...);
    dbg_default_trace("joined group.");
    // STEP 4 - construct context
    context->construct(group.get());
    // STEP 5 - create service thread
    this->_is_running = true;
    service_thread = std::thread(&Service<CascadeTypes...>::run, this);
    dbg_default_trace("created daemon thread.");
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

template <typename... CascadeTypes>
std::unique_ptr<Service<CascadeTypes...>> Service<CascadeTypes...>::service_ptr;

template <typename... CascadeTypes>
void Service<CascadeTypes...>::start(const std::vector<DeserializationContext*>& dsms, 
        derecho::cascade::Factory<CascadeMetadataService<CascadeTypes...>> metadata_factory,
        derecho::cascade::Factory<CascadeTypes>... factories) {
    if (!service_ptr) {
        service_ptr = std::make_unique<Service<CascadeTypes...>>(dsms, metadata_factory, factories...);
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
}

template <typename... CascadeTypes>
ServiceClient<CascadeTypes...>::ServiceClient(derecho::Group<CascadeMetadataService<CascadeTypes...>,CascadeTypes...>* _group_ptr):
    group_ptr(_group_ptr) {
    if (group_ptr == nullptr) {
        this->external_group_ptr = std::make_unique<derecho::ExternalGroup<CascadeMetadataService<CascadeTypes...>,CascadeTypes...>>();
    } 
}

template <typename... CascadeTypes>
node_id_t ServiceClient<CascadeTypes...>::get_my_id() const {
    if (group_ptr != nullptr) {
        return group_ptr->get_my_id();
    } else {
        return external_group_ptr->get_my_id();
    }
}

template <typename... CascadeTypes>
std::vector<node_id_t> ServiceClient<CascadeTypes...>::get_members() const {
    if (group_ptr != nullptr) {
        return group_ptr->get_members();
    } else {
        return external_group_ptr->get_members();
    }
}

/**
 * disable the APIs exposing subgroup_id, which are originally designed for internal use.
template <typename... CascadeTypes>
std::vector<node_id_t> ServiceClient<CascadeTypes...>::get_shard_members(derecho::subgroup_id_t subgroup_id, uint32_t shard_index) {
    if (group_ptr != nullptr) {
        // TODO: There is no API exposed in Group for getting shard members by subgroup_id.
        return group_ptr->get_shard_members(subgroup_id,shard_index);
    } else {
        return external_group_ptr->get_shard_members(subgroup_id,shard_index);
    }
}
*/

template <typename... CascadeTypes>
template <typename SubgroupType>
std::vector<node_id_t> ServiceClient<CascadeTypes...>::get_shard_members(uint32_t subgroup_index, uint32_t shard_index) const {
    if (group_ptr != nullptr) {
        std::vector<std::vector<node_id_t>> subgroup_members = group_ptr->template get_subgroup_members<SubgroupType>(subgroup_index);
        if (subgroup_members.size() > shard_index) {
            return subgroup_members[shard_index];
        } else {
            return {};
        }
    } else {
        return external_group_ptr->template get_shard_members<SubgroupType>(subgroup_index,shard_index);
    }
}

/**
 * disable the APIs exposing subgroup_id, which are originally designed for internal use.
template <typename... CascadeTypes>
template <typename SubgroupType>
uint32_t ServiceClient<CascadeTypes...>::get_number_of_subgroups() {
    if (group_ptr != nullptr) {
        //TODO: how to get the number of subgroups of a given type?
    } else {
        return external_group_ptr->template get_number_of_subgroups<SubgroupType>();
    }
}

template <typename... CascadeTypes>
uint32_t ServiceClient<CascadeTypes...>::get_number_of_shards(derecho::subgroup_id_t subgroup_id) {
    if (group_ptr != nullptr) {
        //TODO: There is no API exposed in Group for getting number of shards by subgroup_id
    } else {
        return external_group_ptr->get_number_of_shards(subgroup_id);
    }
}
 */

template <typename... CascadeTypes>
template <typename SubgroupType>
uint32_t ServiceClient<CascadeTypes...>::get_number_of_shards(uint32_t subgroup_index) const {
    if (group_ptr != nullptr) {
        return group_ptr->template get_subgroup_members<SubgroupType>(subgroup_index).size();
    } else {
        return external_group_ptr->template get_number_of_shards<SubgroupType>(subgroup_index);
    }
}

template <typename... CascadeTypes>
template <typename SubgroupType>
void ServiceClient<CascadeTypes...>::set_member_selection_policy(uint32_t subgroup_index,uint32_t shard_index,
        ShardMemberSelectionPolicy policy, node_id_t user_specified_node_id) {
    // write lock policies
    std::unique_lock wlck(this->member_selection_policies_mutex);
    // update map
    this->member_selection_policies[std::make_tuple(std::type_index(typeid(SubgroupType)),subgroup_index,shard_index)] =
            std::make_tuple(policy,user_specified_node_id);
}

template <typename... CascadeTypes>
template <typename SubgroupType>
std::tuple<ShardMemberSelectionPolicy,node_id_t> ServiceClient<CascadeTypes...>::get_member_selection_policy(
        uint32_t subgroup_index, uint32_t shard_index) const {
    // read lock policies
    std::shared_lock rlck(this->member_selection_policies_mutex);
    auto key = std::make_tuple(std::type_index(typeid(SubgroupType)),subgroup_index,shard_index);
    // read map
    if (member_selection_policies.find(key) != member_selection_policies.end()) {
        return member_selection_policies.at(key);
    } else {
        return std::make_tuple(DEFAULT_SHARD_MEMBER_SELECTION_POLICY,INVALID_NODE_ID);
    }
}

template <typename... CascadeTypes>
template <typename SubgroupType>
void ServiceClient<CascadeTypes...>::refresh_member_cache_entry(uint32_t subgroup_index,
                                                          uint32_t shard_index) {
    auto key = std::make_tuple(std::type_index(typeid(SubgroupType)),subgroup_index,shard_index);
    auto members = get_shard_members<SubgroupType>(subgroup_index,shard_index);
    std::shared_lock rlck(member_cache_mutex);
    if (member_cache.find(key) == member_cache.end()) {
        rlck.unlock();
        std::unique_lock wlck(member_cache_mutex);
        member_cache[key] = members;
    } else {
        member_cache[key].swap(members);
    }
}

template <typename... CascadeTypes>
template <typename SubgroupType>
std::pair<uint32_t,uint32_t> ServiceClient<CascadeTypes...>::key_to_subgroup_index_and_shard_index(
	const typename SubgroupType::KeyType& key,
    bool check_object_location) {
    std::string object_pool_pathname = get_pathname<typename SubgroupType::KeyType>(key);
    if (object_pool_pathname.empty()) {
        std::string exp_msg("Key:");
        throw derecho::derecho_exception(std::string("Key:") + key + " does not belong to any object pool.");
    }

    auto opm = find_object_pool(object_pool_pathname);
    uint32_t shard_index = opm.key_to_shard_index(key,get_number_of_shards<SubgroupType>(opm.subgroup_index),check_object_location);
    return std::pair<uint32_t,uint32_t>{opm.subgroup_index,shard_index};
}


template <typename... CascadeTypes>
template <typename SubgroupType>
node_id_t ServiceClient<CascadeTypes...>::pick_member_by_policy(uint32_t subgroup_index,
                                                                uint32_t shard_index,
                                                                bool retry) {
    ShardMemberSelectionPolicy policy;
    node_id_t last_specified_node_id_or_index;

    std::tie(policy,last_specified_node_id_or_index) = get_member_selection_policy<SubgroupType>(subgroup_index,shard_index);

    if (policy == ShardMemberSelectionPolicy::UserSpecified) {
        return last_specified_node_id_or_index;
    }

    auto key = std::make_tuple(std::type_index(typeid(SubgroupType)),subgroup_index,shard_index);

    if (member_cache.find(key) == member_cache.end() || retry) {
        refresh_member_cache_entry<SubgroupType>(subgroup_index,shard_index);
    }

    std::shared_lock rlck(member_cache_mutex);

    node_id_t node_id = last_specified_node_id_or_index;

    switch(policy) {
    case ShardMemberSelectionPolicy::FirstMember:
        node_id = member_cache.at(key).front();
        break;
    case ShardMemberSelectionPolicy::LastMember:
        node_id = member_cache.at(key).back();
        break;
    case ShardMemberSelectionPolicy::Random:
        node_id = member_cache.at(key)[get_time()%member_cache.at(key).size()]; // use time as random source.
        break;
    case ShardMemberSelectionPolicy::FixedRandom:
        if (node_id == INVALID_NODE_ID || retry) {
            node_id = member_cache.at(key)[get_time()%member_cache.at(key).size()]; // use time as random source.
        }
        break;
    case ShardMemberSelectionPolicy::RoundRobin:
        {
            std::shared_lock rlck(member_selection_policies_mutex);
            node_id = static_cast<uint32_t>(node_id+1)%member_cache.at(key).size();
            auto new_policy = std::make_tuple(ShardMemberSelectionPolicy::RoundRobin,node_id);
            member_selection_policies[key].swap(new_policy);
        }
        node_id = member_cache.at(key)[node_id];
        break;
    default:
        throw derecho::derecho_exception("Unknown member selection policy:" + std::to_string(static_cast<unsigned int>(policy)) );
    }

    return node_id;
}

template <typename... CascadeTypes>
template <typename SubgroupType>
derecho::rpc::QueryResults<std::tuple<persistent::version_t,uint64_t>> ServiceClient<CascadeTypes...>::put(
        const typename SubgroupType::ObjectType& value,
        uint32_t subgroup_index,
        uint32_t shard_index) {
    if (group_ptr != nullptr) {
        std::lock_guard(this->group_ptr_mutex);
        if (static_cast<uint32_t>(group_ptr->template get_my_shard<SubgroupType>(subgroup_index)) == shard_index) {
            // do ordered put as a member (Replicated).
            auto& subgroup_handle = group_ptr->template get_subgroup<SubgroupType>(subgroup_index);
            return subgroup_handle.template ordered_send<RPC_NAME(ordered_put)>(value);
        } else {
            // do normal put as a non member (ExternalCaller).
            auto& subgroup_handle = group_ptr->template get_nonmember_subgroup<SubgroupType>(subgroup_index);
            node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index,shard_index);
            return subgroup_handle.template p2p_send<RPC_NAME(put)>(node_id,value);
        }
    } else {
        std::lock_guard(this->external_group_ptr_mutex);
        // call as an external client (ExternalClientCaller).
        auto& caller = external_group_ptr->template get_subgroup_caller<SubgroupType>(subgroup_index);
        node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index,shard_index);
        return caller.template p2p_send<RPC_NAME(put)>(node_id,value);
    }
}

template <typename... CascadeTypes>
template <typename SubgroupType>
derecho::rpc::QueryResults<std::tuple<persistent::version_t,uint64_t>> ServiceClient<CascadeTypes...>::put(
        const typename SubgroupType::ObjectType& value) {
    uint32_t subgroup_index,shard_index;
    std::tie(subgroup_index,shard_index) = this->template key_to_subgroup_index_and_shard_index<SubgroupType>(value.get_key_ref());
    return this->template put<SubgroupType>(value,subgroup_index,shard_index);
}

template <typename... CascadeTypes>
template <typename SubgroupType>
derecho::rpc::QueryResults<void> ServiceClient<CascadeTypes...>::trigger_put(
        const typename SubgroupType::ObjectType& value,
        uint32_t subgroup_index,
        uint32_t shard_index) {
    if (group_ptr != nullptr) {
        // TODO: can we do p2p_call to myself?
        std::lock_guard(this->group_ptr_mutex);
        auto& subgroup_handle = group_ptr->template get_nonmember_subgroup<SubgroupType>(subgroup_index);
        node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index,shard_index);
        return subgroup_handle.template p2p_send<RPC_NAME(trigger_put)>(node_id,value);
    } else {
        std::lock_guard(this->external_group_ptr_mutex);
        // call as an external client (ExternalClientCaller).
        auto& caller = external_group_ptr->template get_subgroup_caller<SubgroupType>(subgroup_index);
        node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index,shard_index);
        return caller.template p2p_send<RPC_NAME(trigger_put)>(node_id,value);
    }
}

template <typename... CascadeTypes>
template <typename SubgroupType>
derecho::rpc::QueryResults<void> ServiceClient<CascadeTypes...>::trigger_put(
        const typename SubgroupType::ObjectType& value) {
    uint32_t subgroup_index,shard_index;
    std::tie(subgroup_index,shard_index) = this->template key_to_subgroup_index_and_shard_index<SubgroupType>(value.get_key_ref());
    return trigger_put<SubgroupType>(value,subgroup_index,shard_index);
}

template <typename... CascadeTypes>
template <typename SubgroupType>
void ServiceClient<CascadeTypes...>::collective_trigger_put(
        const typename SubgroupType::ObjectType& value,
        uint32_t subgroup_index,
        std::unordered_map<node_id_t,std::unique_ptr<derecho::rpc::QueryResults<void>>>& nodes_and_futures) {
    if (group_ptr != nullptr) {
        std::lock_guard(this->group_ptr_mutex);
        auto& subgroup_handle = group_ptr->template get_nonmember_subgroup<SubgroupType>(subgroup_index);
        for (auto& kv: nodes_and_futures) {
            nodes_and_futures[kv.first] = std::make_unique<derecho::rpc::QueryResults<void>>(
                    std::move(subgroup_handle.template p2p_send<RPC_NAME(trigger_put)>(kv.first,value)));
        }
    } else {
        std::lock_guard(this->external_group_ptr_mutex);
        auto& caller = external_group_ptr->template get_subgroup_caller<SubgroupType>(subgroup_index);
        for (auto& kv: nodes_and_futures) {
            nodes_and_futures[kv.first] = std::make_unique<derecho::rpc::QueryResults<void>>(
                    std::move(caller.template p2p_send<RPC_NAME(trigger_put)>(kv.first,value)));
        }
    }
}

template <typename... CascadeTypes>
template <typename SubgroupType>
derecho::rpc::QueryResults<std::tuple<persistent::version_t,uint64_t>> ServiceClient<CascadeTypes...>::remove(
        const typename SubgroupType::KeyType& key,
        uint32_t subgroup_index,
        uint32_t shard_index) {
    if (group_ptr != nullptr) {
        std::lock_guard(this->group_ptr_mutex);
        if (static_cast<uint32_t>(group_ptr->template get_my_shard<SubgroupType>(subgroup_index)) == shard_index) {
            // do ordered remove as a member (Replicated).
            auto& subgroup_handle = group_ptr->template get_subgroup<SubgroupType>(subgroup_index);
            return subgroup_handle.template ordered_send<RPC_NAME(ordered_remove)>(key);
        } else {
            auto& subgroup_handle = group_ptr->template get_nonmember_subgroup<SubgroupType>(subgroup_index);
            // do normal remove as a non member (ExternalCaller).
            node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index,shard_index);
            return subgroup_handle.template p2p_send<RPC_NAME(remove)>(node_id,key);
        }
    } else {
        std::lock_guard(this->external_group_ptr_mutex);
        // call as an external client (ExternalClientCaller).
        auto& caller = external_group_ptr->template get_subgroup_caller<SubgroupType>(subgroup_index);
        node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index,shard_index);
        return caller.template p2p_send<RPC_NAME(remove)>(node_id,key);
    }
}

template <typename... CascadeTypes>
template <typename SubgroupType>
derecho::rpc::QueryResults<std::tuple<persistent::version_t,uint64_t>> ServiceClient<CascadeTypes...>::remove(
        const typename SubgroupType::KeyType& key) {
    uint32_t subgroup_index,shard_index;
    std::tie(subgroup_index,shard_index) = this->template key_to_subgroup_index_and_shard_index<SubgroupType>(key);
    return remove<SubgroupType>(key,subgroup_index,shard_index);
}

template <typename... CascadeTypes>
template <typename SubgroupType>
derecho::rpc::QueryResults<const typename SubgroupType::ObjectType> ServiceClient<CascadeTypes...>::get(
        const typename SubgroupType::KeyType& key,
        const persistent::version_t& version,
        uint32_t subgroup_index,
        uint32_t shard_index) {
    if (group_ptr != nullptr) {
        std::lock_guard(this->group_ptr_mutex);
        if (static_cast<uint32_t>(group_ptr->template get_my_shard<SubgroupType>(subgroup_index)) == shard_index) {
            // do ordered put as a member (Replicated).
            auto& subgroup_handle = group_ptr->template get_subgroup<SubgroupType>(subgroup_index);
            return subgroup_handle.template p2p_send<RPC_NAME(get)>(group_ptr->get_my_id(),key,version,false);
        } else {
            // do normal put as a non member (ExternalCaller).
            auto& subgroup_handle = group_ptr->template get_nonmember_subgroup<SubgroupType>(subgroup_index);
            node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index,shard_index);
            return subgroup_handle.template p2p_send<RPC_NAME(get)>(node_id,key,version,false);
        }
    } else {
        std::lock_guard(this->external_group_ptr_mutex);
        // call as an external client (ExternalClientCaller).
        auto& caller = external_group_ptr->template get_subgroup_caller<SubgroupType>(subgroup_index);
        node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index,shard_index);
        return caller.template p2p_send<RPC_NAME(get)>(node_id,key,version,false); 
    }
}

template <typename... CascadeTypes>
template <typename SubgroupType>
derecho::rpc::QueryResults<const typename SubgroupType::ObjectType> ServiceClient<CascadeTypes...>::get(
        const typename SubgroupType::KeyType& key,
        const persistent::version_t& version) {
    uint32_t subgroup_index,shard_index;
    std::tie(subgroup_index,shard_index) = this->template key_to_subgroup_index_and_shard_index<SubgroupType>(key);
    return get<SubgroupType>(key,version,subgroup_index,shard_index);
}

template <typename... CascadeTypes>
template <typename SubgroupType>
derecho::rpc::QueryResults<const typename SubgroupType::ObjectType> ServiceClient<CascadeTypes...>::get_by_time(
        const typename SubgroupType::KeyType& key,
        const uint64_t& ts_us,
        uint32_t subgroup_index,
        uint32_t shard_index) {
    if (group_ptr != nullptr) {
        std::lock_guard(this->group_ptr_mutex);
        if (static_cast<uint32_t>(group_ptr->template get_my_shard<SubgroupType>(subgroup_index)) == shard_index) {
            // do ordered put as a member (Replicated).
            auto& subgroup_handle = group_ptr->template get_subgroup<SubgroupType>(subgroup_index);
            return subgroup_handle.template p2p_send<RPC_NAME(get_by_time)>(group_ptr->get_my_id(),key,ts_us);
        } else {
            // do normal put as a non member (ExternalCaller).
            auto& subgroup_handle = group_ptr->template get_nonmember_subgroup<SubgroupType>(subgroup_index);
            node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index,shard_index);
            return subgroup_handle.template p2p_send<RPC_NAME(get_by_time)>(node_id,key,ts_us);
        }
    } else {
        std::lock_guard(this->external_group_ptr_mutex);
        // call as an external client (ExternalClientCaller).
        auto& caller = external_group_ptr->template get_subgroup_caller<SubgroupType>();
        node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index,shard_index);
        return caller.template p2p_send<RPC_NAME(get_by_time)>(node_id,key,ts_us);
    }
}

template <typename... CascadeTypes>
template <typename SubgroupType>
derecho::rpc::QueryResults<const typename SubgroupType::ObjectType> ServiceClient<CascadeTypes...>::get_by_time(
        const typename SubgroupType::KeyType& key,
        const uint64_t& ts_us) {
    uint32_t subgroup_index,shard_index;
    std::tie(subgroup_index,shard_index) = this->template key_to_subgroup_index_and_shard_index<SubgroupType>(key);
    return get_by_time<SubgroupType>(key,ts_us,subgroup_index,shard_index);
}

template <typename... CascadeTypes>
template <typename SubgroupType>
derecho::rpc::QueryResults<uint64_t> ServiceClient<CascadeTypes...>::get_size(
        const typename SubgroupType::KeyType& key,
        const persistent::version_t& version,
        uint32_t subgroup_index,
        uint32_t shard_index) {
    if (group_ptr != nullptr) {
        std::lock_guard(this->group_ptr_mutex);
        if (static_cast<uint32_t>(group_ptr->template get_my_shard<SubgroupType>(subgroup_index)) == shard_index) {
            // do ordered put as a member (Replicated).
            auto& subgroup_handle = group_ptr->template get_subgroup<SubgroupType>(subgroup_index);
            return subgroup_handle.template p2p_send<RPC_NAME(get_size)>(group_ptr->get_my_id(),key,version,false);
        } else {
            // do normal put as a non member (ExternalCaller).
            auto& subgroup_handle = group_ptr->template get_nonmember_subgroup<SubgroupType>(subgroup_index);
            node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index,shard_index);
            return subgroup_handle.template p2p_send<RPC_NAME(get_size)>(node_id,key,version,false);
        }
    } else {
        std::lock_guard(this->external_group_ptr_mutex);
        // call as an external client (ExternalClientCaller).
        auto& caller = external_group_ptr->template get_subgroup_caller<SubgroupType>(subgroup_index);
        node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index,shard_index);
        return caller.template p2p_send<RPC_NAME(get_size)>(node_id,key,version,false);
    }
}

template <typename... CascadeTypes>
template <typename SubgroupType>
derecho::rpc::QueryResults<uint64_t> ServiceClient<CascadeTypes...>::get_size(
        const typename SubgroupType::KeyType& key,
        const persistent::version_t& version) {
    uint32_t subgroup_index,shard_index;
    std::tie(subgroup_index,shard_index) = this->template key_to_subgroup_index_and_shard_index<SubgroupType>(key);
    return get_size<SubgroupType>(key,version,subgroup_index,shard_index);
}

template <typename... CascadeTypes>
template <typename SubgroupType>
derecho::rpc::QueryResults<uint64_t> ServiceClient<CascadeTypes...>::get_size_by_time(
        const typename SubgroupType::KeyType& key,
        const uint64_t& ts_us,
        uint32_t subgroup_index,
        uint32_t shard_index) {
    if (group_ptr != nullptr) {
        std::lock_guard(this->group_ptr_mutex);
        if (static_cast<uint32_t>(group_ptr->template get_my_shard<SubgroupType>(subgroup_index)) == shard_index) {
            // do ordered put as a member (Replicated).
            auto& subgroup_handle = group_ptr->template get_subgroup<SubgroupType>(subgroup_index);
            return subgroup_handle.template p2p_send<RPC_NAME(get_size_by_time)>(group_ptr->get_my_id(),key,ts_us);
        } else {
            // do normal put as a non member (ExternalCaller).
            auto& subgroup_handle = group_ptr->template get_nonmember_subgroup<SubgroupType>(subgroup_index);
            node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index,shard_index);
            return subgroup_handle.template p2p_send<RPC_NAME(get_size_by_time)>(node_id,key,ts_us);
        }
    } else {
        std::lock_guard(this->external_group_ptr_mutex);
        // call as an external client (ExternalClientCaller).
        auto& caller = external_group_ptr->template get_subgroup_caller<SubgroupType>(subgroup_index);
        node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index,shard_index);
        return caller.template p2p_send<RPC_NAME(get_size_by_time)>(node_id,key,ts_us);
    }
}

template <typename... CascadeTypes>
template <typename SubgroupType>
derecho::rpc::QueryResults<uint64_t> ServiceClient<CascadeTypes...>::get_size_by_time(
        const typename SubgroupType::KeyType& key,
        const uint64_t& ts_us) {
    uint32_t subgroup_index,shard_index;
    std::tie(subgroup_index,shard_index) = this->template key_to_subgroup_index_and_shard_index<SubgroupType>(key);
    return get_size_by_time<SubgroupType>(key,ts_us,subgroup_index,shard_index);
}

template <typename... CascadeTypes>
template <typename SubgroupType>
derecho::rpc::QueryResults<std::vector<typename SubgroupType::KeyType>> ServiceClient<CascadeTypes...>::list_keys(
        const persistent::version_t& version,
        uint32_t subgroup_index,
        uint32_t shard_index) {
    if (group_ptr != nullptr) {
        std::lock_guard(this->group_ptr_mutex);
        if (static_cast<uint32_t>(group_ptr->template get_my_shard<SubgroupType>(subgroup_index)) == shard_index) {
            // do ordered put as a member (Replicated).
            auto& subgroup_handle = group_ptr->template get_subgroup<SubgroupType>(subgroup_index);
            return subgroup_handle.template p2p_send<RPC_NAME(list_keys)>(group_ptr->get_my_id(),version);
        } else {
            // do normal put as a non member (ExternalCaller).
            auto& subgroup_handle = group_ptr->template get_nonmember_subgroup<SubgroupType>(subgroup_index);
            node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index,shard_index);
            return subgroup_handle.template p2p_send<RPC_NAME(list_keys)>(node_id,version);
        }
    } else {
        std::lock_guard(this->external_group_ptr_mutex);
        // call as an external client (ExternalClientCaller).
        auto& caller = external_group_ptr->template get_subgroup_caller<SubgroupType>(subgroup_index);
        node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index,shard_index);
        return caller.template p2p_send<RPC_NAME(list_keys)>(node_id,version);
    }
}

template <typename... CascadeTypes>
template <typename SubgroupType>
std::vector<std::unique_ptr<derecho::rpc::QueryResults<std::vector<typename SubgroupType::KeyType>>>> ServiceClient<CascadeTypes...>::list_keys(
        const persistent::version_t& version,
        const std::string& object_pool_pathname){
    auto opm = find_object_pool(object_pool_pathname);
    uint32_t subgroup_index = opm.subgroup_index;
    uint32_t shards = get_number_of_shards<SubgroupType>(subgroup_index);
    std::vector<std::unique_ptr<derecho::rpc::QueryResults<std::vector<typename SubgroupType::KeyType>>>> result;
    for (uint32_t shard_index = 0; shard_index < shards; shard_index ++){
        if (group_ptr != nullptr) {
            if (static_cast<uint32_t>(group_ptr->template get_my_shard<SubgroupType>(subgroup_index)) == shard_index) {
                auto& subgroup_handle = group_ptr->template get_subgroup<SubgroupType>(subgroup_index);
                auto shard_keys = subgroup_handle.template p2p_send<RPC_NAME(op_list_keys)>(group_ptr->get_my_id(),version,object_pool_pathname);
                result.emplace_back(std::make_unique<derecho::rpc::QueryResults<std::vector<typename SubgroupType::KeyType>>>(std::move(shard_keys)));
            } else {
                auto& subgroup_handle = group_ptr->template get_nonmember_subgroup<SubgroupType>(subgroup_index);
                node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index,shard_index);
                auto shard_keys= subgroup_handle.template p2p_send<RPC_NAME(op_list_keys)>(node_id,version,object_pool_pathname);
                result.emplace_back(std::make_unique<derecho::rpc::QueryResults<std::vector<typename SubgroupType::KeyType>>>(std::move(shard_keys)));
            }
        } else {
            std::lock_guard(this->external_group_ptr_mutex);
            auto& caller = external_group_ptr->template get_subgroup_caller<SubgroupType>(subgroup_index);
            node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index,shard_index);
            auto shard_keys = caller.template p2p_send<RPC_NAME(op_list_keys)>(node_id,version,object_pool_pathname);
            result.emplace_back(std::make_unique<derecho::rpc::QueryResults<std::vector<typename SubgroupType::KeyType>>>(std::move(shard_keys)));
        }
    }
    return result;
}

template <typename ReturnType>  
inline ReturnType wait_for_future(derecho::rpc::QueryResults<ReturnType>& result){
    // iterate through ReplyMap
    for (auto& reply_future: result.get()) {
        ReturnType reply = static_cast<ReturnType>(reply_future.second.get());
        return reply;
    }
    return ReturnType();
};


template <typename... CascadeTypes>
template <typename SubgroupType>
std::vector<typename SubgroupType::KeyType> ServiceClient<CascadeTypes...>::wait_list_keys(
        std::vector<std::unique_ptr<derecho::rpc::QueryResults<std::vector<typename SubgroupType::KeyType>>>>& future){
    std::vector<typename SubgroupType::KeyType> result;
    // iterate over each shard's Query result
    for(auto& query_result: future){
        std::vector<typename SubgroupType::KeyType> reply = wait_for_future<std::vector<typename SubgroupType::KeyType>>(*(query_result.get()));
        std::move(reply.begin(), reply.end(), std::back_inserter(result));
    }
    return result;
}

template <typename... CascadeTypes>
template <typename SubgroupType>
derecho::rpc::QueryResults<std::vector<typename SubgroupType::KeyType>> ServiceClient<CascadeTypes...>::list_keys_by_time(
        const uint64_t& ts_us,
        uint32_t subgroup_index,
        uint32_t shard_index) {
    if (group_ptr != nullptr) {
        std::lock_guard(this->group_ptr_mutex);
        if (static_cast<uint32_t>(group_ptr->template get_my_shard<SubgroupType>(subgroup_index)) == shard_index) {
            // do ordered put as a member (Replicated).
            auto& subgroup_handle = group_ptr->template get_subgroup<SubgroupType>(subgroup_index);
            return subgroup_handle.template p2p_send<RPC_NAME(list_keys_by_time)>(group_ptr->get_my_id(),ts_us);
        } else {
            // do normal put as a non member (ExternalCaller).
            auto& subgroup_handle = group_ptr->template get_nonmember_subgroup<SubgroupType>(subgroup_index);
            node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index,shard_index);
            return subgroup_handle.template p2p_send<RPC_NAME(list_keys_by_time)>(node_id,ts_us);
        }
    } else {
        std::lock_guard(this->external_group_ptr_mutex);
        // call as an external client (ExternalClientCaller).
        auto& caller = external_group_ptr->template get_subgroup_caller<SubgroupType>(subgroup_index);
        node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index,shard_index);
        return caller.template p2p_send<RPC_NAME(list_keys_by_time)>(node_id,ts_us);
    }
}

template <typename... CascadeTypes>
template <typename SubgroupType> 
std::vector<std::unique_ptr<derecho::rpc::QueryResults<std::vector<typename SubgroupType::KeyType>>>> ServiceClient<CascadeTypes...>::list_keys_by_time(
                const uint64_t& ts_us, const std::string& object_pool_pathname){
    auto opm = find_object_pool(object_pool_pathname);
    uint32_t subgroup_index = opm.subgroup_index;
    uint32_t shards = get_number_of_shards<SubgroupType>(subgroup_index);
    std::vector<std::unique_ptr<derecho::rpc::QueryResults<std::vector<typename SubgroupType::KeyType>>>> result;
    for (uint32_t shard_index = 0; shard_index < shards; shard_index ++){
        if (group_ptr != nullptr) {
            std::lock_guard(this->group_ptr_mutex);
            if (static_cast<uint32_t>(group_ptr->template get_my_shard<SubgroupType>(subgroup_index)) == shard_index) {
                // do ordered put as a member (Replicated).
                auto& subgroup_handle = group_ptr->template get_subgroup<SubgroupType>(subgroup_index);
                auto shard_keys = subgroup_handle.template p2p_send<RPC_NAME(op_list_keys_by_time)>(group_ptr->get_my_id(),ts_us,object_pool_pathname);
                result.emplace_back(std::make_unique<derecho::rpc::QueryResults<std::vector<typename SubgroupType::KeyType>>>(std::move(shard_keys)));
            } else {
                // do normal put as a non member (ExternalCaller).
                auto& subgroup_handle = group_ptr->template get_nonmember_subgroup<SubgroupType>(subgroup_index);
                node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index,shard_index);
                auto shard_keys = subgroup_handle.template p2p_send<RPC_NAME(op_list_keys_by_time)>(node_id,ts_us,object_pool_pathname);
                result.emplace_back(std::make_unique<derecho::rpc::QueryResults<std::vector<typename SubgroupType::KeyType>>>(std::move(shard_keys)));
            }
        } else {
            std::lock_guard(this->external_group_ptr_mutex);
            // call as an external client (ExternalClientCaller).
            auto& caller = external_group_ptr->template get_subgroup_caller<SubgroupType>(subgroup_index);
            node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index,shard_index);
            auto shard_keys = caller.template p2p_send<RPC_NAME(op_list_keys_by_time)>(node_id,ts_us,object_pool_pathname);
            result.emplace_back(std::make_unique<derecho::rpc::QueryResults<std::vector<typename SubgroupType::KeyType>>>(std::move(shard_keys)));
        }
    }
    return result;
}

template <typename... CascadeTypes>
void ServiceClient<CascadeTypes...>::refresh_object_pool_metadata_cache() {
    std::unordered_map<std::string,ObjectPoolMetadata<CascadeTypes...>> refreshed_metadata;
    uint32_t num_shards = this->template get_number_of_shards<CascadeMetadataService<CascadeTypes...>>(METADATA_SERVICE_SUBGROUP_INDEX);
    for(uint32_t shard=0;shard<num_shards;shard++) {
        auto results = this->template list_keys<CascadeMetadataService<CascadeTypes...>>(CURRENT_VERSION,METADATA_SERVICE_SUBGROUP_INDEX,shard);
        for (auto& reply : results.get()) { // only once
            for(auto& key: reply.second.get()) { // iterate over keys
                auto opm_result = this->template get<CascadeMetadataService<CascadeTypes...>>(key,CURRENT_VERSION,METADATA_SERVICE_SUBGROUP_INDEX,shard);
                for (auto& opm_reply:opm_result.get()) { // only once
                    refreshed_metadata[key] = opm_reply.second.get();
                    break;
                }
            }
            break;
        }
    }

    std::unique_lock<std::shared_mutex> wlck(object_pool_metadata_cache_mutex);
    this->object_pool_metadata_cache = refreshed_metadata;
}

template <typename... CascadeTypes>
template <typename SubgroupType>
derecho::rpc::QueryResults<std::tuple<persistent::version_t,uint64_t>> ServiceClient<CascadeTypes...>::create_object_pool(
        const std::string& pathname, const uint32_t subgroup_index,
        const sharding_policy_t sharding_policy, const std::unordered_map<std::string,uint32_t>& object_locations) {
    uint32_t subgroup_type_index = ObjectPoolMetadata<CascadeTypes...>::template get_subgroup_type_index<SubgroupType>();
    if (subgroup_type_index == ObjectPoolMetadata<CascadeTypes...>::invalid_subgroup_type_index) {
        dbg_default_crit("Create object pool failed because of invalid SubgroupType:{}", typeid(SubgroupType).name());
        throw derecho::derecho_exception(std::string("Create object pool failed because SubgroupType is invalid:")+typeid(SubgroupType).name());
    }
    ObjectPoolMetadata<CascadeTypes...> opm(pathname,subgroup_type_index,subgroup_index,sharding_policy,object_locations,false);
    // clear local cache entry.
    std::shared_lock<std::shared_mutex> rlck(object_pool_metadata_cache_mutex);
    if (object_pool_metadata_cache.find(pathname)==object_pool_metadata_cache.end()) {
        rlck.unlock();
    } else {
        rlck.unlock();
        std::unique_lock<std::shared_mutex> wlck(object_pool_metadata_cache_mutex);
        object_pool_metadata_cache.erase(pathname);
    }
    // determine the shard index by hashing
    uint32_t metadata_service_shard_index = std::hash<std::string>{}(pathname) % this->template get_number_of_shards<CascadeMetadataService<CascadeTypes...>>(METADATA_SERVICE_SUBGROUP_INDEX);

    return this->template put<CascadeMetadataService<CascadeTypes...>>(opm,METADATA_SERVICE_SUBGROUP_INDEX,metadata_service_shard_index);
}

template <typename... CascadeTypes>
derecho::rpc::QueryResults<std::tuple<persistent::version_t,uint64_t>> ServiceClient<CascadeTypes...>::remove_object_pool(const std::string& pathname) {
    // determine the shard index by hashing
    uint32_t metadata_service_shard_index = std::hash<std::string>{}(pathname) % this->template get_number_of_shards<CascadeMetadataService<CascadeTypes...>>(METADATA_SERVICE_SUBGROUP_INDEX);


    // check if this object pool exist in metadata service.
    auto opm = find_object_pool(pathname);
    // remove it from local cache.
    std::shared_lock<std::shared_mutex> rlck(object_pool_metadata_cache_mutex);
    if (object_pool_metadata_cache.find(pathname) == object_pool_metadata_cache.end()) {
        // no entry in cache
        rlck.unlock();
    } else {
        // remove from cache
        rlck.unlock();
        std::unique_lock<std::shared_mutex> wlck(object_pool_metadata_cache_mutex);
        object_pool_metadata_cache.erase(pathname);
        wlck.unlock();
    }
    if (opm.is_valid() && !opm.is_null() && !opm.deleted) {
        opm.deleted = true;
        opm.set_previous_version(CURRENT_VERSION,opm.version); // only check previous_version_by_key
        return this->template put<CascadeMetadataService<CascadeTypes...>>(opm,METADATA_SERVICE_SUBGROUP_INDEX,metadata_service_shard_index);
    }

    // we didn't find any entry with "id", but we do the normal 'remove', which has no effect but return a version.
    dbg_default_warn("deleteing a non-existing objectpool:{}.", pathname);
    return this->template remove<CascadeMetadataService<CascadeTypes...>>(pathname,METADATA_SERVICE_SUBGROUP_INDEX,metadata_service_shard_index);
}

template <typename... CascadeTypes>
ObjectPoolMetadata<CascadeTypes...> ServiceClient<CascadeTypes...>::find_object_pool(const std::string& pathname) {
    std::shared_lock<std::shared_mutex> rlck(object_pool_metadata_cache_mutex);
    
    auto components = str_tokenizer(pathname);
    std::string prefix;
    for (const auto& comp: components) {
        prefix = prefix + PATH_SEPARATOR + comp;
        if (object_pool_metadata_cache.find(prefix) != object_pool_metadata_cache.end()) {
            return object_pool_metadata_cache.at(prefix);
        }
    }
    rlck.unlock();

    // refresh and try again.
    refresh_object_pool_metadata_cache();
    prefix = "";
    rlck.lock();
    for (const auto& comp: components) {
        prefix = prefix + PATH_SEPARATOR + comp;
        if (object_pool_metadata_cache.find(prefix) != object_pool_metadata_cache.end()) {
            return object_pool_metadata_cache.at(prefix);
        }
    }
    return ObjectPoolMetadata<CascadeTypes...>::IV;
}

template <typename... CascadeTypes>
std::vector<std::string> ServiceClient<CascadeTypes...>::list_object_pools(bool refresh) {
    if (refresh) {
        this->refresh_object_pool_metadata_cache();
    }

    std::vector<std::string> ret;
    std::shared_lock rlck(this->object_pool_metadata_cache_mutex);
    for (auto& op:this->object_pool_metadata_cache) {
        ret.emplace_back(op.first);
    }

    return ret;
}

#ifdef ENABLE_EVALUATION

template <typename... CascadeTypes>
template <typename SubgroupType>
derecho::rpc::QueryResults<void> ServiceClient<CascadeTypes...>::dump_timestamp(const std::string& filename, const uint32_t subgroup_index, const uint32_t shard_index) {
    std::lock_guard(this->external_group_ptr_mutex);
    // call as an external client (ExternalClientCaller).
    auto& caller = external_group_ptr->template get_subgroup_caller<SubgroupType>(subgroup_index);
    node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index,shard_index);
    return caller.template p2p_send<RPC_NAME(dump_timestamp_log)>(node_id,filename);

    if (group_ptr != nullptr) {
        if (static_cast<uint32_t>(group_ptr->template get_my_shard<SubgroupType>(subgroup_index)) == shard_index) {
            auto& subgroup_handle = group_ptr->template get_subgroup<SubgroupType>(subgroup_index);
            return subgroup_handle.template ordered_send<RPC_NAME(ordered_dump_timestamp_log)>(filename);
        } else {
            auto& subgroup_handle = group_ptr->template get_nonmember_subgroup<SubgroupType>(subgroup_index);
            node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index,shard_index);
            return subgroup_handle.template p2p_send<RPC_NAME(dump_timestamp_log)>(filename);
        }
    } else {
        std::lock_guard(this->external_group_ptr_mutex);
        auto& caller = external_group_ptr->template get_subgroup_caller<SubgroupType>(subgroup_index);
        node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index,shard_index);
        return caller.template p2p_send<RPC_NAME(dump_timestamp_log)>(filename);
    }
}

template <typename... CascadeTypes>
template <typename SubgroupType>
std::vector<std::unique_ptr<derecho::rpc::QueryResults<void>>> ServiceClient<CascadeTypes...>::dump_timestamp(const std::string& filename, const std::string& object_pool_pathname) {
    auto opm = find_object_pool(object_pool_pathname);
    uint32_t subgroup_index = opm.subgroup_index;
    uint32_t shards = get_number_of_shards<SubgroupType>(subgroup_index);
    std::vector<std::unique_ptr<derecho::rpc::QueryResults<void>>> result;
    for (uint32_t shard_index = 0; shard_index < shards; shard_index ++){
        if (group_ptr != nullptr) {
            if (static_cast<uint32_t>(group_ptr->template get_my_shard<SubgroupType>(subgroup_index)) == shard_index) {
                auto& subgroup_handle = group_ptr->template get_subgroup<SubgroupType>(subgroup_index);
                auto qr = subgroup_handle.template ordered_send<RPC_NAME(ordered_dump_timestamp_log)>(filename);
                result.emplace_back(std::make_unique<derecho::rpc::QueryResults<void>>(std::move(qr)));
            } else {
                auto& subgroup_handle = group_ptr->template get_nonmember_subgroup<SubgroupType>(subgroup_index);
                node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index,shard_index);
                auto qr = subgroup_handle.template p2p_send<RPC_NAME(dump_timestamp_log)>(node_id,filename);
                result.emplace_back(std::make_unique<derecho::rpc::QueryResults<void>>(std::move(qr)));
            }
        } else {
            std::lock_guard(this->external_group_ptr_mutex);
            auto& caller = external_group_ptr->template get_subgroup_caller<SubgroupType>(subgroup_index);
            node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index,shard_index);
            auto qr = caller.template p2p_send<RPC_NAME(dump_timestamp_log)>(node_id,filename);
            result.emplace_back(std::make_unique<derecho::rpc::QueryResults<void>>(std::move(qr)));
        }
    }
    return result;
}
#endif//ENABLE_EVALUATION

template <typename... CascadeTypes>
const std::vector<std::type_index> ServiceClient<CascadeTypes...>::subgroup_type_order{typeid(CascadeTypes)...};

template <typename... CascadeTypes>
const uint32_t ServiceClient<CascadeTypes...>::invalid_subgroup_type_index = 0xffffffff;

template <typename... CascadeTypes>
template <typename SubgroupType>
uint32_t ServiceClient<CascadeTypes...>::get_subgroup_type_index() {
    uint32_t index = 0;
    while (index < subgroup_type_order.size()) {
        if ( std::type_index(typeid(SubgroupType)) == subgroup_type_order.at(index)) {
            return index;
        }
        index ++;
    }
    return invalid_subgroup_type_index;
}

template <typename... CascadeTypes>
CascadeContext<CascadeTypes...>::CascadeContext() {
    action_queue_for_multicast.initialize();
    action_queue_for_p2p.initialize();
    prefix_registry_ptr = std::make_shared<PrefixRegistry<prefix_entry_t,PATH_SEPARATOR>>();
}

template <typename... CascadeTypes>
void CascadeContext<CascadeTypes...>::construct(derecho::Group<CascadeMetadataService<CascadeTypes...>,CascadeTypes...>* group_ptr) {
    // 0 - TODO: load resources configuration here.
    // 1 - prepare the service client
    service_client = std::make_unique<ServiceClient<CascadeTypes...>>(group_ptr);
    // 2 - create data path logic loader and register the prefixes. Ideally, this part should be done in the control
    // plane, where a centralized controller should issue the control messages to do load/unload.
    // TODO: implement the control plane.
    user_defined_logic_manager = UserDefinedLogicManager<CascadeTypes...>::create(this);
    auto dfgs = DataFlowGraph::get_data_flow_graphs();
    for (auto& dfg:dfgs) {
        for (auto& vertex:dfg.vertices) {
            for (auto& edge:vertex.second.edges) {
                register_prefixes({vertex.second.pathname},edge.first,user_defined_logic_manager->get_observer(edge.first),edge.second);
            }
        }
    }
    // 3 - start the working threads
    is_running.store(true);
    uint32_t num_multicast_workers = 0;
    uint32_t num_p2p_workers = 0;
    if (derecho::hasCustomizedConfKey(CASCADE_CONTEXT_NUM_WORKERS_MULTICAST) == false) {
        dbg_default_error("{} is not found, using 0...fix it, or posting to multicast off critical data path causes deadlock.", CASCADE_CONTEXT_NUM_WORKERS_MULTICAST);
    } else {
        num_multicast_workers = derecho::getConfUInt32(CASCADE_CONTEXT_NUM_WORKERS_MULTICAST);
    }
    for (uint32_t i=0;i<num_multicast_workers;i++) {
        // off_critical_data_path_thread_pool.emplace_back(std::thread(&CascadeContext<CascadeTypes...>::workhorse,this,i));
        workhorses_for_multicast.emplace_back(
            [this,i](){
                // set cpu affinity
                if (this->resource_descriptor.multicast_ocdp_worker_to_cpu_cores.find(i)!=
                    this->resource_descriptor.multicast_ocdp_worker_to_cpu_cores.end()) {
                    cpu_set_t cpuset;
                    CPU_ZERO(&cpuset);
                    for (auto core: this->resource_descriptor.multicast_ocdp_worker_to_cpu_cores.at(i)) {
                        CPU_SET(core,&cpuset);
                    }
                    if(pthread_setaffinity_np(pthread_self(),sizeof(cpuset),&cpuset)!=0) {
                        dbg_default_warn("Failed to set affinity for cascade worker-{}", i);
                    }
                }
                // call workhorse
                this->workhorse(i,action_queue_for_multicast);
            });
    }
    if (derecho::hasCustomizedConfKey(CASCADE_CONTEXT_NUM_WORKERS_P2P) == false) {
        dbg_default_error("{} is not found, using 0...fix it, or posting to multicast off critical data path causes deadlock.", CASCADE_CONTEXT_NUM_WORKERS_MULTICAST);
    } else {
        num_p2p_workers = derecho::getConfUInt32(CASCADE_CONTEXT_NUM_WORKERS_P2P);
    }
    for (uint32_t i=0;i<num_p2p_workers;i++) {
        // off_critical_data_path_thread_pool.emplace_back(std::thread(&CascadeContext<CascadeTypes...>::workhorse,this,i));
        workhorses_for_p2p.emplace_back(
            [this,i](){
                // set cpu affinity
                if (this->resource_descriptor.p2p_ocdp_worker_to_cpu_cores.find(i)!=
                    this->resource_descriptor.p2p_ocdp_worker_to_cpu_cores.end()) {
                    cpu_set_t cpuset;
                    CPU_ZERO(&cpuset);
                    for (auto core: this->resource_descriptor.p2p_ocdp_worker_to_cpu_cores.at(i)) {
                        CPU_SET(core,&cpuset);
                    }
                    if(pthread_setaffinity_np(pthread_self(),sizeof(cpuset),&cpuset)!=0) {
                        dbg_default_warn("Failed to set affinity for cascade worker-{}", i);
                    }
                }
                // call workhorse
                this->workhorse(i,action_queue_for_p2p);
            });
    }
}

template <typename... CascadeTypes>
void CascadeContext<CascadeTypes...>::workhorse(uint32_t worker_id, struct action_queue& aq) {
    pthread_setname_np(pthread_self(), ("cascade_context_t" + std::to_string(worker_id)).c_str());
    dbg_default_trace("Cascade context workhorse[{}] started", worker_id);
    while(is_running) {
        // waiting for an action
        Action action = std::move(aq.action_buffer_dequeue(is_running));
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
void CascadeContext<CascadeTypes...>::action_queue::initialize() {
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
void CascadeContext<CascadeTypes...>::action_queue::action_buffer_enqueue(Action&& action) {
    std::unique_lock<std::mutex> lck(action_buffer_slot_mutex);
    while (ACTION_BUFFER_IS_FULL) {
        dbg_default_warn("In {}: Critical data path waits for 10 ms.", __PRETTY_FUNCTION__);
        action_buffer_slot_cv.wait_for(lck,10ms,[this]{return !ACTION_BUFFER_IS_EMPTY;});
    }

    ACTION_BUFFER_NEXT_TAIL = std::move(action);
    ACTION_BUFFER_ENQUEUE;
    action_buffer_data_cv.notify_one();
}

/* All worker threads dequeues. */
template <typename... CascadeTypes>
Action CascadeContext<CascadeTypes...>::action_queue::action_buffer_dequeue(std::atomic<bool>& is_running) {
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
void CascadeContext<CascadeTypes...>::action_queue::notify_all() {
    action_buffer_data_cv.notify_all();
    action_buffer_slot_cv.notify_all();
}

template <typename... CascadeTypes>
void CascadeContext<CascadeTypes...>::destroy() {
    dbg_default_trace("Destroying Cascade context@{:p}.",static_cast<void*>(this));
    is_running.store(false);
    action_queue_for_multicast.notify_all();
    action_queue_for_p2p.notify_all();
    for (auto& th:workhorses_for_multicast) {
        if (th.joinable()) {
            th.join();
        }
    }
    for (auto& th:workhorses_for_p2p) {
        if (th.joinable()) {
            th.join();
        }
    }
    workhorses_for_multicast.clear();
    workhorses_for_p2p.clear();
    dbg_default_trace("Cascade context@{:p} is destroyed.",static_cast<void*>(this));
}

template <typename... CascadeTypes>
ServiceClient<CascadeTypes...>& CascadeContext<CascadeTypes...>::get_service_client_ref() const {
    return *service_client.get();
}

template <typename... CascadeTypes>
void CascadeContext<CascadeTypes...>::register_prefixes(
        const std::unordered_set<std::string>& prefixes,
        const std::string& user_defined_logic_id,
        const std::shared_ptr<OffCriticalDataPathObserver>& ocdpo_ptr,
        const std::unordered_map<std::string,bool>& outputs) {
    for (const auto& prefix:prefixes) {
        prefix_registry_ptr->atomically_modify(prefix,
            [&prefix,&user_defined_logic_id,&ocdpo_ptr,&outputs](const std::shared_ptr<prefix_entry_t>& entry){
                std::shared_ptr<prefix_entry_t> new_entry;
                if (entry) {
                    new_entry = std::make_shared<prefix_entry_t>(*entry);
                } else {
                    new_entry = std::make_shared<prefix_entry_t>(prefix_entry_t{});
                }
                if (new_entry->find(user_defined_logic_id) == new_entry->end()) {
                    new_entry->emplace(user_defined_logic_id,std::pair{ocdpo_ptr,outputs});
                } else {
                    new_entry->at(user_defined_logic_id).second.insert(outputs.cbegin(),outputs.cend());
                }
                return new_entry;
            },true);
    }
}

template <typename... CascadeTypes>
void CascadeContext<CascadeTypes...>::unregister_prefixes(const std::unordered_set<std::string>& prefixes,
                                                          const std::string& user_defined_logic_id) {
    for (const auto& prefix:prefixes) {
        prefix_registry_ptr->atomically_modify(prefix,
            [&prefix,&user_defined_logic_id](const std::shared_ptr<prefix_entry_t>& entry){
                if (entry) {
                    std::shared_ptr<prefix_entry_t> new_value = std::make_shared<prefix_entry_t>(*entry);
                    new_value->erase(user_defined_logic_id);
                    return new_value;
                } else {
                    return entry;
                }
            }
        );
    }
}

/* Note: On the same hardware, copying a shared_ptr spends ~7.4ns, and copying a raw pointer spends ~1.8 ns*/
template <typename... CascadeTypes>
match_results_t CascadeContext<CascadeTypes...>::get_prefix_handlers(const std::string& path) {

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
bool CascadeContext<CascadeTypes...>::post(Action&& action, bool is_trigger) {
    dbg_default_trace("Posting an action to Cascade context@{:p}.", static_cast<void*>(this));
    if (is_running) {
        if (is_trigger) {
            action_queue_for_p2p.action_buffer_enqueue(std::move(action));
        } else {
            action_queue_for_multicast.action_buffer_enqueue(std::move(action));
        }
    } else {
        dbg_default_warn("Failed to post to Cascade context@{:p} because it is not running.", static_cast<void*>(this));
        return false;
    }
    dbg_default_trace("Action posted to Cascade context@{:p}.", static_cast<void*>(this));
    return true;
}

template <typename... CascadeTypes>
size_t CascadeContext<CascadeTypes...>::action_queue_length_p2p() {
    return (action_queue_for_p2p.action_buffer_tail - action_queue_for_multicast.action_buffer_head + ACTION_BUFFER_SIZE)%ACTION_BUFFER_SIZE;
}

template <typename... CascadeTypes>
size_t CascadeContext<CascadeTypes...>::action_queue_length_multicast() {
    return (action_queue_for_multicast.action_buffer_tail - action_queue_for_multicast.action_buffer_head + ACTION_BUFFER_SIZE)%ACTION_BUFFER_SIZE;
}

template <typename... CascadeTypes>
CascadeContext<CascadeTypes...>::~CascadeContext() {
    destroy();
}


}
}
