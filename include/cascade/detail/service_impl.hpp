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

/**
 * parse_json_subgroup_policy()
 *
 * Generate a single-type subgroup allocation policy from json string
 * @param json_config subgroup configuration represented in json format.
 * @return SubgroupAllocationPolicy
 */
SubgroupAllocationPolicy parse_json_subgroup_policy(const json&);

template <typename CascadeType>
void populate_policy_by_subgroup_type_map(
        std::map<std::type_index,std::variant<SubgroupAllocationPolicy, CrossProductPolicy>> &dsa_map,
        const json& layout, int type_idx) {
    dsa_map.emplace(std::type_index(typeid(CascadeType)),parse_json_subgroup_policy(layout[type_idx]));
}

template <typename FirstCascadeType, typename SecondCascadeType, typename... RestCascadeTypes>
void populate_policy_by_subgroup_type_map(
        std::map<std::type_index,std::variant<SubgroupAllocationPolicy, CrossProductPolicy>> &dsa_map,
        const json& layout, int type_idx) {
    dsa_map.emplace(std::type_index(typeid(FirstCascadeType)),parse_json_subgroup_policy(layout[type_idx]));
    populate_policy_by_subgroup_type_map<SecondCascadeType, RestCascadeTypes...>(dsa_map,layout,type_idx+1);
}

/**
 * Generate a derecho::SubgroupInfo object from layout
 *
 * @param layout - user provided layout in json format
 * @return derecho::SbugroupInfo object for Group constructor.
 */
template <typename... CascadeTypes>
derecho::SubgroupInfo generate_subgroup_info(const json& layout) {
    std::map<std::type_index,std::variant<SubgroupAllocationPolicy, CrossProductPolicy>> dsa_map;
    populate_policy_by_subgroup_type_map<CascadeMetadataService<CascadeTypes...>,CascadeTypes...>(dsa_map,layout,0);
    return derecho::SubgroupInfo(derecho::DefaultSubgroupAllocator(dsa_map));
}

template <typename CascadeType>
derecho::Factory<CascadeType> factory_wrapper(ICascadeContext* context_ptr, derecho::cascade::Factory<CascadeType> cascade_factory) {
    return [context_ptr,cascade_factory](persistent::PersistentRegistry *pr, subgroup_id_t subgroup_id) {
            return cascade_factory(pr,subgroup_id,context_ptr);
        };
}

template <typename... CascadeTypes>
Service<CascadeTypes...>::Service(const json& layout,
                                  const std::vector<DeserializationContext*>& dsms,
                                  derecho::cascade::Factory<CascadeMetadataService<CascadeTypes...>> metadata_service_factory,
                                  derecho::cascade::Factory<CascadeTypes>... factories) {
    // STEP 1 - load configuration
    derecho::SubgroupInfo si = generate_subgroup_info<CascadeTypes...>(layout);
    dbg_default_trace("subgroups info created from layout.");
    // STEP 2 - setup cascade context
    context = std::make_unique<CascadeContext<CascadeTypes...>>();
    std::vector<DeserializationContext*> new_dsms(dsms);
    new_dsms.emplace_back(context.get());
    // STEP 3 - create derecho group
    group = std::make_unique<derecho::Group<CascadeMetadataService<CascadeTypes...>,CascadeTypes...>>(
                CallbackSet{},
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
void Service<CascadeTypes...>::start(const json& layout, const std::vector<DeserializationContext*>& dsms, 
        derecho::cascade::Factory<CascadeMetadataService<CascadeTypes...>> metadata_factory,
        derecho::cascade::Factory<CascadeTypes>... factories) {
    if (!service_ptr) {
        service_ptr = std::make_unique<Service<CascadeTypes...>>(layout, dsms, metadata_factory, factories...);
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
    std::string object_pool_pathname = get_pathname(key);
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
        throw new derecho::derecho_exception("Unknown member selection policy:" + std::to_string(static_cast<unsigned int>(policy)) );
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
    std::tie(subgroup_index,shard_index) = this->template key_to_subgroup_and_shard_index<SubgroupType>(value.get_key_ref());
    return put<SubgroupType>(value,subgroup_index,shard_index);
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
    std::tie(subgroup_index,shard_index) = this->template key_to_subgroup_and_shard_index<SubgroupType>(value.get_key_ref());
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
    std::tie(subgroup_index,shard_index) = this->template key_to_subgroup_and_shard_index<SubgroupType>(key);
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
    std::tie(subgroup_index,shard_index) = this->template key_to_subgroup_and_shard_index<SubgroupType>(key);
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
    std::tie(subgroup_index,shard_index) = this->template key_to_subgroup_and_shard_index<SubgroupType>(key);
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
    std::tie(subgroup_index,shard_index) = this->template key_to_subgroup_and_shard_index<SubgroupType>(key);
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
    std::tie(subgroup_index,shard_index) = this->template key_to_subgroup_and_shard_index<SubgroupType>(key);
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
        const std::string& id, const uint32_t subgroup_index,
        const sharding_policy_t sharding_policy, const std::unordered_map<std::string,uint32_t>& object_locations) {
    uint32_t subgroup_type_index = ObjectPoolMetadata<CascadeTypes...>::template get_subgroup_type_index<SubgroupType>();
    if (subgroup_type_index == ObjectPoolMetadata<CascadeTypes...>::invalid_subgroup_type_index) {
        dbg_default_crit("Create object pool failed because of invalid SubgroupType:{}", typeid(SubgroupType).name());
        throw new derecho::derecho_exception(std::string("Create object pool failed because SubgroupType is invalid:")+typeid(SubgroupType).name());
    }
    ObjectPoolMetadata<CascadeTypes...> opm(id,subgroup_type_index,subgroup_index,sharding_policy,object_locations,false);
    // clear local cache entry.
    std::shared_lock<std::shared_mutex> rlck(object_pool_metadata_cache_mutex);
    if (object_pool_metadata_cache.find(id)==object_pool_metadata_cache.end()) {
        rlck.unlock();
    } else {
        rlck.unlock();
        std::unique_lock<std::shared_mutex> wlck(object_pool_metadata_cache_mutex);
        object_pool_metadata_cache.erase(id);
    }
    // determine the shard index by hashing
    uint32_t metadata_service_shard_index = std::hash<std::string>{}(id) % this->template get_number_of_shards<CascadeMetadataService<CascadeTypes...>>(METADATA_SERVICE_SUBGROUP_INDEX);

    return this->template put<CascadeMetadataService<CascadeTypes...>>(opm,METADATA_SERVICE_SUBGROUP_INDEX,metadata_service_shard_index);
}

template <typename... CascadeTypes>
derecho::rpc::QueryResults<std::tuple<persistent::version_t,uint64_t>> ServiceClient<CascadeTypes...>::remove_object_pool(const std::string& id) {
    // determine the shard index by hashing
    uint32_t metadata_service_shard_index = std::hash<std::string>{}(id) % this->template get_number_of_shards<CascadeMetadataService<CascadeTypes...>>(METADATA_SERVICE_SUBGROUP_INDEX);


    // check if this object pool exist in metadata service.
    auto opm = find_object_pool(id);
    // remove it from local cache.
    std::shared_lock<std::shared_mutex> rlck(object_pool_metadata_cache_mutex);
    if (object_pool_metadata_cache.find(id) == object_pool_metadata_cache.end()) {
        // no entry in cache
        rlck.unlock();
    } else {
        // remove from cache
        rlck.unlock();
        std::unique_lock<std::shared_mutex> wlck(object_pool_metadata_cache_mutex);
        object_pool_metadata_cache.erase(id);
        wlck.unlock();
    }
    if (opm.is_valid() && !opm.is_null() && !opm.deleted) {
        opm.deleted = true;
        opm.set_previous_version(CURRENT_VERSION,opm.version); // only check previous_version_by_key
        return this->template put<CascadeMetadataService<CascadeTypes...>>(opm,METADATA_SERVICE_SUBGROUP_INDEX,metadata_service_shard_index);
    }

    // we didn't find any entry with "id", but we do the normal 'remove', which has no effect but return a version.
    dbg_default_warn("deleteing a non-existing objectpool:{}.", id);
    return this->template remove<CascadeMetadataService<CascadeTypes...>>(id,METADATA_SERVICE_SUBGROUP_INDEX,metadata_service_shard_index);
}

template <typename... CascadeTypes>
ObjectPoolMetadata<CascadeTypes...> ServiceClient<CascadeTypes...>::find_object_pool(const std::string& id) {
    std::shared_lock<std::shared_mutex> rlck(object_pool_metadata_cache_mutex);
    if (object_pool_metadata_cache.find(id) == object_pool_metadata_cache.end()) {
        rlck.unlock();
        uint32_t metadata_service_shard_index = std::hash<std::string>{}(id) % this->template get_number_of_shards<CascadeMetadataService<CascadeTypes...>>(METADATA_SERVICE_SUBGROUP_INDEX);
        auto result = this->template get<CascadeMetadataService<CascadeTypes...>>(id,CURRENT_VERSION,METADATA_SERVICE_SUBGROUP_INDEX,metadata_service_shard_index);
        for (auto& reply_future:result.get()) {
            auto opm = reply_future.second.get();
            // update the metadata cache.
            std::unique_lock<std::shared_mutex> wlck(object_pool_metadata_cache_mutex);
            object_pool_metadata_cache[id] = opm;
            return opm;
        }
        // not found, return an invalid one.
        return ObjectPoolMetadata<CascadeTypes...>::IV;
    } else {
        return object_pool_metadata_cache.at(id);
    }
}

template <typename... CascadeTypes>
std::vector<std::string> ServiceClient<CascadeTypes...>::list_object_pool_ids(bool refresh) {
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


template <typename... CascadeTypes>
CascadeContext<CascadeTypes...>::CascadeContext() {
    action_queue_for_multicast.initialize();
    action_queue_for_p2p.initialize();
    prefix_registry_ptr = std::make_shared<
                                std::unordered_map<
                                    std::string, 
                                    std::unordered_map<
                                        std::string,
                                        std::pair<
                                            std::shared_ptr<OffCriticalDataPathObserver>,
                                            std::unordered_map<std::string,bool>
                                        >
                                    >
                                >
                          >();
}

template <typename... CascadeTypes>
void CascadeContext<CascadeTypes...>::construct(derecho::Group<CascadeMetadataService<CascadeTypes...>,CascadeTypes...>* group_ptr) {
    // 0 - TODO: load resources configuration here.
    // 1 - prepare the service client
    service_client = std::make_unique<ServiceClient<CascadeTypes...>>(group_ptr);
    // 2 - create data path logic loader and register the prefixes. Ideally, this part should be done in the control
    // plane, where a centralized controller should issue the control messages to do load/unload.
    // TODO: implement the control plane.
    data_path_logic_manager = DataPathLogicManager<CascadeTypes...>::create(this);
    auto dfgs = DataFlowGraph::get_data_flow_graphs();
    for (auto& dfg:dfgs) {
        for (auto& vertex:dfg.vertices) {
            for (auto& edge:vertex.second.edges) {
                register_prefixes({vertex.second.object_pool_id},edge.first,data_path_logic_manager->get_observer(edge.first),edge.second);
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
        const std::string& data_path_logic_id,
        const std::shared_ptr<OffCriticalDataPathObserver>& ocdpo_ptr,
        const std::unordered_map<std::string,bool>& outputs) {
    // 0 - write lock the prefix_registry_ptr to exclude concurrent writers
    std::unique_lock lck(prefix_registry_ptr_mutex);
    // 1 - copy-construct a new prefix_registry
    auto new_pr = std::make_shared<std::unordered_map<std::string, std::unordered_map<std::string,std::pair<std::shared_ptr<OffCriticalDataPathObserver>,std::unordered_map<std::string,bool>>>>>(*prefix_registry_ptr);

    // 2 - insert prefixes
    for (auto& prefix: prefixes) {
        if (new_pr->find(prefix) == new_pr->end()){
            new_pr->emplace(prefix,
                            std::unordered_map<std::string,
                                               std::pair<std::shared_ptr<OffCriticalDataPathObserver>,
                                                         std::unordered_map<std::string,bool>>
                                              >{});
        }
        new_pr->at(prefix).emplace(data_path_logic_id,std::pair{ocdpo_ptr,outputs});
    }

    // 3 - flip the prefix_registry
    // lock on the rw mutex to coordinate with the read.
    // In C++20, it is possible to define a std::atomic<std::shared_ptr<T>> to allow atomic access to the shared
    // pointer. However, we support C++17 and use a read-write lock here. 
    std::unique_lock rwlck(prefix_registry_ptr_rw_mutex);
    // If the reader does not hold a copy of prefix_registry_ptr, the old prefix_registry is released here.
    prefix_registry_ptr = new_pr;
}

template <typename... CascadeTypes>
void CascadeContext<CascadeTypes...>::unregister_prefixes(const std::unordered_set<std::string>& prefixes,
                                                          const std::string& data_path_logic_id) {
    std::unique_lock lck(prefix_registry_ptr_mutex);
    // 1 - copy-construct a new prefix_registry
    auto new_pr = std::make_shared<std::unordered_map<std::string, std::unordered_map<std::string,std::pair<std::shared_ptr<OffCriticalDataPathObserver>,std::unordered_map<std::string,bool>>>>>(*prefix_registry_ptr);

    // 2 - remove prefixes
    for (auto& prefix: prefixes) {
        if (new_pr->find(prefix) == new_pr->end()){
            dbg_default_warn("In {}, erase an unknown prefix:'{}'.",__PRETTY_FUNCTION__,prefix);
        }
        new_pr->at(prefix).erase(data_path_logic_id);
    }

    // 3 - flip the prefix_registry
    // lock on the rw mutex to coordinate with the read.
    // In C++20, it is possible to define a std::atomic<std::shared_ptr<T>> to allow atomic access to the shared
    // pointer. However, we support C++17 and use a read-write lock here.
    std::unique_lock rwlck(prefix_registry_ptr_rw_mutex);
    // If the reader does not hold a copy of prefix_registry_ptr, the old prefix registry is destructed here.
    prefix_registry_ptr = new_pr;
}

/* Note: On the same hardware, copying a shared_ptr spends ~7.4ns, and copying a raw pointer spends ~1.8 ns*/
template <typename... CascadeTypes>
std::unordered_map<std::string,std::pair<std::shared_ptr<OffCriticalDataPathObserver>,std::unordered_map<std::string,bool>>> CascadeContext<CascadeTypes...>::get_prefix_handlers(const std::string& prefix) {
    // 1 - copy the shared ptr
    std::shared_lock rlck(prefix_registry_ptr_rw_mutex);
    std::shared_ptr<std::unordered_map<std::string, std::unordered_map<std::string,std::pair<std::shared_ptr<OffCriticalDataPathObserver>,std::unordered_map<std::string,bool>>>>> pr = prefix_registry_ptr;
    rlck.unlock();

    // 2 - read the shared ptr
    if (pr->find(prefix)==pr->end()) {
        return {};
    }
    return pr->at(prefix);

    // 3 - If the prefix_registry has been changed by the writer after step 1, releasing pr destructs the old prefix
    // registry.
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
