#include <derecho/core/derecho.hpp>
#include <map>
#include <typeindex>
#include <variant>
#include <vector>

namespace derecho {
namespace cascade {

using derecho::CrossProductPolicy;
using derecho::ShardAllocationPolicy;
using derecho::SubgroupAllocationPolicy;

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
        std::map<std::type_index, std::variant<SubgroupAllocationPolicy, CrossProductPolicy>>& dsa_map,
        const json& layout, int type_idx) {
    dsa_map.emplace(std::type_index(typeid(CascadeType)), parse_json_subgroup_policy(layout[type_idx]));
}

template <typename FirstCascadeType, typename SecondCascadeType, typename... RestCascadeTypes>
void populate_policy_by_subgroup_type_map(
        std::map<std::type_index, std::variant<SubgroupAllocationPolicy, CrossProductPolicy>>& dsa_map,
        const json& layout, int type_idx) {
    dbg_default_trace("prepare to call parse_json_subgroup_policy");
    dsa_map.emplace(std::type_index(typeid(FirstCascadeType)), 
    parse_json_subgroup_policy(layout[type_idx]));
    populate_policy_by_subgroup_type_map<SecondCascadeType, RestCascadeTypes...>(dsa_map, layout, type_idx + 1);
}

/**
 * Generate a derecho::SubgroupInfo object from layout
 *
 * @param layout - user provided layout in json format
 * @return derecho::SbugroupInfo object for Group constructor.
 */
template <typename... CascadeTypes>
derecho::SubgroupInfo generate_subgroup_info(const json& layout) {
    std::map<std::type_index, std::variant<SubgroupAllocationPolicy, CrossProductPolicy>> dsa_map;
    populate_policy_by_subgroup_type_map<CascadeTypes...>(dsa_map, layout, 0);
    return derecho::SubgroupInfo(derecho::DefaultSubgroupAllocator(dsa_map));
}

template <typename... CascadeTypes>
Service<CascadeTypes...>::Service(const json& layout, const std::vector<DeserializationContext*>& dsms, derecho::Factory<CascadeTypes>... factories) {
    // STEP 1 - load configuration
    derecho::SubgroupInfo si = generate_subgroup_info<CascadeTypes...>(layout);
    dbg_default_trace("subgroups info created from layout.");
    // STEP 2 - create derecho group
    group = std::make_unique<derecho::Group<CascadeTypes...>>(
            CallbackSet{},
            si,
            dsms,
            std::vector<derecho::view_upcall_t>{},
            factories...);
    dbg_default_trace("joined group.");
    // STEP 3 - create service thread
    this->_is_running = true;
    service_thread = std::thread(&Service<CascadeTypes...>::run, this);
    dbg_default_trace("created daemon thread.");
}

template <typename... CascadeTypes>
void Service<CascadeTypes...>::run() {
    std::unique_lock<std::mutex> lck(this->service_control_mutex);
    this->service_control_cv.wait(lck, [this]() { return !this->_is_running; });
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
    if(is_joining && this->service_thread.joinable()) {
        this->service_thread.join();
    }
}

template <typename... CascadeTypes>
void Service<CascadeTypes...>::join() {
    if(this->service_thread.joinable()) {
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
void Service<CascadeTypes...>::start(const json& layout, const std::vector<DeserializationContext*>& dsms, derecho::Factory<CascadeTypes>... factories) {
    if(!service_ptr) {
        service_ptr = std::make_unique<Service<CascadeTypes...>>(layout, dsms, factories...);
    }
}

template <typename... CascadeTypes>
void Service<CascadeTypes...>::shutdown(bool is_joining) {
    if(service_ptr) {
        if(service_ptr->is_running()) {
            service_ptr->stop(is_joining);
        }
    }
}

template <typename... CascadeTypes>
void Service<CascadeTypes...>::wait() {
    if(service_ptr) {
        service_ptr->join();
    }
}

template <typename... CascadeTypes>
std::vector<node_id_t> ServiceClient<CascadeTypes...>::get_members() {
    return external_group.get_members();
}

template <typename... CascadeTypes>
std::vector<node_id_t> ServiceClient<CascadeTypes...>::get_shard_members(derecho::subgroup_id_t subgroup_id, uint32_t shard_index) {
    return external_group.get_shard_members(subgroup_id, shard_index);
}

template <typename... CascadeTypes>
template <typename SubgroupType>
std::vector<node_id_t> ServiceClient<CascadeTypes...>::get_shard_members(uint32_t subgroup_index, uint32_t shard_index) {
    return external_group.template get_shard_members<SubgroupType>(subgroup_index, shard_index);
}

template <typename... CascadeTypes>
template <typename SubgroupType>
uint32_t ServiceClient<CascadeTypes...>::get_number_of_subgroups() {
    return external_group.template get_number_of_subgroups<SubgroupType>();
}

template <typename... CascadeTypes>
uint32_t ServiceClient<CascadeTypes...>::get_number_of_shards(derecho::subgroup_id_t subgroup_id) {
    return external_group.get_number_of_shards(subgroup_id);
}

template <typename... CascadeTypes>
template <typename SubgroupType>
uint32_t ServiceClient<CascadeTypes...>::get_number_of_shards(uint32_t subgroup_index) {
    return external_group.template get_number_of_shards<SubgroupType>(subgroup_index);
}

template <typename... CascadeTypes>
template <typename SubgroupType>
void ServiceClient<CascadeTypes...>::set_member_selection_policy(uint32_t subgroup_index, uint32_t shard_index,
                                                                 ShardMemberSelectionPolicy policy, node_id_t user_specified_node_id) {
    // write lock policies
    std::unique_lock wlck(this->member_selection_policies_mutex);
    // update map
    this->member_selection_policies[std::make_tuple(std::type_index(typeid(SubgroupType)), subgroup_index, shard_index)] = std::make_tuple(policy, user_specified_node_id);
}

template <typename... CascadeTypes>
template <typename SubgroupType>
std::tuple<ShardMemberSelectionPolicy, node_id_t> ServiceClient<CascadeTypes...>::get_member_selection_policy(
        uint32_t subgroup_index, uint32_t shard_index) {
    // read lock policies
    std::shared_lock rlck(this->member_selection_policies_mutex);
    auto key = std::make_tuple(std::type_index(typeid(SubgroupType)), subgroup_index, shard_index);
    // read map
    if(member_selection_policies.find(key) != member_selection_policies.end()) {
        return member_selection_policies.at(key);
    } else {
        return std::make_tuple(DEFAULT_SHARD_MEMBER_SELECTION_POLICY, INVALID_NODE_ID);
    }
}

template <typename... CascadeTypes>
template <typename SubgroupType>
void ServiceClient<CascadeTypes...>::refresh_member_cache_entry(uint32_t subgroup_index,
                                                                uint32_t shard_index) {
    auto key = std::make_tuple(std::type_index(typeid(SubgroupType)), subgroup_index, shard_index);
    auto members = get_shard_members<SubgroupType>(subgroup_index, shard_index);
    std::shared_lock rlck(member_cache_mutex);
    if(member_cache.find(key) == member_cache.end()) {
        rlck.unlock();
        std::unique_lock wlck(member_cache_mutex);
        member_cache[key] = members;
    } else {
        member_cache[key].swap(members);
    }
}

template <typename... CascadeTypes>
template <typename SubgroupType>
node_id_t ServiceClient<CascadeTypes...>::pick_member_by_policy(uint32_t subgroup_index,
                                                                uint32_t shard_index,
                                                                bool retry) {
    ShardMemberSelectionPolicy policy;
    node_id_t last_specified_node_id_or_index;

    std::tie(policy, last_specified_node_id_or_index) = get_member_selection_policy<SubgroupType>(subgroup_index, shard_index);

    if(policy == ShardMemberSelectionPolicy::UserSpecified) {
        return last_specified_node_id_or_index;
    }

    auto key = std::make_tuple(std::type_index(typeid(SubgroupType)), subgroup_index, shard_index);

    if(member_cache.find(key) == member_cache.end() || retry) {
        refresh_member_cache_entry<SubgroupType>(subgroup_index, shard_index);
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
            node_id = member_cache.at(key)[get_time() % member_cache.at(key).size()];  // use time as random source.
            break;
        case ShardMemberSelectionPolicy::FixedRandom:
            if(node_id == INVALID_NODE_ID || retry) {
                node_id = member_cache.at(key)[get_time() % member_cache.at(key).size()];  // use time as random source.
            }
            break;
        case ShardMemberSelectionPolicy::RoundRobin: {
            std::shared_lock rlck(member_selection_policies_mutex);
            node_id = static_cast<uint32_t>(node_id + 1) % member_cache.at(key).size();
            auto new_policy = std::make_tuple(ShardMemberSelectionPolicy::RoundRobin, node_id);
            member_selection_policies[key].swap(new_policy);
        }
            node_id = member_cache.at(key)[node_id];
            break;
        default:
            throw new derecho::derecho_exception("Unknown member selection policy:" + std::to_string(static_cast<unsigned int>(policy)));
    }

    return node_id;
}

template <typename... CascadeTypes>
template <typename SubgroupType>
derecho::rpc::QueryResults<std::tuple<persistent::version_t, uint64_t>> ServiceClient<CascadeTypes...>::put(
        const typename SubgroupType::ObjectType& value,
        uint32_t subgroup_index,
        uint32_t shard_index) {
    auto& caller = external_group.template get_subgroup_caller<SubgroupType>(subgroup_index);
    node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index, shard_index);
    return caller.template p2p_send<RPC_NAME(put)>(node_id, value);
}

template <typename... CascadeTypes>
template <typename SubgroupType>
derecho::rpc::QueryResults<std::tuple<persistent::version_t, uint64_t>> ServiceClient<CascadeTypes...>::remove(
        const typename SubgroupType::KeyType& key,
        uint32_t subgroup_index,
        uint32_t shard_index) {
    auto& caller = external_group.template get_subgroup_caller<SubgroupType>(subgroup_index);
    node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index, shard_index);
    return caller.template p2p_send<RPC_NAME(remove)>(node_id, key);
}

template <typename... CascadeTypes>
template <typename SubgroupType>
derecho::rpc::QueryResults<const typename SubgroupType::ObjectType> ServiceClient<CascadeTypes...>::get(
        const typename SubgroupType::KeyType& key,
        const persistent::version_t& version,
        uint32_t subgroup_index,
        uint32_t shard_index) {
    auto& caller = external_group.template get_subgroup_caller<SubgroupType>(subgroup_index);
    node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index, shard_index);
    return caller.template p2p_send<RPC_NAME(get)>(node_id, key, version, false);
}

template <typename... CascadeTypes>
template <typename SubgroupType>
derecho::rpc::QueryResults<const typename SubgroupType::ObjectType> ServiceClient<CascadeTypes...>::get_by_time(
        const typename SubgroupType::KeyType& key,
        const uint64_t& ts_us,
        uint32_t subgroup_index,
        uint32_t shard_index) {
    auto& caller = external_group.template get_subgroup_caller<SubgroupType>();
    node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index, shard_index);
    return caller.template p2p_send<RPC_NAME(get_by_time)>(node_id, key, ts_us);
}

template <typename... CascadeTypes>
template <typename SubgroupType>
derecho::rpc::QueryResults<uint64_t> ServiceClient<CascadeTypes...>::get_size(
        const typename SubgroupType::KeyType& key,
        const persistent::version_t& version,
        uint32_t subgroup_index,
        uint32_t shard_index) {
    auto& caller = external_group.template get_subgroup_caller<SubgroupType>(subgroup_index);
    node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index, shard_index);
    return caller.template p2p_send<RPC_NAME(get_size)>(node_id, key, version, false);
}

template <typename... CascadeTypes>
template <typename SubgroupType>
derecho::rpc::QueryResults<uint64_t> ServiceClient<CascadeTypes...>::get_size_by_time(
        const typename SubgroupType::KeyType& key,
        const uint64_t& ts_us,
        uint32_t subgroup_index,
        uint32_t shard_index) {
    auto& caller = external_group.template get_subgroup_caller<SubgroupType>(subgroup_index);
    node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index, shard_index);
    return caller.template p2p_send<RPC_NAME(get_size_by_time)>(node_id, key, ts_us);
}

template <typename... CascadeTypes>
template <typename SubgroupType>
derecho::rpc::QueryResults<std::vector<typename SubgroupType::KeyType>> ServiceClient<CascadeTypes...>::list_keys(
        const persistent::version_t& version,
        uint32_t subgroup_index,
        uint32_t shard_index) {
    auto& caller = external_group.template get_subgroup_caller<SubgroupType>(subgroup_index);
    node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index, shard_index);
    return caller.template p2p_send<RPC_NAME(list_keys)>(node_id, version);
}

template <typename... CascadeTypes>
template <typename SubgroupType>
derecho::rpc::QueryResults<std::vector<typename SubgroupType::KeyType>> ServiceClient<CascadeTypes...>::list_keys_by_time(
        const uint64_t& ts_us,
        uint32_t subgroup_index,
        uint32_t shard_index) {
    auto& caller = external_group.template get_subgroup_caller<SubgroupType>(subgroup_index);
    node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index, shard_index);
    return caller.template p2p_send<RPC_NAME(list_keys_by_time)>(node_id, ts_us);
}

template <typename... CascadeTypes>
template <typename SubgroupType>
void ServiceClient<CascadeTypes...>::submit_predicate(const std::string& key, const std::string& predicate_str, const bool inplace, uint32_t subgroup_index, uint32_t shard_index) {
    auto& caller = external_group.template get_subgroup_caller<SubgroupType>();
    node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index, shard_index);
    caller.template p2p_send<RPC_NAME(submit_predicate)>(node_id, key, predicate_str, inplace);
}

template <typename... CascadeTypes>
template <typename SubgroupType>
void ServiceClient<CascadeTypes...>::change_predicate(const std::string& key, uint32_t subgroup_index, uint32_t shard_index) {
    auto& caller = external_group.template get_subgroup_caller<SubgroupType>();
    node_id_t node_id = pick_member_by_policy<SubgroupType>(subgroup_index, shard_index);
    caller.template p2p_send<RPC_NAME(change_predicate)>(node_id, key);
}

}  // namespace cascade
}  // namespace derecho
