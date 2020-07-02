#include <vector>
#include <map>
#include <typeindex>
#include <variant>
#include <derecho/core/derecho.hpp>

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
    populate_policy_by_subgroup_type_map<CascadeTypes...>(dsa_map,layout,0);
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
void Service<CascadeTypes...>::start(const json& layout, const std::vector<DeserializationContext*>& dsms, derecho::Factory<CascadeTypes>... factories) {
    if (!service_ptr) {
        service_ptr = std::make_unique<Service<CascadeTypes...>>(layout, dsms, factories...);
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

}
}
