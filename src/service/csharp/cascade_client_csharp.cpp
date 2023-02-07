#ifndef __EXTERNAL_CLIENT__
#define __WITHOUT_SERVICE_SINGLETONS__
#endif//not __EXTERNAL_CLIENT__

#include <cascade/cascade.hpp>
#include <cascade/service_client_api.hpp>
#include <derecho/core/detail/rpc_utils.hpp>
#include <derecho/persistent/PersistentInterface.hpp>
#include <string>
#include <vector>

// ----------------
// Regular C++ code
// ----------------

using namespace derecho::cascade;

#define on_all_subgroup_type(x, ft, ...)                      \
    if((x) == "VolatileCascadeStoreWithStringKey") {          \
        ft<VolatileCascadeStoreWithStringKey>(__VA_ARGS__);   \
    } else if((x) == "PersistentCascadeStoreWithStringKey") { \
        ft<PersistentCascadeStoreWithStringKey>(__VA_ARGS__); \
    } else if((x) == "TriggerCascadeNoStoreWithStringKey") {  \
        ft<TriggerCascadeNoStoreWithStringKey>(__VA_ARGS__);  \
    } else {                                                  \
        print_red("unknown subgroup type:" + x);              \
    }

/**
 * The following names have to match ShardMemberSelectionPolicy defined in include/cascade/service.hpp
 */
static const char* policy_names[] = {
        "FirstMember",
        "LastMember",
        "Random",
        "FixedRandom",
        "RoundRobin",
        "KeyHashing",
        "UserSpecified",
        nullptr};

/**
    For error messages.
*/
extern "C" static void print_red(const char* msg) {
    std::cout << "\033[1;31m"
              << msg
              << "\033[0m" << std::endl;
}

extern "C" static ServiceClientAPI& get_service_client_ref() {
    return ServiceClientAPI::get_service_client();
}

extern "C" static uint32_t get_subgroup_index_vcss(ServiceClientAPI& capi) {
    return capi.get_subgroup_type_index<VolatileCascadeStoreWithStringKey>();
}

extern "C" static uint32_t get_my_id(ServiceClientAPI& capi) {
    return capi.get_my_id();
}

// ====================
// Below here is currently copied from Python code.
// ====================

/**
    Put objects into cascade store.
    Please note that if subgroup_index is not specified, we will use the object_pool API.

    @param capi             the service client API for this client.
    @param object           to be put in for coressponding key
    @param subgroup_index
    @param shard_index
    @return QueryResultsStore that handles the tuple of version and ts_us.
*/
template <typename SubgroupType>
auto put(ServiceClientAPI& capi, const typename SubgroupType::ObjectType& obj, uint32_t subgroup_index = UINT32_MAX, uint32_t shard_index = 0) {
    derecho::rpc::QueryResults<std::tuple<persistent::version_t, uint64_t>> result = (subgroup_index == UINT32_MAX) ? capi.put(obj) : capi.template put<SubgroupType>(obj, subgroup_index, shard_index);

    QueryResultsStore<std::tuple<persistent::version_t, uint64_t>, std::vector<long>>* s = new QueryResultsStore<std::tuple<persistent::version_t, uint64_t>, std::vector<long>>(std::move(result), bundle_f);
    return py::cast(s);
}

/**
    Remove objects from cascade store.
    Please note that if subgroup_index is not specified, we will use the object_pool API.

    @param capi the service client API for this client.
    @param key key to remove value from
    @param subgroup_index 
    @param shard_index
    @return QueryResultsStore that handles the tuple of version and ts_us.
*/
template <typename SubgroupType>
auto remove(ServiceClientAPI& capi, const char* key, uint32_t subgroup_index = UINT32_MAX, uint32_t shard_index = 0) {
    if constexpr(std::is_integral<typename SubgroupType::KeyType>::value) {
        derecho::rpc::QueryResults<std::tuple<persistent::version_t, uint64_t>> result = std::move(capi.template remove<SubgroupType>(static_cast<uint64_t>(std::stol(key)), subgroup_index, shard_index));
        QueryResultsStore<std::tuple<persistent::version_t, uint64_t>, std::vector<long>>* s = new QueryResultsStore<std::tuple<persistent::version_t, uint64_t>, std::vector<long>>(std::move(result), bundle_f);
        return py::cast(s);

    } else if constexpr(std::is_convertible<typename SubgroupType::KeyType, std::string>::value) {
        derecho::rpc::QueryResults<std::tuple<persistent::version_t, uint64_t>> result = (subgroup_index == UINT32_MAX) ? capi.remove(key) : capi.template remove<SubgroupType>(key, subgroup_index, shard_index);
        QueryResultsStore<std::tuple<persistent::version_t, uint64_t>, std::vector<long>>* s = new QueryResultsStore<std::tuple<persistent::version_t, uint64_t>, std::vector<long>>(std::move(result), bundle_f);
        return py::cast(s);

    } else {
        print_red(std::string("Unhandled KeyType:") + typeid(typename SubgroupType::KeyType).name());
        return;
    }
}

/**
    Get objects from cascade store.
    @param capi the service client API for this client.
    @param key key to remove value from
    @param ver version of the object you want to get.
    @param stable using stable get or not.
    @param subgroup_index 
    @param shard_index
    @return QueryResultsStore that handles the return type.
*/
template <typename SubgroupType>
auto get(ServiceClientAPI& capi, const std::string& key, persistent::version_t ver, bool stable, uint32_t subgroup_index = 0, uint32_t shard_index = 0) {
    derecho::rpc::QueryResults<const typename SubgroupType::ObjectType> result = capi.template get<SubgroupType>(key, ver, stable, subgroup_index, shard_index);
    // check_get_result(result);
    auto s = new QueryResultsStore<const typename SubgroupType::ObjectType, py::dict>(std::move(result), object_unwrapper);
    return py::cast(s);
}
