#ifndef __EXTERNAL_CLIENT__
#define __WITHOUT_SERVICE_SINGLETONS__
#endif//not __EXTERNAL_CLIENT__

// Platform Invoke (P/Invoke) requires C-style linkage for all exported, unmanaged
// functions. As such, we declare each function as extern "C"
#define EXPORT extern "C"

#include <cascade/cascade.hpp>
#include <cascade/service_types.hpp>
#include <cascade/service_client_api.hpp>
#include <derecho/core/detail/rpc_utils.hpp>
#include <derecho/persistent/PersistentInterface.hpp>
#include <string>

// ----------------
// Regular C++ code
// ----------------

using namespace derecho::cascade;

/*
 * Value struct to be marshalled into C# for object metadata. 
 */
struct ObjectProperties {
    const char* key;
    const uint8_t* bytes;
    std::size_t bytes_size;
    int64_t version;
    uint64_t timestamp;
    int64_t previous_version;
    int64_t previous_version_by_key;
    uint64_t message_id;
};

struct StdVectorWrapper {
    void* data;
    uint64_t length;
};

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
    To parse the policy name and return its enum type from a string.
    @param policy_name string representation of policy name.
    @return ShardMemberSelectionPolicy
*/
inline ShardMemberSelectionPolicy parse_policy_name(std::string_view policy_name) {
    ShardMemberSelectionPolicy policy = ShardMemberSelectionPolicy::FirstMember;
    int i = 1;
    while (policy_names[i]) {
        if(policy_name == policy_names[i]) {
            policy = static_cast<ShardMemberSelectionPolicy>(i);
            break;
        }
        i++;
    }
    if(policy_names[i] == nullptr) {
        return ShardMemberSelectionPolicy::InvalidPolicy;
    }
    return policy;
}

/**
    For error messages.
*/
static void print_red(std::string_view msg) {
    std::cout << "\033[1;31m"
              << msg
              << "\033[0m" << std::endl;
}

/**
 * Allows for usage of template functions in P/Invoke exported functions in this file.
 * This is necessary, since functions exported to managed code require C linkage. 
 */
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
    Object made to handle the results of a derecho::rpc::QueryResults object for the C#
    side. T being the type that is to be returned from the QueryResults object and K being
    the type that needs to be returned from the lambda unwrapping.
*/
template <typename T, typename K>
class QueryResultsStore {
    /**
        std::function<K(T)> f: Lambda function for unwrapping the K return type.
        derecho::rpc::QueryResults<T> result: Future results object
    */
private:
    std::function<K(T&)> f;
    derecho::rpc::QueryResults<T> result;

public:
    /**
        Setter constructor.
    */
    QueryResultsStore(derecho::rpc::QueryResults<T>&& res, std::function<K(T&)> _f) : f(_f), result(std::move(res)) {
    }

    /**
        Return result for C# side.
        @return
    */
    K get_result() {
        for (auto& reply_future : result.get()) {
            T reply = reply_future.second.get();
            return f(reply);
        }
        
        std::cout << "The reply was empty... Should not happen" << std::endl;
        return {};
    }
};

/**
 * Lambda function for handling the unwrapping of ObjectWithStringKey
 */
std::function<ObjectProperties(const ObjectWithStringKey&)> object_unwrapper = [](const ObjectWithStringKey& obj) {
    ObjectProperties props;
    props.key = obj.get_key_ref().c_str();
    props.bytes = obj.blob.bytes;
    props.bytes_size = obj.blob.bytes_size();
    props.version = obj.get_version();
    props.timestamp = obj.get_timestamp();
    props.previous_version = obj.previous_version;
    props.previous_version_by_key = obj.previous_version_by_key;
#ifdef ENABLE_EVALUATION
    props.message_id = obj.get_message_id();
#endif
    return props;
};

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
auto get(ServiceClientAPI& capi, const std::string& key, persistent::version_t ver, bool stable, 
            uint32_t subgroup_index = 0, uint32_t shard_index = 0) {
    derecho::rpc::QueryResults<const typename SubgroupType::ObjectType> result 
        = capi.template get<SubgroupType>(key, ver, stable, subgroup_index, shard_index);
    auto s = new QueryResultsStore<const typename SubgroupType::ObjectType, ObjectProperties>(std::move(result), object_unwrapper);
    return s;
}


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
    derecho::rpc::QueryResults<derecho::cascade::version_tuple> result = (subgroup_index == UINT32_MAX) ? capi.put(obj) : capi.template put<SubgroupType>(obj, subgroup_index, shard_index);

    QueryResultsStore<derecho::cascade::version_tuple, std::vector<long>>* s = new QueryResultsStore<derecho::cascade::version_tuple, std::vector<long>>(std::move(result), bundle_f);
    return s;
}


// iterators:
// - op_get_keys from an object pool
// - all in a subgroup
// - shard of a subgroup
// - versions of a key

// ------------------------------------
// Exported functions through P/Invoke
// ------------------------------------

/*
 * Utilities
 */

struct TwoDimensionalNodeList {
    std::vector<std::vector<node_id_t>> vec;
    std::size_t outerSize;
};

struct PolicyMetadata {
    char* policyString;
    ShardMemberSelectionPolicy policy;
    node_id_t userNode;
};

EXPORT StdVectorWrapper indexTwoDimensionalNodeVector(std::vector<std::vector<node_id_t>> vec, std::size_t index) {
    return {vec[index].data(), vec[index].size()};
}

/*
 * Cascade functions.
 *
 * These functions are all stateless and will be loaded into C# code through
 * a DLL. We use camelCase to be more idiomatic, as this is a C# library.
 */

EXPORT ServiceClientAPI& getServiceClientRef() {
    return ServiceClientAPI::get_service_client();
}

EXPORT StdVectorWrapper getMembers(ServiceClientAPI& capi) {
    return {capi.get_members().data(), capi.get_members().size()};
}

EXPORT TwoDimensionalNodeList getSubgroupMembers(ServiceClientAPI& capi, char* serviceType, uint32_t subgroupIndex) {
    std::vector<std::vector<node_id_t>> members;
    on_all_subgroup_type(std::string(serviceType), members = capi.template get_subgroup_members, subgroupIndex);
    return {members, members.size()};
}

EXPORT TwoDimensionalNodeList getSubgroupMembersByObjectPool(ServiceClientAPI& capi, char* objectPoolPathname) {
    std::vector<std::vector<node_id_t>> members = capi.get_subgroup_members(objectPoolPathname);
    return {members, members.size()};
}

EXPORT StdVectorWrapper getShardMembers(ServiceClientAPI& capi, char* serviceType, uint32_t subgroupIndex, uint32_t shardIndex) {
    std::vector<node_id_t> members;
    on_all_subgroup_type(std::string(serviceType), members = capi.template get_shard_members, subgroupIndex, shardIndex);
    return {members.data(), members.size()};
}

EXPORT StdVectorWrapper getShardMembersByObjectPool(ServiceClientAPI& capi, char* objectPoolPathname, uint32_t shardIndex) {
    std::vector<node_id_t> members = capi.get_shard_members(objectPoolPathname, shardIndex);
    return {members.data(), members.size()};
}

EXPORT uint32_t getNumberOfSubgroups(ServiceClientAPI& capi, char* serviceType) {
    uint32_t num_subgroups;
    on_all_subgroup_type(std::string(serviceType), num_subgroups = capi.template get_number_of_subgroups);
    return num_subgroups;
}

EXPORT uint32_t getNumberOfShards(ServiceClientAPI& capi, char* serviceType, uint32_t subgroupIndex) {
    uint32_t num_shards;
    on_all_subgroup_type(std::string(serviceType), num_shards = capi.template get_number_of_shards, subgroupIndex);
    return num_shards;
}

EXPORT void setMemberSelectionPolicy(ServiceClientAPI& capi, char* serviceType, uint32_t subgroupIndex, uint32_t shardIndex, char* policy, node_id_t userNode) {
    ShardMemberSelectionPolicy real_policy = parse_policy_name(policy);
    on_all_subgroup_type(std::string(serviceType), capi.template set_member_selection_policy, subgroupIndex, shardIndex, real_policy, userNode);
}

EXPORT PolicyMetadata getMemberSelectionPolicy(ServiceClientAPI& capi, char* serviceType, uint32_t subgroupIndex, uint32_t shardIndex) {
    std::tuple<derecho::cascade::ShardMemberSelectionPolicy, unsigned int> policy;
    on_all_subgroup_type(std::string(serviceType), policy = capi.template get_member_selection_policy, subgroupIndex, shardIndex);
    char* pol;
    switch(std::get<0>(policy)) {
        case ShardMemberSelectionPolicy::FirstMember:
            pol = "FirstMember";
            break;
        case ShardMemberSelectionPolicy::LastMember:
            pol = "LastMember";
            break;
        case ShardMemberSelectionPolicy::Random:
            pol = "Random";
            break;
        case ShardMemberSelectionPolicy::FixedRandom:
            pol = "FixedRandom";
            break;
        case ShardMemberSelectionPolicy::RoundRobin:
            pol = "RoundRobin";
            break;
        case ShardMemberSelectionPolicy::KeyHashing:
            pol = "KeyHashing";
            break;
        case ShardMemberSelectionPolicy::UserSpecified:
            pol = "UserSpecified";
            break;
        case ShardMemberSelectionPolicy::InvalidPolicy:
            pol = "InvalidPolicy";
            break;
    }
    return {pol, std::get<0>(policy), std::get<1>(policy)};
}

EXPORT uint32_t getSubgroupIndex(ServiceClientAPI& capi, char* serviceType) {
    uint32_t subgroup_index;
    on_all_subgroup_type(std::string(serviceType), subgroup_index = capi.template get_subgroup_type_index);
    return subgroup_index;
}

EXPORT uint32_t getMyId(ServiceClientAPI& capi) {
    return capi.get_my_id();
}
