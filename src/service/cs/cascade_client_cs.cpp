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

using version_tuple = std::tuple<persistent::version_t, uint64_t>;

/*
 * Value struct to be marshalled into C# for object metadata. 
 */
struct ObjectProperties {
    const char* key;
    uint8_t* bytes;
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

struct VersionTimestampPair {
    long version;
    uint64_t timestamp;
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
    Lambda function for handling the unwrapping of tuple of version and timestamp
*/
auto bundle_f = [](std::tuple<persistent::version_t, uint64_t>& obj) {
    VersionTimestampPair pair;
    pair.version = static_cast<long>(std::get<0>(obj));
    pair.timestamp = static_cast<long>(std::get<1>(obj));
    return pair;
};

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
    // TODO: remove memcpy here (1 copy)
    props.bytes = static_cast<uint8_t*>(malloc(obj.blob.size));
    memcpy(props.bytes, obj.blob.bytes, obj.blob.size);
    props.bytes_size = obj.blob.size;
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
auto get_internal(ServiceClientAPI& capi, const std::string& key, persistent::version_t ver, bool stable, 
            uint32_t subgroup_index = 0, uint32_t shard_index = 0) {
    derecho::rpc::QueryResults<const typename SubgroupType::ObjectType> result 
        = capi.template get<SubgroupType>(key, ver, stable, subgroup_index, shard_index);
    auto s = new QueryResultsStore<const typename SubgroupType::ObjectType, ObjectProperties>(std::move(result), object_unwrapper);

    return s;
}

/**
    Get objects from cascade store by time.
    @param capi the service client API for this client.
    @param key key to remove value from
    @param ts_us timestamp of the object you want to get.
    @param subgroup_index
    @param shard_index
    @return QueryResultsStore that handles the return type.
*/
template <typename SubgroupType>
auto get_by_time(ServiceClientAPI& capi, const std::string& key, uint64_t ts_us, bool stable, uint32_t subgroup_index, uint32_t shard_index) {
    derecho::rpc::QueryResults<const typename SubgroupType::ObjectType> result = capi.template get_by_time<SubgroupType>(key, ts_us, stable, subgroup_index, shard_index);
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
auto put_internal(ServiceClientAPI& capi, const typename SubgroupType::ObjectType& obj, uint32_t subgroup_index = UINT32_MAX, uint32_t shard_index = 0) {
    derecho::rpc::QueryResults<version_tuple> result = (subgroup_index == UINT32_MAX) ? capi.put(obj) : capi.template put<SubgroupType>(obj, subgroup_index, shard_index);
    QueryResultsStore<version_tuple, VersionTimestampPair>* s = new QueryResultsStore<version_tuple, VersionTimestampPair>(std::move(result), bundle_f);
    return s;
}

/**
    Put objects into cascade store and return immediately
    Please note that if subgroup_index is not specified, we will use the object_pool API.
    @param capi             the service client API for this client.
    @param obj object
    @param subgroup_index
    @param shard_index
    @return QueryResultsStore that handles the tuple of version and ts_us.
*/
template <typename SubgroupType>
void put_and_forget(ServiceClientAPI& capi, const typename SubgroupType::ObjectType& obj, uint32_t subgroup_index = UINT32_MAX, uint32_t shard_index = 0) {
    if(subgroup_index == UINT32_MAX) {
        capi.put_and_forget(obj);
    } else {
        capi.template put_and_forget<SubgroupType>(obj, subgroup_index, shard_index);
    }
}

/**
    Trigger put objects into cascade store.
    Please note that if subgroup_index is not specified, we will use the object_pool API.
    @param capi             the service client API for this client.
    @param obj object
    @param subgroup_index
    @param shard_index
    @return QueryResultsStore that handles the tuple of version and ts_us.
*/
template <typename SubgroupType>
void trigger_put(ServiceClientAPI& capi, const typename SubgroupType::ObjectType& obj, uint32_t subgroup_index = UINT32_MAX, uint32_t shard_index = 0) {
    if(subgroup_index == UINT32_MAX) {
        capi.trigger_put(obj);
    } else {
        capi.template trigger_put<SubgroupType>(obj, subgroup_index, shard_index);
    }
}

/**
 * Create an object pool
 * @tparam  SubgroupType
 * @param   capi
 * @param   object_pool_pathname
 * @param   subgroup_index
 * @param   affinity_set_regex, default to empty string
 * @return  QueryResultsStore that handles the return type
*/
template <typename SubgroupType>
auto create_object_pool(ServiceClientAPI& capi, char* object_pool_pathname, uint32_t subgroup_index, const std::string& affinity_set_regex="") {
    auto result =
        capi.template create_object_pool<SubgroupType>(std::string(object_pool_pathname), subgroup_index, sharding_policy_t::HASH, {}, affinity_set_regex);
    QueryResultsStore<version_tuple, VersionTimestampPair>* s = new QueryResultsStore<version_tuple, VersionTimestampPair>(std::move(result), bundle_f);
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
    const char* policyString;
    ShardMemberSelectionPolicy policy;
    node_id_t userNode;
};

EXPORT StdVectorWrapper indexTwoDimensionalNodeVector(std::vector<std::vector<node_id_t>> vec, std::size_t index) {
    return {vec[index].data(), vec[index].size()};
}

EXPORT ObjectProperties extractObjectPropertiesFromQueryResults(QueryResultsStore<const ObjectWithStringKey, ObjectProperties>* store) {
    auto res = store->get_result();
    return res;
}

EXPORT void freePointer(void* ptr) {
    free(ptr);
}

/*
 * Cascade functions.
 *
 * These functions are all stateless and will be loaded into C# code through
 * a DLL. We use camelCase to be more idiomatic, as this is a C# library.
 */

EXPORT ServiceClientAPI& EXPORT_getServiceClientRef() {
    return ServiceClientAPI::get_service_client();
}

EXPORT uint32_t EXPORT_getMyId(ServiceClientAPI& capi) {
    return capi.get_my_id();
}

EXPORT StdVectorWrapper EXPORT_getMembers(ServiceClientAPI& capi) {
    return {capi.get_members().data(), capi.get_members().size()};
}

EXPORT TwoDimensionalNodeList EXPORT_getSubgroupMembers(ServiceClientAPI& capi, char* serviceType, uint32_t subgroupIndex) {
    std::vector<std::vector<node_id_t>> members;
    on_all_subgroup_type(std::string(serviceType), members = capi.template get_subgroup_members, subgroupIndex);
    return {members, members.size()};
}

EXPORT TwoDimensionalNodeList EXPORT_getSubgroupMembersByObjectPool(ServiceClientAPI& capi, char* objectPoolPathname) {
    std::vector<std::vector<node_id_t>> members = capi.get_subgroup_members(objectPoolPathname);
    return {members, members.size()};
}

EXPORT StdVectorWrapper EXPORT_getShardMembers(ServiceClientAPI& capi, char* serviceType, uint32_t subgroupIndex, uint32_t shardIndex) {
    std::vector<node_id_t> members;
    on_all_subgroup_type(std::string(serviceType), members = capi.template get_shard_members, subgroupIndex, shardIndex);
    return {members.data(), members.size()};
}

EXPORT StdVectorWrapper EXPORT_getShardMembersByObjectPool(ServiceClientAPI& capi, char* objectPoolPathname, uint32_t shardIndex) {
    std::vector<node_id_t> members = capi.get_shard_members(objectPoolPathname, shardIndex);
    return {members.data(), members.size()};
}

EXPORT uint32_t EXPORT_getNumberOfSubgroups(ServiceClientAPI& capi, char* serviceType) {
    uint32_t num_subgroups = 0;
    on_all_subgroup_type(std::string(serviceType), num_subgroups = capi.template get_number_of_subgroups);
    return num_subgroups;
}

EXPORT uint32_t EXPORT_getNumberOfShards(ServiceClientAPI& capi, char* serviceType, uint32_t subgroupIndex) {
    uint32_t num_shards = 0;
    on_all_subgroup_type(std::string(serviceType), num_shards = capi.template get_number_of_shards, subgroupIndex);
    return num_shards;
}

EXPORT void EXPORT_setMemberSelectionPolicy(ServiceClientAPI& capi, char* serviceType, uint32_t subgroupIndex, uint32_t shardIndex, char* policy, node_id_t userNode) {
    ShardMemberSelectionPolicy real_policy = parse_policy_name(policy);
    on_all_subgroup_type(std::string(serviceType), capi.template set_member_selection_policy, subgroupIndex, shardIndex, real_policy, userNode);
}

EXPORT PolicyMetadata EXPORT_getMemberSelectionPolicy(ServiceClientAPI& capi, char* serviceType, uint32_t subgroupIndex, uint32_t shardIndex) {
    std::tuple<derecho::cascade::ShardMemberSelectionPolicy, unsigned int> policy;
    on_all_subgroup_type(std::string(serviceType), policy = capi.template get_member_selection_policy, subgroupIndex, shardIndex);
    std::string pol;
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
    return {pol.c_str(), std::get<0>(policy), std::get<1>(policy)};
}

EXPORT uint32_t EXPORT_getSubgroupIndex(ServiceClientAPI& capi, char* serviceType) {
    uint32_t subgroup_index = 0;
    on_all_subgroup_type(std::string(serviceType), subgroup_index = capi.template get_subgroup_type_index);
    return subgroup_index;
}

// Arguments to be used for the get function, made of blittable types (besides the string)
struct GetArgs {
    char* subgroupType;
    uint32_t subgroupIndex;
    uint32_t shardIndex;
    int64_t version;
    bool stable;
    uint64_t timestamp;
};

EXPORT auto EXPORT_get(ServiceClientAPI& capi, char* key, GetArgs args) {
    std::cout << "Received get call with key: " << key << " and subgroup type: " 
        << args.subgroupType << std::endl;
    std::string subgroup_type;
    uint32_t subgroup_index = 0;
    uint32_t shard_index = 0;
    persistent::version_t version = CURRENT_VERSION;
    bool stable = args.stable;
    uint64_t timestamp = 0ull;

    if (args.subgroupType[0] != '\0') {
        subgroup_type = std::string(args.subgroupType);
    }
    if (args.subgroupIndex != 0) {
        subgroup_index = args.subgroupIndex;
    }
    if (args.shardIndex != 0) {
        shard_index = args.shardIndex;
    }
    if (args.version != CURRENT_VERSION) {
        version = args.version;
    }
    if (args.timestamp != 0) {
        timestamp = args.timestamp;
    }

    if (timestamp != 0 && version == CURRENT_VERSION) {
        // timestamped get
        if (subgroup_type.empty()) {
            auto res = capi.get_by_time(std::string(key), timestamp, stable);
            auto s = new QueryResultsStore<const ObjectWithStringKey, ObjectProperties>(std::move(res), object_unwrapper);
            return s;
        } else {
            on_all_subgroup_type(subgroup_type, return get_by_time, capi, std::string(key), timestamp, stable, subgroup_index, shard_index);
        }
    } else {
        // get versioned get
        if (subgroup_type.empty()) {
            auto res = capi.get(std::string(key), version, stable);
            auto s = new QueryResultsStore<const ObjectWithStringKey, ObjectProperties>(std::move(res), object_unwrapper);
            return s;
        } else {
            on_all_subgroup_type(subgroup_type, return get_internal, capi, std::string(key), version, stable, subgroup_index, shard_index);
        }
    }
}

struct PutArgs {
    char* subgroupType;
    uint32_t subgroupIndex;
    uint32_t shardIndex;
    int64_t previousVersion;
    int64_t previousVersionByKey;
    bool blocking;
    bool trigger;
    uint64_t messageId;
};

EXPORT auto EXPORT_put(ServiceClientAPI& capi, char* key, uint8_t* bytes, std::size_t bytesSize, PutArgs args) {
    std::string subgroup_type;
    uint32_t subgroup_index = UINT32_MAX;
    uint32_t shard_index = 0;
    persistent::version_t previous_version = CURRENT_VERSION;
    persistent::version_t previous_version_by_key = CURRENT_VERSION;
    bool blocking = args.blocking;
    bool trigger = args.trigger;
#ifdef ENABLE_EVALUATION
    uint64_t message_id = 0;
#endif
    if (args.subgroupType[0] != '\0') {
        subgroup_type = std::string(args.subgroupType);
    }
    if (args.subgroupIndex != UINT32_MAX) {
        subgroup_index = args.subgroupIndex;
    }
    if (args.shardIndex != 0) {
        shard_index = args.shardIndex;
    }
    if (args.subgroupType[0] != '\0') {
        subgroup_type = std::string(args.subgroupType);
    }
    if (args.previousVersion != -1L) {
        previous_version = args.previousVersion;
    }
    if (args.previousVersionByKey != -1L) {
        previous_version_by_key = args.previousVersionByKey;
    }
#ifdef ENABLE_EVALUATION
    if (args.messageId != 0) {
        message_id = args.messageId;
    }
#endif

    ObjectWithStringKey obj;
    obj.key = key;
    obj.set_previous_version(previous_version, previous_version_by_key);
    obj.blob = Blob(bytes, bytesSize);
#ifdef ENABLE_EVALUATION
    obj.message_id = message_id;
#endif
    if (subgroup_type.empty()) {
        if (trigger) {
            capi.trigger_put(obj);
        } else if (blocking) {
            auto result = capi.put(obj);
            auto s = new QueryResultsStore<version_tuple, VersionTimestampPair>(std::move(result), bundle_f);
            return s;
        } else {
            capi.put_and_forget(obj);
        }
    } else {
        if (trigger) {
            on_all_subgroup_type(subgroup_type, trigger_put, capi, obj, subgroup_index, shard_index);
        } else if (blocking) {
            on_all_subgroup_type(subgroup_type, return put_internal, capi, obj, subgroup_index, shard_index);
        } else {
            on_all_subgroup_type(subgroup_type, put_and_forget, capi, obj, subgroup_index, shard_index);
        }
    }
}

EXPORT auto EXPORT_createObjectPool(ServiceClientAPI& capi, char* objectPoolPathname, char* serviceType, uint32_t subgroupIndex, char* affinitySetRegex) {
    on_all_subgroup_type(std::string(serviceType), return create_object_pool, capi, objectPoolPathname, subgroupIndex, std::string(affinitySetRegex));
}

// TODO(unimplemented, @ptwu):
// remove
// multiGet
// getSize
// multiGetSize
// listKeysInShard
// multiListKeysInShard
// listKeysInObjectPool
// multiListKeysInObjectPool
// createObjectPool
// listObjectPools
// getObjectPool
// removeObjectPool
