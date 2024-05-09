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
 * Value structs to be marshalled into C#.
 */

// The fields of an ObjectWithStringKey.
struct ObjectProperties {
    const char* key;
    uint8_t* bytes;
    std::size_t bytesSize;
    int64_t version;
    uint64_t timestamp;
    int64_t previousVersion;
    int64_t previousVersionByKey;
    uint64_t messageId;
};

// A wrapper for the std::vector data structure, generalizable to any type.
// Make sure to call delete on the vecBasePtr in the managed code to prevent
// memory leaks.
struct StdVectorWrapper {
    void* data;
    void* vecBasePtr;
    uint64_t length;
};

// A single object location, with its key and corresponding shard index.
struct ObjectLocation {
    const char* key;
    uint32_t shard;
};

// The metadata associated with a certain operation in Cascade.
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
inline ShardMemberSelectionPolicy parse_policy_name(const std::string& policy_name) {
    ShardMemberSelectionPolicy policy = ShardMemberSelectionPolicy::InvalidPolicy;
    int i = 0;
    while (policy_names[i]) {
        if (policy_name == policy_names[i]) {
            policy = static_cast<ShardMemberSelectionPolicy>(i);
            break;
        }
        i++;
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
        
        print_red("The reply was empty... Should not happen");
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
    props.bytesSize = obj.blob.size;
    props.version = obj.get_version();
    props.timestamp = obj.get_timestamp();
    props.previousVersion = obj.previous_version;
    props.previousVersionByKey = obj.previous_version_by_key;
#ifdef ENABLE_EVALUATION
    props.messageId = obj.get_message_id();
#endif
    return props;
};

/**
 * Lambda function for handling the unwrapping of vector
 */
std::function<StdVectorWrapper(std::vector<std::string>&)> list_unwrapper = [](std::vector<std::string>& keys)->StdVectorWrapper {
    std::vector<std::string>* key_list = new std::vector<std::string>();
    for(const auto& key : keys) {
        key_list->push_back(key);
    }
    return {key_list->data(), key_list, key_list->size()};
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
    Remove objects from cascade store.
    Please note that if subgroup_index is not specified, we will use the object_pool API.

    @param capi the service client API for this client.
    @param key key to remove value from
    @param subgroup_index 
    @param shard_index
    @return QueryResultsStore that handles the tuple of version and ts_us.
*/
template <typename SubgroupType>
auto remove_internal(ServiceClientAPI& capi, const std::string& key, uint32_t subgroup_index = UINT32_MAX, uint32_t shard_index = 0) {
    if constexpr(std::is_integral<typename SubgroupType::KeyType>::value) {
        derecho::rpc::QueryResults<version_tuple> result = std::move(capi.template remove<SubgroupType>(static_cast<uint64_t>(std::stol(key)), subgroup_index, shard_index));
        QueryResultsStore<version_tuple, VersionTimestampPair>* s = new QueryResultsStore<version_tuple, VersionTimestampPair>(std::move(result), bundle_f);
        return s;

    } else if constexpr(std::is_convertible<typename SubgroupType::KeyType, std::string>::value) {
        derecho::rpc::QueryResults<version_tuple> result = (subgroup_index == UINT32_MAX) ? capi.remove(key) : capi.template remove<SubgroupType>(key, subgroup_index, shard_index);
        QueryResultsStore<version_tuple, VersionTimestampPair>* s = new QueryResultsStore<version_tuple, VersionTimestampPair>(std::move(result), bundle_f);
        return s;

    } else {
        print_red(std::string("Unhandled KeyType:") + typeid(typename SubgroupType::KeyType).name());
        return;
    }
}

/**
    Get objects from cascade store using multi_get.
    @param capi the service client API for this client.
    @param key key to remove value from
    @param subgroup_index 
    @param shard_index
    @return QueryResultsStore that handles the return type.
*/
template <typename SubgroupType>
auto multi_get(ServiceClientAPI& capi, const std::string& key, uint32_t subgroup_index = 0, uint32_t shard_index = 0) {
    auto result = capi.template multi_get<SubgroupType>(key, subgroup_index, shard_index);
    auto s = new QueryResultsStore<const typename SubgroupType::ObjectType, ObjectProperties>(std::move(result), object_unwrapper);
    return s;
}

/**
    Get object size from cascade store.
    @param capi the service client API for this client.
    @param key key to remove value from
    @param ver version of the object you want to get.
    @param stable using stable get or not.
    @param subgroup_index 
    @param shard_index
    @return QueryResultsStore that handles the return type.
*/
template <typename SubgroupType>
auto get_size(ServiceClientAPI& capi, const std::string& key, persistent::version_t ver, bool stable, uint32_t subgroup_index = 0, uint32_t shard_index = 0) {
    derecho::rpc::QueryResults<uint64_t> result = capi.template get_size<SubgroupType>(key, ver, stable, subgroup_index, shard_index);
    auto s = new QueryResultsStore<uint64_t, uint64_t>(std::move(result), [](const uint64_t& size){ return size; });
    return s;
}

template <typename SubgroupType>
auto get_size_by_time(ServiceClientAPI& capi, const std::string& key, uint64_t ts_us, bool stable, uint32_t subgroup_index, uint32_t shard_index) {
    derecho::rpc::QueryResults<uint64_t> result = capi.template get_size_by_time<SubgroupType>(key, ts_us,stable, subgroup_index, shard_index);
    auto s = new QueryResultsStore<uint64_t, uint64_t>(std::move(result), [](const uint64_t& size){ return size; });
    return s;
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
auto create_object_pool(ServiceClientAPI& capi, const std::string& object_pool_pathname, uint32_t subgroup_index, const std::string& affinity_set_regex="") {
    auto result =
        capi.template create_object_pool<SubgroupType>(object_pool_pathname, subgroup_index, sharding_policy_t::HASH, {}, affinity_set_regex);
    QueryResultsStore<version_tuple, VersionTimestampPair>* s = new QueryResultsStore<version_tuple, VersionTimestampPair>(std::move(result), bundle_f);
    return s;
}

/**
    Get objects from cascade store using multi_get_size.
    @param capi the service client API for this client.
    @param key key to remove value from
    @param subgroup_index 
    @param shard_index
    @return QueryResultsStore that handles the return type.
*/
template <typename SubgroupType>
auto multi_get_size(ServiceClientAPI& capi, const std::string& key, uint32_t subgroup_index = 0, uint32_t shard_index = 0) {
    auto result = capi.template multi_get_size<SubgroupType>(key, subgroup_index, shard_index);
    auto s = new QueryResultsStore<uint64_t, uint64_t>(std::move(result), [](const uint64_t& size){ return size; });
    return s;
}

template <typename SubgroupType>
auto list_keys(ServiceClientAPI& capi, persistent::version_t version, bool stable, uint32_t subgroup_index = 0, uint32_t shard_index = 0) {
    derecho::rpc::QueryResults<std::vector<typename SubgroupType::KeyType>> result = capi.template list_keys<SubgroupType>(version, stable, subgroup_index, shard_index);
    auto s = new QueryResultsStore<typename std::vector<typename SubgroupType::KeyType>, StdVectorWrapper>(std::move(result), list_unwrapper);
    return s;
}

template <typename SubgroupType>
auto list_keys_by_time(ServiceClientAPI& capi, uint64_t ts_us, bool stable, uint32_t subgroup_index = 0, uint32_t shard_index = 0) {
    derecho::rpc::QueryResults<std::vector<typename SubgroupType::KeyType>> result = capi.template list_keys_by_time<SubgroupType>(ts_us, stable, subgroup_index, shard_index);
    auto s = new QueryResultsStore<typename std::vector<typename SubgroupType::KeyType>, StdVectorWrapper>(std::move(result), list_unwrapper);
    return s;
}

template <typename SubgroupType>
auto multi_list_keys(ServiceClientAPI& capi, uint32_t subgroup_index = 0, uint32_t shard_index = 0) {
    derecho::rpc::QueryResults<std::vector<typename SubgroupType::KeyType>> result = capi.template multi_list_keys<SubgroupType>(subgroup_index, shard_index);
    auto s = new QueryResultsStore<typename std::vector<typename SubgroupType::KeyType>, StdVectorWrapper> (std::move(result), list_unwrapper);
    return s;
}

struct GetObjectPoolMetadataInternal {
    persistent::version_t version;
    uint64_t timestamp;
    persistent::version_t previousVersion;
    persistent::version_t previousVersionByKey;
    const char* pathname;
    uint32_t subgroupTypeIndex;
    uint32_t subgroupIndex;
    int32_t shardingPolicy;
    StdVectorWrapper objectLocations;
    const char* affinitySetRegex;
    bool deleted;
};

/**
 * Get details of an object pool
 * @param   capi
 * @param   object_pool_pathname
 * @return  the object pool metadata
 */
auto get_object_pool(ServiceClientAPI& capi, const std::string& object_pool_pathname) {
    auto copm = capi.find_object_pool(object_pool_pathname);
    GetObjectPoolMetadataInternal metadata;
    metadata.version = copm.version;
    metadata.timestamp = copm.timestamp_us;
    metadata.previousVersion = copm.previous_version;
    metadata.previousVersionByKey = copm.previous_version_by_key;
    metadata.pathname = copm.pathname.c_str();
    metadata.subgroupTypeIndex = copm.subgroup_type_index;
    metadata.subgroupIndex = copm.subgroup_index;
    metadata.shardingPolicy = copm.sharding_policy;
    std::vector<ObjectLocation>* object_locations = new std::vector<ObjectLocation>();
    for (const auto& [k, v] : copm.object_locations) {
        object_locations->push_back({k.c_str(), v});
    }
    metadata.objectLocations = {object_locations->data(), object_locations, object_locations->size()};
    metadata.affinitySetRegex = copm.affinity_set_regex.c_str();
    metadata.deleted = copm.deleted;
    return metadata;
}

// ------------------------------------
// Exported functions through P/Invoke
// ------------------------------------

/*
 * Utilities
 */

struct TwoDimensionalNodeList {
    StdVectorWrapper flattenedList;
    StdVectorWrapper vectorSizes;
};

struct PolicyMetadataInternal {
    const char* policyString;
    ShardMemberSelectionPolicy policy;
    derecho::node_id_t userNode;
};

TwoDimensionalNodeList convert_2d_vector(std::vector<std::vector<derecho::node_id_t>> vector) {
    // heap-allocated so that they persist into the managed code without being destructed
    auto flattened_list = new std::vector<derecho::node_id_t>();
    auto vector_sizes = new std::vector<uint64_t>();
    for (const auto& inner_list : vector) {
        vector_sizes->push_back(inner_list.size());
        for (const derecho::node_id_t node : inner_list) {
            flattened_list->push_back(node);
        }
    }
    StdVectorWrapper flattened_list_wrapper = {flattened_list->data(), flattened_list, flattened_list->size()};
    StdVectorWrapper vector_sizes_wrapper = {vector_sizes->data(), vector_sizes, vector_sizes->size()};
    return {flattened_list_wrapper, vector_sizes_wrapper};
}

EXPORT ObjectProperties extractObjectPropertiesFromQueryResults(QueryResultsStore<const ObjectWithStringKey, ObjectProperties>* store) {
    return store->get_result();
}

EXPORT VersionTimestampPair extractVersionTimestampFromQueryResults(QueryResultsStore<version_tuple, VersionTimestampPair>* store) {
    return store->get_result();   
}

EXPORT uint64_t extractUInt64FromQueryResults(QueryResultsStore<uint64_t, uint64_t>* store) {
    return store->get_result();
}

EXPORT StdVectorWrapper extractStdVectorWrapperFromQueryResults(QueryResultsStore<std::vector<std::string>, StdVectorWrapper>* store) {
    return store->get_result();
}

EXPORT const char* indexStdVectorWrapperString(StdVectorWrapper vector, std::size_t index) {
    return static_cast<std::vector<std::string>*>(vector.vecBasePtr)->at(index).c_str();
} 

EXPORT ObjectLocation indexStdVectorWrapperObjectLocation(StdVectorWrapper vector, std::size_t index) {
    return static_cast<std::vector<ObjectLocation>*>(vector.vecBasePtr)->at(index);
}

EXPORT auto indexStdVectorWrapperStringVectorQueryResults(StdVectorWrapper vector, std::size_t index) {
    return static_cast<std::vector<QueryResultsStore<std::vector<std::string>, StdVectorWrapper>*>*>(vector.vecBasePtr)->at(index);
}

EXPORT bool deleteObjectLocationVectorPointer(std::vector<ObjectLocation>* ptr) {
    delete ptr;
    return true;
}

EXPORT bool deleteStringVectorPointer(std::vector<std::string>* ptr) {
    delete ptr;
    return true;
}

EXPORT bool deleteNodeIdVectorPointer(std::vector<derecho::node_id_t>* ptr) {
    delete ptr;
    return true;
}

EXPORT bool freeBytePointer(void* ptr) {
    // The byte pointer is allocated with malloc, so we use free
    free(ptr);
    return true;
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
    // heap-allocated so that it persists into the managed code without being destructed
    auto vec = new std::vector<derecho::node_id_t>(capi.get_members());
    return {vec->data(), vec, vec->size()};
}

EXPORT TwoDimensionalNodeList EXPORT_getSubgroupMembers(ServiceClientAPI& capi, char* serviceType, uint32_t subgroupIndex) {
    std::vector<std::vector<derecho::node_id_t>> members;
    on_all_subgroup_type(std::string(serviceType), members = capi.template get_subgroup_members, subgroupIndex);
    return convert_2d_vector(members);
}

EXPORT TwoDimensionalNodeList EXPORT_getSubgroupMembersByObjectPool(ServiceClientAPI& capi, char* objectPoolPathname) {
    std::vector<std::vector<derecho::node_id_t>> members = capi.get_subgroup_members(objectPoolPathname);
    return convert_2d_vector(members);
}

EXPORT StdVectorWrapper EXPORT_getShardMembers(ServiceClientAPI& capi, char* serviceType, uint32_t subgroupIndex, uint32_t shardIndex) {
    // heap-allocated so that it persists into the managed code without being destructed
    auto members_ptr = new std::vector<derecho::node_id_t>();
    std::vector<derecho::node_id_t> members;
    on_all_subgroup_type(std::string(serviceType), members = capi.template get_shard_members, subgroupIndex, shardIndex);
    for (auto member : members) {
        members_ptr->push_back(member);
    }

    return {members_ptr->data(), members_ptr, members_ptr->size()};
}

EXPORT StdVectorWrapper EXPORT_getShardMembersByObjectPool(ServiceClientAPI& capi, char* objectPoolPathname, uint32_t shardIndex) {
    // heap-allocated so that it persists into the managed code without being destructed
    auto members = new std::vector<derecho::node_id_t>();
    for (auto member : capi.get_shard_members(objectPoolPathname, shardIndex)) {
        members->push_back(member);
    }
    return {members->data(), members,  members->size()};
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

EXPORT void EXPORT_setMemberSelectionPolicy(ServiceClientAPI& capi, char* serviceType, uint32_t subgroupIndex, uint32_t shardIndex, char* policy, derecho::node_id_t userNode) {
    ShardMemberSelectionPolicy real_policy = parse_policy_name(std::string(policy));
    on_all_subgroup_type(std::string(serviceType), capi.template set_member_selection_policy, subgroupIndex, shardIndex, real_policy, userNode);
}

EXPORT PolicyMetadataInternal EXPORT_getMemberSelectionPolicy(ServiceClientAPI& capi, char* serviceType, uint32_t subgroupIndex, uint32_t shardIndex) {
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
    // Execution should never reach this point. If it does, then stopping control flow is
    // sensible to prevent clients from receiving malformed data, which will cause further issues. 
    throw derecho::derecho_exception("Reached end of exported get implementation.");
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
    uint32_t subgroup_index = 0;
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
    if (args.subgroupIndex != 0) {
        subgroup_index = args.subgroupIndex;
    }
    if (args.shardIndex != 0) {
        shard_index = args.shardIndex;
    }
    if (args.subgroupType[0] != '\0') {
        subgroup_type = std::string(args.subgroupType);
    }
    if (args.previousVersion != CURRENT_VERSION) {
        previous_version = args.previousVersion;
    }
    if (args.previousVersionByKey != CURRENT_VERSION) {
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
    // Execution should never reach this point. If it does, then stopping control flow is
    // sensible to prevent clients from receiving malformed data, which will cause further issues. 
    throw derecho::derecho_exception("Reached end of exported put implementation.");
}

EXPORT auto EXPORT_remove(ServiceClientAPI& capi, char* key, char* subgroupType, uint32_t subgroupIndex, uint32_t shardIndex) {
    std::string subgroup_type;
    uint32_t subgroup_index = subgroupIndex;
    uint32_t shard_index = shardIndex;
    if (subgroupType[0] != '\0') {
        subgroup_type = subgroupType;
    }

    if (subgroup_type.empty()) {
        auto result = capi.remove(std::string(key));
        auto s = new QueryResultsStore<version_tuple, VersionTimestampPair>(std::move(result), bundle_f);
        return s;
    } else {
        on_all_subgroup_type(subgroup_type, return remove_internal, capi, std::string(key), subgroup_index, shard_index);
    }
    // Execution should never reach this point. If it does, then stopping control flow is
    // sensible to prevent clients from receiving malformed data, which will cause further issues. 
    throw derecho::derecho_exception("Reached end of exported remove implementation.");
}

EXPORT auto EXPORT_multiGet(ServiceClientAPI& capi, char* key, char* subgroupType, uint32_t subgroupIndex, uint32_t shardIndex) {
    std::string subgroup_type;
    uint32_t subgroup_index = subgroupIndex;
    uint32_t shard_index = shardIndex;
    if (subgroupType[0] != '\0') {
        subgroup_type = subgroupType;
    }
    // get versioned get
    if (subgroup_type.empty()) {
        auto res = capi.multi_get(std::string(key));
        auto s = new QueryResultsStore<const ObjectWithStringKey, ObjectProperties>(std::move(res), object_unwrapper);
        return s;
    } else {
        on_all_subgroup_type(subgroup_type, return multi_get, capi, std::string(key), subgroup_index, shard_index);
    }
    // Execution should never reach this point. If it does, then stopping control flow is
    // sensible to prevent clients from receiving malformed data, which will cause further issues. 
    throw derecho::derecho_exception("Reached end of exported multi_get implementation.");
}

EXPORT auto EXPORT_getSize(ServiceClientAPI& capi, char* key, char* subgroupType, uint32_t subgroupIndex, 
    uint32_t shardIndex, persistent::version_t version, bool stable, uint64_t timestamp) {
        std::string subgroup_type;
        uint32_t subgroup_index = subgroupIndex;
        uint32_t shard_index = shardIndex;
        if (subgroupType[0] != '\0') {
            subgroup_type = subgroupType;
        }

        if (timestamp != 0 && version == CURRENT_VERSION) {
            // timestamped get
            if (subgroup_type.empty()) {
                auto res = capi.get_size_by_time(std::string(key), timestamp, stable);
                auto s = new QueryResultsStore<uint64_t, uint64_t>(std::move(res), [](const uint64_t& size){ return size; });
                return s;
            } else {
                on_all_subgroup_type(subgroup_type, return get_size_by_time, capi, std::string(key), timestamp, stable, subgroup_index, shard_index);
            }
        } else {
            // get versioned get
            if (subgroup_type.empty()) {
                auto res = capi.get_size(std::string(key), version, stable);
                auto s = new QueryResultsStore<uint64_t, uint64_t>(std::move(res), [](const uint64_t& size){ return size; });
                return s;
            } else {
                on_all_subgroup_type(subgroup_type, return get_size, capi, std::string(key), version, stable, subgroup_index, shard_index);
            }
        }
        // Execution should never reach this point. If it does, then stopping control flow is
        // sensible to prevent clients from receiving malformed data, which will cause further issues. 
        throw derecho::derecho_exception("Reached end of exported get_size implementation.");
}

EXPORT auto EXPORT_multiGetSize(ServiceClientAPI& capi, char* key, char* subgroupType, uint32_t subgroupIndex, uint32_t shardIndex) {
    std::string subgroup_type;
    uint32_t subgroup_index = subgroupIndex;
    uint32_t shard_index = shardIndex;
    if (subgroupType[0] != '\0') {
        subgroup_type = subgroupType;
    }

    // get versioned get
    if (subgroup_type.empty()) {
        auto res = capi.multi_get_size(std::string(key));
        auto s = new QueryResultsStore<uint64_t, uint64_t>(std::move(res), [](const uint64_t& size){ return size; });
        return s;
    } else {
        on_all_subgroup_type(subgroup_type, return multi_get_size, capi, std::string(key), subgroup_index, shard_index);
    }
    // Execution should never reach this point. If it does, then stopping control flow is
    // sensible to prevent clients from receiving malformed data, which will cause further issues. 
    throw derecho::derecho_exception("Reached end of exported multi_get_size implementation.");
}

EXPORT auto EXPORT_listKeysInShard(ServiceClientAPI& capi, char* subgroupType, uint32_t subgroupIndex, 
    uint32_t shardIndex, persistent::version_t version, bool stable, uint64_t timestamp) {
        std::string subgroup_type = subgroupType;
        uint32_t subgroup_index = subgroupIndex;
        uint32_t shard_index = shardIndex;

        if (timestamp != 0 && version == CURRENT_VERSION ) {
            // timestamped get
            on_all_subgroup_type(subgroup_type, return list_keys_by_time, capi, timestamp, stable, subgroup_index, shard_index);
        } else {
            on_all_subgroup_type(subgroup_type, return list_keys, capi, version, stable, subgroup_index, shard_index);
        }
        // Execution should never reach this point. If it does, then stopping control flow is
        // sensible to prevent clients from receiving malformed data, which will cause further issues. 
        throw derecho::derecho_exception("Reached end of exported list_keys_in_shard implementation.");
}

EXPORT auto EXPORT_multiListKeysInShard(ServiceClientAPI& capi, char* subgroupType, uint32_t subgroupIndex, uint32_t shardIndex) {
    std::string subgroup_type = subgroupType;
    uint32_t subgroup_index = subgroupIndex;
    uint32_t shard_index = shardIndex;

    on_all_subgroup_type(subgroup_type, return multi_list_keys, capi, subgroup_index, shard_index);
    // Execution should never reach this point. If it does, then stopping control flow is
    // sensible to prevent clients from receiving malformed data, which will cause further issues. 
    throw derecho::derecho_exception("Reached end of exported multi_list_keys_in_shard implementation.");
}

EXPORT StdVectorWrapper EXPORT_listKeysInObjectPool(ServiceClientAPI& capi, char* objectPoolPathname, persistent::version_t version, bool stable, uint64_t timestamp) {
    std::vector<std::unique_ptr<derecho::rpc::QueryResults<std::vector<std::string>>>> results;
    if (timestamp != 0 && version == CURRENT_VERSION) {
        results = std::move(capi.list_keys_by_time(timestamp, stable, std::string(objectPoolPathname)));
    } else {
        results = std::move(capi.list_keys(version,stable, std::string(objectPoolPathname)));
    }
    auto future_list = new std::vector<QueryResultsStore<std::vector<std::string>, StdVectorWrapper>*>();
    for (auto& result : results) {
        auto s = new QueryResultsStore<std::vector<std::string>, StdVectorWrapper>(std::move(*result), list_unwrapper);
        future_list->push_back(s);
    }
    return {future_list->data(), future_list, future_list->size()};
}

EXPORT StdVectorWrapper EXPORT_multiListKeysInObjectPool(ServiceClientAPI& capi, char* objectPoolPathname) {
    std::vector<std::unique_ptr<derecho::rpc::QueryResults<std::vector<std::string>>>> results;
    results = std::move(capi.multi_list_keys(objectPoolPathname));
    auto future_list = new std::vector<QueryResultsStore<std::vector<std::string>, StdVectorWrapper>*>();
    for (auto& result : results) {
        auto s = new QueryResultsStore<std::vector<std::string>, StdVectorWrapper>(std::move(*result), list_unwrapper);
        future_list->push_back(s);
    }
    return {future_list->data(), future_list, future_list->size()};
}

EXPORT StdVectorWrapper EXPORT_listObjectPools(ServiceClientAPI& capi) {
    std::vector<std::string>* pools = new std::vector<std::string>();
    for (const std::string& opp : capi.list_object_pools(true, true)) {
        pools->push_back(opp);
    }
    return {pools->data(), pools, pools->size()};
}

EXPORT auto EXPORT_createObjectPool(ServiceClientAPI& capi, char* objectPoolPathname, char* serviceType, uint32_t subgroupIndex, char* affinitySetRegex) {
    on_all_subgroup_type(std::string(serviceType), return create_object_pool, capi, std::string(objectPoolPathname), subgroupIndex, std::string(affinitySetRegex));
    // Execution should never reach this point. If it does, then stopping control flow is
    // sensible to prevent clients from receiving malformed data, which will cause further issues. 
    throw derecho::derecho_exception("Reached end of exported create_object_pool implementation.");
}

EXPORT auto EXPORT_getObjectPool(ServiceClientAPI& capi, char* objectPoolPathname) {
    return get_object_pool(capi, std::string(objectPoolPathname));
}

EXPORT auto EXPORT_removeObjectPool(ServiceClientAPI& capi, char* objectPoolPathname) {
    derecho::rpc::QueryResults<version_tuple> result = capi.remove_object_pool(std::string(objectPoolPathname));
    QueryResultsStore<version_tuple, VersionTimestampPair>* s =
        new QueryResultsStore<version_tuple, VersionTimestampPair>(std::move(result), bundle_f);
    return s;
}
