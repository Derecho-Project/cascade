#ifndef __EXTERNAL_CLIENT__
#define __WITHOUT_SERVICE_SINGLETONS__
#endif//not __EXTERNAL_CLIENT__

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
 * Object made to handle the results of a derecho::rpc::QueryResults object for the C#
 * side. 
 */
class QueryResultsStore {
    /**
        f: Lambda function for unwrapping the K return type.
        result: Future results object
    */
private:
    std::function<ObjectProperties(const ObjectWithStringKey&)> f;
    derecho::rpc::QueryResults<const ObjectWithStringKey> result;

public:
    /**
     *   Setter constructor.
     */
    QueryResultsStore(derecho::rpc::QueryResults<const ObjectWithStringKey>&& res, 
        std::function<ObjectProperties(const ObjectWithStringKey&)> _f) : f(_f), result(std::move(res)) {}

    /**
     * Return result for C# side.
     */
    ObjectProperties get_result() {
        for (auto& reply_future : result.get()) {
            ObjectWithStringKey reply = reply_future.second.get();

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

extern "C" ObjectProperties invoke_get_result(QueryResultsStore* results) {
    return results->get_result();
}

/**
 * Get object from the Cascade store. Assumes stable = true and getting the current version.
 * @param capi the service client API for this client.
 * @param key key to remove value from
*/
extern "C" QueryResultsStore* get(ServiceClientAPI& capi, const char* key) {
    derecho::rpc::QueryResults<const ObjectWithStringKey> result = capi.get(std::string(key));
    auto s = new QueryResultsStore(std::move(result), object_unwrapper);
    return s;
}

/**
 *  Put object in the Cascade store.
 *  @param capi the service client API for this client
 *  @param object_pool_path the object pool path with the key (e.g. /console_printer/obj_a)
 *  @param data byte pointer for the blob data
 *  @param data_size size of the bytes associated with the data
 *  @param subgroup_index
 *  @param shard_index
 */
template <typename SubgroupType>
QueryResultsStore* put(ServiceClientAPI& capi, const char* object_pool_path, const uint8_t* data, 
    std::size_t data_size, uint32_t subgroup_index = UINT32_MAX, uint32_t shard_index = 0) {
    std::cout << "Received key " << object_pool_path << " from sender!" << std::endl;
    Blob blob (data, data_size, true);
    const ObjectWithStringKey obj (std::string(object_pool_path), blob);
    auto result = (subgroup_index == UINT32_MAX) 
        ? capi.put(obj)
        : capi.put(obj, subgroup_index, shard_index);
    auto s = new QueryResultsStore(std::move(result), object_unwrapper);
    return s;
}


// iterators:
// - op_get_keys from an object pool
// - all in a subgroup
// - shard of a subgroup
// - versions of a key


extern "C" ServiceClientAPI& get_service_client_ref() {
    return ServiceClientAPI::get_service_client();
}

extern "C" uint32_t get_subgroup_index_vcss(ServiceClientAPI& capi) {
    return capi.get_subgroup_type_index<VolatileCascadeStoreWithStringKey>();
}

extern "C" uint32_t get_my_id(ServiceClientAPI& capi) {
    return capi.get_my_id();
}
