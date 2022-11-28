#ifndef __EXTERNAL_CLIENT__
#define __WITHOUT_SERVICE_SINGLETONS__
#endif//not __EXTERNAL_CLIENT__

#include <cascade/cascade.hpp>
#include <cascade/service_client_api.hpp>
#include <derecho/core/detail/rpc_utils.hpp>
#include <derecho/persistent/PersistentInterface.hpp>
#include <pybind11/pybind11.h>
#include <pybind11/pytypes.h>
#include <pybind11/stl.h>
#include <string>
#include <vector>

// ----------------
// Regular C++ code
// ----------------

using namespace derecho::cascade;


class ServiceClientAPI_PythonWrapper {
public:
    ServiceClientAPI& ref;
    ServiceClientAPI_PythonWrapper():
        ref(ServiceClientAPI::get_service_client()){}
};


namespace py = pybind11;
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
    Lambda function for handling the unwrapping of tuple of version and timestamp
*/
auto bundle_f = [](std::tuple<persistent::version_t, uint64_t>& obj) {
    std::vector<long> a;
    a.push_back(static_cast<long>(std::get<0>(obj)));
    a.push_back(static_cast<long>(std::get<1>(obj)));
    return a;
};

/**
 * Lambda function for handling the unwrapping of ObjectWithStringKey
 */
std::function<py::dict(const ObjectWithStringKey&)> object_unwrapper = [](const ObjectWithStringKey& obj) {
    py::dict object_dict;
    object_dict["key"] = py::str(obj.get_key_ref());
    object_dict["value"] = py::bytes(std::string(reinterpret_cast<const char*>(obj.blob.bytes), obj.blob.size));
    object_dict["version"] = obj.get_version();
    object_dict["timestamp"] = obj.get_timestamp();
    object_dict["previous_version"] = obj.previous_version;
    object_dict["previous_version_by_key"] = obj.previous_version_by_key;
#ifdef ENABLE_EVALUATION
    object_dict["message_id"] = obj.get_message_id();
#endif
    return object_dict;
};

/**
 * Lambda function for handling the unwrapping of vector
 */
std::function<py::list(std::vector<std::string>&)> list_unwrapper = [](std::vector<std::string>& keys)->py::list {
    py::list key_list;
    for(const auto& key:keys) {
        key_list.append(key);
    }
    return key_list;
};

/**
    Object made to handle the results of a derecho::rpc::QueryResults object for the python
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
        Return result for python side.
        @return 
    */
    std::optional<K> get_result() {
        for(auto& reply_future : result.get()) {
            T reply = reply_future.second.get();

            return f(reply);
        }
        std::cout << "The reply was empty... Should not happen" << std::endl;
        return {};
    }
};

/**
    To parse the policy name and return its enum type from a string.
    @param policy_name string representation of policy name.
    @return ShardMemberSelectionPolicy
*/
inline ShardMemberSelectionPolicy parse_policy_name(std::string& policy_name) {
    ShardMemberSelectionPolicy policy = ShardMemberSelectionPolicy::FirstMember;
    int i = 1;
    while(policy_names[i]) {
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
static void print_red(std::string msg) {
    std::cout << "\033[1;31m"
              << msg
              << "\033[0m" << std::endl;
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
    derecho::rpc::QueryResults<std::tuple<persistent::version_t, uint64_t>> result = (subgroup_index == UINT32_MAX) ? capi.put(obj) : capi.template put<SubgroupType>(obj, subgroup_index, shard_index);

    QueryResultsStore<std::tuple<persistent::version_t, uint64_t>, std::vector<long>>* s = new QueryResultsStore<std::tuple<persistent::version_t, uint64_t>, std::vector<long>>(std::move(result), bundle_f);
    return py::cast(s);
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
auto remove(ServiceClientAPI& capi, std::string& key, uint32_t subgroup_index = UINT32_MAX, uint32_t shard_index = 0) {
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

#define check_get_result(result)                                                                     \
    for(auto& reply_future : result.get()) {                                                         \
        auto reply = reply_future.second.get();                                                      \
        std::cout << "node(" << reply_future.first << ") replied with value:" << reply << std::endl; \
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
    auto result = capi.template multi_get<SubgroupType>(key,subgroup_index,shard_index);
    auto s = new QueryResultsStore<const typename SubgroupType::ObjectType, py::dict>(std::move(result), object_unwrapper);
    return py::cast(s);
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
    // check_get_result(result);
    auto s = new QueryResultsStore<uint64_t, uint64_t>(std::move(result),[](const uint64_t& size){return size;});
    return py::cast(s);
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
    auto result = capi.template multi_get_size<SubgroupType>(key,subgroup_index,shard_index);
    auto s = new QueryResultsStore<uint64_t, uint64_t>(std::move(result), [](const uint64_t& size){return size;});
    return py::cast(s);
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
    auto s = new QueryResultsStore<const typename SubgroupType::ObjectType, py::dict>(std::move(result), object_unwrapper);
    return py::cast(s);
}

template <typename SubgroupType>
auto get_size_by_time(ServiceClientAPI& capi, const std::string& key, uint64_t ts_us, bool stable, uint32_t subgroup_index, uint32_t shard_index) {
    derecho::rpc::QueryResults<uint64_t> result = capi.template get_size_by_time<SubgroupType>(key, ts_us,stable, subgroup_index, shard_index);
    auto s = new QueryResultsStore<uint64_t, uint64_t>(std::move(result), [](const uint64_t& size){return size;});
    return py::cast(s);
}

template <typename SubgroupType>
auto list_keys(ServiceClientAPI& capi, persistent::version_t version, bool stable, uint32_t subgroup_index = 0, uint32_t shard_index = 0) {
    derecho::rpc::QueryResults<std::vector<typename SubgroupType::KeyType>> result = capi.template list_keys<SubgroupType>(version, stable, subgroup_index, shard_index);
    auto s = new QueryResultsStore<typename std::vector<typename SubgroupType::KeyType>, py::list> (std::move(result), list_unwrapper);
    return py::cast(s);
}

template <typename SubgroupType>
auto list_keys_by_time(ServiceClientAPI& capi, uint64_t ts_us, bool stable, uint32_t subgroup_index = 0, uint32_t shard_index = 0) {
    derecho::rpc::QueryResults<std::vector<typename SubgroupType::KeyType>> result = capi.template list_keys_by_time<SubgroupType>(ts_us, stable, subgroup_index, shard_index);
    auto s = new QueryResultsStore<typename std::vector<typename SubgroupType::KeyType>, py::list> (std::move(result), list_unwrapper);
    return py::cast(s);
}

template <typename SubgroupType>
auto multi_list_keys(ServiceClientAPI& capi, uint32_t subgroup_index = 0, uint32_t shard_index = 0) {
    derecho::rpc::QueryResults<std::vector<typename SubgroupType::KeyType>> result = capi.template multi_list_keys<SubgroupType>(subgroup_index, shard_index);
    auto s = new QueryResultsStore<typename std::vector<typename SubgroupType::KeyType>, py::list> (std::move(result), list_unwrapper);
    return py::cast(s);
}

/**
 * Create an object pool
 * @tparam  SubgroupType
 * @param   capi
 * @param   object_pool_pathname
 * @param   subgroup_index
 * @return  QueryResultsStore that handles the return type
*/
template <typename SubgroupType>
auto create_object_pool(ServiceClientAPI& capi, const std::string& object_pool_pathname, uint32_t subgroup_index) {
    derecho::rpc::QueryResults<std::tuple<persistent::version_t, uint64_t>> result = capi.template create_object_pool<SubgroupType>(object_pool_pathname, subgroup_index);
    QueryResultsStore<std::tuple<persistent::version_t, uint64_t>, std::vector<long>>* s = new QueryResultsStore<std::tuple<persistent::version_t, uint64_t>, std::vector<long>>(std::move(result), bundle_f);
    return py::cast(s);
}

/**
 * List all object pools
 * @param capi
 * @return a list of the object pools
 */
auto list_object_pools(ServiceClientAPI& capi) {
    py::list ops;
    for(std::string& opp : capi.list_object_pools(true)) {
        ops.append(opp);
    }
    return ops;
}

/**
 * Get details of an object pool
 * @param   capi
 * @param   object_pool_pathname
 * @return  the object pool metadata
 */
auto get_object_pool(ServiceClientAPI& capi, const std::string& object_pool_pathname) {
    py::dict opm;
    auto copm = capi.find_object_pool(object_pool_pathname);
    opm["version"] = py::int_(copm.version);
    opm["timestamp_us"] = py::int_(copm.timestamp_us);
    opm["previous_version"] = py::int_(copm.previous_version);
    opm["previous_version_by_key"] = py::int_(copm.previous_version_by_key);
    opm["pathname"] = copm.pathname;
    opm["subgroup_type_index"] = py::int_(copm.subgroup_type_index);
    opm["subgroup_index"] = py::int_(copm.subgroup_index);
    opm["sharding_policy"] = py::int_(static_cast<int>(copm.sharding_policy));
    py::dict object_locations;
    for(const auto& kv : copm.object_locations) {
        object_locations[py::str(kv.first)] = kv.second;
    }
    opm["object_locations"] = object_locations;
    opm["deleted"] = py::bool_(copm.deleted);
    return opm;
}

// ----------------
// Python interface
// ----------------
std::vector<std::string> legal_cascade_subgroup_types{
    "VolatileCascadeStoreWithStringKey",
    "PersistentCascadeStoreWithStringKey",
    "TriggerCascadeNoStoreWithStringKey"
};

#ifdef __EXTERNAL_CLIENT__
PYBIND11_MODULE(external_client, m) {
    m.attr("__name__") = "derecho.cascade.external_client";
#else
PYBIND11_MODULE(member_client, m) {
    m.attr("__name__") = "derecho.cascade.member_client";
#endif//__EXTERNAL_CLIENT__
    m.doc() = "Cascade Client Python API.";
    py::class_<ServiceClientAPI_PythonWrapper>(m, "ServiceClientAPI")
            .def(py::init(), "Service Client API to access cascade K/V store.")
            .def_property_readonly_static("CASCADE_SUBGROUP_TYPES",
                    [](py::object/*self*/) {
                        return legal_cascade_subgroup_types;
                    }
                )
            .def_property_readonly_static("CURRENT_VERSION",
                    [](py::object/*self*/) {
                        return static_cast<int64_t>(CURRENT_VERSION);
                    }
                )
            .def(
                    "__repr__",
                    [](const ServiceClientAPI_PythonWrapper& a) {
                        return "Service Client API for managing cascade store.";
                    }
                )
            .def(
                    "get_my_id",
                    [](ServiceClientAPI_PythonWrapper& capi) {
                        return static_cast<int64_t>(capi.ref.get_my_id());
                    },
                    "Get my node id. \n"
                    "\t@return my node id."
                ) 
            .def(
                    "get_members",
                    [](ServiceClientAPI_PythonWrapper& capi) {
                        return capi.ref.get_members();
                    },
                    "Get all members in the current derecho group.\n"
                    "\r@return  a list of node ids"
                )
            .def(
                    "get_subgroup_members",
                    [](ServiceClientAPI_PythonWrapper& capi, std::string service_type, uint32_t subgroup_index) {
                        std::vector<std::vector<node_id_t>> members;
                        on_all_subgroup_type(service_type, members = capi.ref.template get_subgroup_members, subgroup_index);
                        return members;
                    },
                    "Get members of a subgroup.\n"
                    "\t@arg0    service_type    VolatileCascadeStoreWithStringKey | \n"
                    "\t                         PersistentCascadeStoreWithStringKey | \n"
                    "\t                         TriggerCascadeNoStoreWithStringKey \n"
                    "\t@arg1    subgroup_index  \n"
                    "\t@return  a list of shard members, which is a list of node ids"
                )
            .def(
                    "get_subgroup_members_by_object_pool",
                    [](ServiceClientAPI_PythonWrapper& capi, const std::string& object_pool_pathname){
                        return capi.ref.get_subgroup_members(object_pool_pathname);
                    },
                    "Get members of a subgroup by object pool.\n"
                    "\t@arg0    object_pool_pathname \n"
                    "\t@return  a list of shard members, which is a list of node ids"
                )
            .def(
                    "get_shard_members", 
                    [](ServiceClientAPI_PythonWrapper& capi, std::string service_type, uint32_t subgroup_index, uint32_t shard_index) {
                        std::vector<node_id_t> members;
                        on_all_subgroup_type(service_type, members = capi.ref.template get_shard_members, subgroup_index, shard_index);
                        return members;
                    },
                    "Get members of a shard.\n"
                    "\t@arg0    service_type    VolatileCascadeStoreWithStringKey | \n"
                    "\t                         PersistentCascadeStoreWithStringKey | \n"
                    "\t                         TriggerCascadeNoStoreWithStringKey \n"
                    "\t@arg1    subgroup_index  \n"
                    "\t@arg2    shard_index     \n"
                    "\t@return  a list of node ids"
                )
            .def(
                    "get_shard_members_by_object_pool",
                    [](ServiceClientAPI_PythonWrapper& capi, const std::string& object_pool_pathname, uint32_t shard_index){
                        return capi.ref.get_shard_members(object_pool_pathname,shard_index);
                    },
                    "Get members of a shard.\n"
                    "\t@arg0    object_pool_pathname \n"
                    "\t@arg1    shard_index     \n"
                    "\t@return  a list of node ids"
                )
            .def(
                    "get_number_of_subgroups",
                    [](ServiceClientAPI_PythonWrapper& capi, std::string service_type) {
                        uint32_t nsubgroups;
                        on_all_subgroup_type(service_type, nsubgroups = capi.ref.template get_number_of_subgroups);
                        return nsubgroups;
                    },
                    "Get number of subgroups of a subgroup type.\n"
                    "\t@arg0    service_type    VolatileCascadeStoreWithStringKey | \n"
                    "\t                         PersistentCascadeStoreWithStringKey | \n"
                    "\t                         TriggerCascadeNoStoreWithStringKey \n"
                    "\t@arg1    subgroup_index"
                )
            .def(
                    "get_number_of_shards",
                    [](ServiceClientAPI_PythonWrapper& capi, std::string service_type, uint32_t subgroup_index) {
                        uint32_t nshard;
                        on_all_subgroup_type(service_type, nshard = capi.ref.template get_number_of_shards, subgroup_index);
                        return nshard;
                    },
                    "Get number of shards in a subgroup.\n"
                    "\t@arg0    service_type    VolatileCascadeStoreWithStringKey | \n"
                    "\t                         PersistentCascadeStoreWithStringKey | \n"
                    "\t                         TriggerCascadeNoStoreWithStringKey \n"
                    "\t@arg1    subgroup_index  \n"
                    "\t@return  the number of shards in the subgroup."
                )
            .def(
                    "set_member_selection_policy",
                    [](ServiceClientAPI_PythonWrapper& capi, std::string service_type, uint32_t subgroup_index, uint32_t shard_index, std::string policy, uint32_t usernode) {
                        ShardMemberSelectionPolicy real_policy = parse_policy_name(policy);
                        on_all_subgroup_type(service_type, capi.ref.template set_member_selection_policy, subgroup_index, shard_index, real_policy, usernode);
                    },
                    "Set the member selection policy of a specified subgroup and shard.\n"
                    "\t@arg0    service_type    VolatileCascadeStoreWithStringKey | \n"
                    "\t                         PersistentCascadeStoreWithStringKey | \n"
                    "\t                         TriggerCascadeNoStoreWithStringKey \n"
                    "\t@arg1    subgroup_index  \n"
                    "\t@arg2    shard_index     \n"
                    "\t@arg3    policy          FirstMember | \n",
                    "\t                         LastMember | \n",
                    "\t                         Random | \n",
                    "\t                         FixedRandom | \n",
                    "\t                         RoundRobin | \n",
                    "\t                         KeyHashing | \n",
                    "\t                         UserSpecified \n",
                    "\t@arg4    usernode        The node id for 'UserSpecified' policy"
                )
            .def(
                    "get_member_selection_policy",
                    [](ServiceClientAPI_PythonWrapper& capi, std::string service_type, uint32_t subgroup_index, uint32_t shard_index) {
                        std::tuple<derecho::cascade::ShardMemberSelectionPolicy, unsigned int> policy;
                        on_all_subgroup_type(service_type, policy = capi.ref.template get_member_selection_policy, subgroup_index, shard_index);

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
                        return std::tie(pol,std::get<1>(policy));
                    },
                    "Get the member selection policy of the specified subgroup and shard.\n"
                    "\t@arg0    service_type    VolatileCascadeStoreWithStringKey | \n"
                    "\t                         PersistentCascadeStoreWithStringKey | \n"
                    "\t                         TriggerCascadeNoStoreWithStringKey \n"
                    "\t@arg1    subgroup_index  \n"
                    "\t@arg2    shard_index     \n"
                    "\t@return  a pair of the member selection policy, policy name, user specified node id"
                )
            .def(
                    "put",
                    [](ServiceClientAPI_PythonWrapper& capi, std::string& key, py::bytes value, py::kwargs kwargs) {
                        std::string subgroup_type;
                        uint32_t subgroup_index = 0;
                        uint32_t shard_index = 0;
                        persistent::version_t previous_version = CURRENT_VERSION;
                        persistent::version_t previous_version_by_key = CURRENT_VERSION;
                        bool blocking = true;
                        bool trigger = false;
#ifdef ENABLE_EVALUATION
                        uint64_t message_id = 0;
#endif
                        if (kwargs.contains("subgroup_index")) {
                            subgroup_index = kwargs["subgroup_index"].cast<uint32_t>();
                        }
                        if (kwargs.contains("shard_index")) {
                            shard_index = kwargs["shard_index"].cast<uint32_t>();
                        }
                        if (kwargs.contains("subgroup_type")) {
                            subgroup_type = kwargs["subgroup_type"].cast<std::string>();
                        }
                        if (kwargs.contains("previous_version")) {
                            previous_version = kwargs["previous_version"].cast<persistent::version_t>();
                        }
                        if (kwargs.contains("previous_version_by_key")) {
                            previous_version_by_key = kwargs["previous_version_by_key"].cast<persistent::version_t>();
                        }
                        if (kwargs.contains("blocking")) {
                            blocking = kwargs["blocking"].cast<bool>();
                        }
                        if (kwargs.contains("trigger")) {
                            trigger = kwargs["trigger"].cast<bool>();
                        }
#ifdef ENABLE_EVALUATION
                        if (kwargs.contains("message_id")) {
                            message_id = kwargs["message_id"].cast<uint64_t>();
                        }
#endif

                        ObjectWithStringKey obj;
                        obj.key = key;
                        obj.set_previous_version(previous_version,previous_version_by_key);
#ifdef ENABLE_EVALUATION
                        obj.message_id = message_id;
#endif
                        obj.blob = Blob(reinterpret_cast<const uint8_t*>(value.cast<std::string>().c_str()),value.cast<std::string>().size());
                        if (subgroup_type.empty()) {
                            if (trigger) {
                                capi.ref.trigger_put(obj);
                            } else if (blocking) {
                                auto result = capi.ref.put(obj);
                                auto s = new QueryResultsStore<std::tuple<persistent::version_t, uint64_t>, std::vector<long>>(std::move(result), bundle_f);
                                return py::cast(s);
                            } else {
                                capi.ref.put_and_forget(obj);
                            }
                        } else {
                            if (trigger) {
                                on_all_subgroup_type(subgroup_type, trigger_put, capi.ref, obj, subgroup_index, shard_index);
                            } else if (blocking) {
                                on_all_subgroup_type(subgroup_type, return put, capi.ref, obj, subgroup_index, shard_index);
                            } else {
                                on_all_subgroup_type(subgroup_type, put_and_forget, capi.ref, obj, subgroup_index, shard_index);
                            }
                        }

                        return py::cast(NULL);
                    },
                    "Put an object. \n"
                    "The new object would replace the old object with the same key.\n"
                    "\t@arg0    key \n"
                    "\t@arg1    value \n"
                    "\t** Optional keyword argument: ** \n"
                    "\t@argX    subgroup_type   VolatileCascadeStoreWithStringKey | \n"
                    "\t                         PersistentCascadeStoreWithStringKey | \n"
                    "\t                         TriggerCascadeNoStoreWithStringKey \n"
                    "\t@argX    subgroup_index  \n"
                    "\t@argX    shard_index     \n"
                    "\t@argX    pervious_version        \n"
                    "\t@argX    pervious_version_by_key \n"
                    "\t@argX    blocking \n"
                    "\t@argX    trigger         Using trigger put, always non-blocking regardless of blocking argument.\n"
#ifdef ENABLE_EVALUATION
                    "\t@argX    message_id \n"
#endif
                    "\t@return  a future of the (version,timestamp) for blocking put; or 'False' object for non-blocking put."
            )
            .def(
                    "remove",
                    [](ServiceClientAPI_PythonWrapper& capi, std::string& key, py::kwargs kwargs) {
                        std::string subgroup_type;
                        uint32_t subgroup_index = 0;
                        uint32_t shard_index = 0;
                        if (kwargs.contains("subgroup_type")) {
                            subgroup_type = kwargs["subgroup_type"].cast<std::string>();
                        }
                        if (kwargs.contains("subgroup_index")) {
                            subgroup_index = kwargs["subgroup_index"].cast<uint32_t>();
                        }
                        if (kwargs.contains("shard_index")) {
                            shard_index = kwargs["shard_index"].cast<uint32_t>();
                        }

                        if (subgroup_type.empty()) {
                            auto result = capi.ref.remove(key);
                            auto s = new QueryResultsStore<std::tuple<persistent::version_t, uint64_t>, std::vector<long>>(std::move(result), bundle_f);
                            return py::cast(s);
                        } else {
                            on_all_subgroup_type(subgroup_type, return remove, capi.ref, key, subgroup_index, shard_index);
                        }

                        return py::cast(NULL);
                    },
                    "Remove an object. \n"
                    "Remove an object by key.\n"
                    "\t@arg0    key \n"
                    "\t** Optional keyword argument: ** \n"
                    "\t@argX    subgroup_type   VolatileCascadeStoreWithStringKey | \n"
                    "\t                         PersistentCascadeStoreWithStringKey | \n"
                    "\t                         TriggerCascadeNoStoreWithStringKey \n"
                    "\t@argX    subgroup_index  \n"
                    "\t@argX    shard_index     \n"
                    "\t@return  a future of the (version,timestamp)"
            )
            .def(
                    "get",
                    [](ServiceClientAPI_PythonWrapper& capi, std::string& key, py::kwargs kwargs) {
                        std::string subgroup_type;
                        uint32_t subgroup_index = 0;
                        uint32_t shard_index = 0;
                        persistent::version_t version = CURRENT_VERSION;
                        bool stable = true;
                        uint64_t timestamp = 0ull;
                        if (kwargs.contains("subgroup_type")) {
                            subgroup_type = kwargs["subgroup_type"].cast<std::string>();
                        }
                        if (kwargs.contains("subgroup_index")) {
                            subgroup_index = kwargs["subgroup_index"].cast<uint32_t>();
                        }
                        if (kwargs.contains("shard_index")) {
                            shard_index = kwargs["shard_index"].cast<uint32_t>();
                        }
                        if (kwargs.contains("version")) {
                            version = kwargs["version"].cast<persistent::version_t>();
                        }
                        if (kwargs.contains("stable")) {
                            stable = kwargs["stable"].cast<bool>();
                        }
                        if (kwargs.contains("timestamp")) {
                            timestamp = kwargs["timestamp"].cast<uint64_t>();
                        }

                        if (timestamp != 0 && version == CURRENT_VERSION ) {
                            // timestamped get
                            if (subgroup_type.empty()) {
                                auto res = capi.ref.get_by_time(key,timestamp,stable);
                                auto s = new QueryResultsStore<const ObjectWithStringKey,py::dict>(std::move(res), object_unwrapper);
                                return py::cast(s);
                            } else {
                                on_all_subgroup_type(subgroup_type, return get_by_time, capi.ref, key, timestamp, stable, subgroup_index, shard_index);
                            }
                        } else {
                            // get versioned get
                            if (subgroup_type.empty()) {
                                auto res = capi.ref.get(key,version,stable);
                                auto s = new QueryResultsStore<const ObjectWithStringKey,py::dict>(std::move(res), object_unwrapper);
                                return py::cast(s);
                            } else {
                                on_all_subgroup_type(subgroup_type, return get, capi.ref, key, version, stable, subgroup_index, shard_index);
                            }
                        }

                        return py::cast(NULL);
                    },
                    "Get an object. \n"
                    "\t@arg0    key \n"
                    "\t** Optional keyword argument: ** \n"
                    "\t@argX    subgroup_type   VolatileCascadeStoreWithStringKey | \n"
                    "\t                         PersistentCascadeStoreWithStringKey | \n"
                    "\t                         TriggerCascadeNoStoreWithStringKey \n"
                    "\t@argX    subgroup_index  \n"
                    "\t@argX    shard_index     \n"
                    "\t@argX    version         Specify version for a versioned get.\n"
                    "\t@argX    stable          Specify if using stable get or not. Defaulted to true.\n"
                    "\t@argX    timestamp       Specify timestamp (as an integer in unix epoch microsecond) for a timestampped get.\n"
                    "\t@return  a dict version of the object."
            )
            .def(
                    "multi_get",
                    [](ServiceClientAPI_PythonWrapper& capi, std::string& key, py::kwargs kwargs) {
                        std::string subgroup_type;
                        uint32_t subgroup_index = 0;
                        uint32_t shard_index = 0;
                        if (kwargs.contains("subgroup_type")) {
                            subgroup_type = kwargs["subgroup_type"].cast<std::string>();
                        }
                        if (kwargs.contains("subgroup_index")) {
                            subgroup_index = kwargs["subgroup_index"].cast<uint32_t>();
                        }
                        if (kwargs.contains("shard_index")) {
                            shard_index = kwargs["shard_index"].cast<uint32_t>();
                        }

                        // get versioned get
                        if (subgroup_type.empty()) {
                            auto res = capi.ref.multi_get(key);
                            auto s = new QueryResultsStore<const ObjectWithStringKey,py::dict>(std::move(res), object_unwrapper);
                            return py::cast(s);
                        } else {
                            on_all_subgroup_type(subgroup_type, return multi_get, capi.ref, key, subgroup_index, shard_index);
                        }

                        return py::cast(NULL);
                    },
                    "Get an object using multi_get. \n"
                    "\t@arg0    key \n"
                    "\t** Optional keyword argument: ** \n"
                    "\t@argX    subgroup_type   VolatileCascadeStoreWithStringKey | \n"
                    "\t                         PersistentCascadeStoreWithStringKey | \n"
                    "\t                         TriggerCascadeNoStoreWithStringKey \n"
                    "\t@argX    subgroup_index  \n"
                    "\t@argX    shard_index     \n"
                    "\t@return  a dict version of the object."
            )
            .def(
                    "get_size",
                    [](ServiceClientAPI_PythonWrapper& capi, std::string& key, py::kwargs kwargs) {
                        std::string subgroup_type;
                        uint32_t subgroup_index = 0;
                        uint32_t shard_index = 0;
                        persistent::version_t version = CURRENT_VERSION;
                        bool stable = true;
                        uint64_t timestamp = 0ull;
                        if (kwargs.contains("subgroup_type")) {
                            subgroup_type = kwargs["subgroup_type"].cast<std::string>();
                        }
                        if (kwargs.contains("subgroup_index")) {
                            subgroup_index = kwargs["subgroup_index"].cast<uint32_t>();
                        }
                        if (kwargs.contains("shard_index")) {
                            shard_index = kwargs["shard_index"].cast<uint32_t>();
                        }
                        if (kwargs.contains("version")) {
                            version = kwargs["version"].cast<persistent::version_t>();
                        }
                        if (kwargs.contains("stable")) {
                            stable = kwargs["stable"].cast<bool>();
                        }
                        if (kwargs.contains("timestamp")) {
                            timestamp = kwargs["timestamp"].cast<uint64_t>();
                        }

                        if (timestamp != 0 && version == CURRENT_VERSION ) {
                            // timestamped get
                            if (subgroup_type.empty()) {
                                auto res = capi.ref.get_size_by_time(key,timestamp,stable);
                                auto s = new QueryResultsStore<uint64_t,uint64_t>(std::move(res), [](const uint64_t& size){return size;});
                                return py::cast(s);
                            } else {
                                on_all_subgroup_type(subgroup_type, return get_size_by_time, capi.ref, key, timestamp, stable, subgroup_index, shard_index);
                            }
                        } else {
                            // get versioned get
                            if (subgroup_type.empty()) {
                                auto res = capi.ref.get_size(key,version,stable);
                                auto s = new QueryResultsStore<uint64_t,uint64_t>(std::move(res), [](const uint64_t& size){return size;});
                                return py::cast(s);
                            } else {
                                on_all_subgroup_type(subgroup_type, return get_size, capi.ref, key, version, stable, subgroup_index, shard_index);
                            }
                        }

                        return py::cast(NULL);
                    },
                    "Get the size of an object. \n"
                    "\t@arg0    key \n"
                    "\t** Optional keyword argument: ** \n"
                    "\t@argX    subgroup_type   VolatileCascadeStoreWithStringKey | \n"
                    "\t                         PersistentCascadeStoreWithStringKey | \n"
                    "\t                         TriggerCascadeNoStoreWithStringKey \n"
                    "\t@argX    subgroup_index  \n"
                    "\t@argX    shard_index     \n"
                    "\t@argX    version         Specify version for a versioned get.\n"
                    "\t@argX    stable          Specify if using stable get or not. Defaulted to true.\n"
                    "\t@argX    timestamp       Specify timestamp (as an integer in unix epoch microsecond) for a timestampped get.\n"
                    "\t@return  the size of the object"
            )
            .def(
                    "multi_get_size",
                    [](ServiceClientAPI_PythonWrapper& capi, std::string& key, py::kwargs kwargs) {
                        std::string subgroup_type;
                        uint32_t subgroup_index = 0;
                        uint32_t shard_index = 0;
                        if (kwargs.contains("subgroup_type")) {
                            subgroup_type = kwargs["subgroup_type"].cast<std::string>();
                        }
                        if (kwargs.contains("subgroup_index")) {
                            subgroup_index = kwargs["subgroup_index"].cast<uint32_t>();
                        }
                        if (kwargs.contains("shard_index")) {
                            shard_index = kwargs["shard_index"].cast<uint32_t>();
                        }

                        // get versioned get
                        if (subgroup_type.empty()) {
                            auto res = capi.ref.multi_get_size(key);
                            auto s = new QueryResultsStore<uint64_t,uint64_t>(std::move(res), [](const uint64_t& size){return size;});
                            return py::cast(s);
                        } else {
                            on_all_subgroup_type(subgroup_type, return multi_get_size, capi.ref, key, subgroup_index, shard_index);
                        }

                        return py::cast(NULL);
                    },
                    "Get object size using multi_get_size. \n"
                    "\t@arg0    key \n"
                    "\t** Optional keyword argument: ** \n"
                    "\t@argX    subgroup_type   VolatileCascadeStoreWithStringKey | \n"
                    "\t                         PersistentCascadeStoreWithStringKey | \n"
                    "\t                         TriggerCascadeNoStoreWithStringKey \n"
                    "\t@argX    subgroup_index  \n"
                    "\t@argX    shard_index     \n"
                    "\t@return  the size of the object."
            )
            .def(
                    "list_keys_in_shard",
                    [](ServiceClientAPI_PythonWrapper& capi, std::string& subgroup_type, py::kwargs kwargs) {
                        uint32_t subgroup_index = 0;
                        uint32_t shard_index = 0;
                        persistent::version_t version = CURRENT_VERSION;
                        bool stable = true;
                        uint64_t timestamp = 0ull;
                        if (kwargs.contains("subgroup_index")) {
                            subgroup_index = kwargs["subgroup_index"].cast<uint32_t>();
                        }
                        if (kwargs.contains("shard_index")) {
                            shard_index = kwargs["shard_index"].cast<uint32_t>();
                        }
                        if (kwargs.contains("version")) {
                            version = kwargs["version"].cast<persistent::version_t>();
                        }
                        if (kwargs.contains("stable")) {
                            stable = kwargs["stable"].cast<bool>();
                        }
                        if (kwargs.contains("timestamp")) {
                            timestamp = kwargs["timestamp"].cast<uint64_t>();
                        }

                        if (timestamp != 0 && version == CURRENT_VERSION ) {
                            // timestamped get
                            on_all_subgroup_type(subgroup_type, return list_keys_by_time, capi.ref, timestamp, stable, subgroup_index, shard_index);
                        } else {
                            on_all_subgroup_type(subgroup_type, return list_keys, capi.ref, version, stable, subgroup_index, shard_index);
                        }

                        return py::cast(NULL);
                    },
                    "List the keys in a shard. \n"
                    "\t@arg0    subgroup_type   VolatileCascadeStoreWithStringKey | \n"
                    "\t                         PersistentCascadeStoreWithStringKey | \n"
                    "\t                         TriggerCascadeNoStoreWithStringKey \n"
                    "\t** Optional keyword argument: ** \n"
                    "\t@argX    subgroup_index  default to 0\n"
                    "\t@argX    shard_index     default to 0\n"
                    "\t@argX    version         Specify version for a versioned get.\n"
                    "\t@argX    stable          Specify if using stable get or not. Defaulted to true.\n"
                    "\t@argX    timestamp       Specify timestamp (as an integer in unix epoch microsecond) for a timestampped get.\n"
                    "\t@return  the list of keys."
            )
            .def(
                    "multi_list_keys_in_shard",
                    [](ServiceClientAPI_PythonWrapper& capi, std::string& subgroup_type, py::kwargs kwargs) {
                        uint32_t subgroup_index = 0;
                        uint32_t shard_index = 0;
                        if (kwargs.contains("subgroup_index")) {
                            subgroup_index = kwargs["subgroup_index"].cast<uint32_t>();
                        }
                        if (kwargs.contains("shard_index")) {
                            shard_index = kwargs["shard_index"].cast<uint32_t>();
                        }

                        on_all_subgroup_type(subgroup_type, return multi_list_keys, capi.ref, subgroup_index, shard_index);
                        return py::cast(NULL);
                    },
                    "List the keys in a shard using multi_get\n"
                    "\t@arg0    subgroup_type   VolatileCascadeStoreWithStringKey | \n"
                    "\t                         PersistentCascadeStoreWithStringKey | \n"
                    "\t                         TriggerCascadeNoStoreWithStringKey \n"
                    "\t** Optional keyword argument: ** \n"
                    "\t@argX    subgroup_index  default to 0\n"
                    "\t@argX    shard_index     default to 0\n"
                    "\t@return  the list of keys."
            )
            .def(
                    "list_keys_in_object_pool",
                    [](ServiceClientAPI_PythonWrapper& capi, std::string& object_pool_pathname, py::kwargs kwargs) {
                        persistent::version_t version = CURRENT_VERSION;
                        bool stable = true;
                        uint64_t timestamp = 0ull;
                        if (kwargs.contains("version")) {
                            version = kwargs["version"].cast<persistent::version_t>();
                        }
                        if (kwargs.contains("stable")) {
                            stable = kwargs["stable"].cast<bool>();
                        }
                        if (kwargs.contains("timestamp")) {
                            timestamp = kwargs["timestamp"].cast<uint64_t>();
                        }

                        std::vector<std::unique_ptr<derecho::rpc::QueryResults<std::vector<std::string>>>> results;
                        if (timestamp != 0 && version == CURRENT_VERSION) {
                            results = std::move(capi.ref.list_keys_by_time(timestamp, stable, object_pool_pathname));
                        } else {
                            results = std::move(capi.ref.list_keys(version,stable,object_pool_pathname));
                        }
                        py::list future_list;
                        for (auto& result:results) {
                            auto s = new QueryResultsStore<std::vector<std::string>, py::list> (std::move(*result), list_unwrapper);
                            future_list.append(py::cast(s));
                        }
                        return future_list;
                    },
                    "List the keys in an object pool, a.k.a folder\n"
                    "\t@arg0    object_pool_pathname\n"
                    "\t** Optional keyword argument: ** \n"
                    "\t@argX    version         Specify version for a versioned get.\n"
                    "\t@argX    stable          Specify if using stable get or not. Defaulted to true.\n"
                    "\t@argX    timestamp       Specify timestamp (as an integer in unix epoch microsecond) for a timestampped get.\n"
                    "\t@return  the list of keys."
            )
            .def(
                    "multi_list_keys_in_object_pool",
                    [](ServiceClientAPI_PythonWrapper& capi, std::string& object_pool_pathname, py::kwargs kwargs) {
                        std::vector<std::unique_ptr<derecho::rpc::QueryResults<std::vector<std::string>>>> results;
                        results = std::move(capi.ref.multi_list_keys(object_pool_pathname));
                        py::list future_list;
                        for (auto& result:results) {
                            auto s = new QueryResultsStore<std::vector<std::string>, py::list> (std::move(*result), list_unwrapper);
                            future_list.append(py::cast(s));
                        }
                        return future_list;
                    },
                    "List the keys in an object pool, a.k.a folder, using multi_get\n"
                    "\t@arg0    object_pool_pathname\n"
                    "\t@return  the list of keys."
            )
            .def(
                    "create_object_pool", 
                    [](ServiceClientAPI_PythonWrapper& capi, const std::string& object_pool_pathname, const std::string& service_type, uint32_t subgroup_index) {
                        on_all_subgroup_type(service_type, return create_object_pool, capi.ref, object_pool_pathname, subgroup_index);
                        return py::cast(NULL);
                    },
                    "Create an Object Pool. \n"
                    "\t@arg0    object pool pathname \n"
                    "\t@arg1    subgroup_type, could be either of the following \n"
                    "\t         VolatileCascadeStoreWithStringKey | \n"
                    "\t         PersistentCascadeStoreWithStringKey | \n"
                    "\t         TriggerCascadeNoStoreWithStringKey \n"
                    "\t@arg2    subgroup_index \n"
                    "\t@return  a future of the (version,timestamp)"
            )
            .def(
                    "list_object_pools",
                    [](ServiceClientAPI_PythonWrapper& capi) {
                        return list_object_pools(capi.ref);
                    },
                    "List the object pools\n"
                    "\t@return  a list of object pools")
            .def(
                    "get_object_pool",
                    [](ServiceClientAPI_PythonWrapper& capi, const std::string& object_pool_pathname) {
                        return get_object_pool(capi.ref, object_pool_pathname);
                    },
                    "Get an object pool by pathname. \n"
                    "\t@arg0    object pool pathname \n"
                    "\t@return  object pool details.");

    py::class_<QueryResultsStore<std::tuple<persistent::version_t, uint64_t>, std::vector<long>>>(m, "QueryResultsStoreVerTmeStmp")
            .def(
                    "get_result", [](QueryResultsStore<std::tuple<persistent::version_t, uint64_t>, std::vector<long>>& qrs) {
                        return qrs.get_result();
                    },
                    "Get result from QueryResultsStore for version and timestamp");

    py::class_<QueryResultsStore<const ObjectWithStringKey, py::bytes>>(m, "QueryResultsStoreObjectWithStringKey_deprecated")
            .def(
                    "get_result", [](QueryResultsStore<const ObjectWithStringKey, py::bytes>& qrs) {
                        return qrs.get_result();
                    },
                    "Get result from QueryResultsStore for ObjectWithStringKey");
    py::class_<QueryResultsStore<const ObjectWithStringKey, py::dict>>(m, "QueryResultsStoreObjectWithStringKey")
            .def(
                    "get_result", [](QueryResultsStore<const ObjectWithStringKey, py::dict>& qrs) {
                        return qrs.get_result();
                    },
                    "Get dict result from QueryResultsStore for ObjectWithStringKey");

    py::class_<QueryResultsStore<const ObjectWithUInt64Key, py::bytes>>(m, "QueryResultsStoreObjectWithUInt64Key")
            .def(
                    "get_result", [](QueryResultsStore<const ObjectWithUInt64Key, py::bytes>& qrs) {
                        return qrs.get_result();
                    },
                    "Get result from QueryResultsStore for ObjectWithUInt64Key");
    py::class_<QueryResultsStore<uint64_t,uint64_t>>(m, "QueryResultsStoreSize")
            .def(
                    "get_result", [](QueryResultsStore<uint64_t,uint64_t>& qrs) {
                        return qrs.get_result();
                    },
                    "Get result from QueryResultsStore for uint64_t");
    py::class_<QueryResultsStore<std::vector<std::string>,py::list>>(m, "QueryResultsStoreKeyList")
            .def(
                    "get_result", [](QueryResultsStore<std::vector<std::string>,py::list>& qrs) {
                    return qrs.get_result();
                    },
                    "Get result from QueryResultsStore for std::vector<std::string>");
#ifdef ENABLE_EVALUATION
    /* TimeLogger facility */
    class TimestampLogger_PythonWrapper {
    public:
        TimestampLogger_PythonWrapper() {}
    };
    py::class_<TimestampLogger_PythonWrapper>(m, "TimestampLogger")
        .def(py::init(), "TimestampLogger API to log timestamps.")
        .def(
                "__repr__",
                [](const TimestampLogger& tl) {
                    return "TimestampLogger for logging timestamps.";
                }
            )
        .def(
                "log", [](TimestampLogger_PythonWrapper&, uint64_t tag, uint64_t node_id, uint64_t msg_id, uint64_t ts_ns, uint64_t extra) {
                    TimestampLogger::log(tag,node_id,msg_id,ts_ns,extra);
                },
                "Log given timestamp. \n"
                "\t@arg0    tag, an uint64_t number defined in <include>/cascade/utils.hpp.\n"
                "\t@arg1    node_id, the id of local node.\n"
                "\t@arg2    msg_id, the message id of this log.\n"
                "\t@arg3    ts_ns, the timestamp in nano seconds.\n"
                "\t@arg4    extra, the extra information you want to add."
            )
        .def(
                "log", [](TimestampLogger_PythonWrapper&, uint64_t tag, uint64_t node_id, uint64_t msg_id, uint64_t extra) {
                    uint64_t ts_ns = get_time_ns();
                    TimestampLogger::log(tag,node_id,msg_id,ts_ns,extra);
                },
                "Log current timestamp. \n"
                "\t@arg0    tag, an uint64_t number defined in <include>/cascade/utils.hpp.\n"
                "\t@arg1    node_id, the id of local node.\n"
                "\t@arg2    msg_id, the message id of this log.\n"
                "\t@arg3    extra, the extra information you want to add."
            )
        .def(
                "flush", [](TimestampLogger_PythonWrapper&, const std::string& filename, bool clear) {
                    TimestampLogger::flush(filename,clear);
                },
                "Flush timestamp log to file. \n"
                "\t@arg0    filename, the filename\n"
                "\t@arg1    clear, if True, the timestamp log in memory will be cleared. otherwise, we keep it."
            )
        .def(
                "clear", [](TimestampLogger_PythonWrapper&) {
                    TimestampLogger::clear();
                },
                "Clear timestamp log. \n"
            );
#endif // ENABLE_EVALUATION
}
