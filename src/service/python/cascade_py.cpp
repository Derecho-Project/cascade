#include <cascade/service_client_api.hpp>
#include <pybind11/pybind11.h>
#include <pybind11/pytypes.h>
#include <pybind11/stl.h>
#include <vector>

// ----------------
// Regular C++ code
// ----------------

using namespace derecho::cascade;

namespace py = pybind11;
#define on_non_trigger_subgroup_type(x, ft, ...)                          \
    if((x) == "VolatileCascadeStoreWithStringKey") {          \
        ft<VolatileCascadeStoreWithStringKey>(__VA_ARGS__);   \
    } else if((x) == "PersistentCascadeStoreWithStringKey") { \
        ft<PersistentCascadeStoreWithStringKey>(__VA_ARGS__); \
    } else {                                                  \
        print_red("unknown subgroup type:" + x);              \
    }

#define on_all_subgroup_type(x, ft, ...)                  \
    if((x) == "VolatileCascadeStoreWithStringKey") {          \
        ft<VolatileCascadeStoreWithStringKey>(__VA_ARGS__);   \
    } else if((x) == "PersistentCascadeStoreWithStringKey") { \
        ft<PersistentCascadeStoreWithStringKey>(__VA_ARGS__); \
    } else if((x) == "TriggerCascadeNoStoreWithStringKey") {  \
        ft<TriggerCascadeNoStoreWithStringKey>(__VA_ARGS__);  \
    } else {                                                  \
        print_red("unknown subgroup type:" + x);              \
    }

static const char* policy_names[] = {
        "FirstMember",
        "LastMember",
        "Random",
        "FixedRandom",
        "RoundRobin",
        "UserSpecified",
        nullptr};

/**
    Lambda function for handling the unwrapping of ObjectWithStringKey
*/
std::function<py::bytes(ObjectWithStringKey)> s_f = [](ObjectWithStringKey obj) {
    std::string s(obj.blob.bytes, obj.blob.size);
    return py::bytes(s);
};

/**
    Lambda function for handling the unwrapping of ObjectWithUInt64Key
*/
std::function<py::bytes(ObjectWithUInt64Key)> u_f = [](ObjectWithUInt64Key obj) {
    std::string s(obj.blob.bytes, obj.blob.size);
    return py::bytes(s);
};

/**
    Lambda function for handling the unwrapping of tuple of version and timestamp
*/
auto bundle_f = [](std::tuple<persistent::version_t, uint64_t> obj) {
    std::vector<long> a;
    a.push_back(static_cast<long>(std::get<0>(obj)));
    a.push_back(static_cast<long>(std::get<1>(obj)));
    return a;
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
    std::function<K(T)> f;
    derecho::rpc::QueryResults<T> result;

public:
    /**
        Setter constructor.
    */
    QueryResultsStore(derecho::rpc::QueryResults<T>& res, std::function<K(T)> _f) : f(_f), result(std::move(res)) {
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

    @param capi the service client API for this client.
    @param key key to put value into
    @param value value to be put in for coressponding key
    @param subgroup_index
    @param shard_index
    @return QueryResultsStore that handles the tuple of version and ts_us.
*/
template <typename SubgroupType>
auto put(ServiceClientAPI& capi, std::string& key, std::string& value, uint32_t subgroup_index = UINT32_MAX, uint32_t shard_index = 0) {
    typename SubgroupType::ObjectType obj;
    if constexpr(std::is_integral<typename SubgroupType::KeyType>::value) {
        obj.key = static_cast<uint64_t>(std::stol(key));
    } else if constexpr(std::is_convertible<typename SubgroupType::KeyType, std::string>::value) {
        obj.key = key;
    } else {
        print_red(std::string("Unhandled KeyType:") + typeid(typename SubgroupType::KeyType).name());
        return;
    }
    obj.blob = Blob(value.c_str(), value.length());
    derecho::rpc::QueryResults<std::tuple<persistent::version_t, uint64_t>> result = (subgroup_index == UINT32_MAX) ? capi.put(obj) : capi.template put<SubgroupType>(obj, subgroup_index, shard_index);

    QueryResultsStore<std::tuple<persistent::version_t, uint64_t>, std::vector<long>>* s = new QueryResultsStore<std::tuple<persistent::version_t, uint64_t>, std::vector<long>>(result, bundle_f);
    return py::cast(s);
}

/**
    Trigger put objects into cascade store.
    Please note that if subgroup_index is not specified, we will use the object_pool API.

    @param capi the service client API for this client.
    @param key key to put value into
    @param value value to be put in for coressponding key
    @param subgroup_index
    @param shard_index
    @return QueryResultsStore that handles the tuple of version and ts_us.
*/
template <typename SubgroupType>
void trigger_put(ServiceClientAPI& capi, std::string& key, std::string& value, uint32_t subgroup_index = UINT32_MAX, uint32_t shard_index = 0) {
    typename SubgroupType::ObjectType obj;
    if constexpr(std::is_integral<typename SubgroupType::KeyType>::value) {
        obj.key = static_cast<uint64_t>(std::stol(key));
    } else if constexpr(std::is_convertible<typename SubgroupType::KeyType, std::string>::value) {
        obj.key = key;
    } else {
        print_red(std::string("Unhandled KeyType:") + typeid(typename SubgroupType::KeyType).name());
        return;
    }
    obj.blob = Blob(value.c_str(), value.length());
    if (subgroup_index == UINT32_MAX) {
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
        QueryResultsStore<std::tuple<persistent::version_t, uint64_t>, std::vector<long>>* s = new QueryResultsStore<std::tuple<persistent::version_t, uint64_t>, std::vector<long>>(result, bundle_f);
        return py::cast(s);

    } else if constexpr(std::is_convertible<typename SubgroupType::KeyType, std::string>::value) {
        derecho::rpc::QueryResults<std::tuple<persistent::version_t, uint64_t>> result = (subgroup_index == UINT32_MAX) ? capi.remove(key) : capi.template remove<SubgroupType>(key, subgroup_index, shard_index);
        QueryResultsStore<std::tuple<persistent::version_t, uint64_t>, std::vector<long>>* s = new QueryResultsStore<std::tuple<persistent::version_t, uint64_t>, std::vector<long>>(result, bundle_f);
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
    @param subgroup_index 
    @param shard_index
    @return QueryResultsStore that handles the return type.
*/
template <typename SubgroupType>
auto get(ServiceClientAPI& capi, const std::string& key, persistent::version_t ver, uint32_t subgroup_index = UINT32_MAX, uint32_t shard_index = 0) {
    if constexpr(std::is_integral<typename SubgroupType::KeyType>::value) {
        derecho::rpc::QueryResults<const typename SubgroupType::ObjectType> result = capi.template get<SubgroupType>(static_cast<uint64_t>(std::stol(key)), ver, subgroup_index, shard_index);
        // check_get_result(result);
        QueryResultsStore<const typename SubgroupType::ObjectType, py::bytes>* s = new QueryResultsStore<const typename SubgroupType::ObjectType, py::bytes>(result, u_f);
        return py::cast(s);

    } else if constexpr(std::is_convertible<typename SubgroupType::KeyType, std::string>::value) {
        derecho::rpc::QueryResults<const typename SubgroupType::ObjectType> result = (subgroup_index == UINT32_MAX) ? capi.get(key, ver) : capi.template get<SubgroupType>(key, ver, subgroup_index, shard_index);
        // check_get_result(result);
        QueryResultsStore<const typename SubgroupType::ObjectType, py::bytes>* s = new QueryResultsStore<const typename SubgroupType::ObjectType, py::bytes>(result, s_f);
        return py::cast(s);
    }
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
auto get_by_time(ServiceClientAPI& capi, const std::string& key, uint64_t ts_us, uint32_t subgroup_index, uint32_t shard_index) {
    if constexpr(std::is_integral<typename SubgroupType::KeyType>::value) {
        derecho::rpc::QueryResults<const typename SubgroupType::ObjectType> result = capi.template get_by_time<SubgroupType>(
                static_cast<uint64_t>(std::stol(key)), ts_us, subgroup_index, shard_index);
        QueryResultsStore<const typename SubgroupType::ObjectType, py::bytes>* s = new QueryResultsStore<const typename SubgroupType::ObjectType, py::bytes>(result, u_f);
        return py::cast(s);

    } else if constexpr(std::is_convertible<typename SubgroupType::KeyType, std::string>::value) {
        derecho::rpc::QueryResults<const typename SubgroupType::ObjectType> result = (subgroup_index == UINT32_MAX) ? capi.get_by_time(key, ts_us) : capi.template get_by_time<SubgroupType>(key, ts_us, subgroup_index, shard_index);

        QueryResultsStore<const typename SubgroupType::ObjectType, py::bytes>* s = new QueryResultsStore<const typename SubgroupType::ObjectType, py::bytes>(result, s_f);
        return py::cast(s);
    }
}

/**
 * Create an object pool
 * @param
*/
template <typename SubgroupType>
auto create_object_pool(ServiceClientAPI& capi, const std::string& object_pool_pathname, uint32_t subgroup_index) {
    derecho::rpc::QueryResults<std::tuple<persistent::version_t, uint64_t>> result = capi.template create_object_pool<SubgroupType>(object_pool_pathname,subgroup_index);
    QueryResultsStore<std::tuple<persistent::version_t, uint64_t>, std::vector<long>>* s = new QueryResultsStore<std::tuple<persistent::version_t, uint64_t>, std::vector<long>>(result, bundle_f);
        return py::cast(s);
}

auto list_object_pools(ServiceClientAPI& capi) {
    py::list ops;
    for (std::string& opp: capi.list_object_pools(true)) {
        ops.append(opp);
    }
    return ops;
}

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
    for(const auto& kv:copm.object_locations) {
        object_locations[py::str(kv.first)] = kv.second;
    }
    opm["object_locations"] = object_locations;
    opm["deleted"] = py::bool_(copm.deleted);
    return opm;
}

// ----------------
// Python interface
// ----------------

PYBIND11_MODULE(client, m) {
    m.attr("__name__") = "derecho.cascade.client";
    m.doc() = "Cascade Client Python API.";

    py::class_<ServiceClientAPI>(m, "ServiceClientAPI")
            .def(py::init(), "Service Client API for managing cascade store.")
            .def("__repr__",
                 [](const ServiceClientAPI& a) {
                     return "Service Client API for managing cascade store.";
                 })
            .def("get_members", &ServiceClientAPI::get_members, "Get all members in the current derecho group.")
            /* deprecated: subgroup ID should be hidden from application.
	  .def("get_shard_members", [](ServiceClientAPI &capi, uint32_t subgroup_index, uint32_t shard_index){
           return capi.get_shard_members(subgroup_index, shard_index);
	   }, "Get all members in the current derecho subgroup and shard.")
      */
            .def(
                    "get_shard_members", [](ServiceClientAPI& capi, std::string service_type, uint32_t subgroup_index, uint32_t shard_index) {
                        std::vector<node_id_t> members;
                        on_non_trigger_subgroup_type(service_type, members = capi.template get_shard_members, subgroup_index, shard_index);
                        return members;
                    },
                    "Get all members in the current derecho subgroup and shard.")
            .def(
                    "set_member_selection_policy", [](ServiceClientAPI& capi, std::string service_type, uint32_t subgroup_index, uint32_t shard_index, std::string policy, uint32_t usernode) {
                        ShardMemberSelectionPolicy real_policy = parse_policy_name(policy);
                        on_non_trigger_subgroup_type(service_type, capi.template set_member_selection_policy, subgroup_index, shard_index, real_policy, usernode);
                    },
                    "Set the member selection policy of the specified subgroup and shard.")
            .def(
                    "get_member_selection_policy", [](ServiceClientAPI& capi, std::string service_type, uint32_t subgroup_index, uint32_t shard_index) {
                        std::tuple<derecho::cascade::ShardMemberSelectionPolicy, unsigned int> policy;
                        on_non_trigger_subgroup_type(service_type, policy = capi.template get_member_selection_policy, subgroup_index, shard_index);

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
                            case ShardMemberSelectionPolicy::UserSpecified:
                                pol = "UserSpecified";
                                break;
                            case ShardMemberSelectionPolicy::InvalidPolicy:
                                pol = "InvalidPolicy";
                                break;
                        }
                        return pol;
                    },
                    "Get the member selection policy of the specified subgroup and shard.")
            .def(
                    "put", [](ServiceClientAPI& capi, std::string service_type, std::string& key, py::bytes value, py::kwargs kwargs) {
                        // Test if subgroup_index and shard_index is specified.
                        uint32_t subgroup_index = UINT32_MAX;
                        uint32_t shard_index = 0;
                        if(kwargs.contains("subgroup_index")) {
                            subgroup_index = kwargs["subgroup_index"].cast<uint32_t>();
                        }
                        if(kwargs.contains("shard_index")) {
                            shard_index = kwargs["shard_index"].cast<uint32_t>();
                        }
                        std::string val = std::string(value);
                        on_non_trigger_subgroup_type(service_type, return put, capi, key, val, subgroup_index, shard_index);

                        return py::cast(NULL);
                    },
                    "Put an object. The new object would replace the old object if a new key-value pair with the same\nkey as one put before is put.")
            .def(
                    "trigger_put", [](ServiceClientAPI& capi, std::string service_type, std::string& key, py::bytes value, py::kwargs kwargs) {
                        // Test is subgroup_index and shard_index is specified.
                        uint32_t subgroup_index = UINT32_MAX;
                        uint32_t shard_index = 0;
                        if(kwargs.contains("subgroup_index")) {
                            subgroup_index = kwargs["subgroup_index"].cast<uint32_t>();
                        }
                        if(kwargs.contains("shard_index")) {
                            shard_index = kwargs["shard_index"].cast<uint32_t>();
                        }
                        std::string val = std::string(value);
                        on_all_subgroup_type(service_type, trigger_put, capi, key, val, subgroup_index, shard_index);
                    },
                    "Trigger put an object. The object only trigger an action. The object is not stored.")
            .def(
                    "remove", [](ServiceClientAPI& capi, std::string service_type, std::string& key, py::kwargs kwargs) {
                        // Test if subgroup_index and shard_index is specified.
                        uint32_t subgroup_index = UINT32_MAX;
                        uint32_t shard_index = 0;
                        if(kwargs.contains("subgroup_index")) {
                            subgroup_index = kwargs["subgroup_index"].cast<uint32_t>();
                        }
                        if(kwargs.contains("shard_index")) {
                            shard_index = kwargs["shard_index"].cast<uint32_t>();
                        }
                        on_non_trigger_subgroup_type(service_type, return remove, capi, key, subgroup_index, shard_index);

                        return py::cast(NULL);
                    },
                    "Remove a long key and its corresponding value from cascade.")
            .def(
                    "get", [](ServiceClientAPI& capi, std::string service_type, std::string& key, persistent::version_t ver, py::kwargs kwargs) {
                        // Test if subgroup_index and shard_index is specified.
                        uint32_t subgroup_index = UINT32_MAX;
                        uint32_t shard_index = 0;
                        if(kwargs.contains("subgroup_index")) {
                            subgroup_index = kwargs["subgroup_index"].cast<uint32_t>();
                        }
                        if(kwargs.contains("shard_index")) {
                            shard_index = kwargs["shard_index"].cast<uint32_t>();
                        }
                        on_non_trigger_subgroup_type(service_type, return get, capi, key, ver, subgroup_index, shard_index);

                        return py::cast(std::string(""));
                    },
                    "Get the value corresponding to the long key from cascade.")
            .def(
                    "get_by_time", [](ServiceClientAPI& capi, std::string service_type, std::string& key, uint64_t ts_us, py::kwargs kwargs) {
                        // Test if subgroup_index and shard_index is specified.
                        uint32_t subgroup_index = UINT32_MAX;
                        uint32_t shard_index = 0;
                        if(kwargs.contains("subgroup_index")) {
                            subgroup_index = kwargs["subgroup_index"].cast<uint32_t>();
                        }
                        if(kwargs.contains("shard_index")) {
                            shard_index = kwargs["shard_index"].cast<uint32_t>();
                        }
                        on_non_trigger_subgroup_type(service_type, return get_by_time, capi, key, ts_us, subgroup_index, shard_index);

                        return py::cast(std::string(""));
                    },
                    "Get the value corresponding to the long key from cascade by the timestamp.")
            .def(
                    "create_object_pool", [](ServiceClientAPI& capi, const std::string& service_type,const std::string& object_pool_pathname, uint32_t subgroup_index) {
                        on_all_subgroup_type(service_type, return create_object_pool, capi, object_pool_pathname, subgroup_index);
                        return py::cast(std::string(""));
                    },
                    "Create an Object Pool")
            .def(
                    "list_object_pools", [](ServiceClientAPI& capi) {
                        return list_object_pools(capi);
                    },
                    "List the object pools")
            .def(
                    "get_object_pool", [](ServiceClientAPI& capi, const std::string& object_pool_pathname) {
                        return get_object_pool(capi, object_pool_pathname);
                    },
                    "Get an object pool by pathname."
                 );

    py::class_<QueryResultsStore<std::tuple<persistent::version_t, uint64_t>, std::vector<long>>>(m, "QueryResultsStoreVerTmeStmp")
            .def(
                    "get_result", [](QueryResultsStore<std::tuple<persistent::version_t, uint64_t>, std::vector<long>>& qrs) {
                        return qrs.get_result();
                    },
                    "Get result from QueryResultsStore for version and timestamp");

    py::class_<QueryResultsStore<const ObjectWithStringKey, py::bytes>>(m, "QueryResultsStoreObjectWithStringKey")
            .def(
                    "get_result", [](QueryResultsStore<const ObjectWithStringKey, py::bytes>& qrs) {
                        return qrs.get_result();
                    },
                    "Get result from QueryResultsStore for ObjectWithStringKey");

    py::class_<QueryResultsStore<const ObjectWithUInt64Key, py::bytes>>(m, "QueryResultsStoreObjectWithUInt64Key")
            .def(
                    "get_result", [](QueryResultsStore<const ObjectWithUInt64Key, py::bytes>& qrs) {
                        return qrs.get_result();
                    },
                    "Get result from QueryResultsStore for ObjectWithUInt64Key");
}
