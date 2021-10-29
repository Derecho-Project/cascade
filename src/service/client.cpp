#include <cascade/service_client_api.hpp>
#include <iostream>
#include <string>
#include <fstream>
#include <typeindex>
#include <stdio.h>
#include <readline/readline.h>
#include <readline/history.h>
#include <cascade/utils.hpp>
#include <sys/prctl.h>
#ifdef ENABLE_EVALUATION
#include "perftest.hpp"
#endif//ENABLE_EVALUATION

using namespace derecho::cascade;

#define PROC_NAME   "cascade_client"

template <typename SubgroupType>
void print_shard_member(ServiceClientAPI& capi, uint32_t subgroup_index, uint32_t shard_index) {
    std::cout << "Subgroup (Type=" << std::type_index(typeid(SubgroupType)).name() << ","
              << "subgroup_index=" << subgroup_index << ","
              << "shard_index="    << shard_index    << ") member list = [";
    auto members = capi.template get_shard_members<SubgroupType>(subgroup_index,shard_index);
    for (auto nid : members) {
        std::cout << nid << ",";
    }
    std::cout << "]" << std::endl;
}

/** disabled
void print_shard_member(ServiceClientAPI& capi, derecho::subgroup_id_t subgroup_id, uint32_t shard_index) {
    std::cout << "subgroup_id=" << subgroup_id << ","
              << "shard_index=" << shard_index << " member list = [";
    auto members = capi.get_shard_members(subgroup_id,shard_index);
    for (auto nid : members) {
        std::cout << nid << ",";
    }
    std::cout << "]" << std::endl;
}
**/

static const char* policy_names[] = {
    "FirstMember",
    "LastMember",
    "Random",
    "FixedRandom",
    "RoundRobin",
    "UserSpecified",
    nullptr
};

inline ShardMemberSelectionPolicy parse_policy_name(const std::string& policy_name) {
    ShardMemberSelectionPolicy policy = ShardMemberSelectionPolicy::FirstMember;
    int i=1;
    while(policy_names[i]){
        if (policy_name == policy_names[i]) {
            policy = static_cast<ShardMemberSelectionPolicy>(i);
            break;
        }
        i++;
    }
    if (policy_names[i] == nullptr) {
        return ShardMemberSelectionPolicy::InvalidPolicy;
    }
    return policy;
}

template <typename SubgroupType>
void print_member_selection_policy(ServiceClientAPI& capi, uint32_t subgroup_index, uint32_t shard_index) {
    std::cout << "Subgroup (Type=" << std::type_index(typeid(SubgroupType)).name() << ","
              << "subgroup_index=" << subgroup_index << ","
              << "shard_index="    << shard_index    << ") policy=";
    auto policy = capi.template get_member_selection_policy<SubgroupType>(subgroup_index,shard_index);
    std::cout << policy_names[std::get<0>(policy)] << "(" << std::get<0>(policy) << ")," << std::get<1>(policy) << "" << std::endl;
}

template <typename SubgroupType>
void set_member_selection_policy(ServiceClientAPI& capi, uint32_t subgroup_index, uint32_t shard_index,
        ShardMemberSelectionPolicy policy, node_id_t user_specified_node_id) {
    capi.template set_member_selection_policy<SubgroupType>(subgroup_index,shard_index,policy,user_specified_node_id);
}

/* TEST1: members */
void member_test(ServiceClientAPI& capi) {
    // print all members.
    std::cout << "Top Derecho group members = [";
    auto members = capi.get_members();
    for (auto nid: members) {
        std::cout << nid << "," ;
    }
    std::cout << "]" << std::endl;
    // print per Subgroup Members:
    print_shard_member<VolatileCascadeStoreWithStringKey>(capi,0,0);
    print_shard_member<PersistentCascadeStoreWithStringKey>(capi,0,0);
}

static std::vector<std::string> tokenize(std::string &line, const char *delimiter) {
    std::vector<std::string> tokens;
    char line_buf[1024];
    std::strcpy(line_buf, line.c_str());
    char *token = std::strtok(line_buf, delimiter);
    while (token != nullptr) {
        tokens.push_back(std::string(token));
        token = std::strtok(NULL, delimiter);
    }
    return tokens; // RVO
}

static void print_red(std::string msg) {
    std::cout << "\033[1;31m"
              << msg
              << "\033[0m" << std::endl;
}

static void print_cyan(std::string msg) {
    std::cout << "\033[1;36m"
              << msg
              << "\033[0m" << std::endl;
}

#define on_subgroup_type(x, ft, ...) \
    if ((x) == "VCSS") { \
        ft <VolatileCascadeStoreWithStringKey>(__VA_ARGS__); \
    } else if ((x) == "PCSS") { \
        ft <PersistentCascadeStoreWithStringKey>(__VA_ARGS__); \
    } else if ((x) == "TCSS") { \
        ft <TriggerCascadeNoStoreWithStringKey>(__VA_ARGS__); \
    } else { \
        print_red("unknown subgroup type:" + x); \
    }

#define check_put_and_remove_result(result) \
    for (auto& reply_future:result.get()) {\
        auto reply = reply_future.second.get();\
        std::cout << "node(" << reply_future.first << ") replied with version:" << std::get<0>(reply)\
                  << ",ts_us:" << std::get<1>(reply) << std::endl;\
    }

template <typename SubgroupType>
void put(ServiceClientAPI& capi, const std::string& key, const std::string& value, persistent::version_t pver, persistent::version_t pver_bk, uint32_t subgroup_index, uint32_t shard_index) {
    typename SubgroupType::ObjectType obj;
    if constexpr (std::is_same<typename SubgroupType::KeyType,uint64_t>::value) {
        obj.key = static_cast<uint64_t>(std::stol(key));
    } else if constexpr (std::is_same<typename SubgroupType::KeyType,std::string>::value) {
        obj.key = key;
    } else {
        print_red(std::string("Unhandled KeyType:") + typeid(typename SubgroupType::KeyType).name());
        return;
    }
    obj.previous_version = pver;
    obj.previous_version_by_key = pver_bk;
    obj.blob = Blob(value.c_str(),value.length());
    derecho::rpc::QueryResults<std::tuple<persistent::version_t,uint64_t>> result = capi.template put<SubgroupType>(obj, subgroup_index, shard_index);
    check_put_and_remove_result(result);
}

template <typename SubgroupType>
void put_and_forget(ServiceClientAPI& capi, const std::string& key, const std::string& value, persistent::version_t pver, persistent::version_t pver_bk, uint32_t subgroup_index, uint32_t shard_index) {
    typename SubgroupType::ObjectType obj;
    if constexpr (std::is_same<typename SubgroupType::KeyType,uint64_t>::value) {
        obj.key = static_cast<uint64_t>(std::stol(key));
    } else if constexpr (std::is_same<typename SubgroupType::KeyType,std::string>::value) {
        obj.key = key;
    } else {
        print_red(std::string("Unhandled KeyType:") + typeid(typename SubgroupType::KeyType).name());
        return;
    }
    obj.previous_version = pver;
    obj.previous_version_by_key = pver_bk;
    obj.blob = Blob(value.c_str(),value.length());
    capi.template put_and_forget<SubgroupType>(obj, subgroup_index, shard_index);
    std::cout << "put done." << std::endl;
}

void op_put(ServiceClientAPI& capi, const std::string& key, const std::string& value, persistent::version_t pver, persistent::version_t pver_bk) {
    ObjectWithStringKey obj;
    obj.key = key;
    obj.previous_version = pver;
    obj.previous_version_by_key = pver_bk;
    obj.blob = Blob(value.c_str(),value.length());
    derecho::rpc::QueryResults<std::tuple<persistent::version_t,uint64_t>> result = capi.put(obj);
    check_put_and_remove_result(result);
}

void op_put_and_forget(ServiceClientAPI& capi, const std::string& key, const std::string& value, persistent::version_t pver, persistent::version_t pver_bk) {
    ObjectWithStringKey obj;
    obj.key = key;
    obj.previous_version = pver;
    obj.previous_version_by_key = pver_bk;
    obj.blob = Blob(value.c_str(),value.length());
    capi.put_and_forget(obj);
    std::cout << "put done." << std::endl;
}

template <typename SubgroupType>
void create_object_pool(ServiceClientAPI& capi, const std::string& id, uint32_t subgroup_index) {
    auto result = capi.template create_object_pool<SubgroupType>(id,subgroup_index);
    check_put_and_remove_result(result);
    std::cout << "create_object_pool is done." << std::endl;
} 

template <typename SubgroupType>
void trigger_put(ServiceClientAPI& capi, const std::string& key, const std::string& value, uint32_t subgroup_index, uint32_t shard_index) {
    typename SubgroupType::ObjectType obj;
    if constexpr (std::is_same<typename SubgroupType::KeyType,uint64_t>::value) {
        obj.key = static_cast<uint64_t>(std::stol(key));
    } else if constexpr (std::is_same<typename SubgroupType::KeyType,std::string>::value) {
        obj.key = key;
    } else {
        print_red(std::string("Unhandled KeyType:") + typeid(typename SubgroupType::KeyType).name());
        return;
    }

    obj.blob = Blob(value.c_str(),value.length());
    derecho::rpc::QueryResults<void> result = capi.template trigger_put<SubgroupType>(obj, subgroup_index, shard_index);
    result.get();
   
    std::cout << "trigger_put is done." << std::endl;
}

void op_trigger_put(ServiceClientAPI& capi, const std::string& key, const std::string& value) {
    ObjectWithStringKey obj;

    obj.key = key;
    obj.blob = Blob(value.c_str(),value.length());
    derecho::rpc::QueryResults<void> result = capi.trigger_put(obj);
    result.get();
   
    std::cout << "op_trigger_put is done." << std::endl;
}

template <typename SubgroupType>
void collective_trigger_put(ServiceClientAPI& capi, const std::string& key, const std::string& value, uint32_t subgroup_index, std::vector<node_id_t> nodes) {
    typename SubgroupType::ObjectType obj;
    if constexpr (std::is_same<typename SubgroupType::KeyType,uint64_t>::value) {
        obj.key = static_cast<uint64_t>(std::stol(key));
    } else if constexpr (std::is_same<typename SubgroupType::KeyType,std::string>::value) {
        obj.key = key;
    } else {
        print_red(std::string("Unhandled KeyType:") + typeid(typename SubgroupType::KeyType).name());
        return;
    }

    obj.blob = Blob(value.c_str(),value.length());
    std::unordered_map<node_id_t,std::unique_ptr<derecho::rpc::QueryResults<void>>> nodes_and_futures;
    for (auto& nid: nodes) {
        nodes_and_futures.emplace(nid,nullptr);
    }
    capi.template collective_trigger_put<SubgroupType>(obj, subgroup_index, nodes_and_futures);
    for (auto& kv: nodes_and_futures) {
        kv.second.get();
        std::cout << "Finish sending to node " << kv.first << std::endl;
    }
   
    std::cout << "collective_trigger_put is done." << std::endl;

}

template <typename SubgroupType>
void remove(ServiceClientAPI& capi, const std::string& key, uint32_t subgroup_index, uint32_t shard_index) {
    if constexpr (std::is_same<typename SubgroupType::KeyType,uint64_t>::value) {
        derecho::rpc::QueryResults<std::tuple<persistent::version_t,uint64_t>> result = std::move(capi.template remove<SubgroupType>(static_cast<uint64_t>(std::stol(key)), subgroup_index, shard_index));
        check_put_and_remove_result(result);
    } else if constexpr (std::is_same<typename SubgroupType::KeyType,std::string>::value) {
        derecho::rpc::QueryResults<std::tuple<persistent::version_t,uint64_t>> result = std::move(capi.template remove<SubgroupType>(key, subgroup_index, shard_index));
        check_put_and_remove_result(result);
    } else {
        print_red(std::string("Unhandled KeyType:") + typeid(typename SubgroupType::KeyType).name());
        return;
    }
}

void op_remove(ServiceClientAPI& capi, const std::string& key) {
    auto result = capi.remove(key);
    check_put_and_remove_result(result);
}

#define check_get_result(result) \
    for (auto& reply_future:result.get()) {\
        auto reply = reply_future.second.get();\
        std::cout << "node(" << reply_future.first << ") replied with value:" << reply << std::endl;\
    }

template <typename SubgroupType>
void get(ServiceClientAPI& capi, const std::string& key, persistent::version_t ver, uint32_t subgroup_index,uint32_t shard_index) {
    if constexpr (std::is_same<typename SubgroupType::KeyType,uint64_t>::value) {
        derecho::rpc::QueryResults<const typename SubgroupType::ObjectType> result = capi.template get<SubgroupType>(
                static_cast<uint64_t>(std::stol(key)),ver,subgroup_index,shard_index);
        check_get_result(result);
    } else if constexpr (std::is_same<typename SubgroupType::KeyType,std::string>::value) {
        derecho::rpc::QueryResults<const typename SubgroupType::ObjectType> result = capi.template get<SubgroupType>(
                key,ver,subgroup_index,shard_index);
        check_get_result(result);
    }
}

template <typename SubgroupType>
void get_by_time(ServiceClientAPI& capi, const std::string& key, uint64_t ts_us, uint32_t subgroup_index, uint32_t shard_index) {
    if constexpr (std::is_same<typename SubgroupType::KeyType,uint64_t>::value) {
        derecho::rpc::QueryResults<const typename SubgroupType::ObjectType> result = capi.template get_by_time<SubgroupType>(
                static_cast<uint64_t>(std::stol(key)),ts_us,subgroup_index,shard_index);
        check_get_result(result);
    } else if constexpr (std::is_same<typename SubgroupType::KeyType,std::string>::value) {
        derecho::rpc::QueryResults<const typename SubgroupType::ObjectType> result = capi.template get_by_time<SubgroupType>(
                key,ts_us,subgroup_index,shard_index);
        check_get_result(result);
    }
}

template <typename SubgroupType>
void get_size(ServiceClientAPI& capi, const std::string& key, persistent::version_t ver, uint32_t subgroup_index,uint32_t shard_index) {
    if constexpr (std::is_same<typename SubgroupType::KeyType,uint64_t>::value) {
        derecho::rpc::QueryResults<uint64_t> result = capi.template get_size<SubgroupType>(
                static_cast<uint64_t>(std::stol(key)),ver,subgroup_index,shard_index);
        check_get_result(result);
    } else if constexpr (std::is_same<typename SubgroupType::KeyType,std::string>::value) {
        derecho::rpc::QueryResults<uint64_t> result = capi.template get_size<SubgroupType>(
                key,ver,subgroup_index,shard_index);
        check_get_result(result);
    }
}

template <typename SubgroupType>
void get_size_by_time(ServiceClientAPI& capi, const std::string& key, uint64_t ts_us, uint32_t subgroup_index, uint32_t shard_index) {
    if constexpr (std::is_same<typename SubgroupType::KeyType,uint64_t>::value) {
        derecho::rpc::QueryResults<uint64_t> result = capi.template get_size_by_time<SubgroupType>(
                static_cast<uint64_t>(std::stol(key)),ts_us,subgroup_index,shard_index);
        check_get_result(result);
    } else if constexpr (std::is_same<typename SubgroupType::KeyType,std::string>::value) {
        derecho::rpc::QueryResults<uint64_t> result = capi.template get_size_by_time<SubgroupType>(
                key,ts_us,subgroup_index,shard_index);
        check_get_result(result);
    }
}

#define check_list_keys_result(result) \
    for (auto& reply_future:result.get()) {\
        auto reply = reply_future.second.get();\
        std::cout << "Keys:" << std::endl;\
        for (auto& key:reply) {\
            std::cout << "    " << key << std::endl;\
        }\
    }

#define check_op_list_keys_result(result)\
    std::cout << "Keys:" << std::endl;\
    for (auto& key:result) {\
        std::cout << "    " << key << std::endl;\
    }


template <typename SubgroupType>
void list_keys(ServiceClientAPI& capi, persistent::version_t ver, uint32_t subgroup_index, uint32_t shard_index) {
    std::cout << "list_keys: ver = " << ver << ", subgroup_index = " << subgroup_index << ", shard_index = " << shard_index << std::endl;
    derecho::rpc::QueryResults<std::vector<typename SubgroupType::KeyType>> result = capi.template list_keys<SubgroupType>(ver,subgroup_index,shard_index);
    check_list_keys_result(result);
}

template <typename SubgroupType>
void list_keys_by_time(ServiceClientAPI& capi, uint64_t ts_us, uint32_t subgroup_index, uint32_t shard_index) {
    derecho::rpc::QueryResults<std::vector<typename SubgroupType::KeyType>> result = capi.template list_keys_by_time<SubgroupType>(ts_us,subgroup_index,shard_index);
    check_list_keys_result(result);
}

#ifdef HAS_BOOLINQ
//    "list_data_by_prefix <type> <prefix> [version] [subgroup_index] [shard_index\n\t test LINQ api\n]"
template <typename SubgroupType>
void list_data_by_prefix(ServiceClientAPI& capi, std::string prefix, persistent::version_t ver, uint32_t subgroup_index, uint32_t shard_index) {
    std::vector<typename SubgroupType::KeyType> keys;
    for (auto& obj : from_shard<SubgroupType,ServiceClientAPI>(keys,capi,subgroup_index,shard_index,ver).where([&prefix](typename SubgroupType::ObjectType o){
                if (o.blob.size < prefix.size()) {
                    return false;
                } else {
                    return (std::string(o.blob.bytes,prefix.size()) == prefix);
                }
            }).toStdVector()) {
        std::cout << "Found:" << obj << std::endl;
    }
}

template <>
void list_data_by_prefix<TriggerCascadeNoStoreWithStringKey>(ServiceClientAPI& capi, std::string prefix, persistent::version_t ver, uint32_t subgroup_index, uint32_t shard_index) {
    print_red("TCSS does not support list_data_by_prefix.");
}


//    "list_data_between_versions <type> <key> <subgroup_index> <shard_index> [version_begin] [version_end]\n\t test LINQ api - version_iterator \n"
template <typename SubgroupType>
void list_data_between_versions(ServiceClientAPI &capi, const std::string& key, uint32_t subgroup_index, uint32_t shard_index, persistent::version_t ver_begin, persistent::version_t ver_end) {
    if constexpr (std::is_same<typename SubgroupType::KeyType, uint64_t>::value) {
        auto result = capi.template get<SubgroupType>(static_cast<uint64_t>(std::stol(key)), ver_end, subgroup_index, shard_index);
        for (auto &reply_future : result.get()) {
            auto reply = reply_future.second.get();
            if (reply.is_valid()) {
                ver_end = reply.version;
            } else {
                return;
            }
        }
        for (auto &obj : from_versions<SubgroupType, ServiceClientAPI>(static_cast<uint64_t>(std::stol(key)), capi, subgroup_index, shard_index, ver_end).where([ver_begin](typename SubgroupType::ObjectType obj) {
                    return ver_begin == INVALID_VERSION || obj.version >= ver_begin;
                }).toStdVector()) {
            std::cout << "Found:" << obj << std::endl;
        }
    } else if constexpr (std::is_same<typename SubgroupType::KeyType, std::string>::value) {
        auto result = capi.template get<SubgroupType>(key, ver_end, subgroup_index, shard_index);
        for (auto &reply_future : result.get()) {
            auto reply = reply_future.second.get();
            if (reply.is_valid()) {
                ver_end = reply.version;
            } else {
                return;
            }
        }
        for (auto &obj : from_versions<SubgroupType, ServiceClientAPI>(key, capi, subgroup_index, shard_index, ver_end).where([ver_begin](typename SubgroupType::ObjectType obj) {
                    return ver_begin == INVALID_VERSION || obj.version >= ver_begin;
                }).toStdVector()) {
            std::cout << "Found:" << obj << std::endl;
        }
    }
}

template <>
void list_data_between_versions<TriggerCascadeNoStoreWithStringKey>(ServiceClientAPI &capi, const std::string& key, uint32_t subgroup_index, uint32_t shard_index, persistent::version_t ver_begin, persistent::version_t ver_end) {
    print_red("TCSS does not support list_data_between_versions.");
}

template <typename SubgroupType>
void list_data_between_timestamps(ServiceClientAPI &capi, const std::string& key, uint32_t subgroup_index, uint32_t shard_index, uint64_t ts_begin, uint64_t ts_end) {
    std::vector<typename SubgroupType::KeyType> keys;
    if constexpr (std::is_same<typename SubgroupType::KeyType, uint64_t>::value) {
        auto result = capi.template get<SubgroupType>(static_cast<uint64_t>(std::stol(key)), CURRENT_VERSION, subgroup_index, shard_index);
        for (auto &reply_future : result.get()) {
            auto reply = reply_future.second.get();
            if (reply.is_valid()) {
                ts_end = reply.timestamp_us >= ts_end ? ts_end : reply.timestamp_us;
            } else {
                return;
            }
        }
        for (auto &obj : from_shard_by_time<SubgroupType, ServiceClientAPI>(keys, capi, subgroup_index, shard_index, ts_end).where([&key,ts_begin](typename SubgroupType::ObjectType obj) {
                    return !obj.is_null() && static_cast<uint64_t>(std::stol(key)) == obj.key && obj.timestamp_us >= ts_begin;
                }).toStdVector()) {
            std::cout << "Found:" << obj << std::endl;
        }
    } else if constexpr (std::is_same<typename SubgroupType::KeyType, std::string>::value) {
        // set the timestamp to the latest update if ts_end > latest_ts
        auto result = capi.template get<SubgroupType>(key, CURRENT_VERSION, subgroup_index, shard_index);
        for (auto &reply_future : result.get()) {
            auto reply = reply_future.second.get();
            if (reply.is_valid()) {
                ts_end = reply.timestamp_us >= ts_end ? ts_end : reply.timestamp_us;
            } else {
                return;
            }
        }
        for (auto &obj : from_shard_by_time<SubgroupType, ServiceClientAPI>(keys, capi, subgroup_index, shard_index, ts_end).where([&key,ts_begin](typename SubgroupType::ObjectType obj) {
                    return (!obj.is_null() && key == obj.key && obj.timestamp_us >= ts_begin);
                }).toStdVector()) {
            std::cout << "Found:" << obj << std::endl;
        }
    }
}

template <>
void list_data_between_timestamps<TriggerCascadeNoStoreWithStringKey>(ServiceClientAPI &capi, const std::string& key, uint32_t subgroup_index, uint32_t shard_index, uint64_t ts_begin, uint64_t ts_end) {
    print_red("TCSS does not support list_data_between_timestamp.");
}

//    "list_data_in_subgroup <type> <subgroup_index> [version]\n\t test LINQ api - subgroup_iterator \n"
template <typename SubgroupType>
void list_data_in_subgroup(ServiceClientAPI& capi, uint32_t subgroup_index, persistent::version_t version) {
    std::vector<typename SubgroupType::KeyType> keys;
    std::vector<CascadeShardLinq<SubgroupType, ServiceClientAPI>> shard_linq_list;

    std::unordered_map<uint32_t, std::vector<typename SubgroupType::KeyType>> shardidx_to_keys; 

    for (auto &obj : from_subgroup<SubgroupType, ServiceClientAPI>(shardidx_to_keys, shard_linq_list, capi, subgroup_index, version).toStdVector()) {
        std::cout << "Found:" << obj << std::endl;
    }
}

template <>
void list_data_in_subgroup<TriggerCascadeNoStoreWithStringKey>(ServiceClientAPI& capi, uint32_t subgroup_index, persistent::version_t version) {
    print_red("TCSS does not support list_data_in_subgroup.");
}

template <typename SubgroupType>
void list_data_in_objectpool(ServiceClientAPI& capi, persistent::version_t version, const std::string& objpool_path) {
    std::vector<typename SubgroupType::KeyType> keys;
    for (auto &obj : from_objectpool<SubgroupType, ServiceClientAPI>(capi,keys,version,objpool_path).toStdVector()) {
        std::cout << "Found:" << obj << std::endl;
    }
}
#endif// HAS_BOOLINQ

#ifdef ENABLE_EVALUATION
// The object pool version of perf test
template <typename SubgroupType>
bool perftest(PerfTestClient& ptc,
              PutType put_type,
              const std::string& object_pool_pathname,
              ExternalClientToCascadeServerMapping ec2cs,
              double read_write_ratio,
              uint64_t ops_threshold,
              uint64_t duration_secs,
              const std::string& output_file) {
    debug_enter_func_with_args("put_type={},object_pool_pathname={},ec2cs={},read_write_ratio={},ops_threshold={},duration_secs={},output_file={}",
                               put_type,object_pool_pathname,static_cast<uint32_t>(ec2cs),read_write_ratio,ops_threshold,duration_secs,output_file);
    bool ret = ptc.template perf_put<SubgroupType>(put_type,object_pool_pathname,ec2cs,read_write_ratio,ops_threshold,duration_secs,output_file);
    debug_leave_func();
    return ret;
}

// The raw shard version of perf test
template <typename SubgroupType>
bool perftest(PerfTestClient& ptc,
              PutType put_type,
              uint32_t subgroup_index,
              uint32_t shard_index,
              ExternalClientToCascadeServerMapping ec2cs,
              double read_write_ratio,
              uint64_t ops_threshold,
              uint64_t duration_secs,
              const std::string& output_file) {
    debug_enter_func_with_args("put_type={},subgroup_index={},shard_index={},ec2cs={},read_write_ratio={},ops_threshold={},duration_secs={},output_file={}",
                               put_type,subgroup_index, shard_index,static_cast<uint32_t>(ec2cs),read_write_ratio,ops_threshold,duration_secs,output_file);
    bool ret = ptc.template perf_put<SubgroupType>(put_type,subgroup_index,shard_index,ec2cs,read_write_ratio,ops_threshold,duration_secs,output_file);
    debug_leave_func();
    return ret;
}

template <typename SubgroupType>
bool perftest_ordered_put(ServiceClientAPI &capi,
                          uint32_t message_size,
                          uint64_t duration_sec,
                          uint32_t subgroup_index,
                          uint32_t shard_index){
    debug_enter_func_with_args("message_size={},duration_sec={},subgroup_index={},shard_index={}.",
                               message_size,duration_sec,subgroup_index,shard_index);
    auto result = capi.template perf_put<SubgroupType>(message_size,duration_sec,subgroup_index,shard_index);
    check_get_result(result);
    debug_leave_func();
    return true;
}

template <>
bool perftest_ordered_put<TriggerCascadeNoStoreWithStringKey>(ServiceClientAPI &capi,
                          uint32_t message_size,
                          uint64_t duration_sec,
                          uint32_t subgroup_index,
                          uint32_t shard_index){
    print_red("TCSS does not support perftest_ordered_put");
    return false;
}

template <typename SubgroupType>
bool dump_timestamp(ServiceClientAPI &capi,
                    uint32_t subgroup_index,
                    uint32_t shard_index,
                    const std::string& filename) {
    debug_enter_func_with_args("subgroup_index={}, shard_index={}, filename={}",
                               subgroup_index,shard_index,filename);
    auto result = capi.template dump_timestamp<SubgroupType>(filename,subgroup_index,shard_index);
    result.get();
    global_timestamp_logger.flush(filename);
    debug_leave_func();
    return true;
}
#endif // ENABLE_EVALUATION


/* TEST2: put/get/remove tests */
using command_handler_t = std::function<bool(ServiceClientAPI& capi,const std::vector<std::string>& cmd_tokens)>;
struct command_entry_t {
    const std::string cmd;              // command name
    const std::string desc;             // help info
    const std::string help;             // full help
    const command_handler_t handler;    // handler
};

void list_commands(const std::vector<command_entry_t>& command_list) {
    for (const auto& entry: command_list) {
        if (entry.handler) {
            std::cout << std::left << std::setw(32) << entry.cmd << "- " << entry.desc << std::endl;
        } else {
            print_cyan("# " + entry.cmd + " #");
        }
    }
}

ssize_t find_command(const std::vector<command_entry_t>& command_list, const std::string& command) {
    ssize_t pos = 0;
    for(;pos < static_cast<ssize_t>(command_list.size());pos++) {
        if (command_list.at(pos).cmd == command) {
            break;
        }
    }
    if (pos == static_cast<ssize_t>(command_list.size())) {
        pos = -1;
    }
    return pos;
}


bool shell_is_active = true;
#define SUBGROUP_TYPE_LIST "VCSS|PCSS|TCSS"
#define SHARD_MEMBER_SELECTION_POLICY_LIST "FirstMember|LastMember|Random|FixedRandom|RoundRobin|UserSpecified"
std::vector<command_entry_t> commands = 
{
    {
        "General Commands","","",command_handler_t()
    },
    {
        "help",
        "Print help info",
        "help [command name]",
        [](ServiceClientAPI&,const std::vector<std::string>& cmd_tokens){
            if (cmd_tokens.size() >= 2) {
                ssize_t command_index = find_command(commands,cmd_tokens[1]);
                if (command_index < 0) {
                    print_red("unknown command:'"+cmd_tokens[1]+"'.");
                } else {
                    std::cout << commands.at(command_index).help << std::endl;
                }
                return (command_index>=0);
            } else {
                list_commands(commands);
                return true;
            }
        }
    },
    {
        "quit",
        "Exit",
        "quit",
        [](ServiceClientAPI&,const std::vector<std::string>& cmd_tokens) {
            shell_is_active = false;
            return true;
        }
    },
    {
        "Membership Commands","","",command_handler_t()
    },
    {
        "list_members",
        "List the IDs of all nodes in the Cascade service.",
        "list_members",
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            std::cout << "Cascade service members = [";
            auto members = capi.get_members();
            for (auto nid: members) {
                std::cout << nid << "," ;
            }
            std::cout << "]" << std::endl;
            return true;
        }
    },
    {
        "list_shard_members",
        "List the IDs in a shard specified by type, subgroup index, and shard index.",
        "list_shard_members <type> [subgroup index(default:0)] [shard index(default:0)]\n"
            "type := " SUBGROUP_TYPE_LIST,
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            uint32_t subgroup_index = 0, shard_index = 0;
            if (cmd_tokens.size() < 2) {
                print_red("Invalid command format. Please try help " + cmd_tokens[0] + ".");
                return false;
            }
            if (cmd_tokens.size() >= 3) {
                subgroup_index = static_cast<uint32_t>(std::stoi(cmd_tokens[2]));
            }
            if (cmd_tokens.size() >= 4) {
                shard_index = static_cast<uint32_t>(std::stoi(cmd_tokens[3]));
            }
            on_subgroup_type(cmd_tokens[1],print_shard_member,capi,subgroup_index,shard_index);
            return true;
        }
    },
    {
        "set_member_selection_policy",
        "Set the policy for choosing among a set of server members.",
        "set_member_selection_policy <type> <subgroup_index> <shard_index> <policy> [user specified node id]\n"
            "type := " SUBGROUP_TYPE_LIST
            "policy := " SHARD_MEMBER_SELECTION_POLICY_LIST,
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            if (cmd_tokens.size() < 5) {
                print_red("Invalid command format. Please try help " + cmd_tokens[0] + ".");
                return false;
            }
            uint32_t subgroup_index = static_cast<uint32_t>(std::stoi(cmd_tokens[2]));
            uint32_t shard_index = static_cast<uint32_t>(std::stoi(cmd_tokens[3]));
            ShardMemberSelectionPolicy policy = parse_policy_name(cmd_tokens[4]);
            if (policy == ShardMemberSelectionPolicy::InvalidPolicy) {
                print_red("Invalid policy name:" + cmd_tokens[4]);
                return false;
            }
            node_id_t user_specified_node_id = INVALID_NODE_ID;
            if (cmd_tokens.size() >= 6) {
                user_specified_node_id = static_cast<node_id_t>(std::stoi(cmd_tokens[5]));
            }
            on_subgroup_type(cmd_tokens[1],set_member_selection_policy,capi,subgroup_index,shard_index,policy,user_specified_node_id);
            return true;
        }
    },
    {
        "get_member_selection_policy",
        "Get the policy for choosing among a set of server members.",
        "get_member_selection_policy <type> <subgroup_index> <shard_index>\n"
            "type := " SUBGROUP_TYPE_LIST,
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            if (cmd_tokens.size() < 4) {
                print_red("Invalid command format. Please try help " + cmd_tokens[0] + ".");
                return false;
            }
            uint32_t subgroup_index = static_cast<uint32_t>(std::stoi(cmd_tokens[2]));
            uint32_t shard_index = static_cast<uint32_t>(std::stoi(cmd_tokens[3]));
            on_subgroup_type(cmd_tokens[1],print_member_selection_policy,capi,subgroup_index,shard_index);
            return true;
        }
    },
    {
        "Object Pool Manipulation Commands","","",command_handler_t()
    },
    {
        "list_object_pools",
        "List existing object pools",
        "list_object_pools",
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            std::cout << "refreshed object pools:" << std::endl;
            for (std::string& opath: capi.list_object_pools(true)) {
                std::cout << "\t" << opath << std::endl;
            }
            return true;
        }
    },
    {
        "create_object_pool",
        "Create an object pool",
        "create_object_pool <path> <type> <subgroup_index>\n"
            "type := " SUBGROUP_TYPE_LIST,
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            if (cmd_tokens.size() < 4) {
                print_red("Invalid command format. Please try help " + cmd_tokens[0] + ".");
                return false;
            }
            std::string opath = cmd_tokens[1];
            uint32_t subgroup_index = static_cast<uint32_t>(std::stoi(cmd_tokens[3]));
            on_subgroup_type(cmd_tokens[2],create_object_pool,capi,opath,subgroup_index);
            return true;
        }
    },
    {
        "remove_object_pool",
        "Soft-Remove an object pool",
        "remove_object_pool <path>",
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            if (cmd_tokens.size() < 2) {
                print_red("Invalid command format. Please try help " + cmd_tokens[0] + ".");
                return false;
            }
            auto result = capi.remove_object_pool(cmd_tokens[1]);
            check_put_and_remove_result(result);
            return true;
        }
    },
    {
        "get_object_pool",
        "Get details of an object pool",
        "get_object_pool <path>",
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            if (cmd_tokens.size() < 2) {
                print_red("Invalid command format. Please try help " + cmd_tokens[0] + ".");
                return false;
            }
            auto opm = capi.find_object_pool(cmd_tokens[1]);
            std::cout << "get_object_pool returns:"
                      << opm << std::endl;
            return true;
        }
    },
    {
        "Object Maniputlation Commands","","",command_handler_t()
    },
    {
        "put",
        "Put an object to a shard.",
        "put <type> <key> <value> <subgroup_index> <shard_index> [previous_version(default:-1)] [previous_version_by_key(default:-1)]\n"
            "type := " SUBGROUP_TYPE_LIST,
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            persistent::version_t pver = persistent::INVALID_VERSION;
            persistent::version_t pver_bk = persistent::INVALID_VERSION;
            if (cmd_tokens.size() < 6) {
                print_red("Invalid command format. Please try help " + cmd_tokens[0] + ".");
                return false;
            }
            uint32_t subgroup_index = static_cast<uint32_t>(std::stoi(cmd_tokens[4]));
            uint32_t shard_index = static_cast<uint32_t>(std::stoi(cmd_tokens[5]));
            if (cmd_tokens.size() >= 7)
                pver = static_cast<persistent::version_t>(std::stol(cmd_tokens[6]));
            if (cmd_tokens.size() >= 8)
                pver_bk = static_cast<persistent::version_t>(std::stol(cmd_tokens[7]));
            on_subgroup_type(cmd_tokens[1],put,capi,cmd_tokens[2]/*key*/,cmd_tokens[3]/*value*/,pver,pver_bk,subgroup_index,shard_index);
            return true;
        }
    },
    {
        "put_and_forget",
        "Put an object to a shard, without a return value",
        "put_and_forget <type> <key> <value> <subgroup_index> <shard_index> [previous_version(default:-1)] [previous_version_by_key(default:-1)]\n"
            "type := " SUBGROUP_TYPE_LIST,
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            persistent::version_t pver = persistent::INVALID_VERSION;
            persistent::version_t pver_bk = persistent::INVALID_VERSION;
            if (cmd_tokens.size() < 6) {
                print_red("Invalid command format. Please try help " + cmd_tokens[0] + ".");
                return false;
            }
            uint32_t subgroup_index = static_cast<uint32_t>(std::stoi(cmd_tokens[4]));
            uint32_t shard_index = static_cast<uint32_t>(std::stoi(cmd_tokens[5]));
            if (cmd_tokens.size() >= 7)
                pver = static_cast<persistent::version_t>(std::stol(cmd_tokens[6]));
            if (cmd_tokens.size() >= 8)
                pver_bk = static_cast<persistent::version_t>(std::stol(cmd_tokens[7]));
            on_subgroup_type(cmd_tokens[1],put_and_forget,capi,cmd_tokens[2]/*key*/,cmd_tokens[3]/*value*/,pver,pver_bk,subgroup_index,shard_index);
            return true;
        }
    },
    {
        "op_put",
        "Put an object into an object pool",
        "op_put <key> <value> [previous_version(default:-1)] [previous_version_by_key(default:-1)]\n"
            "Please note that cascade automatically decides the object pool path using the key's prefix.",
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            persistent::version_t pver = persistent::INVALID_VERSION;
            persistent::version_t pver_bk = persistent::INVALID_VERSION;
            if (cmd_tokens.size() < 3) {
                print_red("Invalid command format. Please try help " + cmd_tokens[0] + ".");
                return false;
            }
            if (cmd_tokens.size() >= 4)
                pver = static_cast<persistent::version_t>(std::stol(cmd_tokens[3]));
            if (cmd_tokens.size() >= 5)
                pver_bk = static_cast<persistent::version_t>(std::stol(cmd_tokens[4]));
            op_put(capi,cmd_tokens[1]/*key*/,cmd_tokens[2]/*value*/,pver,pver_bk);
            return true;
        }
    },
    {
        "op_put_and_forget",
        "Put an object into an object pool, without a return value",
        "op_put_and_forget <key> <value> [previous_version(default:-1)] [previous_version_by_key(default:-1)]\n"
            "type := " SUBGROUP_TYPE_LIST "\n"
            "Please note that cascade automatically decides the object pool path using the key's prefix.",
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            persistent::version_t pver = persistent::INVALID_VERSION;
            persistent::version_t pver_bk = persistent::INVALID_VERSION;
            if (cmd_tokens.size() < 3) {
                print_red("Invalid command format. Please try help " + cmd_tokens[0] + ".");
                return false;
            }
            if (cmd_tokens.size() >= 4)
                pver = static_cast<persistent::version_t>(std::stol(cmd_tokens[3]));
            if (cmd_tokens.size() >= 5)
                pver_bk = static_cast<persistent::version_t>(std::stol(cmd_tokens[4]));
            op_put_and_forget(capi,cmd_tokens[1]/*key*/,cmd_tokens[2]/*value*/,pver,pver_bk);
            return true;
        }
    },
    {
        "trigger_put",
        "Trigger put an object to a shard.",
        "trigger_put <type> <key> <value> <subgroup_index> <shard_index>\n"
            "type := " SUBGROUP_TYPE_LIST,
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            if (cmd_tokens.size() < 6) {
                print_red("Invalid command format. Please try help " + cmd_tokens[0] + ".");
                return false;
            }
            uint32_t subgroup_index = static_cast<uint32_t>(std::stoi(cmd_tokens[4]));
            uint32_t shard_index = static_cast<uint32_t>(std::stoi(cmd_tokens[5]));
            on_subgroup_type(cmd_tokens[1],trigger_put,capi,cmd_tokens[2]/*key*/,cmd_tokens[3]/*value*/,subgroup_index,shard_index);
            return true;
        }
    },
    {
        "op_trigger_put",
        "Trigger put an object to an object pool.",
        "op_trigger_put <key> <value>\n"
            "Please note that cascade automatically decides the object pool path using the key's prefix.",
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            if (cmd_tokens.size() < 3) {
                print_red("Invalid command format. Please try help " + cmd_tokens[0] + ".");
                return false;
            }
            op_trigger_put(capi,cmd_tokens[1]/*key*/,cmd_tokens[2]/*value*/);
            return true;
        }
    },
    {
        "collective_trigger_put",
        "Collectively trigger put an object to a set of nodes in a subgroup.",
        "collective_trigger_put <type> <key> <value> <subgroup_index> <node id 1> [node id 2, ...] \n"
            "type := " SUBGROUP_TYPE_LIST,
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            std::vector<node_id_t> nodes;
            if (cmd_tokens.size() < 6) {
                print_red("Invalid command format. Please try help " + cmd_tokens[0] + ".");
                return false;
            }
            uint32_t subgroup_index = static_cast<uint32_t>(std::stoi(cmd_tokens[4]));
            size_t arg_idx = 5;
            while(arg_idx < cmd_tokens.size()) {
                nodes.push_back(static_cast<node_id_t>(std::stoi(cmd_tokens[arg_idx++])));
            }
            on_subgroup_type(cmd_tokens[1],collective_trigger_put,capi,cmd_tokens[2]/*key*/,cmd_tokens[3]/*value*/,subgroup_index,nodes);
            return true;
        }
    },
    {
        "remove",
        "Remove an object from a shard.",
        "remove <type> <key> <subgroup_index> <shard_index> \n"
            "type := " SUBGROUP_TYPE_LIST,
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            if (cmd_tokens.size() < 5) {
                print_red("Invalid command format. Please try help " + cmd_tokens[0] + ".");
                return false;
            }
            uint32_t subgroup_index = static_cast<uint32_t>(std::stoi(cmd_tokens[3]));
            uint32_t shard_index = static_cast<uint32_t>(std::stoi(cmd_tokens[4]));
            on_subgroup_type(cmd_tokens[1],remove,capi,cmd_tokens[2]/*key*/,subgroup_index,shard_index);
            return true;
        }
    },
    {
        "op_remove",
        "Remove an object from an object pool.",
        "op_remove <key>\n"
            "Please note that cascade automatically decides the object pool path using the key's prefix.",
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            if (cmd_tokens.size() < 2) {
                print_red("Invalid command format. Please try help " + cmd_tokens[0] + ".");
                return false;
            }
            op_remove(capi,cmd_tokens[1]);
            return true;
        }
    },
    {
        "get",
        "Get an object (by version).",
        "get <type> <key> <subgroup_index> <shard_index> [ version(default:current version) ]\n"
            "type := " SUBGROUP_TYPE_LIST,
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            if (cmd_tokens.size() < 5) {
                print_red("Invalid command format. Please try help " + cmd_tokens[0] + ".");
                return false;
            }
            uint32_t subgroup_index = static_cast<uint32_t>(std::stoi(cmd_tokens[3]));
            uint32_t shard_index = static_cast<uint32_t>(std::stoi(cmd_tokens[4]));
            persistent::version_t version = CURRENT_VERSION;
            if (cmd_tokens.size() >= 6) {
                version = static_cast<persistent::version_t>(std::stol(cmd_tokens[5]));
            }
            on_subgroup_type(cmd_tokens[1],get,capi,cmd_tokens[2],version,subgroup_index,shard_index);
            return true;
        }
    },
    {
        "op_get",
        "Get an object from an object pool (by version).",
        "op_get <key> [ version(default:current version) ]\n"
            "Please note that cascade automatically decides the object pool path using the key's prefix.",
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            if (cmd_tokens.size() < 2) {
                print_red("Invalid command format. Please try help " + cmd_tokens[0] + ".");
                return false;
            }
            persistent::version_t version = CURRENT_VERSION;
            if (cmd_tokens.size() >= 3) {
                version = static_cast<persistent::version_t>(std::stol(cmd_tokens[2]));
            }
            auto res = capi.get(cmd_tokens[1],version);
            check_get_result(res);
            return true;
        }
    },
    {
        "get_by_time",
        "Get an object (by timestamp in microseconds).",
        "get_by_time <type> <key> <subgroup_index> <shard_index> <timestamp in us>\n"
            "type := " SUBGROUP_TYPE_LIST,
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            if (cmd_tokens.size() < 6) {
                print_red("Invalid command format. Please try help " + cmd_tokens[0] + ".");
                return false;
            }
            uint32_t subgroup_index = static_cast<uint32_t>(std::stoi(cmd_tokens[3]));
            uint32_t shard_index = static_cast<uint32_t>(std::stoi(cmd_tokens[4]));
            uint64_t ts_us = static_cast<uint64_t>(std::stol(cmd_tokens[3]));
            on_subgroup_type(cmd_tokens[1],get_by_time,capi,cmd_tokens[2],ts_us,subgroup_index,shard_index);
            return true;
        }
    },
    {
        "op_get_by_time",
        "Get an object from an object pool (by timestamp in microseconds).",
        "op_get_by_time <key> <timestamp in us>\n"
            "Please note that cascade automatically decides the object pool path using the key's prefix.",
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            if (cmd_tokens.size() < 3) {
                print_red("Invalid command format. Please try help " + cmd_tokens[0] + ".");
                return false;
            }
            uint64_t ts_us = static_cast<uint64_t>(std::stol(cmd_tokens[2]));
            auto res = capi.get_by_time(cmd_tokens[1],ts_us);
            check_get_result(res);
            return true;
        }
    },
    {
        "get_size",
        "Get the size of an object (by version).",
        "get_size <type> <key> <subgroup_index> <shard_index> [ version(default:current version) ]\n"
            "type := " SUBGROUP_TYPE_LIST,
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            if (cmd_tokens.size() < 5) {
                print_red("Invalid command format. Please try help " + cmd_tokens[0] + ".");
                return false;
            }
            uint32_t subgroup_index = static_cast<uint32_t>(std::stoi(cmd_tokens[3]));
            uint32_t shard_index = static_cast<uint32_t>(std::stoi(cmd_tokens[4]));
            persistent::version_t version = CURRENT_VERSION;
            if (cmd_tokens.size() >= 6) {
                version = static_cast<persistent::version_t>(std::stol(cmd_tokens[5]));
            }
            on_subgroup_type(cmd_tokens[1],get_size,capi,cmd_tokens[2],version,subgroup_index,shard_index);
            return true;
        }
    },
    {
        "op_get_size",
        "Get the size of an object from an object pool (by version).",
        "op_get_size <key> [ version(default:current version) ]\n"
            "Please note that cascade automatically decides the object pool path using the key's prefix.",
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            if (cmd_tokens.size() < 2) {
                print_red("Invalid command format. Please try help " + cmd_tokens[0] + ".");
                return false;
            }
            persistent::version_t version = CURRENT_VERSION;
            if (cmd_tokens.size() >= 3) {
                version = static_cast<persistent::version_t>(std::stol(cmd_tokens[2]));
            }
            auto res = capi.get_size(cmd_tokens[1],version);
            check_get_result(res);
            return true;
        }
    },
    {
        "get_size_by_time",
        "Get the size of an object (by timestamp in microseconds).",
        "get_size_by_time <type> <key> <subgroup_index> <shard_index> <timestamp in us>\n"
            "type := " SUBGROUP_TYPE_LIST,
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            if (cmd_tokens.size() < 6) {
                print_red("Invalid command format. Please try help " + cmd_tokens[0] + ".");
                return false;
            }
            uint32_t subgroup_index = static_cast<uint32_t>(std::stoi(cmd_tokens[3]));
            uint32_t shard_index = static_cast<uint32_t>(std::stoi(cmd_tokens[4]));
            uint64_t ts_us = static_cast<uint64_t>(std::stol(cmd_tokens[3]));
            on_subgroup_type(cmd_tokens[1],get_size_by_time,capi,cmd_tokens[2],ts_us,subgroup_index,shard_index);
            return true;
        }
    },
    {
        "op_get_size_by_time",
        "Get the size of an object from an object pool (by timestamp in microseconds).",
        "op_get_size_by_time <key> <timestamp in us>\n"
            "Please note that cascade automatically decides the object pool path using the key's prefix.",
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            if (cmd_tokens.size() < 3) {
                print_red("Invalid command format. Please try help " + cmd_tokens[0] + ".");
                return false;
            }
            uint64_t ts_us = static_cast<uint64_t>(std::stol(cmd_tokens[2]));
            auto res = capi.get_size_by_time(cmd_tokens[1],ts_us);
            check_get_result(res);
            return true;
        }
    },
    {
        "list_keys",
        "list the object keys in a shard (by version).",
        "list_keys <type> <subgroup_index> <shard_index> [ version(default:current version) ]\n"
            "type := " SUBGROUP_TYPE_LIST,
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            if (cmd_tokens.size() < 4) {
                print_red("Invalid command format. Please try help " + cmd_tokens[0] + ".");
                return false;
            }
            uint32_t subgroup_index = static_cast<uint32_t>(std::stoi(cmd_tokens[2]));
            uint32_t shard_index = static_cast<uint32_t>(std::stoi(cmd_tokens[3]));
            persistent::version_t version = CURRENT_VERSION;
            if (cmd_tokens.size() >= 5) {
                version = static_cast<persistent::version_t>(std::stol(cmd_tokens[4]));
            }
            on_subgroup_type(cmd_tokens[1],list_keys,capi,version,subgroup_index,shard_index);
            return true;
        }
    },
    {
        "op_list_keys",
        "list the object keys in an object pool (by version).",
        "op_list_keys <object pool pathname> [ version(default:current version) ]\n",
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            if (cmd_tokens.size() < 2) {
                print_red("Invalid command format. Please try help " + cmd_tokens[0] + ".");
                return false;
            }
            persistent::version_t version = CURRENT_VERSION;
            if (cmd_tokens.size() >= 3) {
                version = static_cast<persistent::version_t>(std::stol(cmd_tokens[2]));
            }
            auto result = capi.list_keys(version,cmd_tokens[1]);
            check_op_list_keys_result(capi.wait_list_keys(result));
            return true;
        }
    },
    {
        "list_keys_by_time",
        "list the object keys in a shard (by timestamp in mircoseconds).",
        "list_keys_by_time <type> <subgroup_index> <shard_index> <timestamp in us>\n"
            "type := " SUBGROUP_TYPE_LIST,
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            if (cmd_tokens.size() < 5) {
                print_red("Invalid command format. Please try help " + cmd_tokens[0] + ".");
                return false;
            }
            uint32_t subgroup_index = static_cast<uint32_t>(std::stoi(cmd_tokens[2]));
            uint32_t shard_index = static_cast<uint32_t>(std::stoi(cmd_tokens[3]));
            uint64_t ts_us = static_cast<uint64_t>(std::stoull(cmd_tokens[4]));
            on_subgroup_type(cmd_tokens[1],list_keys_by_time,capi,ts_us,subgroup_index,shard_index);
            return true;
        }
    },
    {
        "op_list_keys_by_time",
        "list the object keys in an object pool (by timestamp in microseconds).",
        "op_list_keys_by_time <object pool pathname> <timestamp in us>\n",
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            if (cmd_tokens.size() < 3) {
                print_red("Invalid command format. Please try help " + cmd_tokens[0] + ".");
                return false;
            }
            uint64_t ts_us = static_cast<uint64_t>(std::stoull(cmd_tokens[2]));
            auto result = capi.list_keys_by_time(ts_us,cmd_tokens[1]);
            check_op_list_keys_result(capi.wait_list_keys(result));
            return true;
        }
    },
#ifdef HAS_BOOLINQ
    {
        "LINQ Tester Commands", "", "", command_handler_t()
    },
    {
        "list_data_by_prefix",
        "LINQ API Tester: list the object with a specific prefix",
        "list_data_by_prefix <type> <prefix> <subgroup_index> <shard_index> [ version(default:current version) ] \n"
            "type := " SUBGROUP_TYPE_LIST,
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            if (cmd_tokens.size() < 5) {
                print_red("Invalid command format. Please try help " + cmd_tokens[0] + ".");
                return false;
            }
            const std::string& prefix = cmd_tokens[2];
            uint32_t subgroup_index = static_cast<uint32_t>(std::stoi(cmd_tokens[3]));
            uint32_t shard_index = static_cast<uint32_t>(std::stoi(cmd_tokens[4]));
            persistent::version_t version = CURRENT_VERSION;
            if (cmd_tokens.size() >= 6) {
                version = static_cast<persistent::version_t>(std::stol(cmd_tokens[5]));
            }
            on_subgroup_type(cmd_tokens[1],list_data_by_prefix,capi,prefix,version,subgroup_index,shard_index);
            return true;
        }
    },
    {
        "list_data_between_versions",
        "LINQ API Tester: list an object data between versions",
        "list_data_between_versions <type> <key> <subgroup_index> <shard_index> [ start version(default:MIN) ] [ end version (default:MAX) ] \n"
            "type := " SUBGROUP_TYPE_LIST,
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            if (cmd_tokens.size() < 5) {
                print_red("Invalid command format. Please try help " + cmd_tokens[0] + ".");
                return false;
            }
            uint32_t subgroup_index = static_cast<uint32_t>(std::stoi(cmd_tokens[3]));
            uint32_t shard_index = static_cast<uint32_t>(std::stoi(cmd_tokens[4]));
  
            persistent::version_t version_start = INVALID_VERSION;
            persistent::version_t version_end = INVALID_VERSION;
            if (cmd_tokens.size() >= 6) {
                version_start = static_cast<persistent::version_t>(std::stol(cmd_tokens[5]));
            }
            if (cmd_tokens.size() >= 7) {
                version_end = static_cast<persistent::version_t>(std::stol(cmd_tokens[6]));
            }
            on_subgroup_type(cmd_tokens[1], list_data_between_versions, capi, cmd_tokens[2], subgroup_index, shard_index, version_start, version_end);
            return true;
        }
    },
    {
        "list_data_between_timestamps",
        "LINQ API Tester: list an object data between points of time",
        "list_data_between_timestamps <type> <key> <subgroup_index> <shard_index> [ start time(default:MIN) ] [ end time (default:MAX) ] \n"
            "type := " SUBGROUP_TYPE_LIST,
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            if (cmd_tokens.size() < 5) {
                print_red("Invalid command format. Please try help " + cmd_tokens[0] + ".");
                return false;
            }
            uint32_t subgroup_index = static_cast<uint32_t>(std::stoi(cmd_tokens[3]));
            uint32_t shard_index = static_cast<uint32_t>(std::stoi(cmd_tokens[4]));
  
            uint64_t start = 0;
            uint64_t end = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
            if (cmd_tokens.size() >= 6) {
                start = static_cast<uint64_t>(std::stol(cmd_tokens[5]));
            }
            if (cmd_tokens.size() >= 7) {
                end = static_cast<uint64_t>(std::stol(cmd_tokens[6]));
            }
            on_subgroup_type(cmd_tokens[1], list_data_between_timestamps, capi, cmd_tokens[2], subgroup_index, shard_index, start, end);
            return true;
        }
    },
    {
        "list_data_in_subgroup",
        "LINQ API Tester: list all objects in a subgroup",
        "list_data_in_subgroup <type> <subgroup_index> [ version (default:CURRENT_VERSION) ] \n"
            "type := " SUBGROUP_TYPE_LIST,
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            if (cmd_tokens.size() < 3) {
                print_red("Invalid command format. Please try help " + cmd_tokens[0] + ".");
                return false;
            }
            uint32_t subgroup_index = static_cast<uint32_t>(std::stoi(cmd_tokens[2]));

            persistent::version_t version = INVALID_VERSION;
            if (cmd_tokens.size() >= 4) {
                version = static_cast<persistent::version_t>(std::stol(cmd_tokens[3]));
            }
            on_subgroup_type(cmd_tokens[1], list_data_in_subgroup, capi, subgroup_index, version);
            return true;
        }
    },
#endif// HAS_BOOLINQ
#ifdef ENABLE_EVALUATION
    {
        "Performance Test Commands","","",command_handler_t()
    },
    {
        "perftest_object_pool",
        "Performance Tester for put to an object pool.",
        "perftest_object_pool <type> <forget> <object pool pathname> <member selection policy> <r/w ratio> <max rate> <duration in sec> <client1> [<client2>, ...] \n"
            "type := " SUBGROUP_TYPE_LIST "\n"
            "put_type := put|put_and_forget|trigger_put \n"
            "'member selection policy' refers how the external clients pick a member in a shard;\n"
            "    Available options: FIXED|RANDOM|ROUNDROBIN;\n"
            "'r/w ratio' is the ratio of get vs put operations, INF for all put test; \n"
            "'max rate' is the maximum number of operations in Operations per Second, 0 for best effort; \n"
            "'duration' is the span of the whole experiments; \n"
            "'clientn' is a host[:port] pair representing the parallel clients. The port is default to " + std::to_string(PERFTEST_PORT),
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            if (cmd_tokens.size() < 9) {
                print_red("Invalid command format. Please try help " + cmd_tokens[0] + ".");
                return false;
            }

            PutType put_type = PutType::PUT;

            if (cmd_tokens[2] == "put_and_forget") {
                put_type = PutType::PUT_AND_FORGET;
            } else if (cmd_tokens[2] == "trigger_put") {
                put_type = PutType::TRIGGER_PUT;
            }

            std::string object_pool_pathname = cmd_tokens[3];
            ExternalClientToCascadeServerMapping member_selection_policy = FIXED;
            if (cmd_tokens[4] == "RANDOM") {
                member_selection_policy = ExternalClientToCascadeServerMapping::RANDOM;
            } else if (cmd_tokens[4] == "ROUNDROBIN") {
                member_selection_policy = ExternalClientToCascadeServerMapping::ROUNDROBIN;
            }
            double read_write_ratio = std::stod(cmd_tokens[5]);
            uint64_t max_rate = std::stoul(cmd_tokens[6]);
            uint64_t duration_sec = std::stoul(cmd_tokens[7]);

            PerfTestClient ptc{capi};
            uint32_t pos = 8;
            while (pos < cmd_tokens.size()) {
                std::string::size_type colon_pos = cmd_tokens[pos].find(':');
                if (colon_pos == std::string::npos) {
                    ptc.add_or_update_server(cmd_tokens[pos],PERFTEST_PORT);
                } else {
                    ptc.add_or_update_server(cmd_tokens[pos].substr(0,colon_pos),
                                             static_cast<uint16_t>(std::stoul(cmd_tokens[pos].substr(colon_pos+1))));
                }
                pos ++;
            }
            bool ret = false;
            on_subgroup_type(cmd_tokens[1], ret = perftest, ptc, put_type, object_pool_pathname, member_selection_policy,read_write_ratio,max_rate,duration_sec,"timestamp.log");
            return ret;
        }
    },
    {
        "perftest_shard",
        "Performance Tester for put to a shard.",
        "perftest_shard <type> <put type> <subgroup index> <shard index> <member selection policy> <r/w ratio> <max rate> <duration in sec> <client1> [<client2>, ...] \n"
            "type := " SUBGROUP_TYPE_LIST "\n"
            "put_type := put|put_and_forget|trigger_put \n"
            "'member selection policy' refers how the external clients pick a member in a shard;\n"
            "    Available options: FIXED|RANDOM|ROUNDROBIN;\n"
            "'r/w ratio' is the ratio of get vs put operations, INF for all put test; \n"
            "'max rate' is the maximum number of operations in Operations per Second, 0 for best effort; \n"
            "'duration' is the span of the whole experiments; \n"
            "'clientn' is a host[:port] pair representing the parallel clients. The port is default to " + std::to_string(PERFTEST_PORT),
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            if (cmd_tokens.size() < 10) {
                print_red("Invalid command format. Please try help " + cmd_tokens[0] + ".");
                return false;
            }

            PutType put_type = PutType::PUT;

            if (cmd_tokens[2] == "put_and_forget") {
                put_type = PutType::PUT_AND_FORGET;
            } else if (cmd_tokens[2] == "trigger_put") {
                put_type = PutType::TRIGGER_PUT;
            }

            uint32_t subgroup_index = std::stoul(cmd_tokens[3]);
            uint32_t shard_index = std::stoul(cmd_tokens[4]);

            ExternalClientToCascadeServerMapping member_selection_policy = FIXED;
            if (cmd_tokens[5] == "RANDOM") {
                member_selection_policy = ExternalClientToCascadeServerMapping::RANDOM;
            } else if (cmd_tokens[5] == "ROUNDROBIN") {
                member_selection_policy = ExternalClientToCascadeServerMapping::ROUNDROBIN;
            }
            double read_write_ratio = std::stod(cmd_tokens[6]);
            uint64_t max_rate = std::stoul(cmd_tokens[7]);
            uint64_t duration_sec = std::stoul(cmd_tokens[8]);

            PerfTestClient ptc{capi};
            uint32_t pos = 9;
            while (pos < cmd_tokens.size()) {
                std::string::size_type colon_pos = cmd_tokens[pos].find(':');
                if (colon_pos == std::string::npos) {
                    ptc.add_or_update_server(cmd_tokens[pos],PERFTEST_PORT);
                } else {
                    ptc.add_or_update_server(cmd_tokens[pos].substr(0,colon_pos),
                                             static_cast<uint16_t>(std::stoul(cmd_tokens[pos].substr(colon_pos+1))));
                }
                pos ++;
            }
            bool ret = false;
            on_subgroup_type(cmd_tokens[1], ret = perftest,ptc,put_type,subgroup_index,shard_index,member_selection_policy,read_write_ratio,max_rate,duration_sec,"output.log");
            return ret;
        }
    },
    {
        "perftest_ordered_put",
        "Performance Test for ordered_put in a shard.",
        "perftest_ordered_put <type> <message_size> <duration_sec> <subgroup index> <shard_index>\n"
            "type := " SUBGROUP_TYPE_LIST "\n"
            "'duration_sec' is the span of the whole experiments",
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            if (cmd_tokens.size() < 6) {
                print_red("Invalid command format. Please try help " + cmd_tokens[0] + ".");
                return false;
            }
            uint32_t message_size = std::stoul(cmd_tokens[2]);
            uint64_t duration_sec = std::stoul(cmd_tokens[3]);
            uint32_t subgroup_index = std::stoul(cmd_tokens[4]);
            uint32_t shard_index = std::stoul(cmd_tokens[5]);

            on_subgroup_type(cmd_tokens[1], perftest_ordered_put, capi, message_size, duration_sec, subgroup_index, shard_index);
            return true;
        }
    },
    {
        "dump_timestamp",
        "Dump timestamp for a given shard. Each node will write its timestamps to the given file.",
        "dump_timestamp <type> <subgroup index> <shard index> <filename>\n"
            "type := " SUBGROUP_TYPE_LIST "\n"
            "filename := timestamp log filename",
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            if (cmd_tokens.size() < 5) {
                print_red("Invalid command format. Please try help " + cmd_tokens[0] + ".");
                return false;
            }
            uint32_t subgroup_index = std::stoul(cmd_tokens[2]);
            uint32_t shard_index = std::stoul(cmd_tokens[3]);
            on_subgroup_type(cmd_tokens[1]/*subgroup type*/, dump_timestamp, capi, subgroup_index, shard_index, cmd_tokens[4]/*filename*/);
            return true;
        }
    },
#endif
};

inline void do_command(ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
    try {
        ssize_t command_index = find_command(commands, cmd_tokens[0]);
        if (command_index>=0) {
            if (commands.at(command_index).handler(capi,cmd_tokens)) {
                std::cout << "-> Succeeded." << std::endl;
            } else {
                std::cout << "-> Failed." << std::endl;
            }
        } else {
            print_red("unknown command:" + cmd_tokens[0]);
        }
    } catch (const derecho::derecho_exception &ex) {
        print_red (std::string("Exception:") + ex.what());
    } catch (...) {
        print_red ("Unknown exception caught.");
    }
}

void interactive_test(ServiceClientAPI& capi) {
    // loop
    while (shell_is_active) {
        char* malloced_cmd = readline("cmd> ");
        std::string cmdline(malloced_cmd);
        free(malloced_cmd);
        if (cmdline == "")continue;
        add_history(cmdline.c_str());

        std::string delimiter = " ";
        do_command(capi,tokenize(cmdline, delimiter.c_str()));
    }
    std::cout << "Client exits." << std::endl;
}

void detached_test(ServiceClientAPI& capi, int argc, char** argv) {
    std::vector<std::string> cmd_tokens;
    for(int i=1;i<argc;i++) {
        cmd_tokens.emplace_back(argv[i]);
    }
    do_command(capi, cmd_tokens);
}

int main(int argc,char** argv) {
    if( prctl(PR_SET_NAME, PROC_NAME, 0, 0, 0) != 0 ) {
        dbg_default_debug("Failed to set proc name to {}.",PROC_NAME);
    }
    ServiceClientAPI capi;
#ifdef ENABLE_EVALUATION
    // start working thread.
    PerfTestServer pts(capi);
#endif 
    if (argc == 1) {
        // by default, we use the interactive shell.
        interactive_test(capi);
    } else {
        detached_test(capi,argc,argv);
    }
    return 0;
}
