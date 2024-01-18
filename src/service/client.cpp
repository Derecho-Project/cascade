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

/**
 * The shell variables.
 */
std::map<std::string,std::string> shell_vars = {};

template <typename SubgroupType>
void print_subgroup_member(ServiceClientAPI& capi, uint32_t subgroup_index) {
    std::cout << "Subgroup (Type=" << std::type_index(typeid(SubgroupType)).name() << ","
              << "subgroup_index=" << subgroup_index << ")" << std::endl;
    auto members = capi.template get_subgroup_members<SubgroupType>(subgroup_index);
    uint32_t shard_index = 0;
    for (const auto& shard: members) {
        std::cout << "shard-" << shard_index << " = [";
        for (const auto& nid: shard) {
            std::cout << nid << ",";
        }
        std::cout << "]" << std::endl;
        shard_index ++;
    }
}

void print_subgroup_member(ServiceClientAPI& capi, const std::string& op) {
    std::cout << "Object Pool=" << op << std::endl;
    auto members = capi.get_subgroup_members(op);
    uint32_t shard_index = 0;
    for (const auto& shard: members) {
        std::cout << "shard-" << shard_index << " = [";
        for (const auto& nid: shard) {
            std::cout << nid << ",";
        }
        std::cout << "]" << std::endl;
    }
    shard_index++;
}

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

void print_shard_member(ServiceClientAPI& capi, const std::string& op, uint32_t shard_index) {
    std::cout << "Object Pool=" << op << ",\n"
              << "shard_index=" << shard_index << ",\nmember list=[";
    auto members = capi.get_shard_members(op,shard_index);
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

/**
 * IMPORTANT: the order of the policy_name has to match ShardMemberSelectionPolicy
 * defined in include/cascade/service.hpp
 */
static const char* policy_names[] = {
    "FirstMember",
    "LastMember",
    "Random",
    "FixedRandom",
    "RoundRobin",
    "KeyHashing",
    "UserSpecified",
    nullptr
};

inline ShardMemberSelectionPolicy parse_policy_name(const std::string& policy_name) {
    ShardMemberSelectionPolicy policy = ShardMemberSelectionPolicy::InvalidPolicy;
    int i=0;
    while(policy_names[i]){
        if (policy_name == policy_names[i]) {
            policy = static_cast<ShardMemberSelectionPolicy>(i);
            break;
        }
        i++;
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
        shell_vars["put.version"] =         std::to_string(std::get<0>(reply)); \
        shell_vars["put.timestamp_us"] =    std::to_string(std::get<1>(reply)); \
    }

template <typename SubgroupType>
void put(ServiceClientAPI& capi, const std::string& key, const std::string& value, persistent::version_t pver, persistent::version_t pver_bk, uint32_t subgroup_index, uint32_t shard_index) {
    typename SubgroupType::ObjectType obj;
    if constexpr (std::is_same<typename SubgroupType::KeyType,uint64_t>::value) {
        obj.key = static_cast<uint64_t>(std::stol(key,nullptr,0));
    } else if constexpr (std::is_same<typename SubgroupType::KeyType,std::string>::value) {
        obj.key = key;
    } else {
        print_red(std::string("Unhandled KeyType:") + typeid(typename SubgroupType::KeyType).name());
        return;
    }
    obj.previous_version = pver;
    obj.previous_version_by_key = pver_bk;
    obj.blob = Blob(reinterpret_cast<const uint8_t*>(value.c_str()),value.length());
    derecho::rpc::QueryResults<derecho::cascade::version_tuple> result = capi.template put<SubgroupType>(obj, subgroup_index, shard_index);
    check_put_and_remove_result(result);
}

template <typename SubgroupType>
void put_and_forget(ServiceClientAPI& capi, const std::string& key, const std::string& value, persistent::version_t pver, persistent::version_t pver_bk, uint32_t subgroup_index, uint32_t shard_index) {
    typename SubgroupType::ObjectType obj;
    if constexpr (std::is_same<typename SubgroupType::KeyType,uint64_t>::value) {
        obj.key = static_cast<uint64_t>(std::stol(key,nullptr,0));
    } else if constexpr (std::is_same<typename SubgroupType::KeyType,std::string>::value) {
        obj.key = key;
    } else {
        print_red(std::string("Unhandled KeyType:") + typeid(typename SubgroupType::KeyType).name());
        return;
    }
    obj.previous_version = pver;
    obj.previous_version_by_key = pver_bk;
    obj.blob = Blob(reinterpret_cast<const uint8_t*>(value.c_str()),value.length());
    capi.template put_and_forget<SubgroupType>(obj, subgroup_index, shard_index);
    std::cout << "put done." << std::endl;
}

void op_put(ServiceClientAPI& capi, const std::string& key, const std::string& value, persistent::version_t pver, persistent::version_t pver_bk) {
    ObjectWithStringKey obj;
    obj.key = key;
    obj.previous_version = pver;
    obj.previous_version_by_key = pver_bk;
    obj.blob = Blob(reinterpret_cast<const uint8_t*>(value.c_str()),value.length());
    derecho::rpc::QueryResults<derecho::cascade::version_tuple> result = capi.put(obj);
    check_put_and_remove_result(result);
}

void op_put_file(ServiceClientAPI& capi, const std::string& key, const std::string& filename, persistent::version_t pver, persistent::version_t pver_bk) {
    // get file size
    std::ifstream value_file(filename,std::ios::binary);
    if(!value_file.good()) {
        dbg_default_error("Cannot open file:{} for read.", filename);
        throw std::runtime_error("Cannot open file:" + filename + "for read");
    }
    value_file.seekg(0, std::ios::end);
    std::size_t file_size = value_file.tellg();
    value_file.seekg(0);
    // message generator
    blob_generator_func_t message_generator = [&value_file] (uint8_t* buffer, const std::size_t size) {
        value_file.read(reinterpret_cast<char*>(buffer),size);
        return size;
    };
    ObjectWithStringKey obj(key,message_generator,file_size);
    obj.previous_version = pver;
    obj.previous_version_by_key = pver_bk;
    derecho::rpc::QueryResults<derecho::cascade::version_tuple> result = capi.put(obj);
    value_file.close();
    check_put_and_remove_result(result);
}

void op_put_and_forget(ServiceClientAPI& capi, const std::string& key, const std::string& value, persistent::version_t pver, persistent::version_t pver_bk) {
    ObjectWithStringKey obj;
    obj.key = key;
    obj.previous_version = pver;
    obj.previous_version_by_key = pver_bk;
    obj.blob = Blob(reinterpret_cast<const uint8_t*>(value.c_str()),value.length());
    capi.put_and_forget(obj);
    std::cout << "put done." << std::endl;
}

void op_put_file_and_forget(ServiceClientAPI& capi, const std::string& key, const std::string& filename, persistent::version_t pver, persistent::version_t pver_bk) {
    // get file size
    std::ifstream value_file(filename,std::ios::binary);
    if(!value_file.good()) {
        dbg_default_error("Cannot open file:{} for read.", filename);
        throw std::runtime_error("Cannot open file:" + filename + "for read");
    }
    value_file.seekg(0, std::ios::end);
    std::size_t file_size = value_file.tellg();
    value_file.seekg(0);
    // message generator
    blob_generator_func_t message_generator = [&value_file] (uint8_t* buffer, const std::size_t size) {
        value_file.read(reinterpret_cast<char*>(buffer),size);
        return size;
    };
    ObjectWithStringKey obj(key,message_generator,file_size);
    obj.previous_version = pver;
    obj.previous_version_by_key = pver_bk;
    capi.put_and_forget(obj);
    std::cout << "put done." << std::endl;
}

template <typename SubgroupType>
void create_object_pool(ServiceClientAPI& capi, const std::string& id, uint32_t subgroup_index,
                        const std::string& affinity_set_regex) {
    auto result = capi.template create_object_pool<SubgroupType>(
            id,
            subgroup_index,
            sharding_policy_type::HASH,
            {},
            affinity_set_regex);
    check_put_and_remove_result(result);
    std::cout << "create_object_pool is done." << std::endl;
}

template <typename SubgroupType>
void trigger_put(ServiceClientAPI& capi, const std::string& key, const std::string& value, uint32_t subgroup_index, uint32_t shard_index) {
    typename SubgroupType::ObjectType obj;
    if constexpr (std::is_same<typename SubgroupType::KeyType,uint64_t>::value) {
        obj.key = static_cast<uint64_t>(std::stol(key,nullptr,0));
    } else if constexpr (std::is_same<typename SubgroupType::KeyType,std::string>::value) {
        obj.key = key;
    } else {
        print_red(std::string("Unhandled KeyType:") + typeid(typename SubgroupType::KeyType).name());
        return;
    }

    obj.blob = Blob(reinterpret_cast<const uint8_t*>(value.c_str()),value.length());
    derecho::rpc::QueryResults<void> result = capi.template trigger_put<SubgroupType>(obj, subgroup_index, shard_index);
    result.get();

    std::cout << "trigger_put is done." << std::endl;
}

void op_trigger_put(ServiceClientAPI& capi, const std::string& key, const std::string& value) {
    ObjectWithStringKey obj;

    obj.key = key;
    obj.blob = Blob(reinterpret_cast<const uint8_t*>(value.c_str()),value.length());
    derecho::rpc::QueryResults<void> result = capi.trigger_put(obj);
    result.get();

    std::cout << "op_trigger_put is done." << std::endl;
}

template <typename SubgroupType>
void collective_trigger_put(ServiceClientAPI& capi, const std::string& key, const std::string& value, uint32_t subgroup_index, std::vector<node_id_t> nodes) {
    typename SubgroupType::ObjectType obj;
    if constexpr (std::is_same<typename SubgroupType::KeyType,uint64_t>::value) {
        obj.key = static_cast<uint64_t>(std::stol(key,nullptr,0));
    } else if constexpr (std::is_same<typename SubgroupType::KeyType,std::string>::value) {
        obj.key = key;
    } else {
        print_red(std::string("Unhandled KeyType:") + typeid(typename SubgroupType::KeyType).name());
        return;
    }

    obj.blob = Blob(reinterpret_cast<const uint8_t*>(value.c_str()),value.length());
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
        derecho::rpc::QueryResults<derecho::cascade::version_tuple> result = std::move(capi.template remove<SubgroupType>(static_cast<uint64_t>(std::stol(key,nullptr,0)), subgroup_index, shard_index));
        check_put_and_remove_result(result);
    } else if constexpr (std::is_same<typename SubgroupType::KeyType,std::string>::value) {
        derecho::rpc::QueryResults<derecho::cascade::version_tuple> result = std::move(capi.template remove<SubgroupType>(key, subgroup_index, shard_index));
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
        [](auto const & x) { \
            if constexpr (std::is_same_v<std::decay_t<decltype(x)>,ObjectWithStringKey> || \
                          std::is_same_v<std::decay_t<decltype(x)>,ObjectWithUInt64Key> ) { \
                shell_vars["object.version"] =                  std::to_string(x.version);\
                shell_vars["object.timestamp_us"] =             std::to_string(x.timestamp_us);\
                shell_vars["object.previous_version"] =         std::to_string(x.previous_version);\
                shell_vars["object.previous_version_by_key"] =  std::to_string(x.previous_version_by_key);\
            } \
        } (reply); \
    }

template <typename SubgroupType>
void get(ServiceClientAPI& capi, const std::string& key, persistent::version_t ver, bool stable, uint32_t subgroup_index,uint32_t shard_index) {
    if constexpr (std::is_same<typename SubgroupType::KeyType,uint64_t>::value) {
        derecho::rpc::QueryResults<const typename SubgroupType::ObjectType> result = capi.template get<SubgroupType>(
                static_cast<uint64_t>(std::stol(key,nullptr,0)),ver,stable,subgroup_index,shard_index);
        check_get_result(result);
    } else if constexpr (std::is_same<typename SubgroupType::KeyType,std::string>::value) {
        derecho::rpc::QueryResults<const typename SubgroupType::ObjectType> result = capi.template get<SubgroupType>(
                key,ver,stable,subgroup_index,shard_index);
        check_get_result(result);
    }
}

template <typename SubgroupType>
void get_by_time(ServiceClientAPI& capi, const std::string& key, uint64_t ts_us, bool stable, uint32_t subgroup_index, uint32_t shard_index) {
    if constexpr (std::is_same<typename SubgroupType::KeyType,uint64_t>::value) {
        derecho::rpc::QueryResults<const typename SubgroupType::ObjectType> result = capi.template get_by_time<SubgroupType>(
                static_cast<uint64_t>(std::stol(key,nullptr,0)),ts_us,stable,subgroup_index,shard_index);
        check_get_result(result);
    } else if constexpr (std::is_same<typename SubgroupType::KeyType,std::string>::value) {
        derecho::rpc::QueryResults<const typename SubgroupType::ObjectType> result = capi.template get_by_time<SubgroupType>(
                key,ts_us,stable,subgroup_index,shard_index);
        check_get_result(result);
    }
}

template <typename SubgroupType>
void multi_get(ServiceClientAPI& capi, const std::string& key, uint32_t subgroup_index, uint32_t shard_index) {
    if constexpr (std::is_same<typename SubgroupType::KeyType,uint64_t>::value) {
        derecho::rpc::QueryResults<const typename SubgroupType::ObjectType> result = capi.template multi_get<SubgroupType>(
                static_cast<uint64_t>(std::stol(key,nullptr,0)),subgroup_index,shard_index);
        check_get_result(result);
    } else if constexpr (std::is_same<typename SubgroupType::KeyType,std::string>::value) {
        derecho::rpc::QueryResults<const typename SubgroupType::ObjectType> result = capi.template multi_get<SubgroupType>(
                key,subgroup_index,shard_index);
        check_get_result(result);
    }
}

template <typename SubgroupType>
void get_size(ServiceClientAPI& capi, const std::string& key, persistent::version_t ver, bool stable, uint32_t subgroup_index,uint32_t shard_index) {
    if constexpr (std::is_same<typename SubgroupType::KeyType,uint64_t>::value) {
        derecho::rpc::QueryResults<uint64_t> result = capi.template get_size<SubgroupType>(
                static_cast<uint64_t>(std::stol(key,nullptr,0)),ver,stable,subgroup_index,shard_index);
        check_get_result(result);
    } else if constexpr (std::is_same<typename SubgroupType::KeyType,std::string>::value) {
        derecho::rpc::QueryResults<uint64_t> result = capi.template get_size<SubgroupType>(
                key,ver,stable,subgroup_index,shard_index);
        check_get_result(result);
    }
}

template <typename SubgroupType>
void multi_get_size(ServiceClientAPI& capi, const std::string& key, uint32_t subgroup_index, uint32_t shard_index) {
    if constexpr ( std::is_same<typename SubgroupType::KeyType,uint64_t>::value) {
        derecho::rpc::QueryResults<uint64_t> result = capi.template multi_get_size<SubgroupType> (
                static_cast<uint64_t>(std::stol(key,nullptr,0)),subgroup_index,shard_index);
        check_get_result(result);
    } else if constexpr (std::is_same<typename SubgroupType::KeyType,std::string>::value) {
        derecho::rpc::QueryResults<uint64_t> result = capi.template multi_get_size<SubgroupType>(
                key,subgroup_index,shard_index);
        check_get_result(result);
    }
}

template <typename SubgroupType>
void get_size_by_time(ServiceClientAPI& capi, const std::string& key, uint64_t ts_us, bool stable, uint32_t subgroup_index, uint32_t shard_index) {
    if constexpr (std::is_same<typename SubgroupType::KeyType,uint64_t>::value) {
        derecho::rpc::QueryResults<uint64_t> result = capi.template get_size_by_time<SubgroupType>(
                static_cast<uint64_t>(std::stol(key,nullptr,0)),ts_us,stable,subgroup_index,shard_index);
        check_get_result(result);
    } else if constexpr (std::is_same<typename SubgroupType::KeyType,std::string>::value) {
        derecho::rpc::QueryResults<uint64_t> result = capi.template get_size_by_time<SubgroupType>(
                key,ts_us,stable,subgroup_index,shard_index);
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
void multi_list_keys(ServiceClientAPI& capi, uint32_t subgroup_index, uint32_t shard_index) {
    derecho::rpc::QueryResults<std::vector<typename SubgroupType::KeyType>> result = capi.template multi_list_keys<SubgroupType>(subgroup_index,shard_index);
    check_list_keys_result(result);
}

template <typename SubgroupType>
void list_keys(ServiceClientAPI& capi, persistent::version_t ver, bool stable, uint32_t subgroup_index, uint32_t shard_index) {
    derecho::rpc::QueryResults<std::vector<typename SubgroupType::KeyType>> result = capi.template list_keys<SubgroupType>(ver,stable,subgroup_index,shard_index);
    check_list_keys_result(result);
}

template <typename SubgroupType>
void list_keys_by_time(ServiceClientAPI& capi, uint64_t ts_us, bool stable, uint32_t subgroup_index, uint32_t shard_index) {
    derecho::rpc::QueryResults<std::vector<typename SubgroupType::KeyType>> result = capi.template list_keys_by_time<SubgroupType>(ts_us,stable,subgroup_index,shard_index);
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
                    return (std::string(reinterpret_cast<const char*>(o.blob.bytes),prefix.size()) == prefix);
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
        auto result = capi.template get<SubgroupType>(static_cast<uint64_t>(std::stol(key,nullptr,0)), ver_end, subgroup_index, shard_index);
        for (auto &reply_future : result.get()) {
            auto reply = reply_future.second.get();
            if (reply.is_valid()) {
                ver_end = reply.version;
            } else {
                return;
            }
        }
        for (auto &obj : from_versions<SubgroupType, ServiceClientAPI>(static_cast<uint64_t>(std::stol(key,nullptr,0)), capi, subgroup_index, shard_index, ver_end).where([ver_begin](typename SubgroupType::ObjectType obj) {
                    return ver_begin == INVALID_VERSION || obj.version >= ver_begin;
                }).toStdVector()) {
            std::cout << "Found:" << obj << std::endl;
        }
    } else if constexpr (std::is_same<typename SubgroupType::KeyType, std::string>::value) {
        auto result = capi.template get<SubgroupType>(key, ver_end, true/*always use stable here*/, subgroup_index, shard_index);
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
        auto result = capi.template get<SubgroupType>(static_cast<uint64_t>(std::stol(key,nullptr,0)), CURRENT_VERSION, subgroup_index, shard_index);
        for (auto &reply_future : result.get()) {
            auto reply = reply_future.second.get();
            if (reply.is_valid()) {
                ts_end = reply.timestamp_us >= ts_end ? ts_end : reply.timestamp_us;
            } else {
                return;
            }
        }
        for (auto &obj : from_shard_by_time<SubgroupType, ServiceClientAPI>(keys, capi, subgroup_index, shard_index, ts_end).where([&key,ts_begin](typename SubgroupType::ObjectType obj) {
                    return !obj.is_null() && static_cast<uint64_t>(std::stol(key,nullptr,0)) == obj.key && obj.timestamp_us >= ts_begin;
                }).toStdVector()) {
            std::cout << "Found:" << obj << std::endl;
        }
    } else if constexpr (std::is_same<typename SubgroupType::KeyType, std::string>::value) {
        // set the timestamp to the latest update if ts_end > latest_ts
        auto result = capi.template get<SubgroupType>(key, CURRENT_VERSION, true/*always stable version here*/, subgroup_index, shard_index);
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

// register notification
template <typename SubgroupType>
bool register_notification(ServiceClientAPI& capi, uint32_t subgroup_index) {
    return capi.template register_notification_handler<SubgroupType>(
            [](const Blob& msg)->void{
                std::cout << "Subgroup Notification received:"
                          << "data:" << std::string(reinterpret_cast<const char*>(msg.bytes),msg.size)
                          << std::endl;
            },
            subgroup_index);
}

// unregister notification
template <typename SubgroupType>
bool unregister_notification(ServiceClientAPI& capi, uint32_t subgroup_index) {
    return capi.template register_notification_handler<SubgroupType>({},subgroup_index);
}

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

// The object pool version of get perf test
template <typename SubgroupType>
bool perftest_get(PerfTestClient& ptc,
                  const std::string& object_pool_pathname,
                  ExternalClientToCascadeServerMapping ec2cs,
                  int32_t log_depth,
                  uint64_t ops_threshold,
                  uint64_t duration_secs,
                  const std::string& output_filename) {
    debug_enter_func_with_args("object_pool_pathname={},ec2cs={},log_depth={},ops_threshold={},duration_secs={},output_filename={}",
                               object_pool_pathname, static_cast<uint32_t>(ec2cs), log_depth, ops_threshold, duration_secs, output_filename);
    bool ret = ptc.template perf_get<SubgroupType>(object_pool_pathname, ec2cs, log_depth, ops_threshold, duration_secs, output_filename);
    debug_leave_func();
    return ret;
}

// The raw shard version of get perf test
template <typename SubgroupType>
bool perftest_get(PerfTestClient& ptc,
                  uint32_t subgroup_index,
                  uint32_t shard_index,
                  ExternalClientToCascadeServerMapping ec2cs,
                  int32_t log_depth,
                  uint64_t ops_threshold,
                  uint64_t duration_secs,
                  const std::string& output_filename) {
    debug_enter_func_with_args("subgroup_index={},shard_index={},ec2cs={},log_depth={},ops_threshold={},duration_secs={},output_filename={}",
                               subgroup_index, shard_index, static_cast<uint32_t>(ec2cs), log_depth, ops_threshold, duration_secs, output_filename);
    bool ret = ptc.template perf_get<SubgroupType>(subgroup_index, shard_index, ec2cs, log_depth, ops_threshold, duration_secs, output_filename);
    debug_leave_func();
    return ret;
}

// Object pool version of get_by_time perf test
// Can only run on PersistentCascadeStore, so no template parameter
bool perftest_get_by_time(PerfTestClient& ptc,
                          const std::string& object_pool_pathname,
                          ExternalClientToCascadeServerMapping ec2cs,
                          uint64_t ms_in_past,
                          uint64_t ops_threshold,
                          uint64_t duration_secs,
                          const std::string& output_filename) {
    debug_enter_func_with_args("object_pool_pathname={},ec2cs={},ms_in_past={},ops_threshold={},duration_secs={},output_filename={}",
                               object_pool_pathname, static_cast<uint32_t>(ec2cs), ms_in_past, ops_threshold, duration_secs, output_filename);
    bool ret = ptc.perf_get_by_time<PersistentCascadeStoreWithStringKey>(object_pool_pathname, ec2cs, ms_in_past, ops_threshold, duration_secs, output_filename);
    debug_leave_func();
    return ret;
}

// Raw shard version of get_by_time perf test
// Can only run on PersistentCascadeStore, so no template parameter
bool perftest_get_by_time(PerfTestClient& ptc,
                          uint32_t subgroup_index,
                          uint32_t shard_index,
                          ExternalClientToCascadeServerMapping ec2cs,
                          uint64_t ms_in_past,
                          uint64_t ops_threshold,
                          uint64_t duration_secs,
                          const std::string& output_filename) {
    debug_enter_func_with_args("subgroup_index={},shard_index={},ec2cs={},ms_in_past={},ops_threshold={},duration_secs={},output_filename={}",
                               subgroup_index, shard_index, static_cast<uint32_t>(ec2cs), ms_in_past, ops_threshold, duration_secs, output_filename);
    bool ret = ptc.perf_get_by_time<PersistentCascadeStoreWithStringKey>(subgroup_index, shard_index, ec2cs, ms_in_past, ops_threshold, duration_secs, output_filename);
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
    TimestampLogger::flush(filename);
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


inline bool do_command(ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens);

bool shell_is_active = true;
#define SUBGROUP_TYPE_LIST "VCSS|PCSS|TCSS"
#define SHARD_MEMBER_SELECTION_POLICY_LIST "FirstMember|LastMember|Random|FixedRandom|RoundRobin|KeyHashing|UserSpecified"
#define CHECK_FORMAT(tks,argc) \
            if (tks.size() < argc) { \
                print_red("Invalid command format. Please try help " + tks[0] + "."); \
                return false; \
            }
/**
 * Replace all @varname@ with variables from shell_vars.
 */
inline std::string expand_variables(const std::string& input) {
    // STEP 1 - find variables
    int32_t state = 0; // searching
    std::string::size_type s = 0,pos = -1;
    std::string expanded = input;
    do {
        pos = expanded.find("@",pos+1);
        if (pos == std::string::npos) {
            break;
        }
        if (state == 0) {
            state = 1;
            s = pos;
        } else {
            std::string var_name = expanded.substr(s+1,pos-s-1);
            if (shell_vars.find(var_name) == shell_vars.cend()) {
                print_red("Variable " + var_name + " does not exist.");
            } else {
                expanded = expanded.substr(0,s) + shell_vars.at(var_name) + expanded.substr(pos+1);
            }
            // state == 1
            state = 0;
        }
    } while (true);
    return expanded;
}


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
        "Script commands","","",command_handler_t()
    },
    {
        "script",
        "Run a client script composed of command separated by lines",
        "script <script_file1> [script_File2,script_File3,...]",
        [](ServiceClientAPI& capi,const std::vector<std::string>& cmd_tokens){
            CHECK_FORMAT(cmd_tokens,2);
            for(size_t fidx = 1; fidx < cmd_tokens.size(); fidx++) {
                std::ifstream iscript(cmd_tokens[fidx]);
                char command_buffer[4096];
                while(iscript.getline(command_buffer,4096)) {
                    std::string cmd_str(command_buffer);
                    auto tokens = tokenize(cmd_str, " ");
                    if (tokens.empty()) {
                        continue;
                    }
                    if (tokens[0].at(0) == '#') {
                        continue;
                    }
                    if (do_command(capi, tokenize(cmd_str," ")) == false) {
                        return false;
                    }
                }
            }
            return true;
        }
    },
    {
        "vars",
        "show the shell variables.",
        "vars",
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            CHECK_FORMAT(cmd_tokens,1);
            std::cout << std::left << std::setw(32) << std::setfill(' ') << "KEY";
            std::cout << std::left << std::setw(64) << std::setfill(' ') << "VALUE";
            std::cout << std::endl;
            for(const auto& kv:shell_vars) {
                std::cout << std::left << std::setw(32) << std::setfill(' ') << (kv.first+" = ");
                std::cout << std::left << std::setw(64) << std::setfill(' ') << kv.second;
                std::cout << std::endl;
            }
            return true;
        }
    },
    {
        "setvar",
        "set an environment variable.",
        "setvar <key> <value>",
        [](ServiceClientAPI& capi,const std::vector<std::string>& cmd_tokens) {
            CHECK_FORMAT(cmd_tokens,3);
            shell_vars[cmd_tokens[1]] = cmd_tokens[2];
            return true;
        }
    },
    {
        "calc",
        "set an environment variable.",
        "calc <resvar> <value>\n"
        "   value can be an arithmetic expression of integers.",
        [](ServiceClientAPI& capi,const std::vector<std::string>& cmd_tokens) {
            CHECK_FORMAT(cmd_tokens,3);
            std::string expression = cmd_tokens[2];
            for(size_t idx=3;idx<cmd_tokens.size();idx++) {
                expression=expression+cmd_tokens[idx];
            }
            shell_vars[cmd_tokens[1]] = std::to_string(evaluate_arithmetic_expression(expression));
            return true;
        }
    },
    {
        "getvar",
        "get an environment variable.",
        "getvar <key>",
        [](ServiceClientAPI& capi,const std::vector<std::string>& cmd_tokens) {
            CHECK_FORMAT(cmd_tokens,2);
            if (shell_vars.find(cmd_tokens[1])!=shell_vars.end()) {
                std::cout << std::left << std::setw(32) << std::setfill(' ') << (cmd_tokens[1]+" = ");
                std::cout << std::left << std::setw(64) << std::setfill(' ') << shell_vars.at(cmd_tokens[1]);
                std::cout << std::endl;
            } else {
                std::cout << cmd_tokens[1] << "is not found." << std::endl;
                return false;
            }
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
        "list_subgroup_members",
        "List the nodes in a subgroup specified by type and subgroup index.",
        "list_subgroup_members <type> [subgroup index(default:0)]\n"
            "type := " SUBGROUP_TYPE_LIST,
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            uint32_t subgroup_index = 0;
            CHECK_FORMAT(cmd_tokens,2);
            if (cmd_tokens.size() >= 3) {
                subgroup_index = static_cast<uint32_t>(std::stoi(cmd_tokens[2],nullptr,0));
            }
            on_subgroup_type(cmd_tokens[1],print_subgroup_member,capi,subgroup_index);
            return true;
        }
    },
    {
        "op_list_subgroup_members",
        "List the subgroup members by object pool name.",
        "op_list_subgroup_members <object pool pathname>",
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            CHECK_FORMAT(cmd_tokens,2);
            print_subgroup_member(capi,cmd_tokens[1]);
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
            CHECK_FORMAT(cmd_tokens,2);
            if (cmd_tokens.size() >= 3) {
                subgroup_index = static_cast<uint32_t>(std::stoi(cmd_tokens[2],nullptr,0));
            }
            if (cmd_tokens.size() >= 4) {
                shard_index = static_cast<uint32_t>(std::stoi(cmd_tokens[3],nullptr,0));
            }
            on_subgroup_type(cmd_tokens[1],print_shard_member,capi,subgroup_index,shard_index);
            return true;
        }
    },
    {
        "op_list_shard_members",
        "List the shard members by object pool name.",
        "op_list_shard_members <object pool pathname> [shard index(default:0)]",
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            uint32_t shard_index = 0;
            CHECK_FORMAT(cmd_tokens,2);
            if (cmd_tokens.size() >= 3) {
                shard_index = static_cast<uint32_t>(std::stoi(cmd_tokens[2],nullptr,0));
            }
            print_shard_member(capi,cmd_tokens[1],shard_index);
            return true;
        }
    },
    {
        "set_member_selection_policy",
        "Set the policy for choosing among a set of server members.",
        "set_member_selection_policy <type> <subgroup_index> <shard_index> <policy> [user specified node id]\n"
            "type := " SUBGROUP_TYPE_LIST "\n"
            "policy := " SHARD_MEMBER_SELECTION_POLICY_LIST,
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            CHECK_FORMAT(cmd_tokens,5);
            uint32_t subgroup_index = static_cast<uint32_t>(std::stoi(cmd_tokens[2],nullptr,0));
            uint32_t shard_index = static_cast<uint32_t>(std::stoi(cmd_tokens[3],nullptr,0));
            ShardMemberSelectionPolicy policy = parse_policy_name(cmd_tokens[4]);
            if (policy == ShardMemberSelectionPolicy::InvalidPolicy) {
                print_red("Invalid policy name:" + cmd_tokens[4]);
                return false;
            }
            node_id_t user_specified_node_id = INVALID_NODE_ID;
            if (cmd_tokens.size() >= 6) {
                user_specified_node_id = static_cast<node_id_t>(std::stoi(cmd_tokens[5],nullptr,0));
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
            CHECK_FORMAT(cmd_tokens,4);
            uint32_t subgroup_index = static_cast<uint32_t>(std::stoi(cmd_tokens[2],nullptr,0));
            uint32_t shard_index = static_cast<uint32_t>(std::stoi(cmd_tokens[3],nullptr,0));
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
            for (std::string& opath: capi.list_object_pools(true,true)) {
                std::cout << "\t" << opath << std::endl;
            }
            return true;
        }
    },
    {
        "create_object_pool",
        "Create an object pool",
        "create_object_pool <path> <type> <subgroup_index> [affinity_set_regex]\n"
        "type := " SUBGROUP_TYPE_LIST "\n"
        "Note: put.[version,timestamp_us] will be set.",
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            CHECK_FORMAT(cmd_tokens,4);
            std::string opath = cmd_tokens[1];
            uint32_t subgroup_index = static_cast<uint32_t>(std::stoi(cmd_tokens[3],nullptr,0));
            std::string affinity_set_regex;
            if (cmd_tokens.size() >= 5) {
                affinity_set_regex = cmd_tokens[4];
            }
            on_subgroup_type(cmd_tokens[2],create_object_pool,capi,opath,subgroup_index,affinity_set_regex);
            return true;
        }
    },
    {
        "remove_object_pool",
        "Soft-Remove an object pool",
        "remove_object_pool <path>\n"
        "Note: put.[version,timestamp_us] will be set.",
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            CHECK_FORMAT(cmd_tokens,2);
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
            CHECK_FORMAT(cmd_tokens,2);
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
            "type := " SUBGROUP_TYPE_LIST "\n"
            "Note: put.[version,timestamp_us] will be set.",
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            persistent::version_t pver = persistent::INVALID_VERSION;
            persistent::version_t pver_bk = persistent::INVALID_VERSION;
            CHECK_FORMAT(cmd_tokens,6);
            uint32_t subgroup_index = static_cast<uint32_t>(std::stoi(cmd_tokens[4],nullptr,0));
            uint32_t shard_index = static_cast<uint32_t>(std::stoi(cmd_tokens[5],nullptr,0));
            if (cmd_tokens.size() >= 7)
                pver = static_cast<persistent::version_t>(std::stol(cmd_tokens[6],nullptr,0));
            if (cmd_tokens.size() >= 8)
                pver_bk = static_cast<persistent::version_t>(std::stol(cmd_tokens[7],nullptr,0));
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
            CHECK_FORMAT(cmd_tokens,6);
            uint32_t subgroup_index = static_cast<uint32_t>(std::stoi(cmd_tokens[4],nullptr,0));
            uint32_t shard_index = static_cast<uint32_t>(std::stoi(cmd_tokens[5],nullptr,0));
            if (cmd_tokens.size() >= 7)
                pver = static_cast<persistent::version_t>(std::stol(cmd_tokens[6],nullptr,0));
            if (cmd_tokens.size() >= 8)
                pver_bk = static_cast<persistent::version_t>(std::stol(cmd_tokens[7],nullptr,0));
            on_subgroup_type(cmd_tokens[1],put_and_forget,capi,cmd_tokens[2]/*key*/,cmd_tokens[3]/*value*/,pver,pver_bk,subgroup_index,shard_index);
            return true;
        }
    },
    {
        "op_put",
        "Put an object into an object pool",
        "op_put <key> <value> [previous_version(default:-1)] [previous_version_by_key(default:-1)]\n"
        "Please note that cascade automatically decides the object pool path using the key's prefix.\n"
        "Note: put.[version,timestamp_us] will be set.",
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            persistent::version_t pver = persistent::INVALID_VERSION;
            persistent::version_t pver_bk = persistent::INVALID_VERSION;
            CHECK_FORMAT(cmd_tokens,3);
            if (cmd_tokens.size() >= 4)
                pver = static_cast<persistent::version_t>(std::stol(cmd_tokens[3],nullptr,0));
            if (cmd_tokens.size() >= 5)
                pver_bk = static_cast<persistent::version_t>(std::stol(cmd_tokens[4],nullptr,0));
            op_put(capi,cmd_tokens[1]/*key*/,cmd_tokens[2]/*value*/,pver,pver_bk);
            return true;
        }
    },
    {
        "op_put_file",
        "Put an object into an object pool, where object's value is from a file,",
        "op_put_file <key> <filename> [previous_version(default:-1)] [previous_version_by_key(default:-1)]\n"
        "Please note that cascade automatically decides the object pool path using the key's prefix.\n"
        "Note: put.[version,timestamp_us] will be set.",
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            persistent::version_t pver = persistent::INVALID_VERSION;
            persistent::version_t pver_bk = persistent::INVALID_VERSION;
            CHECK_FORMAT(cmd_tokens,3);
            if (cmd_tokens.size() >= 4)
                pver = static_cast<persistent::version_t>(std::stol(cmd_tokens[3],nullptr,0));
            if (cmd_tokens.size() >= 5)
                pver_bk = static_cast<persistent::version_t>(std::stol(cmd_tokens[4],nullptr,0));
            op_put_file(capi,cmd_tokens[1]/*key*/,cmd_tokens[2]/*filename*/,pver,pver_bk);
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
            CHECK_FORMAT(cmd_tokens,3);
            if (cmd_tokens.size() >= 4)
                pver = static_cast<persistent::version_t>(std::stol(cmd_tokens[3],nullptr,0));
            if (cmd_tokens.size() >= 5)
                pver_bk = static_cast<persistent::version_t>(std::stol(cmd_tokens[4],nullptr,0));
            op_put_and_forget(capi,cmd_tokens[1]/*key*/,cmd_tokens[2]/*value*/,pver,pver_bk);
            return true;
        }
    },
    {
        "op_put_file_and_forget",
        "Put an object into an object pool, where object's value is from a file,",
        "op_put_file_and_forget <key> <filename> [previous_version(default:-1)] [previous_version_by_key(default:-1)]\n"
            "Please note that cascade automatically decides the object pool path using the key's prefix.",
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            persistent::version_t pver = persistent::INVALID_VERSION;
            persistent::version_t pver_bk = persistent::INVALID_VERSION;
            CHECK_FORMAT(cmd_tokens,3);
            if (cmd_tokens.size() >= 4)
                pver = static_cast<persistent::version_t>(std::stol(cmd_tokens[3],nullptr,0));
            if (cmd_tokens.size() >= 5)
                pver_bk = static_cast<persistent::version_t>(std::stol(cmd_tokens[4],nullptr,0));
            op_put_file_and_forget(capi,cmd_tokens[1]/*key*/,cmd_tokens[2]/*filename*/,pver,pver_bk);
            return true;
        }
    },
    {
        "trigger_put",
        "Trigger put an object to a shard.",
        "trigger_put <type> <key> <value> <subgroup_index> <shard_index>\n"
            "type := " SUBGROUP_TYPE_LIST,
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            CHECK_FORMAT(cmd_tokens,6);
            uint32_t subgroup_index = static_cast<uint32_t>(std::stoi(cmd_tokens[4],nullptr,0));
            uint32_t shard_index = static_cast<uint32_t>(std::stoi(cmd_tokens[5],nullptr,0));
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
            CHECK_FORMAT(cmd_tokens,3);
            op_trigger_put(capi,cmd_tokens[1]/*key*/,cmd_tokens[2]/*value*/);
            return true;
        }
    },
    {
        "collective_trigger_put",
        "Collectively trigger put an object to a set of nodes in a subgroup.",
        "collective_trigger_put <type> <key> <value> <subgroup_index> <node id 1> [node id 2, ...] \n"
        "    type := " SUBGROUP_TYPE_LIST,
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            std::vector<node_id_t> nodes;
            CHECK_FORMAT(cmd_tokens,6);
            uint32_t subgroup_index = static_cast<uint32_t>(std::stoi(cmd_tokens[4],nullptr,0));
            size_t arg_idx = 5;
            while(arg_idx < cmd_tokens.size()) {
                nodes.push_back(static_cast<node_id_t>(std::stoi(cmd_tokens[arg_idx++],nullptr,0)));
            }
            on_subgroup_type(cmd_tokens[1],collective_trigger_put,capi,cmd_tokens[2]/*key*/,cmd_tokens[3]/*value*/,subgroup_index,nodes);
            return true;
        }
    },
    {
        "remove",
        "Remove an object from a shard.",
        "remove <type> <key> <subgroup_index> <shard_index> \n"
        "type := " SUBGROUP_TYPE_LIST "\n"
        "Note: variable put.[version,timestamp_us] will be set.",
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            CHECK_FORMAT(cmd_tokens,5);
            uint32_t subgroup_index = static_cast<uint32_t>(std::stoi(cmd_tokens[3],nullptr,0));
            uint32_t shard_index = static_cast<uint32_t>(std::stoi(cmd_tokens[4],nullptr,0));
            on_subgroup_type(cmd_tokens[1],remove,capi,cmd_tokens[2]/*key*/,subgroup_index,shard_index);
            return true;
        }
    },
    {
        "op_remove",
        "Remove an object from an object pool.",
        "op_remove <key>\n"
        "Please note that cascade automatically decides the object pool path using the key's prefix.\n"
        "Note: variable put.[version,timestamp_us] will be set.",
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            CHECK_FORMAT(cmd_tokens,2);
            op_remove(capi,cmd_tokens[1]);
            return true;
        }
    },
    {
        "get",
        "Get an object (by version).",
        "get <type> <key> <stable> <subgroup_index> <shard_index> [ version(default:current version) ]\n"
        "type := " SUBGROUP_TYPE_LIST "\n"
        "stable := 0|1  using stable data or not.\n"
        "Note: variable object.[version,timestamp_us,previous_version,previous_version_by_key] will be set.",
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            CHECK_FORMAT(cmd_tokens,6);
            bool stable = static_cast<bool>(std::stoi(cmd_tokens[3],nullptr,0));
            uint32_t subgroup_index = static_cast<uint32_t>(std::stoi(cmd_tokens[4],nullptr,0));
            uint32_t shard_index = static_cast<uint32_t>(std::stoi(cmd_tokens[5],nullptr,0));
            persistent::version_t version = CURRENT_VERSION;
            if (cmd_tokens.size() >= 7) {
                version = static_cast<persistent::version_t>(std::stol(cmd_tokens[6],nullptr,0));
            }
            on_subgroup_type(cmd_tokens[1],get,capi,cmd_tokens[2],version,stable,subgroup_index,shard_index);
            return true;
        }
    },
    {
        "op_get",
        "Get an object from an object pool (by version).",
        "op_get <key> <stable> [ version(default:current version) ]\n"
        "stable := 0|1  using stable data or not.\n"
        "Please note that cascade automatically decides the object pool path using the key's prefix.\n"
        "Note: variable object.[version,timestamp_us,previous_version,previous_version_by_key] will be set.",
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            CHECK_FORMAT(cmd_tokens,3);
            bool stable = static_cast<bool>(std::stoi(cmd_tokens[2],nullptr,0));
            persistent::version_t version = CURRENT_VERSION;
            if (cmd_tokens.size() >= 4) {
                version = static_cast<persistent::version_t>(std::stol(cmd_tokens[3],nullptr,0));
            }
            auto res = capi.get(cmd_tokens[1],version,stable);
            check_get_result(res);
            return true;
        }
    },
    {
        "op_get_file",
        "Get an object from an object pool (by version.) and save it to file.",
        "op_get_file <file> <key> <stable> [ version(default:current version) ]\n"
        "stable := 0|1  using stable data or not.\n"
        "Please note that cascade automatically decides the object pool path using the key's prefix.\n"
        "Note: variable object.[version,timestamp_us,previous_version,previous_version_by_key] will be set.",
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            CHECK_FORMAT(cmd_tokens,4);
            bool stable = static_cast<bool>(std::stoi(cmd_tokens[3],nullptr,0));
            persistent::version_t version = CURRENT_VERSION;
            if (cmd_tokens.size() >= 5) {
                version = static_cast<persistent::version_t>(std::stol(cmd_tokens[4],nullptr,0));
            }
            auto res = capi.get(cmd_tokens[2],version,stable);
            for (auto& reply_future:res.get()) {\
                auto reply = reply_future.second.get();\
                std::cout << "node(" << reply_future.first << ") replied with value:" << reply << std::endl;\
                // write blob to file
                std::ofstream of(cmd_tokens[1]);
                of.write(reinterpret_cast<const char*>(reply.blob.bytes),reply.blob.size);
                of.close();
                // set variables
                shell_vars["object.version"] =                  std::to_string(reply.version);\
                shell_vars["object.timestamp_us"] =             std::to_string(reply.timestamp_us);\
                shell_vars["object.previous_version"] =         std::to_string(reply.previous_version);\
                shell_vars["object.previous_version_by_key"] =  std::to_string(reply.previous_version_by_key);\
            }
            return true;
        }
    },
    {
        "get_by_time",
        "Get an object (by timestamp in microseconds).",
        "get_by_time <type> <key> <subgroup_index> <shard_index> <timestamp in us> <stable>\n"
        "type := " SUBGROUP_TYPE_LIST "\n"
        "stable := 0|1 using stable data or not\n"
        "Note: variable object.[version,timestamp_us,previous_version,previous_version_by_key] will be set.",
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            CHECK_FORMAT(cmd_tokens,7);
            uint32_t subgroup_index = static_cast<uint32_t>(std::stoi(cmd_tokens[3],nullptr,0));
            uint32_t shard_index = static_cast<uint32_t>(std::stoi(cmd_tokens[4],nullptr,0));
            uint64_t ts_us = static_cast<uint64_t>(std::stol(cmd_tokens[5],nullptr,0));
            bool stable = static_cast<bool>(std::stoi(cmd_tokens[6],nullptr,0));
            on_subgroup_type(cmd_tokens[1],get_by_time,capi,cmd_tokens[2],ts_us,stable,subgroup_index,shard_index);
            return true;
        }
    },
    {
        "op_get_by_time",
        "Get an object from an object pool (by timestamp in microseconds).",
        "op_get_by_time <key> <timestamp in us> <stable>\n"
        "stable := 0|1 using stable data or not\n"
        "Please note that cascade automatically decides the object pool path using the key's prefix.\n"
        "Note: variable object.[version,timestamp_us,previous_version,previous_version_by_key] will be set.",
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            CHECK_FORMAT(cmd_tokens,4);
            uint64_t ts_us = static_cast<uint64_t>(std::stol(cmd_tokens[2],nullptr,0));
            bool stable = static_cast<bool>(std::stoi(cmd_tokens[3],nullptr,0));
            auto res = capi.get_by_time(cmd_tokens[1],ts_us,stable);
            check_get_result(res);
            return true;
        }
    },
    {
        "multi_get",
        "Get an object, which will participate atomic broadcast for the latest value.",
        "multi_get <type> <key> <subgroup_index> <shard_index>\n"
        "type := " SUBGROUP_TYPE_LIST "\n"
        "Note: variable object.[version,timestamp_us,previous_version,previous_version_by_key] will be set.",
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            CHECK_FORMAT(cmd_tokens,5);
            uint32_t subgroup_index = static_cast<uint32_t>(std::stoi(cmd_tokens[3],nullptr,0));
            uint32_t shard_index = static_cast<uint32_t>(std::stoi(cmd_tokens[4],nullptr,0));
            on_subgroup_type(cmd_tokens[1],multi_get,capi,cmd_tokens[2],subgroup_index,shard_index);
            return true;
        }
    },
    {
        "op_multi_get",
        "Get an object, which will participate atomic broadcast for the latest value.",
        "op_multi_get <key>\n"
        "Note: variable object.[version,timestamp_us,previous_version,previous_version_by_key] will be set.",
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            CHECK_FORMAT(cmd_tokens,2);
            auto res = capi.multi_get(cmd_tokens[1]);
            check_get_result(res);
            return true;
        }
    },
    {
        "multi_get_size",
        "Get the size of an object, which will participate atomic broadcast for the latest size.",
        "multi_get_size <type> <key> <subgroup_index> <shard_index>\n"
        "type := " SUBGROUP_TYPE_LIST,
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            CHECK_FORMAT(cmd_tokens,5);
            uint32_t subgroup_index = static_cast<uint32_t>(std::stoi(cmd_tokens[3],nullptr,0));
            uint32_t shard_index = static_cast<uint32_t>(std::stoi(cmd_tokens[4],nullptr,0));
            on_subgroup_type(cmd_tokens[1],multi_get_size,capi,cmd_tokens[2],subgroup_index,shard_index);
            return true;
        }
    },
    {
        "op_multi_get_size",
        "Get the size of an object, which will participate atomic broadcast for the latest size.",
        "op_multi_get_size <key>\n"
            "type := " SUBGROUP_TYPE_LIST,
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            CHECK_FORMAT(cmd_tokens,2);
            auto res = capi.multi_get_size(cmd_tokens[1]);
            check_get_result(res);
            return true;
        }
    },
    {
        "get_size",
        "Get the size of an object (by version).",
        "get_size <type> <key> <stable> <subgroup_index> <shard_index> [ version(default:current version) ]\n"
            "type := " SUBGROUP_TYPE_LIST,
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            CHECK_FORMAT(cmd_tokens,6);
            bool stable = static_cast<bool>(std::stoi(cmd_tokens[3],nullptr,0));
            uint32_t subgroup_index = static_cast<uint32_t>(std::stoi(cmd_tokens[4],nullptr,0));
            uint32_t shard_index = static_cast<uint32_t>(std::stoi(cmd_tokens[5],nullptr,0));
            persistent::version_t version = CURRENT_VERSION;
            if (cmd_tokens.size() >= 7) {
                version = static_cast<persistent::version_t>(std::stol(cmd_tokens[6],nullptr,0));
            }
            on_subgroup_type(cmd_tokens[1],get_size,capi,cmd_tokens[2],version,stable,subgroup_index,shard_index);
            return true;
        }
    },
    {
        "op_get_size",
        "Get the size of an object from an object pool (by version).",
        "op_get_size <key> <stable> [ version(default:current version) ]\n"
            "Please note that cascade automatically decides the object pool path using the key's prefix.",
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            CHECK_FORMAT(cmd_tokens,3);
            persistent::version_t version = CURRENT_VERSION;
            bool stable = static_cast<bool>(std::stoi(cmd_tokens[2],nullptr,0));
            if (cmd_tokens.size() >= 4) {
                version = static_cast<persistent::version_t>(std::stol(cmd_tokens[3],nullptr,0));
            }
            auto res = capi.get_size(cmd_tokens[1],version,stable);
            check_get_result(res);
            return true;
        }
    },
    {
        "get_size_by_time",
        "Get the size of an object (by timestamp in microseconds).",
        "get_size_by_time <type> <key> <subgroup_index> <shard_index> <timestamp in us> <stable>\n"
            "type := " SUBGROUP_TYPE_LIST,
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            CHECK_FORMAT(cmd_tokens,7);
            uint32_t subgroup_index = static_cast<uint32_t>(std::stoi(cmd_tokens[3],nullptr,0));
            uint32_t shard_index = static_cast<uint32_t>(std::stoi(cmd_tokens[4],nullptr,0));
            uint64_t ts_us = static_cast<uint64_t>(std::stol(cmd_tokens[5],nullptr,0));
            bool stable = static_cast<bool>(std::stoi(cmd_tokens[6],nullptr,0));
            on_subgroup_type(cmd_tokens[1],get_size_by_time,capi,cmd_tokens[2],ts_us,stable,subgroup_index,shard_index);
            return true;
        }
    },
    {
        "op_get_size_by_time",
        "Get the size of an object from an object pool (by timestamp in microseconds).",
        "op_get_size_by_time <key> <timestamp in us> <stable>\n"
            "Please note that cascade automatically decides the object pool path using the key's prefix.",
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            CHECK_FORMAT(cmd_tokens,4);
            uint64_t ts_us = static_cast<uint64_t>(std::stol(cmd_tokens[2],nullptr,0));
            bool stable = static_cast<bool>(std::stoi(cmd_tokens[3],nullptr,0));
            auto res = capi.get_size_by_time(cmd_tokens[1],ts_us,stable);
            check_get_result(res);
            return true;
        }
    },
    {
        "multi_list_keys",
        "list the object keys in a shard using atomic broadcast for the latest version.",
        "multi_list_keys <type> <subgroup_index> <shard_index> \n"
            "type := " SUBGROUP_TYPE_LIST,
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            CHECK_FORMAT(cmd_tokens,4);
            uint32_t subgroup_index = static_cast<uint32_t>(std::stoi(cmd_tokens[2],nullptr,0));
            uint32_t shard_index = static_cast<uint32_t>(std::stoi(cmd_tokens[3],nullptr,0));
            on_subgroup_type(cmd_tokens[1],multi_list_keys,capi,subgroup_index,shard_index);
            return true;
        }
    },
    {
        "op_multi_list_keys",
        "list the object keys in a shard using atomic broadcast for the latest version.",
        "op_multi_list_keys <object pool pathname>\n"
            "type := " SUBGROUP_TYPE_LIST,
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            CHECK_FORMAT(cmd_tokens,2);
            auto result = capi.multi_list_keys(cmd_tokens[1]);
            check_op_list_keys_result(capi.wait_list_keys(result));
            return true;
        }
    },
    {
        "list_keys",
        "list the object keys in a shard (by version).",
        "list_keys <type> <stable> <subgroup_index> <shard_index> [ version(default:current version) ]\n"
            "type := " SUBGROUP_TYPE_LIST,
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            CHECK_FORMAT(cmd_tokens,5);
            bool stable = static_cast<bool>(std::stoi(cmd_tokens[2],nullptr,0));
            uint32_t subgroup_index = static_cast<uint32_t>(std::stoi(cmd_tokens[3],nullptr,0));
            uint32_t shard_index = static_cast<uint32_t>(std::stoi(cmd_tokens[4],nullptr,0));
            persistent::version_t version = CURRENT_VERSION;
            if (cmd_tokens.size() >= 6) {
                version = static_cast<persistent::version_t>(std::stol(cmd_tokens[5],nullptr,0));
            }
            on_subgroup_type(cmd_tokens[1],list_keys,capi,version,stable,subgroup_index,shard_index);
            return true;
        }
    },
    {
        "op_list_keys",
        "list the object keys in an object pool (by version).",
        "op_list_keys <object pool pathname> <stable> [ version(default:current version) ]\n",
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            CHECK_FORMAT(cmd_tokens,3);
            bool stable = static_cast<bool>(std::stoi(cmd_tokens[2],nullptr,0));
            persistent::version_t version = CURRENT_VERSION;
            if (cmd_tokens.size() >= 4) {
                version = static_cast<persistent::version_t>(std::stol(cmd_tokens[3],nullptr,0));
            }
            auto result = capi.list_keys(version,stable,cmd_tokens[1]);
            check_op_list_keys_result(capi.wait_list_keys(result));
            return true;
        }
    },
    {
        "list_keys_by_time",
        "list the object keys in a shard (by timestamp in mircoseconds).",
        "list_keys_by_time <type> <subgroup_index> <shard_index> <timestamp in us> <stable>\n"
            "type := " SUBGROUP_TYPE_LIST,
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            CHECK_FORMAT(cmd_tokens,6);
            uint32_t subgroup_index = static_cast<uint32_t>(std::stoi(cmd_tokens[2],nullptr,0));
            uint32_t shard_index = static_cast<uint32_t>(std::stoi(cmd_tokens[3],nullptr,0));
            uint64_t ts_us = static_cast<uint64_t>(std::stoull(cmd_tokens[4],nullptr,0));
            bool stable = static_cast<bool>(std::stoi(cmd_tokens[5],nullptr,0));
            on_subgroup_type(cmd_tokens[1],list_keys_by_time,capi,ts_us,stable,subgroup_index,shard_index);
            return true;
        }
    },
    {
        "op_list_keys_by_time",
        "list the object keys in an object pool (by timestamp in microseconds).",
        "op_list_keys_by_time <object pool pathname> <timestamp in us> <stable>\n",
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            CHECK_FORMAT(cmd_tokens,4);
            uint64_t ts_us = static_cast<uint64_t>(std::stoull(cmd_tokens[2],nullptr,0));
            bool stable = static_cast<bool>(std::stoi(cmd_tokens[3],nullptr,0));
            auto result = capi.list_keys_by_time(ts_us,stable,cmd_tokens[1]);
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
            CHECK_FORMAT(cmd_tokens,5);
            const std::string& prefix = cmd_tokens[2];
            uint32_t subgroup_index = static_cast<uint32_t>(std::stoi(cmd_tokens[3],nullptr,0));
            uint32_t shard_index = static_cast<uint32_t>(std::stoi(cmd_tokens[4],nullptr,0));
            persistent::version_t version = CURRENT_VERSION;
            if (cmd_tokens.size() >= 6) {
                version = static_cast<persistent::version_t>(std::stol(cmd_tokens[5],nullptr,0));
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
            CHECK_FORMAT(cmd_tokens,5);
            uint32_t subgroup_index = static_cast<uint32_t>(std::stoi(cmd_tokens[3],nullptr,0));
            uint32_t shard_index = static_cast<uint32_t>(std::stoi(cmd_tokens[4],nullptr,0));

            persistent::version_t version_start = INVALID_VERSION;
            persistent::version_t version_end = INVALID_VERSION;
            if (cmd_tokens.size() >= 6) {
                version_start = static_cast<persistent::version_t>(std::stol(cmd_tokens[5],nullptr,0));
            }
            if (cmd_tokens.size() >= 7) {
                version_end = static_cast<persistent::version_t>(std::stol(cmd_tokens[6],nullptr,0));
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
            CHECK_FORMAT(cmd_tokens,5);
            uint32_t subgroup_index = static_cast<uint32_t>(std::stoi(cmd_tokens[3],nullptr,0));
            uint32_t shard_index = static_cast<uint32_t>(std::stoi(cmd_tokens[4],nullptr,0));

            uint64_t start = 0;
            uint64_t end = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
            if (cmd_tokens.size() >= 6) {
                start = static_cast<uint64_t>(std::stol(cmd_tokens[5],nullptr,0));
            }
            if (cmd_tokens.size() >= 7) {
                end = static_cast<uint64_t>(std::stol(cmd_tokens[6],nullptr,0));
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
            CHECK_FORMAT(cmd_tokens,3);
            uint32_t subgroup_index = static_cast<uint32_t>(std::stoi(cmd_tokens[2],nullptr,0));

            persistent::version_t version = INVALID_VERSION;
            if (cmd_tokens.size() >= 4) {
                version = static_cast<persistent::version_t>(std::stol(cmd_tokens[3],nullptr,0));
            }
            on_subgroup_type(cmd_tokens[1], list_data_in_subgroup, capi, subgroup_index, version);
            return true;
        }
    },
#endif// HAS_BOOLINQ
    {
        "Notification Test Commands","","",command_handler_t()
    },
    {
        "op_register_notification",
        "Register a notification to an object pool",
        "op_reigster_notification <object_pool_pathname>",
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            CHECK_FORMAT(cmd_tokens, 2);
            bool ret = capi.register_notification_handler(
                    [](const Blob& msg)->void{
                        std::cout << "Object Pool Notification received:"
                                  << "data:" << std::string(reinterpret_cast<const char*>(msg.bytes),msg.size)
                                  << std::endl;
                    },
                    cmd_tokens[1]);
            std::cout << "Notification Registered to object pool:" << cmd_tokens[1]
                      << ". Old handler replaced? " << ret << std::endl;
            return true;
        }
    },
    {
        "op_unregister_notification",
        "Unregister a notification from an object pool",
        "op_unreigster_notification <object_pool_pathname>",
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            CHECK_FORMAT(cmd_tokens, 2);
            bool ret = capi.register_notification_handler({},
                    cmd_tokens[1]);
            std::cout << "Notification Unregistered from object pool:" << cmd_tokens[1]
                      << ". Old handler replaced? " << ret << std::endl;
            return true;
        }
    },
    {
        "register_notification",
        "Register a notification handler to a subgroup",
        "register_notification <type> <subgroup_index> \n"
            "type := " SUBGROUP_TYPE_LIST,
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            CHECK_FORMAT(cmd_tokens,3);
            uint32_t subgroup_index = static_cast<uint32_t>(std::stoi(cmd_tokens[2],nullptr,0));

            bool ret = false;
            on_subgroup_type(cmd_tokens[1], ret = register_notification, capi, subgroup_index);

            std::cout << "Notification Registered to Subgroup " << cmd_tokens[1] << ":" << subgroup_index
                      << ". Old handler replaced?" << ret << std::endl;
            return true;
        }
    },
    {
        "unregister_notification",
        "Unregister a notification handler from a subgroup",
        "unregister_notification <type> <subgroup_index> \n"
            "type := " SUBGROUP_TYPE_LIST,
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            CHECK_FORMAT(cmd_tokens,3);
            uint32_t subgroup_index = static_cast<uint32_t>(std::stoi(cmd_tokens[2],nullptr,0));

            bool ret = false;
            on_subgroup_type(cmd_tokens[1], ret = unregister_notification, capi, subgroup_index);

            std::cout << "Notification Registered to Subgroup " << cmd_tokens[1] << ":" << subgroup_index
                      << ". Old handler replaced?" << ret << std::endl;
            return true;
        }
    },
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
            CHECK_FORMAT(cmd_tokens,9);

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
            uint64_t max_rate = std::stoul(cmd_tokens[6],nullptr,0);
            uint64_t duration_sec = std::stoul(cmd_tokens[7],nullptr,0);

            PerfTestClient ptc{capi};
            uint32_t pos = 8;
            while (pos < cmd_tokens.size()) {
                std::string::size_type colon_pos = cmd_tokens[pos].find(':');
                if (colon_pos == std::string::npos) {
                    ptc.add_or_update_server(cmd_tokens[pos],PERFTEST_PORT);
                } else {
                    ptc.add_or_update_server(cmd_tokens[pos].substr(0,colon_pos),
                                             static_cast<uint16_t>(std::stoul(cmd_tokens[pos].substr(colon_pos+1),nullptr,0)));
                }
                pos ++;
            }
            bool ret = false;
            on_subgroup_type(cmd_tokens[1], ret = perftest, ptc, put_type, object_pool_pathname, member_selection_policy,read_write_ratio,max_rate,duration_sec,"timestamp.log");
            return ret;
        }
    },
    {
        "perftest_op_get",
        "Performance tester for get from an object pool.",
        "perftest_op_get <type> <object pool pathname> <member selection policy> <log depth> <max rate> <duration> <client1> \n"
            "type := " SUBGROUP_TYPE_LIST "\n"
            "'member selection policy' refers how the external clients pick a member in a shard;\n"
            "    Available options: FIXED|RANDOM|ROUNDROBIN;\n"
            "'log depth' is the number of versions prior to the current version each get should request, 0 means to request the current version \n"
            "'max rate' is the maximum number of operations in Operations per Second, 0 for best effort; \n"
            "'duration' is the span of the whole experiment in seconds; \n"
            "'client1' is a host[:port] pair representing the client. Currently only one client is supported. The port defaults to " + std::to_string(PERFTEST_PORT),
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens){
            CHECK_FORMAT(cmd_tokens, 9);
            std::string object_pool_pathname = cmd_tokens[2];
            ExternalClientToCascadeServerMapping member_selection_policy = FIXED;
            if (cmd_tokens[3] == "RANDOM") {
                member_selection_policy = ExternalClientToCascadeServerMapping::RANDOM;
            } else if (cmd_tokens[3] == "ROUNDROBIN") {
                member_selection_policy = ExternalClientToCascadeServerMapping::ROUNDROBIN;
            }
            int32_t log_depth = std::stoi(cmd_tokens[4], nullptr, 0);
            uint64_t max_rate = std::stoul(cmd_tokens[5], nullptr, 0);
            uint64_t duration_sec = std::stoul(cmd_tokens[6], nullptr, 0);

            PerfTestClient ptc{capi};
            uint32_t pos = 7;
            while (pos < cmd_tokens.size()) {
                std::string::size_type colon_pos = cmd_tokens[pos].find(':');
                if (colon_pos == std::string::npos) {
                    ptc.add_or_update_server(cmd_tokens[pos], PERFTEST_PORT);
                } else {
                    ptc.add_or_update_server(cmd_tokens[pos].substr(0, colon_pos),
                                             static_cast<uint16_t>(std::stoul(cmd_tokens[pos].substr(colon_pos+1),nullptr,0)));
                }
                pos++;
            }
            bool ret = false;
            on_subgroup_type(cmd_tokens[1], ret = perftest_get, ptc, object_pool_pathname, member_selection_policy, log_depth, max_rate, duration_sec, "timestamp.log");
            return ret;
        }
    },
    {
        "perftest_op_get_by_time",
        "Performance tester for get_by_time from an object pool.",
        "perftest_op_get <type> <object pool pathname> <member selection policy> <time in past> <max rate> <duration> <client1> \n"
            "type: must be PCSS because get_by_time is not supported for any other subgroup type \n"
            "'member selection policy' refers how the external clients pick a member in a shard;\n"
            "    Available options: FIXED|RANDOM|ROUNDROBIN;\n"
            "'time in past' is the number of milliseconds prior to the start of the experiment that each get_by_time should request \n"
            "'max rate' is the maximum number of operations in Operations per Second, 0 for best effort; \n"
            "'duration' is the span of the whole experiment in seconds; \n"
            "'client1' is a host[:port] pair representing the client. Currently only one client is supported. The port defaults to " + std::to_string(PERFTEST_PORT),
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens){
            CHECK_FORMAT(cmd_tokens, 9);
            if(cmd_tokens[1] != "PCSS") {
                print_red("Invalid subgroup type. Only Persistent Cascade Store supports get_by_time.");
                return false;
            }

            std::string object_pool_pathname = cmd_tokens[2];
            ExternalClientToCascadeServerMapping member_selection_policy = FIXED;
            if (cmd_tokens[3] == "RANDOM") {
                member_selection_policy = ExternalClientToCascadeServerMapping::RANDOM;
            } else if (cmd_tokens[3] == "ROUNDROBIN") {
                member_selection_policy = ExternalClientToCascadeServerMapping::ROUNDROBIN;
            }
            uint64_t ms_in_past = std::stoul(cmd_tokens[4], nullptr, 0);
            uint64_t max_rate = std::stoul(cmd_tokens[5], nullptr, 0);
            uint64_t duration_sec = std::stoul(cmd_tokens[6], nullptr, 0);

            PerfTestClient ptc{capi};
            uint32_t pos = 7;
            while (pos < cmd_tokens.size()) {
                std::string::size_type colon_pos = cmd_tokens[pos].find(':');
                if (colon_pos == std::string::npos) {
                    ptc.add_or_update_server(cmd_tokens[pos], PERFTEST_PORT);
                } else {
                    ptc.add_or_update_server(cmd_tokens[pos].substr(0, colon_pos),
                                             static_cast<uint16_t>(std::stoul(cmd_tokens[pos].substr(colon_pos+1),nullptr,0)));
                }
                pos++;
            }
            bool ret = false;
            ret = perftest_get_by_time(ptc, object_pool_pathname, member_selection_policy, ms_in_past, max_rate, duration_sec, "timestamp.log");
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
            CHECK_FORMAT(cmd_tokens,10);

            PutType put_type = PutType::PUT;

            if (cmd_tokens[2] == "put_and_forget") {
                put_type = PutType::PUT_AND_FORGET;
            } else if (cmd_tokens[2] == "trigger_put") {
                put_type = PutType::TRIGGER_PUT;
            }

            uint32_t subgroup_index = std::stoul(cmd_tokens[3],nullptr,0);
            uint32_t shard_index = std::stoul(cmd_tokens[4],nullptr,0);

            ExternalClientToCascadeServerMapping member_selection_policy = FIXED;
            if (cmd_tokens[5] == "RANDOM") {
                member_selection_policy = ExternalClientToCascadeServerMapping::RANDOM;
            } else if (cmd_tokens[5] == "ROUNDROBIN") {
                member_selection_policy = ExternalClientToCascadeServerMapping::ROUNDROBIN;
            }
            double read_write_ratio = std::stod(cmd_tokens[6]);
            uint64_t max_rate = std::stoul(cmd_tokens[7],nullptr,0);
            uint64_t duration_sec = std::stoul(cmd_tokens[8],nullptr,0);

            PerfTestClient ptc{capi};
            uint32_t pos = 9;
            while (pos < cmd_tokens.size()) {
                std::string::size_type colon_pos = cmd_tokens[pos].find(':');
                if (colon_pos == std::string::npos) {
                    ptc.add_or_update_server(cmd_tokens[pos],PERFTEST_PORT);
                } else {
                    ptc.add_or_update_server(cmd_tokens[pos].substr(0,colon_pos),
                                             static_cast<uint16_t>(std::stoul(cmd_tokens[pos].substr(colon_pos+1),nullptr,0)));
                }
                pos ++;
            }
            bool ret = false;
            on_subgroup_type(cmd_tokens[1], ret = perftest,ptc,put_type,subgroup_index,shard_index,member_selection_policy,read_write_ratio,max_rate,duration_sec,"output.log");
            return ret;
        }
    },
    {
        "perftest_shard_get",
        "Performance tester for get from a shard.",
        "perfest_shard_get <type> <subgroup index> <shard index> <member selection policy> <log depth> <max rate> <duration> <client1>"
            "type := " SUBGROUP_TYPE_LIST "\n"
            "'member selection policy' refers how the external clients pick a member in a shard;\n"
            "    Available options: FIXED|RANDOM|ROUNDROBIN;\n"
            "'log depth' is the number of versions prior to the current version each get should request, 0 means to request the current version \n"
            "'max rate' is the maximum number of operations in Operations per Second, 0 for best effort; \n"
            "'duration' is the span of the whole experiment in seconds; \n"
            "'client1' is a host[:port] pair representing the client. Currently only one client is supported. The port defaults to " + std::to_string(PERFTEST_PORT),
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            CHECK_FORMAT(cmd_tokens, 9);

            uint32_t subgroup_index = std::stoul(cmd_tokens[2], nullptr, 0);
            uint32_t shard_index = std::stoul(cmd_tokens[3], nullptr, 0);

            ExternalClientToCascadeServerMapping member_selection_policy = FIXED;
            if (cmd_tokens[4] == "RANDOM") {
                member_selection_policy = ExternalClientToCascadeServerMapping::RANDOM;
            } else if (cmd_tokens[4] == "ROUNDROBIN") {
                member_selection_policy = ExternalClientToCascadeServerMapping::ROUNDROBIN;
            }
            int32_t log_depth = std::stoi(cmd_tokens[5], nullptr, 0);
            uint64_t max_rate = std::stoul(cmd_tokens[6], nullptr, 0);
            uint64_t duration_sec = std::stoul(cmd_tokens[7], nullptr, 0);

            PerfTestClient ptc{capi};
            uint32_t pos = 8;
            while (pos < cmd_tokens.size()) {
                std::string::size_type colon_pos = cmd_tokens[pos].find(':');
                if (colon_pos == std::string::npos) {
                    ptc.add_or_update_server(cmd_tokens[pos], PERFTEST_PORT);
                } else {
                    ptc.add_or_update_server(cmd_tokens[pos].substr(0, colon_pos),
                                             static_cast<uint16_t>(std::stoul(cmd_tokens[pos].substr(colon_pos+1),nullptr,0)));
                }
                pos++;
            }
            bool ret = false;
            on_subgroup_type(cmd_tokens[1], ret = perftest_get, ptc, subgroup_index, shard_index, member_selection_policy, log_depth, max_rate, duration_sec, "timestamp.log");
            return ret;
        }
    },
        {
        "perftest_shard_get_by_time",
        "Performance tester for get_by_time from a shard.",
        "perfest_shard_get <type> <subgroup index> <shard index> <member selection policy> <time in past> <max rate> <duration> <client1>"
            "type: must be PCSS because get_by_time is not supported for any other subgroup type \n"
            "'member selection policy' refers how the external clients pick a member in a shard;\n"
            "    Available options: FIXED|RANDOM|ROUNDROBIN;\n"
            "'time in past' is the number of milliseconds prior to the start of the experiment that each get_by_time should request \n"
            "'max rate' is the maximum number of operations in Operations per Second, 0 for best effort; \n"
            "'duration' is the span of the whole experiment in seconds; \n"
            "'client1' is a host[:port] pair representing the client. Currently only one client is supported. The port defaults to " + std::to_string(PERFTEST_PORT),
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            CHECK_FORMAT(cmd_tokens, 9);
            if(cmd_tokens[1] != "PCSS") {
                print_red("Invalid subgroup type. Only Persistent Cascade Store supports get_by_time.");
                return false;
            }

            uint32_t subgroup_index = std::stoul(cmd_tokens[2], nullptr, 0);
            uint32_t shard_index = std::stoul(cmd_tokens[3], nullptr, 0);

            ExternalClientToCascadeServerMapping member_selection_policy = FIXED;
            if (cmd_tokens[4] == "RANDOM") {
                member_selection_policy = ExternalClientToCascadeServerMapping::RANDOM;
            } else if (cmd_tokens[4] == "ROUNDROBIN") {
                member_selection_policy = ExternalClientToCascadeServerMapping::ROUNDROBIN;
            }
            uint64_t ms_in_past = std::stoul(cmd_tokens[5], nullptr, 0);
            uint64_t max_rate = std::stoul(cmd_tokens[6], nullptr, 0);
            uint64_t duration_sec = std::stoul(cmd_tokens[7], nullptr, 0);

            PerfTestClient ptc{capi};
            uint32_t pos = 8;
            while (pos < cmd_tokens.size()) {
                std::string::size_type colon_pos = cmd_tokens[pos].find(':');
                if (colon_pos == std::string::npos) {
                    ptc.add_or_update_server(cmd_tokens[pos], PERFTEST_PORT);
                } else {
                    ptc.add_or_update_server(cmd_tokens[pos].substr(0, colon_pos),
                                             static_cast<uint16_t>(std::stoul(cmd_tokens[pos].substr(colon_pos+1),nullptr,0)));
                }
                pos++;
            }
            bool ret = false;
            ret = perftest_get_by_time(ptc, subgroup_index, shard_index, member_selection_policy, ms_in_past, max_rate, duration_sec, "timestamp.log");
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
            CHECK_FORMAT(cmd_tokens,6);
            uint32_t message_size = std::stoul(cmd_tokens[2],nullptr,0);
            uint64_t duration_sec = std::stoul(cmd_tokens[3],nullptr,0);
            uint32_t subgroup_index = std::stoul(cmd_tokens[4],nullptr,0);
            uint32_t shard_index = std::stoul(cmd_tokens[5],nullptr,0);

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
            CHECK_FORMAT(cmd_tokens,5);
            uint32_t subgroup_index = std::stoul(cmd_tokens[2],nullptr,0);
            uint32_t shard_index = std::stoul(cmd_tokens[3],nullptr,0);
            on_subgroup_type(cmd_tokens[1]/*subgroup type*/, dump_timestamp, capi, subgroup_index, shard_index, cmd_tokens[4]/*filename*/);
            return true;
        }
    },
    {
        "op_dump_timestamp",
        "Dump timestamps for a given object pool. Each node will write its timestamps to the given file.",
        "op_dump_timestamp <object_pool> <filename>\n"
            "filename := timestamp log filename",
        [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
            CHECK_FORMAT(cmd_tokens,3);
            capi.dump_timestamp(cmd_tokens[2],cmd_tokens[1]);
            TimestampLogger::flush(cmd_tokens[2]);
            return true;
        }
    },
#endif
};

inline bool do_command(ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
    bool ret = false;

    std::vector<std::string> new_tokens{};

    for(const auto& token: cmd_tokens) {
        new_tokens.emplace_back(expand_variables(token));
    }

    try {
        ssize_t command_index = find_command(commands, new_tokens[0]);
        if (command_index>=0) {
            if (commands.at(command_index).handler(capi,new_tokens)) {
                std::cout << "-> Succeeded." << std::endl;
                ret = true;
            } else {
                std::cout << "-> Failed." << std::endl;
            }
        } else {
            print_red("unknown command:" + new_tokens[0]);
        }
    } catch (const derecho::derecho_exception &ex) {
        print_red (std::string("Exception:") + ex.what());
    } catch (...) {
        print_red ("Unknown exception caught.");
    }
    return ret;
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

bool detached_test(ServiceClientAPI& capi, int argc, char** argv) {
    std::vector<std::string> cmd_tokens;
    for(int i=1;i<argc;i++) {
        cmd_tokens.emplace_back(argv[i]);
    }
    return do_command(capi, cmd_tokens);
}

int main(int argc,char** argv) {
    if( prctl(PR_SET_NAME, PROC_NAME, 0, 0, 0) != 0 ) {
        dbg_default_debug("Failed to set proc name to {}.",PROC_NAME);
    }
    auto& capi = ServiceClientAPI::get_service_client();
#ifdef ENABLE_EVALUATION
    // start working thread.
    PerfTestServer pts(capi);
#endif
    if (argc == 1) {
        // by default, we use the interactive shell.
        interactive_test(capi);
    } else {
        if (!detached_test(capi,argc,argv))
            return -1;
    }
    return 0;
}
