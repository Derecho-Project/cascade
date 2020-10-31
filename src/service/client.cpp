#include <cascade/service_client_api.hpp>
#include <iostream>
#include <string>
#include <fstream>
#include <typeindex>
#include <stdio.h>
#include <readline/readline.h>
#include <readline/history.h>

using namespace derecho::cascade;

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

void print_shard_member(ServiceClientAPI& capi, derecho::subgroup_id_t subgroup_id, uint32_t shard_index) {
    std::cout << "subgroup_id=" << subgroup_id << ","
              << "shard_index=" << shard_index << " member list = [";
    auto members = capi.get_shard_members(subgroup_id,shard_index);
    for (auto nid : members) {
        std::cout << nid << ",";
    }
    std::cout << "]" << std::endl;
}

static const char* policy_names[] = {
    "FirstMember",
    "LastMember",
    "Random",
    "FixedRandom",
    "RoundRobin",
    "UserSpecified",
    nullptr
};

inline ShardMemberSelectionPolicy parse_policy_name(std::string& policy_name) {
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
    print_shard_member<VCSU>(capi,0,0);
    print_shard_member<VCSS>(capi,0,0);
    print_shard_member<PCSU>(capi,0,0);
    print_shard_member<PCSS>(capi,0,0);
    print_shard_member(capi,0,0);
    print_shard_member(capi,1,0);
    print_shard_member(capi,2,0);
    print_shard_member(capi,3,0);
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

#define on_subgroup_type(x, ft, ...) \
    if ((x) == "VCSU") { \
        ft <VCSU>(__VA_ARGS__); \
    } else if ((x) == "VCSS") { \
        ft <VCSS>(__VA_ARGS__); \
    } else if ((x) == "PCSU") { \
        ft <PCSU>(__VA_ARGS__); \
    } else if ((x) == "PCSS") { \
        ft <PCSS>(__VA_ARGS__); \
    } else { \
        print_red("unknown subgroup type:" + cmd_tokens[1]); \
    }

#define check_put_and_remove_result(result) \
    for (auto& reply_future:result.get()) {\
        auto reply = reply_future.second.get();\
        std::cout << "node(" << reply_future.first << ") replied with version:" << std::get<0>(reply)\
                  << ",ts_us:" << std::get<1>(reply) << std::endl;\
    }

template <typename SubgroupType>
void put(ServiceClientAPI& capi, std::string& key, std::string& value, persistent::version_t pver, persistent::version_t pver_bk, uint32_t subgroup_index, uint32_t shard_index) {
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
void remove(ServiceClientAPI& capi, std::string& key, uint32_t subgroup_index, uint32_t shard_index) {
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

#define check_get_result(result) \
    for (auto& reply_future:result.get()) {\
        auto reply = reply_future.second.get();\
        std::cout << "node(" << reply_future.first << ") replied with value:" << reply << std::endl;\
    }

template <typename SubgroupType>
void get(ServiceClientAPI& capi, std::string& key, persistent::version_t ver, uint32_t subgroup_index,uint32_t shard_index) {
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
void get_by_time(ServiceClientAPI& capi, std::string& key, uint64_t ts_us, uint32_t subgroup_index, uint32_t shard_index) {
    if constexpr (std::is_same<typename SubgroupType::KeyType,uint64_t>::value) {
        derecho::rpc::QueryResults<const typename SubgroupType::ObjectType> result = capi.template get_by_time<SubgroupType>(
                static_cast<uint64_t>(std::stol(key)),ts_us,subgroup_index,shard_index);
        check_get_result(result);
    } else if constexpr (std::is_same<typename SubgroupType::KeyType,std::string>::value) {
        derecho::rpc::QueryResults<const typename SubgroupType::ObjectType> result = capi.template get<SubgroupType>(
                key,ts_us,subgroup_index,shard_index);
        check_get_result(result);
    }
}

template <typename SubgroupType>
void get_size(ServiceClientAPI& capi, std::string& key, persistent::version_t ver, uint32_t subgroup_index,uint32_t shard_index) {
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
void get_size_by_time(ServiceClientAPI& capi, std::string& key, uint64_t ts_us, uint32_t subgroup_index, uint32_t shard_index) {
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

#if HAS_BOOLINQ
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

//    "list_data_between_version <type> <key> <subgroup_index> <shard_index> [version_begin] [version_end]\n\t test LINQ api - version_iterator \n"
template <typename SubgroupType>
void list_data_between_version(ServiceClientAPI &capi, std::string &key, uint32_t subgroup_index, uint32_t shard_index, persistent::version_t ver_begin, persistent::version_t ver_end) {
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

//    "list_data_of_key_between_timestamp <type> <key> [ts_begin] [ts_end] [subgroup_index] [shard_index]\n\t test LINQ api - time_iterator \n"
template <typename SubgroupType>
void list_data_of_key_between_timestamp(ServiceClientAPI &capi, std::string &key, uint64_t ts_begin, uint64_t ts_end, uint32_t subgroup_index, uint32_t shard_index) {
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
#endif// HAS_BOOLINQ

/* TEST2: put/get/remove tests */
void interactive_test(ServiceClientAPI& capi) {
    const char* help_info =
    "list_all_members\n\tlist all members in top level derecho group.\n"
    "list_type_members <type> [subgroup_index(0)] [shard_index(0)]\n\tlist members in shard by subgroup type.\n"
    "list_subgroup_members [subgroup_id(0)] [shard_index(0)]\n\tlist members in shard by subgroup id.\n"
    "set_member_selection_policy <type> <subgroup_index> <shard_index> <policy> [user_specified_node_id]\n\tset member selection policy\n"
    "get_member_selection_policy <type> [subgroup_index(0)] [shard_index(0)]\n\tget member selection policy\n"
    "put <type> <key> <value> [pver(-1)] [pver_by_key(-1)] [subgroup_index(0)] [shard_index(0)]\n\tput an object\n"
    "remove <type> <key> [subgroup_index(0)] [shard_index(0)]\n\tremove an object\n"
    "get <type> <key> [version(-1)] [subgroup_index(0)] [shard_index(0)]\n\tget an object(by version)\n"
    "get_by_time <type> <key> <ts_us> [subgroup_index(0)] [shard_index(0)]\n\tget an object by timestamp\n"
    "get_size <type> <key> [version(-1)] [subgroup_index(0)] [shard_index(0)]\n\tget the size of an object(by version)\n"
    "get_size_by_time <type> <key> <ts_us> [subgroup_index(0)] [shard_index(0)]\n\tget the size of an object by timestamp\n"
    "list_keys <type> [version(-1)] [subgroup_index(0)] [shard_index(0)]\n\tlist keys in shard (by version)\n"
    "list_keys_by_time <type> <ts_us> [subgroup_index(0)] [shard_index(0)]\n\tlist keys in shard by time\n"
#if HAS_BOOLINQ
    "list_data_by_prefix <type> <prefix> [version(-1)] [subgroup_index(0)] [shard_index(0)]\n\t test LINQ api\n"
    "list_data_between_version <type> <key> <subgroup_index> <shard_index> [version_begin(MIN)] [version_end(MAX)]\n\t test LINQ api - version_iterator \n"
    "list_data_of_key_between_timestamp <type> <key> [ts_begin(MIN)] [ts_end(MAX)] [subgroup_index(0)] [shard_index(0)]\n\t test LINQ api - time_iterator \n"
    "list_data_in_subgroup <type> <subgroup_index> [version(-1)]\n\t test LINQ api - subgroup_iterator \n"
#endif// HAS_BOOLINQ
    "quit|exit\n\texit the client.\n"
    "help\n\tprint this message.\n"
    "\n"
    "type:=VCSU|VCSS|PCSU|PCSS\n"
    "policy:=FirstMember|LastMember|Random|FixedRandom|RoundRobin|UserSpecified\n"
    ;
    derecho::subgroup_id_t subgroup_id;
    uint32_t subgroup_index,shard_index;
    persistent::version_t version;

    // loop
    while (true) {
        subgroup_id = 0;
        subgroup_index = 0;
        shard_index = 0;
        version = CURRENT_VERSION;
        char* malloced_cmd = readline("cmd> ");
        std::string cmdline(malloced_cmd);
        free(malloced_cmd);
        if (cmdline == "")continue;
        add_history(cmdline.c_str());

        std::string delimiter = " ";
        auto cmd_tokens = tokenize(cmdline, delimiter.c_str());
        if (cmd_tokens[0] == "help") {
            std::cout << help_info << std::endl;
        } else if (cmd_tokens[0] == "quit" || cmd_tokens[0] == "exit") {
            break;
        } else if (cmd_tokens[0] == "list_all_members") {
            std::cout << "Top Derecho group members = [";
            auto members = capi.get_members();
            for (auto nid: members) {
                std::cout << nid << "," ;
            }
            std::cout << "]" << std::endl;
        } else if (cmd_tokens[0] == "list_type_members") {
            if (cmd_tokens.size() < 2) {
                print_red("Invalid format:" + cmdline);
                continue;
            }
            if (cmd_tokens.size() >= 3) {
                subgroup_index = static_cast<uint32_t>(std::stoi(cmd_tokens[2]));
            }
            if (cmd_tokens.size() >= 4) {
                shard_index = static_cast<uint32_t>(std::stoi(cmd_tokens[3]));
            }
            on_subgroup_type(cmd_tokens[1],print_shard_member,capi,subgroup_index,shard_index);
        } else if (cmd_tokens[0] == "list_subgroup_members") {
            if (cmd_tokens.size() >= 2)
                subgroup_id = static_cast<derecho::subgroup_id_t>(std::stoi(cmd_tokens[1]));
            if (cmd_tokens.size() >= 3)
                shard_index = static_cast<uint32_t>(std::stoi(cmd_tokens[2]));
            print_shard_member(capi,subgroup_id,shard_index);
        } else if (cmd_tokens[0] == "get_member_selection_policy") {
            if (cmd_tokens.size() < 2) {
                print_red("Invalid format:" + cmdline);
                continue;
            }
            if (cmd_tokens.size() >= 3)
                subgroup_index = static_cast<uint32_t>(std::stoi(cmd_tokens[2]));
            if (cmd_tokens.size() >= 4)
                shard_index = static_cast<uint32_t>(std::stoi(cmd_tokens[3]));
            on_subgroup_type(cmd_tokens[1],print_member_selection_policy,capi,subgroup_index,shard_index);
        } else if (cmd_tokens[0] == "set_member_selection_policy") {
            if (cmd_tokens.size() < 5) {
                print_red("Invalid format:" + cmdline);
                continue;
            }
            subgroup_index = static_cast<uint32_t>(std::stoi(cmd_tokens[2]));
            shard_index = static_cast<uint32_t>(std::stoi(cmd_tokens[3]));
            ShardMemberSelectionPolicy policy = parse_policy_name(cmd_tokens[4]);
            if (policy == ShardMemberSelectionPolicy::InvalidPolicy) {
                print_red("Invalid policy name:" + cmd_tokens[4]);
                continue;
            }
            node_id_t user_specified_node_id = INVALID_NODE_ID;
            if (cmd_tokens.size() >= 6) {
                user_specified_node_id = static_cast<node_id_t>(std::stoi(cmd_tokens[5]));
            }
            on_subgroup_type(cmd_tokens[1],set_member_selection_policy,capi,subgroup_index,shard_index,policy,user_specified_node_id);
        } else if (cmd_tokens[0] == "put") {
            persistent::version_t pver = persistent::INVALID_VERSION;
            persistent::version_t pver_bk = persistent::INVALID_VERSION;
            if (cmd_tokens.size() < 4) {
                print_red("Invalid format:" + cmdline);
                continue;
            }
            if (cmd_tokens.size() >= 5)
                pver = static_cast<persistent::version_t>(std::stol(cmd_tokens[4]));
            if (cmd_tokens.size() >= 6)
                pver_bk = static_cast<persistent::version_t>(std::stol(cmd_tokens[5]));
            if (cmd_tokens.size() >= 7)
                subgroup_index = static_cast<uint32_t>(std::stoi(cmd_tokens[6]));
            if (cmd_tokens.size() >= 8)
                shard_index = static_cast<uint32_t>(std::stoi(cmd_tokens[7]));
            on_subgroup_type(cmd_tokens[1],put,capi,cmd_tokens[2]/*key*/,cmd_tokens[3]/*value*/,pver,pver_bk,subgroup_index,shard_index);
        } else if (cmd_tokens[0] == "remove") {
            if (cmd_tokens.size() < 3) {
                print_red("Invalid format:" + cmdline);
                continue;
            }
            if (cmd_tokens.size() >= 4)
                subgroup_index = static_cast<uint32_t>(std::stoi(cmd_tokens[3]));
            if (cmd_tokens.size() >= 5)
                shard_index = static_cast<uint32_t>(std::stoi(cmd_tokens[4]));
            on_subgroup_type(cmd_tokens[1],remove,capi,cmd_tokens[2]/*key*/,subgroup_index,shard_index);
        } else if (cmd_tokens[0] == "get") {
            if (cmd_tokens.size() < 3) {
                print_red("Invalid format:" + cmdline);
                continue;
            }
            if (cmd_tokens.size() >= 4)
                version = static_cast<persistent::version_t>(std::stol(cmd_tokens[3]));
            if (cmd_tokens.size() >= 5)
                subgroup_index = static_cast<uint32_t>(std::stoi(cmd_tokens[4]));
            if (cmd_tokens.size() >= 6)
                shard_index = static_cast<uint32_t>(std::stoi(cmd_tokens[5]));
            on_subgroup_type(cmd_tokens[1],get,capi,cmd_tokens[2],version,subgroup_index,shard_index);
        } else if (cmd_tokens[0] == "get_by_time") {
            if (cmd_tokens.size() < 4) {
                print_red("Invalid format:" + cmdline);
                continue;
            }
            uint64_t ts_us = static_cast<uint64_t>(std::stol(cmd_tokens[3]));
            if (cmd_tokens.size() >= 5)
                subgroup_index = static_cast<uint32_t>(std::stoi(cmd_tokens[4]));
            if (cmd_tokens.size() >= 6)
                shard_index = static_cast<uint32_t>(std::stoi(cmd_tokens[5]));
            on_subgroup_type(cmd_tokens[1],get_by_time,capi,cmd_tokens[2],ts_us,subgroup_index,shard_index);
        } else if (cmd_tokens[0] == "get_size") {
            if (cmd_tokens.size() < 3) {
                print_red("Invalid format:" + cmdline);
                continue;
            }
            if (cmd_tokens.size() >= 4)
                version = static_cast<persistent::version_t>(std::stol(cmd_tokens[3]));
            if (cmd_tokens.size() >= 5)
                subgroup_index = static_cast<uint32_t>(std::stoi(cmd_tokens[4]));
            if (cmd_tokens.size() >= 6)
                shard_index = static_cast<uint32_t>(std::stoi(cmd_tokens[5]));
            on_subgroup_type(cmd_tokens[1],get_size,capi,cmd_tokens[2],version,subgroup_index,shard_index);
        } else if (cmd_tokens[0] == "get_size_by_time") {
            if (cmd_tokens.size() < 4) {
                print_red("Invalid format:" + cmdline);
                continue;
            }
            uint64_t ts_us = static_cast<uint64_t>(std::stol(cmd_tokens[3]));
            if (cmd_tokens.size() >= 5)
                subgroup_index = static_cast<uint32_t>(std::stoi(cmd_tokens[4]));
            if (cmd_tokens.size() >= 6)
                shard_index = static_cast<uint32_t>(std::stoi(cmd_tokens[5]));
            on_subgroup_type(cmd_tokens[1],get_size_by_time,capi,cmd_tokens[2],ts_us,subgroup_index,shard_index);
        } else if (cmd_tokens[0] == "list_keys") {
            if (cmd_tokens.size() < 2) {
                print_red("Invalid format:" + cmdline);
                continue;
            }
            if (cmd_tokens.size() >= 3) {
                version = static_cast<persistent::version_t>(std::stol(cmd_tokens[2]));
            }
            if (cmd_tokens.size() >= 4) {
                subgroup_index = static_cast<uint32_t>(std::stol(cmd_tokens[3]));
            }
            if (cmd_tokens.size() >= 5) {
                shard_index = static_cast<uint32_t>(std::stoi(cmd_tokens[4]));
            }
            on_subgroup_type(cmd_tokens[1],list_keys,capi,version,subgroup_index,shard_index);
        } else if (cmd_tokens[0] == "list_keys_by_time") {
            if (cmd_tokens.size() < 3) {
                print_red("Invalid format:" + cmdline);
                continue;
            }
            uint64_t ts_us = static_cast<uint64_t>(std::stol(cmd_tokens[3]));
            if (cmd_tokens.size() >= 4) {
                subgroup_index = static_cast<uint32_t>(std::stol(cmd_tokens[3]));
            }
            if (cmd_tokens.size() >= 5) {
                shard_index = static_cast<uint32_t>(std::stoi(cmd_tokens[4]));
            }
            on_subgroup_type(cmd_tokens[1],list_keys_by_time,capi,ts_us,subgroup_index,shard_index);
#if HAS_BOOLINQ
        } else if (cmd_tokens[0] == "list_data_by_prefix") {
            if (cmd_tokens.size() < 3) {
                print_red("Invalid format:" + cmdline);
                continue;
            }
            std::string& prefix = cmd_tokens[2];
            if (cmd_tokens.size() >= 4)
                version = static_cast<persistent::version_t>(std::stol(cmd_tokens[3]));
            if (cmd_tokens.size() >= 5)
                subgroup_index = static_cast<uint32_t>(std::stoi(cmd_tokens[4]));
            if (cmd_tokens.size() >= 6)
                shard_index = static_cast<uint32_t>(std::stoi(cmd_tokens[5]));
            on_subgroup_type(cmd_tokens[1],list_data_by_prefix,capi,prefix,version,subgroup_index,shard_index);
        } else if (cmd_tokens[0] == "list_data_between_version") {
            if (cmd_tokens.size() < 5) {
                print_red("Invalid format:" + cmdline);
                continue;
            }
            uint32_t subgroup_index = static_cast<uint32_t>(std::stoi(cmd_tokens[3]));
            uint32_t shard_index = static_cast<uint32_t>(std::stoi(cmd_tokens[4]));

            persistent::version_t version_begin = INVALID_VERSION;
            if (cmd_tokens.size() >= 6) {
                version_begin = static_cast<persistent::version_t>(std::stol(cmd_tokens[5]));
            }
            if (cmd_tokens.size() >= 7) {
                version = static_cast<persistent::version_t>(std::stol(cmd_tokens[6]));
            }
            on_subgroup_type(cmd_tokens[1], list_data_between_version, capi, cmd_tokens[2] /*key*/, subgroup_index, shard_index, version_begin, version);
        } else if (cmd_tokens[0] == "list_data_of_key_between_timestamp") {
            if (cmd_tokens.size() < 3) {
                print_red("Invalid format:" + cmdline);
                continue;
            }

            uint64_t start = 0;
            uint64_t end = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
            if (cmd_tokens.size() >= 4) {
                start = static_cast<uint64_t>(std::stol(cmd_tokens[3]));
            }
            if (cmd_tokens.size() >= 5) {
                end = static_cast<uint64_t>(std::stol(cmd_tokens[4]));
            }
            if (cmd_tokens.size() >= 6) {
                subgroup_index = static_cast<uint32_t>(std::stoi(cmd_tokens[5]));
            }
            if (cmd_tokens.size() >= 7) {
                shard_index = static_cast<uint32_t>(std::stoi(cmd_tokens[6]));
            }
            on_subgroup_type(cmd_tokens[1], list_data_of_key_between_timestamp, capi, cmd_tokens[2], start, end, subgroup_index, shard_index);
        } else if (cmd_tokens[0] == "list_data_in_subgroup") {
            if (cmd_tokens.size() < 3) {
                print_red("Invalid format:" + cmdline);
                continue;
            }
            uint32_t subgroup_index = static_cast<uint32_t>(std::stoi(cmd_tokens[2]));

            if (cmd_tokens.size() >= 4) {
                version = static_cast<persistent::version_t>(std::stol(cmd_tokens[3]));
            }
            on_subgroup_type(cmd_tokens[1], list_data_in_subgroup, capi, subgroup_index, version);
#endif//HAS_BOOLINQ
        } else {
            print_red("command:" + cmd_tokens[0] + " is not implemented or unknown.");
        }
    }
    std::cout << "Client exits." << std::endl;
}

int main(int,char**) {
    std::cout << "This is a Service Client Example." << std::endl;

    ServiceClientAPI capi;
    // TEST 1 
    // member_test(capi);
    // TEST 2
    interactive_test(capi);
    // 
    return 0;
}
