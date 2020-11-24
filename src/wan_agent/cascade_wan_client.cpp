#include <cascade/wan_service_client_api.hpp>
#include <fstream>
#include <iostream>
#include <readline/history.h>
#include <readline/readline.h>
#include <stdio.h>
#include <string>
#include <typeindex>

using namespace derecho::cascade;

template <typename SubgroupType>
void print_shard_member(ServiceClientAPI& capi, uint32_t subgroup_index, uint32_t shard_index) {
    std::cout << "Subgroup (Type=" << std::type_index(typeid(SubgroupType)).name() << ","
              << "subgroup_index=" << subgroup_index << ","
              << "shard_index=" << shard_index << ") member list = [";
    auto members = capi.template get_shard_members<SubgroupType>(subgroup_index, shard_index);
    for(auto nid : members) {
        std::cout << nid << ",";
    }
    std::cout << "]" << std::endl;
}

void print_shard_member(ServiceClientAPI& capi, derecho::subgroup_id_t subgroup_id, uint32_t shard_index) {
    std::cout << "subgroup_id=" << subgroup_id << ","
              << "shard_index=" << shard_index << " member list = [";
    auto members = capi.get_shard_members(subgroup_id, shard_index);
    for(auto nid : members) {
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
        nullptr};

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

template <typename SubgroupType>
void print_member_selection_policy(ServiceClientAPI& capi, uint32_t subgroup_index, uint32_t shard_index) {
    std::cout << "Subgroup (Type=" << std::type_index(typeid(SubgroupType)).name() << ","
              << "subgroup_index=" << subgroup_index << ","
              << "shard_index=" << shard_index << ") policy=";
    auto policy = capi.template get_member_selection_policy<SubgroupType>(subgroup_index, shard_index);
    std::cout << policy_names[std::get<0>(policy)] << "(" << std::get<0>(policy) << ")," << std::get<1>(policy) << "" << std::endl;
}

template <typename SubgroupType>
void set_member_selection_policy(ServiceClientAPI& capi, uint32_t subgroup_index, uint32_t shard_index,
                                 ShardMemberSelectionPolicy policy, node_id_t user_specified_node_id) {
    capi.template set_member_selection_policy<SubgroupType>(subgroup_index, shard_index, policy, user_specified_node_id);
}

/* TEST1: members */
void member_test(ServiceClientAPI& capi) {
    // print all members.
    std::cout << "Top Derecho group members = [";
    auto members = capi.get_members();
    for(auto nid : members) {
        std::cout << nid << ",";
    }
    std::cout << "]" << std::endl;
    // print per Subgroup Members:
    print_shard_member<WPCSU>(capi, 0, 0);
    print_shard_member<WPCSS>(capi, 0, 0);
    print_shard_member(capi, 0, 0);
    print_shard_member(capi, 1, 0);
    print_shard_member(capi, 2, 0);
    print_shard_member(capi, 3, 0);
}

static std::vector<std::string> tokenize(std::string& line) {
    std::vector<std::string> tokens;
    char line_buf[1024];
    std::strcpy(line_buf, line.c_str());
    char* token = std::strtok(line_buf, " ");
    while(token != nullptr) {
        tokens.push_back(std::string(token));
        token = std::strtok(NULL, " ");
    }
    return tokens;  // RVO
}

static void print_red(std::string msg) {
    std::cout << "\033[1;31m"
              << msg
              << "\033[0m" << std::endl;
}

#define on_subgroup_type(x, ft, ...)                         \
    if((x) == "WPCSU") {                                     \
        ft<WPCSU>(__VA_ARGS__);                              \
    } else if((x) == "WPCSS") {                              \
        ft<WPCSS>(__VA_ARGS__);                              \
    } else {                                                 \
        print_red("unknown subgroup type:" + cmd_tokens[1]); \
    }

#define check_put_and_remove_result(result)                                                           \
    for(auto& reply_future : result.get()) {                                                          \
        auto reply = reply_future.second.get();                                                       \
        std::cout << "node(" << reply_future.first << ") replied with version:" << std::get<0>(reply) \
                  << ",ts_us:" << std::get<1>(reply) << std::endl;                                    \
    }

template <typename SubgroupType>
void put(ServiceClientAPI& capi, std::string& key, std::string& value, uint32_t subgroup_index, uint32_t shard_index) {
    typename SubgroupType::ObjectType obj;
    if constexpr(std::is_same<typename SubgroupType::KeyType, uint64_t>::value) {
        obj.key = static_cast<uint64_t>(std::stol(key));
    } else if constexpr(std::is_same<typename SubgroupType::KeyType, std::string>::value) {
        obj.key = key;
    } else {
        print_red(std::string("Unhandled KeyType:") + typeid(typename SubgroupType::KeyType).name());
        return;
    }
    obj.blob = Blob(value.c_str(), value.length());
    derecho::rpc::QueryResults<std::tuple<persistent::version_t, uint64_t>> result = capi.template put<SubgroupType>(obj, subgroup_index, shard_index);
    check_put_and_remove_result(result);
}

template <typename SubgroupType>
void remove(ServiceClientAPI& capi, std::string& key, uint32_t subgroup_index, uint32_t shard_index) {
    if constexpr(std::is_same<typename SubgroupType::KeyType, uint64_t>::value) {
        derecho::rpc::QueryResults<std::tuple<persistent::version_t, uint64_t>> result = std::move(capi.template remove<SubgroupType>(static_cast<uint64_t>(std::stol(key)), subgroup_index, shard_index));
        check_put_and_remove_result(result);
    } else if constexpr(std::is_same<typename SubgroupType::KeyType, std::string>::value) {
        derecho::rpc::QueryResults<std::tuple<persistent::version_t, uint64_t>> result = std::move(capi.template remove<SubgroupType>(key, subgroup_index, shard_index));
        check_put_and_remove_result(result);
    } else {
        print_red(std::string("Unhandled KeyType:") + typeid(typename SubgroupType::KeyType).name());
        return;
    }
}

template <typename SubgroupType>
void submit_predicate(ServiceClientAPI& capi, std::string& key, std::string& predicate_str, bool inplace) {
    if constexpr(std::is_same<typename SubgroupType::KeyType, uint64_t>::value) {
        // derecho::rpc::QueryResults<std::tuple<persistent::version_t,uint64_t>> result = std::move(capi.template submit_predicate<SubgroupType>(key, predicate_str, inplace));
        capi.template submit_predicate<SubgroupType>(key, predicate_str, inplace);
    } else if constexpr(std::is_same<typename SubgroupType::KeyType, std::string>::value) {
        // derecho::rpc::QueryResults<std::tuple<persistent::version_t,uint64_t>> result = std::move(capi.template submit_predicate<SubgroupType>(key, predicate_str, inplace));
        capi.template submit_predicate<SubgroupType>(key, predicate_str, inplace);
    } else {
        print_red(std::string("Unhandled KeyType:") + typeid(typename SubgroupType::KeyType).name());
        return;
    }
}

template <typename SubgroupType>
void change_predicate(ServiceClientAPI& capi, std::string& key) {
    if constexpr(std::is_same<typename SubgroupType::KeyType, uint64_t>::value) {
        capi.template change_predicate<SubgroupType>(key);
        // derecho::rpc::QueryResults<std::tuple<persistent::version_t,uint64_t>> result = std::move(capi.template change_predicate<SubgroupType>(key));
    } else if constexpr(std::is_same<typename SubgroupType::KeyType, std::string>::value) {
        capi.template change_predicate<SubgroupType>(key);
        // derecho::rpc::QueryResults<std::tuple<persistent::version_t,uint64_t>> result = std::move(capi.template change_predicate<SubgroupType>(key));
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

template <typename SubgroupType>
void get(ServiceClientAPI& capi, std::string& key, persistent::version_t ver, uint32_t subgroup_index, uint32_t shard_index) {
    if constexpr(std::is_same<typename SubgroupType::KeyType, uint64_t>::value) {
        derecho::rpc::QueryResults<const typename SubgroupType::ObjectType> result = capi.template get<SubgroupType>(
                static_cast<uint64_t>(std::stol(key)), ver, subgroup_index, shard_index);
        check_get_result(result);
    } else if constexpr(std::is_same<typename SubgroupType::KeyType, std::string>::value) {
        derecho::rpc::QueryResults<const typename SubgroupType::ObjectType> result = capi.template get<SubgroupType>(
                key, ver, subgroup_index, shard_index);
        check_get_result(result);
    }
}

template <typename SubgroupType>
void get_by_time(ServiceClientAPI& capi, std::string& key, uint64_t ts_us, uint32_t subgroup_index, uint32_t shard_index) {
    if constexpr(std::is_same<typename SubgroupType::KeyType, uint64_t>::value) {
        derecho::rpc::QueryResults<const typename SubgroupType::ObjectType> result = capi.template get_by_time<SubgroupType>(
                static_cast<uint64_t>(std::stol(key)), ts_us, subgroup_index, shard_index);
        check_get_result(result);
    } else if constexpr(std::is_same<typename SubgroupType::KeyType, std::string>::value) {
        derecho::rpc::QueryResults<const typename SubgroupType::ObjectType> result = capi.template get<SubgroupType>(
                key, ts_us, subgroup_index, shard_index);
        check_get_result(result);
    }
}

template <typename SubgroupType>
void get_size(ServiceClientAPI& capi, std::string& key, persistent::version_t ver, uint32_t subgroup_index, uint32_t shard_index) {
    if constexpr(std::is_same<typename SubgroupType::KeyType, uint64_t>::value) {
        derecho::rpc::QueryResults<uint64_t> result = capi.template get_size<SubgroupType>(
                static_cast<uint64_t>(std::stol(key)), ver, subgroup_index, shard_index);
        check_get_result(result);
    } else if constexpr(std::is_same<typename SubgroupType::KeyType, std::string>::value) {
        derecho::rpc::QueryResults<uint64_t> result = capi.template get_size<SubgroupType>(
                key, ver, subgroup_index, shard_index);
        check_get_result(result);
    }
}

template <typename SubgroupType>
void get_size_by_time(ServiceClientAPI& capi, std::string& key, uint64_t ts_us, uint32_t subgroup_index, uint32_t shard_index) {
    if constexpr(std::is_same<typename SubgroupType::KeyType, uint64_t>::value) {
        derecho::rpc::QueryResults<uint64_t> result = capi.template get_size_by_time<SubgroupType>(
                static_cast<uint64_t>(std::stol(key)), ts_us, subgroup_index, shard_index);
        check_get_result(result);
    } else if constexpr(std::is_same<typename SubgroupType::KeyType, std::string>::value) {
        derecho::rpc::QueryResults<uint64_t> result = capi.template get_size_by_time<SubgroupType>(
                key, ts_us, subgroup_index, shard_index);
        check_get_result(result);
    }
}

#define check_list_keys_result(result)               \
    for(auto& reply_future : result.get()) {         \
        auto reply = reply_future.second.get();      \
        std::cout << "Keys:" << std::endl;           \
        for(auto& key : reply) {                     \
            std::cout << "    " << key << std::endl; \
        }                                            \
    }

template <typename SubgroupType>
void list_keys(ServiceClientAPI& capi, persistent::version_t ver, uint32_t subgroup_index, uint32_t shard_index) {
    derecho::rpc::QueryResults<std::vector<typename SubgroupType::KeyType>> result = capi.template list_keys<SubgroupType>(ver, subgroup_index, shard_index);
    check_list_keys_result(result);
}

template <typename SubgroupType>
void list_keys_by_time(ServiceClientAPI& capi, uint64_t ts_us, uint32_t subgroup_index, uint32_t shard_index) {
    derecho::rpc::QueryResults<std::vector<typename SubgroupType::KeyType>> result = capi.template list_keys_by_time<SubgroupType>(ts_us, subgroup_index, shard_index);
    check_list_keys_result(result);
}

/* TEST2: put/get/remove tests */
void interactive_test(ServiceClientAPI& capi) {
    const char* help_info = "list_all_members\n\tlist all members in top level derecho group.\n"
                            "list_type_members <type> [subgroup_index] [shard_index]\n\tlist members in shard by subgroup type.\n"
                            "list_subgroup_members [subgroup_id] [shard_index]\n\tlist members in shard by subgroup id.\n"
                            "set_member_selection_policy <type> <subgroup_index> <shard_index> <policy> [user_specified_node_id]\n\tset member selection policy\n"
                            "get_member_selection_policy <type> [subgroup_index] [shard_index]\n\tget member selection policy\n"
                            "put <type> <key> <value> [subgroup_index] [shard_index]\n\tput an object\n"
                            "remove <type> <key> [subgroup_index] [shard_index]\n\tremove an object\n"
                            "get <type> <key> [version] [subgroup_index] [shard_index]\n\tget an object(by version)\n"
                            "get_by_time <type> <key> <ts_us> [subgroup_index] [shard_index]\n\tget an object by timestamp\n"
                            "get_size <type> <key> [version] [subgroup_index] [shard_index]\n\tget the size of an object(by version)\n"
                            "get_size_by_time <type> <key> <ts_us> [subgroup_index] [shard_index]\n\tget the size of an object by timestamp\n"
                            "list_keys <type> [version] [subgroup_index] [shard_index]\n\tlist keys in shard (by version)\n"
                            "list_keys_by_time <type> <ts_us> [subgroup_index] [shard_index]\n\tlist keys in shard by time\n"
                            "submit_predicate <type> <key> <predicate_sentence> <inplace>\n"
                            "change_predicate <type> <key>\n"
                            "quit|exit\n\texit the client.\n"
                            "help\n\tprint this message.\n"
                            "\n"
                            "type:=WPCSU|WPCSS\n"
                            "policy:=FirstMember|LastMember|Random|FixedRandom|RoundRobin|UserSpecified\n";
    derecho::subgroup_id_t subgroup_id;
    uint32_t subgroup_index, shard_index;
    persistent::version_t version;

    // loop
    while(true) {
        subgroup_id = 0;
        subgroup_index = 0;
        shard_index = 0;
        version = CURRENT_VERSION;
        char* malloced_cmd = readline("cmd> ");
        std::string cmdline(malloced_cmd);
        free(malloced_cmd);
        if(cmdline == "") continue;
        add_history(cmdline.c_str());

        auto cmd_tokens = tokenize(cmdline);
        if(cmd_tokens[0] == "help") {
            std::cout << help_info << std::endl;
        } else if(cmd_tokens[0] == "quit" || cmd_tokens[0] == "exit") {
            break;
        } else if(cmd_tokens[0] == "list_all_members") {
            std::cout << "Top Derecho group members = [";
            auto members = capi.get_members();
            for(auto nid : members) {
                std::cout << nid << ",";
            }
            std::cout << "]" << std::endl;
        } else if(cmd_tokens[0] == "list_type_members") {
            if(cmd_tokens.size() < 2) {
                print_red("Invalid format:" + cmdline);
                continue;
            }
            if(cmd_tokens.size() >= 3) {
                subgroup_index = static_cast<uint32_t>(std::stoi(cmd_tokens[2]));
            }
            if(cmd_tokens.size() >= 4) {
                shard_index = static_cast<uint32_t>(std::stoi(cmd_tokens[3]));
            }
            on_subgroup_type(cmd_tokens[1], print_shard_member, capi, subgroup_index, shard_index);
        } else if(cmd_tokens[0] == "list_subgroup_members") {
            if(cmd_tokens.size() >= 2)
                subgroup_id = static_cast<derecho::subgroup_id_t>(std::stoi(cmd_tokens[1]));
            if(cmd_tokens.size() >= 3)
                shard_index = static_cast<uint32_t>(std::stoi(cmd_tokens[2]));
            print_shard_member(capi, subgroup_id, shard_index);
        } else if(cmd_tokens[0] == "get_member_selection_policy") {
            if(cmd_tokens.size() < 2) {
                print_red("Invalid format:" + cmdline);
                continue;
            }
            if(cmd_tokens.size() >= 3)
                subgroup_index = static_cast<uint32_t>(std::stoi(cmd_tokens[2]));
            if(cmd_tokens.size() >= 4)
                shard_index = static_cast<uint32_t>(std::stoi(cmd_tokens[3]));
            on_subgroup_type(cmd_tokens[1], print_member_selection_policy, capi, subgroup_index, shard_index);
        } else if(cmd_tokens[0] == "set_member_selection_policy") {
            if(cmd_tokens.size() < 5) {
                print_red("Invalid format:" + cmdline);
                continue;
            }
            subgroup_index = static_cast<uint32_t>(std::stoi(cmd_tokens[2]));
            shard_index = static_cast<uint32_t>(std::stoi(cmd_tokens[3]));
            ShardMemberSelectionPolicy policy = parse_policy_name(cmd_tokens[4]);
            if(policy == ShardMemberSelectionPolicy::InvalidPolicy) {
                print_red("Invalid policy name:" + cmd_tokens[4]);
                continue;
            }
            node_id_t user_specified_node_id = INVALID_NODE_ID;
            if(cmd_tokens.size() >= 6) {
                user_specified_node_id = static_cast<node_id_t>(std::stoi(cmd_tokens[5]));
            }
            on_subgroup_type(cmd_tokens[1], set_member_selection_policy, capi, subgroup_index, shard_index, policy, user_specified_node_id);
        } else if(cmd_tokens[0] == "put") {
            if(cmd_tokens.size() < 4) {
                print_red("Invalid format:" + cmdline);
                continue;
            }
            if(cmd_tokens.size() >= 5)
                subgroup_index = static_cast<uint32_t>(std::stoi(cmd_tokens[4]));
            if(cmd_tokens.size() >= 6)
                shard_index = static_cast<uint32_t>(std::stoi(cmd_tokens[5]));
            on_subgroup_type(cmd_tokens[1], put, capi, cmd_tokens[2] /*key*/, cmd_tokens[3] /*value*/, subgroup_index, shard_index);
        } else if(cmd_tokens[0] == "remove") {
            if(cmd_tokens.size() < 3) {
                print_red("Invalid format:" + cmdline);
                continue;
            }
            if(cmd_tokens.size() >= 4)
                subgroup_index = static_cast<uint32_t>(std::stoi(cmd_tokens[3]));
            if(cmd_tokens.size() >= 5)
                shard_index = static_cast<uint32_t>(std::stoi(cmd_tokens[4]));
            on_subgroup_type(cmd_tokens[1], remove, capi, cmd_tokens[2] /*key*/, subgroup_index, shard_index);
        } else if(cmd_tokens[0] == "get") {
            if(cmd_tokens.size() < 3) {
                print_red("Invalid format:" + cmdline);
                continue;
            }
            if(cmd_tokens.size() >= 4)
                version = static_cast<persistent::version_t>(std::stol(cmd_tokens[3]));
            if(cmd_tokens.size() >= 5)
                subgroup_index = static_cast<uint32_t>(std::stoi(cmd_tokens[4]));
            if(cmd_tokens.size() >= 6)
                shard_index = static_cast<uint32_t>(std::stoi(cmd_tokens[5]));
            on_subgroup_type(cmd_tokens[1], get, capi, cmd_tokens[2], version, subgroup_index, shard_index);
        } else if(cmd_tokens[0] == "get_by_time") {
            if(cmd_tokens.size() < 4) {
                print_red("Invalid format:" + cmdline);
                continue;
            }
            uint64_t ts_us = static_cast<uint64_t>(std::stol(cmd_tokens[3]));
            if(cmd_tokens.size() >= 5)
                subgroup_index = static_cast<uint32_t>(std::stoi(cmd_tokens[4]));
            if(cmd_tokens.size() >= 6)
                shard_index = static_cast<uint32_t>(std::stoi(cmd_tokens[5]));
            on_subgroup_type(cmd_tokens[1], get_by_time, capi, cmd_tokens[2], ts_us, subgroup_index, shard_index);
        } else if(cmd_tokens[0] == "get_size") {
            if(cmd_tokens.size() < 3) {
                print_red("Invalid format:" + cmdline);
                continue;
            }
            if(cmd_tokens.size() >= 4)
                version = static_cast<persistent::version_t>(std::stol(cmd_tokens[3]));
            if(cmd_tokens.size() >= 5)
                subgroup_index = static_cast<uint32_t>(std::stoi(cmd_tokens[4]));
            if(cmd_tokens.size() >= 6)
                shard_index = static_cast<uint32_t>(std::stoi(cmd_tokens[5]));
            on_subgroup_type(cmd_tokens[1], get_size, capi, cmd_tokens[2], version, subgroup_index, shard_index);
        } else if(cmd_tokens[0] == "get_size_by_time") {
            if(cmd_tokens.size() < 4) {
                print_red("Invalid format:" + cmdline);
                continue;
            }
            uint64_t ts_us = static_cast<uint64_t>(std::stol(cmd_tokens[3]));
            if(cmd_tokens.size() >= 5)
                subgroup_index = static_cast<uint32_t>(std::stoi(cmd_tokens[4]));
            if(cmd_tokens.size() >= 6)
                shard_index = static_cast<uint32_t>(std::stoi(cmd_tokens[5]));
            on_subgroup_type(cmd_tokens[1], get_size_by_time, capi, cmd_tokens[2], ts_us, subgroup_index, shard_index);
        } else if(cmd_tokens[0] == "list_keys") {
            if(cmd_tokens.size() < 2) {
                print_red("Invalid format:" + cmdline);
                continue;
            }
            if(cmd_tokens.size() >= 3) {
                version = static_cast<persistent::version_t>(std::stol(cmd_tokens[2]));
            }
            if(cmd_tokens.size() >= 4) {
                subgroup_index = static_cast<uint32_t>(std::stol(cmd_tokens[3]));
            }
            if(cmd_tokens.size() >= 5) {
                shard_index = static_cast<uint32_t>(std::stoi(cmd_tokens[4]));
            }
            on_subgroup_type(cmd_tokens[1], list_keys, capi, version, subgroup_index, shard_index);
        } else if(cmd_tokens[0] == "list_keys_by_time") {
            if(cmd_tokens.size() < 3) {
                print_red("Invalid format:" + cmdline);
                continue;
            }
            uint64_t ts_us = static_cast<uint64_t>(std::stol(cmd_tokens[3]));
            if(cmd_tokens.size() >= 4) {
                subgroup_index = static_cast<uint32_t>(std::stol(cmd_tokens[3]));
            }
            if(cmd_tokens.size() >= 5) {
                shard_index = static_cast<uint32_t>(std::stoi(cmd_tokens[4]));
            }
            on_subgroup_type(cmd_tokens[1], list_keys_by_time, capi, ts_us, subgroup_index, shard_index);
        } else if(cmd_tokens[0] == "submit_predicate") {
            if(cmd_tokens.size() < 5) {
                print_red("Invalid format:" + cmdline);
                continue;
            }
            bool inplace;
            if(cmd_tokens[4] == "T" || cmd_tokens[4] == "t") {
                inplace = true;
            } else {
                inplace = false;
            }
            on_subgroup_type(cmd_tokens[1], submit_predicate, capi, cmd_tokens[2], cmd_tokens[3], inplace);
        } else if(cmd_tokens[0] == "change_predicate") {
            if(cmd_tokens.size() < 3) {
                print_red("Invalid format:" + cmdline);
                continue;
            }
            on_subgroup_type(cmd_tokens[1], change_predicate, capi, cmd_tokens[2]);
        } else {
            print_red("command:" + cmd_tokens[0] + " is not implemented or unknown.");
        }
    }
    std::cout << "Client exits." << std::endl;
}

int main(int, char**) {
    std::cout << "This is a Service Client Example." << std::endl;

    ServiceClientAPI capi;
    // TEST 1
    // member_test(capi);
    // TEST 2
    interactive_test(capi);
    //
    return 0;
}
