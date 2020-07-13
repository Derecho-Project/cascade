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
    auto members = capi.template get_shard_members(subgroup_id,shard_index);
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

static std::vector<std::string> tokenize(std::string& line) {
    std::vector<std::string> tokens;
    char line_buf[1024];
    std::strcpy(line_buf, line.c_str());
    char *token = std::strtok(line_buf, " ");
    while (token != nullptr) {
        tokens.push_back(std::string(token));
        token = std::strtok(NULL, " ");
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

/* TEST2: put/get/remove tests */
void interactive_test(ServiceClientAPI& capi) {
    const char* help_info = "\
list_all_members\n\tlist all members in top level derecho group.\n\
list_type_members <type> [subgroup_index] [shard_index]\n\tlist members in shard by subgroup type.\n\
list_subgroup_members [subgroup_id] [shard_index]\n\tlist members in shard by subgroup id.\n\
set_member_selection_policy <type> <subgroup_index> <shard_index> <policy> [user_specified_node_id]\n\tset member selection policy\n\
get_member_selection_policy <type> [subgroup_index] [shard_index]\n\tget member selection policy\n\
put <type> <key> <value> [subgroup_index] [shard_index]\n\tput an object\n\
remove <type> <key> [subgroup_index] [shard_index]\n\tremove an object\n\
get <type> <key> [version] [subgroup_index] [shard_index]\n\tget an object(by version)\n\
get_by_time <type> <key> <ts_us> [subgroup_index] [shard_index]\n\tget an object by timestamp\n\
quit|exit\n\texit the client.\n\
help\n\tprint this message.\n\
\n\
type:=VCSU|VCSS|PCSU|PCSS\n\
policy:=FirstMember|LastMember|Random|FixedRandom|RoundRobin|UserSpecified\n\
";
    derecho::subgroup_id_t subgroup_id;
    uint32_t subgroup_index,shard_index;

    // loop
    while (true) {
        subgroup_id = 0;
        subgroup_index = 0;
        shard_index = 0;
        char* malloced_cmd = readline("cmd> ");
        std::string cmdline(malloced_cmd);
        free(malloced_cmd);
        if (cmdline == "")continue;

        auto cmd_tokens = tokenize(cmdline);
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
        }
        add_history(cmdline.c_str());
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
