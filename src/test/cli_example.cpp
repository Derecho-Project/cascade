#include <iostream>
#include <vector>
#include <memory>
#include <derecho/core/derecho.hpp>
#include <derecho/utils/logger.hpp>
#include <cascade/cascade.hpp>
#include <cascade/object.hpp>

using namespace derecho::cascade;
using derecho::ExternalClientCaller;

static void print_help(const char* cmd_str) {
    std::cout << "Usage: " << cmd_str << " [(derecho options) --] <server|client>" << std::endl;
    return;
}

using VCS = VolatileCascadeStore<uint64_t,ObjectWithUInt64Key,&ObjectWithUInt64Key::IK,&ObjectWithUInt64Key::IV>;
using PCS = PersistentCascadeStore<uint64_t,ObjectWithUInt64Key,&ObjectWithUInt64Key::IK,&ObjectWithUInt64Key::IV,ST_FILE>;

static std::vector<std::string> tokenize(std::string& line) {
    std::vector<std::string> tokens;
    char line_buf[1024];
    std::strcpy(line_buf, line.c_str());
    char *token = std::strtok(line_buf, " ");
    while (token != nullptr) {
        tokens.push_back(std::string(token));
        token = std::strtok(NULL, " ");
    }
    return std::move(tokens);
}

static void client_help() {
    static const char* HELP_STR = 
        "(v/p)put <object_id> <contents>\n"
        "    - Put an object\n"
        "(v/p)get <object_id> [-t timestamp_in_us | -v version_number]\n"
        "    - Get the latest version of an object if no '-t' or '-v' is specified.\n"
        "    - '-t' specifies the timestamp in microseconds.\n"
        "    - '-v' specifies the version.\n"
        "(v/p)remove <object_id>\n"
        "    - Remove an object specified by the key.\n"
        "help\n"
        "    - print this message.\n"
        "quit/exit\n"
        "    - quit the client.\n"
        "Notes: prefix 'v' specifies the volatile store, 'p' specifies the persistent store.\n";
    std::cout << HELP_STR << std::endl;
    return;
}

// put
static void client_put(derecho::ExternalGroup<VCS,PCS>& group,
                       node_id_t member,
                       const std::vector<std::string>& tokens,
                       bool is_persistent) {
    if (tokens.size() != 3) {
        std::cout << "Invalid format of 'put' command." << std::endl;
    }

    uint64_t key = std::stoll(tokens[1]);
    
    ObjectWithUInt64Key o(key,Blob(tokens[2].c_str(),tokens[2].size()));

    if (is_persistent) {
        ExternalClientCaller<PCS,std::remove_reference<decltype(group)>::type>& pcs_ec = group.get_subgroup_caller<PCS>();
        auto result = pcs_ec.p2p_send<RPC_NAME(put)>(member,o);
        auto reply = result.get().get(member);
        std::cout << "put finished with timestamp=" << std::get<0>(reply)
                  << ",version=" << std::get<1>(reply) << std::endl;
    } else {
        ExternalClientCaller<VCS,std::remove_reference<decltype(group)>::type>& vcs_ec = group.get_subgroup_caller<VCS>();
        auto result = vcs_ec.p2p_send<RPC_NAME(put)>(member,o);
        auto reply = result.get().get(member);
        std::cout << "put finished with timestamp=" << std::get<0>(reply)
                  << ",version=" << std::get<1>(reply) << std::endl;
    }
    return;
}

// get
static void client_get(derecho::ExternalGroup<VCS,PCS>& group,
                       node_id_t member,
                       const std::vector<std::string>& tokens,
                       bool is_persistent) {
    if (tokens.size() != 2 && tokens.size() != 4) {
        std::cout << "Invalid format of 'put' command." << std::endl;
    }

    uint64_t key = std::stoll(tokens[1]);
    uint64_t ver = persistent::INVALID_VERSION;
    uint64_t ts  = 0;

    if (tokens.size() == 4) {
        if (tokens[2].compare("-t")) {
            ts = static_cast<uint64_t>(std::stoll(tokens[3]));
        } else if (tokens[2].compare("-v")) {
            ver = static_cast<uint64_t>(std::stoll(tokens[3]));
        } else {
            std::cout << "Unknown option " << tokens[2] << std::endl;
            return;
        }
    }

    std::optional<derecho::rpc::QueryResults<const ObjectWithUInt64Key>> opt;
    if (is_persistent) {
        ExternalClientCaller<PCS,std::remove_reference<decltype(group)>::type>& pcs_ec = group.get_subgroup_caller<PCS>();
        if (ts != 0) {
            opt.emplace(pcs_ec.p2p_send<RPC_NAME(get_by_time)>(member,key,ts));
        } else {
            opt.emplace(pcs_ec.p2p_send<RPC_NAME(get)>(member,key,ver));
        }
    } else {
        ExternalClientCaller<VCS,std::remove_reference<decltype(group)>::type>& vcs_ec = group.get_subgroup_caller<VCS>();
        if (ts != 0) {
            opt.emplace(vcs_ec.p2p_send<RPC_NAME(get_by_time)>(member,key,ts));
        } else {
            opt.emplace(vcs_ec.p2p_send<RPC_NAME(get)>(member,key,ver));
        }
    }
    auto reply = opt.value().get().get(member);
    std::cout << "get finished with object:" << reply << std::endl;
}

// remove
static void client_remove(derecho::ExternalGroup<VCS,PCS>& group,
                          node_id_t member,
                          const std::vector<std::string>& tokens,
                          bool is_persistent) {
    if (tokens.size() != 2) {
        std::cout << "Invalid format of 'put' command." << std::endl;
    }

    uint64_t key = std::stoll(tokens[1]);
    
    if (is_persistent) {
        ExternalClientCaller<PCS,std::remove_reference<decltype(group)>::type>& pcs_ec = group.get_subgroup_caller<PCS>();
        auto result = pcs_ec.p2p_send<RPC_NAME(remove)>(member,key);
        auto reply = result.get().get(member);
        std::cout << "put finished with timestamp=" << std::get<0>(reply)
                  << ",version=" << std::get<1>(reply) << std::endl;
    } else {
        ExternalClientCaller<VCS,std::remove_reference<decltype(group)>::type>& vcs_ec = group.get_subgroup_caller<VCS>();
        auto result = vcs_ec.p2p_send<RPC_NAME(remove)>(member,key);
        auto reply = result.get().get(member);
        std::cout << "put finished with timestamp=" << std::get<0>(reply)
                  << ",version=" << std::get<1>(reply) << std::endl;
    }
    return;
}

void do_client() {
    /** 1 - create external client group*/
    derecho::ExternalGroup<VCS,PCS> group;
    std::cout << "Finished constructing ExternalGroup." << std::endl;

    /** 2 - get members */
    std::vector<node_id_t> g_members = group.get_members();
    std::cout << "Members in top derecho group:[ ";
    for(auto& nid:g_members) {
        std::cout << nid << " ";
    }
    std::cout << "]" << std::endl;

    std::vector<node_id_t> vcs_members = group.template get_shard_members<VCS>(0,0);
    std::cout << "Members in the single shard of Volatile Cascade Store:[ ";
    for (auto& nid:vcs_members) {
        std::cout << nid << " ";
    }
    std::cout << "]" << std::endl;

    std::vector<node_id_t> pcs_members = group.template get_shard_members<PCS>(0,0);
    std::cout << "Members in the single shard of Persistent Cascade Store:[ ";
    for (auto& nid:pcs_members) {
        std::cout << nid << " ";
    }
    std::cout << "]" << std::endl;

    /** 3 - run command line. */
    while(true) {
        std::string cmdline;
        std::cout << "cmd> " << std::flush;
        std::getline(std::cin, cmdline);
        auto cmd_tokens = tokenize(cmdline);
        if (cmd_tokens.size() == 0) {
            continue;
        }

        if (cmd_tokens[0].compare("help") == 0) {
            client_help();
        } else if (cmd_tokens[0].compare("vput") == 0) {
            client_put(group,vcs_members[0],cmd_tokens,false);
        } else if (cmd_tokens[0].compare("pput") == 0) {
            client_put(group,pcs_members[0],cmd_tokens,true);
        } else if (cmd_tokens[0].compare("vget") == 0) {
            client_get(group,vcs_members[0],cmd_tokens,false);
        } else if (cmd_tokens[0].compare("pget") == 0) {
            client_get(group,pcs_members[0],cmd_tokens,true);
        } else if (cmd_tokens[0].compare("vremove") == 0) {
            client_remove(group,vcs_members[0],cmd_tokens,false);
        } else if (cmd_tokens[0].compare("premove") == 0) {
            client_remove(group,pcs_members[0],cmd_tokens,true);
        } else if (cmd_tokens[0].compare("quit") == 0 ||
                   cmd_tokens[0].compare("exit") == 0) {
            std::cout << "Exiting client." << std::endl;
            break;
        } else {
            std::cout << "Unknown command:" << cmd_tokens[0] << std::endl;
        }
    }
}

  
class PerfCascadeWatcherContext : public ICascadeWatcherContext<uint64_t,ObjectWithUInt64Key,&ObjectWithUInt64Key::IK,&ObjectWithUInt64Key::IV> {
    std::shared_ptr<CascadeWatcher<uint64_t,ObjectWithUInt64Key,&ObjectWithUInt64Key::IK,&ObjectWithUInt64Key::IV>> watcher_ptr;
public:
    PerfCascadeWatcherContext() {
        watcher_ptr = std::make_shared<CascadeWatcher<uint64_t,ObjectWithUInt64Key,&ObjectWithUInt64Key::IK,&ObjectWithUInt64Key::IV>> (
            [](derecho::subgroup_id_t sid,
               const uint32_t shard_num,
               const uint64_t& key,
               const ObjectWithUInt64Key& value,
               void* cascade_context){
                dbg_default_info("Watcher is called with\n\tsubgroup id = {},\n\tshard number = {},\n\tkey = {},\n\tvalue = [hidden].", sid, shard_num, key);
               });
    }
    std::shared_ptr<CascadeWatcher<uint64_t,ObjectWithUInt64Key,&ObjectWithUInt64Key::IK,&ObjectWithUInt64Key::IV>> get_cascade_watcher() override {
        return this->watcher_ptr;
    }
};


void do_server() {
    dbg_default_info("Starting cascade server.");

    /** 1 - group building blocks*/
    derecho::CallbackSet callback_set {
        nullptr,    // delivery callback
        nullptr,    // local persistence callback
        nullptr     // global persistence callback
    };
    derecho::SubgroupInfo si {
        derecho::DefaultSubgroupAllocator({
            {std::type_index(typeid(VCS)),
             derecho::one_subgroup_policy(derecho::flexible_even_shards("VCS"))},
            {std::type_index(typeid(PCS)),
             derecho::one_subgroup_policy(derecho::flexible_even_shards("PCS"))}
        })
    };
	PerfCascadeWatcherContext pcwc;
    auto vcs_factory = [&pcwc](persistent::PersistentRegistry* pr, derecho::subgroup_id_t) {
        return std::make_unique<VCS>(pcwc.get_cascade_watcher());
    };
    auto pcs_factory = [&pcwc](persistent::PersistentRegistry* pr, derecho::subgroup_id_t) {
        return std::make_unique<PCS>(pr,pcwc.get_cascade_watcher());
    };
    /** 2 - create group */
    derecho::Group<VCS,PCS> group(callback_set,si,{&pcwc}/*deserialization manager*/,
                                  std::vector<derecho::view_upcall_t>{},
                                  vcs_factory,pcs_factory);
    std::cout << "Cascade Server finished constructing Derecho group." << std::endl;
    std::cout << "Press ENTER to shutdown..." << std::endl;
    std::cin.get();
    group.barrier_sync();
    group.leave();
    dbg_default_info("Cascade server shutdown.");
}

int main(int argc, char** argv) {
    /** initialize the parameters */
    derecho::Conf::initialize(argc,argv);

    /** check parameters */
    if (argc < 2) {
        print_help(argv[0]);
        return -1;
    }

    if (std::string("client").compare(argv[argc-1]) == 0) {
        do_client();
    } else if (std::string("server").compare(argv[argc-1]) == 0) {
        do_server();
    } else {
        std::cerr << "Unknown mode:" << argv[argc-1] << std::endl;
        print_help(argv[0]);
        return -1;
    }

    return 0;
}
