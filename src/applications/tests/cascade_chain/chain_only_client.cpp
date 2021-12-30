#include <fstream>
#include <iomanip>
#include <iostream>
#include <readline/history.h>
#include <readline/readline.h>
#include <string>
#include <sys/prctl.h>
#include <typeindex>

#include <cascade/service_client_api.hpp>
#include <cascade/utils.hpp>

#include <derecho/openssl/signature.hpp>

using namespace derecho::cascade;

#define PROC_NAME "cascade_client"

/* -------- Standard client functions copied from client.cpp -------- */

static std::vector<std::string> tokenize(std::string& line, const char* delimiter) {
    std::vector<std::string> tokens;
    char line_buf[1024];
    std::strcpy(line_buf, line.c_str());
    char* token = std::strtok(line_buf, delimiter);
    while(token != nullptr) {
        tokens.push_back(std::string(token));
        token = std::strtok(NULL, delimiter);
    }
    return tokens;  // RVO
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

#define check_put_and_remove_result(result)                                                           \
    for(auto& reply_future : result.get()) {                                                          \
        auto reply = reply_future.second.get();                                                       \
        std::cout << "node(" << reply_future.first << ") replied with version:" << std::get<0>(reply) \
                  << ",ts_us:" << std::get<1>(reply) << std::endl;                                    \
    }

void op_put(ServiceClientAPI& capi, const std::string& key, const std::string& value, persistent::version_t pver, persistent::version_t pver_bk) {
    ObjectWithStringKey obj;
    obj.key = key;
    obj.previous_version = pver;
    obj.previous_version_by_key = pver_bk;
    obj.blob = Blob(value.c_str(), value.length());
    derecho::rpc::QueryResults<std::tuple<persistent::version_t, uint64_t>> result = capi.put(obj);
    check_put_and_remove_result(result);
}

void op_put_and_forget(ServiceClientAPI& capi, const std::string& key, const std::string& value, persistent::version_t pver, persistent::version_t pver_bk) {
    ObjectWithStringKey obj;
    obj.key = key;
    obj.previous_version = pver;
    obj.previous_version_by_key = pver_bk;
    obj.blob = Blob(value.c_str(), value.length());
    capi.put_and_forget(obj);
    std::cout << "put done." << std::endl;
}

template <typename SubgroupType>
void create_object_pool(ServiceClientAPI& capi, const std::string& id, uint32_t subgroup_index) {
    auto result = capi.template create_object_pool<SubgroupType>(id, subgroup_index);
    check_put_and_remove_result(result);
    std::cout << "create_object_pool is done." << std::endl;
}

void op_trigger_put(ServiceClientAPI& capi, const std::string& key, const std::string& value) {
    ObjectWithStringKey obj;

    obj.key = key;
    obj.blob = Blob(value.c_str(), value.length());
    derecho::rpc::QueryResults<void> result = capi.trigger_put(obj);
    result.get();

    std::cout << "op_trigger_put is done." << std::endl;
}

void op_remove(ServiceClientAPI& capi, const std::string& key) {
    auto result = capi.remove(key);
    check_put_and_remove_result(result);
}

#define check_get_result(result)                                                                     \
    for(auto& reply_future : result.get()) {                                                         \
        auto reply = reply_future.second.get();                                                      \
        std::cout << "node(" << reply_future.first << ") replied with value:" << reply << std::endl; \
    }

#define check_op_list_keys_result(result)        \
    std::cout << "Keys:" << std::endl;           \
    for(auto& key : result) {                    \
        std::cout << "    " << key << std::endl; \
    }

/* -------- CascadeChain-specific commands -------- */

//Data the client needs to store for each signature on an object it submitted
struct ObjectSignature {
    std::string key_suffix;
    persistent::version_t object_version;
    persistent::version_t signature_version;
    persistent::version_t object_previous_version;
    persistent::version_t signature_previous_version;
    Blob object_hash;
    std::vector<uint8_t> signature;
};

const std::string delimiter = "/";
//State variables shared among all the commands. Maybe it would be better to use an object here.
std::string storage_pool_name;
std::string signature_pool_name;
// For each key (suffix), contains a map from object version -> signature for that version
std::map<std::string, std::map<persistent::version_t, ObjectSignature>> cached_signatures_by_key;
//Verifier for the service's public key. Initialized after startup by a command.
std::unique_ptr<openssl::Verifier> service_verifier;

// Use this command once at the beginning of a test to create the two object pools for storage and signatures
// Right now it assumes there will only be a single subgroup (index 0) of each type
/**
 * Command that initializes the two object pools needed to run the CascadeChain service.
 * This should only be run once, at the beginning of a test. Right now it assumes there
 * will only be a single PCSS subgroup and a single SCSS subgroup, so it creates both
 * object pools on subgroup index 0.
 *
 * Expected arguments: [storage-pool-name] [signature-pool-name]
 */
bool setup_object_pools(ServiceClientAPI& client, const std::vector<std::string>& cmd_tokens) {
    if(cmd_tokens.size() >= 2) {
        storage_pool_name = cmd_tokens[1];
    } else {
        storage_pool_name = "/storage";
    }
    if(cmd_tokens.size() >= 3) {
        signature_pool_name = cmd_tokens[2];
    } else {
        signature_pool_name = "/signatures";
    }
    create_object_pool<PersistentCascadeStoreWithStringKey>(client, storage_pool_name, 0);
    create_object_pool<SignatureCascadeStoreWithStringKey>(client, signature_pool_name, 0);
    return true;
}

/**
 * Command that loads the public key for the CascadeChain service from a file into
 * the client's memory.
 *
 * Expected arguments: <filename>
 *  filename: A path (relative or absolute) to a PEM file containing the service's public key
 */
bool load_service_key(ServiceClientAPI& client, const std::vector<std::string>& cmd_tokens) {
    if(cmd_tokens.size() < 2) {
        print_red("Invalid command format. Please try help " + cmd_tokens[0] + ".");
        return false;
    }
    service_verifier = std::make_unique<openssl::Verifier>(openssl::EnvelopeKey::from_pem_public(cmd_tokens[1]),
                                                           openssl::DigestAlgorithm::SHA256);
    return true;
}

/**
 * Command that puts a string object into CascadeChain and retrieves its corresponding signature.
 *
 * Expected arguments: <key-suffix> <value-string>
 *  key-suffix: The key identifying the object, without any object-pool prefix.
 *              The object-pool prefix will be added automatically based on the configured
 *              storage and signatures pools.
 *  value-string: A string that will be used as the "value" for the object (converted to bytes).
 */
bool put_with_signature(ServiceClientAPI& client, const std::vector<std::string>& cmd_tokens) {
    if(cmd_tokens.size() < 3) {
        print_red("Invalid command format. Please try help " + cmd_tokens[0] + ".");
        return false;
    }
    std::string key_suffix = cmd_tokens[1];
    //Step 1: Put the object into the storage pool
    ObjectWithStringKey obj;
    obj.key = storage_pool_name + delimiter + key_suffix;
    obj.blob = Blob(cmd_tokens[2].c_str(), cmd_tokens[2].length());
    derecho::rpc::QueryResults<std::tuple<persistent::version_t, uint64_t>> put_result = client.put(obj);
    //The reply map will only have 1 entry
    auto put_reply = put_result.get().begin()->second.get();
    std::cout << "Node " << put_result.get().begin()->first << " finished putting the object, replied with version:"
              << std::get<0>(put_reply) << ", ts_us:" << std::get<1>(put_reply) << std::endl;
    ObjectSignature signature_record;
    signature_record.key_suffix = key_suffix;
    signature_record.object_version = std::get<0>(put_reply);
    auto previous_record_iter = cached_signatures_by_key.find(key_suffix);
    if(previous_record_iter != cached_signatures_by_key.end()) {
        //The previous version of the object is the highest version number in our signature cache
        signature_record.object_previous_version = previous_record_iter->second.rbegin()->first;
    } else {
        signature_record.object_previous_version = persistent::INVALID_VERSION;
    }
    //Step 2: Wait a little bit for the signature to be finished
    //TODO: replace this with a more explicit check or callback
    std::this_thread::sleep_for(std::chrono::seconds(1));
    //Step 3: Get the hash object for the "latest version" of the key, and figure out what version got assigned to it
    std::string signature_key = signature_pool_name + delimiter + key_suffix;
    derecho::rpc::QueryResults<const ObjectWithStringKey> get_result = client.get(signature_key);
    ObjectWithStringKey hash_object = get_result.get().begin()->second.get();
    std::cout << "Node " << get_result.get().begin()->first << " reports that the latest version of the hash for "
              << key_suffix << " is " << hash_object.get_version() << std::endl;
    //Save the hash, so we don't have to compute it ourselves in order to verify the signature
    signature_record.object_hash = hash_object.blob;
    signature_record.signature_version = hash_object.get_version();
    //Step 4: Get the signature for this version. Query by version to avoid races with other clients.
    auto signature_result = client.get_signature(signature_key, signature_record.signature_version);
    std::tuple<std::vector<uint8_t>, persistent::version_t> signature_reply = signature_result.get().begin()->second.get();
    std::cout << "Node " << signature_result.get().begin()->first << " replied with signature=" << std::get<0>(signature_reply)
              << " and previous_signed_version=" << std::get<1>(signature_reply) << std::endl;
    signature_record.signature = std::get<0>(signature_reply);
    signature_record.signature_previous_version = std::get<1>(signature_reply);
    //Now store the entire signature record in the cache
    cached_signatures_by_key[signature_record.key_suffix].emplace(signature_record.object_version, signature_record);
    return true;
}

/**
 * Command that verifies the signature received from the primary site on a particular key,
 * assuming the client's cache contains a signature for both the requested version and the
 * previous version of that key.
 *
 * Expected arguments: <key-suffix> [version]
 *  key-suffix: The key identifying the object, without any object-pool prefix.
 *  version: Optionally, the version to verify. Defaults to the current version.
 */
bool verify_primary_signature(ServiceClientAPI& client, const std::vector<std::string>& cmd_tokens) {
    if(cmd_tokens.size() < 2) {
        print_red("Invalid command format. Please try help " + cmd_tokens[0] + ".");
        return false;
    }
    std::string key_suffix = cmd_tokens[1];
    if(cached_signatures_by_key.find(key_suffix) == cached_signatures_by_key.end()) {
        print_red("Key " + key_suffix + " has no cached signatures to verify.");
        return false;
    }
    if(!service_verifier) {
        print_red("Service's public key has not been loaded. Cannot verify.");
        return false;
    }
    persistent::version_t verify_version;
    if(cmd_tokens.size() >= 3) {
        verify_version = static_cast<persistent::version_t>(std::stol(cmd_tokens[2]));
    } else {
        verify_version = cached_signatures_by_key[key_suffix].rbegin()->first;
    }
    persistent::version_t prev_object_version = cached_signatures_by_key[key_suffix][verify_version].object_previous_version;
    // A signature for version X of an object should have signed bytes in this order:
    // 1. Hash of the object at version X
    // 2. Signature for version X-1 of the object
    // Note that the signature for version X-1 of the object will be stored in SignatureCascadeStore
    // at a different version Y-1, but it's in our cache at the object's version, not the signature's version
    service_verifier->init();
    service_verifier->add_bytes(cached_signatures_by_key[key_suffix][verify_version].object_hash.bytes,
                                cached_signatures_by_key[key_suffix][verify_version].object_hash.size);
    service_verifier->add_bytes(cached_signatures_by_key[key_suffix][prev_object_version].signature.data(),
                                cached_signatures_by_key[key_suffix][prev_object_version].signature.size());
    bool verified = service_verifier->finalize(cached_signatures_by_key[key_suffix][verify_version].signature);
    if(verified) {
        std::cout << "Key " << key_suffix << " has a valid signature on version " << verify_version << " with previous version " << prev_object_version << std::endl;
    } else {
        print_red("Key " + key_suffix + " had an invalid signature on version " + std::to_string(verify_version));
    }
    return verified;
}

/* -------- Command-line interface functions copied from client.cpp ------- */

using command_handler_t = std::function<bool(ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens)>;
struct command_entry_t {
    const std::string cmd;            // command name
    const std::string desc;           // help info
    const std::string help;           // full help
    const command_handler_t handler;  // handler
};

void list_commands(const std::vector<command_entry_t>& command_list) {
    for(const auto& entry : command_list) {
        if(entry.handler) {
            std::cout << std::left << std::setw(32) << entry.cmd << "- " << entry.desc << std::endl;
        } else {
            print_cyan("# " + entry.cmd + " #");
        }
    }
}

ssize_t find_command(const std::vector<command_entry_t>& command_list, const std::string& command) {
    ssize_t pos = 0;
    for(; pos < static_cast<ssize_t>(command_list.size()); pos++) {
        if(command_list.at(pos).cmd == command) {
            break;
        }
    }
    if(pos == static_cast<ssize_t>(command_list.size())) {
        pos = -1;
    }
    return pos;
}

bool shell_is_active = true;
#define SUBGROUP_TYPE_LIST "VCSS|PCSS|SCSS|TCSS"
#define SHARD_MEMBER_SELECTION_POLICY_LIST "FirstMember|LastMember|Random|FixedRandom|RoundRobin|UserSpecified"

std::vector<command_entry_t> commands = {
        {"General Commands", "", "", command_handler_t()},
        {"help",
         "Print help info",
         "help [command name]",
         [](ServiceClientAPI&, const std::vector<std::string>& cmd_tokens) {
             if(cmd_tokens.size() >= 2) {
                 ssize_t command_index = find_command(commands, cmd_tokens[1]);
                 if(command_index < 0) {
                     print_red("unknown command:'" + cmd_tokens[1] + "'.");
                 } else {
                     std::cout << commands.at(command_index).help << std::endl;
                 }
                 return (command_index >= 0);
             } else {
                 list_commands(commands);
                 return true;
             }
         }},
        {"quit",
         "Exit",
         "quit",
         [](ServiceClientAPI&, const std::vector<std::string>& cmd_tokens) {
             shell_is_active = false;
             return true;
         }},
        {"load_service_key",
         "Load the CascadeChain service's public key from a PEM file",
         "load_service_key <filename>",
         &load_service_key},
        {"Object Pool Manipulation Commands", "", "", command_handler_t()},
        {"list_object_pools",
         "List existing object pools",
         "list_object_pools",
         [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
             std::cout << "refreshed object pools:" << std::endl;
             for(std::string& opath : capi.list_object_pools(true)) {
                 std::cout << "\t" << opath << std::endl;
             }
             return true;
         }},
        {"setup_object_pools",
         "Create the object pools needed for CascadeChain",
         "setup_object_pools [storage-pool-name] [signature-pool-name]",
         &setup_object_pools},
        {"get_object_pool",
         "Get details of an object pool",
         "get_object_pool <path>",
         [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
             if(cmd_tokens.size() < 2) {
                 print_red("Invalid command format. Please try help " + cmd_tokens[0] + ".");
                 return false;
             }
             auto opm = capi.find_object_pool(cmd_tokens[1]);
             std::cout << "get_object_pool returns:"
                       << opm << std::endl;
             return true;
         }},
        {"Object Manipulation Commands", "", "", command_handler_t()},
        {"put_with_signature",
         "Put an object into CascadeChain and cache its signature",
         "put_with_signature <key-suffix> <value-string>\n"
         "Note: key-suffix should not include an object pool path; the object pool will be chosen automatically",
         &put_with_signature},
        {"op_remove",
         "Remove an object from an object pool.",
         "op_remove <key>\n"
         "Please note that cascade automatically decides the object pool path using the key's prefix.",
         [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
             if(cmd_tokens.size() < 2) {
                 print_red("Invalid command format. Please try help " + cmd_tokens[0] + ".");
                 return false;
             }
             op_remove(capi, cmd_tokens[1]);
             return true;
         }},
        {"op_get",
         "Get an object from an object pool (by version).",
         "op_get <key> [ version(default:current version) ]\n"
         "Please note that cascade automatically decides the object pool path using the key's prefix.",
         [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
             if(cmd_tokens.size() < 2) {
                 print_red("Invalid command format. Please try help " + cmd_tokens[0] + ".");
                 return false;
             }
             persistent::version_t version = CURRENT_VERSION;
             if(cmd_tokens.size() >= 3) {
                 version = static_cast<persistent::version_t>(std::stol(cmd_tokens[2]));
             }
             auto res = capi.get(cmd_tokens[1], version);
             check_get_result(res);
             return true;
         }},
        {"op_get_signature",
         "Get an object's signature from the object pool (by version).",
         "op_get_signature <key> [ version(default:current version) ]\n"
         "Note that Cascade will automatically decide the subgroup to contact based on the key's prefix, "
         "but only object pools hosted on a SignatureCascadeStore subgroup will have signatures.",
         [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
             if(cmd_tokens.size() < 2) {
                 print_red("Invalid command format. Please try help " + cmd_tokens[0] + ".");
                 return false;
             }
             persistent::version_t version = CURRENT_VERSION;
             if(cmd_tokens.size() >= 3) {
                 version = static_cast<persistent::version_t>(std::stol(cmd_tokens[2]));
             }
             auto query_result = capi.get_signature(cmd_tokens[1], version);
             //std::tuple doesn't have an operator<<, so I have to customize check_get_result here
             for(auto& reply_future : query_result.get()) {
                 auto reply = reply_future.second.get();
                 std::cout << "node(" << reply_future.first << ") replied with value: (" << std::get<0>(reply) << "," << std::get<1>(reply) << ")" << std::endl;
             }
             return true;
         }},
        {"op_list_keys",
         "list the object keys in an object pool (by version).",
         "op_list_keys <object pool pathname> [ version(default:current version) ]\n",
         [](ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
             if(cmd_tokens.size() < 2) {
                 print_red("Invalid command format. Please try help " + cmd_tokens[0] + ".");
                 return false;
             }
             persistent::version_t version = CURRENT_VERSION;
             if(cmd_tokens.size() >= 3) {
                 version = static_cast<persistent::version_t>(std::stol(cmd_tokens[2]));
             }
             auto result = capi.list_keys(version, cmd_tokens[1]);
             check_op_list_keys_result(capi.wait_list_keys(result));
             return true;
         }},
        {"verify_primary_signature",
         "Verify the cached signature on a specific version of an object",
         "verify_primary_signature [version(default:current version)]",
         &verify_primary_signature}};

inline void do_command(ServiceClientAPI& capi, const std::vector<std::string>& cmd_tokens) {
    try {
        ssize_t command_index = find_command(commands, cmd_tokens[0]);
        if(command_index >= 0) {
            if(commands.at(command_index).handler(capi, cmd_tokens)) {
                std::cout << "-> Succeeded." << std::endl;
            } else {
                std::cout << "-> Failed." << std::endl;
            }
        } else {
            print_red("unknown command:" + cmd_tokens[0]);
        }
    } catch(const derecho::derecho_exception& ex) {
        print_red(std::string("Exception:") + ex.what());
    } catch(...) {
        print_red("Unknown exception caught.");
    }
}

void interactive_test(ServiceClientAPI& capi) {
    // loop
    while(shell_is_active) {
        char* malloced_cmd = readline("cmd> ");
        std::string cmdline(malloced_cmd);
        free(malloced_cmd);
        if(cmdline == "") continue;
        add_history(cmdline.c_str());

        std::string delimiter = " ";
        do_command(capi, tokenize(cmdline, delimiter.c_str()));
    }
    std::cout << "Client exits." << std::endl;
}

void detached_test(ServiceClientAPI& capi, int argc, char** argv) {
    std::vector<std::string> cmd_tokens;
    for(int i = 1; i < argc; i++) {
        cmd_tokens.emplace_back(argv[i]);
    }
    do_command(capi, cmd_tokens);
}

int main(int argc, char** argv) {
    if(prctl(PR_SET_NAME, PROC_NAME, 0, 0, 0) != 0) {
        dbg_default_debug("Failed to set proc name to {}.", PROC_NAME);
    }
    ServiceClientAPI capi;
    if(argc == 1) {
        // by default, we use the interactive shell.
        interactive_test(capi);
    } else {
        detached_test(capi, argc, argv);
    }
    return 0;
}