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
    std::vector<uint8_t> local_hash;
    ObjectWithStringKey hash_object;
    std::vector<uint8_t> signature;
};

const std::string delimiter = "/";
//State variables shared among all the commands. Maybe it would be better to use an object here.
std::string storage_pool_name;
std::string signature_pool_name;
// For each key (suffix), contains a map from object version -> signature for that version
std::map<std::string, std::map<persistent::version_t, std::shared_ptr<ObjectSignature>>> cached_signatures_by_key;
// A map containing the same signature records, indexed by signature version instead of object
std::map<persistent::version_t, std::shared_ptr<ObjectSignature>> cached_signatures_by_version;
//Verifier for the service's public key. Initialized after startup by a command.
std::unique_ptr<openssl::Verifier> service_verifier;

/**
 * Command that configures both the client and the servers with the two object pools
 * needed to run the CascadeChain service. It must be run before running any other client
 * commands, to set the values of storage_pool_name and signature_pool_name. It also queries
 * the servers to see if the object pools exist and, if not, it creates them. Right now it
 * assumes there will only be a single PCSS subgroup and a single SCSS subgroup, so it
 * creates both object pools on subgroup index 0.
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
    auto storage_opm = client.find_object_pool(storage_pool_name);
    //find_object_pool returns ObjectPoolMetadata::IV if the object pool cannot be found
    if(!storage_opm.is_valid()) {
        create_object_pool<PersistentCascadeStoreWithStringKey>(client, storage_pool_name, 0);
    }
    auto signatures_opm = client.find_object_pool(signature_pool_name);
    if(!signatures_opm.is_valid()) {
        create_object_pool<SignatureCascadeStoreWithStringKey>(client, signature_pool_name, 0);
    }
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
 * Helper function that computes the SHA256 hash of an ObjectWithStringKey
 * in the same way that sha_hash_udl would compute it.
 */
std::vector<uint8_t> compute_hash(const ObjectWithStringKey& data_obj) {
    openssl::Hasher object_hasher(openssl::DigestAlgorithm::SHA256);
    std::vector<uint8_t> hash(object_hasher.get_hash_size());
    object_hasher.init();
    object_hasher.add_bytes(&data_obj.version, sizeof(persistent::version_t));
    object_hasher.add_bytes(&data_obj.timestamp_us, sizeof(uint64_t));
    object_hasher.add_bytes(&data_obj.previous_version, sizeof(persistent::version_t));
    object_hasher.add_bytes(&data_obj.previous_version_by_key, sizeof(persistent::version_t));
    object_hasher.add_bytes(data_obj.key.data(), data_obj.key.size());
    object_hasher.add_bytes(data_obj.blob.bytes, data_obj.blob.size);
    object_hasher.finalize(hash.data());
    return hash;
}

/**
 * Helper function that verifies a chained signature on a hash of an object.
 * The hash must itself be stored in an ObjectWithStringKey, whose headers are
 * populated by the SignatureCascadeStore, in order to match signatures generated
 * by the SignatureCascadeStore.
 */
bool verify_object_signature(const ObjectWithStringKey& hash, const std::vector<uint8_t>& signature, const std::vector<uint8_t>& previous_signature) {
    if(!service_verifier) {
        print_red("Service's public key has not been loaded. Cannot verify.");
        return false;
    }
    service_verifier->init();
    // Because DeltaCascadeStoreCore stores the hashes in deltas (which are just ObjectWithStringKeys),
    // the data that PersistentRegistry ends up signing is actually the to_bytes serialization of the entire
    // ObjectWithStringKey object, not just the hash.
    std::size_t hash_object_size = mutils::bytes_size(hash);
    char bytes_of_hash_object[hash_object_size];
    mutils::to_bytes(hash, bytes_of_hash_object);
    service_verifier->add_bytes(bytes_of_hash_object,
                                hash_object_size);
    service_verifier->add_bytes(previous_signature.data(),
                                previous_signature.size());
    return service_verifier->finalize(signature);
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
              << std::hex << std::get<0>(put_reply) << std::dec << ", ts_us:" << std::get<1>(put_reply) << std::endl;
    std::shared_ptr<ObjectSignature> signature_record = std::make_shared<ObjectSignature>();
    signature_record->key_suffix = key_suffix;
    signature_record->object_version = std::get<0>(put_reply);
    auto previous_record_iter = cached_signatures_by_key.find(key_suffix);
    if(previous_record_iter != cached_signatures_by_key.end()) {
        //The previous version of the object is the highest version number in our signature cache
        signature_record->object_previous_version = previous_record_iter->second.rbegin()->first;
    } else {
        signature_record->object_previous_version = persistent::INVALID_VERSION;
    }
    //Also store these fields in our local copy of the object
    obj.version = signature_record->object_version;
    obj.previous_version_by_key = signature_record->object_previous_version;
    obj.timestamp_us = std::get<1>(put_reply);
    //Step 2: Wait a little bit for the signature to be finished, then retreive it from the signature group
    //TODO: replace this with a notification sent from SignatureCascadeStore to the client
    std::this_thread::sleep_for(std::chrono::seconds(2));
    std::string signature_key = signature_pool_name + delimiter + key_suffix;
    //Specify the desired version of the data object; SignatureCascadeStore will look up the corresponding hash object
    auto signature_result = client.get_signature(signature_key, signature_record->object_version);
    std::tuple<std::vector<uint8_t>, persistent::version_t> signature_reply = signature_result.get().begin()->second.get();
    std::cout << "Node " << signature_result.get().begin()->first << " replied with signature=" << std::hex << std::get<0>(signature_reply)
              << " and previous_signed_version=" << std::get<1>(signature_reply) << std::dec << std::endl;
    signature_record->signature = std::get<0>(signature_reply);
    signature_record->signature_previous_version = std::get<1>(signature_reply);

    //Step 3: Get the hash object that corresponds to the data object, to find out what timestamp and previous_version_by_key it got
    //This is necessary because the signature includes the entire ObjectWithStringKey, not just the hash blob
    //If it was possible to fill in all the headers (including timestamp, etc), we could construct this object locally
    auto hash_get_result = client.get(signature_key, signature_record->object_version);
    ObjectWithStringKey hash_object = hash_get_result.get().begin()->second.get();
    persistent::version_t signature_version = hash_object.get_version();
    std::cout << "Got the hash object for data version " << std::hex << signature_record->object_version << std::dec << " from node "
              << hash_get_result.get().begin()->first << " and its version is " << std::hex << signature_version << std::dec << std::endl;
    signature_record->signature_version = signature_version;
    //Step 4: Retrieve the same object we just put, to find out what its previous_version is
    //This is wasteful, but it's the only way to fill in all the headers of the object so we can hash it locally
    auto get_result = client.get(obj.key, obj.version);
    ObjectWithStringKey stored_object = get_result.get().begin()->second.get();
    obj.previous_version = stored_object.previous_version;
    std::cout << "Got the object back from node " << get_result.get().begin()->first << " and its previous_version is "
              << std::hex << stored_object.previous_version << std::dec << std::endl;
    if(stored_object.previous_version_by_key != obj.previous_version_by_key) {
        std::cout << "Oh no! The local cache did not have the right previous_version_by_key! We thought it was " << std::hex << obj.previous_version_by_key
                  << " but Cascade recorded it as " << stored_object.previous_version_by_key << std::dec << std::endl;
        obj.previous_version_by_key = stored_object.previous_version_by_key;
        signature_record->object_previous_version = stored_object.previous_version_by_key;
    }
    //Step 6: Compute the hash of the object locally, since we don't trust the service
    std::vector<uint8_t> hash = compute_hash(obj);
    if(memcmp(hash_object.blob.bytes, hash.data(), hash.size() != 0)) {
        print_red("Object hash stored in Cascade does not match object hash computed locally!");
        return false;
    }
    signature_record->local_hash = std::move(hash);
    //Step 7: Validate the service's signature on this version of the object
    std::vector<uint8_t> prev_signature(signature_record->signature.size());
    if(signature_record->signature_previous_version != INVALID_VERSION) {
        //The service's signature includes the previous signature in the *log*, not the previous signature on *this object*
        auto signature_find_result = cached_signatures_by_version.find(signature_record->signature_previous_version);
        if(signature_find_result == cached_signatures_by_version.end()) {
            //This is why the service needs to send a message back to the client containing the signature for the previous log entry once a put is complete
            std::cout << "Previous signature on version " << std::hex << signature_record->signature_previous_version << std::dec << " is not in the cache, retrieving it" << std::endl;
            auto prev_signature_result = client.get_signature_by_version(signature_key, signature_record->signature_previous_version);
            prev_signature = std::get<0>(prev_signature_result.get().begin()->second.get());
            //It would be nice if we could put this signature in the cache, but that would require constructing a whole ObjectSignature record
        } else {
            prev_signature = signature_find_result->second->signature;
        }
    }
    bool validated = verify_object_signature(hash_object, signature_record->signature, prev_signature);
    if(validated) {
        std::cout << "Signature is valid" << std::endl;
    } else {
        print_red("Signature is invalid!");
        return false;
    }
    signature_record->hash_object = std::move(hash_object);
    //Now store the entire signature record in the cache
    cached_signatures_by_key[signature_record->key_suffix].emplace(signature_record->object_version, signature_record);
    cached_signatures_by_version.emplace(signature_record->signature_version, signature_record);
    return true;
}

/**
 * Command to manually add a signature (and hash) to the client's cache, for
 * recovering when the put_with_signature command fails or restarting a client
 * with no in-memory state.
 *
 * Expected arguments: <key-suffix> <object-version>
 */
bool cache_signature(ServiceClientAPI& client, const std::vector<std::string>& cmd_tokens) {
    if(cmd_tokens.size() < 3) {
        print_red("Invalid command format. Please try help " + cmd_tokens[0] + ".");
        return false;
    }
    std::string key_suffix = cmd_tokens[1];
    persistent::version_t object_version = std::stol(cmd_tokens[2]);
    if(cached_signatures_by_key[key_suffix][object_version] == nullptr) {
        cached_signatures_by_key[key_suffix][object_version] = std::make_shared<ObjectSignature>();
        cached_signatures_by_key[key_suffix][object_version]->key_suffix = key_suffix;
        cached_signatures_by_key[key_suffix][object_version]->object_version = object_version;
        //But how do we initialized object_previous_version?
    }
    //Get the hash - we can't compute the hash locally without knowing the object data
    auto hash_get_result = client.get(signature_pool_name + delimiter + key_suffix, object_version);
    ObjectWithStringKey hash_object = hash_get_result.get().begin()->second.get();
    std::cout << "Got a hash object for data version " << std::hex << object_version << "; its version is " << hash_object.get_version() << std::dec << std::endl;
    cached_signatures_by_key[key_suffix][object_version]->signature_version = hash_object.get_version();
    cached_signatures_by_key[key_suffix][object_version]->hash_object = std::move(hash_object);
    //Get the signature
    auto signature_get_result = client.get_signature(signature_pool_name + delimiter + key_suffix, object_version);
    std::tuple<std::vector<uint8_t>, persistent::version_t> signature_reply = signature_get_result.get().begin()->second.get();
    std::cout << "Node " << signature_get_result.get().begin()->first << " replied with signature=" << std::hex << std::get<0>(signature_reply)
              << " and previous_signed_version=" << std::get<1>(signature_reply) << std::dec << std::endl;
    cached_signatures_by_key[key_suffix][object_version]->signature = std::get<0>(signature_reply);
    cached_signatures_by_key[key_suffix][object_version]->signature_previous_version = std::get<1>(signature_reply);
    //Copy the pointer to this cache entry into the by-signature-version index
    cached_signatures_by_version[hash_object.get_version()] = cached_signatures_by_key[key_suffix][object_version];
    return true;
}

/**
 * Command that verifies the signature received from the primary site on a particular key,
 * assuming the client's cache contains a signature for both the requested version and the
 * previous version of that key.
 *
 * Expected arguments: <key-suffix> [version]
 *  key-suffix: The key identifying the object, without any object-pool prefix.
 *  version: Optionally, the version to verify. Defaults to the latest cached version.
 */
bool verify_cached_signature(ServiceClientAPI& client, const std::vector<std::string>& cmd_tokens) {
    if(cmd_tokens.size() < 2) {
        print_red("Invalid command format. Please try help " + cmd_tokens[0] + ".");
        return false;
    }
    std::string key_suffix = cmd_tokens[1];
    if(cached_signatures_by_key.find(key_suffix) == cached_signatures_by_key.end()) {
        print_red("Key " + key_suffix + " has no cached signatures to verify.");
        return false;
    }
    persistent::version_t verify_version;
    if(cmd_tokens.size() >= 3) {
        verify_version = static_cast<persistent::version_t>(std::stol(cmd_tokens[2]));
        if(cached_signatures_by_key[key_suffix].find(verify_version) == cached_signatures_by_key[key_suffix].end()) {
            print_red("No signature in the cache for version " + std::to_string(verify_version));
            return false;
        }
    } else {
        verify_version = cached_signatures_by_key[key_suffix].rbegin()->first;
    }
    persistent::version_t previous_signature_version = cached_signatures_by_key[key_suffix].at(verify_version)->signature_previous_version;
    //Debug output:
    std::cout << "Object " << key_suffix << " at version " << verify_version << " has previous version "
              << cached_signatures_by_key[key_suffix][verify_version]->object_previous_version
              << ". Its corresponding signature version is " << cached_signatures_by_key[key_suffix][verify_version]->signature_version
              << " and the previous signature version is " << previous_signature_version << std::endl;
    // A signature for version X of an object should have signed bytes in this order:
    // 1. Headers of the ObjectWithStringKey containing the hash of the object at version X, which has its own version Y
    // 2. Hash of the object at version X
    // 3. Signature for the previous log entry (delta right before X), which may be a different object
    std::vector<uint8_t> previous_signature;
    auto signature_find_iter = cached_signatures_by_version.find(previous_signature_version);
    if(signature_find_iter == cached_signatures_by_version.end()) {
        std::cout << "Previous signature on version " << previous_signature_version << " is not in the cache, retrieving it" << std::endl;
        auto prev_signature_result = client.get_signature_by_version(signature_pool_name + delimiter + key_suffix, previous_signature_version);
        previous_signature = std::get<0>(prev_signature_result.get().begin()->second.get());
    } else {
        previous_signature = signature_find_iter->second->signature;
    }
    bool verified = verify_object_signature(cached_signatures_by_key[key_suffix].at(verify_version)->hash_object,
                                            cached_signatures_by_key[key_suffix].at(verify_version)->signature,
                                            previous_signature);
    if(verified) {
        std::cout << "Key " << key_suffix << " has a valid signature on version " << verify_version
                  << " with previous signature version " << previous_signature_version << std::endl;
    } else {
        print_red("Key " + key_suffix + " had an invalid signature on version " + std::to_string(verify_version));
    }
    return verified;
}

/**
 * Command that retrieves the signature for a key at a particular version and verifies
 * it using the service's public key and the signature for the previous version.
 *
 * Expected arguments: <key-suffix> [object-version]
 *  key-suffix: The key identifying an object, without any object-pool prefix
 *  signature-version: The version of the object to get a signature for.
 *                     Defaults to the current version if omitted.
 */
bool get_and_verify_signature(ServiceClientAPI& client, const std::vector<std::string>& cmd_tokens) {
    if(cmd_tokens.size() < 2) {
        print_red("Invalid command format. Please try help " + cmd_tokens[0] + ".");
        return false;
    }
    std::string key_suffix = cmd_tokens[1];
    persistent::version_t object_version;
    if(cmd_tokens.size() >= 3) {
        object_version = static_cast<persistent::version_t>(std::stol(cmd_tokens[2]));
    } else {
        object_version = CURRENT_VERSION;
    }
    auto hash_query_result = client.get(signature_pool_name + delimiter + key_suffix, object_version);
    ObjectWithStringKey hash_object = hash_query_result.get().begin()->second.get();
    auto sig_query_result = client.get_signature(signature_pool_name + delimiter + key_suffix, object_version);
    const auto [signature, previous_signature_version] = sig_query_result.get().begin()->second.get();
    //Get the previous signature, which may or may not relate to this key (but will be in the same object pool)
    auto prev_sig_query_result = client.get_signature_by_version(signature_pool_name + delimiter + key_suffix, previous_signature_version);
    const auto [prev_signature, prev_prev_version] = prev_sig_query_result.get().begin()->second.get();
    bool verified = verify_object_signature(hash_object, signature, prev_signature);
    if(verified) {
        std::cout << "Key " << key_suffix << " has a valid signature on version " << object_version
                  << " with previous signature version " << previous_signature_version << std::endl;
    } else {
        print_red("Key " + key_suffix + " had an invalid signature on version " + std::to_string(object_version));
    }
    return verified;
}

/**
 * Command that gets an object from the storage pool and its corresponding signature
 * from the signature pool, then verifies the signature on the hash of the object.
 *
 * Expected arguments: <key-suffix> [object-version]
 *  key-suffix: The key identifying an object, without any object-pool prefix
 *  signature-version: The version of the object to get a signature for.
 *                     Defaults to the current version if omitted.
 */
bool get_and_verify_object(ServiceClientAPI& client, const std::vector<std::string>& cmd_tokens) {
    if(cmd_tokens.size() < 2) {
        print_red("Invalid command format. Please try help " + cmd_tokens[0] + ".");
        return false;
    }
    std::string key_suffix = cmd_tokens[1];
    persistent::version_t object_version;
    if(cmd_tokens.size() >= 3) {
        object_version = static_cast<persistent::version_t>(std::stol(cmd_tokens[2]));
    } else {
        object_version = CURRENT_VERSION;
    }
    //Get the object
    auto get_result = client.get(storage_pool_name + delimiter + key_suffix, object_version);
    check_get_result(get_result);
    ObjectWithStringKey stored_object = get_result.get().begin()->second.get();
    //Compute the hash of the object locally, since we don't trust the service
    std::vector<uint8_t> hash;
    auto cache_search_iter = cached_signatures_by_key[key_suffix].find(object_version);
    if(cache_search_iter != cached_signatures_by_key[key_suffix].end()) {
        hash = cache_search_iter->second->local_hash;
    } else {
        hash = compute_hash(stored_object);
    }
    //Get the hash from the service, because we need to know its header fields to compute the signature
    auto hash_query_result = client.get(signature_pool_name + delimiter + key_suffix, object_version);
    ObjectWithStringKey hash_object = hash_query_result.get().begin()->second.get();
    if(memcmp(hash_object.blob.bytes, hash.data(), hash.size() != 0)) {
        print_red("Object hash stored in Cascade does not match object hash computed locally!");
        return false;
    }
    auto sig_query_result = client.get_signature(signature_pool_name + delimiter + key_suffix, object_version);
    const auto [signature, previous_signature_version] = sig_query_result.get().begin()->second.get();
    //Get the previous signature, which may or may not relate to this key (but will be in the same object pool)
    auto prev_sig_query_result = client.get_signature_by_version(signature_pool_name + delimiter + key_suffix, previous_signature_version);
    const auto [prev_signature, prev_prev_version] = prev_sig_query_result.get().begin()->second.get();
    bool verified = verify_object_signature(hash_object, signature, prev_signature);
    if(verified) {
        std::cout << "Object has a valid signature on version " << object_version
                  << " with previous signature version " << previous_signature_version << std::endl;
    } else {
        print_red("Object has an invalid signature on version " + std::to_string(object_version));
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
         "Put an object into CascadeChain, then verify and cache its signature",
         "put_with_signature <key-suffix> <value-string>\n"
         "Note: key-suffix should not include an object pool path; the object pool will be chosen automatically",
         &put_with_signature},
        {"get_and_verify",
         "Get an object and its signature from CascadeChain, then verify the signature",
         "get_and_verify <key-suffix> [version(default:current version)]",
         &get_and_verify_object},
        {"cache_signature",
         "Retrieve and cache a signature for a particular version of an object",
         "cache_signature <key-suffix> [version(default:current version)]\n"
         "Note: key-suffix should not include an object pool path; the object pool will be chosen automatically",
         &cache_signature},
        {"get_and_verify_signature",
         "Retrieve and verify a signature for a particular version of an object",
         "get_and_verify_signature <key-suffix> [version(default:current version)]\n"
         "Note: key-suffix should not include an object pool path; the object pool will be chosen automatically",
         &get_and_verify_signature},
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
        {"verify_cached_signature",
         "Verify the cached signature on a specific version of an object",
         "verify_cached_signature <key-suffix> [version(default:current version)]",
         &verify_cached_signature}};

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
