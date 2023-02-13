
#include <cascade/service_client_api.hpp>
#include <cascade/utils.hpp>

#include <derecho/openssl/hash.hpp>
#include <derecho/openssl/signature.hpp>

#include <fstream>
#include <iomanip>
#include <iostream>
#include <readline/history.h>
#include <readline/readline.h>
#include <set>
#include <string>
#include <sys/prctl.h>
#include <tuple>
#include <typeindex>

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
                  << ",previous_version:" << std::get<1>(reply)                                       \
                  << ",previous_version_by_key:" << std::get<2>(reply)                                \
                  << ",ts_us:" << std::get<3>(reply) << std::endl;                                    \
    }

void op_put(ServiceClientAPI& capi, const std::string& key, const std::string& value, persistent::version_t pver, persistent::version_t pver_bk) {
    ObjectWithStringKey obj;
    obj.key = key;
    obj.previous_version = pver;
    obj.previous_version_by_key = pver_bk;
    obj.blob = Blob(reinterpret_cast<const uint8_t*>(value.c_str()), value.length());
    derecho::rpc::QueryResults<derecho::cascade::version_tuple> result = capi.put(obj);
    check_put_and_remove_result(result);
}

void op_put_and_forget(ServiceClientAPI& capi, const std::string& key, const std::string& value, persistent::version_t pver, persistent::version_t pver_bk) {
    ObjectWithStringKey obj;
    obj.key = key;
    obj.previous_version = pver;
    obj.previous_version_by_key = pver_bk;
    obj.blob = Blob(reinterpret_cast<const uint8_t*>(value.c_str()), value.length());
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
    obj.blob = Blob(reinterpret_cast<const uint8_t*>(value.c_str()), value.length());
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

/** The delimiter character used between sections of an object-pool path */
const std::string op_delimiter = "/";

// Data the client needs to store for each signature on an object it submitted
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
// A function that receives the body of a signature notification message as its arguments
// Message format: data version, hash version, signature data, previous signed version, previous signature
using signature_callback_t = std::function<void(persistent::version_t, persistent::version_t,
                                                const std::vector<uint8_t>&, persistent::version_t,
                                                const std::vector<uint8_t>&)>;
/**
 * A functor that will be registered as the signature notification handler
 * for the "signatures/" object pool, and helps notify the put_with_signature()
 * command when a particular version has been signed
 */
class SignatureNotificationHandler {
private:
    std::map<persistent::version_t, signature_callback_t> callbacks_by_version;

public:
    void operator()(const Blob& message_body) {
        // Peek at the data version, which is the first element in the message
        persistent::version_t data_object_version;
        std::memcpy(&data_object_version, message_body.bytes, sizeof(data_object_version));
        // If there is a callback registered for this version, call it, then delete it
        auto find_callback = callbacks_by_version.find(data_object_version);
        if(find_callback != callbacks_by_version.end()) {
            mutils::deserialize_and_run(nullptr, message_body.bytes, find_callback->second);
            callbacks_by_version.erase(find_callback);
        }
    }
    void register_callback(persistent::version_t desired_data_version, const signature_callback_t& callback) {
        callbacks_by_version.emplace(desired_data_version, callback);
    }
    SignatureNotificationHandler() = default;
    // Copying this object would mean the main thread loses the ability to register callbacks
    // There needs to be only one copy of it shared between the main thread and the Cascade notification handler
    SignatureNotificationHandler(const SignatureNotificationHandler&) = delete;
};

/**
 * This object contains all the state a CascadeChain client needs to preserve
 * between commands, such as the cache of signatures it has received from servers.
 * It also keeps a reference to the ServiceClient object that will be used to send
 * those commands, which should be the singleton created by ServiceClient::initialize().
 *
 * @tparam CascadeTypes The Cascade Type template parameters for the ServiceClient object
 */
template <typename... CascadeTypes>
class ChainClientContext {
private:
    /** The name of the object pool that has been configured to store CascadeChain objects */
    std::string storage_pool_name;
    /** The name of the object pool that has been configured to store signed hashes of the objects stored in the storage pool */
    std::string signature_pool_name;
    /** For each key (suffix), contains a map from object version -> signature for that version */
    std::map<std::string, std::map<persistent::version_t, std::shared_ptr<ObjectSignature>>> cached_signatures_by_key;
    /** A map containing the same signature records, indexed by signature version instead of object */
    std::map<persistent::version_t, std::shared_ptr<ObjectSignature>> cached_signatures_by_version;
    /** Verifier for the service's public key. Initialized after startup by a command. */
    std::unique_ptr<openssl::Verifier> service_verifier;
    /** Keys for which the client has subscribed to signature-finished notifications */
    std::set<std::string> subscribed_notification_keys;
    /** The notification handler object that will be registered with the ServiceClient */
    SignatureNotificationHandler signature_notification_handler;
    /** The ServiceClient object this CascadeChain client will use to issue commands to the service */
    ServiceClient<CascadeTypes...>& service_client;
    /**
     * Helper function that verifies a chained signature on a hash of an object.
     * The hash must itself be stored in an ObjectWithStringKey, whose headers are
     * populated by the SignatureCascadeStore, in order to match signatures generated
     * by the SignatureCascadeStore.
     */
    bool verify_object_signature(const ObjectWithStringKey& hash, const std::vector<uint8_t>& signature, const std::vector<uint8_t>& previous_signature);

public:
    /**
     * Constructs a ChainClientContext for a particular ServiceClient object.
     * The ChainClientContext will store a reference to the ServiceClient, so
     * it should be already initialized.
     *
     * @param client The ServiceClient that this ChainClientContext should use
     */
    ChainClientContext(ServiceClient<CascadeTypes...>& client);
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
    bool setup_object_pools(const std::vector<std::string>& cmd_tokens);
    /**
     * Command that loads the public key for the CascadeChain service from a file into
     * the client's memory.
     *
     * Expected arguments: <filename>
     *  filename: A path (relative or absolute) to a PEM file containing the service's public key
     */
    bool load_service_key(const std::vector<std::string>& cmd_tokens);
    /**
     * Command that puts a string object into CascadeChain and retrieves its corresponding signature.
     *
     * Expected arguments: <key-suffix> <value-string>
     *  key-suffix: The key identifying the object, without any object-pool prefix.
     *              The object-pool prefix will be added automatically based on the configured
     *              storage and signatures pools.
     *  value-string: A string that will be used as the "value" for the object (converted to bytes).
     */
    bool put_with_signature(const std::vector<std::string>& cmd_tokens);
    /**
     * Command to manually add a signature (and hash) to the client's cache, for
     * recovering when the put_with_signature command fails or restarting a client
     * with no in-memory state.
     *
     * Expected arguments: <key-suffix> <object-version>
     */
    bool cache_signature(const std::vector<std::string>& cmd_tokens);
    /**
     * Command that verifies the signature received from the primary site on a particular key,
     * assuming the client's cache contains a signature for both the requested version and the
     * previous version of that key.
     *
     * Expected arguments: <key-suffix> [version]
     *  key-suffix: The key identifying the object, without any object-pool prefix.
     *  version: Optionally, the version to verify. Defaults to the latest cached version.
     */
    bool verify_cached_signature(const std::vector<std::string>& cmd_tokens);
    /**
     * Command that retrieves the signature for a key at a particular version and verifies
     * it using the service's public key and the signature for the previous version.
     *
     * Expected arguments: <key-suffix> [object-version]
     *  key-suffix: The key identifying an object, without any object-pool prefix
     *  signature-version: The version of the object to get a signature for.
     *                     Defaults to the current version if omitted.
     */
    bool get_and_verify_signature(const std::vector<std::string>& cmd_tokens);
    /**
     * Command that gets an object from the storage pool and its corresponding signature
     * from the signature pool, then verifies the signature on the hash of the object.
     *
     * Expected arguments: <key-suffix> [object-version]
     *  key-suffix: The key identifying an object, without any object-pool prefix
     *  signature-version: The version of the object to get a signature for.
     *                     Defaults to the current version if omitted.
     */
    bool get_and_verify_object(const std::vector<std::string>& cmd_tokens);

    /**
     * Gets a reference to the ServiceClient object contained within this
     * ClientContext. This allows the caller to call client commands directly
     * instead of using the stateful CascadeChain-related commands provided
     * by ChainClientContext.
     *
     * @return A reference to a ServiceClient<CascadeTypes...>
     */
    ServiceClient<CascadeTypes...>& get_service_client();
};
using DefaultChainClientContext = ChainClientContext<VolatileCascadeStoreWithStringKey,
                                                     PersistentCascadeStoreWithStringKey,
                                                     SignatureCascadeStoreWithStringKey,
                                                     TriggerCascadeNoStoreWithStringKey>;
template <typename... CascadeTypes>
ChainClientContext<CascadeTypes...>::ChainClientContext(ServiceClient<CascadeTypes...>& client)
        : service_client(client) {}

template <typename... CascadeTypes>
ServiceClient<CascadeTypes...>& ChainClientContext<CascadeTypes...>::get_service_client() {
    return service_client;
}

template <typename... CascadeTypes>
bool ChainClientContext<CascadeTypes...>::setup_object_pools(const std::vector<std::string>& cmd_tokens) {
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
    auto storage_opm = service_client.find_object_pool(storage_pool_name);
    // find_object_pool returns ObjectPoolMetadata::IV if the object pool cannot be found
    if(!storage_opm.is_valid()) {
        create_object_pool<PersistentCascadeStoreWithStringKey>(service_client, storage_pool_name, 0);
    }
    auto signatures_opm = service_client.find_object_pool(signature_pool_name);
    if(!signatures_opm.is_valid()) {
        create_object_pool<SignatureCascadeStoreWithStringKey>(service_client, signature_pool_name, 0);
    }
    // Once the object pool exists, register the signature notification handler for it
    // The functor must be wrapped in a lambda that uses it by reference, otherwise std::function makes a copy of it
    service_client.register_signature_notification_handler([&](const Blob& message) { signature_notification_handler(message); },
                                                           signature_pool_name);
    return true;
}

template <typename... CascadeTypes>
bool ChainClientContext<CascadeTypes...>::load_service_key(const std::vector<std::string>& cmd_tokens) {
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
    object_hasher.init();
    // Note: get_hash_size() only works after init()
    std::vector<uint8_t> hash(object_hasher.get_hash_size());
    object_hasher.add_bytes(&data_obj.version, sizeof(persistent::version_t));
    object_hasher.add_bytes(&data_obj.timestamp_us, sizeof(uint64_t));
    object_hasher.add_bytes(&data_obj.previous_version, sizeof(persistent::version_t));
    object_hasher.add_bytes(&data_obj.previous_version_by_key, sizeof(persistent::version_t));
    object_hasher.add_bytes(data_obj.key.data(), data_obj.key.size());
    object_hasher.add_bytes(data_obj.blob.bytes, data_obj.blob.size);
    object_hasher.finalize(hash.data());
    return hash;
}

template <typename... CascadeTypes>
bool ChainClientContext<CascadeTypes...>::verify_object_signature(const ObjectWithStringKey& hash,
                                                                  const std::vector<uint8_t>& signature,
                                                                  const std::vector<uint8_t>& previous_signature) {
    if(!service_verifier) {
        print_red("Service's public key has not been loaded. Cannot verify.");
        return false;
    }
    service_verifier->init();
    // Because DeltaCascadeStoreCore stores the hashes in deltas (which are just ObjectWithStringKeys),
    // the data that PersistentRegistry ends up signing is actually the to_bytes serialization of the entire
    // ObjectWithStringKey object, not just the hash.
    std::cout << "Verifying signature on hash object " << hash << " with previous signature " << std::hex << previous_signature << std::dec << std::endl;
    std::size_t hash_object_size = mutils::bytes_size(hash);
    uint8_t bytes_of_hash_object[hash_object_size];
    mutils::to_bytes(hash, bytes_of_hash_object);
    /*
     * Verbose debug output:
    std::ios normal_stream_state(nullptr);
    normal_stream_state.copyfmt(std::cout);
    std::cout << "Verifying these bytes: " << std::hex << std::setfill('0');
    for(std::size_t i = 0; i < hash_object_size; ++i) {
        // It sure is hard to convince std::cout to print bytes as bytes. Why do I need two casts?
        std::cout << std::setw(2) << std::right << static_cast<int>(static_cast<uint8_t>(bytes_of_hash_object[i])) << " ";
    }
    std::cout.copyfmt(normal_stream_state);
     */
    std::cout << std::endl;
    service_verifier->add_bytes(bytes_of_hash_object,
                                hash_object_size);
    service_verifier->add_bytes(previous_signature.data(),
                                previous_signature.size());
    return service_verifier->finalize(signature);
}

template <typename... CascadeTypes>
bool ChainClientContext<CascadeTypes...>::put_with_signature(const std::vector<std::string>& cmd_tokens) {
    if(cmd_tokens.size() < 3) {
        print_red("Invalid command format. Please try help " + cmd_tokens[0] + ".");
        return false;
    }
    std::string key_suffix = cmd_tokens[1];
    // Create the object
    ObjectWithStringKey obj;
    obj.key = storage_pool_name + op_delimiter + key_suffix;
    obj.blob = Blob(reinterpret_cast<const uint8_t*>(cmd_tokens[2].c_str()), cmd_tokens[2].length());
    std::string signature_key = signature_pool_name + op_delimiter + key_suffix;
    // Step 1: Subscribe to signature notifications for the object's key, if not subscribed already
    // This must be done before beginning the put to avoid a race condition.
    if(subscribed_notification_keys.find(signature_key) == subscribed_notification_keys.end()) {
        service_client.subscribe_signature_notifications(signature_key);
        subscribed_notification_keys.emplace(signature_key);
    }
    // Step 2: Put the object into the storage pool
    derecho::rpc::QueryResults<derecho::cascade::version_tuple> put_result = service_client.put(obj);
    auto put_reply = put_result.get().begin()->second.get();
    std::cout << "Node " << put_result.get().begin()->first << " finished putting the object, replied with version:"
              << std::hex << std::get<0>(put_reply) << std::dec << ", ts_us:" << std::get<1>(put_reply) << std::endl;
    // Store the version fields in our local copy of the object, so we can hash it accurately
    obj.version = std::get<0>(put_reply);
    obj.previous_version = std::get<1>(put_reply);
    obj.previous_version_by_key = std::get<2>(put_reply);
    obj.timestamp_us = std::get<3>(put_reply);
    // Create a record of this object's headers for the cache
    std::shared_ptr<ObjectSignature> signature_record = std::make_shared<ObjectSignature>();
    signature_record->key_suffix = key_suffix;
    signature_record->object_version = std::get<0>(put_reply);
    signature_record->object_previous_version = std::get<2>(put_reply);
    // Step 3: Wait for a signature notification from the signatures object pool that matches this data object's version
    // Register a notification handler callback for this version
    std::mutex callback_waiting_mutex;
    bool callback_fired = false;
    std::condition_variable notification_received;
    std::vector<uint8_t> previous_signature;
    signature_notification_handler.register_callback(
            signature_record->object_version,
            [&](persistent::version_t data_object_version, persistent::version_t hash_object_version,
                const std::vector<uint8_t>& signature, persistent::version_t prev_signed_version, const std::vector<uint8_t>& prev_signature) {
                assert(data_object_version == signature_record->object_version);
                std::cout << "Got a signature notification for data version " << std::hex << data_object_version << " with hash-object version "
                          << hash_object_version << ". Previous signed version is " << prev_signed_version << std::dec << std::endl;
                signature_record->signature_version = hash_object_version;
                signature_record->signature = signature;
                signature_record->signature_previous_version = prev_signed_version;
                previous_signature = prev_signature;
                callback_fired = true;
                notification_received.notify_all();
            });
    // Block the main thread until the notification arrives, since there is more work to do once we get the signature
    // It would be nice to do this asynchronously, and have a continuation that executes when the notification arrives
    {
        std::cout << "Waiting for the signature notification..." << std::endl;
        std::unique_lock<std::mutex> lock(callback_waiting_mutex);
        notification_received.wait(lock, [&]() { return callback_fired; });
    }

    // Step 4: Get the hash object that corresponds to the data object, to find out what timestamp and previous_version_by_key it got
    // This is necessary because the signature includes the entire ObjectWithStringKey, not just the hash blob
    // If it was possible to fill in all the headers (including timestamp, etc), we could construct this object locally
    auto hash_get_result = service_client.get(signature_key, signature_record->object_version);
    ObjectWithStringKey hash_object = hash_get_result.get().begin()->second.get();
    persistent::version_t signature_version = hash_object.get_version();
    std::cout << "Got the hash object for data version " << std::hex << signature_record->object_version << std::dec << " from node "
              << hash_get_result.get().begin()->first << " and its version is " << std::hex << signature_version << std::dec << std::endl;
    assert(signature_record->signature_version == signature_version);

    // Step 6: Compute the hash of the object locally, since we don't trust the service
    std::vector<uint8_t> hash = compute_hash(obj);
    if(memcmp(hash_object.blob.bytes, hash.data(), hash.size() != 0)) {
        print_red("Object hash stored in Cascade does not match object hash computed locally!");
        return false;
    }
    signature_record->local_hash = std::move(hash);
    // Step 7: Validate the service's signature on this version of the object
    bool validated = verify_object_signature(hash_object, signature_record->signature, previous_signature);
    if(validated) {
        std::cout << "Signature is valid" << std::endl;
    } else {
        print_red("Signature is invalid!");
        return false;
    }
    signature_record->hash_object = std::move(hash_object);
    // Now store the entire signature record in the cache
    cached_signatures_by_key[signature_record->key_suffix].emplace(signature_record->object_version, signature_record);
    cached_signatures_by_version.emplace(signature_record->signature_version, signature_record);
    return true;
}

template <typename... CascadeTypes>
bool ChainClientContext<CascadeTypes...>::cache_signature(const std::vector<std::string>& cmd_tokens) {
    if(cmd_tokens.size() < 3) {
        print_red("Invalid command format. Please try help " + cmd_tokens[0] + ".");
        return false;
    }
    std::string key_suffix = cmd_tokens[1];
    persistent::version_t object_version = std::stol(cmd_tokens[2], nullptr, 0);
    if(cached_signatures_by_key[key_suffix][object_version] == nullptr) {
        cached_signatures_by_key[key_suffix][object_version] = std::make_shared<ObjectSignature>();
        cached_signatures_by_key[key_suffix][object_version]->key_suffix = key_suffix;
        cached_signatures_by_key[key_suffix][object_version]->object_version = object_version;
        // But how do we initialize object_previous_version?
    }
    // Get the hash - we can't compute the hash locally without knowing the object data
    auto hash_get_result = service_client.get(signature_pool_name + op_delimiter + key_suffix, object_version);
    ObjectWithStringKey hash_object = hash_get_result.get().begin()->second.get();
    std::cout << "Got a hash object for data version " << std::hex << object_version << "; its version is " << hash_object.get_version() << std::dec << std::endl;
    cached_signatures_by_key[key_suffix][object_version]->signature_version = hash_object.get_version();
    cached_signatures_by_key[key_suffix][object_version]->hash_object = std::move(hash_object);
    // Get the signature
    auto signature_get_result = service_client.get_signature(signature_pool_name + op_delimiter + key_suffix, object_version);
    std::tuple<std::vector<uint8_t>, persistent::version_t> signature_reply = signature_get_result.get().begin()->second.get();
    std::cout << "Node " << signature_get_result.get().begin()->first << " replied with signature=" << std::hex << std::get<0>(signature_reply)
              << " and previous_signed_version=" << std::get<1>(signature_reply) << std::dec << std::endl;
    cached_signatures_by_key[key_suffix][object_version]->signature = std::get<0>(signature_reply);
    cached_signatures_by_key[key_suffix][object_version]->signature_previous_version = std::get<1>(signature_reply);
    // Copy the pointer to this cache entry into the by-signature-version index
    cached_signatures_by_version[hash_object.get_version()] = cached_signatures_by_key[key_suffix][object_version];
    return true;
}

template <typename... CascadeTypes>
bool ChainClientContext<CascadeTypes...>::verify_cached_signature(const std::vector<std::string>& cmd_tokens) {
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
        verify_version = static_cast<persistent::version_t>(std::stol(cmd_tokens[2], nullptr, 0));
        if(cached_signatures_by_key[key_suffix].find(verify_version) == cached_signatures_by_key[key_suffix].end()) {
            print_red("No signature in the cache for version " + std::to_string(verify_version));
            return false;
        }
    } else {
        verify_version = cached_signatures_by_key[key_suffix].rbegin()->first;
    }
    persistent::version_t previous_signature_version = cached_signatures_by_key[key_suffix].at(verify_version)->signature_previous_version;
    // Debug output:
    // std::cout << "Object " << key_suffix << " at version " << verify_version << " has previous version "
    //           << cached_signatures_by_key[key_suffix][verify_version]->object_previous_version
    //           << ". Its corresponding signature version is " << cached_signatures_by_key[key_suffix][verify_version]->signature_version
    //           << " and the previous signature version is " << previous_signature_version << std::endl;
    // A signature for version X of an object should have signed bytes in this order:
    // 1. Headers of the ObjectWithStringKey containing the hash of the object at version X, which has its own version Y
    // 2. Hash of the object at version X
    // 3. Signature for the previous log entry (delta right before X), which may be a different object
    std::vector<uint8_t> previous_signature;
    auto signature_find_iter = cached_signatures_by_version.find(previous_signature_version);
    if(signature_find_iter == cached_signatures_by_version.end()) {
        std::cout << "Previous signature on version " << previous_signature_version << " is not in the cache, retrieving it" << std::endl;
        auto prev_signature_result = service_client.get_signature_by_version(signature_pool_name + op_delimiter + key_suffix, previous_signature_version);
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

template <typename... CascadeTypes>
bool ChainClientContext<CascadeTypes...>::get_and_verify_signature(const std::vector<std::string>& cmd_tokens) {
    if(cmd_tokens.size() < 2) {
        print_red("Invalid command format. Please try help " + cmd_tokens[0] + ".");
        return false;
    }
    std::string key_suffix = cmd_tokens[1];
    persistent::version_t object_version;
    if(cmd_tokens.size() >= 3) {
        object_version = static_cast<persistent::version_t>(std::stol(cmd_tokens[2], nullptr, 0));
    } else {
        object_version = CURRENT_VERSION;
    }
    const std::string signature_key = signature_pool_name + op_delimiter + key_suffix;
    auto hash_query_result = service_client.get(signature_key, object_version);
    ObjectWithStringKey hash_object = hash_query_result.get().begin()->second.get();
    auto sig_query_result = service_client.get_signature(signature_key, object_version);
    const auto [signature, previous_signature_version] = sig_query_result.get().begin()->second.get();
    std::cout << "Node " << sig_query_result.get().begin()->first << " replied with signature=" << std::hex << signature
              << " and previous_signed_version=" << previous_signature_version << std::dec << std::endl;
    // Get the previous signature, which may or may not relate to this key (but will be in the same object pool)
    auto prev_sig_query_result = service_client.get_signature_by_version(signature_key, previous_signature_version);
    const auto prev_signature_tuple = prev_sig_query_result.get().begin()->second.get();
    bool verified = verify_object_signature(hash_object, signature, std::get<0>(prev_signature_tuple));
    if(verified) {
        std::cout << "Key " << key_suffix << " has a valid signature on version " << object_version
                  << " with previous signature version " << previous_signature_version << std::endl;
    } else {
        print_red("Key " + key_suffix + " had an invalid signature on version " + std::to_string(object_version));
    }
    return verified;
}

template <typename... CascadeTypes>
bool ChainClientContext<CascadeTypes...>::get_and_verify_object(const std::vector<std::string>& cmd_tokens) {
    if(cmd_tokens.size() < 2) {
        print_red("Invalid command format. Please try help " + cmd_tokens[0] + ".");
        return false;
    }
    std::string key_suffix = cmd_tokens[1];
    persistent::version_t object_version;
    if(cmd_tokens.size() >= 3) {
        object_version = static_cast<persistent::version_t>(std::stol(cmd_tokens[2], nullptr, 0));
    } else {
        object_version = CURRENT_VERSION;
    }
    const std::string storage_key = storage_pool_name + op_delimiter + key_suffix;
    const std::string signature_key = signature_pool_name + op_delimiter + key_suffix;
    // Get the object
    std::cout << "Requesting version " << std::hex << object_version << std::dec << " of key " << storage_key << std::endl;
    auto get_result = service_client.get(storage_key, object_version);
    ObjectWithStringKey stored_object = get_result.get().begin()->second.get();
    std::cout << "node(" << get_result.get().begin()->first << ") replied with value:" << stored_object << std::endl;
    if(!stored_object.is_valid()) {
        print_red("Invalid object returned; service could not find key or version");
        return false;
    }
    // The signature cache and SignatureCascadeStore require a specific version, not CURRENT_VERSION
    if(object_version == CURRENT_VERSION) {
        object_version = stored_object.get_version();
    }
    // Compute the hash of the object locally, since we don't trust the service
    std::vector<uint8_t> hash;
    auto cache_search_iter = cached_signatures_by_key[key_suffix].find(object_version);
    if(cache_search_iter != cached_signatures_by_key[key_suffix].end()) {
        hash = cache_search_iter->second->local_hash;
    } else {
        hash = compute_hash(stored_object);
    }
    // Get the hash from the service, because we need to know its header fields to compute the signature
    std::cout << "Requesting version " << std::hex << object_version << std::dec << " of key " << signature_key << std::endl;
    auto hash_query_result = service_client.get(signature_key, object_version);
    ObjectWithStringKey hash_object = hash_query_result.get().begin()->second.get();
    std::cout << "node(" << get_result.get().begin()->first << ") replied with value:" << hash_object << std::endl;
    if(!hash_object.is_valid()) {
        print_red("Invalid hash object returned; SignatureStore could not find key or version");
        return false;
    }
    if(memcmp(hash_object.blob.bytes, hash.data(), hash.size() != 0)) {
        print_red("Object hash stored in Cascade does not match object hash computed locally!");
        return false;
    }
    std::cout << "Requesting signature on version " << std::hex << object_version << std::dec << " of key " << signature_key << std::endl;
    auto sig_query_result = service_client.get_signature(signature_key, object_version);
    const auto [signature, previous_signature_version] = sig_query_result.get().begin()->second.get();
    // Get the previous signature, which may or may not relate to this key (but will be in the same object pool)
    std::cout << "Requesting signature on version " << std::hex << previous_signature_version << std::dec << " of key " << signature_key << std::endl;
    auto prev_sig_query_result = service_client.get_signature_by_version(signature_key, previous_signature_version);
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

using command_handler_t = std::function<bool(DefaultChainClientContext& client_context, const std::vector<std::string>& cmd_tokens)>;
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
         [](DefaultChainClientContext&, const std::vector<std::string>& cmd_tokens) {
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
         [](DefaultChainClientContext&, const std::vector<std::string>& cmd_tokens) {
             shell_is_active = false;
             return true;
         }},
        {"load_service_key",
         "Load the CascadeChain service's public key from a PEM file",
         "load_service_key <filename>",
         [](DefaultChainClientContext& context, const std::vector<std::string>& cmd_tokens) {
             return context.load_service_key(cmd_tokens);
         }},
        {"Object Pool Manipulation Commands", "", "", command_handler_t()},
        {"list_object_pools",
         "List existing object pools",
         "list_object_pools",
         [](DefaultChainClientContext& context, const std::vector<std::string>& cmd_tokens) {
             std::cout << "refreshed object pools:" << std::endl;
             for(std::string& opath : context.get_service_client().list_object_pools(true)) {
                 std::cout << "\t" << opath << std::endl;
             }
             return true;
         }},
        {"setup_object_pools",
         "Create the object pools needed for CascadeChain",
         "setup_object_pools [storage-pool-name] [signature-pool-name]",
         [](DefaultChainClientContext& context, const std::vector<std::string>& cmd_tokens) {
             return context.setup_object_pools(cmd_tokens);
         }},
        {"get_object_pool",
         "Get details of an object pool",
         "get_object_pool <path>",
         [](DefaultChainClientContext& context, const std::vector<std::string>& cmd_tokens) {
             if(cmd_tokens.size() < 2) {
                 print_red("Invalid command format. Please try help " + cmd_tokens[0] + ".");
                 return false;
             }
             auto opm = context.get_service_client().find_object_pool(cmd_tokens[1]);
             std::cout << "get_object_pool returns:"
                       << opm << std::endl;
             return true;
         }},
        {"Object Manipulation Commands", "", "", command_handler_t()},
        {"put_with_signature",
         "Put an object into CascadeChain, then verify and cache its signature",
         "put_with_signature <key-suffix> <value-string>\n"
         "Note: key-suffix should not include an object pool path; the object pool will be chosen automatically",
         [](DefaultChainClientContext& context, const std::vector<std::string>& cmd_tokens) {
             return context.put_with_signature(cmd_tokens);
         }},
        {"get_and_verify",
         "Get an object and its signature from CascadeChain, then verify the signature",
         "get_and_verify <key-suffix> [version(default:current version)]",
         [](DefaultChainClientContext& context, const std::vector<std::string>& cmd_tokens) {
             return context.get_and_verify_object(cmd_tokens);
         }},
        {"cache_signature",
         "Retrieve and cache a signature for a particular version of an object",
         "cache_signature <key-suffix> [version(default:current version)]\n"
         "Note: key-suffix should not include an object pool path; the object pool will be chosen automatically",
         [](DefaultChainClientContext& context, const std::vector<std::string>& cmd_tokens) {
             return context.cache_signature(cmd_tokens);
         }},
        {"get_and_verify_signature",
         "Retrieve and verify a signature for a particular version of an object",
         "get_and_verify_signature <key-suffix> [version(default:current version)]\n"
         "Note: key-suffix should not include an object pool path; the object pool will be chosen automatically",
         [](DefaultChainClientContext& context, const std::vector<std::string>& cmd_tokens) {
             return context.get_and_verify_signature(cmd_tokens);
         }},
        {"op_remove",
         "Remove an object from an object pool.",
         "op_remove <key>\n"
         "Please note that cascade automatically decides the object pool path using the key's prefix.",
         [](DefaultChainClientContext& context, const std::vector<std::string>& cmd_tokens) {
             if(cmd_tokens.size() < 2) {
                 print_red("Invalid command format. Please try help " + cmd_tokens[0] + ".");
                 return false;
             }
             op_remove(context.get_service_client(), cmd_tokens[1]);
             return true;
         }},
        {"op_get",
         "Get an object from an object pool (by version).",
         "op_get <key> [ version(default:current version) ]\n"
         "Please note that cascade automatically decides the object pool path using the key's prefix.",
         [](DefaultChainClientContext& context, const std::vector<std::string>& cmd_tokens) {
             if(cmd_tokens.size() < 2) {
                 print_red("Invalid command format. Please try help " + cmd_tokens[0] + ".");
                 return false;
             }
             persistent::version_t version = CURRENT_VERSION;
             if(cmd_tokens.size() >= 3) {
                 version = static_cast<persistent::version_t>(std::stol(cmd_tokens[2], nullptr, 0));
             }
             auto res = context.get_service_client().get(cmd_tokens[1], version);
             check_get_result(res);
             return true;
         }},
        {"op_get_signature",
         "Get an object's signature from the object pool (by version).",
         "op_get_signature <key> [ version(default:current version) ]\n"
         "Note that Cascade will automatically decide the subgroup to contact based on the key's prefix, "
         "but only object pools hosted on a SignatureCascadeStore subgroup will have signatures.",
         [](DefaultChainClientContext& context, const std::vector<std::string>& cmd_tokens) {
             if(cmd_tokens.size() < 2) {
                 print_red("Invalid command format. Please try help " + cmd_tokens[0] + ".");
                 return false;
             }
             persistent::version_t version = CURRENT_VERSION;
             if(cmd_tokens.size() >= 3) {
                 version = static_cast<persistent::version_t>(std::stol(cmd_tokens[2], nullptr, 0));
             }
             auto query_result = context.get_service_client().get_signature(cmd_tokens[1], version);
             // std::tuple doesn't have an operator<<, so I have to customize check_get_result here
             for(auto& reply_future : query_result.get()) {
                 auto reply = reply_future.second.get();
                 std::cout << "node(" << reply_future.first << ") replied with value: (" << std::get<0>(reply) << "," << std::get<1>(reply) << ")" << std::endl;
             }
             return true;
         }},
        {"op_list_keys",
         "list the object keys in an object pool (by version).",
         "op_list_keys <object pool pathname> <stable> [ version(default:current version) ]\n",
         [](DefaultChainClientContext& context, const std::vector<std::string>& cmd_tokens) {
             if(cmd_tokens.size() < 3) {
                 print_red("Invalid command format. Please try help " + cmd_tokens[0] + ".");
                 return false;
             }
             bool stable = static_cast<bool>(std::stoi(cmd_tokens[2], nullptr, 0));
             persistent::version_t version = CURRENT_VERSION;
             if(cmd_tokens.size() >= 4) {
                 version = static_cast<persistent::version_t>(std::stol(cmd_tokens[3], nullptr, 0));
             }
             auto result = context.get_service_client().list_keys(version, stable, cmd_tokens[1]);
             check_op_list_keys_result(context.get_service_client().wait_list_keys(result));
             return true;
         }},
        {"verify_cached_signature",
         "Verify the cached signature on a specific version of an object",
         "verify_cached_signature <key-suffix> [version(default:current version)]",
         [](DefaultChainClientContext& context, const std::vector<std::string>& cmd_tokens) {
             return context.verify_cached_signature(cmd_tokens);
         }}};

inline void do_command(DefaultChainClientContext& client_context, const std::vector<std::string>& cmd_tokens) {
    try {
        ssize_t command_index = find_command(commands, cmd_tokens[0]);
        if(command_index >= 0) {
            if(commands.at(command_index).handler(client_context, cmd_tokens)) {
                std::cout << "-> Succeeded." << std::endl;
            } else {
                std::cout << "-> Failed." << std::endl;
            }
        } else {
            print_red("unknown command:" + cmd_tokens[0]);
        }
    } catch(const derecho::derecho_exception& ex) {
        print_red(std::string("Exception: ") + ex.what());
    } catch(const std::exception& ex) {
        print_red(std::string("Exception: ") + ex.what());
    } catch(...) {
        print_red("Unknown exception caught.");
    }
}

void interactive_test(DefaultChainClientContext& client_context) {
    // loop
    while(shell_is_active) {
        char* malloced_cmd = readline("cmd> ");
        std::string cmdline(malloced_cmd);
        free(malloced_cmd);
        if(cmdline == "") continue;
        add_history(cmdline.c_str());

        std::string delimiter = " ";
        do_command(client_context, tokenize(cmdline, delimiter.c_str()));
    }
    std::cout << "Client exits." << std::endl;
}

void detached_test(DefaultChainClientContext& client_context, int argc, char** argv) {
    std::vector<std::string> cmd_tokens;
    for(int i = 1; i < argc; i++) {
        cmd_tokens.emplace_back(argv[i]);
    }
    do_command(client_context, cmd_tokens);
}

int main(int argc, char** argv) {
    if(prctl(PR_SET_NAME, PROC_NAME, 0, 0, 0) != 0) {
        dbg_default_debug("Failed to set proc name to {}.", PROC_NAME);
    }
    ServiceClientAPI::initialize(nullptr);
    DefaultChainClientContext client_context(ServiceClientAPI::get_service_client());
    if(argc == 1) {
        // by default, we use the interactive shell.
        interactive_test(client_context);
    } else {
        detached_test(client_context, argc, argv);
    }
    return 0;
}
