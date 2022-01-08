#include <cascade/service_types.hpp>
#include <cascade/user_defined_logic_interface.hpp>
#include <derecho/openssl/hash.hpp>
#include <iostream>

namespace derecho {
namespace cascade {

class ShaHashObserver : public OffCriticalDataPathObserver {
private:
    static std::shared_ptr<OffCriticalDataPathObserver> singleton_ptr;

public:
    ShaHashObserver(ICascadeContext* context) {
        auto test_context = dynamic_cast<DefaultCascadeContextType*>(context);
        if(test_context == nullptr) {
            std::cerr << "ERROR: ShaHashObserver was constructed on a server where the context type does not match DefaultCascadeContextType!" << std::endl;
        }
    }
    virtual void operator()(const std::string& key_string,
                            const uint32_t prefix_length,
                            persistent::version_t version,
                            const mutils::ByteRepresentable* const value_ptr,
                            const std::unordered_map<std::string, bool>& outputs,
                            ICascadeContext* context,
                            uint32_t worker_id) override {
        openssl::Hasher sha_hasher(openssl::DigestAlgorithm::SHA256);
        sha_hasher.init();
        uint8_t hash_bytes[sha_hasher.get_hash_size()];
        //Assume this observer is installed on a PersistentCascadeStoreWithStringKey
        const ObjectWithStringKey* const value_object = dynamic_cast<const ObjectWithStringKey* const>(value_ptr);
        if(value_object) {
            assert(value_object->version == version);
            assert(value_object->key == key_string);
            //Hash each field of the object in place instead of using to_bytes to copy it to a byte array
            sha_hasher.add_bytes(&value_object->version, sizeof(persistent::version_t));
            sha_hasher.add_bytes(&value_object->timestamp_us, sizeof(uint64_t));
            sha_hasher.add_bytes(&value_object->previous_version, sizeof(persistent::version_t));
            sha_hasher.add_bytes(&value_object->previous_version_by_key, sizeof(persistent::version_t));
            sha_hasher.add_bytes(value_object->key.data(), value_object->key.size());
            sha_hasher.add_bytes(value_object->blob.bytes, value_object->blob.size);
        } else {
            //This will work for any object, but it's slower
            const std::size_t value_size = mutils::bytes_size(*value_ptr);
            char* value_bytes = new char[value_size];
            mutils::to_bytes(*value_ptr, value_bytes);
            sha_hasher.add_bytes(&version, sizeof(version));
            sha_hasher.add_bytes(value_bytes, value_size);
        }
        sha_hasher.finalize(hash_bytes);
        //Assume prefix_length identifies the "object pool" prefix of key_string
        std::string key_without_object_pool = key_string.substr(prefix_length);
        //Outputs should only have one entry (the object pool for signatures), but loop just in case
        for(const auto& dest_trigger_pair : outputs) {
            //If the current object's key is /object_pool/key_name, create the "parallel" key /signature_pool/key_name
            std::string destination_key = dest_trigger_pair.first + key_without_object_pool;
            std::unique_ptr<ObjectWithStringKey> hash_object;
            if(!value_object) {
                std::cerr << "WARNING: Object to hash was not an ObjectWithStringKey" << std::endl;
                hash_object = std::make_unique<ObjectWithStringKey>(
                        destination_key, (char*)hash_bytes, sha_hasher.get_hash_size());
                hash_object->set_signature_version(version);
            } else {
                // It would be nice if we could set the hash object's version to exactly the
                // same as this object's version. Unfortunately, the object will be assigned
                // a new version when it is received by SignatureCascadeStore, and that behavior
                // can't be changed. Instead, we will save this object's version in the
                // hash object's signature_corresponding_version header, and then later
                // save the hash object's version in this object's signature_corresponding_version header.
                hash_object = std::make_unique<ObjectWithStringKey>(
                        destination_key, (char*)hash_bytes, sha_hasher.get_hash_size());
                hash_object->set_signature_version(version);
            }
            DefaultCascadeContextType* typed_context = dynamic_cast<DefaultCascadeContextType*>(context);
            if(typed_context) {
                if(dest_trigger_pair.second) {
                    std::cout << "WARNING: Doing a trigger_put on an update hash, which means the hash will not be signed. "
                              << "This is probably not what you wanted." << std::endl;
                    auto result = typed_context->get_service_client_ref().trigger_put(*hash_object);
                    result.get();
                } else {
                    auto query_results = typed_context->get_service_client_ref().put(*hash_object);
                    //Wait for the reply from the signature pool, which will contain the version assigned to the hash object
                    auto version_timestamp = query_results.get().begin()->second.get();
                    //Add this version to the data object's headers so we can find the signature
                    value_object->set_signature_version(std::get<0>(version_timestamp));
                }
            } else {
                std::cerr << "ERROR: ShaHashObserver is running on a server where the context type does not match "
                          << "DefaultCascadeContextType. Cannot forward the hash to a SignatureCascadeStore" << std::endl;
            }
        }
    }

    static void initialize(ICascadeContext* context) {
        if(!singleton_ptr) {
            singleton_ptr = std::make_shared<ShaHashObserver>(context);
        }
    }

    static std::shared_ptr<OffCriticalDataPathObserver> get() {
        return singleton_ptr;
    }
};

std::shared_ptr<OffCriticalDataPathObserver> ShaHashObserver::singleton_ptr;

/* ----------------------- UDL Interface ----------------------- */

std::string get_uuid() {
    // Generated by uuidtools.com. I'm not sure where these are supposed to come from for Cascade's purposes.
    return "38a8ae35-37a8-4d6e-929e-64e7cba86de3";
}

std::string get_description() {
    return "UDL module bundled with CascadeChain that computes the SHA256 hash of the data it receives, "
           "then forwards that hash to a SignatureCascadeStore node";
}

void initialize(ICascadeContext* context) {
    ShaHashObserver::initialize(context);
}

void release(ICascadeContext* context) {
}

std::shared_ptr<OffCriticalDataPathObserver> get_observer(
        ICascadeContext* context, const nlohmann::json& config) {
    return ShaHashObserver::get();
}

}  // namespace cascade
}  // namespace derecho
