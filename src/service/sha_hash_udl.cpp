#include <cascade/service_types.hpp>
#include <cascade/user_defined_logic_interface.hpp>
#include <derecho/openssl/hash.hpp>
#include <iostream>

namespace derecho {
namespace cascade {

using ChainServiceClient = ServiceClient<PersistentCascadeStoreWithStringKey, SignatureCascadeStoreWithStringKey>;

class ShaHashObserver : public OffCriticalDataPathObserver {
private:
    static std::shared_ptr<OffCriticalDataPathObserver> singleton_ptr;

    std::pair<uint32_t, uint32_t> get_my_shard(ChainContextType* cascade_context) {
        ChainServiceClient& service_client = cascade_context->get_service_client_ref();
        const node_id_t my_id = service_client.get_my_id();
        const uint32_t num_storage_subgroups = service_client.get_number_of_subgroups<PersistentCascadeStoreWithStringKey>();
        for(uint32_t subgroup_index = 0; subgroup_index < num_storage_subgroups; ++subgroup_index) {
            const uint32_t num_shards = service_client.get_number_of_shards<PersistentCascadeStoreWithStringKey>(subgroup_index);

            for(uint32_t shard_num = 0; shard_num < num_shards; shard_num++) {
                std::vector<node_id_t> shard_members = service_client.get_shard_members<PersistentCascadeStoreWithStringKey>(
                        subgroup_index, shard_num);
                if(std::find(shard_members.begin(), shard_members.end(), my_id) != shard_members.end()) {
                    return {subgroup_index, shard_num};
                }
            }
        }
        return {0, 0};
    }

public:
    ShaHashObserver(ICascadeContext* context) {
        auto test_context = dynamic_cast<ChainContextType*>(context);
        if(test_context == nullptr) {
            std::cerr << "ERROR: ShaHashObserver was constructed on a server where the context type does not match ChainContextType!" << std::endl;
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
        //Create an ObjectWithStringKey to send to the SignatureCascadeStore, whose Blob value will be a hash
        //Apparently there is no ObjectWithStringKey constructor that's not a copy constructor?
        ObjectWithStringKey hash_object;
        hash_object.key = key_string;
        hash_object.version = version;
        //Annoyingly, this will copy hash_bytes into Blob, then copy Blob into hash_object
        //Also, since Blob uses the old, wrong char* as a "byte buffer",
        //but Hasher users unsigned char*, I have this unnecessary cast
        hash_object.blob = Blob((char*)hash_bytes, sha_hasher.get_hash_size());
        //I hope this observer will only be called when the context type is ChainContextType
        ChainContextType* chain_typed_context = dynamic_cast<ChainContextType*>(context);
        if(chain_typed_context) {
            //The hash should be forwarded to the SignatureCascadeStore shard that has the same
            //subgroup number and shard number as this PersistentCascadeStore
            //Maybe this can be configured with the "DFG" feature?
            uint32_t my_subgroup_index, my_shard_num;
            std::tie(my_subgroup_index, my_shard_num) = get_my_shard(chain_typed_context);
            auto results = chain_typed_context->get_service_client_ref().put<SignatureCascadeStoreWithStringKey>(
                    hash_object, my_subgroup_index, my_shard_num);
        } else {
            std::cerr << "ERROR: ShaHashObserver is running on a server where the context type does not match ChainContextType. Cannot forward the hash to a SignatureCascadeStore" << std::endl;
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
