#include <cascade/user_defined_logic_interface.hpp>
#include <cascade_dds/config.h>
#include <cascade_dds/dds.hpp>
#include <iostream>
#include <memory>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

namespace derecho{
namespace cascade{

#define MY_DESC     "Cascade DDS UDL"

std::string get_uuid() {
    return UDL_UUID;
}

std::string get_description() {
    return MY_DESC;
}

class DDSOCDPO: public OffCriticalDataPathObserver {
    /* configuration */
    std::shared_ptr<DDSConfig> config;
    /* control plane suffix */
    std::string control_plane_suffix;
    /* subscriber registry */
    std::unordered_map<std::string,std::unordered_set<node_id_t>> subscriber_registry;
    /* Is shared_mutex fast enough? */
    mutable std::shared_mutex subscriber_registry_mutex;

    virtual void operator () (const node_id_t sender,
                              const std::string& key_string,
                              const uint32_t prefix_length,
                              persistent::version_t, // version
                              const mutils::ByteRepresentable* const value_ptr,
                              const std::unordered_map<std::string,bool>&, // output
                              ICascadeContext* ctxt,
                              uint32_t /*worker_id*/) override {
        if (key_string.size() <= prefix_length) {
            dbg_default_warn("{}: skipping invalid key_string:{}.", __PRETTY_FUNCTION__, key_string);
            return;
        }
        std::string key_without_prefix = key_string.substr(prefix_length);
        dbg_default_trace("{}: key_without_prefix={}.", __PRETTY_FUNCTION__, key_without_prefix);
        const ObjectWithStringKey* object = dynamic_cast<const ObjectWithStringKey*>(value_ptr);
        if (key_without_prefix == control_plane_suffix) {
            // control plane
            mutils::deserialize_and_run(
                    nullptr, 
                    object->blob.bytes, 
                    [&sender,this](const DDSCommand& command){
                        if (command.command_type == DDSCommand::SUBSCRIBE) {
                            std::unique_lock<std::shared_mutex> wlock(this->subscriber_registry_mutex);
                            if (subscriber_registry.find(command.topic) == subscriber_registry.end()) {
                                subscriber_registry.emplace(command.topic,std::unordered_set<node_id_t>{});
                            }
                            subscriber_registry.at(command.topic).emplace(sender);
                            dbg_default_trace("Sender {} subscribes to topic:{}",sender,command.topic);
                        } else if (command.command_type == DDSCommand::UNSUBSCRIBE){
                            std::unique_lock<std::shared_mutex> wlock(this->subscriber_registry_mutex);
                            if (subscriber_registry.find(command.topic) != subscriber_registry.end()) {
                                subscriber_registry.at(command.topic).erase(sender);
                            }
                            dbg_default_trace("Sender {} unsubscribed from topic:{}",sender,command.topic);
                        } else {
                            dbg_default_warn("Unknown DDS command Received: type={},topic='{}'",
                                    command.command_type,command.topic);
                        }
                    });
        } else {
            // data plane
            std::shared_lock<std::shared_mutex> rlck(subscriber_registry_mutex);
            if (subscriber_registry.find(key_without_prefix) != subscriber_registry.cend()) {
                dbg_default_trace("Key:{} is found in subscriber_registry.", key_without_prefix);
                auto* typed_ctxt = dynamic_cast<DefaultCascadeContextType*>(ctxt);
                for (const auto& client_id: subscriber_registry.at(key_without_prefix)) {
                    dbg_default_trace("Forward a message of {} bytes from topic '{}' to external client {}.",
                            object->blob.size, key_without_prefix, client_id);
                    typed_ctxt->get_service_client_ref().notify(object->blob,key_string.substr(0,prefix_length-1),client_id);
                }
            } else {
                dbg_default_trace("Key:{} is not found in subscriber_registry.", key_without_prefix);
            }
        }
    }

    static std::shared_ptr<OffCriticalDataPathObserver> ocdpo_ptr;
public:
    /**
     * Constructor
     */
    DDSOCDPO() {
        config = DDSConfig::get();
        control_plane_suffix = config->get_control_plane_suffix();
    }

    static void initialize() {
        if(!ocdpo_ptr) {
            ocdpo_ptr = std::make_shared<DDSOCDPO>();
        }
    }

    static auto get() {
        return ocdpo_ptr;
    }
};

/* singleton design: all prefixes will use the same ocdpo */
std::shared_ptr<OffCriticalDataPathObserver> DDSOCDPO::ocdpo_ptr;

void initialize(ICascadeContext* ctxt) {
    DDSOCDPO::initialize();
}

std::shared_ptr<OffCriticalDataPathObserver> get_observer(
        ICascadeContext*,const nlohmann::json&) {
    return DDSOCDPO::get();
}

void release(ICascadeContext* ctxt) {
    // nothing to release
    return;
}

} // namespace cascade
} // namespace derecho
