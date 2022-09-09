#include <cascade/user_defined_logic_interface.hpp>
#include <derecho/core/notification.hpp>
#include <iostream>

namespace derecho{
namespace cascade{

#define MY_UUID     "b4e58924-a169-11ec-9150-0242ac110002"
#define MY_DESC     "Demo DLL UDL that echo the message to all connected clients."

std::string get_uuid() {
    return MY_UUID;
}

std::string get_description() {
    return MY_DESC;
}

class NotificationOCDPO: public OffCriticalDataPathObserver {
    virtual void operator () (const node_id_t sender,
                              const std::string& key_string,
                              const uint32_t prefix_length,
                              persistent::version_t,
                              const mutils::ByteRepresentable* const,
                              const std::unordered_map<std::string,bool>&,
                              ICascadeContext* ctxt,
                              uint32_t worker_id) override {
        std::cout << "[notification ocdpo]: I(" << worker_id << ") received an object with key=" << key_string 
                  << ", matching prefix=" << key_string.substr(0,prefix_length) << std::endl;
        std::string object_pool_pathname = key_string.substr(0, prefix_length - 1);
        usleep(1000);
        auto *typed_ctxt = dynamic_cast<DefaultCascadeContextType*>(ctxt);
        auto& capi = typed_ctxt->get_service_client_ref();
        try {
            Blob echo_blob(reinterpret_cast<const uint8_t*>(key_string.c_str()),key_string.size(),true);
            capi.notify(echo_blob,object_pool_pathname,sender);
            std::cout << "[notification ocdpo]: echo back to node:" << sender << std::endl;
        } catch (derecho::derecho_exception& ex) {
            std::cout << "[notification ocdpo]: exception on notification:" << ex.what() << std::endl;
        }
    }

    static std::shared_ptr<OffCriticalDataPathObserver> ocdpo_ptr;
public:
    static void initialize() {
        if(!ocdpo_ptr) {
            ocdpo_ptr = std::make_shared<NotificationOCDPO>();
        }
    }
    static auto get() {
        return ocdpo_ptr;
    }
};

std::shared_ptr<OffCriticalDataPathObserver> NotificationOCDPO::ocdpo_ptr;

void initialize(ICascadeContext*) {
    NotificationOCDPO::initialize();
}

std::shared_ptr<OffCriticalDataPathObserver> get_observer(
        ICascadeContext*,const nlohmann::json&) {
    return NotificationOCDPO::get();
}

void release(ICascadeContext*) {
    // nothing to release
    return;
}

} // namespace cascade
} // namespace derecho
