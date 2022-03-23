#include <cascade/user_defined_logic_interface.hpp>
#include <cascade_dds/config.h>
#include <iostream>

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
    virtual void operator () (const node_id_t sender,
                              const std::string& key_string,
                              const uint32_t prefix_length,
                              persistent::version_t version,
                              const mutils::ByteRepresentable* const value_ptr,
                              const std::unordered_map<std::string,bool>& outputs,
                              ICascadeContext* ctxt,
                              uint32_t worker_id) override {
        // TODO:

        // 1) control plane
        // 2) data plane
    }

    static std::shared_ptr<OffCriticalDataPathObserver> ocdpo_ptr;
public:
    static void initialize() {
        if(!ocdpo_ptr) {
            ocdpo_ptr = std::make_shared<DDSOCDPO>();
        }
    }
    static auto get() {
        return ocdpo_ptr;
    }
};

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
