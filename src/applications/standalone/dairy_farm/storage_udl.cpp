#include <cascade/user_defined_logic_interface.hpp>
#include <iostream>
#include <vector>
#include "time_probes.hpp"
#include "config.h"

namespace derecho{
namespace cascade{

#define MY_UUID     "36590e58-4ca2-11ec-b26b-0242ac110002"
#define MY_DESC     "The Dairy Farm DEMO: Storage UDL for evaluation"

std::string get_uuid() {
    return MY_UUID;
}

std::string get_description() {
    return MY_DESC;
}

class DairyFarmStorageOCDPO: public OffCriticalDataPathObserver {

    virtual void operator () (const node_id_t,
                              const std::string&,
                              const uint32_t,
                              persistent::version_t,
                              const mutils::ByteRepresentable* const value_ptr,
                              const std::unordered_map<std::string,bool>&,
                              ICascadeContext* ctxt,
                              uint32_t) override {
        // test if there is a cow in the incoming frame.
        auto* typed_ctxt = dynamic_cast<DefaultCascadeContextType*>(ctxt);
#ifdef ENABLE_EVALUATION
        if (std::is_base_of<IHasMessageID,ObjectWithStringKey>::value) {
            TimestampLogger::log(TLT_STORAGE_TRIGGERED,
                                        typed_ctxt->get_service_client_ref().get_my_id(),
                                        reinterpret_cast<const ObjectWithStringKey*>(value_ptr)->get_message_id(),
                                        get_walltime());
        }
#endif
    }

    static std::shared_ptr<OffCriticalDataPathObserver> ocdpo_ptr;
public:
    static void initialize() {
        if(!ocdpo_ptr) {
            ocdpo_ptr = std::make_shared<DairyFarmStorageOCDPO>();
        }
    }
    static auto get() {
        return ocdpo_ptr;
    }
};

std::shared_ptr<OffCriticalDataPathObserver> DairyFarmStorageOCDPO::ocdpo_ptr;

void initialize(ICascadeContext*) {
    DairyFarmStorageOCDPO::initialize();
}

std::shared_ptr<OffCriticalDataPathObserver> get_observer(
        ICascadeContext*,const nlohmann::json&) {
    return DairyFarmStorageOCDPO::get();
}

void release(ICascadeContext*) {
    // nothing to release
    return;
}

} // namespace cascade
} // namespace derecho
