#include <cascade/data_path_logic_interface.hpp>
#include <iostream>

namespace derecho{
namespace cascade{

#define MY_UUID     "6793c66c-9d92-11eb-9aa9-0242ac110002"
#define MY_DESC     "The Dairy Farm DEMO inference DPL."

std::string get_uuid() {
    return MY_UUID;
}

std::string get_description() {
    return MY_DESC;
}

class DairyFarmInferOCDPO: public OffCriticalDataPathObserver {
    virtual void operator () (const std::string& key_string,
                              persistent::version_t version,
                              const mutils::ByteRepresentable* const value_ptr,
                              const std::unordered_map<std::string,bool>& outputs,
                              ICascadeContext* ctxt,
                              uint32_t worker_id) override {
        // TODO: do inference and put it to the storage object pool specified by outputs.
    }

    static std::shared_ptr<OffCriticalDataPathObserver> ocdpo_ptr;
public:
    static void initialize() {
        if(!ocdpo_ptr) {
            ocdpo_ptr = std::make_shared<DairyFarmInferOCDPO>();
        }
    }
    static auto get() {
        return ocdpo_ptr;
    }
};

std::shared_ptr<OffCriticalDataPathObserver> DairyFarmInferOCDPO::ocdpo_ptr;

void initialize(ICascadeContext* ctxt) {
    DairyFarmInferOCDPO::initialize();
}

std::shared_ptr<OffCriticalDataPathObserver> get_observer() {
    return DairyFarmInferOCDPO::get();
}

void release(ICascadeContext* ctxt) {
    // nothing to release
    return;
}

} // namespace cascade
} // namespace derecho
