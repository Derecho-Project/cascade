#include <cascade/user_defined_logic_interface.hpp>
#include <iostream>

namespace derecho{
namespace cascade{

#define MY_UUID     "48e60f7c-8500-11eb-8755-0242ac110002"
#define MY_DESC     "Demo DLL UDL that printing what ever received on console."

std::string get_uuid() {
    return MY_UUID;
}

std::string get_description() {
    return MY_DESC;
}

class ConsolePrinterOCDPO: public OffCriticalDataPathObserver {
    virtual void operator () (const std::string& key_string,
                              const uint32_t prefix_length,
                              persistent::version_t version,
                              const mutils::ByteRepresentable* const value_ptr,
                              const std::unordered_map<std::string,bool>& outputs,
                              ICascadeContext* ctxt,
                              uint32_t worker_id) override {
        std::cout << "[console printer ocdpo]: I(" << worker_id << ") received an object with key=" << key_string 
                  << ", matching prefix=" << key_string.substr(0,prefix_length) << std::endl;
    }

    static std::shared_ptr<OffCriticalDataPathObserver> ocdpo_ptr;
public:
    static void initialize() {
        if(!ocdpo_ptr) {
            ocdpo_ptr = std::make_shared<ConsolePrinterOCDPO>();
        }
    }
    static auto get() {
        return ocdpo_ptr;
    }
};

std::shared_ptr<OffCriticalDataPathObserver> ConsolePrinterOCDPO::ocdpo_ptr;

void initialize(ICascadeContext* ctxt) {
    ConsolePrinterOCDPO::initialize();
}

std::shared_ptr<OffCriticalDataPathObserver> get_observer(
        ICascadeContext*,const nlohmann::json&) {
    return ConsolePrinterOCDPO::get();
}

void release(ICascadeContext* ctxt) {
    // nothing to release
    return;
}

} // namespace cascade
} // namespace derecho
