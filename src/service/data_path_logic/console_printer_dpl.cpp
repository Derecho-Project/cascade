#include <cascade/data_path_logic_interface.hpp>
#include <iostream>

namespace derecho{
namespace cascade{

#define MY_PREFIX   "/console_printer"
#define MY_UUID     "48e60f7c-8500-11eb-8755-0242ac110002"
#define MY_DESC     "Demo DLL DPL that printing what ever received on prefix " MY_PREFIX " on console."

std::unordered_set<std::string> list_prefixes() {
    return {MY_PREFIX};
}

std::string get_uuid() {
    return MY_UUID;
}

std::string get_description() {
    return MY_DESC;
}

void initialize(ICascadeContext* ctxt) {
    // nothing to initialize
    return;
}

class ConsolePrinterOCDPO: public OffCriticalDataPathObserver {
    virtual void operator () (const std::string& key_string,
                              persistent::version_t version,
                              const mutils::ByteRepresentable* const value_ptr,
                              ICascadeContext* ctxt,
                              uint32_t worker_id) override {
        std::cout << "[console printer ocdpo]: I(" << worker_id << ") received an object with key=" << key_string << std::endl;
    }
};

void register_triggers(ICascadeContext* ctxt) {
    // Please make sure the CascadeContext type matches the CascadeService type, which is defined in server.cpp if you
    // use the default cascade service binary.
    auto* typed_ctxt = dynamic_cast<CascadeContext<VolatileCascadeStoreWithStringKey,PersistentCascadeStoreWithStringKey>*>(ctxt);
    typed_ctxt->register_prefixes({MY_PREFIX},MY_UUID,std::make_shared<ConsolePrinterOCDPO>());
}

void unregister_triggers(ICascadeContext* ctxt) {
    auto* typed_ctxt = dynamic_cast<CascadeContext<VolatileCascadeStoreWithStringKey,PersistentCascadeStoreWithStringKey>*>(ctxt);
    typed_ctxt->unregister_prefixes({MY_PREFIX},MY_UUID);
}

void release(ICascadeContext* ctxt) {
    // nothing to release
    return;
}

} // namespace cascade
} // namespace derecho
