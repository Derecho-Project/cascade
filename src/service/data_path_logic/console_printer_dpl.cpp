#include <cascade/data_path_logic_interface.hpp>
#include <iostream>

namespace derecho{
namespace cascade{

#define MY_PREFIX   "/console_printer"

std::vector<std::string> list_prefixes() {
    return {MY_PREFIX};
}

class ConsolePrinterOCDPO: public OffCriticalDataPathObserver {
    virtual void operator () (Action&& action, ICascadeContext* ctxt) {
        //TODO: print action
        std::cout << "[console printer ocdpo]: I received an Action" << action << std::endl;
    }
};

void register_triggers(ICascadeContext* ctxt) {
    // Please make sure the CascadeContext type matches the CascadeService type, which is defined in server.cpp if you
    // use the default cascade service binary.
    auto* typed_ctxt = dynamic_cast<CascadeContext<VolatileCascadeStoreWithStringKey,PersistentCascadeStoreWithStringKey>*>(ctxt);
    typed_ctxt->register_prefix("MY_PREFIX",std::make_shared<ConsolePrinterOCDPO>());
}

void unregister_triggers(ICascadeContext* ctxt) {
    // release allocated resources...
}

} // namespace cascade
} // namespace derecho
