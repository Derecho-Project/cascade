// detecting udl signature
#define BOOTSTRAPPING_UDL_SIGNATURE
#include <cascade/user_defined_logic_interface.hpp>

namespace derecho{
namespace cascade{

std::string get_uuid() {
    return "";
}

std::string get_description() {
    return "";
}

void initialize(ICascadeContext* ctxt) { }

std::shared_ptr<OffCriticalDataPathObserver> get_observer(
        ICascadeContext*,const nlohmann::json&) {
    return nullptr;
}

void release(ICascadeContext* ctxt) { }

} // namespace cascade
} // namespace derecho
