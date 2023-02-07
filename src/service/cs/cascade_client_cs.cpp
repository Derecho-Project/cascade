#ifndef __EXTERNAL_CLIENT__
#define __WITHOUT_SERVICE_SINGLETONS__
#endif//not __EXTERNAL_CLIENT__

#include <cascade/cascade.hpp>
#include <cascade/service_client_api.hpp>
#include <derecho/core/detail/rpc_utils.hpp>
#include <derecho/persistent/PersistentInterface.hpp>
#include <string>
#include <vector>

// ----------------
// Regular C++ code
// ----------------

using namespace derecho::cascade;

/**
    For error messages.
*/
extern "C" void print_red(const char* msg) {
    std::cout << "\033[1;31m"
              << msg
              << "\033[0m" << std::endl;
}

extern "C" ServiceClientAPI& get_service_client_ref() {
    return ServiceClientAPI::get_service_client();
}

extern "C" uint32_t get_subgroup_index_vcss(ServiceClientAPI& capi) {
    return capi.get_subgroup_type_index<VolatileCascadeStoreWithStringKey>();
}

extern "C" uint32_t get_my_id(ServiceClientAPI& capi) {
    return capi.get_my_id();
}
