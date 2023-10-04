/**
 * @file mproc_udl.cpp
 * @brief   Multi-process UDL stub.
 */
#include <string>
#include <cstring>
#include <chrono>
#include <memory>

#include <cascade/user_defined_logic_interface.hpp>
#include <cascade/mproc/mproc_service.hpp>

namespace derecho {
namespace cascade {

/**
 * @brief UUID
 */
const char* mproc_udl_uuid  = "fb6458a8-60cb-11ee-b058-0242ac110003";

/**
 * @brief DESC
 */
const char* mproc_udl_desc  = "The mproc stub udl.";

/**
 * @fn std::string get_uuid()
 * @brief DLL interface.
 * @return UUID string of this DLL.
 */
__attribute__ ((visibility ("default"))) std::string get_uuid() {
    return mproc_udl_uuid;
}

/**
 * @fn std::string get_description()
 * @brief DLL interface.
 * @return DESC string of this DLL.
 */
__attribute__ ((visibility ("default"))) std::string get_description() {
    return mproc_udl_desc;
}

/**
 * @fn void initialize(ICascadeContext* ctxt)
 * @brief DLL global initializer.
 * @param[in] ctxt      An Opaque pointer to the CascadeContext object.
 */
__attribute__ ((visibility ("default"))) void initialize(ICascadeContext* ctxt) {
    // TODO:
}

/**
 * @fn std::shared_ptr<OffCriticalDataPathObserver> get_observer(
 *         ICascadeContext* ctxt,const nlohmann::json& conf)
 * @brief Generate a new observer with given configuration.
 * @param[in]   conf    The configuration for the mproc udl stub.
 * @return  A shared pointer to the observer.
 */
__attribute__ ((visibility ("default")))
std::shared_ptr<OffCriticalDataPathObserver> get_observer(
    ICascadeContext* ctxt,const nlohmann::json& conf) {
    // TODO:
    return nullptr;
}

/**
 * @fn void release(ICascadeContext* ctxt)
 * @brief DLL global destructor.
 * @param[in] ctxt      An Opaque pointer to the CascadeContext object.
 */
__attribute__((visibility ("default"))) void release(ICascadeContext* ctxt) {
    // TODO:
}

/**
 * @class MProcOCDPO mproc_udl.cpp
 * @brief Implementation of the multi-process udl stub.
 */
class MProcOCDPO : public DefaultOffCriticalDataPathObserver {
public:
    /**
     * @fn MProcOCDPO()
     * @brief The constructor.
     */
    MProcOCDPO () {}
    /**
     * @fn ~MProcOCDPO()
     * @brief The destructor.
     */
    virtual ~MProcOCDPO() {}
};



}
}
