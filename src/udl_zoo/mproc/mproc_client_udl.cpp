/**
 * @file mproc_udl.cpp
 * @brief   Multi-process UDL stub.
 */
#include <derecho/persistent/PersistentInterface.hpp>
#include <string>
#include <cstring>
#include <chrono>
#include <memory>

#include <cascade/user_defined_logic_interface.hpp>
#include "mproc_udl_client.hpp"

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
 * @class MProcOCDPO mproc_udl.cpp
 * @brief Implementation of the multi-process udl stub.
 */
class MProcOCDPO : public OffCriticalDataPathObserver {
private:
    std::unique_ptr<MProcUDLClient<CASCADE_SUBGROUP_TYPE_LIST>> client;
public:
    /**
     * @fn MProcOCDPO()
     * @brief The constructor.
     * @param[in]   rbkey   The object commit ring buffer key.
     */
    MProcOCDPO (const key_t rbkey) {
        client = MProcUDLClient<CASCADE_SUBGROUP_TYPE_LIST>::create(rbkey);
    }

    /**
     * @fn ~MProcOCDPO()
     * @brief The destructor.
     */
    virtual ~MProcOCDPO() {
        // the client will destruct itself.
    }

    virtual void operator () (
        const node_id_t         sender_id,
        const std::string&      full_key_string,
        const uint32_t          prefix_length,
        persistent::version_t   version,
        const mutils::ByteRepresentable* const
                                value_ptr,
        const std::unordered_map<std::string,bool>&
                                outputs,
        ICascadeContext*        , // ctxt is not used here.
        uint32_t                worker_id) override {
        client->submit(sender_id,full_key_string,prefix_length,version,value_ptr,outputs,worker_id);
    }
};

/**
 * @fn void initialize(ICascadeContext* ctxt)
 * @brief DLL global initializer.
 * @param[in] ctxt      An Opaque pointer to the CascadeContext object.
 */
__attribute__ ((visibility ("default"))) void initialize(ICascadeContext* ctxt) {
    // nothing to do.
}

/**
 * @fn std::shared_ptr<OffCriticalDataPathObserver> get_observer(
 *         ICascadeContext* ctxt,const nlohmann::json& conf)
 * @brief Generate a new observer with given configuration.
 * @param[in]   ctxt    Context
 * @param[in]   conf    The configuration for the mproc udl stub.
 * @return  A shared pointer to the observer.
 */
__attribute__ ((visibility ("default")))
std::shared_ptr<OffCriticalDataPathObserver> get_observer(
    ICascadeContext* ctxt,const nlohmann::json& conf) {
    // TODO: Information about the UDL server should be passed in through conf.
    // Right now, we just hard-coded it.
    return std::make_shared<MProcOCDPO>(0xabcd0123);
}

/**
 * @fn void release(ICascadeContext* ctxt)
 * @brief DLL global destructor.
 * @param[in] ctxt      An Opaque pointer to the CascadeContext object.
 */
__attribute__((visibility ("default"))) void release(ICascadeContext* ctxt) {
    // nothing to do.
}


}
}
