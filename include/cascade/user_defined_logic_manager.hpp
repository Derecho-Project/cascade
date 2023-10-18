#pragma once

#include <memory>
#include <string>
#include <nlohmann/json.hpp>

namespace derecho {
namespace cascade {

/* forward reference */
class OffCriticalDataPathObserver;

/**
 * This file defines the API between cascade service and data path logic manager a.k.a. UDLM.
 *
 * On cascade service initialization, it first loads all the prefixes from the data path logic. Later, upon request,
 * corresponding handlers are loaded lazily.
 *
 * TODO: in the future, we should provide a console to the cascade server so that a cascade administrator is able to
 * load/unload it manually.
 */
template <typename... CascadeTypes>
class CascadeContext;

template <typename... CascadeTypes>
class UserDefinedLogic {
public:
    // the Hex UUID
    std::string id;
    // description of the UDL
    std::string description;

    /**
     * Initialize the UDL, Please note at this moment, the CascadeContext workers do not start, and the external client
     * is ready to go.
     * @param ctxt   - the CascadeContext
     */
    virtual void initialize(CascadeContext<CascadeTypes...>* ctxt) = 0;

    /**
     * Get a shared ocdpo ptr. The implementation should keep a shared pointer to the ocdpo to avoid recreating ocdpo
     * for multiple calls on get_observer().
     *
     * @param ctxt        - the CascadeContext
     * @param udl_config  - a JSON configuration string for this UDL.
     *
     * @return a shared pointer to the ocdpo.
     */
    virtual std::shared_ptr<OffCriticalDataPathObserver> get_observer(
            CascadeContext<CascadeTypes...>* ctxt,
            const nlohmann::json& udl_config = nlohmann::json{}) = 0;

    /**
     * release the UDL
     * @param ctxt   - the CascadeContext
     */
    virtual void release(CascadeContext<CascadeTypes...>* ctxt) = 0;
};

/**
 * @brief UserDefinedLogicManager
 * The data path logic manager(UDLM) interface manages the UDLs using its ID. Ideally, UDLM knows where to get the
 * corresponding UDL from its ID. The current implementation manages all those UDLs in DLL files. In the future, the
 * UDLM will pull the UDLs from Cascade Metadata Service. We plan to introduce the "WrapperUDL" concept to allow UDL
 * written in high-level languages like Python and JAVA. A Python UDL should specify its dependent wrapper UDL. On
 * loading such a UDL, the UDLM creates a UDL by customizing the wrapper UDL with the high-level languaged UDL.
 */
template <typename... CascadeTypes>
class UserDefinedLogicManager {
public:
    /**
     * @brief list_user_defined_logics
     * List all data path logics. The UDL object will post to udl_function.
     *
     * @param[in] udl_func  - list_user_defined_logic feeds the udl_func with the UDLs one by one.
     */
    virtual void list_user_defined_logics(const std::function<void(const UserDefinedLogic<CascadeTypes...>&)>& udl_func) const = 0;

    /**
     * Get a shared ocdpo ptr by UDL id.
     *
     * @param[in] udl_id        - The UDL id.
     * @param[in] udl_config    - a JSON configuration for this UDL.
     *
     * @return a shared pointer to the ocdpo.
     */
    virtual std::shared_ptr<OffCriticalDataPathObserver> get_observer(
            const std::string& udl_id,
            const nlohmann::json& udl_config = nlohmann::json{}) = 0;

    /**
     * @brief Destructor
     * A virtual destructor: we need this because the default destructor is not virtual.
     */
    virtual ~UserDefinedLogicManager();

    /**
     * @brief Factory
     *
     * @param[in]   ctxt        The cascade context
     *
     * @return the created data path logical manager
     */
    static std::unique_ptr<UserDefinedLogicManager<CascadeTypes...>> create(CascadeContext<CascadeTypes...>* ctxt);
};

} //cascade
} //derecho
#include "detail/user_defined_logic_manager_impl.hpp"
