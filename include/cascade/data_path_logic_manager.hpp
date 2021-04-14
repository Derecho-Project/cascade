#pragma once

#include <memory>
#include <string>

namespace derecho {
namespace cascade {

/* forward reference */
class OffCriticalDataPathObserver;
/**
 * This file defines the API between cascade service and data path logic manager a.k.a. DPLM.
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
class DataPathLogic {
public:
    // the Hex UUID
    std::string id;
    // description of the DPL
    std::string description;

    /**
     * Initialize the DPL, Please note at this moment, the CascadeContext workers do not start, and the external client
     * is ready to go.
     * @param ctxt   - the CascadeContext
     */
    virtual void initialize(CascadeContext<CascadeTypes...>* ctxt) = 0;

    /**
     * Get a shared ocdpo ptr. The implementation should keep a shared pointer to the ocdpo to avoid recreating ocdpo
     * for multiple calls on get_observer().
     *
     * @return a shared pointer to the ocdpo.
     */
    virtual std::shared_ptr<OffCriticalDataPathObserver> get_observer() = 0;

    /**
     * release the DPL
     * @param ctxt   - the CascadeContext
     */
    virtual void release(CascadeContext<CascadeTypes...>* ctxt) = 0;
};

/**
 * The data path logic manager(DPLM) interface manages the DPLs using its ID. Ideally, DPLM knows where to get the
 * corresponding DPL from its ID. The current implementation manages all those DPLs in DLL files. In the future, the
 * DPLM will pull the DPLs from Cascade Metadata Service. We plan to introduce the "WrapperDPL" concept to allow DPL
 * written in high-level languages like Python and JAVA. A Python DPL should specify its dependent wrapper DPL. On
 * loading such a DPL, the DPLM creates a DPL by customizing the wrapper DPL with the high-level languaged DPL.
 */
template <typename... CascadeTypes>
class DataPathLogicManager {
public:
    /**
     * List all data path logics. The DPL object will post to dpl_function.
     * @param dpl_func  - list_data_path_logic feeds the dpl_func with the DPLs one by one.
     */
    virtual void list_data_path_logics(const std::function<void(const DataPathLogic<CascadeTypes...>&)>& dpl_func) const = 0;

    /**
     * Get a shared ocdpo ptr by DPL id.
     *
     * @param dpl_id    The DPL id.
     *
     * @return a shared pointer to the ocdpo.
     */
    virtual std::shared_ptr<OffCriticalDataPathObserver> get_observer(std::string dpl_id) = 0;

    /**
     * Factory
     *
     * @return the created data path logical manager
     */
    static std::unique_ptr<DataPathLogicManager<CascadeTypes...>> create(CascadeContext<CascadeTypes...>* ctxt);
};

} //cascade
} //derecho
#include "detail/data_path_logic_manager_impl.hpp"
