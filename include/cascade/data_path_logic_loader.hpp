#pragma once

#include <memory>
#include <string>

namespace derecho {
namespace cascade {
/**
 * This file defines the API between cascade service and data path logic loader a.k.a. dpl_loader.
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
class DataPathLogicLoader {
public:
    /**
     * Get the prefixes available in all data path logics (dlls).
     *
     * Implementation of this function (should) makes sure the prefixes from different sources do not overlap.
     *
     * @return a vector of prefixes.
     */
    virtual std::vector<std::string> get_prefixes() const = 0;
    
    /**
     * Load the handler for a prefix group.
     *
     * A "prefix group" means a set of prefixes handled by a same data path logic package, like an dll.
     *
     * @param ctxt   - the CascadeContext to register the logics
     * @param prefix - the requested prefix. Please note that this triggers the loading of the data path logic package that
     *                 contains the requested prefix. And therefore, the handlers for supported prefix are all loaded.
     */
    virtual void load_prefix_group_handler(CascadeContext<CascadeTypes...>* ctxt, const std::string& prefix) = 0;

    /**
     * Factory
     *
     * @return the created data path logical loader
     */
    static std::unique_ptr<DataPathLogicLoader<CascadeTypes...>> create();
};

} //cascade
} //derecho
#include "detail/data_path_logic_loader_impl.hpp"
