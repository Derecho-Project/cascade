#pragma once
#include <typeindex>
#include <unordered_map>

namespace derecho {
namespace cascade {

/*
 * Registry for Cascade Store instances
 */
class CascadeStoreRegistry {
private:
    /*
     * Maps SubgroupType to an instance. Used to access the object stores in case of a local get.
     */
    std::unordered_map<std::type_index,void*> cascade_store;
public:
    CascadeStoreRegistry() = default;

    /*
     * Register a cascade store instance
     * @param instance Pointer to the cascade store with type SubgroupType.
     */
    template<typename SubgroupType>
    void register_cascade_store(SubgroupType *instance);

    template<typename SubgroupType>
    SubgroupType* get_cascade_store();
};

} // cascade
} // derecho

#include "detail/cascade_store_registry_impl.hpp"
