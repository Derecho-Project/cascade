#pragma once
#include "service.hpp"
#include "service_types.hpp"
#include <memory>

namespace derecho {
namespace cascade {

/**
 * The actions on data path
 *
 * Application specifies the data path actions by providing CascadeWatcher implementations for UCW and SCW. The
 * definition of UCW and SCW is in "service_types.hpp", specialized from CascadeWatcher template type defined in
 * "service.hpp". Please refer to those files for details. But no worries: the interface is pretty simple and
 * self-explanatory.
 *
 * Once UCW and SCW implementations are ready, the application is required to implement the following two functions
 * which will exposed as shared library API:
 * 
 * template<>
 * std::shared_ptr<UCW> get_cascade_watcher<UCW>();
 *
 * template<>
 * std::shared_ptr<SCW> get_cascade_watcher<SCW>();
 *
 * @return a shared pointer to the CascadeWatcher implementation. Cascade service will hold this pointer using its
 * lifetime.
 */
template <typename CWType>
std::shared_ptr<CWType> get_cascade_watcher();

template <>
std::shared_ptr<UCW> get_cascade_watcher<UCW>();

template <>
std::shared_ptr<SCW> get_cascade_watcher<SCW>();

} // namepsace cascade
} // namespace derecho
