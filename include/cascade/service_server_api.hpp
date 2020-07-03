#pragma once
#include "service.hpp"
#include "service_types.hpp"
#include <memory>

namespace derecho {
namespace cascade {

/**
 * The critical data path
 *
 * Application can define their own critical data path handling by implementing the get_cascade_watcher_context<>
 * functions. Application defines 
 *
 * @return a shared pointer to the CascadeWatcherContext implementation.
 */
template <typename CWType>
std::shared_ptr<CWType> get_cascade_watcher();

template <>
std::shared_ptr<UCW> get_cascade_watcher<UCWC>();

template <>
std::shared_ptr<SCW> get_cascade_watcher<SCWC>();

} // namepsace cascade
} // namespace derecho
