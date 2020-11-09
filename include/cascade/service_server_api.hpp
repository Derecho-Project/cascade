#pragma once
#include "service.hpp"
#include "service_types.hpp"
#include <memory>

namespace derecho {
namespace cascade {


/**
 * "on_cascade_initialization" will be called before cascade server joining derecho group.
 */
void on_cascade_initialization();

/**
 * "on_cascade_exit" will be called after cascade server leave derecho group.
 */
void on_cascade_exit();

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

/**
 * The logic off critical data path
 *
 * The on critical data path handler (as implemented in the watchers) generates action objects and post them to the
 * off critical data path in CascadeContext(cctx). CascadeContext runs a thread pool to process those action objects by
 * calling this function. The CascadeContext* contains the resources for the critical data path handler like:
 * - handle to the derecho group
 * - description of the available resources
 *
 * @param action    The action object
 * @param cctx      The cascade context
 */
void off_critical_data_path_action_handler(Action&& action, ICascadeContext* cctx);

} // namespace cascade
} // namespace derecho
