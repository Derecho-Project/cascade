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
 * The critical data path observers
 *
 * Application specifies the critical data path observer by providing CriticalDataPathObserver implementations for each
 * of the Cascade subgroup types in the service: VCSU/PCSU/VCSS/PCSS. The definition of the above types is in
 * "service_types.hpp" and "cascade.hpp".
 *
 * To process data on the critical path, an application programmer need implement corresponding CriticalDataPathObserver
 * for each of the Cascade subgroup types. Once the observer implementations are ready, the application is required to
 * implement the following functions exposed as shared library API:
 * 
 * template <>
 * std::shared_ptr<CriticalDataPathObserver<VCSU>> get_critical_data_path_observer<VCSU>();
 * template <>
 * std::shared_ptr<CriticalDataPathObserver<VCSS>> get_critical_data_path_observer<VCSS>();
 * template <>
 * std::shared_ptr<CriticalDataPathObserver<PCSU>> get_critical_data_path_observer<PCSU>();
 * template <>
 * std::shared_ptr<CriticalDataPathObserver<PCSS>> get_critical_data_path_observer<PCSS>();
 *
 * @return a shared pointer to the CriticalDataPathObserver implementation. Cascade service will hold this pointer using its
 * lifetime.
 */
template <typename CascadeType>
std::shared_ptr<CriticalDataPathObserver<CascadeType>> get_critical_data_path_observer();

template <>
std::shared_ptr<CriticalDataPathObserver<VCSU>> get_critical_data_path_observer<VCSU>();
template <>
std::shared_ptr<CriticalDataPathObserver<VCSS>> get_critical_data_path_observer<VCSS>();
template <>
std::shared_ptr<CriticalDataPathObserver<PCSU>> get_critical_data_path_observer<PCSU>();
template <>
std::shared_ptr<CriticalDataPathObserver<PCSS>> get_critical_data_path_observer<PCSS>();

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
