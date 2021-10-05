#pragma once
#include "service.hpp"
#include "service_types.hpp"
#include <unordered_set>
#include <memory>
#include <string>

/**
 * This interface defines the function the user defined logic dll should implement.
 *
 * The user defined logic manager (donated as UDLM) loads a udl dll as follows:
 * 1) UDLM loads the dll, and gets the four interface api functions: register_triggers, register_triggers, and
 *    unregister_triggers.
 * 2) UDLM calls "list_prefixes" to get the list of supported prefixes and pre-registers them to the cascade context.
 * 3) On demand, UDLM calls "register_triggers" to register the prefixes and corresponding user defined logic handlers,
 *    where the implementation of register_triggers() should call CascadeContext<>::register_prefix(prefix,ocdpo_ptr) to
 *    do the work.
 * 4) When UDLM decides to unload some prefix group, it calls "unregister_triggers()" to do the work.
 */

namespace derecho {
namespace cascade {

/**
 * Get the UUID of this UDL
 * @return UUID string like "48e60f7c-8500-11eb-8755-0242ac110002"
 */
std::string get_uuid();

/**
 * Get Description of this UDL
 * @return description string.
 */
std::string get_description();

/**
 * Initialize the user defined logic
 *
 * @param ctxt - cascade context
 */
void initialize(ICascadeContext* ctxt);

/**
 * register triggers to cascade
 *
 * @param   ctxt - cascade context
 * @param   pathname represents which node in the prefix tree is requesting the observer.
 * @param   config is a configuration string from dfgs.json to customize the UDL behaviour.
 */
std::shared_ptr<OffCriticalDataPathObserver> get_observer(
        ICascadeContext* ctxt,
        const std::string& pathname,
        const std::string& udl_config);

/**
 * Release the user defined logic
 *
 * @param ctxt
 */
void release(ICascadeContext* ctxt);

} // namespace cascade
} // namespace derecho
