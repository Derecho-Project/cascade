#pragma once
#include "service.hpp"
#include "service_types.hpp"
#include <unordered_set>
#include <memory>
#include <string>

/**
 * This interface defines the function the data path logic dll should implement.
 *
 * The data path logic manager (donated as DPLM) loads a dpl dll as follows:
 * 1) DPLM loads the dll, and gets the four interface api functions: register_triggers, register_triggers, and
 *    unregister_triggers.
 * 2) DPLM calls "list_prefixes" to get the list of supported prefixes and pre-registers them to the cascade context.
 * 3) On demand, DPLM calls "register_triggers" to register the prefixes and corresponding data path logic handlers,
 *    where the implementation of register_triggers() should call CascadeContext<>::register_prefix(prefix,ocdpo_ptr) to
 *    do the work.
 * 4) When DPLM decides to unload some prefix group, it calls "unregister_triggers()" to do the work.
 */

namespace derecho {
namespace cascade {

/**
 * list the prefixes to be fixed.
 *
 * @return the supported prefixes.
 */
std::unordered_set<std::string> list_prefixes();

/**
 * Get the UUID of this DPL
 * @return UUID string like "48e60f7c-8500-11eb-8755-0242ac110002"
 */
std::string get_uuid();

/**
 * Get Description of this DPL
 * @return description string.
 */
std::string get_description();

/**
 * Initialize the data path logic
 *
 * @param ctxt
 */
void initialize(ICascadeContext* ctxt);

/**
 * register triggers to cascade
 *
 * @param   ctxt
 */
void register_triggers(ICascadeContext* ctxt);

/**
 * "unregister_triggers" will be called when data path logic loader unload this dll.
 *
 * @param   ctxt
 */
void unregister_triggers(ICascadeContext* ctxt);

/**
 * Release the data path logic
 *
 * @param ctxt
 */
void release(ICascadeContext* ctxt);

} // namespace cascade
} // namespace derecho
