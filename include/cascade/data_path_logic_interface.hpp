#pragma once
#include "service.hpp"
#include "service_types.hpp"
#include <vector>
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
std::vector<std::string> list_prefixes();

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

} // namespace cascade
} // namespace derecho
