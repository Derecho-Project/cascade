#pragma once
#include <cascade/config.h>
#include "../service.hpp"
#include "../service_types.hpp"
#include <unordered_set>
#include <memory>
#include <string>
#include <nlohmann/json.hpp>

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
 * This function is called only once on dll loading.
 *
 * @param ctxt - cascade context
 */
void initialize(ICascadeContext* ctxt);

/**
 * register triggers to cascade
 * This function will be called on each UDL instance registered in application DFGs.
 *
 * @param   ctxt - cascade context
 * @param   config is a configuration string from dfgs.json to customize the UDL behaviour.
 */
std::shared_ptr<OffCriticalDataPathObserver> get_observer(
        ICascadeContext* ctxt,
        const nlohmann::json& udl_config);

/**
 * Release the user defined logic
 * This function is called only once on dll unloading.
 *
 * @param ctxt
 */
void release(ICascadeContext* ctxt);

/**
 * An Easier to use API with service type awareness.
 * Hierarchy:
 *
 * OffCriticalDataPathObserver    [IDefaultOffCriticalDataPathObserver]
 *             ^                                     ^
 *             |                                     |
 *             |      +------------------------------+
 *             |      |
 * DefaultOffCriticalDataPathObserver
 * 
 * Please derive your own ocdpo from DefaultOffCriticalDataPathObserver, and override the virtual methods defined in
 * IDefaultOffCriticalDataPathObserver
 */
using emit_func_t = std::function<void(const std::string&,
                                       persistent::version_t version,
                                       uint64_t              timestamp_us,
                                       persistent::version_t previous_version,
                                       persistent::version_t previous_version_by_key,
#ifdef ENABLE_EVALUATION
                                       uint64_t message_id,
#endif
                                       const Blob&)>;

#ifdef ENABLE_EVALUATION
#define EMIT_NO_VERSION_AND_TIMESTAMP   persistent::INVALID_VERSION,0,persistent::INVALID_VERSION,persistent::INVALID_VERSION,0
#else
#define EMIT_NO_VERSION_AND_TIMESTAMP   persistent::INVALID_VERSION,0,persistent::INVALID_VERSION,persistent::INVALID_VERSION
#endif

class IDefaultOffCriticalDataPathObserver {
public:
    /** 
     * Typed ocdpo handler derived from the Cascade service types defined in service_types.hpp
     * @param sender                The sender id
     * @param object_pool_pathname  The object pool pathname
     * @param key_string            The key inside the object pool's domain
     * @param object                The immutable object live in the temporary buffer shared by multiple worker threads.
     * @param emit                  A function to emit the output results.
     * @param typed_ctxt            The typed context pointer to get access of extra Cascade service
     * @param worker_id             The off critical data path worker id.
     */
    virtual void ocdpo_handler (
            const node_id_t                 sender,
            const std::string&              object_pool_pathname,
            const std::string&              key_string,
            const ObjectWithStringKey&      object,
            const emit_func_t&              emit,
            DefaultCascadeContextType*      typed_ctxt,
            uint32_t                        worker_id) = 0;
};
class DefaultOffCriticalDataPathObserver;

#include "udl_toolkits.hpp"

} // namespace cascade
} // namespace derecho
