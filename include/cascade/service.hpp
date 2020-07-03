#pragma once
#include <memory>
#include <mutex>
#include <nlohmann/json.hpp>
#include "cascade.hpp"

using json = nlohmann::json; 

/**
 * The cascade service templates
 * 
 * Type neutral templates components go here. Since the server binary and client library has to be type aware (because
 * they are pre-compiled), we separate the api and implementation of them in type-awared header files as follows:
 * - service_types.hpp contains the predefined types for derecho Subgroups, which are specialized from
 *   derecho::cascade::VolatileCascadeStore/PersistentCascadeStore templates.
 * - service_client_api.hpp contains the client API definition.
 * - service_server_api.hpp contains the server API definition. Huh, Server API??? YES! because the application need to
 *   specify their 'onData()' behaviours by implementing the APIs in service_server_api.hpp as a shared library. The
 *   server will load them on restart.
 */
namespace derecho {
namespace cascade {

/**
 * The service will start a cascade service node to serve the client.
 */
template <typename... CascadeTypes>
class Service {
public:
    /**
     * Constructor
     * The constructor will load the configuration, start the service thread.
     * @param layout TODO: explain layout
     * @param dsms TODO: explain it here
     * @param factories: explain it here
     */
    Service(const json& layout, const std::vector<DeserializationContext*>& dsms, derecho::Factory<CascadeTypes>... factories);
    /**
     * The workhorse
     */
    void run();
    /**
     * Stop the service
     */
    void stop(bool is_joining);
    /**
     * Join the service thread
     */
    void join();
    /**
     * Test if the service is running or stopped.
     */ 
    bool is_running();
private:
    /**
     * control synchronization members
     */
    std::mutex service_control_mutex;
    std::condition_variable service_control_cv;
    bool _is_running;
    std::thread service_thread;
    /**
     * The group
     */
    std::unique_ptr<derecho::Group<CascadeTypes...>> group;

    /**
     * Singleton pointer
     */
    static std::unique_ptr<Service<CascadeTypes...>> service_ptr;

public:
    /**
     * Start the singleton service
     * Please make sure only one thread call start. We do not defense such an incorrect usage.
     * @param layout TODO: explain layout
     * @param factories - the factories to create objects.
     */
    static void start(const json& layout, const std::vector<DeserializationContext*>& dsms, derecho::Factory<CascadeTypes>... factories);
    /**
     * Check if service is started or not.
     */
    static bool is_started();
    /**
     * shutdown the service
     */
    static void shutdown(bool is_joining=true);
    /**
     * wait on the service util it stop
     */
    static void wait();
};

/**
 * The Service Context
 */
template <typename... CascadeTypes>
class ServiceContext {
};

/**
 * Create the critical data path callback function.
 * Application should provide corresponding callbacks. The application MUST hold the ownership of the
 * callback objects and make sure its availability during service lifecycle.
 */
template <typename KT, typename VT, KT* IK, VT *IV>
std::shared_ptr<CascadeWatcher<KT,VT,IK,IV>> create_critical_data_path_callback();

/**
 * defining key strings used in the [CASCADE] section of configuration file.
 */
#define MIN_NODES_BY_SHARD      "min_nodes_by_shard"
#define MAX_NODES_BY_SHARD      "max_nodes_by_shard"
#define DELIVERY_MODES_BY_SHARD "delivery_modes_by_shard"
#define DELIVERY_MODE_ORDERED   "Ordered"
#define DELIVERY_MODE_RAW       "Raw"
#define PROFILES_BY_SHARD       "profiles_by_shard"


}// namespace cascade
}// namespace derecho

#include "detail/service_impl.hpp"
