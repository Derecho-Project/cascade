#pragma once
#include <memory>
#include <mutex>
#include <nlohmann/json.hpp>
#include "cascade.hpp"

using json = nlohmann::json; 

/**
 * The cascade service 
 */
namespace derecho {
namespace cascade {

    /**
     * The service will start a cascade service node to serve the client.
     */
    template <typename... CascadeTypes>
    class Service {
    private:
        /**
         * Constructor
         * The constructor will load the configuration, start the service thread.
         * @param layout TODO: explain layout
         */
        Service(const json& layout);
        /**
         * The workhorse
         */
        void run();
        /**
         * Stop the service
         */
        void stop();
        /**
         * Test if the service is running or stopped.
         */ 
        bool is_running();
        /**
         * control synchronization members
         */
        std::mutex service_control_mutex;
        std::condition_variable service_control_cv;
        bool _is_running;
        std::thread service_thread;

        /**
         * Singleton pointer
         */
        static std::unique_ptr<Service<CascadeTypes...>> service_ptr;

    public:
        /**
         * Start the singleton service
         * Please make sure only one thread call start. We do not defense such an incorrect usage.
         */
        static void start();
        /**
         * Check if service is started or not.
         */
        static bool is_started();
        /**
         * shutdown the service
         */
        static void shutdown();
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

}// namespace cascade
}// namespace derecho

#include "detail/service_impl.hpp"
