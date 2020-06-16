#pragma once
#include "cascade.hpp"

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
    protected:
        /**
         * Constructor
         */
        Service();
        /**
         * start the service
         */
        virtual void start();
        /**
         * Singleton pointer
         */
        static std::unique<Service<CascadeTypes...>> service_ptr;

    public:
        /**
         * start the service
         */
        static void start();
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
