#include "server.hpp"

#include "cascade/cascade.hpp"
#include "cascade/service.hpp"
#include "cascade/service_types.hpp"
//
// We should add a cascade service library side-by-side to libcascade.so to include the client-side implementations for
// cascade service client. Right now, we put some of the implementations (create_null_object_cb<>, e.g.) as inline
// functions in service_client_api.hpp. It's not a good design, which should change later.
#include "cascade/object.hpp"
#include "cascade/service_client_api.hpp"

#include <derecho/conf/conf.hpp>
#include <derecho/utils/logger.hpp>
#include <dlfcn.h>
#include <sys/prctl.h>
#include <type_traits>

#define PROC_NAME "cascade_server"

using namespace derecho::cascade;

int main(int argc, char** argv) {
    // set proc name
    if(prctl(PR_SET_NAME, PROC_NAME, 0, 0, 0) != 0) {
        dbg_default_warn("Cannot set proc name to {}.", PROC_NAME);
    }

    CascadeServiceCDPO<VolatileCascadeStoreWithStringKey> cdpo_vcss;
    CascadeServiceCDPO<PersistentCascadeStoreWithStringKey> cdpo_pcss;
    CascadeServiceCDPO<TriggerCascadeNoStoreWithStringKey> cdpo_tcss;

    auto meta_factory = [](persistent::PersistentRegistry* pr, derecho::subgroup_id_t, ICascadeContext* context_ptr) {
        // critical data path for metadata service is currently disabled. But we can leverage it later for object pool
        // metadata handling.
        return std::make_unique<CascadeMetadataService<VolatileCascadeStoreWithStringKey, PersistentCascadeStoreWithStringKey, TriggerCascadeNoStoreWithStringKey>>(
                pr, nullptr, context_ptr);
    };
    auto vcss_factory = [&cdpo_vcss](persistent::PersistentRegistry*, derecho::subgroup_id_t, ICascadeContext* context_ptr) {
        return std::make_unique<VolatileCascadeStoreWithStringKey>(&cdpo_vcss, context_ptr);
    };
    auto pcss_factory = [&cdpo_pcss](persistent::PersistentRegistry* pr, derecho::subgroup_id_t, ICascadeContext* context_ptr) {
        return std::make_unique<PersistentCascadeStoreWithStringKey>(pr, &cdpo_pcss, context_ptr);
    };
    auto tcss_factory = [&cdpo_tcss](persistent::PersistentRegistry*, derecho::subgroup_id_t, ICascadeContext* context_ptr) {
        return std::make_unique<TriggerCascadeNoStoreWithStringKey>(&cdpo_tcss, context_ptr);
    };
    dbg_default_trace("starting service...");
    Service<VolatileCascadeStoreWithStringKey,
            PersistentCascadeStoreWithStringKey,
            TriggerCascadeNoStoreWithStringKey>::start({&cdpo_vcss, &cdpo_pcss, &cdpo_tcss},
                                                       meta_factory,
                                                       vcss_factory, pcss_factory, tcss_factory);
    dbg_default_trace("started service, waiting till it ends.");
    std::cout << "Press Enter to Shutdown." << std::endl;
    std::cin.get();
    // wait for service to quit.
    Service<VolatileCascadeStoreWithStringKey,
            PersistentCascadeStoreWithStringKey,
            TriggerCascadeNoStoreWithStringKey>::shutdown(false);
    dbg_default_trace("shutdown service gracefully");
    // you can do something here to parallel the destructing process.
    Service<VolatileCascadeStoreWithStringKey,
            PersistentCascadeStoreWithStringKey,
            TriggerCascadeNoStoreWithStringKey>::wait();
    dbg_default_trace("Finish shutdown.");

    return 0;
}
