#include "server.hpp"

#include "cascade/cascade.hpp"
#include "cascade/service.hpp"
#include "cascade/service_types.hpp"
#include "cascade/object.hpp"

#include <derecho/conf/conf.hpp>
#include <derecho/utils/logger.hpp>

#include <nlohmann/json.hpp>
#include <wan_agent.hpp>

#include <dlfcn.h>
#include <sys/prctl.h>
#include <type_traits>

#define PROC_NAME "backup_server"

using namespace derecho::cascade;

int main(int argc, char** argv) {
    // set proc name
    if(prctl(PR_SET_NAME, PROC_NAME, 0, 0, 0) != 0) {
        dbg_default_warn("Cannot set proc name to {}.", PROC_NAME);
    }

    // Get WanAgent configuration file
    // (it would be nice to have this in the derecho.cfg instead of a command-line argument)
    std::string wanagent_conf_path;
    if(argc > 1) {
        wanagent_conf_path = argv[1];
    } else {
        wanagent_conf_path = "wanagent.json";
    }
    std::ifstream wanagent_config_file(wanagent_conf_path);
    nlohmann::json wanagent_config = nlohmann::json::parse(wanagent_config_file);

    CascadeServiceCDPO<VolatileCascadeStoreWithStringKey, DefaultCascadeContextType> cdpo_vcss;
    CascadeServiceCDPO<PersistentCascadeStoreWithStringKey, DefaultCascadeContextType> cdpo_pcss;
    CascadeServiceCDPO<SignatureCascadeStoreWithStringKey, DefaultCascadeContextType> cdpo_scss;
    CascadeServiceCDPO<TriggerCascadeNoStoreWithStringKey, DefaultCascadeContextType> cdpo_tcss;

    auto meta_factory = [](persistent::PersistentRegistry* pr, derecho::subgroup_id_t, ICascadeContext* context_ptr) {
        return std::make_unique<CascadeMetadataService<VolatileCascadeStoreWithStringKey, PersistentCascadeStoreWithStringKey, SignatureCascadeStoreWithStringKey, TriggerCascadeNoStoreWithStringKey>>(
                pr, nullptr, context_ptr);
    };
    auto vcss_factory = [&cdpo_vcss](persistent::PersistentRegistry*, derecho::subgroup_id_t, ICascadeContext* context_ptr) {
        return std::make_unique<VolatileCascadeStoreWithStringKey>(&cdpo_vcss, context_ptr);
    };
    auto pcss_factory = [&cdpo_pcss](persistent::PersistentRegistry* pr, derecho::subgroup_id_t, ICascadeContext* context_ptr) {
        return std::make_unique<PersistentCascadeStoreWithStringKey>(pr, &cdpo_pcss, context_ptr);
    };
    auto scss_factory = [&cdpo_scss](persistent::PersistentRegistry* pr, derecho::subgroup_id_t subgroup_id,
                                     ICascadeContext* context_ptr) {
        return std::make_unique<SignatureCascadeStoreWithStringKey>(pr, subgroup_id, &cdpo_scss, context_ptr);
    };
    auto tcss_factory = [&cdpo_tcss](persistent::PersistentRegistry*, derecho::subgroup_id_t, ICascadeContext* context_ptr) {
        return std::make_unique<TriggerCascadeNoStoreWithStringKey>(&cdpo_tcss, context_ptr);
    };
    dbg_default_trace("starting service...");
    DefaultServiceType::start(
            {&cdpo_vcss, &cdpo_pcss, &cdpo_tcss},
            meta_factory,
            vcss_factory, pcss_factory, scss_factory, tcss_factory);

    // Start a WanAgent instance that turns received messages into put() requests to the service
    auto* cascade_context_ptr = DefaultServiceType::get_context();
    assert(cascade_context_ptr);
    wan_agent::RemoteMessageCallback wanagent_rmc = [cascade_context_ptr](const uint32_t sender, const uint8_t* msg, const size_t size) {
        static_assert(std::is_same<ObjectWithStringKey, PersistentCascadeStoreWithStringKey::ObjectType>::value, "PersistentCascadeStore's object type is not ObjectWithStringKey");
        auto object_from_remote = mutils::from_bytes<ObjectWithStringKey>(nullptr, msg);
        dbg_default_debug("Received an object with key {} from sender {}", object_from_remote->get_key_ref(), sender);
        // This should put the object into the same type of subgroup as it was in originally (storage or signature),
        // since it will have the same key/object pool path
        cascade_context_ptr->get_service_client_ref().put(*object_from_remote);
    };

    auto wanagent = wan_agent::WanAgent::create(wanagent_config, wan_agent::PredicateLambda{}, wanagent_rmc);

    dbg_default_trace("started service, waiting till it ends.");
    std::cout << "Press Enter to Shutdown." << std::endl;
    std::cin.get();
    // wait for service to quit.
    wanagent->shutdown_and_wait();
    DefaultServiceType::shutdown(false);
    dbg_default_trace("shutdown service gracefully");
    // you can do something here to parallel the destructing process.
    DefaultServiceType::wait();
    dbg_default_trace("Finish shutdown.");

    return 0;
}
