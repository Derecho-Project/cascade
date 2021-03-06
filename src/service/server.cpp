#include <cascade/cascade.hpp>
#include <cascade/service.hpp>
#include <cascade/service_types.hpp>
#include <cascade/object.hpp>
#include <sys/prctl.h>
#include <derecho/conf/conf.hpp>
#include <derecho/utils/logger.hpp>
#include <dlfcn.h>
#include <type_traits>

#define PROC_NAME "cascade_server"

using namespace derecho::cascade;

#ifndef NDEBUG
inline void dump_layout(const json& layout) {
    int tid = 0;
    for (const auto& pertype:layout) {
        int sidx = 0;
        for (const auto& persubgroup:pertype ) {
            dbg_default_trace("subgroup={}.{},layout={}.",tid,sidx,persubgroup.dump());
            sidx ++;
        }
        tid ++;
    }
}
#endif//NDEBUG

/**
 * Define the CDPO
 * @tparam CascadeType  the subgroup type
 * @tparam IS_TRIGGER   If true, this is triggered only on p2p message critical data path, otherwise, on ordered send
 *                      message critical data path.
 */
template <typename CascadeType>
class CascadeServiceCDPO: public CriticalDataPathObserver<CascadeType> {
    virtual void operator() (const uint32_t sgidx,
                             const uint32_t shidx,
                             const typename CascadeType::KeyType& key,
                             const typename CascadeType::ObjectType& value,
                             ICascadeContext* cascade_ctxt,
                             bool is_trigger = false) override {
        if constexpr (std::is_convertible<typename CascadeType::KeyType,std::string>::value) {

            auto* ctxt = dynamic_cast<
                CascadeContext<
                    VolatileCascadeStoreWithStringKey,
                    PersistentCascadeStoreWithStringKey,
                    TriggerCascadeNoStoreWithStringKey>*
                >(cascade_ctxt);
            size_t pos = key.rfind(PATH_SEPARATOR);
            std::string prefix;
            if (pos != std::string::npos) {
                // important: we need to keep the trailing PATH_SEPARATOR
                prefix = key.substr(0,pos+1);
            }
            auto handlers = ctxt->get_prefix_handlers(prefix);
            auto value_ptr = std::make_shared<typename CascadeType::ObjectType>(value);
            for(auto& per_prefix : handlers) {
                // per_prefix.first is the matching prefix
                // per_prefix.second is a set of handlers
                for (const auto& handler : per_prefix.second) {
                    // handler.first is handler uuid
                    // handler.second is the output of the handler
                    Action action(
                            key,
                            per_prefix.first.size(),
                            value.get_version(),
                            std::get<0>(handler.second),
                            value_ptr,
                            std::get<1>(handler.second)
                    );
                    ctxt->post(std::move(action),is_trigger);
                }
            }
        }
    }
};

namespace derecho::cascade {
// specialize create_null_object_cb for Cascade Types...
using opm_t = ObjectPoolMetadata<VolatileCascadeStoreWithStringKey,PersistentCascadeStoreWithStringKey,TriggerCascadeNoStoreWithStringKey>;
template<>
opm_t create_null_object_cb<std::string,opm_t,&opm_t::IK,&opm_t::IV>(const std::string& key) {
    opm_t opm;
    opm.pathname = key;
    opm.subgroup_type_index = opm_t::invalid_subgroup_type_index;
    return opm;
}
}

int main(int argc, char** argv) {
    // set proc name
    if( prctl(PR_SET_NAME, PROC_NAME, 0, 0, 0) != 0 ) {
        dbg_default_warn("Cannot set proc name to {}.", PROC_NAME);
    }
    dbg_default_trace("set proc name to {}", PROC_NAME);
    // load configuration
    auto group_layout = json::parse(derecho::getConfString(CONF_GROUP_LAYOUT));

#ifndef NDEBUG
    dbg_default_trace("load layout:");
    dump_layout(group_layout);
#endif//NDEBUG

    CascadeServiceCDPO<VolatileCascadeStoreWithStringKey> cdpo_vcss;
    CascadeServiceCDPO<PersistentCascadeStoreWithStringKey> cdpo_pcss;
    CascadeServiceCDPO<TriggerCascadeNoStoreWithStringKey> cdpo_tcss;

    auto meta_factory = [](persistent::PersistentRegistry* pr, derecho::subgroup_id_t, ICascadeContext* context_ptr) {
        // critical data path for metadata service is currently disabled. But we can leverage it later for object pool
        // metadata handling.
        return std::make_unique<CascadeMetadataService<VolatileCascadeStoreWithStringKey,PersistentCascadeStoreWithStringKey,TriggerCascadeNoStoreWithStringKey>>(
                pr,nullptr,context_ptr);
    };
    auto vcss_factory = [&cdpo_vcss](persistent::PersistentRegistry*, derecho::subgroup_id_t, ICascadeContext* context_ptr) {
        return std::make_unique<VolatileCascadeStoreWithStringKey>(&cdpo_vcss,context_ptr);
    };
    auto pcss_factory = [&cdpo_pcss](persistent::PersistentRegistry* pr, derecho::subgroup_id_t, ICascadeContext* context_ptr) {
        return std::make_unique<PersistentCascadeStoreWithStringKey>(pr,&cdpo_pcss,context_ptr);
    };
    auto tcss_factory = [&cdpo_tcss](persistent::PersistentRegistry*, derecho::subgroup_id_t, ICascadeContext* context_ptr) {
        return std::make_unique<TriggerCascadeNoStoreWithStringKey>(&cdpo_tcss,context_ptr);
    };
    dbg_default_trace("starting service...");
    Service<VolatileCascadeStoreWithStringKey,
            PersistentCascadeStoreWithStringKey,
            TriggerCascadeNoStoreWithStringKey>::start(group_layout,
            {&cdpo_vcss,&cdpo_pcss,&cdpo_tcss},
            meta_factory,
            vcss_factory,pcss_factory,tcss_factory);
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
