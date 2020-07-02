#include <cascade/cascade.hpp>
#include <cascade/service.hpp>
#include <cascade/object.hpp>
#include <sys/prctl.h>
#include <derecho/conf/conf.hpp>
#include <derecho/utils/logger.hpp>

#define PROC_NAME "cascade_service"

#define CONF_VCS_UINT64KEY_LAYOUT "CASCADE/VOLATILECASCADESTORE/UINT64/layout"
#define CONF_VCS_STRINGKEY_LAYOUT "CASCADE/VOLATILECASCADESTORE/STRING/layout"
#define CONF_PCS_UINT64KEY_LAYOUT "CASCADE/PERSISTENTCASCADESTORE/UINT64/layout"
#define CONF_PCS_STRINGKEY_LAYOUT "CASCADE/PERSISTENTCASCADESTORE/STRING/layout"

using namespace derecho::cascade;
using VCSU = VolatileCascadeStore<uint64_t,ObjectWithUInt64Key,&ObjectWithUInt64Key::IK,&ObjectWithUInt64Key::IV>;
using VCSS = VolatileCascadeStore<std::string,ObjectWithStringKey,&ObjectWithStringKey::IK,&ObjectWithStringKey::IV>;
using PCSU = PersistentCascadeStore<uint64_t,ObjectWithUInt64Key,&ObjectWithUInt64Key::IK,&ObjectWithUInt64Key::IV,ST_FILE>;
using PCSS = PersistentCascadeStore<std::string,ObjectWithStringKey,&ObjectWithStringKey::IK,&ObjectWithStringKey::IV,ST_FILE>;

template <typename KT, typename VT, KT* IK, VT* IV>
class CascadeWatcherContext : public ICascadeWatcherContext<KT,VT,IK,&VT::IV> {
    std::shared_ptr<CascadeWatcher<KT,VT,IK,&VT::IV>> watcher_ptr;
public:
    CascadeWatcherContext() {
        watcher_ptr = std::make_shared<CascadeWatcher<KT,VT,IK,&VT::IV>> (
            [](derecho::subgroup_id_t sid,
               const uint32_t shard_num,
               const KT& key,
               const VT& value,
               void* cascade_context){
                dbg_default_info("Watcher is called with\n\tsubgroup id = {},\n\tshard number = {},\n\tkey(type:{}) = {},\n\tvalue = [hidden].", sid, shard_num, typeid(KT).name(), key);
               });
    }
    std::shared_ptr<CascadeWatcher<KT,VT,IK,&VT::IV>> get_cascade_watcher() override {
        return this->watcher_ptr;
    }   
};


int main(int argc, char** argv) {
    // set proc name
    if( prctl(PR_SET_NAME, PROC_NAME, 0, 0, 0) != 0 ) {
        dbg_default_warn("Cannot set proc name to {}.", PROC_NAME);
    }
    dbg_default_trace("set proc name to {}", PROC_NAME);
    // load configuration
    auto layout = json::array({});
    layout.push_back(json::parse(derecho::getConfString(CONF_VCS_UINT64KEY_LAYOUT)));
    layout.push_back(json::parse(derecho::getConfString(CONF_VCS_STRINGKEY_LAYOUT)));
    layout.push_back(json::parse(derecho::getConfString(CONF_PCS_UINT64KEY_LAYOUT)));
    layout.push_back(json::parse(derecho::getConfString(CONF_PCS_STRINGKEY_LAYOUT)));
    dbg_default_trace("loaded layout conf: {}.", layout.get<std::string>());
    // create service
    CascadeWatcherContext<uint64_t,ObjectWithUInt64Key,&ObjectWithUInt64Key::IK,&ObjectWithUInt64Key::IV> icwc; // for int key
    CascadeWatcherContext<std::string,ObjectWithStringKey,&ObjectWithStringKey::IK,&ObjectWithStringKey::IV> scwc; // for string key

    auto vcsu_factory = [&icwc](persistent::PersistentRegistry* pr, derecho::subgroup_id_t) {
        return std::make_unique<VCSU>(icwc.get_cascade_watcher());
    };
    auto vcss_factory = [&scwc](persistent::PersistentRegistry* pr, derecho::subgroup_id_t) {
        return std::make_unique<VCSS>(scwc.get_cascade_watcher());
    };
    auto pcsu_factory = [&icwc](persistent::PersistentRegistry* pr, derecho::subgroup_id_t) {
        return std::make_unique<PCSU>(pr,icwc.get_cascade_watcher());
    };
    auto pcss_factory = [&scwc](persistent::PersistentRegistry* pr, derecho::subgroup_id_t) {
        return std::make_unique<PCSS>(pr,scwc.get_cascade_watcher());
    };
    Service<VCSU,VCSS,PCSU,PCSS>::start(layout,{&icwc,&scwc},vcsu_factory,vcss_factory,pcsu_factory,pcss_factory);
    dbg_default_trace("started service, waiting till it ends.");
    std::cout << "Press Enter to Shutdown." << std::endl;
    std::cin.get();
    // wait for service to quit.
    Service<VCSU,VCSS,PCSU,PCSS>::shutdown(false);
    dbg_default_trace("shutdown service gracefully");
    // you can do something here to parallel the destructing process.
    Service<VCSU,VCSS,PCSU,PCSS>::wait();
    dbg_default_trace("Finish shutdown.");
    return 0;
}
