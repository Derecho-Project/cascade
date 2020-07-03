#include <cascade/cascade.hpp>
#include <cascade/service.hpp>
#include <cascade/object.hpp>
#include <sys/prctl.h>
#include <derecho/conf/conf.hpp>
#include <derecho/utils/logger.hpp>

#define PROC_NAME "cascade_server"

#define CONF_VCS_UINT64KEY_LAYOUT "CASCADE/VOLATILECASCADESTORE/UINT64/layout"
#define CONF_VCS_STRINGKEY_LAYOUT "CASCADE/VOLATILECASCADESTORE/STRING/layout"
#define CONF_PCS_UINT64KEY_LAYOUT "CASCADE/PERSISTENTCASCADESTORE/UINT64/layout"
#define CONF_PCS_STRINGKEY_LAYOUT "CASCADE/PERSISTENTCASCADESTORE/STRING/layout"

using namespace derecho::cascade;
using VCSU = VolatileCascadeStore<uint64_t,ObjectWithUInt64Key,&ObjectWithUInt64Key::IK,&ObjectWithUInt64Key::IV>;
using VCSS = VolatileCascadeStore<std::string,ObjectWithStringKey,&ObjectWithStringKey::IK,&ObjectWithStringKey::IV>;
using PCSU = PersistentCascadeStore<uint64_t,ObjectWithUInt64Key,&ObjectWithUInt64Key::IK,&ObjectWithUInt64Key::IV,ST_FILE>;
using PCSS = PersistentCascadeStore<std::string,ObjectWithStringKey,&ObjectWithStringKey::IK,&ObjectWithStringKey::IV,ST_FILE>;

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
#ifndef NDEBUG
    dbg_default_trace("load layout:");
    dump_layout(layout);
#endif//NDEBUG
    // create service
    CascadeWatcher<uint64_t,ObjectWithUInt64Key,&ObjectWithUInt64Key::IK,&ObjectWithUInt64Key::IV> icw; // for int key
    CascadeWatcher<std::string,ObjectWithStringKey,&ObjectWithStringKey::IK,&ObjectWithStringKey::IV> scw; // for string key

    auto vcsu_factory = [&icw](persistent::PersistentRegistry* pr, derecho::subgroup_id_t) {
        return std::make_unique<VCSU>(&icw);
    };
    auto vcss_factory = [&scw](persistent::PersistentRegistry* pr, derecho::subgroup_id_t) {
        return std::make_unique<VCSS>(&scw);
    };
    auto pcsu_factory = [&icw](persistent::PersistentRegistry* pr, derecho::subgroup_id_t) {
        return std::make_unique<PCSU>(pr,&icw);
    };
    auto pcss_factory = [&scw](persistent::PersistentRegistry* pr, derecho::subgroup_id_t) {
        return std::make_unique<PCSS>(pr,&scw);
    };
    dbg_default_trace("starting service...");
    Service<VCSU,VCSS,PCSU,PCSS>::start(layout,{&icw,&scw},vcsu_factory,vcss_factory,pcsu_factory,pcss_factory);
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
