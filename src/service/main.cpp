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
    Service<VCSU,VCSS,PCSU,PCSS>::start(layout);
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
