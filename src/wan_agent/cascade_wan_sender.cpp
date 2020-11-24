#include <cascade/cascade.hpp>
#include <cascade/object.hpp>
#include <cascade/service.hpp>
#include <cascade/service_types.hpp>
#include <derecho/conf/conf.hpp>
#include <derecho/utils/logger.hpp>
#include <dlfcn.h>
#include <sys/prctl.h>
#include <wan_agent/wan_agent.hpp>

#define PROC_NAME "wan_cascade_test_sender"

using namespace derecho::cascade;

#ifndef NDEBUG
inline void dump_layout(const json& layout) {
    int tid = 0;
    for(const auto& pertype : layout) {
        int sidx = 0;
        for(const auto& persubgroup : pertype) {
            dbg_default_trace("subgroup={}.{},layout={}.", tid, sidx, persubgroup.dump());
            sidx++;
        }
        tid++;
    }
}
#endif  //NDEBUG

int main(int argc, char** argv) {
    // set proc name
    if(prctl(PR_SET_NAME, PROC_NAME, 0, 0, 0) != 0) {
        dbg_default_warn("Cannot set proc name to {}.", PROC_NAME);
    }
    dbg_default_trace("set proc name to {}", PROC_NAME);
    // load configuration
    auto group_layout = json::parse(derecho::getConfString(CONF_GROUP_LAYOUT));
#ifndef NDEBUG
    dbg_default_trace("load layout:");
    dump_layout(group_layout);
#endif  //NDEBUG
    // load on_data_library
    std::string ondata_library = "";
    if(derecho::hasCustomizedConfKey(CONF_ONDATA_LIBRARY)) {
        ondata_library = derecho::getConfString(CONF_ONDATA_LIBRARY);
    }

    std::shared_ptr<UCW> ucw_ptr;
    std::shared_ptr<SCW> scw_ptr;
    void (*on_cascade_initialization)() = nullptr;
    void (*on_cascade_exit)() = nullptr;
    std::shared_ptr<UCW> (*get_ucw)() = nullptr;
    std::shared_ptr<SCW> (*get_scw)() = nullptr;
    void* dl_handle = nullptr;

    if(ondata_library.size() > 0) {
        dl_handle = dlopen(ondata_library.c_str(), RTLD_LAZY);
        if(!dl_handle) {
            dbg_default_error("Failed to load shared ondata_library:{}. error={}", ondata_library, dlerror());
            return -1;
        }
        // TODO: find an dynamic/automatic way to get the mangled symbols.
        // 1 - on_cascade_initialization
        *reinterpret_cast<void**>(&on_cascade_initialization) = dlsym(dl_handle, "_ZN7derecho7cascade25on_cascade_initializationEv");
        if(on_cascade_initialization == nullptr) {
            dbg_default_error("Failed to load on_cascade_initialization(). error={}", dlerror());
            dlclose(dl_handle);
            return -1;
        }
        // 2 - on_cascade_exit
        *reinterpret_cast<void**>(&on_cascade_exit) = dlsym(dl_handle, "_ZN7derecho7cascade15on_cascade_exitEv");
        if(on_cascade_exit == nullptr) {
            dbg_default_error("Failed to load on_cascade_exit(). error={}", dlerror());
            dlclose(dl_handle);
            return -1;
        }
        // 3 - get_ucw
        *reinterpret_cast<void**>(&get_ucw) = dlsym(dl_handle, "_ZN7derecho7cascade19get_cascade_watcherINS0_14CascadeWatcherImNS0_19ObjectWithUInt64KeyEXadL_ZNS3_2IKEEEXadL_ZNS3_2IVEEEEEEESt10shared_ptrIT_Ev");
        if(get_ucw == nullptr) {
            dbg_default_error("Failed to load get_ucw(). error={}", dlerror());
            dlclose(dl_handle);
            return -1;
        }
        // 4 - get_scw
        *reinterpret_cast<void**>(&get_scw) = dlsym(dl_handle, "_ZN7derecho7cascade19get_cascade_watcherINS0_14CascadeWatcherINSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEENS0_19ObjectWithStringKeyEXadL_ZNS9_2IKB5cxx11EEEXadL_ZNS9_2IVEEEEEEESt10shared_ptrIT_Ev");
        if(get_scw == nullptr) {
            dbg_default_error("Failed to load get_scw(). error={}", dlerror());
            dlclose(dl_handle);
            return -1;
        }
    }

    // initialize
    if(on_cascade_initialization) {
        on_cascade_initialization();
    }

    // create service
    if(get_ucw) {
        ucw_ptr = std::move(get_ucw());
    }
    if(get_scw) {
        scw_ptr = std::move(get_scw());
    }

    auto wpcsu_factory = [&ucw_ptr](persistent::PersistentRegistry* pr, derecho::subgroup_id_t) {
        return std::make_unique<WPCSU>(pr, ucw_ptr.get());
    };
    auto wpcss_factory = [&scw_ptr](persistent::PersistentRegistry* pr, derecho::subgroup_id_t) {
        return std::make_unique<WPCSS>(pr, scw_ptr.get());
    };
    dbg_default_trace("starting service...");
    Service<WPCSU, WPCSS>::start(group_layout, {ucw_ptr.get(), scw_ptr.get()}, wpcsu_factory, wpcss_factory);
    dbg_default_trace("started service, waiting till it ends.");
    std::cout << "Press Enter to Shutdown." << std::endl;
    std::cin.get();
    // wait for service to quit.
    Service<WPCSU, WPCSS>::shutdown(false);
    dbg_default_trace("shutdown service gracefully");
    // you can do something here to parallel the destructing process.
    Service<WPCSU, WPCSS>::wait();
    dbg_default_trace("Finish shutdown.");

    // exit
    if(on_cascade_exit) {
        on_cascade_exit();
    }
    if(dl_handle) {
        dlclose(dl_handle);
    }
    return 0;
}
