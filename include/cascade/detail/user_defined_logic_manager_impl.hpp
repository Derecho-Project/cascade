#pragma once

#include <dlfcn.h>

#ifdef BOOTSTRAPPING_UDL_SIGNATURE
// for signature detection
#define GET_UUID_SIG ""
#define GET_DESCRIPTION_SIG ""
#define INITIALIZE_SIG ""
#define GET_OBSERVER_SIG ""
#define RELEASE_SIG ""
#else
// detected signatures
#include <cascade/detail/udl_signature.hpp>
#endif

namespace derecho {
namespace cascade {

template <typename... CascadeTypes>
class DLLUserDefinedLogic: public UserDefinedLogic<CascadeTypes...> {
private:
    std::string                     filename;
    void*                           dl_handle;

    /**
     * load symbol
     * @return the symbol address
     */
    void* load_symbol(const char* symbol) const {
        if (!dl_handle) {
            dbg_default_error("Failed to load symbol:{} from shared library:{}, because dll is not loaded.", symbol, filename);
            return nullptr;
        }

        void* ret = dlsym(dl_handle, symbol);
        if (ret == nullptr) {
            dbg_default_error("Failed to load symbol:{} from shared library:{} with error:{}.",symbol, filename, dlerror());
        }

        return ret;
    }
    /**
     * load dll from file on constructor
     */
    void load() {
        dl_handle = dlopen(filename.c_str(),RTLD_LAZY);
        if (!dl_handle) {
            dbg_default_error("Failed to load shared library file:{}. error={}", filename, dlerror());
            return;
        }

        // load uuid
        std::string (*get_uuid)();
        *reinterpret_cast<void **>(&get_uuid) = load_symbol(GET_UUID_SIG);
        if (get_uuid != nullptr) {
            UserDefinedLogic<CascadeTypes...>::id = get_uuid();
        } else {
            dbg_default_error("Failed to load shared library file:{} because get_uuid is not found.", filename);
            return;
        }
        std::string (*get_desc)();
        *reinterpret_cast<void **>(&get_desc) = load_symbol(GET_DESCRIPTION_SIG);
        if (get_desc != nullptr) {
            UserDefinedLogic<CascadeTypes...>::description = get_desc();
        } else {
            dbg_default_warn("Failed to load description for shared library file:{}", filename);
        }
    }
public:
    /** Constructor
     */
    DLLUserDefinedLogic(const std::string& _filename):
        filename(_filename) {
        load();
    }

    /**
     * Test if dll is valid.
     */
    bool is_valid() {
        return (dl_handle != nullptr) && (UserDefinedLogic<CascadeTypes...>::id.size() > 0);
    }
    //@override
    virtual void initialize(CascadeContext<CascadeTypes...>* ctxt) {
        void (*initialize_fun)(ICascadeContext*);
        *reinterpret_cast<void **>(&initialize_fun) = load_symbol(INITIALIZE_SIG);
        if (initialize_fun != nullptr) {
            initialize_fun(ctxt);
        }
    }
    //@override
    virtual std::shared_ptr<OffCriticalDataPathObserver> get_observer(
            CascadeContext<CascadeTypes...>* ctxt,
            const nlohmann::json& udl_config = nlohmann::json{}) {
        std::shared_ptr<OffCriticalDataPathObserver> (*get_observer_fun)(ICascadeContext*,const nlohmann::json&);
        *reinterpret_cast<void **>(&get_observer_fun) = load_symbol(GET_OBSERVER_SIG);
        if (get_observer_fun != nullptr) {
            return get_observer_fun(ctxt,udl_config);
        } else {
            return std::shared_ptr<OffCriticalDataPathObserver>{nullptr};
        }
    }
    //@override
    virtual void release(CascadeContext<CascadeTypes...>* ctxt) {
        void (*release_fun)(ICascadeContext*);
        *reinterpret_cast<void **>(&release_fun) = load_symbol(RELEASE_SIG);
        if (release_fun != nullptr) {
            release_fun(ctxt);
        }
    }
    /** Destructor
     */
    virtual ~DLLUserDefinedLogic() {
        if (dl_handle) {
            dlclose(dl_handle);
        }
    }
};

template <typename... CascadeTypes>
UserDefinedLogicManager<CascadeTypes...>::~UserDefinedLogicManager() {
    // doing nothing, just make destructor virtual so that the real destructor will be called.
}

#define UDL_DLLS_CONFIG "udl_dlls.cfg"

template <typename... CascadeTypes>
class DLLFileManager: public UserDefinedLogicManager<CascadeTypes...> {
protected:
    /* a table for all the UDLs */
    std::unordered_map<std::string,std::unique_ptr<UserDefinedLogic<CascadeTypes...>>> udl_map;
    CascadeContext<CascadeTypes...>* cascade_context;
    /**
     * Load DLL files from configuration file.
     */
    virtual void load_and_initialize_dlls(CascadeContext<CascadeTypes...>* ctxt,
            const char* udl_dlls_conf) {
        std::ifstream config(udl_dlls_conf);
        //step 1: test if UDL_DLLS_CONFIG exists or not.
        if (!config.good()) {
            dbg_default_warn("{} failed because {} does not exist or is not readable.", __PRETTY_FUNCTION__, UDL_DLLS_CONFIG);
            return;
        }
        //step 2: load .so files one by one.
        std::string dll_file_path;
        while(std::getline(config,dll_file_path)) {
            auto udl = std::make_unique<DLLUserDefinedLogic<CascadeTypes...>>(dll_file_path);
            if (udl->is_valid()) {
                udl->initialize(ctxt);
                dbg_default_trace("Successfully load dll udl:{}",dll_file_path,udl->id);
                udl_map[udl->id] = std::move(udl);
            } else {
                dbg_default_error("Failed loading dll udl:{}.", dll_file_path);
            }
        }
    }
public:
    /**
     * constructor
     * @param[in]   ctxt            The Cascade Context
     * @param[in]   udl_dlls_conf   The UDL DLL configuration, The default configuration file for DLLFileManager is
     *                              udl_dlls.config
     *                              The format is a list of udl dll file paths like the following
     *                              =====================
     *                              dll_folder_1/udl_a.so
     *                              dll_folder_2/udl_b.so
     *                              dll_folder_2/udl_c.so
     *                              =====================
     */
    DLLFileManager(CascadeContext<CascadeTypes...>* ctxt, const char* udl_dlls_conf = UDL_DLLS_CONFIG):
        cascade_context(ctxt) {
        dbg_default_trace("{}:{} DLLFileManager constructor is called.", __FILE__, __LINE__);
        load_and_initialize_dlls(ctxt,udl_dlls_conf);
    }

    //@override
    virtual void list_user_defined_logics(const std::function<void(const UserDefinedLogic<CascadeTypes...>&)>& udl_func) const {
        for (auto& kv: udl_map) {
            udl_func(*kv.second);
        }
    }
    //@override
    virtual std::shared_ptr<OffCriticalDataPathObserver> get_observer(
            const std::string& udl_id,
            const nlohmann::json& udl_config = nlohmann::json{}) {
        if (udl_map.find(udl_id)!=udl_map.end()) {
            return udl_map.at(udl_id)->get_observer(cascade_context,udl_config);
        } else {
            return std::shared_ptr<OffCriticalDataPathObserver>{nullptr};
        }
    }

    virtual ~DLLFileManager() {
        dbg_default_trace("{}:{} DLLFileManager destructor is called.", __FILE__, __LINE__);
        for (auto& kv:udl_map) {
            dbg_default_trace("{}:{} releasing UDL:{}.", __FILE__, __LINE__, kv.first);
            kv.second->release(cascade_context);
        }
    }
};

template <typename... CascadeTypes>
std::unique_ptr<UserDefinedLogicManager<CascadeTypes...>> UserDefinedLogicManager<CascadeTypes...>::create(
        CascadeContext<CascadeTypes...>* ctxt) {
    //TODO: by default, we use DLLFileManager, this will be changed to MetadataService later.
    return std::make_unique<DLLFileManager<CascadeTypes...>>(ctxt);
}

} // cascade
} // derecho
