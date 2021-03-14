#pragma once

#include <dlfcn.h>

namespace derecho {
namespace cascade {

template <typename... CascadeTypes>
class DLLDataPathLogic: public DataPathLogic<CascadeTypes...> {
private:
    std::string                     filename;
    void*                           dl_handle;
    std::unordered_set<std::string> prefixes;
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
        *reinterpret_cast<void **>(&get_uuid) = load_symbol("_ZN7derecho7cascade8get_uuidB5cxx11Ev");
        if (get_uuid != nullptr) {
            DataPathLogic<CascadeTypes...>::id = get_uuid();
        } else {
            dbg_default_error("Failed to load shared library file:{} because get_uuid is not found.", filename);
            return;
        }
        std::string (*get_desc)();
        *reinterpret_cast<void **>(&get_desc) = load_symbol("_ZN7derecho7cascade15get_descriptionB5cxx11Ev");
        if (get_desc != nullptr) {
            DataPathLogic<CascadeTypes...>::description = get_desc();
        } else {
            dbg_default_warn("Failed to load description for shared library file:{}", filename);
        }
        std::unordered_set<std::string> (*list_prefixes)(void);
        *reinterpret_cast<void **>(&list_prefixes) = load_symbol("_ZN7derecho7cascade13list_prefixesB5cxx11Ev");
        if (list_prefixes != nullptr) {
            prefixes = list_prefixes();
        } else {
            dbg_default_warn("Failed to load prefixes for shared library file:{}", filename);
        }
    }
public:
    /** Constructor
     */
    DLLDataPathLogic(const std::string& _filename):
        filename(_filename) {
        load();
    }

    /**
     * Test if dll is valid.
     */
    bool is_valid() {
        return (dl_handle != nullptr) && (DataPathLogic<CascadeTypes...>::id.size() > 0);
    }
    //@override
    virtual const std::unordered_set<std::string>& get_prefixes() const {
        return prefixes;
    }
    //@override
    virtual void initialize(CascadeContext<CascadeTypes...>* ctxt) {
        void (*initialize)(ICascadeContext*);
        *reinterpret_cast<void **>(&initialize) = load_symbol("_ZN7derecho7cascade10initializeEPNS0_15ICascadeContextE");
        if (initialize != nullptr) {
            initialize(ctxt);
        }
    }
    //@override
    virtual void register_prefixes(CascadeContext<CascadeTypes...>* ctxt) const {
        void (*register_triggers)(ICascadeContext*);
        *reinterpret_cast<void **>(&register_triggers) = load_symbol("_ZN7derecho7cascade17register_triggersEPNS0_15ICascadeContextE");
        if (register_triggers != nullptr) {
            register_triggers(ctxt);
        }
    }
    //@override
    virtual void unregister_prefixes(CascadeContext<CascadeTypes...>* ctxt) const {
        void (*unregister_triggers)(ICascadeContext*);
        *reinterpret_cast<void **>(&unregister_triggers) = load_symbol("_ZN7derecho7cascade19unregister_triggersEPNS0_15ICascadeContextE");
        if (unregister_triggers != nullptr) {
            unregister_triggers(ctxt);
        }
    }
    //@override
    virtual void release(CascadeContext<CascadeTypes...>* ctxt) {
        void (*release)(ICascadeContext*);
        *reinterpret_cast<void **>(&release) = load_symbol("_ZN7derecho7cascade7releaseEPNS0_15ICascadeContextE");
        if (release != nullptr) {
            release(ctxt);
        }
    }
    /** Destructor
     */
    virtual ~DLLDataPathLogic() {
        if (dl_handle) {
            dlclose(dl_handle);
        }
    }
};

#define DPL_DLLS_CONFIG "dpl_dlls.cfg"

template <typename... CascadeTypes>
class DLLFileManager: public DataPathLogicManager<CascadeTypes...> {
private:
	/* a table for all the DPLs */
    std::unordered_map<std::string,std::unique_ptr<DataPathLogic<CascadeTypes...>>> dpl_map;
    /**
     * Load DLL files from configuration file.
     * The default configuration file for DLLFileManager is dpl_dlls.config
     * Its looks like
     * =====================
     * dll_folder_1/dpl_a.so
     * dll_folder_2/dpl_b.so
     * dll_folder_2/dpl_c.so
     * =====================
     */
    void load_and_initialize_dlls(CascadeContext<CascadeTypes...>* ctxt) {
        std::ifstream config(DPL_DLLS_CONFIG);
        //step 1: test if DPL_DLLS_CONFIG exists or not.
        if (!config.good()) {
            dbg_default_error("{} failed because {} does not exist or is not readable.", __PRETTY_FUNCTION__, DPL_DLLS_CONFIG);
            return;
        }
        //step 2: load .so files one by one.
        std::string dll_file_path;
        while(std::getline(config,dll_file_path)) {
            auto dpl = std::make_unique<DLLDataPathLogic<CascadeTypes...>>(dll_file_path);
            if (dpl->is_valid()) {
                dpl->initialize(ctxt);
                dbg_default_trace("Successfully load dll dpl:{}",dll_file_path,dpl->id);
                dpl_map[dpl->id] = std::move(dpl);
            } else {
                dbg_default_error("Failed loading dll dpl:{}.", dll_file_path);
            }
        }
    }
public:
    /* constructor */
    DLLFileManager(CascadeContext<CascadeTypes...>* ctxt) {
        load_and_initialize_dlls(ctxt);
    }

    //@override
    void list_data_path_logics(const std::function<void(const DataPathLogic<CascadeTypes...>&)>& dpl_func) const {
        for (auto& kv: dpl_map) {
            dpl_func(*kv.second);
        }
    }

    //@override
    void register_all(CascadeContext<CascadeTypes...>* ctxt) {
        for (auto& kv: dpl_map) {
            kv.second->register_prefixes(ctxt);
        }
    }

    //@override
    void unregister_all(CascadeContext<CascadeTypes...>* ctxt) {
        for (auto& kv: dpl_map) {
            kv.second->unregister_prefixes(ctxt);
        }
    }

    //@override
    void register_dpl(CascadeContext<CascadeTypes...>* ctxt, std::string dpl_id) {
        if (dpl_map.find(dpl_id) == dpl_map.end()) {
            dbg_default_error("{} failed because DPL:{} is not found.",__PRETTY_FUNCTION__,dpl_id);
        } else {
            dpl_map.at(dpl_id)->register_prefixes(ctxt);
        }
    }

    //@override
    void unregister_dpl(CascadeContext<CascadeTypes...>* ctxt, std::string dpl_id) {
        if (dpl_map.find(dpl_id) == dpl_map.end()) {
            dbg_default_error("{} failed because DPL:{} is not found.",__PRETTY_FUNCTION__,dpl_id);
        } else {
            dpl_map.at(dpl_id)->unregister_prefixes(ctxt);
        }
    }
};

template <typename... CascadeTypes>
std::unique_ptr<DataPathLogicManager<CascadeTypes...>> DataPathLogicManager<CascadeTypes...>::create(
        CascadeContext<CascadeTypes...>* ctxt) {
    //TODO: by default, we use DLLFileManager, this will be changed to MetadataService later.
    return std::make_unique<DLLFileManager<CascadeTypes...>>(ctxt);
}

} // cascade
} // derecho
