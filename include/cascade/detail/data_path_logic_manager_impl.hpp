#pragma once

#include <dlfcn.h>

namespace derecho {
namespace cascade {

struct DLLMetadata {
    std::string     filename;
    void*           dl_handle;
};

template <typename... CascadeTypes>
class DLLFileManager: public DataPathLogicManager<CascadeTypes...> {
private:
	/* a table for all the files */
	std::unordered_map<std::string,DLLMetadata>   prefix_to_metadata;
public:
    /* constructor */
    DLLFileManager() {
        //TODO: initialize the dll metadata from the json configuration file.
        prefix_to_metadata.emplace("/console_printer",DLLMetadata{"dlls/libconsole_printer_dpl.so",nullptr});
    }
    /* @override */
    virtual std::vector<std::string> get_prefixes() const {
        std::vector<std::string> prefixes;
        for (auto& kv: prefix_to_metadata) {
            prefixes.emplace_back(kv.first);
        }
        return prefixes;
    }
    /* @override */
    virtual void load_prefix_group_handler(CascadeContext<CascadeTypes...>* ctxt, const std::string& prefix) {
        // load the dll file corresponding to the prefix.
        if (prefix_to_metadata.find(prefix) == prefix_to_metadata.end()) {
            return;
        }
        void* dl_handle = prefix_to_metadata.at(prefix).dl_handle;
        if (dl_handle == nullptr) {
            dl_handle = dlopen(prefix_to_metadata.at(prefix).filename.c_str(),RTLD_LAZY);
            if (!dl_handle) {
                dbg_default_error("Failed to load shared library file:{}. error={}", prefix_to_metadata.at(prefix).filename, dlerror());
                return;
            }
            prefix_to_metadata.at(prefix).dl_handle = dl_handle;
        }
        // 1 - on_cascade_initialization
        void (*register_triggers)(ICascadeContext*);
        *reinterpret_cast<void **>(&register_triggers) = dlsym(dl_handle, "_ZN7derecho7cascade17register_triggersEPNS0_15ICascadeContextE");
        if (register_triggers) {
            register_triggers(ctxt);
        }
    }
    /* @override */
    virtual void unload_prefix_group_handler(CascadeContext<CascadeTypes...>* ctxt, const std::string& prefix) {
        if (prefix_to_metadata.find(prefix) == prefix_to_metadata.end()) {
            return;
        }
        void* dl_handle = prefix_to_metadata.at(prefix).dl_handle;
        if (dl_handle != nullptr) {
            void (*unregister_triggers)(ICascadeContext*);
            *reinterpret_cast<void **>(&unregister_triggers) = dlsym(dl_handle, "_ZN7derecho7cascade19unregister_triggersEPNS0_15ICascadeContextE");
            if (unregister_triggers) {
                unregister_triggers(ctxt);
            }
            dlclose(prefix_to_metadata.at(prefix).dl_handle);
            prefix_to_metadata.at(prefix).dl_handle = nullptr;
        }
    }
};

template <typename... CascadeTypes>
std::unique_ptr<DataPathLogicManager<CascadeTypes...>> DataPathLogicManager<CascadeTypes...>::create() {
    //TODO: by default, we use DLLFileManager, this will be changed to MetadataService later.
    return std::make_unique<DLLFileManager<CascadeTypes...>>();
}

} // cascade
} // derecho
