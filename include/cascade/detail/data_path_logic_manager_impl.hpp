#pragma once

namespace derecho {
namespace cascade {

template <typename... CascadeTypes>
class DLLFileManager: public DataPathLogicManager<CascadeTypes...> {
public:
    /* constructor */
    DLLFileManager() {
        //TODO: initialize the dll files.
    }
    /* @override */
    virtual std::vector<std::string> get_prefixes() const {
        //TODO:
        return {};
    }
    /* @override */
    virtual void load_prefix_group_handler(CascadeContext<CascadeTypes...>* ctxt, const std::string& prefix) {
        //TODO:
    }
    /* @override */
    virtual void unload_prefix_group_handler(CascadeContext<CascadeTypes...>* ctxt, const std::string& prefix) {
        //TODO:
    }
};

template <typename... CascadeTypes>
std::unique_ptr<DataPathLogicManager<CascadeTypes...>> DataPathLogicManager<CascadeTypes...>::create() {
    //TODO: by default, we use DLLFileManager, this will be changed to MetadataService later.
    return std::make_unique<DLLFileManager<CascadeTypes...>>();
}

} // cascade
} // derecho
