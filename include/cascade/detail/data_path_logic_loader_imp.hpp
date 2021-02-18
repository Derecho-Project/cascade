#pragma once

namespace derecho {
namespace cascade {

template <typename... CascadeTypes>
class DLLFileLoader: public DataPathLogicLoader<CascadeTypes...> {
public:
    /* constructor */
    DLLFileLoader() {
        //TODO: initialize the dll files.
    }
    virtual std::vector<std::string> get_prefixes() const override {
        //TODO:
        return {};
    }
    virtual void load_prefix_group_handler(CascadeContext<CascadeTypes...>* ctxt, const std::string& prefix) {
        //TODO:
    }
};

template <typename... CascadeTypes>
static unique_ptr<DataPathLogicLoader<CascadeTypes...>> DataPathLogicLoader<CascadeTypes...>::create() {
    //TODO: by default, we use DLLFileLoader
    return std::make_unique<DLLFileLoader<CascadeTypes...>>();
}

} // cascade
} // derecho
