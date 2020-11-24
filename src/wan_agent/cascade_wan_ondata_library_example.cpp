#include <cascade/service_server_api.hpp>
#include <iostream>

namespace derecho {
namespace cascade {

void on_cascade_initialization() {
    std::cout << "[ondata_library_example]: initialize the ondata library here." << std::endl;
}

void on_cascade_exit() {
    std::cout << "[ondata_library_example]: destroy ondata library environment before exit." << std::endl;
}

template <typename KT, typename VT, KT* IK, VT* IV>
class ExampleCascadeWatcher : public CascadeWatcher<KT, VT, IK, IV> {
    virtual void operator()(const subgroup_id_t subgroup_id, const uint32_t shard_id, const KT& key, const VT& value, void* cascade_ctxt) {
        std::cout << "[ondata_library_example]: on critical data path action triggered with ["
                  << "KT = " << typeid(KT).name()
                  << ", VT = " << typeid(VT).name()
                  << "] in subgroup(" << subgroup_id
                  << "), shard(" << shard_id
                  << "). key = " << key
                  << " and value = " << value
                  << " . cascade_ctxt = " << cascade_ctxt
                  << std::endl;
    }
};

//sing UCW = CascadeWatcher<uint64_t,ObjectWithUInt64Key,&ObjectWithUInt64Key::IK,&ObjectWithUInt64Key::IV>;
template <>
std::shared_ptr<UCW> get_cascade_watcher<UCW>() {
    return std::make_shared<ExampleCascadeWatcher<uint64_t, ObjectWithUInt64Key, &ObjectWithUInt64Key::IK, &ObjectWithUInt64Key::IV>>();
}

template <>
std::shared_ptr<SCW> get_cascade_watcher<SCW>() {
    return std::make_shared<ExampleCascadeWatcher<std::string, ObjectWithStringKey, &ObjectWithStringKey::IK, &ObjectWithStringKey::IV>>();
}

}  // namespace cascade
}  // namespace derecho
