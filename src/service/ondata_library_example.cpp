#include <cascade/service_server_api.hpp>
#include <iostream>

namespace derecho{
namespace cascade{

void on_cascade_initialization() {
    std::cout << "[ondata_library_example]: initialize the ondata library here." << std::endl;
}

void on_cascade_exit() {
    std::cout << "[ondata_library_example]: destroy ondata library environment before exit." << std::endl;
}

template <typename CascadeType>
class ExampleCPDO: public CriticalDataPathObserver<CascadeType> {
    virtual void operator() (const uint32_t sgidx, const uint32_t shidx, const typename CascadeType::KeyType& key, const typename CascadeType::ObjectType& value, ICascadeContext* cascade_ctxt) {
        std::cout << "[ondata_library_example]: on critical data path action triggered with ["
                  << "KT = " << typeid(typename CascadeType::KeyType).name()
                  << ", VT = " << typeid(typename CascadeType::ObjectType).name()
                  << "] in subgroup(" << sgidx
                  << "), shard(" << shidx
                  << "). key = " << key
                  << " and value = " << value
                  << " . cascade_ctxt = " << cascade_ctxt 
                  << std::endl;
    }
};

template <>
std::shared_ptr<CriticalDataPathObserver<VCSU>> get_critical_data_path_observer<VCSU>() {
    return std::make_shared<ExampleCPDO<VCSU>>();
}

template <>
std::shared_ptr<CriticalDataPathObserver<PCSU>> get_critical_data_path_observer<PCSU>() {
    return std::make_shared<ExampleCPDO<PCSU>>();
}

template <>
std::shared_ptr<CriticalDataPathObserver<VCSS>> get_critical_data_path_observer<VCSS>() {
    return std::make_shared<ExampleCPDO<VCSS>>();
}

template <>
std::shared_ptr<CriticalDataPathObserver<PCSS>> get_critical_data_path_observer<PCSS>() {
    return std::make_shared<ExampleCPDO<PCSS>>();
}

} // namespace cascade
} // namespace derecho
