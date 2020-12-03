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
    virtual void operator() (const uint32_t sgidx,
                             const uint32_t shidx,
                             const typename CascadeType::KeyType& key,
                             const typename CascadeType::ObjectType& value, ICascadeContext* cascade_ctxt) {
        std::cout << "[on_critical_data_path] I saw data: ["
                  << "KT = " << typeid(typename CascadeType::KeyType).name()
                  << ", VT = " << typeid(typename CascadeType::ObjectType).name()
                  << "] in subgroup(" << sgidx
                  << "), shard(" << shidx
                  << "). key = " << key
                  << " and value = " << value
                  << " . cascade_ctxt = " << cascade_ctxt 
                  << std::endl;
        auto* ctxt = dynamic_cast<CascadeContext<VCSU,VCSS,PCSU,PCSS>*>(cascade_ctxt);
        Action act;
        act.action_type = static_cast<uint64_t>(typeid(CascadeType).hash_code());
        act.immediate_data = (static_cast<uint64_t>(sgidx)<<32) + shidx; // user defined type, we use subgroup_index(32bit)|shard_index(32bit)
        ctxt->post(std::move(act));
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

class ExampleOCPDO: public OffCriticalDataPathObserver {
    virtual void operator () (Action&& action, ICascadeContext* ctxt) {
        std::cout << "[off_critical_data_path] I received an Action with type=" << std::hex << action.action_type << "; immediate_data=" << action.immediate_data << std::endl;
    }
};

std::shared_ptr<OffCriticalDataPathObserver> get_off_critical_data_path_observer() {
    return std::make_shared<ExampleOCPDO>();
}

} // namespace cascade
} // namespace derecho
