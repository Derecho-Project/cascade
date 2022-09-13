#include <cascade/service_types.hpp>

using namespace derecho::cascade;

int main(int argc, char** argv) {
    uint8_t buf[4096];
    DefaultObjectPoolMetadataType opm;
    opm.to_bytes(buf);
    opm.subgroup_type_index = 1;
    opm.deleted = true;
    std::cout << opm << std::endl;
    std::cout << *(mutils::from_bytes<DefaultObjectPoolMetadataType>(nullptr,buf)) << std::endl;

    std::cout << "VolatileCascadeStoreWithStringKey index is " << DefaultObjectPoolMetadataType::get_subgroup_type_index<VolatileCascadeStoreWithStringKey>() << std::endl;
    std::cout << "PersistentCascadeStoreWithStringKey index is " << DefaultObjectPoolMetadataType::get_subgroup_type_index<PersistentCascadeStoreWithStringKey>() << std::endl;
    std::cout << "TriggerCascadeNoStoreWithStringKey index is " << DefaultObjectPoolMetadataType::get_subgroup_type_index<TriggerCascadeNoStoreWithStringKey>() << std::endl;
    std::cout << "int index is " << DefaultObjectPoolMetadataType::get_subgroup_type_index<int>() << std::endl;
    return 0;
}
