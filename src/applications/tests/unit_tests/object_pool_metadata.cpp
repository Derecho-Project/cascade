#include <cascade/service_types.hpp>

using namespace derecho::cascade;

using opm_t = ObjectPoolMetadata<VolatileCascadeStoreWithStringKey,PersistentCascadeStoreWithStringKey,TriggerCascadeNoStoreWithStringKey>;

int main(int argc, char** argv) {
    char buf[4096];
    opm_t opm;
    opm.to_bytes(buf);
    opm.subgroup_type_index = 1;
    opm.deleted = true;
    std::cout << opm << std::endl;
    std::cout << *(mutils::from_bytes<opm_t>(nullptr,buf)) << std::endl;

    std::cout << "VolatileCascadeStoreWithStringKey index is " << opm_t::get_subgroup_type_index<VolatileCascadeStoreWithStringKey>() << std::endl;
    std::cout << "PersistentCascadeStoreWithStringKey index is " << opm_t::get_subgroup_type_index<PersistentCascadeStoreWithStringKey>() << std::endl;
    std::cout << "TriggerCascadeNoStoreWithStringKey index is " << opm_t::get_subgroup_type_index<TriggerCascadeNoStoreWithStringKey>() << std::endl;
    std::cout << "int index is " << opm_t::get_subgroup_type_index<int>() << std::endl;
    return 0;
}
