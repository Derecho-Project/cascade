#include <cascade/service_types.hpp>

using namespace derecho::cascade;

using opm_t = ObjectPoolMetadata<VolatileCascadeStoreWithStringKey,PersistentCascadeStoreWithStringKey,TriggerCascadeNoStoreWithStringKey>;

int main(int argc, char** argv) {
    char buf[4096];
    opm_t opm;
    opm.to_bytes(buf);
    opm.subgroup_type_index = 1;
    opm.deleted = true;
    std::cout << opm.to_string() << std::endl;
    std::cout << mutils::from_bytes<opm_t>(nullptr,buf)->to_string() << std::endl;
    return 0;
}
