#include <cascade/service_client_api.hpp>
#include <iostream>
#include <string>
#include <fstream>
#include <typeindex>

using namespace derecho::cascade;

template <typename SubgroupType>
void print_shard_member(ServiceClientAPI& capi, uint32_t subgroup_index, uint32_t shard_index) {
    std::cout << "Subgroup (Type=" << std::type_index(typeid(SubgroupType)).name() << ","
              << "subgroup_index=" << subgroup_index << ","
              << "shard_index="    << shard_index    << ") member list = [";
    auto members = capi.template get_shard_members<SubgroupType>(subgroup_index,shard_index);
    for (auto nid : members) {
        std::cout << nid << ",";
    }
    std::cout << "]" << std::endl;
}

void print_shard_member(ServiceClientAPI& capi, derecho::subgroup_id_t subgroup_id, uint32_t shard_index) {
    std::cout << "subgroup_id=" << subgroup_id << ","
              << "shard_index=" << shard_index << " member list = [";
    auto members = capi.template get_shard_members(subgroup_id,shard_index);
    for (auto nid : members) {
        std::cout << nid << ",";
    }
    std::cout << "]" << std::endl;
}

/* TEST1: members */
void member_test(ServiceClientAPI& capi) {
    // print all members.
    std::cout << "Top Derecho group members = [";
    auto members = capi.get_members();
    for (auto nid: members) {
        std::cout << nid << "," ;
    }
    std::cout << "]" << std::endl;
    // print per Subgroup Members:
    print_shard_member<VCSU>(capi,0,0);
    print_shard_member<VCSS>(capi,0,0);
    print_shard_member<PCSU>(capi,0,0);
    print_shard_member<PCSS>(capi,0,0);
    print_shard_member(capi,0,0);
    print_shard_member(capi,1,0);
    print_shard_member(capi,2,0);
    print_shard_member(capi,3,0);
}

/* TEST2: put/get/remove tests */
void interactive_test(ServiceClientAPI& capi) {
}

int main(int,char**) {
    std::cout << "This is a Service Client Example." << std::endl;

    ServiceClientAPI capi;
    // TEST 1 
    member_test(capi);
    // TEST 2
    interactive_test(capi);
    // 
    return 0;
}
