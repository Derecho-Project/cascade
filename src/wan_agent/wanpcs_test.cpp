#include <iostream>
#include <vector>
#include <memory>
#include <derecho/core/derecho.hpp>
#include <derecho/utils/logger.hpp>
#include <cascade/cascade.hpp>
#include <cascade/object.hpp>

using namespace derecho::cascade;
using derecho::ExternalClientCaller;

using WANPCS = WANPersistentCascadeStore<uint64_t,ObjectWithUInt64Key,&ObjectWithUInt64Key::IK,&ObjectWithUInt64Key::IV,ST_FILE>;

class PerfCascadeWatcher : public CascadeWatcher<uint64_t,ObjectWithUInt64Key,&ObjectWithUInt64Key::IK,&ObjectWithUInt64Key::IV> {
public:
    // @overload
    void operator () (derecho::subgroup_id_t sid,
               const uint32_t shard_id,
               const uint64_t& key,
               const ObjectWithUInt64Key& value,
               void* cascade_context) {
                dbg_default_info("Watcher is called with\n\tsubgroup id = {},\n\tshard number = {},\n\tkey = {},\n\tvalue = [hidden].", sid, shard_id, key);
    }
};

int main(int argc, char** argv) {
    /** initialize the parameters */
    derecho::Conf::initialize(argc,argv);

    /** 1 - group building blocks*/
    derecho::CallbackSet callback_set {
        nullptr,    // delivery callback
        nullptr,    // local persistence callback
        nullptr     // global persistence callback
    };
    derecho::SubgroupInfo subgroup_info{[](const std::vector<std::type_index>& subgroup_type_order,
            const std::unique_ptr<derecho::View>& prev_view, derecho::View& curr_view) {
        derecho::subgroup_allocation_map_t subgroup_allocation;
        derecho::subgroup_shard_layout_t subgroup_layout(1);
        // for(const auto& subgroup_type : subgroup_type_order) {
            if(curr_view.num_members < 2) {
                    throw derecho::subgroup_provisioning_exception();
            }
            subgroup_layout[0].emplace_back(curr_view.make_subview(curr_view.members));
            curr_view.next_unassigned_rank = curr_view.members.size();
            // curr_view.next_unassigned_rank = std::max(curr_view.next_unassigned_rank, 1);
            subgroup_allocation.emplace(subgroup_type_order[0], std::move(subgroup_layout));
        // }
        return subgroup_allocation;
    }};
	PerfCascadeWatcher pcw;
    auto wanpcs_factory = [&pcw](persistent::PersistentRegistry* pr, derecho::subgroup_id_t) {
        return std::make_unique<WANPCS>(pr,&pcw);
    };
    /** 2 - create group */
    derecho::Group<WANPCS> group(callback_set,subgroup_info,{&pcw}/*deserialization manager*/,
                                  std::vector<derecho::view_upcall_t>{},
                                  wanpcs_factory);
    std::cout << "Cascade Server finished constructing Derecho group." << std::endl;
    std::cout << "Press ENTER to shutdown..." << std::endl;
    std::cin.get();
    group.barrier_sync();
    group.leave();
    dbg_default_info("Cascade server shutdown.");

    return 0;
}
