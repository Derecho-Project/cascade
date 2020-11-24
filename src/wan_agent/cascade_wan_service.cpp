#include <cascade/cascade.hpp>
#include <cascade/service.hpp>

namespace derecho {
namespace cascade {

SubgroupAllocationPolicy parse_json_subgroup_policy(const json& jconf) {
    if(!jconf.is_object() || !jconf[JSON_CONF_LAYOUT].is_array()) {
        dbg_default_error("parse_json_subgroup_policy cannot parse {}.", jconf.get<std::string>());
        throw derecho::derecho_exception("parse_json_subgroup_policy cannot parse" + jconf.get<std::string>());
    }

    SubgroupAllocationPolicy subgroup_allocation_policy;
    subgroup_allocation_policy.identical_subgroups = false;
    subgroup_allocation_policy.num_subgroups = jconf[JSON_CONF_LAYOUT].size();
    subgroup_allocation_policy.shard_policy_by_subgroup = std::vector<ShardAllocationPolicy>();

    for(auto subgroup_it : jconf[JSON_CONF_LAYOUT]) {
        ShardAllocationPolicy shard_allocation_policy;
        size_t num_shards = subgroup_it[MIN_NODES_BY_SHARD].size();
        if(subgroup_it[MAX_NODES_BY_SHARD].size() != num_shards || subgroup_it[DELIVERY_MODES_BY_SHARD].size() != num_shards || subgroup_it[PROFILES_BY_SHARD].size() != num_shards) {
            dbg_default_error("parse_json_subgroup_policy: shards does not match in at least one subgroup: {}",
                              subgroup_it.get<std::string>());
            throw derecho::derecho_exception("parse_json_subgroup_policy: shards does not match in at least one subgroup:" + subgroup_it.get<std::string>());
        }
        shard_allocation_policy.even_shards = false;
        shard_allocation_policy.num_shards = num_shards;
        shard_allocation_policy.min_num_nodes_by_shard = subgroup_it[MIN_NODES_BY_SHARD].get<std::vector<int>>();
        shard_allocation_policy.max_num_nodes_by_shard = subgroup_it[MAX_NODES_BY_SHARD].get<std::vector<int>>();
        std::vector<Mode> delivery_modes_by_shard;
        for(auto it : subgroup_it[DELIVERY_MODES_BY_SHARD]) {
            if(it == DELIVERY_MODE_RAW) {
                shard_allocation_policy.modes_by_shard.push_back(Mode::UNORDERED);
            } else {
                shard_allocation_policy.modes_by_shard.push_back(Mode::ORDERED);
            }
        }
        shard_allocation_policy.profiles_by_shard = subgroup_it[PROFILES_BY_SHARD].get<std::vector<std::string>>();
        subgroup_allocation_policy.shard_policy_by_subgroup.emplace_back(std::move(shard_allocation_policy));
    }
    return subgroup_allocation_policy;
}

}  // namespace cascade
}  // namespace derecho
