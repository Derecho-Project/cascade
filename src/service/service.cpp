#include <cascade/cascade.hpp>
#include <cascade/service.hpp>

namespace derecho {
namespace cascade {

SubgroupAllocationPolicy parse_json_subgroup_policy(const json& jconf) {
    if (!jconf.is_object() || !jconf[JSON_CONF_LAYOUT].is_array()) {
        dbg_default_error("parse_json_subgroup_policy cannot parse {}.", jconf.get<std::string>());
        throw derecho::derecho_exception("parse_json_subgroup_policy cannot parse" + jconf.get<std::string>());
    }

    SubgroupAllocationPolicy subgroup_allocation_policy;
    subgroup_allocation_policy.identical_subgroups = false;
    subgroup_allocation_policy.num_subgroups = jconf[JSON_CONF_LAYOUT].size();
    subgroup_allocation_policy.shard_policy_by_subgroup = std::vector<ShardAllocationPolicy>();

    for (auto subgroup_it:jconf[JSON_CONF_LAYOUT]) {
        ShardAllocationPolicy shard_allocation_policy;
        size_t num_shards = subgroup_it[MIN_NODES_BY_SHARD].size();
        if (subgroup_it[MAX_NODES_BY_SHARD].size() != num_shards ||  
            subgroup_it[DELIVERY_MODES_BY_SHARD].size() != num_shards ||
            subgroup_it[PROFILES_BY_SHARD].size() != num_shards) {
            dbg_default_error("parse_json_subgroup_policy: shards does not match in at least one subgroup: {}",
				subgroup_it.get<std::string>());
            throw derecho::derecho_exception("parse_json_subgroup_policy: shards does not match in at least one subgroup:" + 
				subgroup_it.get<std::string>());
        }
        shard_allocation_policy.even_shards = false;
        shard_allocation_policy.num_shards = num_shards;
        shard_allocation_policy.min_num_nodes_by_shard = subgroup_it[MIN_NODES_BY_SHARD].get<std::vector<int>>();
        shard_allocation_policy.max_num_nodes_by_shard = subgroup_it[MAX_NODES_BY_SHARD].get<std::vector<int>>();
        std::vector<Mode> delivery_modes_by_shard;
        for(auto it: subgroup_it[DELIVERY_MODES_BY_SHARD]) {
            if (it == DELIVERY_MODE_RAW) {
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

/** 
 * cpu/gpu list examples: 
 * cpu_cores = 0,1,2,3
 * cpu_cores = 0,1-5,6,8
 * cpu_cores = 0-15
 * gpus = 0,1
 **/
static std::vector<uint32_t> parse_cpu_gpu_list(const std::string& str) {
    std::string core_string(str);
    std::vector<uint32_t> ret;
    if (core_string.size() == 0) {
        core_string = "0-";
        core_string = core_string + std::to_string(std::thread::hardware_concurrency()-1);
    }
    std::istringstream in_str(core_string);
    std::string token;
    while(std::getline(in_str,token,',')) {
        std::string::size_type pos = token.find("-");
        if (pos == std::string::npos) {
            // a single core
            ret.emplace_back(std::stoul(token));
        } else {
            // a range of cores
            uint32_t start = std::stoul(token.substr(0,pos));
            uint32_t end   = std::stoul(token.substr(pos+1));
            while(start<=end) {
                ret.emplace_back(start++);
            }
        }
    }
    return ret;
}

static std::map<uint32_t,std::vector<uint32_t>> parse_worker_cpu_affinity() {
    std::map<uint32_t,std::vector<uint32_t>> ret;
    if (derecho::hasCustomizedConfKey(CASCADE_CONTEXT_WORKER_CPU_AFFINITY)) {
        auto worker_cpu_affinity = json::parse(derecho::getConfString(CASCADE_CONTEXT_WORKER_CPU_AFFINITY));
        for(auto affinity:worker_cpu_affinity.items()) {
            uint32_t worker_id = std::stoul(affinity.key());
            ret.emplace(worker_id,parse_cpu_gpu_list(affinity.value()));
        }
    }
    return ret;
}

ResourceDescriptor::ResourceDescriptor():
    cpu_cores(parse_cpu_gpu_list(derecho::hasCustomizedConfKey(CASCADE_CONTEXT_CPU_CORES)?derecho::getConfString(CASCADE_CONTEXT_CPU_CORES):"")),
    worker_to_cpu_cores(parse_worker_cpu_affinity()),
    gpus(parse_cpu_gpu_list(derecho::hasCustomizedConfKey(CASCADE_CONTEXT_GPUS)?derecho::getConfString(CASCADE_CONTEXT_GPUS):"")) {
}

void ResourceDescriptor::dump() const {
    dbg_default_info("Cascade Context Resource:");
    std::ostringstream os_cores;
    for (auto core: cpu_cores) {
        os_cores << core << ",";
    }
    dbg_default_info("cpu cores={}", os_cores.str());
    std::ostringstream os_gpus;
    for (auto gpu: gpus) {
        os_gpus << gpu << ",";
    }
    dbg_default_info("gpus={}", os_gpus.str());
    std::ostringstream os_affinity;
    for(auto affinity:worker_to_cpu_cores) {
        os_affinity << "(worker-" << affinity.first << ":";
        for (auto core: affinity.second) {
            os_affinity << core << ",";
        }
        os_affinity << "); ";
    }
    dbg_default_info("cpu affinity={}", os_affinity.str());
}

ResourceDescriptor::~ResourceDescriptor() {
    // destructor
}

}
}
