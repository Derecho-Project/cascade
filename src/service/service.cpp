#include "cascade/cascade.hpp"
#include "cascade/service.hpp"

namespace derecho {
namespace cascade {

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

typedef enum {
    OCDP_MULTICAST,
    OCDP_P2P
} ocdp_t;

static std::map<uint32_t,std::vector<uint32_t>> parse_worker_cpu_affinity(const ocdp_t ocdp_type) {
    std::map<uint32_t,std::vector<uint32_t>> ret;
    if (derecho::hasCustomizedConfKey(CASCADE_CONTEXT_WORKER_CPU_AFFINITY) &&
        !derecho::getConfString(CASCADE_CONTEXT_WORKER_CPU_AFFINITY).empty()) {
        try {
            auto worker_cpu_affinity = json::parse(derecho::getConfString(CASCADE_CONTEXT_WORKER_CPU_AFFINITY));
            for(auto affinity:worker_cpu_affinity[(ocdp_type==OCDP_MULTICAST)?"p2p_ocdp":"multicast_ocdp"].items()) {
                uint32_t worker_id = std::stoul(affinity.key());
                ret.emplace(worker_id,parse_cpu_gpu_list(affinity.value()));
            }
        } catch(json::exception& jsone) {
            dbg_default_error("Failed to parse {}:{}, execption:{}",
                CASCADE_CONTEXT_WORKER_CPU_AFFINITY,derecho::getConfString(CASCADE_CONTEXT_WORKER_CPU_AFFINITY),
                jsone.what());
        }
    }
    return ret;
}

ResourceDescriptor::ResourceDescriptor():
    cpu_cores(parse_cpu_gpu_list(derecho::hasCustomizedConfKey(CASCADE_CONTEXT_CPU_CORES)?derecho::getConfString(CASCADE_CONTEXT_CPU_CORES):"")),
    multicast_ocdp_worker_to_cpu_cores(parse_worker_cpu_affinity(OCDP_MULTICAST)),
    p2p_ocdp_worker_to_cpu_cores(parse_worker_cpu_affinity(OCDP_P2P)),
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
    for(auto affinity:multicast_ocdp_worker_to_cpu_cores) {
        os_affinity << "(multicast worker-" << affinity.first << ":";
        for (auto core: affinity.second) {
            os_affinity << core << ",";
        }
        os_affinity << "); ";
    }
    for(auto affinity:p2p_ocdp_worker_to_cpu_cores) {
        os_affinity << "(p2p worker-" << affinity.first << ":";
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
