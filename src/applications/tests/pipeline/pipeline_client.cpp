#include <iostream>
#include <cascade/cascade.hpp>
#include <cascade/service_client_api.hpp>
#include <cascade/service_types.hpp>
#include <sys/prctl.h>
#include "pipeline_common.hpp"

using namespace derecho::cascade;

#define PROC_NAME   "pipeline_client"

#ifndef NUMBER_OF_DISTINCT_OBJECTS
#define NUMBER_OF_DISTINCT_OBJECTS (4096)
#endif

static void make_pipeline_workload(uint32_t payload_size, const std::string& key_prefix, std::vector<ObjectWithStringKey>& objects) {
    uint32_t buf_size = payload_size - 128;
    uint8_t* buf = (uint8_t*)malloc(buf_size);
    memset(buf,'A',buf_size);
    for (uint32_t i=0;i<NUMBER_OF_DISTINCT_OBJECTS;i++) {
        objects.emplace_back(key_prefix+std::to_string(i),buf,buf_size);
    }
    free(buf);
}

#ifdef ENABLE_EVALUATION

static std::set<std::string> collect_dfgs_object_pools() {
    std::set<std::string> object_pools;
    for (const auto& dfg: DataFlowGraph::get_data_flow_graphs()) {
        for (const auto& v: dfg.vertices) {
            object_pools.emplace(v.first);
            for (const auto& per_ocdpo_edges: v.second.edges) {
                for (const auto& tkv: per_ocdpo_edges) {
                    object_pools.emplace(tkv.first);
                }
            }
        }
    }
    return object_pools;
}

static std::pair<uint32_t,uint32_t> get_subgroup(ServiceClientAPI& capi, const std::string& object_pool_pathname) {
    auto opm = capi.find_object_pool(object_pool_pathname);
    return std::pair{opm.subgroup_type_index,opm.subgroup_index};
}

static std::set<std::pair<uint32_t,uint32_t>> collect_subgroups(ServiceClientAPI& capi) {
    std::set<std::pair<uint32_t,uint32_t>> subgroups;
    for (auto pathname:collect_dfgs_object_pools()) {
        subgroups.emplace(get_subgroup(capi,pathname));
    }
    return subgroups;
}

template <typename LastType>
static void __dump_subgroup_timestamp(ServiceClientAPI& capi, const std::string& filename, uint32_t subgroup_type_index, uint32_t subgroup_index) {
    if (subgroup_type_index == 0) {
        for(uint32_t shard_index = 0; shard_index < capi.template get_number_of_shards<LastType>(subgroup_index); shard_index ++) {
            auto result = capi.template dump_timestamp<LastType>(filename,subgroup_index,shard_index);
            result.get();
        }
    } else {
        throw derecho::derecho_exception("Error! subgroup_index type is out of boundary.");
    }
}

template <typename FirstType,typename SecondType, typename...RestTypes>
static void __dump_subgroup_timestamp(ServiceClientAPI& capi, const std::string& filename, uint32_t subgroup_type_index,uint32_t subgroup_index) {
    if (subgroup_type_index == 0) {
        __dump_subgroup_timestamp<FirstType>(capi,filename,subgroup_type_index,subgroup_index);
    } else {
        __dump_subgroup_timestamp<SecondType,RestTypes...>(capi,filename,subgroup_type_index-1,subgroup_index);
    }
}

template <typename...SubgroupTypes>
static void dump_subgroup_timestamp(ServiceClient<SubgroupTypes...>& capi, const std::string& filename, uint32_t subgroup_type_index,uint32_t subgroup_index) {
    __dump_subgroup_timestamp<SubgroupTypes...>(capi,filename,subgroup_type_index,subgroup_index);
}
#endif

int main(int argc, char** argv) {
    if (prctl(PR_SET_NAME, PROC_NAME, 0, 0, 0) != 0) {
        dbg_default_debug("Failed to set proc name to {},", PROC_NAME);
    }
    auto& capi = ServiceClientAPI::get_service_client();

    if (argc != 6) {
        std::cout << "Usage:" << argv[0] << " <trigger_put|put_and_forget> <object pool pathname> <member selection policy> <max rate> <duration in sec>" << std::endl;
        return -1;
    }

    bool trigger = (std::string{argv[1]}=="trigger_put")?true:false;
    std::string pathname = argv[2];
    std::string member_selection_policy = argv[3];
    uint64_t max_rate_ops = std::stoul(argv[4]);
    uint64_t duration_sec = std::stoul(argv[5]);
    uint64_t payload_size = derecho::getConfUInt64(derecho::Conf::DERECHO_MAX_P2P_REQUEST_PAYLOAD_SIZE);

    // 1 - create the workload.
    std::vector<ObjectWithStringKey> objects;
    make_pipeline_workload(payload_size,pathname+"/k",objects);

    // 2 - send messages accordingly, and log the timestamp.
    uint64_t interval_ns = 1e9/max_rate_ops;
    uint64_t now_ns = get_walltime();
    uint64_t next_ns = 0;
    uint64_t end_ns = now_ns + duration_sec*1e9;
#ifdef ENABLE_EVALUATION
    node_id_t my_node_id = capi.get_my_id();
    uint64_t msg_id = 0;
#endif

    while (now_ns < end_ns) {
        if (now_ns + 500 >= next_ns) {
            next_ns = now_ns + interval_ns;
#ifdef ENABLE_EVALUATION
            objects.at(now_ns%NUMBER_OF_DISTINCT_OBJECTS).set_message_id(msg_id);
            TimestampLogger::log(TLT_READY_TO_SEND,my_node_id,msg_id,get_walltime());
            objects.at(now_ns%NUMBER_OF_DISTINCT_OBJECTS).set_message_id(msg_id);
#endif
            if (trigger) {
                capi.trigger_put(objects.at(now_ns%NUMBER_OF_DISTINCT_OBJECTS));
            } else {
                capi.put_and_forget(objects.at(now_ns%NUMBER_OF_DISTINCT_OBJECTS));
            }
#ifdef ENABLE_EVALUATION
            TimestampLogger::log(TLT_EC_SENT,my_node_id,msg_id,get_walltime());
            msg_id ++;
#endif
        } else {
            usleep((next_ns-now_ns-500)/1e3); // sleep in microseconds.
        }
        now_ns = get_walltime();
    }

#ifdef ENABLE_EVALUATION
    // wait for 2 seconds so that all messages has been processed.
    usleep(2000000);
    // 3 - flush timestamp log
    // TODO: This does not support overlapping subgroups. Those should be eliminated.
    for(auto& subgroup_pair: collect_subgroups(capi)) {
        dump_subgroup_timestamp(capi,"pipeline.log",std::get<0>(subgroup_pair),std::get<1>(subgroup_pair));
    }
    TimestampLogger::flush("pipeline.log"); 
#endif
    return 0;
}
