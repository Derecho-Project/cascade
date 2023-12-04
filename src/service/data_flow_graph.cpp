#include <derecho/utils/logger.hpp>
#include <cascade/data_flow_graph.hpp>
#include <iostream>
#include <fstream>
#include <cascade/service.hpp>

namespace derecho {
namespace cascade {

DataFlowGraph::DataFlowGraph():id(""),description("uninitialized DFG") {}

DataFlowGraph::DataFlowGraph(const json& dfg_conf):
    id(dfg_conf[DFG_JSON_ID]),
    description(dfg_conf[DFG_JSON_DESCRIPTION]) {
    /* vertex iterator */
    for(auto it=dfg_conf[DFG_JSON_GRAPH].cbegin();it!=dfg_conf[DFG_JSON_GRAPH].cend();it++) {
        DataFlowGraphVertex dfgv;
        dfgv.pathname = (*it)[DFG_JSON_PATHNAME];
        /* fix the pathname if it is not ended by a separator */
        if(dfgv.pathname.back() != PATH_SEPARATOR) {
            dfgv.pathname = dfgv.pathname + PATH_SEPARATOR;
        }
        /* UDL iterator */
        for(size_t i=0;i<(*it)[DFG_JSON_UDL_LIST].size();i++) {
            // udl uuid
            std::string udl_uuid = (*it)[DFG_JSON_UDL_LIST].at(i);
            dfgv.uuids.emplace_back(udl_uuid);

            // shard dispatchers
            dfgv.shard_dispatchers.emplace_back(DataFlowGraph::VertexShardDispatcher::ONE);
            if (it->contains(DFG_JSON_SHARD_DISPATCHER_LIST)) {
                dfgv.shard_dispatchers[i] = ((*it)[DFG_JSON_SHARD_DISPATCHER_LIST].at(i).get<std::string>() == "all")?
                    DataFlowGraph::VertexShardDispatcher::ALL : DataFlowGraph::VertexShardDispatcher::ONE;
            }

            // execution environment
            if (it->contains(DFG_JSON_EXECUTION_ENVIRONMENT_LIST)) {
                if ((*it)[DFG_JSON_EXECUTION_ENVIRONMENT_LIST].at(i)["mode"].get<std::string>() == "process") {
#ifdef ENABLE_MPROC
                    dfgv.execution_environment.emplace_back(DataFlowGraph::VertexExecutionEnvironment::PROCESS);
#else
                    throw derecho::derecho_exception("MPROC is disabled, which the 'process' UDL mode relies on.");
#endif
                } else if ((*it)[DFG_JSON_EXECUTION_ENVIRONMENT_LIST].at(i)["mode"].get<std::string>() == "docker") {
#ifdef ENABLE_MPROC
                    dfgv.execution_environment.emplace_back(DataFlowGraph::VertexExecutionEnvironment::DOCKER);
#else
                    throw derecho::derecho_exception("MPROC is disabled, which the 'docker' UDL mode relies on.");
#endif
                } else {
                    dfgv.execution_environment.emplace_back(DataFlowGraph::VertexExecutionEnvironment::PTHREAD);
                }
                dfgv.execution_environment_conf.emplace_back((*it)[DFG_JSON_EXECUTION_ENVIRONMENT_LIST].at(i));
            } else {
                dfgv.execution_environment.emplace_back(DataFlowGraph::VertexExecutionEnvironment::PTHREAD);
                dfgv.execution_environment_conf.emplace_back(json{});
            }

            // stateful
            dfgv.stateful.emplace_back(DataFlowGraph::Statefulness::STATEFUL);
            if (it->contains(DFG_JSON_UDL_STATEFUL_LIST)) {
                if ((*it)[DFG_JSON_UDL_STATEFUL_LIST].at(i).get<std::string>() == "stateless") {
                    dfgv.stateful[i] = DataFlowGraph::Statefulness::STATELESS;
                } else if ((*it)[DFG_JSON_UDL_STATEFUL_LIST].at(i).get<std::string>() == "singlethreaded") {
                    dfgv.stateful[i] = DataFlowGraph::Statefulness::SINGLETHREADED;
                }
            }

            // hooks
            dfgv.hooks.emplace_back(DataFlowGraph::VertexHook::BOTH);
            if (it->contains(DFG_JSON_UDL_HOOK_LIST)) {
                if ((*it)[DFG_JSON_UDL_HOOK_LIST].at(i).get<std::string>() == "trigger") {
                    dfgv.hooks[i] = DataFlowGraph::VertexHook::TRIGGER_PUT;
                } else if ((*it)[DFG_JSON_UDL_HOOK_LIST].at(i).get<std::string>() == "ordered") {
                    dfgv.hooks[i] = DataFlowGraph::VertexHook::ORDERED_PUT;
                }
            }

            // configurations
            if (it->contains(DFG_JSON_UDL_CONFIG_LIST)) {
                dfgv.configurations.emplace_back((*it)[DFG_JSON_UDL_CONFIG_LIST].at(i));
            } else {
                dfgv.configurations.emplace_back(json{});
            }

            // edges
            std::map<std::string,std::string> dest = 
                (*it)[DFG_JSON_DESTINATIONS].at(i).get<std::map<std::string,std::string>>();

            dfgv.edges.emplace_back(std::unordered_map<std::string,bool>{});
            for(auto& kv:dest) {
                if (kv.first.back() == PATH_SEPARATOR) {
                    dfgv.edges[i].emplace(kv.first,(kv.second==DFG_JSON_TRIGGER_PUT)?true:false);
                } else {
                    dfgv.edges[i].emplace(kv.first + PATH_SEPARATOR,(kv.second==DFG_JSON_TRIGGER_PUT)?true:false);
                }
            }
        }
        vertices.emplace(dfgv.pathname,dfgv);
    }
}

DataFlowGraph::DataFlowGraph(const DataFlowGraph& other):
    id(other.id),
    description(other.description),
    vertices(other.vertices) {}

DataFlowGraph::DataFlowGraph(DataFlowGraph&& other):
    id(other.id),
    description(other.description),
    vertices(std::move(other.vertices)) {}

void DataFlowGraph::dump() const {
    std::cout << "DFG: {\n"
              << "\t" << "id: " << id << "\n"
              << "\t" << "description: " << description << "\n";
    for (auto& kv:vertices) {
        std::cout << kv.second.to_string("\t") << std::endl;
    }
    std::cout << "}" << std::endl;
}

DataFlowGraph::~DataFlowGraph() {}

std::vector<DataFlowGraph> DataFlowGraph::get_data_flow_graphs() {
    std::ifstream i(DFG_JSON_CONF_FILE);
    if (!i.good()) {
        dbg_default_warn("{} is not found.", DFG_JSON_CONF_FILE);
        return {};
    }

    json dfgs_json;
    i >> dfgs_json;
    //TODO: validate dfg.
    std::vector<DataFlowGraph> dfgs;
    for(auto it = dfgs_json.cbegin();it != dfgs_json.cend(); it++) {
        dfgs.emplace_back(DataFlowGraph(*it));
    }
    return dfgs;
}

}
}
