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
    for(auto it=dfg_conf[DFG_JSON_GRAPH].cbegin();it!=dfg_conf[DFG_JSON_GRAPH].cend();it++) {
        DataFlowGraphVertex dfgv;
        dfgv.pathname = (*it)[DFG_JSON_PATHNAME];
        /* fix the pathname if it is not ended by a separator */
        if(dfgv.pathname.back() != PATH_SEPARATOR) {
            dfgv.pathname = dfgv.pathname + PATH_SEPARATOR;
        }
        for(size_t i=0;i<(*it)[DFG_JSON_DATA_PATH_LOGIC_LIST].size();i++) {
            std::string udl_uuid = (*it)[DFG_JSON_DATA_PATH_LOGIC_LIST].at(i);
            // shard dispatchers
            dfgv.shard_dispatchers[udl_uuid] = DataFlowGraph::VertexShardDispatcher::ONE;
            if (it->contains(DFG_JSON_SHARD_DISPATCHER_LIST)) {
                dfgv.shard_dispatchers[udl_uuid] = ((*it)[DFG_JSON_SHARD_DISPATCHER_LIST].at(i).get<std::string>() == "ALL")?
                    DataFlowGraph::VertexShardDispatcher::ALL : DataFlowGraph::VertexShardDispatcher::ONE;
            }
            // configurations
            if (it->contains(DFG_JSON_UDL_CONFIG_LIST)) {
                dfgv.configurations.emplace(udl_uuid,(*it)[DFG_JSON_UDL_CONFIG_LIST].at(i));
            } else {
                dfgv.configurations.emplace(udl_uuid,json{});
            }
            // edges
            std::map<std::string,std::string> dest = 
                (*it)[DFG_JSON_DESTINATIONS].at(i).get<std::map<std::string,std::string>>();

            if (dfgv.edges.find(udl_uuid) == dfgv.edges.end()) {
                dfgv.edges.emplace(udl_uuid,std::unordered_map<std::string,bool>{});
            }
            for(auto& kv:dest) {
                if (kv.first.back() == PATH_SEPARATOR) {
                    dfgv.edges[udl_uuid].emplace(kv.first,(kv.second==DFG_JSON_TRIGGER_PUT)?true:false);
                } else {
                    dfgv.edges[udl_uuid].emplace(kv.first + PATH_SEPARATOR,(kv.second==DFG_JSON_TRIGGER_PUT)?true:false);
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
              << "id: " << id << "\n"
              << "description: " << description << "\n";
    for (auto& kv:vertices) {
        std::cout << kv.second.to_string() << std::endl;
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
