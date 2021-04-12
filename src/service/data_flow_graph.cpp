#include <cascade/data_flow_graph.hpp>
#include <iostream>
#include <fstream>

namespace derecho {
namespace cascade {

DataFlowGraph::DataFlowGraph():id(""),description("uninitialized DFG") {}

DataFlowGraph::DataFlowGraph(const json& dfg_conf):
    id(dfg_conf[DFG_JSON_ID]),
    description(dfg_conf[DFG_JSON_DESCRIPTION]) {
    for(auto it=dfg_conf[DFG_JSON_GRAPH].cbegin();it!=dfg_conf[DFG_JSON_GRAPH].cend();it++) {
        DataFlowGraphVertex dfgv;
        dfgv.object_pool_id = (*it)[DFG_JSON_OBJECT_POOL_ID];
        for(size_t i=0;i<(*it)[DFG_JSON_DATA_PATH_LOGIC_LIST].size();i++) {
            std::string dpl_uuid = (*it)[DFG_JSON_DATA_PATH_LOGIC_LIST].at(i);
            std::unordered_map<std::string,std::string> dest = 
                (*it)[DFG_JSON_DESTINATIONS].at(i).get<std::unordered_map<std::string,std::string>>();

            for(auto& kv:dest) {
                dfgv.edges[dpl_uuid].emplace(kv.first,(kv.second==DFG_JSON_TRIGGER_PUT)?true:false);
            }
        }
        vertices.emplace(dfgv.object_pool_id,dfgv);
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
    //TODO: verify and validate dfg.
    std::ifstream i(DFG_JSON_CONF_FILE);
    json dfgs_json;
    i >> dfgs_json;
    std::vector<DataFlowGraph> dfgs;
    for(auto it = dfgs_json.cbegin();it != dfgs_json.cend(); it++) {
        dfgs.emplace_back(DataFlowGraph(*it));
    }
    return dfgs;
}

}
}
