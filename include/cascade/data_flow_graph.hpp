#pragma once

#include <string>
#include <sstream>
#include <unordered_map>
#include <vector>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

namespace derecho {
namespace cascade {

/**
 * DataFlowGraph is the data structure representing a DFG. A DFG is described by a json string like the following:
 * {
 *     "id": "26639e22-9b3c-11eb-a237-0242ac110002",
 *     "desc": "example DFG"
 *     "graph": [
 *         {
 *             "object_pool_pathname": "/pool0",
 *             "data_path_logic_list": [
 *                 "4e4ecc86-9b3c-11eb-b70c-0242ac110002",
 *                 "4f0373a2-9b3c-11eb-a651-0242ac110002"
 *             ],
 *             "destinations": [
 *                 {"/pool1.1":"put","/pool1.2":"trigger_put"},
 *                 {"/pool2":"put"}
 *             ]
 *         },
 *         {
 *             "object_pool_pathname": "/pool1.1",
 *             "data_path_logic_list": [
 *                 "43fecc86-9b3c-11eb-b70c-0242ac110002"
 *             ],
 *             "destinations": [
 *                 {"/pool3":"put"}
 *             ]
 *         },
 *     ];
 * }
 *
 * Each DFG is composed of an ID, which is a UUID string, and a graph. The graph specifies the DFG structure using a
 * list of vertices. Each vertex has three attributes: "object_pool_pathname", "data_path_logic_list", and "destinations".
 * The "object_pool_pathname" specifies which object pool this vertex is in. The "data_path_logic_list" attribute gives a list
 * of DPLs that should be registered for this object pool. And the "destinations" attribute lists where the output of
 * DPLs should go. Please note that the length of "destinations" should match the size of "data_path_logic_list". Also,
 * each element of the "destinations" value is a dictionary specifying the vertex(object pool) and the method
 * (put/trigger_put).
 */

#define DFG_JSON_ID                     "id"
#define DFG_JSON_DESCRIPTION            "desc"
#define DFG_JSON_GRAPH                  "graph"
#define DFG_JSON_OBJECT_POOL_ID         "object_pool_pathname"
#define DFG_JSON_DATA_PATH_LOGIC_LIST   "data_path_logic_list"
#define DFG_JSON_DESTINATIONS           "destinations"
#define DFG_JSON_PUT                    "put"
#define DFG_JSON_TRIGGER_PUT            "trigger_put"
#define DFG_JSON_CONF_FILE              "dfgs.json"

class DataFlowGraph {
public:
    // the Hex UUID
    const std::string id;
    // description of the DFG
    const std::string description;
    // the vertex table is a map 
    // from object_pool_pathname (or prefix) 
    // to its vertex structure.
    struct DataFlowGraphVertex {
        std::string object_pool_pathname;
        // The edges is a map from DPL uuid string to a vector of destiation object pool ids.
        // An entry "dpl_uuid->[pool1:true,pool2:false,pool3:false]" means three edges from the current vertex to three destination
        // vertices pool1, pool2, and pool3. The input data is processed by DPL specified by dpl_uuid.
        std::unordered_map<std::string,std::unordered_map<std::string,bool>> edges;
        // to string
        inline std::string to_string() const {
            std::ostringstream out;
            out << typeid(*this).name() << ":" << object_pool_pathname << "{\n";
            for (auto& e:edges) {
                for (auto& pool:e.second){
                    out << "\t-[dpl:" << e.first << "]-" << (pool.second?'*':'-') << "->" << pool.first <<"\n";
                }
            }
            out << "}";
            return out.str();
        }
    };
    std::unordered_map<std::string,DataFlowGraphVertex> vertices;
    /**
     * Constructors
     */
    DataFlowGraph();
    DataFlowGraph(const json& dfg_conf);
    DataFlowGraph(const DataFlowGraph& other);
    DataFlowGraph(DataFlowGraph&& other);
    /**
     * for debug
     */
    void dump() const;
    /**
     * Destructor
     */
    virtual ~DataFlowGraph();

    /**
     * Load the data flow graph from the default DFG configuration file, which contains a list of DFG jsons.
     */
    static std::vector<DataFlowGraph> get_data_flow_graphs();
};

}
}
