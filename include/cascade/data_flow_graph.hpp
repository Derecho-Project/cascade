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
 *             "pathname": "/pool0/",
 *             "shard_dispatcher_list": [ 
 *                 "one",
 *                 "all"
 *             ],
 *             "user_defined_logic_list": [
 *                 "4e4ecc86-9b3c-11eb-b70c-0242ac110002",
 *                 "4f0373a2-9b3c-11eb-a651-0242ac110002"
 *             ],
 *             "user_defined_logic_config_list": [
 *                 {"udl_config_op1":"val1","udl_config_op2":"val2"},
 *                 {"udl_config_op1":"val1","udl_config_op2":"val2"}
 *             ],
 *             "destinations": [
 *                 {"/pool1.1/":"put","/pool1.2/":"trigger_put"},
 *                 {"/pool2/":"put"}
 *             ]
 *         },
 *         {
 *             "pathname": "/pool1.1/",
 *             "user_defined_logic_list": [
 *                 "43fecc86-9b3c-11eb-b70c-0242ac110002"
 *             ],
 *             "destinations": [
 *                 {"/pool3/":"put"}
 *             ]
 *         },
 *     ];
 * }
 *
 * Each DFG is composed of an ID, which is a UUID string, and a graph. The graph specifies the DFG structure using a
 * list of vertices. Each vertex has three mandatory and two optional attributes.
 *
 * 1) The MANDATORY "pathname" attribute specifies a folder for this vertex. 
 * 2) The OPTIONAL "shard_dispatcher_list" attribute specifies how a k/v pair is dispatched to shard members for each
 * of the UDLs. The only two options supported are "all" and "one", meaning that this k/v pair is handled by all members
 * or just one of the members. Cascade will randomly pick one of the node using key hash and node's rank in the shard.
 * This option is only relevant to put operation and does not apply to trigger put operation. The default value is "ONE".
 * 3) The MANDATORY "user_defined_logic_list" attribute gives a list of UDLs that should be registered for this vertex.
 * 4) The OPTIONAL "user_defined_logic_config_list" is for a list of the json configurations for all UDLs listed in
 * "user_Defined_logic_list".
 * 5) The "destinations" attribute lists the vertices where the output of UDLs should go. Each element of the 
 * "destinations" value is a dictionary specifying the vertex and the method (put/trigger_put).
 *
 * Please note that the lengthes of "destinations", "user_defined_logic_list", and "user_defined_logic_config_list" 
 * should match each other. 
 */

#define DFG_JSON_ID                     "id"
#define DFG_JSON_DESCRIPTION            "desc"
#define DFG_JSON_GRAPH                  "graph"
#define DFG_JSON_PATHNAME               "pathname"
#define DFG_JSON_SHARD_DISPATCHER_LIST  "shard_dispatcher_list"
#define DFG_JSON_DATA_PATH_LOGIC_LIST   "user_defined_logic_list"
#define DFG_JSON_UDL_CONFIG_LIST        "user_defined_logic_config_list"
#define DFG_JSON_DESTINATIONS           "destinations"
#define DFG_JSON_PUT                    "put"
#define DFG_JSON_TRIGGER_PUT            "trigger_put"
#define DFG_JSON_CONF_FILE              "dfgs.json"

class DataFlowGraph {
public:
    enum VertexShardDispatcher {
        ONE,
        ALL,
    };

    // the Hex UUID
    const std::string id;
    // description of the DFG
    const std::string description;
    // the vertex table is a map 
    // from pathname (or prefix) 
    // to its vertex structure.
    struct DataFlowGraphVertex {
        std::string pathname;
        // The optional shard dispatcher configuration
        // uuid->shard_dispatcher
        std::unordered_map<std::string,VertexShardDispatcher> shard_dispatchers;
        // The optional initialization string for each UUID
        std::unordered_map<std::string,json> configurations;
        // The edges is a map from UDL uuid string to a vector of destiation vertex pathnames.
        // An entry "udl_uuid->[pool1:true,pool2:false,pool3:false]" means three edges from the current vertex to three destination
        // vertices pool1, pool2, and pool3. The input data is processed by UDL specified by udl_uuid.
        std::unordered_map<std::string,std::unordered_map<std::string,bool>> edges;
        // to string
        inline std::string to_string() const {
            std::ostringstream out;
            out << typeid(*this).name() << ":" << pathname << ", "
                << " {\n";
            for (auto& sd: shard_dispatchers) {
                out << "\t-{udl:" << sd.first << "} uses shard dispatcher:" << sd.second << "\n";
            }
            for (auto& c: configurations) {
                out << "\t-{udl:" << c.first << "} is configured with \"" << c.second << "\"\n";
            }
            for (auto& e:edges) {
                for (auto& pool:e.second){
                    out << "\t-[udl:" << e.first << "]-" << (pool.second?'*':'-') << "->" << pool.first <<"\n";
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
