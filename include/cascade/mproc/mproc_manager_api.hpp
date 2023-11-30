/**
 * @file mproc_manager_api.hpp
 * @brief The interface for mproc connector
 */

#include <cinttypes>

#include <string>
#include <wsong/ipc/ring_buffer.hpp>
#include <nlohmann/json.hpp>
#include <rpc/client.h>

#include "../data_flow_graph.hpp"

/**
 * @brief The port number for mproc manager
 */
#define MPROC_MANAGER_PORT  (30001)
/**
 * @brief The hostname of mproc manager
 */
#define MPROC_MANAGER_HOST  "127.0.0.1"

using json = nlohmann::json;

namespace derecho {
namespace cascade {

/**
 * @struct mproc_mgr_req_start_udl_t
 * @brief The data structure for starting a UDL process/container/VM
 */
struct mproc_mgr_req_start_udl_t {
    std::string     object_pool_path;
    std::string     uuid;
    json            udl_conf;
    DataFlowGraph::VertexExecutionEnvironment
                    execution_environment;
    json            execution_environment_conf;
    DataFlowGraph::Statefulness
                    stateful;
    std::unordered_map<std::string,bool>
                    edges;      /// output
    key_t           shm_key;    /// The shared memory for receiving address
};

/**
 * @typedef mproc_mgr_res_start_udl_t
 * @brief The data struct for return value from starting a UDL process/container/VM
 */
struct mproc_mgr_res_start_udl_t {
    uint32_t        error_code;     /// 0 for success, other values for error.
    std::string     info;           /// Extra information about the error.
    std::string     mproc_udl_id;   /// The id of the mproc udl.
    key_t           rb_key;         /// The ring buffer for submitting incoming objects.
};

/**
 * @class MProcManagerAPI   mproc_manager_api.hpp "cascade/mproc/mproc_manager_api.hpp"
 * @brief The API of the mproc manager.
 */
class MProcManagerAPI {
private:
    ::rpc::client client;
public:
    /**
     * @brief The constructor
     */
    MProcManagerAPI();
    /**
     * @brief Call mproc manager api.
     * @param[in]   req     The request.
     * @param[out]  res     The response.
     */
    void start_udl(const mproc_mgr_req_start_udl_t& req,mproc_mgr_res_start_udl_t& res);
};

}
}
