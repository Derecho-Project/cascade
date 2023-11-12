/**
 * @file mproc_udl_server.hpp
 * @brief The interface for mproc udl server.
 */
#include <sys/types.h>

#include <cinttypes>
#include <string>
#include <memory>
#include <atomic>
#include <nlohmann/json.hpp>

#include <wsong/ipc/ring_buffer.hpp>
#include <cascade/user_defined_logic_interface.hpp>
#include <cascade/mproc/mproc_ctxt_client.hpp>
#include <cascade/mproc/mproc_manager_api.hpp>

namespace derecho {
namespace cascade {

using json = nlohmann::json;
/**
 * @struct mproc_udl_server_arg_t   mproc_udl_server.hpp   "mproc_udl_server.hpp"
 * @brief The mproc udl server argument
 */
struct mproc_udl_server_arg_t {
    std::string     cwd = ".";
    std::string     objectpool_path;
    std::string     udl;
    json            udl_conf;
    DataFlowGraph::VertexExecutionEnvironment
                    exe_env = DataFlowGraph::VertexExecutionEnvironment::UNKNOWN_EE;
    json            exe_env_conf;
    DataFlowGraph::Statefulness
                    statefulness = DataFlowGraph::Statefulness::UNKNOWN_S;
    uint32_t        num_threads;
    json            edges;
    json            rbkeys;
};

/**
 * TODO:
 */
class MProcContext {
};

/**
 * @class MProcUDLServer mproc_udl_server.hpp "mproc_udl_server.hpp"
 * @brief the UDL server.
 */
class MProcUDLServer {
private:
    std::shared_ptr<OffCriticalDataPathObserver>    ocdpo;              /// the observer
    std::unique_ptr<wsong::ipc::RingBuffer>         object_commit_rb;   /// 1c1p, as consumer
    std::unique_ptr<wsong::ipc::RingBuffer>         ctxt_request_rb;    /// 1cnp, as producer
    std::unique_ptr<wsong::ipc::RingBuffer>         ctxt_response_rb;   /// 1c1p, as consumer
    std::vector<std::thread>                        upcall_thread_pool; /// upcall thread pool
    std::atomic<bool>                               stop_flag;          /// stop flag
    std::unique_ptr<MProcContext>                   ctxt;               /// cascade context
    /**
     * @fn MProcUDLServer()
     * @brief   The constructor.
     * @param[in]   arg     The argument for the server proc.
     */
    MProcUDLServer(const struct mproc_udl_server_arg_t& arg);
    /**
     * @fn start()
     * @brief   Start the UDL server process.
     * @param[in]   wait    Should we wait until it finishes
     * @return  The id of the started process.
     */
    virtual void start(bool wait);
public:
    /**
     * @fn ~MProcUDLServer()
     * @brief   The destructor.
     */
    virtual ~MProcUDLServer();
    /**
     * @fn run_server
     * @brief   start a server process
     * @param[in]   arg     The UDL argument.
     * @param[in]   wait    If wait is true, we don't create a separate thread. Otherwise, we run server in a separate thread.
     * @return  Returning 0 means the mproc udl server is finished. Non-zero value 
     *          is the pid of the mproc udl server process.
     */
    static void run_server(const struct mproc_udl_server_arg_t& arg, bool wait=true) {
        MProcUDLServer server(arg);
        server.start(wait);
    }
};

}
}
