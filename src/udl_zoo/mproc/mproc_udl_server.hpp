/**
 * @file mproc_udl_server.hpp
 * @brief The interface for mproc udl server.
 */
#include <sys/types.h>

#include <cinttypes>
#include <string>
#include <memory>

#include <cascade/user_defined_logic_interface.hpp>

namespace derecho {
namespace cascade {

struct mproc_udl_server_arg_t {
    // TODO:
};

/**
 * @class MProcUDLServer mproc_udl_server.hpp "mproc_udl_server.hpp"
 * @brief the UDL server.
 */
class MProcUDLServer {
private:
    /**
     * @fn MProcUDLServer()
     * @brief   The constructor.
     * @param[in]   arg     The argument for the server proc.
     */
    MProcUDLServer(const struct udl_server_arg_t& arg);
    /**
     * @fn start()
     * @brief   Start the UDL server process.
     * @return  The id of the started process.
     */
    virtual pid_t start();
public:
    /**
     * @fn ~MProcUDLServer()
     * @brief   The destructor.
     */
    virtual ~MProcUDLServer();
    /**
     * @fn run_server_process
     * @brief   start a server process
     * @return  Returning 0 means the mproc udl server is finished. Non-zero value 
     *          is the pid of the mproc udl server process.
     */
    static pid_t run_server_process(const struct udl_server_arg_t& arg) {
        MProcUDLServer server(arg);
        return server.start();
    }
};

}
}
