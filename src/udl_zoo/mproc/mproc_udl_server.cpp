#include "mproc_udl_server.hpp"

namespace derecho {
namespace cascade {

template <typename... CascadeTypes>
class MProcUDLManager : public DLLFileManager<CascadeTypes...> {
};

MProcUDLServer::MProcUDLServer(const struct mproc_udl_server_arg_t& arg):
    stop_flag(false) {
    //TODO
    // 1 - load udl to this->ocdpo
    // 2 - attach to three ringbuffers
    // 3 - initialize mproc_ctxt
    // 4 - start the upcall thread pool
}

void MProcUDLServer::start(bool wait) {
    //TODO
}

ServiceClient<CASCADE_SUBGROUP_TYPE_LIST>& MProcUDLServer::get_service_client_ref() const {
    throw derecho_exception{"To be implemented."};
}

MProcUDLServer::~MProcUDLServer() {
    //TODO
    // 1 - stop
    // 2 - destroy mproc_ctxt
    // 3 - detach ring buffers
    // 4 - unload ocdpo
}

}
}
