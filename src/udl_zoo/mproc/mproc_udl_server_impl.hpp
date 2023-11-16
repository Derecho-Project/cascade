#pragma once

namespace derecho {
namespace cascade {

template <typename ... CascadeTypes>
MProcUDLServer<CascadeTypes...>::MProcUDLServer(const struct mproc_udl_server_arg_t& arg):
    stop_flag(false) {
    // 1 - load udl to this->ocdpo
    this->user_defined_logic_manager = UserDefinedLogicManager<CascadeTypes...>::create(this);
    this->ocdpo =
        std::move(this->user_defined_logic_manager->get_observer(arg.udl_uuid,arg.udl_conf));
    //TODO
    // 2 - attach to three ringbuffers
    // 3 - initialize mproc_ctxt
    // 4 - start the upcall thread pool
}

template <typename ... CascadeTypes>
void MProcUDLServer<CascadeTypes...>::start(bool wait) {
    //TODO
}

template <typename ... CascadeTypes>
ServiceClient<CascadeTypes...>& MProcUDLServer<CascadeTypes...>::get_service_client_ref() const {
    throw derecho_exception{"To be implemented."};
}

template <typename ... CascadeTypes>
MProcUDLServer<CascadeTypes...>::~MProcUDLServer() {
    //TODO
    // 1 - stop
    // 2 - destroy mproc_ctxt
    // 3 - detach ring buffers
    // 4 - unload ocdpo
}

}
}
