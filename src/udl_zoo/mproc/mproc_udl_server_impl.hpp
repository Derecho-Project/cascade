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
    // 2- create thread pool
    if (arg.num_threads > 1) {
        for (uint32_t worker_id=0;worker_id<arg.num_threads;worker_id ++) {
            // 2.1 - queue
            this->request_queues.emplace_back(std::queue<ObjectCommitRequest>{});
            // 2.2 - locks
            this->request_queue_locks.emplace_back(std::make_unique<std::mutex>());
            // 2.3 - condition variables
            this->request_queue_cvs.emplace_back(std::make_unique<std::condition_variable>());
            // 2.4 - thread
            this->upcall_thread_pool.emplace_back(std::thread([this](uint32_t worker_id){
                // read from queue
                while (!this->stop_flag.load()) {
                    std::unique_lock queue_lock{*this->request_queue_locks[worker_id]};
                    request_queue_cvs[worker_id]->wait(queue_lock,
                        [this,worker_id](){return this->stop_flag || this->request_queues[worker_id].size();
                    });

                    std::queue<ObjectCommitRequest> ocrs = std::move(request_queues[worker_id]);
                    queue_lock.unlock();

                    while (ocrs.size()) {
                        this->process(worker_id,ocrs.front());
                        ocrs.pop();
                    }
                }
            },worker_id));
        }
    }

    //TODO
    // 3 - attach to three ringbuffers
    // 4 - initialize mproc_ctxt
}

template <typename ... CascadeTypes>
void MProcUDLServer<CascadeTypes...>::process(uint32_t worker_id, const ObjectCommitRequest& request) {
    // TODO
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
