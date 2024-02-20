#pragma once

#include <chrono>
using namespace std::chrono;

namespace derecho {
namespace cascade {

template <typename FirstCascadeType, typename ... RestCascadeTypes>
MProcUDLServer<FirstCascadeType,RestCascadeTypes...>::MProcUDLServer(const struct mproc_udl_server_arg_t& arg):
    statefulness(arg.statefulness),
    preset_worker_id(arg.worker_id),
    stop_flag(false) {
    // 1 - load udl to this->ocdpo
    this->user_defined_logic_manager = UserDefinedLogicManager<FirstCascadeType,RestCascadeTypes...>::create(this);
    this->ocdpo =
        std::move(this->user_defined_logic_manager->get_observer(arg.udl_uuid,arg.udl_conf));
    // 2- create thread pool
    if (arg.num_threads > 1) {
        for (uint32_t worker_id=0;worker_id<arg.num_threads;worker_id ++) {
            // 2.1 - queue
            this->request_queues.emplace_back(std::queue<ObjectCommitRequestHeader>{});
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

                    std::queue<ObjectCommitRequestHeader> ocrs = std::move(request_queues[worker_id]);
                    queue_lock.unlock();

                    while (ocrs.size()) {
                        this->process(worker_id,ocrs.front());
                        ocrs.pop();
                    }
                }
            },worker_id));
        }
    }
    // 3 - attach to ring buffer
    if (arg.rbkeys.size() != 3) {
        throw derecho_exception("mproc udl server arg is invalid: expecting 3 ring buffer keys");
    }
    this->object_commit_rb  = wsong::ipc::RingBuffer::get_ring_buffer(arg.rbkeys[0].template get<key_t>());
    /* TODO
    this->ctxt_request_rb   = wsong::ipc::RingBuffer::get_ring_buffer(arg.rbkeys[1].template get<key_t>());
    this->ctxt_response_rb  = wsong::ipc::RingBuffer::get_ring_buffer(arg.rbkeys[2].template get<key_t>());
    */
}

template <typename FirstCascadeType, typename ... RestCascadeTypes>
void MProcUDLServer<FirstCascadeType,RestCascadeTypes...>::process(uint32_t worker_id, const ObjectCommitRequestHeader& request) {
    dbg_default_trace("Handle it to OCDPO.");
    (*this->ocdpo)(
        request.sender_id,
        *request.get_key_string(),
        request.prefix_length,
        request.version,
        request.get_object_nocopy<typename FirstCascadeType::ObjectType>().get(),
        *request.get_output(),
        this,
        worker_id);
    dbg_default_trace("OCDPO Finished.");
}

template <typename FirstCascadeType, typename ... RestCascadeTypes>
void MProcUDLServer<FirstCascadeType, RestCascadeTypes...>::pump_request() {
    uint8_t     request_bytes[OBJECT_COMMIT_REQUEST_SIZE] __attribute__((aligned(PAGE_SIZE)));
    uint32_t    next_worker = 0;
    while(!stop_flag) {
        try {
            this->object_commit_rb->consume(reinterpret_cast<void*>(request_bytes),OBJECT_COMMIT_REQUEST_SIZE,1s);
        } catch (const wsong::ws_timeout_exp& toex) {
            continue;
        }

        ObjectCommitRequestHeader* req = reinterpret_cast<ObjectCommitRequestHeader*>(request_bytes);
        dbg_default_trace("Object commit request of {} bytes retrieved.", req->total_size());

        if (request_queues.size()) {
            switch(this->statefulness) {
            case DataFlowGraph::Statefulness::STATEFUL:
                next_worker = (std::hash<std::string>{}(*req->get_key_string())) % request_queues.size();
                break;
            default:
                next_worker = (next_worker+1) % request_queues.size();
            }

            std::unique_lock queue_lock(*this->request_queue_locks[next_worker]);
            request_queues[next_worker].push(*req); // TODO: this can be improved. No need to copy a whole page.
            queue_lock.unlock();
            request_queue_cvs[next_worker]->notify_one();
        } else {
            // handle it here
            process(preset_worker_id,*req);
        }
    }
}

template <typename FirstCascadeType, typename ... RestCascadeTypes>
void MProcUDLServer<FirstCascadeType,RestCascadeTypes...>::start(bool wait) {
    if (wait) {
        pump_request();
    } else {
        pump_thread = std::thread(&MProcUDLServer<FirstCascadeType,RestCascadeTypes...>::pump_request,this);
    }
}

template <typename FirstCascadeType, typename ... RestCascadeTypes>
ServiceClient<FirstCascadeType,RestCascadeTypes...>& MProcUDLServer<FirstCascadeType,RestCascadeTypes...>::get_service_client_ref() const {
    //TODO
    throw derecho_exception{"To be implemented."};
}

template <typename FirstCascadeType, typename ... RestCascadeTypes>
MProcUDLServer<FirstCascadeType,RestCascadeTypes...>::~MProcUDLServer() {
    this->stop_flag.store(true);

    // 1 - stop threads
    if (pump_thread.joinable()) {
        pump_thread.join();
    }
    for (auto& t: upcall_thread_pool) {
        if (t.joinable()) {
            t.join();
        }
    }
    // 2 - destroy mproc_ctxt TODO
    // 3 - unload ocdpo: automatically in destructor
}

}
}
