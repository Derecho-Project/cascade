#pragma once

namespace derecho {
namespace cascade {

template <typename FirstCascadeType,typename ... RestCascadeTypes>
MProcUDLClient<FirstCascadeType,RestCascadeTypes...>::MProcUDLClient(const key_t object_commit_rbkey) {
    try {
        object_commit_rb = wsong::ipc::RingBuffer::get_ring_buffer(object_commit_rbkey);
    } catch (const wsong::ws_exp& wse) {
        throw derecho::derecho_exception(wse.what());
    }
}

template <typename FirstCascadeType,typename ... RestCascadeTypes>
void MProcUDLClient<FirstCascadeType,RestCascadeTypes...>::submit(
    const node_id_t             sender_id,
    const std::string&          full_key_string,
    const uint32_t              prefix_length,
    persistent::version_t       version,
    const mutils::ByteRepresentable* const
                                value,
    const std::unordered_map<std::string,bool>&
                                outputs,
    uint32_t                    worker_id) {
    // STEP 1 - create an object commit request
    DEFINE_OBJECT_COMMIT_REQUEST(request);
    // STEP 2 - set up the request
    request->sender_id      =   sender_id;
    request->prefix_length  =   prefix_length;
    request->version        =   version;
    // TODO: here we need to test if the size of the value/object can fit in the inline space
    // (OBJECT_COMMIT_REQUEST_SIZE). If not, we should use the shared memory mechanism 
    // (OBJECT_COMMIT_REQUEST_MEMORY_SHM) to send it. By default, we use inline operation
    // (OBJECT_COMMIT_REQUEST_MEMORY_INLINE).
    //
    request->flags          =   OBJECT_COMMIT_REQUEST_MEMORY_INLINE;
    request->shm_key        =   0;
    request->shm_offset     =   0;
    // serialization
    request->output_edges_offset
                            =   mutils::to_bytes(full_key_string,reinterpret_cast<uint8_t*>(request->rest));
    request->inline_object_offset
                            =   mutils::to_bytes(outputs,reinterpret_cast<uint8_t*>(request->rest) +
                                    request->output_edges_offset) +
                                request->output_edges_offset;
    if (sizeof(request) + request->inline_object_offset + mutils::bytes_size(value) >
        OBJECT_COMMIT_REQUEST_SIZE) {
        throw derecho::derecho_exception("Object is too large to fit in inline object request.");
    }
    request->padding_offset =   mutils::to_bytes(value,reinterpret_cast<uint8_t*>(request->rest) +
                                    request->inline_object_offset) +
                                request->inline_object_offset;
    // STEP 3 - commit
    this->object_commit_rb->produce(reinterpret_cast<void*>(request),request->total_size(),0);
}

template <typename FirstCascadeType,typename ... RestCascadeTypes>
MProcUDLClient<FirstCascadeType,RestCascadeTypes...>::~MProcUDLClient() {
    // object_commit_rb will be destructed automatically.
}

template <typename FirstCascadeType,typename ... RestCascadeTypes>
std::unique_ptr<MProcUDLClient<FirstCascadeType,RestCascadeTypes...>>
MProcUDLClient<FirstCascadeType,RestCascadeTypes...>::create(const key_t object_commit_rbkey) {
    auto* client = new MProcUDLClient<FirstCascadeType,RestCascadeTypes...>(object_commit_rbkey);
    return std::unique_ptr<MProcUDLClient<FirstCascadeType,RestCascadeTypes...>>(client);
}

}
}
