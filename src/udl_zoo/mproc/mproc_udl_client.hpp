#pragma once
/**
 * @file mproc_udl_client.hpp
 * @brief The interface for mproc udl client.
 */
#include <sys/types.h>

#include <cinttypes>
#include <string>
#include <memory>
#include <wsong/ipc/ring_buffer.hpp>

#include <cascade/user_defined_logic_interface.hpp>
#include "object_commit_protocol.hpp"

namespace derecho {
namespace cascade {

/**
 * @class MProcUDLClient mproc_udl_client.hpp "mproc_udl_client.hpp"
 * @brief The mproc client API
 * The mproc client UDL use this API to communicate with mproc daemon to start a mproc server in
 * another process.
 * @tparam  FirstCascadeType    The first cascade subgroup type
 * @tparam  RestCascadeTypes    The rest cascade subgroup types.
 */
template <typename FirstCascadeType,typename ... RestCascadeTypes>
class MProcUDLClient {
    static_assert(have_same_object_type<FirstCascadeType,RestCascadeTypes...>());
private:
    std::unique_ptr<wsong::ipc::RingBuffer>     object_commit_rb;   /// object commit ring buffer
    /**
     * @fn MProcUDLClient()
     * @brief The constructor.
     * @param[in]   object_commit_rbkey The ringbuffer for object commit.
     */
    MProcUDLClient(const key_t object_commit_rbkey);
public:
    /**
     * @fn submit(const node_id_t,const std::string&, const std::string&, const ObjectWithStringKey&, uint32_t)
     * @brief submit an object to the user API. The parameters match that of OffCriticalDataPath API.
     * @param[in]   sender                  The sender id.
     * @param[in]   full_key_string
     * @param[in]   prefix_length
     * @param[in]   version
     * @param[in]   value
     * @param[in]   outputs
     * @param[in]   worker_id
     */
    virtual void submit(
        const node_id_t             sender,
        const std::string&          full_key_string,
        const uint32_t              prefix_length,
        persistent::version_t       version,
        const mutils::ByteRepresentable* const
                                    value,
        const std::unordered_map<std::string,bool>&
                                    outputs,
        uint32_t                    worker_id);
    /**
     * @fn ~MProcUDLClient()
     * @brief The destructor.
     */
    virtual ~MProcUDLClient();
    /**
     * @fn create()
     * @brief Create an mproc client instance.
     * @param[in]   object_commit_rbkey The object commit ring buffer key.
     * @return  A unique pointer to created MProcUDLClient object.
     * @throws  If creation failed, throw an exception of type derecho::derecho_exception.
     */
    static std::unique_ptr<MProcUDLClient<FirstCascadeType,RestCascadeTypes...>> create(const key_t object_commit_rbkey);
};

}
}

#include "mproc_udl_client_impl.hpp"
