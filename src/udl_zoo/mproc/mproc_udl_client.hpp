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
    /**
     * @fn MProcUDLClient()
     * @brief The constructor.
     * @param[in]   object_commit_rb    The ringbuffer for object commit.
     */
    MProcUDLClient(const key_t object_commit_rb);
public:
    /**
     * @fn submit(const node_id_t,const std::string&, const std::string&, const ObjectWithStringKey&, uint32_t)
     * @brief submit an object to the user API.
     * @param[in] sender                The sender id.
     * @param[in] object_pool_pathname  The object pool pathname
     * @param[in] key_string            The key inside the object pool's domain
     * @param[in] object                The immutable object live in the trmporary buffer shared by mutiple worker threads.
     * @param[in] worker_id             The off critical data path worker id.
     */
    virtual void submit(
        const node_id_t             sender,
        const std::string&          object_pool_pathname,
        const std::string&          key_string,
        const ObjectWithStringKey&  object,
        uint32_t                    worker_id);
    /**
     * @fn ~MProcUDLClient()
     * @brief The destructor.
     */
    virtual ~MProcUDLClient();
    /**
     * @fn create()
     * @brief Create an mproc client instance.
     * TODO: what information to submit?
     */
    static std::unique_ptr<MProcUDLClient<FirstCascadeType,RestCascadeTypes...>> create();
};

}
}

#include "mproc_udl_client_impl.hpp"
