/**
 * @file mproc_service.hpp
 * @brief The interface for mproc connector
 */

#include <cinttypes>

#include <wsong/ipc/ring_buffer.hpp>

namespace derecho {
namespace cascade {

/**
 * @struct mproc_connector_t mproc_service.hpp <cascade/mproc/mproc_service.hpp>
 * @brief The type of an mproc connector.
 */
struct mproc_connector_t {
    /**
     * @brief The id of a connector.
     * The format of the id string:
     * \<cascade server pid\>:\<object pool path\>:\<udl uuid\>
     */
    char        id[128];
    /**
     * @brief The context request ring buffer.
     */
    key_t       ctxt_req_rb;
    /**
     * @brief The context resource ring buffer.
     */
    key_t       ctxt_res_rb;
    /**
     * @brief The object commit ring buffer.
     */
    key_t       objs_com_rb;
    /**
     * @brief The context shared space.
     */
    key_t       ctxt_ss;
    /**
     * @brief The context shared space size.
     */
    size_t      ctxt_ss_size;
    /**
     * @brief the object pool shared space.
     */
    key_t       object_pool_ss;
    /**
     * @brief the object pool shared space size.
     */
    size_t      object_pool_ss_size;
};

/**
 * @struct mproc_connector_registry_header_t mproc_service.hpp "mproc_service.hpp"
 * @brief struct of the header of the mproc connector registration table.
 */
struct mproc_connector_registry_header_t {
    /**
     * @brief The start signature.
     */
    char        sig1[64];
    /**
     * @brief The bitmap. 0 for idel entry, 1 for used entry.
     */
    uint8_t     bitmap[64];
    uint32_t    capacity;
    /**
     * @brief The end signature.
     */
    char        sig2[64];
};

/**
 * @union mproc_connector_registry_entry mproc_service.hpp "mproc_service.hpp"
 * @brief struct of an entry in the mproc connector registry in sys-V shared memory.
 */
union mproc_connector_registry_entry {
    /** @brief the header entry */
    struct mproc_connector_registry_header_t
        header      __attribute__ ((aligned(CACHELINE_SIZE)));
    /** @brief the connector entry */
    struct mproc_connector_t
        connector   __attribute__ ((aligned(CACHELINE_SIZE)));
    uint8_t __bytes__[256]; // padding
};

}
}
