/**
 * @file mproc_connector.hpp
 * @brief The interface for mproc connector.
 */

#include <cinttypes>

#include <wsong/ipc/ring_buffer.hpp>


namespace derecho {
namespace cascade {

/**
 * @union mproc_connector_registry_entry mproc_connector.hpp "mproc_connector.hpp"
 * @brief struct of an entry in the mproc connector registry in sys-V shared memory.
 */
union mproc_connector_registry_entry {
    struct {
        char        id[CACHELINE_SIZE];
        key_t       ring_buffer_to_udl;
        key_t       ring_buffer_to_cascade;
        key_t       shared_mem_pool;
        uint32_t    mem_pool_size;
    } mproc_connector __attribute__ ((aligned(CACHELINE_SIZE)));
    struct {
        char        sig1[CACHELINE_SIZE];
        uint8_t     bitmap[CACHELINE_SIZE]; // bitmap
        uint32_t    capacity;               // capacity of it.
        char        sig2[CACHELINE_SIZE];
    } header __attribute__ ((aligned(CACHELINE_SIZE)));
    uint8_t __bytes__[256];
};



/**
 * @class MProcConnectorManager mproc_connector.hpp "mproc_connector.hpp"
 */
class MProcConnectorManager {
    //TODO:
};

}
}
