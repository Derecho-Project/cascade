/**
 * @file mproc_ctxt_client.hpp
 * @brief The interface for running an mproc ctxt client
 */
#include <sys/types.h>

#include <cinttypes>
#include <string>
#include <memory>

namespace derecho {
namespace cascade {

template <typename... CascadeTypes>
class MProcCtxtClient {
public:
    /**
     * @fn MProcCtxtClient()
     * @brief MProcCtxtClient constructor
     */
    MProcCtxtClient();
    // TODO: define the ctxt API.
};

}
}
