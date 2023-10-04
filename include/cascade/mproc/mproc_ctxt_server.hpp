/**
 * @file mproc_ctxt_server.hpp
 * @brief The interface for mproc connector.
 */
#include <sys/types.h>

#include <cinttypes>
#include <string>
#include <memory>

#include <cascade/user_defined_logic_interface.hpp>

namespace derecho {
namespace cascade {

/**
 * @class MProcCtxtServer mproc_ctxt_server.hpp "cascade/mproc/mproc_ctxt_server.hpp"
 * @brief   The API of the mproc context server.
 * @tparam  CascadeTypes...     The Cascade template types.
 */
template <typename... CascadeTypes>
class MProcCtxtServer {
public:
    /**
     * @fn std::unique_ptr<MProcCtxtServer<CascadeTypes...>> create(CascadeContext<CascadeTypes...>*)
     * @param[in]   cascade_ctxt    The cascade context.
     * @return  The unique pointer to the create mproc ctxt.
     */
    static std::unique_ptr<MProcCtxtServer<CascadeTypes...>> create(
            CascadeContext<CascadeTypes...>* cascade_ctxt) {
        // TODO:
    }
};

}
}
