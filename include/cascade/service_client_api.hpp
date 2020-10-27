#pragma once
#include "service_types.hpp"
#include "service.hpp"

namespace derecho
{
    namespace cascade
    {
        /**
         * The client API
         */
        using ServiceClientAPI = ServiceClient<VCSU, VCSS, PCSU, PCSS>;

    } // namespace cascade
} // namespace derecho
