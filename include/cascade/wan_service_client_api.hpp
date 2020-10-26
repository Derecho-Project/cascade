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
        using ServiceClientAPI = ServiceClient<WPCSS, WPCSU>;

    } // namespace cascade
} // namespace derecho
