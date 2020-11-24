#pragma once
#include "service.hpp"
#include "service_types.hpp"

namespace derecho {
namespace cascade {
/**
         * The client API
         */
using ServiceClientAPI = ServiceClient<WPCSU, WPCSS>;

}  // namespace cascade
}  // namespace derecho
