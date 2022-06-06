#pragma once

#include "object.hpp"

#include <derecho/mutils-serialization/SerializationSupport.hpp>

#include <cstdint>
#include <functional>
#include <string>

namespace derecho {
namespace cascade {

/** The notification handler type */
using cascade_notification_handler_t = std::function<void(const Blob&)>;

/** The CascadeNotificationMessage types */
enum CascadeNotificationMessageType : uint64_t {
    StandardNotification = 0x100000000ull,
    SignatureNotification = 0x100000001ull
};

struct CascadeNotificationMessage : public mutils::ByteRepresentable {
    /** The object pool pathname, empty string for raw cascade notification message */
    std::string object_pool_pathname;
    /** data */
    Blob blob;

    /** TODO: the default serialization support macro might contain unnecessary copies. Check it!!! */
    DEFAULT_SERIALIZATION_SUPPORT(CascadeNotificationMessage, object_pool_pathname, blob);

    /** constructors */
    CascadeNotificationMessage()
            : object_pool_pathname(),
              blob() {}
    CascadeNotificationMessage(CascadeNotificationMessage&& other)
            : object_pool_pathname(other.object_pool_pathname),
              blob(std::move(other.blob)) {}
    CascadeNotificationMessage(const CascadeNotificationMessage& other)
            : object_pool_pathname(other.object_pool_pathname),
              blob(other.blob) {}
    CascadeNotificationMessage(const std::string& _object_pool_pathname,
                               const Blob& _blob)
            : object_pool_pathname(_object_pool_pathname),
              blob(_blob) {}
};

}  // namespace cascade
}  // namespace derecho