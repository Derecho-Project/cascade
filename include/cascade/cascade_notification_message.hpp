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

    // Use default serialization, but not default deserialization
    DEFAULT_SERIALIZE(object_pool_pathname, blob);

    // Customized from_bytes avoids making an extra copy of the blob by moving it
    // (instead of copying it) into the CascadeNotificationMessage constructor
    static std::unique_ptr<CascadeNotificationMessage> from_bytes(mutils::DeserializationManager* dsm, uint8_t const* buf) {
        auto pathname = mutils::from_bytes<std::string>(dsm, buf);
        auto blob = mutils::from_bytes<Blob>(dsm, buf + mutils::bytes_size(*pathname));
        return std::make_unique<CascadeNotificationMessage>(*pathname, std::move(*blob));
    }
    // Customized from_bytes_noalloc matches from_bytes, calls move version of CascadeNotificationMessage constructor
    static mutils::context_ptr<CascadeNotificationMessage> from_bytes_noalloc(mutils::DeserializationManager* dsm, const uint8_t* const buf) {
        auto pathname = mutils::from_bytes_noalloc<std::string>(dsm, buf);
        auto blob = mutils::from_bytes_noalloc<Blob>(dsm, buf + mutils::bytes_size(*pathname));
        return mutils::context_ptr<CascadeNotificationMessage>(
                new CascadeNotificationMessage(*pathname, std::move(*blob)));
    }
    static mutils::context_ptr<const CascadeNotificationMessage> from_bytes_noalloc_const(mutils::DeserializationManager* dsm, const uint8_t* const buf) {
        const auto pathname = mutils::from_bytes_noalloc<std::string>(dsm, buf);
        const auto blob = mutils::from_bytes_noalloc<Blob>(dsm, buf + mutils::bytes_size(*pathname));
        return mutils::context_ptr<const CascadeNotificationMessage>(
                new CascadeNotificationMessage(*pathname, std::move(*blob)));
    }
    void ensure_registered(mutils::DeserializationManager&){};

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
    /**
     * Blob-moving constructor: Accepts the Blob parameter by rvalue reference
     * and moves it into the instance variable. This avoids making an extra copy
     * of the Blob's data when the constructor argument is intended to be
     * temporary (e.g. during deserialization).
     */
    CascadeNotificationMessage(const std::string& _object_pool_pathname,
                               Blob&& temp_blob)
            : object_pool_pathname(_object_pool_pathname),
              blob(std::move(temp_blob)) {}
};

}  // namespace cascade
}  // namespace derecho