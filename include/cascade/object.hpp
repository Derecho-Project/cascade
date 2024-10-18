#pragma once
#include "cascade_interface.hpp"
#include <cascade/config.h>

#include <derecho/conf/conf.hpp>
#include <derecho/core/derecho.hpp>
#include <derecho/mutils-serialization/SerializationSupport.hpp>
#include <derecho/persistent/Persistent.hpp>

#include <chrono>
#include <iostream>
#include <map>
#include <memory>
#include <string.h>
#include <string>
#include <time.h>
#include <vector>
#include <optional>
#include <tuple>

namespace derecho{
namespace cascade{

enum object_memory_mode_t {
    DEFAULT,
    EMPLACED,
    BLOB_GENERATOR,
};

using blob_generator_func_t = std::function<std::size_t(uint8_t*,const std::size_t)>;

class Blob : public mutils::ByteRepresentable {
public:
    const uint8_t* bytes;
    std::size_t size;
    std::size_t capacity;

    // for BLOB_GENERATOR mode only
    blob_generator_func_t blob_generator;

    object_memory_mode_t   memory_mode;


    /**
     * Copy-in constructor: Copies buf_size bytes from the byte buffer pointed
     * to by buf into Blob's own memory. This Blob now owns a copy of the data.
     * @param buf A pointer to a byte buffer
     * @param buf_size The size of the byte buffer
     */
    Blob(const uint8_t* const buf, const decltype(size) buf_size);

    /**
     * Copy-or-emplace constructor: If emplaced is true, constructs a Blob that
     * points to the same memory as buf, but does not own it. In this case Blob
     * will not free the memory in its destructor, assuming the caller still owns
     * it. If emplaced is false, behaves exactly like the two-argument copy-in
     * constructor above.
     *
     * @param buf A pointer to a byte buffer
     * @param buf_size The size of the byte buffer
     * @param emplaced True if the Blob should be constructed in "emplaced" mode
     * where it does not own the memory its instance variables point to.
     */
    Blob(const uint8_t* buf, const decltype(size) buf_size, bool emplaced);

    /**
     * Generator constructor: Accepts a function that will generate data, and the
     * size of the data it expects to generate, but defers generating the data
     * until the Blob is serialized.
     *
     * @param generator A Blob generator function
     * @param s The number of bytes that the generator intends to generate
     */
    Blob(const blob_generator_func_t& generator, const decltype(size) s);

    /**
     * "Raw move" constructor: Takes ownership of the data pointed to by buf.
     * Like the move constructor, but used when buf is a byte buffer not already
     * owned by a Blob. The caller must have a unique_ptr to the byte buffer to
     * ensure the caller is the sole owner of the memory and will not attempt
     * to use or free it after calling this constructor.
     *
     * @param buf A unique_ptr to a byte buffer
     * @param buf_size The size of the byte buffer.
     */
    Blob(std::unique_ptr<uint8_t[]> buf, const decltype(size) buf_size);

    /**
     * Standard copy constructor: Copies the data from other into a new byte
     * buffer owned by this Blob.
     */
    Blob(const Blob& other);

    /**
     * Standard move constructor: Takes ownership of the other Blob's byte
     * buffer and leaves it with no byte buffer.
     */
    Blob(Blob&& other);

    /**
     * Default constructor: Creates a Blob with no data.
     */
    Blob();

    // destructor
    virtual ~Blob();

    // move evaluator:
    Blob& operator=(Blob&& other);

    // copy evaluator:
    Blob& operator=(const Blob& other);

    // serialization/deserialization supports
    std::size_t to_bytes(uint8_t* v) const;

    std::size_t bytes_size() const;

    void post_object(const std::function<void(uint8_t const* const, std::size_t)>& f) const;

    void ensure_registered(mutils::DeserializationManager&) {}

    static std::unique_ptr<Blob> from_bytes(mutils::DeserializationManager*, const uint8_t* const v);

    static mutils::context_ptr<Blob> from_bytes_noalloc(
        mutils::DeserializationManager* ctx,
        const uint8_t* const v);

    static mutils::context_ptr<const Blob> from_bytes_noalloc_const(
        mutils::DeserializationManager* ctx,
        const uint8_t* const v);
};

#define INVALID_UINT64_OBJECT_KEY (0xffffffffffffffffLLU)

class ObjectWithUInt64Key : public mutils::ByteRepresentable,
                            public ICascadeObject<uint64_t,ObjectWithUInt64Key>,
                            public IKeepTimestamp,
                            public IVerifyPreviousVersion
#ifdef ENABLE_EVALUATION
                            , public IHasMessageID
#endif
                            {
public:
#ifdef ENABLE_EVALUATION
    mutable uint64_t                                    message_id;
#endif
    mutable persistent::version_t                       version;
    mutable uint64_t                                    timestamp_us;
    mutable persistent::version_t                       previous_version; // previous version, INVALID_VERSION for the first version
    mutable persistent::version_t                       previous_version_by_key; // previous version by key, INVALID_VERSION for the first value of the key.
    uint64_t                                            key; // object_id
    Blob                                                blob; // the object

    // bool operator==(const ObjectWithUInt64Key& other);

    // constructor 0 : copy constructor
    ObjectWithUInt64Key(const uint64_t _key,
                        const Blob& _blob);

    // constructor 0.5 : copy/emplace constructor
    ObjectWithUInt64Key(
#ifdef ENABLE_EVALUATION
                        const uint64_t _message_id,
#endif
                        const persistent::version_t _version,
                        const uint64_t _timestamp_us,
                        const persistent::version_t _previous_version,
                        const persistent::version_t _previous_version_by_key,
                        const uint64_t _key,
                        const Blob& _blob,
                        bool  is_emplaced = false);

    // constructor 1 : copy constructor
    ObjectWithUInt64Key(const uint64_t _key,
                        const uint8_t* const _b,
                        const std::size_t _s);

    // constructor 1.5 : copy constructor
    ObjectWithUInt64Key(
#ifdef ENABLE_EVALUATION
                        const uint64_t _message_id,
#endif
                        const persistent::version_t _version,
                        const uint64_t _timestamp_us,
                        const persistent::version_t _previous_version,
                        const persistent::version_t _previous_version_by_key,
                        const uint64_t _key,
                        const uint8_t* const _b,
                        const std::size_t _s);

    // TODO: we need a move version for the deserializer.

    // constructor 2 : move constructor
    ObjectWithUInt64Key(ObjectWithUInt64Key&& other);

    // constructor 3 : copy constructor
    ObjectWithUInt64Key(const ObjectWithUInt64Key& other);

    // constructor 4 : default invalid constructor
    ObjectWithUInt64Key();

    // constructor 5 : using delayed instantiator with message generator
    ObjectWithUInt64Key(const uint64_t _key,
                        const blob_generator_func_t& _message_generator,
                        const std::size_t _size);
    // constructor 5.5 : using delayed instratiator with message generator
    ObjectWithUInt64Key(
#ifdef ENABLE_EVALUATION
                        const uint64_t _message_id,
#endif
                        const persistent::version_t _version,
                        const uint64_t _timestamp_us,
                        const persistent::version_t _previous_version,
                        const persistent::version_t _previous_version_by_key,
                        const uint64_t _key,
                        const blob_generator_func_t& _message_generator,
                        const std::size_t _s);

    virtual const uint64_t& get_key_ref() const override;
    virtual bool is_null() const override;
    virtual bool is_valid() const override;
    virtual void copy_from(const ObjectWithUInt64Key& rhs) override;
    virtual void set_version(persistent::version_t ver) const override;
    virtual persistent::version_t get_version() const override;
    virtual void set_timestamp(uint64_t ts_us) const override;
    virtual uint64_t get_timestamp() const override;
    virtual void set_previous_version(persistent::version_t prev_ver, persistent::version_t prev_ver_by_key) const override;
    virtual bool verify_previous_version(persistent::version_t prev_ver, persistent::version_t prev_ver_by_key) const override;
#ifdef ENABLE_EVALUATION
    virtual void set_message_id(uint64_t id) const override;
    virtual uint64_t get_message_id() const override;
#endif

    // Deprecated: the default no_alloc deserializers are NOT zero-copy!!!
    // DEFAULT_SERIALIZATION_SUPPORT(ObjectWithUInt64Key, version, timestamp_us, previous_version, previous_version_by_key, key, blob);
    std::size_t to_bytes(uint8_t* v) const;
    std::size_t bytes_size() const;
    void post_object(const std::function<void(uint8_t const* const, std::size_t)>& f) const;
    void ensure_registerd(mutils::DeserializationManager&) {}
    static std::unique_ptr<ObjectWithUInt64Key> from_bytes(mutils::DeserializationManager*, const uint8_t* const v);
    static mutils::context_ptr<ObjectWithUInt64Key> from_bytes_noalloc(
        mutils::DeserializationManager* ctx,
        const uint8_t* const v);
    static mutils::context_ptr<const ObjectWithUInt64Key> from_bytes_noalloc_const(
        mutils::DeserializationManager* ctx,
        const uint8_t* const v);

    // IK and IV for volatile cascade store
    static uint64_t IK;
    static ObjectWithUInt64Key IV;
};

inline std::ostream& operator<<(std::ostream& out, const Blob& b) {
    out << "[size:" << b.size << ", data:" << std::hex;
    if(b.size > 0) {
        uint32_t i = 0;
        for(i = 0; i < 8 && i < b.size; i++) {
            out << " " << static_cast<int>(b.bytes[i]);
        }
        if(i < b.size) {
            out << "...";
        }
    }
    out << std::dec << "]";
    return out;
}

inline std::ostream& operator<<(std::ostream& out, const ObjectWithUInt64Key& o) {
    out << "ObjectWithUInt64Key{ver: 0x" << std::hex << o.version << std::dec
        << ", ts(us): " << o.timestamp_us
        << ", prev_ver: " << std::hex << o.previous_version << std::dec
        << ", prev_ver_by_key: " << std::hex << o.previous_version_by_key << std::dec
        << ", id:" << o.key
        << ", data:" << o.blob << "}";
    return out;
}

class ObjectWithStringKey : public mutils::ByteRepresentable,
                            public ICascadeObject<std::string,ObjectWithStringKey>,
                            public IKeepTimestamp,
                            public IVerifyPreviousVersion
#ifdef ENABLE_EVALUATION
                            ,public IHasMessageID
#endif
                            {
public:
#ifdef ENABLE_EVALUATION
    mutable uint64_t                                    message_id;
#endif
    mutable persistent::version_t                       version;                // object version
    mutable uint64_t                                    timestamp_us;           // timestamp in microsecond
    mutable persistent::version_t                       previous_version;       // previous version, INVALID_VERSION for the first version.
    mutable persistent::version_t                       previous_version_by_key; // previous version by key, INVALID_VERSION for the first value of the key.
    std::string                                         key;                     // object_id
    Blob                                                blob;                    // the object data

    // bool operator==(const ObjectWithStringKey& other);

    // constructor 0 : copy constructor
    /**
     * Creates an ObjectWithStringKey by copying an existing Blob.
     * All of the other fields will be initialized to invalid values.
     * @param _key The new object's key
     * @param _blob The data to store in the new object, which will be
     * copied into a new Blob.
     */
    ObjectWithStringKey(const std::string& _key,
                        const Blob& _blob);

    // constructor 0.5 : copy/in-place constructor
    /**
     * Creates an ObjectWithStringKey with the provided values for all the fields,
     * including the key and the value. If parameter is_emplaced is true, the new
     * object's Blob will be constructed in "emplaced" mode, meaning it shares
     * ownership of the bytes in the parameter Blob rather than copying them into a
     * new buffer. This constructor is used by the deserialization functions.
     * @param message_id If the macro ENABLE_EVALUATION is defined, the object's message ID
     * @param _version The object's version
     * @param _timestamp_us The object's timestamp, in microseconds
     * @param _previous_version The version of the previous entry in the persistent log
     * @param _previous_version_by_key The previous persistent version for an entry with the same key as this object
     * @param _key The object's key
     * @param _blob The data to store in the object
     * @param is_emplaced True if the object's Blob should share memory with the parameter _blob
     */
    ObjectWithStringKey(
#ifdef ENABLE_EVALUATION
                        const uint64_t message_id,
#endif
                        const persistent::version_t _version,
                        const uint64_t _timestamp_us,
                        const persistent::version_t _previous_version,
                        const persistent::version_t _previous_version_by_key,
                        const std::string& _key,
                        const Blob& _blob,
                        bool  is_emplaced = false);

    // constructor 1 : copy consotructor
    /**
     * Creates an ObjectWithStringKey by copying the provided byte buffer into
     * a new Blob. All the other fields will be initialized to invalid values.
     * @param _key The new object's key
     * @param _b A pointer to the beginning of the byte buffer to copy
     * @param _s The size of the byte buffer
     */
    ObjectWithStringKey(const std::string& _key,
                        const uint8_t* const _b,
                        const std::size_t _s);

    // constructor 1.5 : copy constructor
    /**
     * Creates an ObjectWithStringKey by copying the provided byte buffer into
     * a new Blob, and initializing the other fields to the provided values.
     * @param message_id If the macro ENABLE_EVALUATION is defined, the object's message ID
     * @param _version The object's version
     * @param _timestamp_us The object's timestamp, in microseconds
     * @param _previous_version The version of the previous entry in the persistent log
     * @param _previous_version_by_key The previous persistent version for an entry with the same key as this object
     * @param _key The object's key
     * @param _b A pointer to the beginning of the byte buffer to copy
     * @param _s The size of the byte buffer
     */
    ObjectWithStringKey(
#ifdef ENABLE_EVALUATION
                        const uint64_t message_id,
#endif
                        const persistent::version_t _version,
                        const uint64_t _timestamp_us,
                        const persistent::version_t _previous_version,
                        const persistent::version_t _previous_version_by_key,
                        const std::string& _key,
                        const uint8_t* const _b,
                        const std::size_t _s);

    // TODO: we need a move version for the deserializer.

    // constructor 2 : move constructor
    /**
     * Move constructor; takes ownership of the data stored in other.
     */
    ObjectWithStringKey(ObjectWithStringKey&& other);

    /** Move assignment operator; matches move constructor. */
    ObjectWithStringKey& operator=(ObjectWithStringKey&& other);

    // constructor 3 : copy constructor
    /**
     * Copy constructor; copies every field of the other ObjectWithStringKey
     */
    ObjectWithStringKey(const ObjectWithStringKey& other);

    // constructor 4 : default invalid constructor
    /**
     * Default constructor; initializes every field to invalid values.
     */
    ObjectWithStringKey();

    // constructor 5 : using delayed instantiator with message generator
    ObjectWithStringKey(const std::string& _key,
                        const blob_generator_func_t& _message_generator,
                        const std::size_t _size);
    // constructor 5.5 : using delayed instatiator withe message generator
    ObjectWithStringKey(
#ifdef ENABLE_EVALUATION
                        const uint64_t message_id,
#endif
                        const persistent::version_t _version,
                        const uint64_t _timestamp_us,
                        const persistent::version_t _previous_version,
                        const persistent::version_t _previous_version_by_key,
                        const std::string& _key,
                        const blob_generator_func_t& _message_generator,
                        const std::size_t _s);

    virtual const std::string& get_key_ref() const override;
    virtual bool is_null() const override;
    virtual bool is_valid() const override;
    virtual void copy_from(const ObjectWithStringKey& rhs) override;
    virtual void set_version(persistent::version_t ver) const override;
    virtual persistent::version_t get_version() const override;
    virtual void set_timestamp(uint64_t ts_us) const override;
    virtual uint64_t get_timestamp() const override;
    virtual void set_previous_version(persistent::version_t prev_ver, persistent::version_t perv_ver_by_key) const override;
    virtual bool verify_previous_version(persistent::version_t prev_ver, persistent::version_t perv_ver_by_key) const override;
#ifdef ENABLE_EVALUATION
    virtual void set_message_id(uint64_t id) const override;
    virtual uint64_t get_message_id() const override;
#endif

//    DEFAULT_SERIALIZATION_SUPPORT(ObjectWithStringKey, version, timestamp_us, previous_version, previous_version_by_key, key, blob);
    std::size_t to_bytes(uint8_t* v) const;
    std::size_t bytes_size() const;
    void post_object(const std::function<void(uint8_t const* const, std::size_t)>& f) const;
    void ensure_registered(mutils::DeserializationManager&) {}
    static std::unique_ptr<ObjectWithStringKey> from_bytes(mutils::DeserializationManager*, const uint8_t* const v);
    static mutils::context_ptr<ObjectWithStringKey> from_bytes_noalloc(
        mutils::DeserializationManager* ctx,
        const uint8_t* const v);
    static mutils::context_ptr<const ObjectWithStringKey> from_bytes_noalloc_const(
        mutils::DeserializationManager* ctx,
        const uint8_t* const v);

    // IK and IV for volatile cascade store
    static std::string IK;
    static ObjectWithStringKey IV;
};

inline std::ostream& operator<<(std::ostream& out, const ObjectWithStringKey& o) {
    out << "ObjectWithStringKey{"
#ifdef ENABLE_EVALUATION
        << "msg_id: " << o.message_id << " "
#endif
        << "ver: 0x" << std::hex << o.version << std::dec
        << ", ts: " << o.timestamp_us
        << ", prev_ver: " << std::hex << o.previous_version << std::dec
        << ", prev_ver_by_key: " << std::hex << o.previous_version_by_key << std::dec
        << ", id: " << o.key
        << ", data: " << o.blob << "}";
    return out;
}


/**
template <typename KT, typename VT, KT* IK, VT* IV>
std::enable_if_t<std::disjunction<std::is_same<ObjectWithStringKey,VT>,std::is_same<ObjectWithStringKey,VT>>::value, VT> create_null_object_cb(const KT& key) {
    return VT(key,Blob{});
}
**/

} // namespace cascade
} // namespace derecho

// Boilerplate definitions needed to enable debug logging of these objects with the spdlog library

template <>
struct fmt::formatter<derecho::cascade::ObjectWithUInt64Key> : fmt::ostream_formatter {};
template <>
struct fmt::formatter<derecho::cascade::ObjectWithStringKey> : fmt::ostream_formatter {};
