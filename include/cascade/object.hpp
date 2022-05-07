#pragma once
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

#include <derecho/conf/conf.hpp>
#include <derecho/core/derecho.hpp>
#include <derecho/mutils-serialization/SerializationSupport.hpp>
#include <derecho/persistent/Persistent.hpp>

#include <cascade/cascade.hpp>

using std::cout;
using std::endl;
using namespace persistent;
using namespace std::chrono_literals;

namespace derecho{
namespace cascade{

class Blob : public mutils::ByteRepresentable {
public:
    const uint8_t* bytes;
    std::size_t size;
    std::size_t capacity;
    bool        is_emplaced;

    // constructor - copy to own the data
    Blob(const uint8_t* const b, const decltype(size) s);

    Blob(const uint8_t* b, const decltype(size) s, bool temporary);

    // copy constructor - copy to own the data
    Blob(const Blob& other);

    // move constructor - accept the memory from another object
    Blob(Blob&& other);

    // default constructor - no data at all
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
            out << " " << b.bytes[i];
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
    ObjectWithStringKey(const std::string& _key, 
                        const Blob& _blob);

    // constructor 0.5 : copy/in-place constructor
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
    ObjectWithStringKey(const std::string& _key,
                        const uint8_t* const _b,
                        const std::size_t _s);

    // constructor 1.5 : copy constructor
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
    ObjectWithStringKey(ObjectWithStringKey&& other);

    // constructor 3 : copy constructor
    ObjectWithStringKey(const ObjectWithStringKey& other);

    // constructor 4 : default invalid constructor
    ObjectWithStringKey();

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
    void ensure_registerd(mutils::DeserializationManager&) {}
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
        << "msg_id: " << o.message_id
#endif
        << "ver: 0x" << std::hex << o.version << std::dec 
        << ", ts: " << o.timestamp_us
        << ", prev_ver: " << std::hex << o.previous_version << std::dec
        << ", prev_ver_by_key: " << std::hex << o.previous_version_by_key << std::dec
        << ", id:" << o.key 
        << ", data:" << o.blob << "}";
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
