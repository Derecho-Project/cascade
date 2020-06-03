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

using std::cout;
using std::endl;
using namespace persistent;
using namespace std::chrono_literals;

namespace derecho{
namespace cascade{

class Blob : public mutils::ByteRepresentable {
public:
    char* bytes;
    std::size_t size;

    // constructor - copy to own the data
    Blob(const char* const b, const decltype(size) s);

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
    std::size_t to_bytes(char* v) const;

    std::size_t bytes_size() const;

    void post_object(const std::function<void(char const* const, std::size_t)>& f) const;

    void ensure_registered(mutils::DeserializationManager&) {}

    static std::unique_ptr<Blob> from_bytes(mutils::DeserializationManager*, const char* const v);

    // from_bytes_noalloc() implementation borrowed from mutils-serialization.
    mutils::context_ptr<Blob> from_bytes_noalloc(
        mutils::DeserializationManager* ctx,
        const char* const v, 
        mutils::context_ptr<Blob> = mutils::context_ptr<Blob>{});
};

#define INVALID_UINT64_OBJECT_KEY (0xffffffffffffffffLLU)

class ObjectWithUInt64Key : public mutils::ByteRepresentable {
public:
    mutable std::tuple<persistent::version_t,uint64_t> ver;  // object version
    uint64_t key;                            // object_id
    Blob blob;                          // the object

    bool operator==(const ObjectWithUInt64Key& other);

    bool is_valid() const;

    // constructor 0 : copy constructor
    ObjectWithUInt64Key(const uint64_t& _key, const Blob& _blob);

    // constructor 0.5 : copy constructor
    ObjectWithUInt64Key(const std::tuple<persistent::version_t,uint64_t> _ver, const uint64_t& _key, const Blob& _blob);

    // constructor 1 : copy consotructor
    ObjectWithUInt64Key(const uint64_t _key, const char* const _b, const std::size_t _s);

    // constructor 1.5 : copy constructor
    ObjectWithUInt64Key(const std::tuple<persistent::version_t,uint64_t> _ver, const uint64_t _key, const char* const _b, const std::size_t _s);

    // TODO: we need a move version for the deserializer.

    // constructor 2 : move constructor
    ObjectWithUInt64Key(ObjectWithUInt64Key&& other);

    // constructor 3 : copy constructor
    ObjectWithUInt64Key(const ObjectWithUInt64Key& other);

    // constructor 4 : default invalid constructor
    ObjectWithUInt64Key();

    DEFAULT_SERIALIZATION_SUPPORT(ObjectWithUInt64Key, ver, key, blob);

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
    out << "ObjectWithUInt64Key{ver: 0x" << std::hex << std::get<0>(o.ver) << std::dec 
        << ", ts: " << std::get<1>(o.ver) << ", id:"
        << o.key << ", data:" << o.blob << "}";
    return out;
}

class ObjectWithStringKey : public mutils::ByteRepresentable {
public:
    mutable std::tuple<persistent::version_t,uint64_t> ver;  // object version
    std::string key;                            // object_id
    Blob blob;                          // the object

    bool operator==(const ObjectWithStringKey& other);

    bool is_valid() const;

    // constructor 0 : copy constructor
    ObjectWithStringKey(const std::string& _key, const Blob& _blob);

    // constructor 0.5 : copy constructor
    ObjectWithStringKey(const std::tuple<persistent::version_t,uint64_t> _ver, const std::string& _key, const Blob& _blob);

    // constructor 1 : copy consotructor
    ObjectWithStringKey(const std::string& _key, const char* const _b, const std::size_t _s);

    // constructor 1.5 : copy constructor
    ObjectWithStringKey(const std::tuple<persistent::version_t,uint64_t> _ver, const std::string& _key, const char* const _b, const std::size_t _s);

    // TODO: we need a move version for the deserializer.

    // constructor 2 : move constructor
    ObjectWithStringKey(ObjectWithStringKey&& other);

    // constructor 3 : copy constructor
    ObjectWithStringKey(const ObjectWithStringKey& other);

    // constructor 4 : default invalid constructor
    ObjectWithStringKey();

    DEFAULT_SERIALIZATION_SUPPORT(ObjectWithStringKey, ver, key, blob);

    // IK and IV for volatile cascade store
    static std::string IK;
    static ObjectWithStringKey IV;
};

inline std::ostream& operator<<(std::ostream& out, const ObjectWithStringKey& o) {
    out << "ObjectWithStringKey{ver: 0x" << std::hex << std::get<0>(o.ver) << std::dec 
        << ", ts: " << std::get<1>(o.ver) << ", id:"
        << o.key << ", data:" << o.blob << "}";
    return out;
}

} // namespace cascade
} // namespace derecho
