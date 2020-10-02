#include <cascade/object.hpp>

namespace derecho {
namespace cascade {

uint64_t ObjectWithUInt64Key::IK = INVALID_UINT64_OBJECT_KEY;
ObjectWithUInt64Key ObjectWithUInt64Key::IV;
std::string ObjectWithStringKey::IK;
ObjectWithStringKey ObjectWithStringKey::IV;

Blob::Blob(const char* const b, const decltype(size) s) :
    bytes(nullptr), size(0) {
    if(s > 0) {
        bytes = new char[s];
        if (b != nullptr) {
            memcpy(bytes, b, s);
        } else {
            bzero(bytes, s);
        }
        size = s;
    }
}

Blob::Blob(const Blob& other) :
    bytes(nullptr), size(0) {
    if(other.size > 0) {
        bytes = new char[other.size];
        memcpy(bytes, other.bytes, other.size);
        size = other.size;
    }
}

Blob::Blob(Blob&& other) : 
    bytes(other.bytes), size(other.size) {
    other.bytes = nullptr;
    other.size = 0;
}

Blob::Blob() : bytes(nullptr), size(0) {}

Blob::~Blob() {
    if(bytes) delete [] bytes;
}

Blob& Blob::operator=(Blob&& other) {
    char* swp_bytes = other.bytes;
    std::size_t swp_size = other.size;
    other.bytes = bytes;
    other.size = size;
    bytes = swp_bytes;
    size = swp_size;
    return *this;
}

Blob& Blob::operator=(const Blob& other) {
    if(bytes != nullptr) {
        delete bytes;
    }
    size = other.size;
    if(size > 0) {
        bytes = new char[size];
        memcpy(bytes, other.bytes, size);
    } else {
        bytes = nullptr;
    }
    return *this;
}

std::size_t Blob::to_bytes(char* v) const {
    ((std::size_t*)(v))[0] = size;
    if(size > 0) {
        memcpy(v + sizeof(size), bytes, size);
    }
    return size + sizeof(size);
}

std::size_t Blob::bytes_size() const {
    return size + sizeof(size);
}

void Blob::post_object(const std::function<void(char const* const, std::size_t)>& f) const {
    f((char*)&size, sizeof(size));
    f(bytes, size);
}

// from_bytes_noalloc() implementation borrowed from mutils-serialization.
mutils::context_ptr<Blob> Blob::from_bytes_noalloc(mutils::DeserializationManager* ctx, const char* const v, mutils::context_ptr<Blob> ) {
    return mutils::context_ptr<Blob>{from_bytes(ctx, v).release()};
}

std::unique_ptr<Blob> Blob::from_bytes(mutils::DeserializationManager*, const char* const v) {
    return std::make_unique<Blob>(v + sizeof(std::size_t), ((std::size_t*)(v))[0]);
}

/*
bool ObjectWithUInt64Key::operator==(const ObjectWithUInt64Key& other) {
    return (this->key == other.key) && (this->version == other.version);
}
*/

bool ObjectWithUInt64Key::is_valid() const {
    return (key != INVALID_UINT64_OBJECT_KEY);
}

// constructor 0 : copy constructor
ObjectWithUInt64Key::ObjectWithUInt64Key(const uint64_t _key,
                                         const Blob& _blob) : 
    version(persistent::INVALID_VERSION),
    timestamp_us(0),
    previous_version(INVALID_VERSION),
    previous_version_by_key(INVALID_VERSION),
    key(_key),
    blob(_blob) {}

// constructor 0.5 : copy constructor
ObjectWithUInt64Key::ObjectWithUInt64Key(const persistent::version_t _version,
                                         const uint64_t _timestamp_us,
                                         const persistent::version_t _previous_version,
                                         const persistent::version_t _previous_version_by_key,
                                         const uint64_t _key,
                                         const Blob& _blob) :
    version(_version),
    timestamp_us(_timestamp_us),
    previous_version(_previous_version),
    previous_version_by_key(_previous_version_by_key),
    key(_key), 
    blob(_blob) {}

// constructor 1 : copy consotructor
ObjectWithUInt64Key::ObjectWithUInt64Key(const uint64_t _key,
                                         const char* const _b,
                                         const std::size_t _s) :
    version(persistent::INVALID_VERSION),
    timestamp_us(0),
    previous_version(INVALID_VERSION),
    previous_version_by_key(INVALID_VERSION),
    key(_key),
    blob(_b, _s) {}

// constructor 1.5 : copy constructor
ObjectWithUInt64Key::ObjectWithUInt64Key(const persistent::version_t _version,
                                         const uint64_t _timestamp_us,
                                         const persistent::version_t _previous_version,
                                         const persistent::version_t _previous_version_by_key,
                                         const uint64_t _key,
                                         const char* const _b,
                                         const std::size_t _s) :
    version(_version),
    timestamp_us(_timestamp_us),
    previous_version(_previous_version),
    previous_version_by_key(_previous_version_by_key),
    key(_key),
    blob(_b, _s) {}

// constructor 2 : move constructor
ObjectWithUInt64Key::ObjectWithUInt64Key(ObjectWithUInt64Key&& other) :
    version(other.version),
    timestamp_us(other.timestamp_us),
    previous_version(other.previous_version),
    previous_version_by_key(other.previous_version_by_key),
    key(other.key),
    blob(std::move(other.blob)) {}

// constructor 3 : copy constructor
ObjectWithUInt64Key::ObjectWithUInt64Key(const ObjectWithUInt64Key& other) :
    version(other.version),
    timestamp_us(other.timestamp_us),
    previous_version(other.previous_version),
    previous_version_by_key(other.previous_version_by_key),
    key(other.key),
    blob(other.blob) {}

// constructor 4 : default invalid constructor
ObjectWithUInt64Key::ObjectWithUInt64Key() :
    version(persistent::INVALID_VERSION),
    timestamp_us(0),
    previous_version(INVALID_VERSION),
    previous_version_by_key(INVALID_VERSION),
    key(INVALID_UINT64_OBJECT_KEY) {}

const uint64_t& ObjectWithUInt64Key::get_key_ref() const {
    return this->key;
}

bool ObjectWithUInt64Key::is_null() const {
    return (this->blob.size == 0);
}

void ObjectWithUInt64Key::set_previous_version(persistent::version_t prev_ver, persistent::version_t prev_ver_by_key) const {
    this->previous_version = prev_ver;
    this->previous_version_by_key = prev_ver_by_key;
}

void ObjectWithUInt64Key::set_version(persistent::version_t ver) const {
    this->version = ver;
}

persistent::version_t ObjectWithUInt64Key::get_version() const {
    return this->version;
}

void ObjectWithUInt64Key::set_timestamp(uint64_t ts_us) const {
    this->timestamp_us = ts_us;
}

uint64_t ObjectWithUInt64Key::get_timestamp() const {
    return this->timestamp_us;
}

template <>
ObjectWithUInt64Key create_null_object_cb<uint64_t,ObjectWithUInt64Key,&ObjectWithUInt64Key::IK,&ObjectWithUInt64Key::IV>(const uint64_t& key) {
    return ObjectWithUInt64Key(key,Blob{});
}

/*
bool ObjectWithStringKey::operator==(const ObjectWithStringKey& other) {
    return (this->key == other.key) && (this->version == other.version);
}
*/

bool ObjectWithStringKey::is_valid() const {
    return !key.empty();
}

// constructor 0 : copy constructor
ObjectWithStringKey::ObjectWithStringKey(const std::string& _key, 
                                         const Blob& _blob) : 
    version(persistent::INVALID_VERSION),
    timestamp_us(0),
    previous_version(INVALID_VERSION),
    previous_version_by_key(INVALID_VERSION),
    key(_key),
    blob(_blob) {}
// constructor 0.5 : copy constructor
ObjectWithStringKey::ObjectWithStringKey(const persistent::version_t _version,
                                         const uint64_t _timestamp_us,
                                         const persistent::version_t _previous_version,
                                         const persistent::version_t _previous_version_by_key,
                                         const std::string& _key,
                                         const Blob& _blob) :
    version(_version),
    timestamp_us(_timestamp_us),
    previous_version(_previous_version),
    previous_version_by_key(_previous_version_by_key),
    key(_key), 
    blob(_blob) {}

// constructor 1 : copy consotructor
ObjectWithStringKey::ObjectWithStringKey(const std::string& _key,
                                         const char* const _b, 
                                         const std::size_t _s) : 
    version(persistent::INVALID_VERSION),
    timestamp_us(0),
    previous_version(INVALID_VERSION),
    previous_version_by_key(INVALID_VERSION),
    key(_key),
    blob(_b, _s) {}
// constructor 1.5 : copy constructor
ObjectWithStringKey::ObjectWithStringKey(const persistent::version_t _version,
                                         const uint64_t _timestamp_us,
                                         const persistent::version_t _previous_version,
                                         const persistent::version_t _previous_version_by_key,
                                         const std::string& _key,
                                         const char* const _b,
                                         const std::size_t _s) : 
    version(_version),
    timestamp_us(_timestamp_us),
    previous_version(_previous_version),
    previous_version_by_key(_previous_version_by_key),
    key(_key), 
    blob(_b, _s) {}

// constructor 2 : move constructor
ObjectWithStringKey::ObjectWithStringKey(ObjectWithStringKey&& other) : 
    version(other.version),
    timestamp_us(other.timestamp_us),
    previous_version(other.previous_version),
    previous_version_by_key(other.previous_version_by_key),
    key(other.key),
    blob(std::move(other.blob)) {}

// constructor 3 : copy constructor
ObjectWithStringKey::ObjectWithStringKey(const ObjectWithStringKey& other) : 
    version(other.version),
    timestamp_us(other.timestamp_us),
    previous_version(other.previous_version),
    previous_version_by_key(other.previous_version_by_key),
    key(other.key),
    blob(other.blob) {}

// constructor 4 : default invalid constructor
ObjectWithStringKey::ObjectWithStringKey() : 
    version(persistent::INVALID_VERSION),
    timestamp_us(0),
    previous_version(INVALID_VERSION),
    previous_version_by_key(INVALID_VERSION),
    key() {}

const std::string& ObjectWithStringKey::get_key_ref() const {
    return this->key;
}

bool ObjectWithStringKey::is_null() const {
    return (this->blob.size == 0);
}


void ObjectWithStringKey::set_previous_version(persistent::version_t prev_ver, persistent::version_t prev_ver_by_key) const {
    this->previous_version = prev_ver;
    this->previous_version_by_key = prev_ver_by_key;
}

void ObjectWithStringKey::set_version(persistent::version_t ver) const {
    this->version = ver;
}

persistent::version_t ObjectWithStringKey::get_version() const {
    return this->version;
}

void ObjectWithStringKey::set_timestamp(uint64_t ts_us) const {
    this->timestamp_us = ts_us;
}

uint64_t ObjectWithStringKey::get_timestamp() const {
    return this->timestamp_us;
}

template <>
ObjectWithStringKey create_null_object_cb<std::string,ObjectWithStringKey,&ObjectWithStringKey::IK,&ObjectWithStringKey::IV>(const std::string& key) {
    return ObjectWithStringKey(key,Blob{});
}

} // namespace cascade
} // namespace derecho

