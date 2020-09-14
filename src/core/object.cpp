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


bool ObjectWithUInt64Key::operator==(const ObjectWithUInt64Key& other) {
    return (this->key == other.key) && (this->ver == other.ver);
}

bool ObjectWithUInt64Key::is_valid() const {
    return (key == INVALID_UINT64_OBJECT_KEY);
}

// constructor 0 : copy constructor
ObjectWithUInt64Key::ObjectWithUInt64Key(const uint64_t& _key,
                                         const persistent::version_t& _previous_version_by_key,
                                         const Blob& _blob) : 
    ver(persistent::INVALID_VERSION,0),
    key(_key),
    previous_version_by_key(_previous_version_by_key),
    blob(_blob) {}

// constructor 0.5 : copy constructor
ObjectWithUInt64Key::ObjectWithUInt64Key(const std::tuple<persistent::version_t,uint64_t> _ver,
                                         const uint64_t& _key,
                                         const persistent::version_t& _previous_version_by_key,
                                         const Blob& _blob) :
    ver(_ver),
    key(_key), 
    previous_version_by_key(_previous_version_by_key),
    blob(_blob) {}

// constructor 1 : copy consotructor
ObjectWithUInt64Key::ObjectWithUInt64Key(const uint64_t _key,
                                         const persistent::version_t& _previous_version_by_key,
                                         const char* const _b,
                                         const std::size_t _s) :
    ver(persistent::INVALID_VERSION,0),
    key(_key),
    previous_version_by_key(_previous_version_by_key),
    blob(_b, _s) {}

// constructor 1.5 : copy constructor
ObjectWithUInt64Key::ObjectWithUInt64Key(const std::tuple<persistent::version_t,uint64_t> _ver,
                                         const uint64_t _key,
                                         const persistent::version_t& _previous_version_by_key,
                                         const char* const _b,
                                         const std::size_t _s) :
    ver(_ver),
    key(_key),
    previous_version_by_key(_previous_version_by_key),
    blob(_b, _s) {}

// constructor 2 : move constructor
ObjectWithUInt64Key::ObjectWithUInt64Key(ObjectWithUInt64Key&& other) :
    ver(other.ver),
    key(other.key),
    previous_version_by_key(other.previous_version_by_key),
    blob(std::move(other.blob)) {}

// constructor 3 : copy constructor
ObjectWithUInt64Key::ObjectWithUInt64Key(const ObjectWithUInt64Key& other) :
    ver(other.ver),
    key(other.key),
    previous_version_by_key(other.previous_version_by_key),
    blob(other.blob) {}

// constructor 4 : default invalid constructor
ObjectWithUInt64Key::ObjectWithUInt64Key() :
    ver(persistent::INVALID_VERSION,0),
    key(INVALID_UINT64_OBJECT_KEY),
    previous_version_by_key(persistent::INVALID_VERSION) {}

void ObjectWithUInt64Key::set_previous_version(persistent::version_t&, persistent::version_t& prev_ver_by_key) {
    this->previous_version_by_key = prev_ver_by_key;
}

bool ObjectWithStringKey::operator==(const ObjectWithStringKey& other) {
    return (this->key == other.key) && (this->ver == other.ver);
}

bool ObjectWithStringKey::is_valid() const {
    return !key.empty();
}

// constructor 0 : copy constructor
ObjectWithStringKey::ObjectWithStringKey(const std::string& _key, 
                                         const persistent::version_t& _previous_version_by_key,
                                         const Blob& _blob) : 
    ver(persistent::INVALID_VERSION,0),
    key(_key),
    previous_version_by_key(_previous_version_by_key),
    blob(_blob) {}
// constructor 0.5 : copy constructor
ObjectWithStringKey::ObjectWithStringKey(const std::tuple<persistent::version_t,uint64_t> _ver,
                                         const std::string& _key,
                                         const persistent::version_t& _previous_version_by_key,
                                         const Blob& _blob) :
    ver(_ver),
    key(_key), 
    previous_version_by_key(_previous_version_by_key),
    blob(_blob) {}

// constructor 1 : copy consotructor
ObjectWithStringKey::ObjectWithStringKey(const std::string& _key,
                                         const persistent::version_t& _previous_version_by_key,
                                         const char* const _b, 
                                         const std::size_t _s) : 
    ver(persistent::INVALID_VERSION,0),
    key(_key),
    previous_version_by_key(_previous_version_by_key),
    blob(_b, _s) {}
// constructor 1.5 : copy constructor
ObjectWithStringKey::ObjectWithStringKey(const std::tuple<persistent::version_t,uint64_t> _ver,
                                         const std::string& _key,
                                         const persistent::version_t& _previous_version_by_key,
                                         const char* const _b,
                                         const std::size_t _s) : 
    ver(_ver),
    key(_key),
    previous_version_by_key(_previous_version_by_key),
    blob(_b, _s) {}

// constructor 2 : move constructor
ObjectWithStringKey::ObjectWithStringKey(ObjectWithStringKey&& other) : 
    ver(other.ver),
    key(other.key),
    previous_version_by_key(other.previous_version_by_key),
    blob(std::move(other.blob)) {}

// constructor 3 : copy constructor
ObjectWithStringKey::ObjectWithStringKey(const ObjectWithStringKey& other) : 
    ver(other.ver),
    key(other.key),
    previous_version_by_key(other.previous_version_by_key),
    blob(other.blob) {}

// constructor 4 : default invalid constructor
ObjectWithStringKey::ObjectWithStringKey() : 
    ver(persistent::INVALID_VERSION,0),
    key(),
    previous_version_by_key(persistent::INVALID_VERSION) {}

void ObjectWithStringKey::set_previous_version(persistent::version_t&, persistent::version_t& prev_ver_by_key) {
    this->previous_version_by_key = prev_ver_by_key;
}

} // namespace cascade
} // namespace derecho

