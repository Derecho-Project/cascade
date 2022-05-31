#include <cascade/object.hpp>
#include <derecho/persistent/detail/PersistLog.hpp>
#include <unistd.h>
#include <stdlib.h>

namespace derecho {
namespace cascade {

uint64_t ObjectWithUInt64Key::IK = INVALID_UINT64_OBJECT_KEY;
ObjectWithUInt64Key ObjectWithUInt64Key::IV;
std::string ObjectWithStringKey::IK;
ObjectWithStringKey ObjectWithStringKey::IV;

/*
 *  IMPORTANT NOTICE of Blob Implementation
 *
 *  Blob is an inner type of ObjectWithXXXKeys class, which is repsonsible for store the objects with various length.
 *  Copy constructor is heavy for large objects. We found that sometimes, the Linux malloc/memcpy behave wiredly if the
 *  memory is not aligned to page boundary. It incurs a lot of page walks (The phenomenon can be reproduced by
 *  'wired_reconnecting_external_client' branch with a setup of 1 VCSS node, 1MB message, and 1 external client). To
 *  solve this issue, we only use full pages for Blob data buffer. To further improve the performance, we should use
 *  hugepages.
 *
 *  Update on Sept 1st, 2021
 *  The reason for the "wired" issue is found. It was because the malloc system adatps the size of idle pages to use
 *  memory smartly. The glibc tunable 'glibc.malloc.trim_threshold' controls that behaviour. If
 *  glibc.malloc.trim_threshold is not set, the default value of the threshold is 128KB and dynamically changing by the
 *  workload. So for the first time, when the workload keeps malloc-ing and free-ing 1MB memory chunks, the free
 *  operation will return the pages to OS. Therefore, the malloc-ed new pages does not exist in the page table.
 *  Accessing the new allocated memory cause a page fault and page walk for each new page (256 4K pages per 1MB),
 *  causing 4~5x overhead in memcpy(). But later, when the threshold adapts to the new workload, The performance is back
 *  to normal.
 *
 *  The right way to avoid this is to use an optimal trim_threshold value instead of setting page alignment.
 */
// static const std::size_t page_size = sysconf(_SC_PAGESIZE);
// #define PAGE_ALIGNED_NEW(x) (new uint8_t[((x)+page_size-1)/page_size*page_size])

Blob::Blob(const uint8_t* const b, const decltype(size) s) :
    bytes(nullptr), size(0), capacity(0), memory_mode(object_memory_mode_t::DEFAULT) {
    if(s > 0) {
        // uint8_t* t_bytes = PAGE_ALIGNED_NEW(s);
        uint8_t* t_bytes = static_cast<uint8_t*>(malloc(s));
        if (b != nullptr) {
            memcpy(t_bytes, b, s);
        } else {
            bzero(t_bytes, s);
        }
        bytes = t_bytes;
        size = s;
        capacity = size;
    }
}

Blob::Blob(const uint8_t* b, const decltype(size) s, bool emplaced) :
    bytes(b), size(s), capacity(s), memory_mode((emplaced)?object_memory_mode_t::EMPLACED:object_memory_mode_t::DEFAULT) {
    if ( (size>0) && (emplaced==false)) {
        // uint8_t* t_bytes = PAGE_ALIGNED_NEW(s);
        uint8_t* t_bytes = static_cast<uint8_t*>(malloc(s));
        if (b != nullptr) {
            memcpy(t_bytes, b, s);
        } else {
            bzero(t_bytes, s);
        }
        bytes = t_bytes;
    }
    // exclude illegal argument combinations like (0x982374,0,false)
    if (size == 0) {
        bytes = nullptr;
    }
}

Blob::Blob(const blob_generator_func_t& generator, const decltype(size) s):
    bytes(nullptr), size(s), capacity(0), blob_generator(generator), memory_mode(object_memory_mode_t::BLOB_GENERATOR) {
    // no data is generated here.
}

Blob::Blob(const Blob& other) :
    bytes(nullptr), size(0), capacity(0), memory_mode(object_memory_mode_t::DEFAULT) {
    if(other.size > 0) {
        uint8_t* t_bytes = static_cast<uint8_t*>(malloc(other.size));
        if (memory_mode == object_memory_mode_t::BLOB_GENERATOR) {
            // instantiate data.
            auto number_bytes_generated = other.blob_generator(t_bytes,other.size);
            if (number_bytes_generated != other.size) {
                dbg_default_error("Expecting {} bytes, but blob generator writes {} bytes.", other.size, number_bytes_generated);
                std::string exception_message("Expecting");
                throw std::runtime_error(std::string("Expecting ") + std::to_string(other.size) 
                        + " bytes, but blob generator writes "
                        + std::to_string(number_bytes_generated) + " bytes.");
            }
        } else {
            // uint8_t* t_bytes = PAGE_ALIGNED_NEW(other.size);
            memcpy(t_bytes, other.bytes, other.size);
        }
        bytes = t_bytes;
        size = other.size;
        capacity = other.size;
    }
}

Blob::Blob(Blob&& other) : 
    bytes(other.bytes), size(other.size), capacity(other.size),
    blob_generator(other.blob_generator), memory_mode(other.memory_mode) {
    other.bytes = nullptr;
    other.size = 0;
    other.capacity = 0;
}

Blob::Blob() : bytes(nullptr), size(0), capacity(0), memory_mode(object_memory_mode_t::DEFAULT) {}

Blob::~Blob() {
    if(bytes && (memory_mode == object_memory_mode_t::DEFAULT)) {
        free(const_cast<void*>(reinterpret_cast<const void*>(bytes)));
    }
}

Blob& Blob::operator=(Blob&& other) {
    auto swp_bytes = other.bytes;
    auto swp_size = other.size;
    auto swp_cap  = other.capacity;
    auto swp_blob_generator = other.blob_generator;
    auto swp_memory_mode = other.memory_mode;
    other.bytes = bytes;
    other.size = size;
    other.capacity = capacity;
    other.blob_generator = blob_generator;
    other.memory_mode = memory_mode;
    bytes = swp_bytes;
    size = swp_size;
    capacity = swp_cap;
    blob_generator = swp_blob_generator;
    memory_mode = swp_memory_mode;
    return *this;
}

Blob& Blob::operator=(const Blob& other) {
    // 1) this->is_emplaced has to be false;
    if (memory_mode != object_memory_mode_t::DEFAULT) {
        throw std::runtime_error("Copy to a Blob that does not own the data (object_memory_mode_T::DEFAULT) is prohibited.");
    }

    // 2) verify that this->capacity has enough memory;
    if (this->capacity < other.size) {
        bytes = static_cast<uint8_t*>(realloc(const_cast<void*>(static_cast<const void*>(bytes)),other.size));
        this->capacity = other.size;
    } 

    // 3) update this->size; copy data, if there is any.
    this->size = other.size;
    if(this->size > 0) {
        if (other.memory_mode == object_memory_mode_t::BLOB_GENERATOR) {
            auto number_bytes_generated = other.blob_generator(const_cast<uint8_t*>(this->bytes),other.size);
            if (number_bytes_generated != other.size) {
                dbg_default_error("Expecting {} bytes, but blob generator writes {} bytes.", other.size, number_bytes_generated);
                std::string exception_message("Expecting");
                throw std::runtime_error(std::string("Expecting ") + std::to_string(other.size) 
                        + " bytes, but blob generator writes "
                        + std::to_string(number_bytes_generated) + " bytes.");
            }
        } else {
            memcpy(const_cast<void*>(static_cast<const void*>(this->bytes)), other.bytes, size);
        }
    }

    return *this;
}

std::size_t Blob::to_bytes(uint8_t* v) const {
    ((std::size_t*)(v))[0] = size;
    if(size > 0) {
        if (memory_mode == object_memory_mode_t::BLOB_GENERATOR) {
            auto number_bytes_generated = blob_generator(v+sizeof(size), size);
            if (number_bytes_generated != size) {
                dbg_default_error("Expecting {} bytes, but blob generator writes {} bytes.", size, number_bytes_generated);
                std::string exception_message("Expecting");
                throw std::runtime_error(std::string("Expecting ") + std::to_string(size) 
                        + " bytes, but blob generator writes "
                        + std::to_string(number_bytes_generated) + " bytes.");
            }
        } else {
            memcpy(v + sizeof(size), bytes, size);
        }
    }
    return size + sizeof(size);
}

std::size_t Blob::bytes_size() const {
    return size + sizeof(size);
}

void Blob::post_object(const std::function<void(uint8_t const* const, std::size_t)>& f) const {
    if (size > 0 && (memory_mode == object_memory_mode_t::BLOB_GENERATOR)) {
        // we have to instatiate the data. CAUTIOUS: this is inefficient. Please use BLOB_GENERATOR mode carefully.
        uint8_t* local_bytes = static_cast<uint8_t*>(malloc(size));
        auto number_bytes_generated = blob_generator(local_bytes,size);
        if (number_bytes_generated != size) {
            free(local_bytes);
            dbg_default_error("Expecting {} bytes, but blob generator writes {} bytes.", size, number_bytes_generated);
            std::string exception_message("Expecting");
            throw std::runtime_error(std::string("Expecting ") + std::to_string(size) 
                    + " bytes, but blob generator writes "
                    + std::to_string(number_bytes_generated) + " bytes.");
        }
        f((uint8_t*)&size, sizeof(size));
        f(local_bytes, size);
        free(local_bytes);
    } else {
        f((uint8_t*)&size, sizeof(size));
        f(bytes, size);
    }
}

mutils::context_ptr<Blob> Blob::from_bytes_noalloc(mutils::DeserializationManager* ctx, const uint8_t* const v) {
    return mutils::context_ptr<Blob>{new Blob(const_cast<uint8_t*>(v) + sizeof(std::size_t), ((std::size_t*)(v))[0], true)};
}

mutils::context_ptr<const Blob> Blob::from_bytes_noalloc_const(mutils::DeserializationManager* ctx, const uint8_t* const v) {
    return mutils::context_ptr<const Blob>{new Blob(const_cast<uint8_t*>(v) + sizeof(std::size_t), ((std::size_t*)(v))[0], true)};
}

std::unique_ptr<Blob> Blob::from_bytes(mutils::DeserializationManager*, const uint8_t* const v) {
    return std::make_unique<Blob>(v + sizeof(std::size_t), ((std::size_t*)(v))[0]);
}

/** ObjectWithStringKey Implementation **/

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
ObjectWithUInt64Key::ObjectWithUInt64Key(
#ifdef ENABLE_EVALUATION
                                         const uint64_t _message_id,
#endif
                                         const persistent::version_t _version,
                                         const uint64_t _timestamp_us,
                                         const persistent::version_t _previous_version,
                                         const persistent::version_t _previous_version_by_key,
                                         const uint64_t _key,
                                         const Blob& _blob,
                                         bool  emplaced) :
#ifdef ENABLE_EVALUATION
    message_id(_message_id),
#endif
    version(_version),
    timestamp_us(_timestamp_us),
    previous_version(_previous_version),
    previous_version_by_key(_previous_version_by_key),
    key(_key), 
    blob(_blob.bytes,_blob.size,emplaced) {}

// constructor 1 : copy consotructor
ObjectWithUInt64Key::ObjectWithUInt64Key(const uint64_t _key,
                                         const uint8_t* const _b,
                                         const std::size_t _s) :
#ifdef ENABLE_EVALUATION
    message_id(0),
#endif
    version(persistent::INVALID_VERSION),
    timestamp_us(0),
    previous_version(INVALID_VERSION),
    previous_version_by_key(INVALID_VERSION),
    key(_key),
    blob(_b, _s) {}

// constructor 1.5 : copy constructor
ObjectWithUInt64Key::ObjectWithUInt64Key(
#ifdef ENABLE_EVALUATION
                                         const uint64_t _message_id,
#endif
                                         const persistent::version_t _version,
                                         const uint64_t _timestamp_us,
                                         const persistent::version_t _previous_version,
                                         const persistent::version_t _previous_version_by_key,
                                         const uint64_t _key,
                                         const uint8_t* const _b,
                                         const std::size_t _s) :
#ifdef ENABLE_EVALUATION
    message_id(_message_id),
#endif
    version(_version),
    timestamp_us(_timestamp_us),
    previous_version(_previous_version),
    previous_version_by_key(_previous_version_by_key),
    key(_key),
    blob(_b, _s) {}

// constructor 2 : move constructor
ObjectWithUInt64Key::ObjectWithUInt64Key(ObjectWithUInt64Key&& other) :
#ifdef ENABLE_EVALUATION
    message_id(other.message_id),
#endif
    version(other.version),
    timestamp_us(other.timestamp_us),
    previous_version(other.previous_version),
    previous_version_by_key(other.previous_version_by_key),
    key(other.key),
    blob(std::move(other.blob)) {}

// constructor 3 : copy constructor
ObjectWithUInt64Key::ObjectWithUInt64Key(const ObjectWithUInt64Key& other) :
#ifdef ENABLE_EVALUATION
    message_id(other.message_id),
#endif
    version(other.version),
    timestamp_us(other.timestamp_us),
    previous_version(other.previous_version),
    previous_version_by_key(other.previous_version_by_key),
    key(other.key),
    blob(other.blob) {}

// constructor 4 : default invalid constructor
ObjectWithUInt64Key::ObjectWithUInt64Key() :
#ifdef ENABLE_EVALUATION
    message_id(0),
#endif
    version(persistent::INVALID_VERSION),
    timestamp_us(0),
    previous_version(INVALID_VERSION),
    previous_version_by_key(INVALID_VERSION),
    key(INVALID_UINT64_OBJECT_KEY) {}

// constructor 5 : using delayed instantiator with message gnerator
ObjectWithUInt64Key::ObjectWithUInt64Key(const uint64_t _key,
                                         const blob_generator_func_t& _message_generator,
                                         const std::size_t _size) :
#ifdef ENABLE_EVALUATION
    message_id(0),
#endif
    version(persistent::INVALID_VERSION),
    timestamp_us(0),
    previous_version(INVALID_VERSION),
    previous_version_by_key(INVALID_VERSION),
    key(_key),
    blob(_message_generator,_size) {}

// constructor 5.5 : using delayed instatiator with message generator
ObjectWithUInt64Key::ObjectWithUInt64Key(
#ifdef ENABLE_EVALUATION
                                         const uint64_t _message_id,
#endif
                                         const persistent::version_t _version,
                                         const uint64_t _timestamp_us,
                                         const persistent::version_t _previous_version,
                                         const persistent::version_t _previous_version_by_key,
                                         const uint64_t _key,
                                         const blob_generator_func_t& _message_generator,
                                         const std::size_t _s) :
#ifdef ENABLE_EVALUATION
    message_id(_message_id),
#endif
    version(_version),
    timestamp_us(_timestamp_us),
    previous_version(_previous_version),
    previous_version_by_key(_previous_version_by_key),
    key(_key),
    blob(_message_generator, _s) {}

const uint64_t& ObjectWithUInt64Key::get_key_ref() const {
    return this->key;
}

bool ObjectWithUInt64Key::is_null() const {
    return (this->blob.size == 0);
}

void ObjectWithUInt64Key::copy_from(const ObjectWithUInt64Key& rhs) {
#ifdef ENABLE_EVALUATION
    this->message_id = rhs.message_id;
#endif
    this->version = rhs.version;
    this->timestamp_us = rhs.timestamp_us;
    this->previous_version = rhs.previous_version;
    this->previous_version_by_key = rhs.previous_version_by_key;
    this->key = rhs.key;
    // copy assignment
    this->blob = rhs.blob;
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

void ObjectWithUInt64Key::set_previous_version(persistent::version_t prev_ver, persistent::version_t prev_ver_by_key) const {
    this->previous_version = prev_ver;
    this->previous_version_by_key = prev_ver_by_key;
}

bool ObjectWithUInt64Key::verify_previous_version(persistent::version_t prev_ver, persistent::version_t prev_ver_by_key) const {
    // NOTICE: We provide the default behaviour of verify_previous_version as a demonstration. Please change the
    // following code or implementing your own Object Types with a verify_previous_version implementation to customize
    // it. The default behavior is self-explanatory and can be disabled by setting corresponding object previous versions to
    // INVALID_VERSION.

    return ((this->previous_version == persistent::INVALID_VERSION)?true:(this->previous_version >= prev_ver)) &&
           ((this->previous_version_by_key == persistent::INVALID_VERSION)?true:(this->previous_version_by_key >= prev_ver_by_key));
}

#ifdef ENABLE_EVALUATION
void ObjectWithUInt64Key::set_message_id(uint64_t id) const {
    this->message_id = id;
}

uint64_t ObjectWithUInt64Key::get_message_id() const {
    return this->message_id;
}
#endif

template <>
ObjectWithUInt64Key create_null_object_cb<uint64_t,ObjectWithUInt64Key,&ObjectWithUInt64Key::IK,&ObjectWithUInt64Key::IV>(const uint64_t& key) {
    return ObjectWithUInt64Key(key,Blob{});
}

std::size_t ObjectWithUInt64Key::to_bytes(uint8_t* v) const {
    std::size_t pos = 0;
#ifdef ENABLE_EVALUATION
    pos+=mutils::to_bytes(message_id, v + pos);
#endif
    pos+=mutils::to_bytes(version, v + pos);
    pos+=mutils::to_bytes(timestamp_us, v + pos);
    pos+=mutils::to_bytes(previous_version, v + pos);
    pos+=mutils::to_bytes(previous_version_by_key, v + pos);
    pos+=mutils::to_bytes(key, v + pos);
    pos+=mutils::to_bytes(blob, v + pos);
    return pos;
}

std::size_t ObjectWithUInt64Key::bytes_size() const {
    return 
#ifdef ENABLE_EVALUATION
           mutils::bytes_size(message_id) +
#endif
           mutils::bytes_size(version) +
           mutils::bytes_size(timestamp_us) +
           mutils::bytes_size(previous_version) +
           mutils::bytes_size(previous_version_by_key) +
           mutils::bytes_size(key) +
           mutils::bytes_size(blob);
}

void ObjectWithUInt64Key::post_object(const std::function<void(uint8_t const* const, std::size_t)>& f) const {
#ifdef ENABLE_EVALUATION
    mutils::post_object(f, message_id);
#endif
    mutils::post_object(f, version);
    mutils::post_object(f, timestamp_us);
    mutils::post_object(f, previous_version);
    mutils::post_object(f, previous_version_by_key);
    mutils::post_object(f, key);
    mutils::post_object(f, blob);
}

std::unique_ptr<ObjectWithUInt64Key> ObjectWithUInt64Key::from_bytes(mutils::DeserializationManager* dsm, const uint8_t* const v) {
    size_t pos = 0;
#ifdef ENABLE_EVALUATION
    auto p_message_id = mutils::from_bytes_noalloc<uint64_t>(dsm,v + pos);
    pos += mutils::bytes_size(*p_message_id);
#endif
    auto p_version = mutils::from_bytes_noalloc<persistent::version_t>(dsm,v + pos);
    pos += mutils::bytes_size(*p_version);
    auto p_timestamp_us = mutils::from_bytes_noalloc<uint64_t>(dsm,v + pos);
    pos += mutils::bytes_size(*p_timestamp_us);
    auto p_previous_version = mutils::from_bytes_noalloc<persistent::version_t>(dsm,v + pos);
    pos += mutils::bytes_size(*p_previous_version);
    auto p_previous_version_by_key = mutils::from_bytes_noalloc<persistent::version_t>(dsm,v + pos);
    pos += mutils::bytes_size(*p_previous_version);
    auto p_key = mutils::from_bytes_noalloc<uint64_t>(dsm,v + pos);
    pos += mutils::bytes_size(*p_key);
    auto p_blob = mutils::from_bytes_noalloc<Blob>(dsm, v + pos);
    // this is a copy constructor
    return std::make_unique<ObjectWithUInt64Key>(
#ifdef ENABLE_EVALUATION
        *p_message_id,
#endif
        *p_version,
        *p_timestamp_us,
        *p_previous_version,
        *p_previous_version_by_key,
        *p_key,
        *p_blob);
}

mutils::context_ptr<ObjectWithUInt64Key> ObjectWithUInt64Key::from_bytes_noalloc(
    mutils::DeserializationManager* dsm,
    const uint8_t* const v) {
    size_t pos = 0;
#ifdef ENABLE_EVALUATION
    auto p_message_id = mutils::from_bytes_noalloc<uint64_t>(dsm,v + pos);
    pos += mutils::bytes_size(*p_message_id);
#endif
    auto p_version = mutils::from_bytes_noalloc<persistent::version_t>(dsm,v + pos);
    pos += mutils::bytes_size(*p_version);
    auto p_timestamp_us = mutils::from_bytes_noalloc<uint64_t>(dsm,v + pos);
    pos += mutils::bytes_size(*p_timestamp_us);
    auto p_previous_version = mutils::from_bytes_noalloc<persistent::version_t>(dsm,v + pos);
    pos += mutils::bytes_size(*p_previous_version);
    auto p_previous_version_by_key = mutils::from_bytes_noalloc<persistent::version_t>(dsm,v + pos);
    pos += mutils::bytes_size(*p_previous_version);
    auto p_key = mutils::from_bytes_noalloc<uint64_t>(dsm,v + pos);
    pos += mutils::bytes_size(*p_key);
    auto p_blob = mutils::from_bytes_noalloc<Blob>(dsm, v + pos);
    return mutils::context_ptr<ObjectWithUInt64Key>(
        new ObjectWithUInt64Key{
#ifdef ENABLE_EVALUATION
        *p_message_id,
#endif
        *p_version,
        *p_timestamp_us,
        *p_previous_version,
        *p_previous_version_by_key,
        *p_key,
        *p_blob,true});
}

mutils::context_ptr<const ObjectWithUInt64Key> ObjectWithUInt64Key::from_bytes_noalloc_const(
    mutils::DeserializationManager* dsm,
    const uint8_t* const v) {
    size_t pos = 0;
#ifdef ENABLE_EVALUATION
    auto p_message_id = mutils::from_bytes_noalloc<uint64_t>(dsm,v + pos);
    pos += mutils::bytes_size(*p_message_id);
#endif
    auto p_version = mutils::from_bytes_noalloc<persistent::version_t>(dsm,v + pos);
    pos += mutils::bytes_size(*p_version);
    auto p_timestamp_us = mutils::from_bytes_noalloc<uint64_t>(dsm,v + pos);
    pos += mutils::bytes_size(*p_timestamp_us);
    auto p_previous_version = mutils::from_bytes_noalloc<persistent::version_t>(dsm,v + pos);
    pos += mutils::bytes_size(*p_previous_version);
    auto p_previous_version_by_key = mutils::from_bytes_noalloc<persistent::version_t>(dsm,v + pos);
    pos += mutils::bytes_size(*p_previous_version);
    auto p_key = mutils::from_bytes_noalloc<uint64_t>(dsm,v + pos);
    pos += mutils::bytes_size(*p_key);
    auto p_blob = mutils::from_bytes_noalloc<Blob>(dsm, v + pos);
    return mutils::context_ptr<const ObjectWithUInt64Key>(
        new ObjectWithUInt64Key{
#ifdef ENABLE_EVALUATION
        *p_message_id,
#endif
        *p_version,
        *p_timestamp_us,
        *p_previous_version,
        *p_previous_version_by_key,
        *p_key,
        *p_blob,true});
}


/** ObjectWithStringKey Implementation **/

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
#ifdef ENABLE_EVALUATION
    message_id(0),
#endif
    version(persistent::INVALID_VERSION),
    timestamp_us(0),
    previous_version(INVALID_VERSION),
    previous_version_by_key(INVALID_VERSION),
    key(_key),
    blob(_blob) {}
// constructor 0.5 : copy/in-place constructor
ObjectWithStringKey::ObjectWithStringKey(
#ifdef ENABLE_EVALUATION
                                         const uint64_t _message_id,
#endif
                                         const persistent::version_t _version,
                                         const uint64_t _timestamp_us,
                                         const persistent::version_t _previous_version,
                                         const persistent::version_t _previous_version_by_key,
                                         const std::string& _key,
                                         const Blob& _blob,
                                         const bool emplaced) :
#ifdef ENABLE_EVALUATION
    message_id(_message_id),
#endif
    version(_version),
    timestamp_us(_timestamp_us),
    previous_version(_previous_version),
    previous_version_by_key(_previous_version_by_key),
    key(_key), 
    blob(_blob.bytes,_blob.size,emplaced) {}

// constructor 1 : copy consotructor
ObjectWithStringKey::ObjectWithStringKey(const std::string& _key,
                                         const uint8_t* const _b, 
                                         const std::size_t _s) :
#ifdef ENABLE_EVALUATION
    message_id(0),
#endif
    version(persistent::INVALID_VERSION),
    timestamp_us(0),
    previous_version(INVALID_VERSION),
    previous_version_by_key(INVALID_VERSION),
    key(_key),
    blob(_b, _s) {}
// constructor 1.5 : copy constructor
ObjectWithStringKey::ObjectWithStringKey(
#ifdef ENABLE_EVALUATION
                                         const uint64_t _message_id,
#endif
                                         const persistent::version_t _version,
                                         const uint64_t _timestamp_us,
                                         const persistent::version_t _previous_version,
                                         const persistent::version_t _previous_version_by_key,
                                         const std::string& _key,
                                         const uint8_t* const _b,
                                         const std::size_t _s) :
#ifdef ENABLE_EVALUATION
    message_id(_message_id),
#endif
    version(_version),
    timestamp_us(_timestamp_us),
    previous_version(_previous_version),
    previous_version_by_key(_previous_version_by_key),
    key(_key), 
    blob(_b, _s) {}

// constructor 2 : move constructor
ObjectWithStringKey::ObjectWithStringKey(ObjectWithStringKey&& other) :
#ifdef ENABLE_EVALUATION
    message_id(other.message_id),
#endif
    version(other.version),
    timestamp_us(other.timestamp_us),
    previous_version(other.previous_version),
    previous_version_by_key(other.previous_version_by_key),
    key(other.key),
    blob(std::move(other.blob)) {}

// constructor 3 : copy constructor
ObjectWithStringKey::ObjectWithStringKey(const ObjectWithStringKey& other) :
#ifdef ENABLE_EVALUATION
    message_id(other.message_id),
#endif
    version(other.version),
    timestamp_us(other.timestamp_us),
    previous_version(other.previous_version),
    previous_version_by_key(other.previous_version_by_key),
    key(other.key),
    blob(other.blob) {}

// constructor 4 : default invalid constructor
ObjectWithStringKey::ObjectWithStringKey() : 
#ifdef ENABLE_EVALUATION
    message_id(0),
#endif
    version(persistent::INVALID_VERSION),
    timestamp_us(0),
    previous_version(INVALID_VERSION),
    previous_version_by_key(INVALID_VERSION),
    key() {}

// constructor 5 : using delayed instatiator with message generator
ObjectWithStringKey::ObjectWithStringKey(const std::string& _key,
                                         const blob_generator_func_t& _message_generator,
                                         const std::size_t _size):
#ifdef ENABLE_EVALUATION
    message_id(0),
#endif
    version(persistent::INVALID_VERSION),
    timestamp_us(0),
    previous_version(INVALID_VERSION),
    previous_version_by_key(INVALID_VERSION),
    key(_key),
    blob(_message_generator,_size) {}

// constructor 5.5 : using delayed instatiator with message generator
ObjectWithStringKey::ObjectWithStringKey(
#ifdef ENABLE_EVALUATION
                                         const uint64_t _message_id,
#endif
                                         const persistent::version_t _version,
                                         const uint64_t _timestamp_us,
                                         const persistent::version_t _previous_version,
                                         const persistent::version_t _previous_version_by_key,
                                         const std::string& _key,
                                         const blob_generator_func_t& _message_generator,
                                         const std::size_t _s) :
#ifdef ENABLE_EVALUATION
    message_id(_message_id),
#endif
    version(_version),
    timestamp_us(_timestamp_us),
    previous_version(_previous_version),
    previous_version_by_key(_previous_version_by_key),
    key(_key),
    blob(_message_generator, _s) {}

const std::string& ObjectWithStringKey::get_key_ref() const {
    return this->key;
}

bool ObjectWithStringKey::is_null() const {
    return (this->blob.size == 0);
}

void ObjectWithStringKey::copy_from(const ObjectWithStringKey& rhs) {
#ifdef ENABLE_EVALUATION
    this->message_id = rhs.message_id;
#endif
    this->version = rhs.version;
    this->timestamp_us = rhs.timestamp_us;
    this->previous_version = rhs.previous_version;
    this->previous_version_by_key = rhs.previous_version_by_key;
    this->key = rhs.key;
    // copy assignment
    this->blob = rhs.blob;
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

void ObjectWithStringKey::set_previous_version(persistent::version_t prev_ver, persistent::version_t prev_ver_by_key) const {
    this->previous_version = prev_ver;
    this->previous_version_by_key = prev_ver_by_key;
}

bool ObjectWithStringKey::verify_previous_version(persistent::version_t prev_ver, persistent::version_t prev_ver_by_key) const {
    // NOTICE: We provide the default behaviour of verify_previous_version as a demonstration. Please change the
    // following code or implementing your own Object Types with a verify_previous_version implementation to customize
    // it. The default behavior is self-explanatory and can be disabled by setting corresponding object previous versions to
    // INVALID_VERSION.

    return ((this->previous_version == persistent::INVALID_VERSION)?true:(this->previous_version >= prev_ver)) &&
           ((this->previous_version_by_key == persistent::INVALID_VERSION)?true:(this->previous_version_by_key >= prev_ver_by_key));
}

#ifdef ENABLE_EVALUATION
void ObjectWithStringKey::set_message_id(uint64_t id) const {
    this->message_id = id;
}

uint64_t ObjectWithStringKey::get_message_id() const {
    return this->message_id;
}
#endif

template <>
ObjectWithStringKey create_null_object_cb<std::string,ObjectWithStringKey,&ObjectWithStringKey::IK,&ObjectWithStringKey::IV>(const std::string& key) {
    return ObjectWithStringKey(key,Blob{});
}

std::size_t ObjectWithStringKey::to_bytes(uint8_t* v) const {
    std::size_t pos = 0;
#ifdef ENABLE_EVALUATION
    pos+=mutils::to_bytes(message_id, v + pos);
#endif
    pos+=mutils::to_bytes(version, v + pos);
    pos+=mutils::to_bytes(timestamp_us, v + pos);
    pos+=mutils::to_bytes(previous_version, v + pos);
    pos+=mutils::to_bytes(previous_version_by_key, v + pos);
    pos+=mutils::to_bytes(key, v + pos);
    pos+=mutils::to_bytes(blob, v + pos);
    return pos;
}

std::size_t ObjectWithStringKey::bytes_size() const {
    return
#ifdef ENABLE_EVALUATION
           mutils::bytes_size(message_id) +
#endif
           mutils::bytes_size(version) +
           mutils::bytes_size(timestamp_us) +
           mutils::bytes_size(previous_version) +
           mutils::bytes_size(previous_version_by_key) +
           mutils::bytes_size(key) +
           mutils::bytes_size(blob);
}

void ObjectWithStringKey::post_object(const std::function<void(uint8_t const* const, std::size_t)>& f) const {
#ifdef ENABLE_EVALUATION
    mutils::post_object(f, message_id);
#endif
    mutils::post_object(f, version);
    mutils::post_object(f, timestamp_us);
    mutils::post_object(f, previous_version);
    mutils::post_object(f, previous_version_by_key);
    mutils::post_object(f, key);
    mutils::post_object(f, blob);
}

std::unique_ptr<ObjectWithStringKey> ObjectWithStringKey::from_bytes(mutils::DeserializationManager* dsm, const uint8_t* const v) {
    size_t pos = 0;
#ifdef ENABLE_EVALUATION
    auto p_message_id = mutils::from_bytes_noalloc<uint64_t>(dsm,v + pos);
    pos += mutils::bytes_size(*p_message_id);
#endif
    auto p_version = mutils::from_bytes_noalloc<persistent::version_t>(dsm,v + pos);
    pos += mutils::bytes_size(*p_version);
    auto p_timestamp_us = mutils::from_bytes_noalloc<uint64_t>(dsm,v + pos);
    pos += mutils::bytes_size(*p_timestamp_us);
    auto p_previous_version = mutils::from_bytes_noalloc<persistent::version_t>(dsm,v + pos);
    pos += mutils::bytes_size(*p_previous_version);
    auto p_previous_version_by_key = mutils::from_bytes_noalloc<persistent::version_t>(dsm,v + pos);
    pos += mutils::bytes_size(*p_previous_version);
    auto p_key = mutils::from_bytes_noalloc<std::string>(dsm,v + pos);
    pos += mutils::bytes_size(*p_key);
    auto p_blob = mutils::from_bytes_noalloc<Blob>(dsm, v + pos);
    // this is a copy constructor
    return std::make_unique<ObjectWithStringKey>(
#ifdef ENABLE_EVALUATION
        *p_message_id,
#endif
        *p_version,
        *p_timestamp_us,
        *p_previous_version,
        *p_previous_version_by_key,
        *p_key,
        *p_blob);
}

mutils::context_ptr<ObjectWithStringKey> ObjectWithStringKey::from_bytes_noalloc(
    mutils::DeserializationManager* dsm,
    const uint8_t* const v) {
    size_t pos = 0;
#ifdef ENABLE_EVALUATION
    auto p_message_id = mutils::from_bytes_noalloc<uint64_t>(dsm,v + pos);
    pos += mutils::bytes_size(*p_message_id);
#endif
    auto p_version = mutils::from_bytes_noalloc<persistent::version_t>(dsm,v + pos);
    pos += mutils::bytes_size(*p_version);
    auto p_timestamp_us = mutils::from_bytes_noalloc<uint64_t>(dsm,v + pos);
    pos += mutils::bytes_size(*p_timestamp_us);
    auto p_previous_version = mutils::from_bytes_noalloc<persistent::version_t>(dsm,v + pos);
    pos += mutils::bytes_size(*p_previous_version);
    auto p_previous_version_by_key = mutils::from_bytes_noalloc<persistent::version_t>(dsm,v + pos);
    pos += mutils::bytes_size(*p_previous_version);
    auto p_key = mutils::from_bytes_noalloc<std::string>(dsm,v + pos);
    pos += mutils::bytes_size(*p_key);
    auto p_blob = mutils::from_bytes_noalloc<Blob>(dsm, v + pos);
    return mutils::context_ptr<ObjectWithStringKey>(
        new ObjectWithStringKey{
#ifdef ENABLE_EVALUATION
        *p_message_id,
#endif
        *p_version,
        *p_timestamp_us,
        *p_previous_version,
        *p_previous_version_by_key,
        *p_key,
        *p_blob,true});
}

mutils::context_ptr<const ObjectWithStringKey> ObjectWithStringKey::from_bytes_noalloc_const(
    mutils::DeserializationManager* dsm,
    const uint8_t* const v) {
    size_t pos = 0;
#ifdef ENABLE_EVALUATION
    auto p_message_id = mutils::from_bytes_noalloc<uint64_t>(dsm,v + pos);
    pos += mutils::bytes_size(*p_message_id);
#endif
    auto p_version = mutils::from_bytes_noalloc<persistent::version_t>(dsm,v + pos);
    pos += mutils::bytes_size(*p_version);
    auto p_timestamp_us = mutils::from_bytes_noalloc<uint64_t>(dsm,v + pos);
    pos += mutils::bytes_size(*p_timestamp_us);
    auto p_previous_version = mutils::from_bytes_noalloc<persistent::version_t>(dsm,v + pos);
    pos += mutils::bytes_size(*p_previous_version);
    auto p_previous_version_by_key = mutils::from_bytes_noalloc<persistent::version_t>(dsm,v + pos);
    pos += mutils::bytes_size(*p_previous_version);
    auto p_key = mutils::from_bytes_noalloc<std::string>(dsm,v + pos);
    pos += mutils::bytes_size(*p_key);
    auto p_blob = mutils::from_bytes_noalloc<Blob>(dsm, v + pos);
    return mutils::context_ptr<const ObjectWithStringKey>(
        new ObjectWithStringKey{
#ifdef ENABLE_EVALUATION
        *p_message_id,
#endif
        *p_version,
        *p_timestamp_us,
        *p_previous_version,
        *p_previous_version_by_key,
        *p_key,
        *p_blob,true});
}

} // namespace cascade
} // namespace derecho

