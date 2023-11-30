#pragma once

/**
 * @file object_commit_protocol.hpp
 * @brief defines the protocol between mproc udl client and mproc udl server
 */

#include <cascade/config.h>
#include <cascade/cascade.hpp>
#include <memory>
#include <type_traits>
#include <wsong/ipc/ring_buffer.hpp>

namespace derecho {
namespace cascade {

#define OBJECT_COMMIT_REQUEST_SIZE          (PAGE_SIZE)
#define OBJECT_COMMIT_REQUEST_MEMORY_MASK   (0x0000000000000003L)
#define OBJECT_COMMIT_REQUEST_MEMORY_INLINE (0x0000000000000000L)
#define OBJECT_COMMIT_REQUEST_MEMORY_SHMEM  (0x0000000000000001L)

#define DEFINE_OBJECT_COMMIT_REQUEST(ocr)   \
    uint8_t     __## ocr ## _buf__[OBJECT_COMMIT_REQUEST_SIZE] __attribute__((aligned(CACHELINE_SIZE))); \
    ObjectCommitRequestHeader* ocr = reinterpret_cast<ObjectCommitRequestHeader*>(__ ## ocr ## _buf__);

/**
 * @brief ObjectCommitRequestHeader class template
 *
 * This class relies on the `SharedMemory` singlton class for zero-copy support(`get_object_nocopy`).
 */
class ObjectCommitRequestHeader {
public:
    /**
     * @brief The sender's id.
     * `node_id_t` is `unit32_t`
     */
    node_id_t       sender_id;
    /**
     * @brief The prefix_length.
     */
    uint32_t        prefix_length;
    /**
     * @brief The version assigned to this put operation.
     */
    version_t       version;
    /**
     * @brief Control flags for the request
     * - `flags & OBJECT_COMMIT_REQUEST_MEMORY_MASK` tells memory type: inline or shared memory
     */
    uint64_t        flags;
    /**
     * @brief Key of the shared memory, valid ONLY when OBJECT_COMMIT_REQUEST_MEMORY_SHMEM is set.
     */
    key_t           shm_key;
    /**
     * @brief The offset of serialized output edges in the rest array
     */
    uint32_t        output_edges_offset;
    /**
     * @brief The offset of seriealized object in `rest`, valid ONLY when OBJECT_COMMIT_REQUEST_MEMORY_INLINE is set.
     */
    uint32_t        inline_object_offset;
    /**
     * @brief The offset of the padding bytes.
     */
    uint32_t        padding_offset;
    /**
     * @brief Offset in the shared memory region, valid ONLY when OBJECT COMMIT_REQUEST_MEMORY_SHM is set.
     */
    uint64_t        shm_offset;
    /**
     * @brief Space for the serialized fields.
     * Currently, two serialized object is stored here as byte array:
     * - a `std::string` object, representing the key, followed by
     * - an `std::unordered_map<std::string,bool>` object, representing the output edges of this UDL, possibly followed
     *   by
     * - an `ObjectType` object, if the flags attribute says the object is in the inline memory:
     *   `this->flags` & `OBJECT_COMMIT_REQUEST_MEMORY_MASK` == `OBJECT_COMMIT_REQUEST_MEMORY_SHMEM`, followed by
     * - padding bytes.
     */
    uint8_t         rest[];
    /**
     * @fn size_t total_size()
     * @brief
     * @return  The total size of the commit request
     */
    inline size_t total_size() const {
        return sizeof(ObjectCommitRequestHeader) + this->padding_offset;
    }
    /**
     * @fn ObjectCommitRequestHeader& copy_from (const ObjectCommitRequestHeader& rhs)
     * @brief evaluator
     * @param[in]   rhs     The other object
     * @return A reference to this object
     */
    inline ObjectCommitRequestHeader& copy_from(const ObjectCommitRequestHeader& rhs) {
        size_t copy_size = rhs.total_size();
        assert(copy_size <= OBJECT_COMMIT_REQUEST_SIZE);
        std::memcpy(this,&rhs,copy_size);
        return *this;
    }
    /**
     * @fn std::unique_ptr<ObjectType> get_object_copy()
     * @brief   Get the object from the Request.
     * @tparam  ObjectType      The type of the object in the request, which must implements mutils::ByteReprentable
     *
     * The object is returned in a unique pointer, which does not rely on `this` object because the object data has been
     * copied out.
     *
     * @return  A unique pointer holding the object.
     */
    template<typename ObjectType>
    std::unique_ptr<ObjectType>      get_object_copy() const;
    /**
     * @fn mutils::context_ptr<ObjectType>  get_object_nocopy()
     * @brief   Get the object from the Request without copy.
     * @tparam  ObjectType      The type of the object in the request, which must implements mutils::ByteReprentable
     *
     * The object is returned in a context pointer, which MAY rely on `this` object
     * - If `this->flags` | `OBJECT_COMMIT_REQUEST_MEMORY_MASK` == `OBJECT_COMMIT_REQUEST_MEMORY_SHMEM`, meaning that
     *   the serialized object is in shared memory, the context_ptr<ObjectType> does NOT rely on `this` object.
     * - If `this->flags` | `OBJECT_COMMIT_REQUEST_MEMORY_MASK` == `OBJECT_COMMIT_REQUEST_MEMORY_INLINE`, meaning that
     *   the serialized object is in `this->rest` following the output edges, the context_ptr<ObjectType> DOES reply on
     *   `this` object -- because it needs the existence of `this->rest`.
     *
     * @return A context pointer holding the object.
     */
    template <typename ObjectType>
    mutils::context_ptr<ObjectType>  get_object_nocopy() const;
    /**
     * @fn std::unordered_map<std::string,bool> get_output()
     * @brief   Get the output edges
     *
     * @return  A unique pointer holding the unordered map from path to trigger_put flag.
     */
    inline std::unique_ptr<std::unordered_map<std::string,bool>> get_output() const {
        return mutils::from_bytes<std::unordered_map<std::string,bool>>(nullptr,this->rest+output_edges_offset);
    }
    /**
     * @fn std::unique_ptr<std::string> get_key_string()
     * @brief   Get the key string
     *
     * @return A unique pointer holding the key string.
     */
    inline std::unique_ptr<std::string> get_key_string() const {
        return mutils::from_bytes<std::string>(nullptr,this->rest);
    }
} __attribute__ ((packed,aligned(CACHELINE_SIZE)));

static_assert(std::is_trivially_copyable_v<ObjectCommitRequestHeader> == true);

template<typename ObjectType>
std::unique_ptr<ObjectType> ObjectCommitRequestHeader::get_object_copy() const {
    if ((this->flags & OBJECT_COMMIT_REQUEST_MEMORY_MASK) == OBJECT_COMMIT_REQUEST_MEMORY_INLINE) {
        return mutils::from_bytes<ObjectType>(nullptr,this->rest + this->inline_object_offset);
    } else {
        throw derecho_exception("Shared memory support to be added.");
    }
}

template<typename ObjectType>
mutils::context_ptr<ObjectType> ObjectCommitRequestHeader::get_object_nocopy() const {
    if ((this->flags & OBJECT_COMMIT_REQUEST_MEMORY_MASK) == OBJECT_COMMIT_REQUEST_MEMORY_INLINE) {
        return mutils::from_bytes_noalloc<ObjectType>(nullptr,this->rest + this->inline_object_offset);
    } else {
        throw derecho_exception("Shared memory support to be added.");
    }
}

static_assert(sizeof(ObjectCommitRequestHeader) <= OBJECT_COMMIT_REQUEST_SIZE);
}
}
