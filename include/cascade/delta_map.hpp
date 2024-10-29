#pragma once

#include <derecho/core/derecho.hpp>

#include <map>
#include <memory>
#include <type_traits>
#include <unordered_map>
#include <vector>

// Note: Nothing in DeltaMap depends on Cascade; it's just in the cascade namespace for consistency.
// It could be part of Derecho and declared only in the derecho namespace.

namespace derecho {
namespace cascade {

/**
 * A class that adds Persistent Delta support to a std::map<K, V>. Requires the
 * user to specify a pointer to a special "invalid value", which is an instance
 * of type V that will be used to represent deleted keys in the delta log. If V
 * is a class, IV should be a static member of that class.
 *
 * @tparam K The key type
 * @tparam V The value type
 * @tparam IV A pointer to a pre-allocated "null" value object
 */
template <typename K, typename V, V* IV>
class DeltaMap : public mutils::ByteRepresentable,
                 public persistent::IDeltaSupport<DeltaMap<K, V, IV>> {
public:
    /**
     * Represents the data stored in a delta entry, which is an unordered list
     * of key-value pairs that were changed in that delta. Used by the Persistent
     * Delta API for the getDelta functions (when called on DeltaMap).
     */
    class DeltaType : public mutils::ByteRepresentable {
    public:
        std::unordered_map<K, V> objects;
        DeltaType();
        virtual std::size_t to_bytes(uint8_t*) const override;
        virtual void post_object(const std::function<void(uint8_t const* const, std::size_t)>&) const override;
        virtual std::size_t bytes_size() const override;
        virtual void ensure_registered(mutils::DeserializationManager&);
        static std::unique_ptr<DeltaType> from_bytes(mutils::DeserializationManager* dsm, const uint8_t* const buffer);
        static mutils::context_ptr<DeltaType> from_bytes_noalloc(
                mutils::DeserializationManager* dsm,
                const uint8_t* const buffer);
        static mutils::context_ptr<const DeltaType> from_bytes_noalloc_const(
                mutils::DeserializationManager* dsm,
                const uint8_t* const buffer);
    };

private:
    /** The current delta is a list of keys that have been changed since the last delta was saved. */
    std::vector<K> delta;
    /** The current state of the map in memory */
    std::map<K, V> current_map;

public:
    // Persistent Delta API
    virtual size_t currentDeltaSize() override;
    /**
     * Serializes the current delta to a buffer as specified in IDeltaSupport.
     * The serialized delta format used by this class is:
     * (1) First sizeof(size_t) bytes is the number of entries in the delta
     * (2) A sequence of the specified number of entries, each of which is a
     *     serialized K object followed by a serialized V object
     * For entries that represent deletions, the serialized V object is *IV
     * (the invalid value).
     *
     * @param buf The buffer to serialize the delta into
     * @param buf_size The size of the buffer, which the method will check to verify it is large enough
     * @return size_t The number of bytes of the buffer actually used
     */
    virtual size_t currentDeltaToBytes(uint8_t* const buf, size_t buf_size) override;
    virtual void applyDelta(uint8_t const* const serialized_delta) override;
    static std::unique_ptr<DeltaMap<K, V, IV>> create(mutils::DeserializationManager* dm);

    /**
     * Put a key-value pair in the map and generate a delta.
     *
     * @param key
     * @param value
     */
    void put(const K& key, const V& value);

    /**
     * Get the current value associated with a key; does not generate a delta.
     *
     * @param key The key to look up in the current version of the map
     * @return The value associated with the key, or *IV if the key is not in the map
     */
    const V get(const K& key) const;

    /**
     * Remove a key-value pair from the map and generate a delta.
     *
     * @param key The key to remove from the map
     */
    void remove(const K& key);

    /**
     * Returns a read-only reference to the current state of the underlying map.
     * This allows the caller to access more advanced "get-like" operations (find,
     * upper_bound, lower_bound) on the std::map without DeltaMap needing a
     * separate wrapper method for each one.
     */
    const std::map<K, V>& get_current_map() const;

    DEFAULT_SERIALIZATION_SUPPORT(DeltaMap, current_map);

    DeltaMap() = default;
    /** Deserialization constructor */
    DeltaMap(const std::map<K, V>& other_map);
    DeltaMap(std::map<K, V>&& other_map);

    virtual ~DeltaMap() = default;

private:
    /**
     * Apply a put to the current state of the map. Used internally by other operations.
     *
     * @param key
     * @param value
     */
    void apply_put(const K& key, const V& value);
};
}  // namespace cascade
}  // namespace derecho
#include "detail/delta_map_impl.hpp"
