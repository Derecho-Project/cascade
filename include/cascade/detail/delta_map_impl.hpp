#pragma once

#include "../delta_map.hpp"

#include <cstdint>

namespace derecho {
namespace cascade {

template <typename K, typename V, V* IV>
DeltaMap<K, V, IV>::DeltaType::DeltaType() {}

template <typename K, typename V, V* IV>
std::size_t DeltaMap<K, V, IV>::DeltaType::to_bytes(uint8_t*) const {
    dbg_default_warn("{} should not be called. It is not designed for serialization.", __PRETTY_FUNCTION__);
    return 0;
}

template <typename K, typename V, V* IV>
void DeltaMap<K, V, IV>::DeltaType::post_object(
        const std::function<void(uint8_t const* const, std::size_t)>&) const {
    dbg_default_warn("{} should not be called. It is not designed for serialization.", __PRETTY_FUNCTION__);
}

template <typename K, typename V, V* IV>
std::size_t DeltaMap<K, V, IV>::DeltaType::bytes_size() const {
    // Derive the number of bytes that would have just been deserialized by from_bytes
    // This is needed by mutils::deserialize_and_run, which is called by persistent::getDelta
    std::size_t num_bytes = sizeof(std::size_t);
    for(const auto& pair : objects) {
        num_bytes += mutils::bytes_size(pair.first) + mutils::bytes_size(pair.second);
    }
    return num_bytes;
}

template <typename K, typename V, V* IV>
void DeltaMap<K, V, IV>::DeltaType::ensure_registered(mutils::DeserializationManager&) {}

template <typename K, typename V, V* IV>
std::unique_ptr<typename DeltaMap<K, V, IV>::DeltaType>
DeltaMap<K, V, IV>::DeltaType::from_bytes(
        mutils::DeserializationManager* dsm, const uint8_t* const buffer) {
    // Expects the same serialized delta format as created in currentDeltaToBytes():
    // Length of the list, followed by a list of (serialized K, serialized V) pairs
    size_t pos = 0;
    std::size_t num_pairs = *mutils::from_bytes_noalloc<std::size_t>(dsm, buffer + pos);
    pos += mutils::bytes_size(num_pairs);
    auto delta = std::make_unique<DeltaMap<K, V, IV>::DeltaType>();
    while(num_pairs--) {
        auto key_ptr = mutils::from_bytes<K>(dsm, buffer + pos);
        pos += mutils::bytes_size(*key_ptr);
        auto val_ptr = mutils::from_bytes<V>(dsm, buffer + pos);
        pos += mutils::bytes_size(*val_ptr);
        delta->objects.emplace(std::move(*key_ptr), std::move(*val_ptr));
    }
    return delta;
}

template <typename K, typename V, V* IV>
mutils::context_ptr<typename DeltaMap<K, V, IV>::DeltaType>
DeltaMap<K, V, IV>::DeltaType::from_bytes_noalloc(
        mutils::DeserializationManager* dsm, const uint8_t* const buffer) {
    size_t pos = 0;
    std::size_t num_pairs = *mutils::from_bytes_noalloc<std::size_t>(dsm, buffer + pos);
    pos += mutils::bytes_size(num_pairs);
    auto* delta = new DeltaMap<K, V, IV>::DeltaType();
    while(num_pairs--) {
        auto key_ptr = mutils::from_bytes_noalloc<K>(dsm, const_cast<uint8_t* const>(buffer) + pos);
        pos += mutils::bytes_size(*key_ptr);
        auto val_ptr = mutils::from_bytes_noalloc<V>(dsm, const_cast<uint8_t* const>(buffer) + pos);
        pos += mutils::bytes_size(*val_ptr);
        delta->objects.emplace(std::move(*key_ptr), std::move(*val_ptr));
    }
    return mutils::context_ptr<DeltaMap<K, V, IV>::DeltaType>(delta);
}

template <typename K, typename V, V* IV>
mutils::context_ptr<const typename DeltaMap<K, V, IV>::DeltaType>
DeltaMap<K, V, IV>::DeltaType::from_bytes_noalloc_const(
        mutils::DeserializationManager* dsm, const uint8_t* const buffer) {
    size_t pos = 0;
    std::size_t num_pairs = *mutils::from_bytes_noalloc<std::size_t>(dsm, buffer + pos);
    pos += mutils::bytes_size(num_pairs);
    auto* delta = new DeltaMap<K, V, IV>::DeltaType();
    while(num_pairs--) {
        auto key_ptr = mutils::from_bytes_noalloc<K>(dsm, const_cast<uint8_t* const>(buffer) + pos);
        pos += mutils::bytes_size(*key_ptr);
        auto val_ptr = mutils::from_bytes_noalloc<V>(dsm, const_cast<uint8_t* const>(buffer) + pos);
        pos += mutils::bytes_size(*val_ptr);
        delta->objects.emplace(std::move(*key_ptr), std::move(*val_ptr));
    }
    return mutils::context_ptr<const DeltaMap<K, V, IV>::DeltaType>(delta);
}

template <typename K, typename V, V* IV>
std::unique_ptr<DeltaMap<K, V, IV>> DeltaMap<K, V, IV>::create(mutils::DeserializationManager* dm) {
    // What's the point of this if statement? (copied from DeltaCascadeStoreCore)
    if(dm != nullptr) {
        try {
            return std::make_unique<DeltaMap<K, V, IV>>();
        } catch(...) {
        }
    }
    return std::make_unique<DeltaMap<K, V, IV>>();
}

template <typename K, typename V, V* IV>
size_t DeltaMap<K, V, IV>::currentDeltaSize() {
    size_t delta_size = 0;
    if(delta.size() > 0) {
        delta_size += mutils::bytes_size(static_cast<std::size_t>(delta.size()));
        for(const auto& k : delta) {
            delta_size += mutils::bytes_size(k) + mutils::bytes_size(current_map[k]);
        }
    }
    return delta_size;
}

template <typename K, typename V, V* IV>
size_t DeltaMap<K, V, IV>::currentDeltaToBytes(uint8_t* const buf, size_t buf_size) {
    // Serialized delta format:
    // First sizeof(size_t) bytes is the number of entries in the delta
    // Sequence of that many (serialized K, serialized V) pairs
    size_t delta_size = currentDeltaSize();
    if(delta_size == 0) return 0;
    if(delta_size > buf_size) {
        dbg_default_error("{}: failed because we need {} bytes for delta, but only a buffer with {} bytes given.\n",
                          __PRETTY_FUNCTION__, delta_size, buf_size);
    }
    size_t offset = mutils::to_bytes(static_cast<std::size_t>(delta.size()), buf);
    for(const auto& k : delta) {
        offset += mutils::to_bytes(k, buf + offset);
        offset += mutils::to_bytes(current_map[k], buf + offset);
    }
    delta.clear();
    return offset;
}

template <typename K, typename V, V* IV>
void DeltaMap<K, V, IV>::applyDelta(uint8_t const* const serialized_delta) {
    std::size_t num_pairs = *mutils::from_bytes<std::size_t>(nullptr, serialized_delta);
    size_t offset = mutils::bytes_size(num_pairs);
    while(num_pairs--) {
        // For each entry in the delta, deserialize a K and V object and pass them to apply_put
        offset += mutils::deserialize_and_run(nullptr, serialized_delta + offset,
                                              [this](const K& key, const V& value) {
                                                  this->apply_put(key, value);
                                                  return mutils::bytes_size(key) + mutils::bytes_size(value);
                                              });
    }
}

template <typename K, typename V, V* IV>
void DeltaMap<K, V, IV>::put(const K& key, const V& value) {
    delta.push_back(key);
    apply_put(key, value);
}

template <typename K, typename V, V* IV>
void DeltaMap<K, V, IV>::remove(const K& key) {
    if(current_map.find(key) == current_map.end()) {
        // Can't remove if the key is not in the map
        if constexpr(std::is_convertible_v<K, std::string>) {
            dbg_default_warn("DeltaMap failed to remove a nonexistent key: {}", key);
        } else {
            dbg_default_warn("DeltaMap failed to remove a nonexistent key: {}", std::to_string(key));
        }
    } else if(current_map.at(key) == *IV) {
        // Can't remove if the key has been deleted already
        if constexpr(std::is_convertible_v<K, std::string>) {
            dbg_default_warn("DeltaMap remove failed; key {} has been removed already", key);
        } else {
            dbg_default_warn("DeltaMap remove failed; key {} has been removed already", std::to_string(key));
        }
    } else {
        // create delta
        delta.push_back(key);
        // Put the invalid value in the map for the removed key
        apply_put(key, *IV);
    }
}

template <typename K, typename V, V* IV>
const V DeltaMap<K, V, IV>::get(const K& key) const {
    const auto find_result = current_map.find(key);
    if(find_result != current_map.end()) {
        return find_result->second;
    } else {
        return *IV;
    }
}

template <typename K, typename V, V* IV>
const std::map<K, V>& DeltaMap<K, V, IV>::get_current_map() const {
    return current_map;
}

template <typename K, typename V, V* IV>
void DeltaMap<K, V, IV>::apply_put(const K& key, const V& value) {
    current_map.erase(key);
    current_map.emplace(key, value);
}

template <typename K, typename V, V* IV>
DeltaMap<K, V, IV>::DeltaMap(const std::map<K, V>& other_map)
        : current_map(other_map) {}

template <typename K, typename V, V* IV>
DeltaMap<K, V, IV>::DeltaMap(std::map<K, V>&& other_map)
        : current_map(std::move(other_map)) {}

}  // namespace cascade
}  // namespace derecho
