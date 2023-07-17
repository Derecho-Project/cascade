#pragma once
#include "delta_store_core.hpp"

#include <cascade/cascade_exception.hpp>
#include <cascade/config.h>
#include <cascade/utils.hpp>
#include "debug_util.hpp"

#include <derecho/core/derecho.hpp>
#include <derecho/persistent/Persistent.hpp>

#ifdef ENABLE_EVALUATION
#include <derecho/utils/time.h>
#endif

#include <cassert>
#include <cstdint>
#include <memory>
#include <type_traits>
#include <vector>

namespace derecho {
namespace cascade {
template <typename KT, typename VT, KT* IK, VT* IV>
void DeltaCascadeStoreCore<KT, VT, IK, IV>::_Delta::set_data_len(const size_t& dlen) {
    assert(capacity >= dlen);
    this->len = dlen;
}

template <typename KT, typename VT, KT* IK, VT* IV>
uint8_t* DeltaCascadeStoreCore<KT, VT, IK, IV>::_Delta::data_ptr() {
    assert(buffer != nullptr);
    return buffer;
}

template <typename KT, typename VT, KT* IK, VT* IV>
void DeltaCascadeStoreCore<KT, VT, IK, IV>::_Delta::calibrate(const size_t& dlen) {
    size_t new_cap = dlen;
    if(this->capacity >= new_cap) {
        return;
    }
    // calculate new capacity
    int width = sizeof(size_t) << 3;
    int right_shift_bits = 1;
    new_cap--;
    while(right_shift_bits < width) {
        new_cap |= new_cap >> right_shift_bits;
        right_shift_bits = right_shift_bits << 1;
    }
    new_cap++;
    // resize
    this->buffer = (uint8_t*)realloc(buffer, new_cap);
    if(this->buffer == nullptr) {
        dbg_default_crit("{}:{} Failed to allocate delta buffer. errno={}", __FILE__, __LINE__, errno);
        throw derecho::derecho_exception("Failed to allocate delta buffer.");
    } else {
        this->capacity = new_cap;
    }
}

template <typename KT, typename VT, KT* IK, VT* IV>
bool DeltaCascadeStoreCore<KT, VT, IK, IV>::_Delta::is_empty() {
    return (this->len == 0);
}

template <typename KT, typename VT, KT* IK, VT* IV>
void DeltaCascadeStoreCore<KT, VT, IK, IV>::_Delta::clean() {
    this->len = 0;
}

template <typename KT, typename VT, KT* IK, VT* IV>
void DeltaCascadeStoreCore<KT, VT, IK, IV>::_Delta::destroy() {
    if(this->capacity > 0) {
        free(this->buffer);
    }
}

template <typename KT, typename VT, KT* IK, VT* IV>
void DeltaCascadeStoreCore<KT, VT, IK, IV>::initialize_delta() {
    delta.buffer = (uint8_t*)malloc(DEFAULT_DELTA_BUFFER_CAPACITY);
    if(delta.buffer == nullptr) {
        dbg_default_crit("{}:{} Failed to allocate delta buffer. errno={}", __FILE__, __LINE__, errno);
        throw derecho::derecho_exception("Failed to allocate delta buffer.");
    }
    delta.capacity = DEFAULT_DELTA_BUFFER_CAPACITY;
    delta.len = 0;
}

template <typename KT, typename VT, KT* IK, VT* IV>
void DeltaCascadeStoreCore<KT, VT, IK, IV>::finalizeCurrentDelta(const persistent::DeltaFinalizer& df) {
    df(this->delta.buffer, this->delta.len);
    this->delta.clean();
}

template <typename KT, typename VT, KT* IK, VT* IV>
void DeltaCascadeStoreCore<KT, VT, IK, IV>::applyDelta(uint8_t const* const delta) {
    apply_ordered_put(*mutils::from_bytes<VT>(nullptr, delta));
    mutils::deserialize_and_run(nullptr, delta, [this](const VT& value) {
        this->apply_ordered_put(value);
    });
}

template <typename KT, typename VT, KT* IK, VT* IV>
void DeltaCascadeStoreCore<KT, VT, IK, IV>::apply_ordered_put(const VT& value) {
    // for lockless check
    this->lockless_v1.store(value.get_version(), std::memory_order_relaxed);
    // compiler reordering barrier
#ifdef __GNUC__
    asm volatile("" ::
                         : "memory");
#else
#error Lockless support is currently for GCC only
#endif

    this->kv_map.erase(value.get_key_ref());
    this->kv_map.emplace(value.get_key_ref(), value);

    // compiler reordering barrier
#ifdef __GNUC__
    asm volatile("" ::
                         : "memory");
#else
#error Lockless support is currently for GCC only
#endif

    // for lockless check
    this->lockless_v2.store(value.get_version(), std::memory_order_relaxed);
}

template <typename KT, typename VT, KT* IK, VT* IV>
std::unique_ptr<DeltaCascadeStoreCore<KT, VT, IK, IV>> DeltaCascadeStoreCore<KT, VT, IK, IV>::create(mutils::DeserializationManager* dm) {
    if(dm != nullptr) {
        try {
            return std::make_unique<DeltaCascadeStoreCore<KT, VT, IK, IV>>();
        } catch(...) {
        }
    }
    return std::make_unique<DeltaCascadeStoreCore<KT, VT, IK, IV>>();
}

template <typename KT, typename VT, KT* IK, VT* IV>
persistent::version_t DeltaCascadeStoreCore<KT, VT, IK, IV>::ordered_put(const VT& value, persistent::version_t previous_version) {
    // call validator
    if constexpr(std::is_base_of<IValidator<KT, VT>, VT>::value) {
        if(!value.validate(this->kv_map)) {
            throw invalid_value_exception("Validation failed with value of key:" + value.get_key_ref() + ".");
        }
    }

    persistent::version_t previous_version_by_key = persistent::INVALID_VERSION;
    if(kv_map.find(value.get_key_ref()) != this->kv_map.end()) {
        previous_version_by_key = this->kv_map.at(value.get_key_ref()).get_version();
    }

    // verify version MUST happen before updating it's previous versions (prev_ver,prev_ver_by_key).
    if constexpr(std::is_base_of<IVerifyPreviousVersion, VT>::value) {
        if(!value.verify_previous_version(previous_version, previous_version_by_key)) {
            // reject the package if verify failed.
            throw invalid_version_exception(previous_version,previous_version_by_key);
        }
    }
    if constexpr(std::is_base_of<IKeepPreviousVersion, VT>::value) {
        value.set_previous_version(previous_version, previous_version_by_key);
    }
    // create delta.
    assert(this->delta.is_empty());
    this->delta.calibrate(mutils::bytes_size(value));
    mutils::to_bytes(value, this->delta.data_ptr());
    this->delta.set_data_len(mutils::bytes_size(value));
    // apply_ordered_put
    apply_ordered_put(value);
    return previous_version_by_key;
}

template <typename KT, typename VT, KT* IK, VT* IV>
persistent::version_t DeltaCascadeStoreCore<KT, VT, IK, IV>::ordered_remove(const VT& value, persistent::version_t previous_version) {
    auto& key = value.get_key_ref();
    assert(value.is_null());
    // test if key exists
    if(kv_map.find(key) == kv_map.end()) {
        // skip it when no such key.
        if constexpr(std::is_convertible_v<KT,std::string>) {
            throw invalid_value_exception(std::string("Cannot found key:") + value.get_key_ref());
        } else {
            throw invalid_value_exception(std::string("Cannot found key:") + std::to_string(value.get_key_ref()));
        }
    } else if(kv_map.at(key).is_null()) {
        // and skip the keys has been deleted already.
        if constexpr(std::is_convertible_v<KT,std::string>) {
            throw invalid_value_exception(std::string("Key :") + value.get_key_ref() + " has been removed already.");
        } else {
            throw invalid_value_exception(std::string("Key :") + std::to_string(value.get_key_ref()) + " has been removed already.");
        }
    }

    persistent::version_t previous_version_by_key = kv_map.at(key).get_version();

    if constexpr(std::is_base_of<IKeepPreviousVersion, VT>::value) {
        value.set_previous_version(previous_version, previous_version_by_key);
    }
    // create delta.
    assert(this->delta.is_empty());
    this->delta.calibrate(mutils::bytes_size(value));
    mutils::to_bytes(value, this->delta.data_ptr());
    this->delta.set_data_len(mutils::bytes_size(value));
    // apply_ordered_put
    apply_ordered_put(value);
    return previous_version_by_key;
}

template <typename KT, typename VT, KT* IK, VT* IV>
const VT DeltaCascadeStoreCore<KT, VT, IK, IV>::ordered_get(const KT& key) const {
    if(kv_map.find(key) != kv_map.end()) {
        return kv_map.at(key);
    } else {
        return *IV;
    }
}

template <typename KT, typename VT, KT* IK, VT* IV>
const VT DeltaCascadeStoreCore<KT, VT, IK, IV>::lockless_get(const KT& key) const {
    persistent::version_t v1, v2;
    static thread_local VT copied_out;
    do {
        // This only for TSO memory reordering.
        v2 = this->lockless_v2.load(std::memory_order_relaxed);
        // compiler reordering barrier
#ifdef __GNUC__
        asm volatile("" ::
                             : "memory");
#else
#error Lockless support is currently for GCC only
#endif
        /*
         * An out_of_range exception can be thrown even if 'key' exists in
         * kv_map. Since std::map is not thread-safe, and there is another
         * thread modifying kv_map concurrently, the internal data structure can
         * be changed while this thread is inside kv_map.at(key). Therefore, we
         * keep trying until it is possible to copy either the object we are
         * looking for, or the invalid object.
         */
        while(true) {
            try {
                if(this->kv_map.find(key) != this->kv_map.end()) {
                    copied_out.copy_from(this->kv_map.at(key));
                } else {
                    copied_out.copy_from(*IV);
                }

                break;
            } catch (const std::out_of_range&) {
                dbg_default_debug("{}: out_of_range exception thrown while trying to get key {}", __PRETTY_FUNCTION__, key);
            }
        }
        // compiler reordering barrier
#ifdef __GNUC__
        asm volatile("" ::
                             : "memory");
#else
#error Lockless support is currently for GCC only
#endif
        v1 = this->lockless_v1.load(std::memory_order_relaxed);
        // busy sleep
        std::this_thread::yield();
    } while(v1 != v2);
    return copied_out;
}

template <typename KT, typename VT, KT* IK, VT* IV>
std::vector<KT> DeltaCascadeStoreCore<KT, VT, IK, IV>::lockless_list_keys(const std::string& prefix) const {
    persistent::version_t v1, v2;
    std::vector<KT> key_list;
    do {
        // This only for TSO memory reordering.
        v2 = this->lockless_v2.load(std::memory_order_relaxed);
        // compiler reordering barrier
#ifdef __GNUC__
        asm volatile("" ::
                             : "memory");
#else
#error Lockless support is currently for GCC only
#endif
        for(const auto& kv : kv_map) {
            if(get_pathname<KT>(kv.first).find(prefix) == 0) {
                key_list.push_back(kv.first);
            }
        }
        // compiler reordering barrier
#ifdef __GNUC__
        asm volatile("" ::
                             : "memory");
#else
#error Lockless support is currently for GCC only
#endif
        v1 = this->lockless_v1.load(std::memory_order_relaxed);
        // busy sleep
        std::this_thread::yield();
    } while(v1 != v2);
    return key_list;
}

template <typename KT, typename VT, KT* IK, VT* IV>
std::vector<KT> DeltaCascadeStoreCore<KT, VT, IK, IV>::ordered_list_keys(const std::string& prefix) {
    std::vector<KT> key_list;
    for(const auto& kv : kv_map) {
        if(get_pathname<KT>(kv.first).find(prefix) == 0) {
            key_list.push_back(kv.first);
        }
    }
    return key_list;
}

template <typename KT, typename VT, KT* IK, VT* IV>
uint64_t DeltaCascadeStoreCore<KT, VT, IK, IV>::ordered_get_size(const KT& key) {
    if(kv_map.find(key) != kv_map.end()) {
        return mutils::bytes_size(kv_map.at(key));
    } else {
        return 0;
    }
}

template <typename KT, typename VT, KT* IK, VT* IV>
uint64_t DeltaCascadeStoreCore<KT, VT, IK, IV>::lockless_get_size(const KT& key) const {
    persistent::version_t v1, v2;
    uint64_t size = 0ull;

    do {
        // This only for TSO memory reordering.
        v2 = this->lockless_v2.load(std::memory_order_relaxed);
        // compiler reordering barrier
#ifdef __GNUC__
        asm volatile("" ::
                             : "memory");
#else
#error Lockless support is currently for GCC only
#endif
        if(this->kv_map.find(key) != this->kv_map.end()) {
            size = mutils::bytes_size(this->kv_map.at(key));
        }
        // compiler reordering barrier
#ifdef __GNUC__
        asm volatile("" ::
                             : "memory");
#else
#error Lockless support is currently for GCC only
#endif
        v1 = this->lockless_v1.load(std::memory_order_relaxed);
        // busy sleep
        std::this_thread::yield();
    } while(v1 != v2);
    return size;
}

template <typename KT, typename VT, KT* IK, VT* IV>
DeltaCascadeStoreCore<KT, VT, IK, IV>::DeltaCascadeStoreCore() : lockless_v1(persistent::INVALID_VERSION),
                                                                 lockless_v2(persistent::INVALID_VERSION) {
    initialize_delta();
}

template <typename KT, typename VT, KT* IK, VT* IV>
DeltaCascadeStoreCore<KT, VT, IK, IV>::DeltaCascadeStoreCore(const std::map<KT, VT>& _kv_map) : lockless_v1(persistent::INVALID_VERSION),
                                                                                                lockless_v2(persistent::INVALID_VERSION),
                                                                                                kv_map(_kv_map) {
    initialize_delta();
}

template <typename KT, typename VT, KT* IK, VT* IV>
DeltaCascadeStoreCore<KT, VT, IK, IV>::DeltaCascadeStoreCore(std::map<KT, VT>&& _kv_map) : lockless_v1(persistent::INVALID_VERSION),
                                                                                           lockless_v2(persistent::INVALID_VERSION),
                                                                                           kv_map(std::move(_kv_map)) {
    initialize_delta();
}

template <typename KT, typename VT, KT* IK, VT* IV>
DeltaCascadeStoreCore<KT, VT, IK, IV>::~DeltaCascadeStoreCore() {
    if(this->delta.buffer != nullptr) {
        free(this->delta.buffer);
    }
}

}  // namespace cascade
}  // namespace derecho
