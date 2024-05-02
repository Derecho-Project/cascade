#pragma once
#include "delta_store_core.hpp"

#include "cascade/config.h"
#include "cascade/utils.hpp"
#include "debug_util.hpp"

#include <derecho/core/derecho.hpp>
#include <derecho/mutils-serialization/SerializationSupport.hpp>
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
DeltaCascadeStoreCore<KT, VT, IK, IV>::DeltaType::DeltaType() {}

template <typename KT, typename VT, KT* IK, VT* IV>
std::size_t DeltaCascadeStoreCore<KT, VT, IK, IV>::DeltaType::to_bytes(uint8_t*) const {
    dbg_default_warn("{} should not be called. It is not designed for serialization.",__PRETTY_FUNCTION__);
    return 0;
}

template <typename KT, typename VT, KT* IK, VT* IV>
void DeltaCascadeStoreCore<KT, VT, IK, IV>::DeltaType::post_object(
        const std::function<void(uint8_t const* const buf, std::size_t)>&) const {
    dbg_default_warn("{} should not be called. It is not designed for serialization.",__PRETTY_FUNCTION__);
}

template <typename KT, typename VT, KT* IK, VT* IV>
std::size_t DeltaCascadeStoreCore<KT, VT, IK, IV>::DeltaType::bytes_size() const {
    dbg_default_warn("{} should not be called. It is not designed for serialization.",__PRETTY_FUNCTION__);
    return 0;
}

template <typename KT, typename VT, KT* IK, VT* IV>
void DeltaCascadeStoreCore<KT, VT, IK, IV>::DeltaType::ensure_registered(mutils::DeserializationManager&) {}

template <typename KT, typename VT, KT* IK, VT* IV>
std::unique_ptr<typename DeltaCascadeStoreCore<KT, VT, IK, IV>::DeltaType>
DeltaCascadeStoreCore<KT, VT, IK, IV>::DeltaType::from_bytes(
    mutils::DeserializationManager* dsm,const uint8_t* const v) {
    size_t pos = 0;
    std::size_t num_objects = *mutils::from_bytes_noalloc<std::size_t>(dsm,v + pos);
    pos += mutils::bytes_size(num_objects);
    auto pdelta = std::make_unique<DeltaCascadeStoreCore<KT,VT,IK,IV>::DeltaType>();
    while (num_objects--) {
        auto po = mutils::from_bytes<VT>(dsm,v + pos);
        pos += mutils::bytes_size(*po);
        KT key = po->get_key_ref();
        pdelta->objects.emplace(std::move(key),std::move(*po));
    }
    return pdelta;
}

template <typename KT, typename VT, KT* IK, VT* IV>
mutils::context_ptr<typename DeltaCascadeStoreCore<KT, VT, IK, IV>::DeltaType>
DeltaCascadeStoreCore<KT, VT, IK, IV>::DeltaType::from_bytes_noalloc(
    mutils::DeserializationManager* dsm,const uint8_t* const v) {
    size_t pos = 0;
    std::size_t num_objects = *mutils::from_bytes_noalloc<std::size_t>(dsm,v + pos);
    pos += mutils::bytes_size(num_objects);
    auto* pdelta = new DeltaCascadeStoreCore<KT,VT,IK,IV>::DeltaType();
    while (num_objects--) {
        auto po = mutils::from_bytes_noalloc<VT>(dsm,const_cast<uint8_t* const>(v) + pos);
        pos += mutils::bytes_size(*po);
        KT key = po->get_key_ref();
        pdelta->objects.emplace(std::move(key),std::move(*po));
    }
    return mutils::context_ptr<DeltaCascadeStoreCore<KT,VT,IK,IV>::DeltaType>(pdelta);
}

template <typename KT, typename VT, KT* IK, VT* IV>
mutils::context_ptr<const typename DeltaCascadeStoreCore<KT, VT, IK, IV>::DeltaType>
DeltaCascadeStoreCore<KT, VT, IK, IV>::DeltaType::from_bytes_noalloc_const(
    mutils::DeserializationManager* dsm,const uint8_t* const v) {
    size_t pos = 0;
    std::size_t num_objects = *mutils::from_bytes_noalloc<std::size_t>(dsm,v + pos);
    pos += mutils::bytes_size(num_objects);
    auto* pdelta = new DeltaCascadeStoreCore<KT,VT,IK,IV>::DeltaType();
    while (num_objects--) {
        auto po = mutils::from_bytes_noalloc<VT>(dsm,const_cast<uint8_t* const>(v) + pos);
        pos += mutils::bytes_size(*po);
        KT key = po->get_key_ref();
        pdelta->objects.emplace(std::move(key),std::move(*po));
    }
    return mutils::context_ptr<const DeltaCascadeStoreCore<KT,VT,IK,IV>::DeltaType>(pdelta);
}

template <typename KT, typename VT, KT* IK, VT* IV>
size_t DeltaCascadeStoreCore<KT, VT, IK, IV>::currentDeltaSize() {
    size_t delta_size = 0;
    if (delta.size() > 0) {
        delta_size += mutils::bytes_size(static_cast<std::size_t>(delta.size()));
        for (const auto& k:delta) {
            delta_size+=mutils::bytes_size(this->kv_map[k]);
        }
    }
    return delta_size;
}

template <typename KT, typename VT, KT* IK, VT* IV>
size_t DeltaCascadeStoreCore<KT, VT, IK, IV>::currentDeltaToBytes(uint8_t * const buf, size_t buf_size) {
    size_t delta_size = currentDeltaSize();
    if (delta_size == 0) return 0;
    if (delta_size > buf_size) {
        dbg_default_error("{}: failed because we need {} bytes for delta, but only a buffer with {} bytes given.\n",
            __PRETTY_FUNCTION__, delta_size, buf_size);
    }
    size_t offset = mutils::to_bytes(static_cast<std::size_t>(delta.size()),buf);
    for(const auto& k:delta) {
        offset += mutils::to_bytes(this->kv_map[k],buf+offset);
    }
    delta.clear();
    return offset;
}

template <typename KT, typename VT, KT* IK, VT* IV>
void DeltaCascadeStoreCore<KT, VT, IK, IV>::applyDelta(uint8_t const* const serialized_delta) {
    std::size_t num_objects = *mutils::from_bytes<std::size_t>(nullptr,serialized_delta);
    size_t offset = mutils::bytes_size(num_objects);
    while (num_objects--) {
        offset +=
            mutils::deserialize_and_run(nullptr, serialized_delta + offset,
                [this](const VT& value) {
                    this->apply_ordered_put(value);
                    return mutils::bytes_size(value);
                }
            );
    }
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
bool DeltaCascadeStoreCore<KT, VT, IK, IV>::ordered_put(const VT& value, persistent::version_t prev_ver) {
    // call validator
    if constexpr(std::is_base_of<IValidator<KT, VT>, VT>::value) {
        if(!value.validate(this->kv_map)) {
            return false;
        }
    }

    // verify version MUST happen before updating it's previous versions (prev_ver,prev_ver_by_key).
    if constexpr(std::is_base_of<IVerifyPreviousVersion, VT>::value) {
        bool verify_result;
        if(kv_map.find(value.get_key_ref()) != this->kv_map.end()) {
            verify_result = value.verify_previous_version(prev_ver, this->kv_map.at(value.get_key_ref()).get_version());
        } else {
            verify_result = value.verify_previous_version(prev_ver, persistent::INVALID_VERSION);
        }
        if(!verify_result) {
            // reject the package if verify failed.
            return false;
        }
    }
    if constexpr(std::is_base_of<IKeepPreviousVersion, VT>::value) {
        persistent::version_t prev_ver_by_key = persistent::INVALID_VERSION;
        if(kv_map.find(value.get_key_ref()) != kv_map.end()) {
            prev_ver_by_key = kv_map.at(value.get_key_ref()).get_version();
        }
        value.set_previous_version(prev_ver, prev_ver_by_key);
    }
    // create delta.
    assert(this->delta.empty());
    this->delta.push_back(value.get_key_ref());
    // apply_ordered_put
    apply_ordered_put(value);
    return true;
}

template <typename KT, typename VT, KT* IK, VT* IV>
bool DeltaCascadeStoreCore<KT, VT, IK, IV>::ordered_remove(const VT& value, persistent::version_t prev_ver) {
    auto& key = value.get_key_ref();
    // test if key exists
    if(kv_map.find(key) == kv_map.end()) {
        // skip it when no such key.
        return false;
    } else if(kv_map.at(key).is_null()) {
        // and skip the keys has been deleted already.
        return false;
    }

    if constexpr(std::is_base_of<IKeepPreviousVersion, VT>::value) {
        value.set_previous_version(prev_ver, kv_map.at(key).get_version());
    }
    // create delta.
    assert(this->delta.empty());
    this->delta.push_back(key);
    // apply_ordered_put
    apply_ordered_put(value);
    return true;
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
}

template <typename KT, typename VT, KT* IK, VT* IV>
DeltaCascadeStoreCore<KT, VT, IK, IV>::DeltaCascadeStoreCore(const std::map<KT, VT>& _kv_map) : lockless_v1(persistent::INVALID_VERSION),
                                                                                                lockless_v2(persistent::INVALID_VERSION),
                                                                                                kv_map(_kv_map) {
}

template <typename KT, typename VT, KT* IK, VT* IV>
DeltaCascadeStoreCore<KT, VT, IK, IV>::DeltaCascadeStoreCore(std::map<KT, VT>&& _kv_map) : lockless_v1(persistent::INVALID_VERSION),
                                                                                           lockless_v2(persistent::INVALID_VERSION),
                                                                                           kv_map(std::move(_kv_map)) {
}

template <typename KT, typename VT, KT* IK, VT* IV>
DeltaCascadeStoreCore<KT, VT, IK, IV>::~DeltaCascadeStoreCore() {
}

}  // namespace cascade
}  // namespace derecho
