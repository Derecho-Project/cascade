#pragma once

#include "cascade/cascade_interface.hpp"

#include <derecho/core/derecho.hpp>
#include <derecho/mutils-serialization/SerializationSupport.hpp>

#include <atomic>
#include <cstdint>
#include <map>
#include <string>
#include <vector>

namespace derecho {
namespace cascade {

/**
 * Persistent Cascade Store Delta Support
 */
template <typename KT, typename VT, KT* IK, VT* IV>
class DeltaCascadeStoreCore : public mutils::ByteRepresentable,
                              public persistent::IDeltaSupport<DeltaCascadeStoreCore<KT, VT, IK, IV>> {
private:
#if defined(__i386__) || defined(__x86_64__) || defined(_M_AMD64) || defined(_M_IX86)
    mutable std::atomic<persistent::version_t> lockless_v1;
    mutable std::atomic<persistent::version_t> lockless_v2;
#else
#error The lockless reader/writer works only with TSO memory reordering. Please check https://en.wikipedia.org/wiki/Memory_ordering
#endif

public:
    /**
     * @class DeltaType
     * @brief the delta type that are stored in a delta.
     * 1) The first sizeof(std::vector<KT>::size_type) bytes is the number of VT objects in the
     * delta, followed by specified number of 
     * 2) Serialized VT objects.
     */
    class DeltaType : public mutils::ByteRepresentable {
    public:
        /* The objects */
        std::unordered_map<KT,VT> objects;
        /** @fn DeltaType
         *  @brief Constructor
         */
        DeltaType();
        virtual std::size_t to_bytes(uint8_t*) const override;
        virtual void post_object(const std::function<void(uint8_t const* const, std::size_t)>&) const override;
        virtual std::size_t bytes_size() const override;
        virtual void ensure_registered(mutils::DeserializationManager&);
        static std::unique_ptr<DeltaType> from_bytes(mutils::DeserializationManager*,const uint8_t* const);
        static mutils::context_ptr<DeltaType> from_bytes_noalloc(
            mutils::DeserializationManager*,
            const uint8_t* const);
        static mutils::context_ptr<const DeltaType> from_bytes_noalloc_const(
            mutils::DeserializationManager*,
            const uint8_t* const);
    };
    /** The delta is a list of keys for the objects that are changed by put or remove. */
    std::vector<KT> delta;
    /** The KV map */
    std::map<KT, VT> kv_map;

    //////////////////////////////////////////////////////////////////////////
    // Delta is represented by a list of objects for both put and remove
    // operations, where the latter one is a list of objects with only key but
    // is empty. Get operations will not create a delta. The first 4 bytes of
    // the delta is the number of deltas.
    ///////////////////////////////////////////////////////////////////////////
    virtual size_t currentDeltaSize() override;
    virtual size_t currentDeltaToBytes(uint8_t * const buf, size_t buf_size) override;
    virtual void applyDelta(uint8_t const* const delta) override;
    static std::unique_ptr<DeltaCascadeStoreCore<KT, VT, IK, IV>> create(mutils::DeserializationManager* dm);
    /**
     * apply put to current state
     */
    void apply_ordered_put(const VT& value);
    /**
     * Ordered put, and generate a delta.
     */
    virtual bool ordered_put(const VT& value, persistent::version_t prever);
    /**
     * Ordered remove, and generate a delta.
     */
    virtual bool ordered_remove(const VT& value, persistent::version_t prev_ver);
    /**
     * ordered get, no need to generate a delta.
     */
    virtual const VT ordered_get(const KT& key) const;
    /**
     * lockless get for the caller from a thread other than the predicate thread.
     */
    virtual const VT lockless_get(const KT& key) const;
    /**
     * ordered list_keys, no need to generate a delta.
     */
    virtual std::vector<KT> ordered_list_keys(const std::string& prefix);
    /**
     * locklessly list keys for the caller from a thread other than the predicate thread.
     */
    virtual std::vector<KT> lockless_list_keys(const std::string& prefix) const;
    /**
     * ordered get_size, not need to generate a delta.
     */
    virtual uint64_t ordered_get_size(const KT& key);
    /**
     * locklessly get size of an object
     */
    virtual uint64_t lockless_get_size(const KT& key) const;

    // serialization supports
    DEFAULT_SERIALIZATION_SUPPORT(DeltaCascadeStoreCore, kv_map);

    // constructors
    DeltaCascadeStoreCore();
    DeltaCascadeStoreCore(const std::map<KT, VT>& _kv_map);
    DeltaCascadeStoreCore(std::map<KT, VT>&& _kv_map);

    // destructor
    virtual ~DeltaCascadeStoreCore();
};
}  // namespace cascade
}  // namespace derecho

#include "delta_store_core_impl.hpp"
