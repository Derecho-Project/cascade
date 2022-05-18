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
    // TODO: use max payload size in subgroup configuration.
#define DEFAULT_DELTA_BUFFER_CAPACITY (4096)
    /**
     * Initialize the delta data structure.
     */
    void initialize_delta();

private:
#if defined(__i386__) || defined(__x86_64__) || defined(_M_AMD64) || defined(_M_IX86)
    mutable std::atomic<persistent::version_t> lockless_v1;
    mutable std::atomic<persistent::version_t> lockless_v2;
#else
#error The lockless reader/writer works only with TSO memory reordering. Please check https://en.wikipedia.org/wiki/Memory_ordering
#endif

public:
    // delta
    typedef struct {
        size_t capacity;
        size_t len;
        uint8_t* buffer;
        // methods
        inline void set_data_len(const size_t& dlen);
        inline uint8_t* data_ptr();
        inline void calibrate(const size_t& dlen);
        inline bool is_empty();
        inline void clean();
        inline void destroy();
    } _Delta;
    _Delta delta;

    struct DeltaBytesFormat {
        uint32_t op;
        uint8_t first_data_byte;
    };

    std::map<KT, VT> kv_map;

    //////////////////////////////////////////////////////////////////////////
    // Delta is represented by an operation id and a list of
    // argument. The operation id (OPID) is a 4 bytes integer.
    // 1) put(const Object& object):
    // [OPID:PUT]   [value]
    // 2) remove(const KT& key)
    // [OPID:REMOVE][key]
    // 3) get(const KT& key)
    // no need to prepare a delta
    ///////////////////////////////////////////////////////////////////////////
    virtual void finalizeCurrentDelta(const persistent::DeltaFinalizer& df) override;
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
