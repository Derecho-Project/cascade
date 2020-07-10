#pragma once

#include <map>
#include <memory>
#include <string>
#include <time.h>
#include <iostream>
#include <tuple>
#include <optional>

#include <derecho/core/derecho.hpp>
#include <derecho/mutils-serialization/SerializationSupport.hpp>
#include <derecho/persistent/Persistent.hpp>

namespace derecho {
namespace cascade {
    /**
     * Watcher Type
     * CascadeWatcher takes four arguments
     * @param subgroup_id_t subgroup_id - subgroup id
     * @param const uint32_t shard_num  - shard num
     * @param const KT& key             - key
     * @param const VT& value           - value
     * @param void* cascade_context     - the cascade context
     * @return void
     */
    template<typename KT, typename VT, KT* IK, VT* IV> // TODO: use shared_ptr or not???
    class CascadeWatcher: public derecho::DeserializationContext {
    public:
        /**
         * The critical data path behaviour is defined here. The default behaviour is do nothing.
         *
         * @param subgroup_id TODO: should we use subgroup idx in the same type? which seems more useful.
         * @param shard_id
         * @param key
         * @param value
         * @param cascade_ctxt - The cascade context to be used later
         */
        virtual void operator () (const subgroup_id_t subgroup_id, const uint32_t shard_id, const KT& key, const VT& value, void* cascade_ctxt) {}
    };

//    /**
//     * Watcher Context interface
//     * Applications using CascadeStore templates MUST provide an ICascadeWatcherContext object
//     * to be notified with the updates (put/remove). Users can just use an object of
//     * IndifferentCascadeWatcherContext to ignore those updates.
//     */
//    template<typename KT, typename VT, KT* IK, VT* IV>
//    class ICascadeWatcherContext : public derecho::DeserializationContext {
//    public:
//        /**
//         * Return a shared pointer to a CascadeWatcher, which is owned by ICascadeWatcherContext.
//         * Derived classes MUST guarantee the referenced watcher is valid throughout Cascade
//         * subgroup's lifetime.
//         */
//        virtual std::shared_ptr<CascadeWatcher<KT,VT,IK,IV>> get_cascade_watcher() = 0;
//    };

//    /**
//     * Use indifferent watcher context to ignore the updates (put/remove).
//     */
//    template<typename KT, typename VT, KT* IK, VT* IV>
//    class IndifferentCascadeWatcherContext : public ICascadeWatcherContext<KT,VT,IK,IV> {
//        std::shared_ptr<CascadeWatcher<KT,VT,IK,IV>> watcher_ptr;
//
//    public:
//        // override ICascadeWatcherContext::get_cascade_watcher()
//        std::shared_ptr<CascadeWatcher<KT,VT,IK,IV>> get_cascade_watcher() override {
//            return this->watcher_ptr;
//        }
//    };

    /**
     * The cascade store interface.
     * @tparam KT The type of the key
     * @tparam VT The type of the value must
     *            - includes a public and mutable field 'ver' of type std::tuple<version_t,uint64_t> for its version and
     *              timestamp.
     *            - includes a public field 'key' of type KT for the key
     *            - TODO: enforce this with 'concepts' in C++ 20?
     * @tparam IK A pointer to an invalid key (generally a static member of class KT)
     * @tparam IV A pointer to an invalid value (generally a static member of class VT)
     */
    template <typename KT, typename VT, KT* IK, VT* IV>
    class ICascadeStore {
    public:
        /**
         * Types
         */
        using KeyType = KT;
        using ValType = VT;
        KT* InvKeyPtr = IK;
        VT* InvValPtr = IV;
        /**
         * put a value (with key)
         * @param value
         * @return a tuple including version number (version_t) and a timestamp in microseconds.
         */
        virtual std::tuple<persistent::version_t,uint64_t> put(const VT& value) = 0;
        /**
         * remove a value with key
         * @param key
         * @return a tuple including version number (version_t) and a timestamp in microseconds.
         */
        virtual std::tuple<persistent::version_t,uint64_t> remove(const KT& key) = 0;
        /**
         * get value by version
         * @param key
         * @param ver - Version, if version == INVALID_VERSION, get the latest value.
         * @return a value
         */
        virtual const VT get(const KT& key, const persistent::version_t& ver) = 0;
        /**
         * get value by timestamp
         * @param key
         * @param ts_us - timestamp in microsecond
         * @return a value
         */
        virtual const VT get_by_time(const KT& key, const uint64_t& ts_us) = 0;

    protected:
        /**
         * ordered_put
         * @param value
         * @return a tuple including version number (version_t) and a timestamp in microseconds.
         */
        virtual std::tuple<persistent::version_t,uint64_t> ordered_put(const VT& value) = 0;
        /**
         * ordered_remove
         * @param key
         * @return a tuple including version number (version_t) and a timestamp in microseconds.
         */
        virtual std::tuple<persistent::version_t,uint64_t> ordered_remove(const KT& key) = 0;
        /**
         * ordered_get
         * @param key
         * @return a value
         */
        virtual const VT ordered_get(const KT& key) = 0;
    };

    /**
     * template volatile cascade stores.
     * 
     * VolatileCascadeStore is highly efficient by manage all the data only in the memory without implementing the heavy
     * log mechanism. Reading by version or time will always return invlaid value.
     */
    template <typename KT, typename VT, KT* IK, VT* IV>
    class VolatileCascadeStore : public ICascadeStore<KT, VT, IK, IV>,
                                 public mutils::ByteRepresentable,
                                 public derecho::GroupReference {
    public:
        /* group reference */
        using derecho::GroupReference::group;
        /* volatile cascade store in memory */
        std::map<KT,VT> kv_map;
        /* watcher */
        CascadeWatcher<KT,VT,IK,IV>* cascade_watcher_ptr;
        
        REGISTER_RPC_FUNCTIONS(VolatileCascadeStore,
                               put,
                               remove,
                               get,
                               get_by_time,
                               ordered_put,
                               ordered_remove,
                               ordered_get);
        virtual std::tuple<persistent::version_t,uint64_t> put(const VT& value) override;
        virtual std::tuple<persistent::version_t,uint64_t> remove(const KT& key) override;
        virtual const VT get(const KT& key, const persistent::version_t& ver) override;
        virtual const VT get_by_time(const KT& key, const uint64_t& ts_us) override;
        virtual std::tuple<persistent::version_t,uint64_t> ordered_put(const VT& value) override;
        virtual std::tuple<persistent::version_t,uint64_t> ordered_remove(const KT& key) override;
        virtual const VT ordered_get(const KT& key) override;


        // serialization support
        DEFAULT_SERIALIZE(kv_map);

        static std::unique_ptr<VolatileCascadeStore> from_bytes(mutils::DeserializationManager* dsm, char const* buf);

        DEFAULT_DESERIALIZE_NOALLOC(VolatileCascadeStore);

        void ensure_registered(mutils::DeserializationManager&) {}

        /* constructors */
        VolatileCascadeStore(CascadeWatcher<KT,VT,IK,IV>* cw=nullptr);
        VolatileCascadeStore(const std::map<KT,VT>& _kvm, CascadeWatcher<KT,VT,IK,IV>* cw=nullptr); // copy kv_map
        VolatileCascadeStore(std::map<KT,VT>&& _kvm, CascadeWatcher<KT,VT,IK,IV>* cw=nullptr); // move kv_map
    };

    /**
     * Persistent Cascade Store Delta Support
     */
    template <typename KT, typename VT, KT* IK, VT* IV>
    class DeltaCascadeStoreCore : public mutils::ByteRepresentable,
                                  public persistent::IDeltaSupport<DeltaCascadeStoreCore<KT,VT,IK,IV>> {
        // TODO: use max payload size in subgroup configuration.
#define DEFAULT_DELTA_BUFFER_CAPACITY (4096)
        enum _OPID {
            PUT,
            REMOVE
        };
        // delta
        typedef struct {
            size_t capacity;
            size_t len;
            char* buffer;
            // methods
            inline void set_opid(_OPID opid); 
            inline void set_data_len(const size_t& dlen);
            inline char* data_ptr();
            inline void calibrate(const size_t& dlen);
            inline bool is_empty();
            inline void clean();
            inline void destroy();
        } _Delta;
        _Delta delta;
        
        /**
         * Initialize the delta data structure.
         */
        void initialize_delta();

    public:
        std::map<KT,VT> kv_map;

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
        virtual void applyDelta(char const* const delta) override;
        static std::unique_ptr<DeltaCascadeStoreCore<KT,VT,IK,IV>> create(mutils::DeserializationManager* dm);
        /**
         * Do ordered put without generating a delta.
         */
        inline void apply_ordered_put(const VT& value);
        /**
         * Do ordered remove without generating a delta.
         */
        inline bool apply_ordered_remove(const KT& key);
        /**
         * Ordered put, and generate a delta.
         */
        virtual bool ordered_put(const VT& value);
        /**
         * Ordered remove, and generate a delta.
         */
        virtual bool ordered_remove(const KT& key);
        /**
         * ordered get, no need to generate a delta.
         */
        virtual const VT ordered_get(const KT& key);

        // serialization supports
        DEFAULT_SERIALIZATION_SUPPORT(DeltaCascadeStoreCore, kv_map);

        // constructors
        DeltaCascadeStoreCore();
        DeltaCascadeStoreCore(const std::map<KT,VT>& _kv_map);
        DeltaCascadeStoreCore(std::map<KT,VT>&& _kv_map);

        // destructor
        virtual ~DeltaCascadeStoreCore();
    };

    /**
     * template for persistent cascade stores.
     * 
     * PersistentCascadeStore is full-fledged implementation with log mechansim. Data can be stored in different
     * persistent devices including file system(persistent::ST_FILE) or SPDK(persistent::ST_SPDK). Please note that the
     * data is cached in memory too.
     */
    template <typename KT, typename VT, KT* IK, VT* IV, persistent::StorageType ST=persistent::ST_FILE>
    class PersistentCascadeStore : public ICascadeStore<KT, VT, IK, IV>,
                                   public mutils::ByteRepresentable,
                                   public derecho::PersistsFields,
                                   public derecho::GroupReference {
    public:
        using derecho::GroupReference::group;
        persistent::Persistent<DeltaCascadeStoreCore<KT,VT,IK,IV>,ST> persistent_core;
        std::shared_ptr<CascadeWatcher<KT,VT,IK,IV>> cascade_watcher_ptr;
        
        REGISTER_RPC_FUNCTIONS(PersistentCascadeStore,
                               put,
                               remove,
                               get,
                               get_by_time,
                               ordered_put,
                               ordered_remove,
                               ordered_get);
        virtual std::tuple<persistent::version_t,uint64_t> put(const VT& value) override;
        virtual std::tuple<persistent::version_t,uint64_t> remove(const KT& key) override;
        virtual const VT get(const KT& key, const persistent::version_t& ver) override;
        virtual const VT get_by_time(const KT& key, const uint64_t& ts_us) override;
        virtual std::tuple<persistent::version_t,uint64_t> ordered_put(const VT& value) override;
        virtual std::tuple<persistent::version_t,uint64_t> ordered_remove(const KT& key) override;
        virtual const VT ordered_get(const KT& key) override;

        // serialization support
        DEFAULT_SERIALIZE(persistent_core);

        static std::unique_ptr<PersistentCascadeStore> from_bytes(mutils::DeserializationManager* dsm, char const* buf);

        DEFAULT_DESERIALIZE_NOALLOC(PersistentCascadeStore);

        void ensure_registered(mutils::DeserializationManager&) {}

        // constructors
        PersistentCascadeStore(persistent::PersistentRegistry *pr, CascadeWatcher<KT,VT,IK,IV>* cw=nullptr);
        PersistentCascadeStore(persistent::Persistent<DeltaCascadeStoreCore<KT,VT,IK,IV>,ST>&& _persistent_core,
                               CascadeWatcher<KT,VT,IK,IV>* cw=nullptr); // move persistent_core

        // destructor
        virtual ~PersistentCascadeStore();
    };
} // namespace cascade
} // namespace derecho
#include "detail/cascade_impl.hpp"
