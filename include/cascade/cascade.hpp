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

#include <cascade/config.h>

namespace derecho {
namespace cascade {

#define CURRENT_VERSION     (persistent::INVALID_VERSION)
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
        using ObjectType = VT;
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
         * @param ver - Version, if version == CURRENT_VERSION, get the latest value.
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
        /**
         * list keys by version
         * @param ver - Version, if version  == CURRENT_VERSION, get the latest list of keys.
         * @return a list of keys.
         */
        virtual std::vector<KT> list_keys(const persistent::version_t& ver) = 0;
        /**
         * list keys by timestamp
         * @param ts_us - timestamp in microsecond
         * @return a list of keys.
         */
        virtual std::vector<KT> list_keys_by_time(const uint64_t& ts_us) = 0;
        /**
         * get size by version
         * @param key
         * @param ver - Version, if version == CURRENT_VERSION, get the latest value.
         * @return the size of serialized value.
         */
        virtual uint64_t get_size(const KT& key, const persistent::version_t& ver) = 0;
        /**
         * get value by timestamp
         * @param key
         * @param ts_us - timestamp in microsecond
         * @return the size of serialized value.
         */
        virtual uint64_t get_size_by_time(const KT& key, const uint64_t& ts_us) = 0;

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
        /**
         * ordered_list_keys
         * @return a list of keys.
         */
        virtual std::vector<KT> ordered_list_keys() = 0;
        /**
         * ordered_get_size
         */
        virtual uint64_t ordered_get_size(const KT& key) = 0;
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
                               list_keys,
                               list_keys_by_time,
                               get_size,
                               get_size_by_time,
                               ordered_put,
                               ordered_remove,
                               ordered_get,
                               ordered_list_keys,
                               ordered_get_size);
        virtual std::tuple<persistent::version_t,uint64_t> put(const VT& value) override;
        virtual std::tuple<persistent::version_t,uint64_t> remove(const KT& key) override;
        virtual const VT get(const KT& key, const persistent::version_t& ver) override;
        virtual const VT get_by_time(const KT& key, const uint64_t& ts_us) override;
        virtual std::vector<KT> list_keys(const persistent::version_t& ver) override;
        virtual std::vector<KT> list_keys_by_time(const uint64_t& ts_us) override;
        virtual uint64_t get_size(const KT& key, const persistent::version_t& ver) override;
        virtual uint64_t get_size_by_time(const KT& key, const uint64_t& ts_us) override;
        virtual std::tuple<persistent::version_t,uint64_t> ordered_put(const VT& value) override;
        virtual std::tuple<persistent::version_t,uint64_t> ordered_remove(const KT& key) override;
        virtual const VT ordered_get(const KT& key) override;
        virtual std::vector<KT> ordered_list_keys() override;
        virtual uint64_t ordered_get_size(const KT& key) override;

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
        /**
         * ordered list_keys, no need to generate a delta.
         */
        virtual std::vector<KT> ordered_list_keys();
        /**
         * ordered get_size, not need to generate a delta.
         */
        virtual uint64_t ordered_get_size(const KT& key);

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
                               list_keys,
                               list_keys_by_time,
                               get_size,
                               get_size_by_time,
                               ordered_put,
                               ordered_remove,
                               ordered_get,
                               ordered_list_keys,
                               ordered_get_size);
        virtual std::tuple<persistent::version_t,uint64_t> put(const VT& value) override;
        virtual std::tuple<persistent::version_t,uint64_t> remove(const KT& key) override;
        virtual const VT get(const KT& key, const persistent::version_t& ver) override;
        virtual const VT get_by_time(const KT& key, const uint64_t& ts_us) override;
        virtual std::vector<KT> list_keys(const persistent::version_t& ver) override;
        virtual std::vector<KT> list_keys_by_time(const uint64_t& ts_us) override;
        virtual uint64_t get_size(const KT& key, const persistent::version_t& ver) override;
        virtual uint64_t get_size_by_time(const KT& key, const uint64_t& ts_us) override;
        virtual std::tuple<persistent::version_t,uint64_t> ordered_put(const VT& value) override;
        virtual std::tuple<persistent::version_t,uint64_t> ordered_remove(const KT& key) override;
        virtual const VT ordered_get(const KT& key) override;
        virtual std::vector<KT> ordered_list_keys() override;
        virtual uint64_t ordered_get_size(const KT& key) override;

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

    /**
     * Interfaces for ValueTypes, derive them to enable corresponding features.
     */

    /**
     * TODO:
     * If the VT template type of PersistentCascadeStore/VolatileCascadeStore implements IKeepVersion interface, its
     * 'set_version' method will be called on 'ordered_put' or 'ordered_remove' with the current version assigned to
     * this operation. The VT implementer may save this version in its states.
     */
    class IKeepVersion {
        /**
         * set_version() is a callback on PersistentCascadeStore/VolatileCascadeStore updates.
         * @param ver   The current version
         */
        virtual void set_version(persistent::version_t ver) const = 0;
        /**
         * get_version returns the version
         * @return the VT's version
         */
        virtual persistent::version_t get_version() const = 0;
    };

    /**
     * TODO:
     * If the VT template type of PersistentCascadeStore/VolatileCascadeStore implements IKeepTimestamp interface, its
     * 'set_timestamp' method will be called on updates with the timestamp in microseconds assigned to this operation.
     * The VT implementer may save this timestamp in its states.
     */
    class IKeepTimestamp {
        /**
         * set_timestamp() is a callback on PersistentCascadeStore/VolatileCascadeStore updates.
         * @param ts_us The timestamp in microseconds
         */
        virtual void set_timestamp(uint64_t ts_us) const = 0;
        /**
         * get_timestamp() returns VT's timestamp
         */
        virtual uint64_t get_timestamp() const = 0;
    };

    /**
     * If the VT template type of PersistentCascadeStore implements IKeepPreviousVersion interface, its
     * 'set_previous_version' method will be called on 'ordered_put' with the previous version in the shard as well as
     * the previous version of the same key. If this is the first value of that key, 'set_previous_version' will be
     * called with "INVALID_VERSION", meaning a genesis value. Therefore, the VT implementer must save the version in
     * its object and knows how to get them when they get a value from cascade.
     */
    class IKeepPreviousVersion {
    public:
        /**
         * set_previous_version() is a callback on PersistentCascadeStore::ordered_put();
         * @param prev_ver          The previous version
         * @param prev_ver_by_key   The previous version of the same key in VT object
         */
        virtual void set_previous_version(persistent::version_t prev_ver, persistent::version_t perv_ver_by_key) const = 0;
    };

    /**
     * TODO:
     * If the VT template of PersistentCascadeStore/VolatileCascadeStore implements IVerifyPreviousVersion interface,
     * its 'verify_previous_version' will be called on 'ordered_put' with the previous version in the shard as well as
     * the previous version of the same key. If this is the first value of that key, 'verify_previous_version' will be
     * called with "INVALID_VERSION", meaning a genesis value. The VT implementer must make sure if that satisfy
     * application semantics.
     *
     * For example, a VT object may compare if the given 'prev_ver' and 'prev_ver_key_key' matche the previous versions
     * it saw (those versions might be VT members). If an application rejects writes from a client without knowing the
     * latest state of corresponding key, it can return false, meaning verify failed, if 'prev_ver_by_key' is greater
     * than the previous state cached in VT.
     *
     * Question: how to support this for VolatileCascadeStore? cache it???
     */
    class IVerifyPreviousVersion {
    public:
        /**
         * verify_previous_version() is a callback on PresistentCascadeStore/VolatileCascadeStore::ordered_put();
         * @param prev_ver          The previous version
         * @param prev_ver_by_key   The previous version of the same key in VT object
         */
        virtual bool verify_previous_version(persistent::version_t prev_ver, persistent::version_t prev_ver_by_key) const = 0;
    };

} // namespace cascade
} // namespace derecho
#include "detail/cascade_impl.hpp"
