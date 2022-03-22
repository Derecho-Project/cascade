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

    /**
     * The off-critical data path handler API
     */
    class ICascadeContext: public derecho::DeserializationContext {};

#define CURRENT_VERSION     (persistent::INVALID_VERSION)
    /**
     * CriticalDataPathObserver
     *
     * @tparam CascadeType - the CriticalDataPathObserver for corresponding cascade type.
     *
     * A CriticalDataPathObserver object takes four arguments
     * @param uint32_t subgroup_idx     - subgroup index
     * @param const uint32_t shard_num  - shard num
     * @param const KT& key             - key
     * @param const VT& value           - value
     * @param ICascadeContext* cascade_context - the cascade context
     * @return void
     */
    template<typename CascadeType>
    class CriticalDataPathObserver: public derecho::DeserializationContext {
    public:
        /**
         * The critical data path behaviour is defined here. The default behaviour is do nothing.
         *
         * @param subgroup_idx
         * @param shard_idx
         * @param sender_id
         * @param key
         * @param value
         * @param cascade_ctxt - The cascade context to be used later
         * @param is_trigger true for critical data path of p2p_send; otherwise, the critical data path of ordered_send.
         */
        virtual void operator () (const uint32_t subgroup_idx,
                                  const uint32_t shard_idx,
                                  const node_id_t sender_id,
                                  const typename CascadeType::KeyType& key,
                                  const typename CascadeType::ObjectType& value,
                                  ICascadeContext* cascade_ctxt,
                                  bool is_trigger = false) {}
    };

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
         * put(const VT&)
         *
         * Put a value. VT must implement ICascadeObject interface. The key is given in value and retrieved by
         * ICascadeObject::get_key_ref()
         *
         * @param value
         *
         * @return a tuple including version number (version_t) and a timestamp in microseconds.
         */
        virtual std::tuple<persistent::version_t,uint64_t> put(const VT& value) const = 0;
        /**
         * put_and_forget(const VT&)
         *
         * Put a value. VT must implement ICascadeObject interface. The key is given in value and retrieved by
         * ICascadeObject::get_key_ref()
         *
         * @param value
         */
        virtual void put_and_forget(const VT& value) const = 0;
#ifdef ENABLE_EVALUATION
        /**
         * perf_put is used to evaluate the performance of an internal shard
         *
         * @param max_payload_size the maximum size of the payload.
         * @param duration_sec duration of the test
         *
         * @return ops
         */
        virtual double perf_put(const uint32_t max_payload_size,const uint64_t duration_sec) const = 0;
#endif
        /**
         * remove(const KT&)
         *
         * Remove a value by key. The key will still be in the map with an empty value.
         *
         * @param key
         *
         * @return a tuple including version number (version_t) and a timestamp in microseconds.
         */
        virtual std::tuple<persistent::version_t,uint64_t> remove(const KT& key) const = 0;

        /**
         * get(const KT&,const persistent::version_t&)
         *
         * Get a value by key and version.
         *
         * @param key
         * @param ver   Version: if version == CURRENT_VERSION, get the latest value.
         * @param stable  
         *              if stable == false, we only return the data reflecting the latest locally delivered atomic
         *              broadcast. Otherwise, stable data will be returned, meaning that the persisted states returned
         *              is safe: they will survive after whole system recovery.
         * @param exact The exact match flag: this function try to return the value of that key at the 'ver'. If such a
         *              value does not exists and exact is true, it will throw an exception. If such a value does not
         *              exists and exact is false, it will return the latest state of the value for 'key' before 'ver'.
         *              The former case is very efficient but the latter one is not because of reconstructing the state.
         *              Please note that the current Persistent<T> in derecho will reconstruct the state at 'ver' from
         *              the beginning of the log entry if 'ver' != CURRENT_VERSION, which is extremely inefficient.
         *              TODO: use checkpoint cache to accelerate that process.
         *
         * @return a value
         *
         * @throws std::runtime_error, if requested value is not found.
         */
        virtual const VT get(const KT& key, const persistent::version_t& ver, const bool stable, bool exact=false) const = 0;

        /**
         * multi_get(const KT&)
         *
         * Get a value by key. This is an ordered get that participates atomic broadcast, which reflecting the latest
         * global atomic broadcast.
         *
         * @param key
         *
         * @return a value
         */
        virtual const VT multi_get(const KT& key) const = 0;

        /**
         * get_by_time(const KT&, const uint64_t& ts_us)
         * 
         * Get a value by key and timestamp.
         *
         * Please note that the current Persistent<T> in derecho will reconstruct the state at 'ts_us' from the
         * beginning of the log entry, which is extremely inefficient. TODO: use checkpoint cache to accelerate that
         * process.
         *
         * @param key
         * @param ts_us - timestamp in microsecond
         * @param stable
         *              if stable == false, we only return the data reflecting the latest locally delivered atomic
         *              broadcast. Otherwise, stable data will be returned, meaning that the persisted states returned
         *              is safe: they will survive after whole system recovery.
         *
         * @return a value
         */
        virtual const VT get_by_time(const KT& key, const uint64_t& ts_us, const bool stable) const = 0;

        /**
         * multi_list_keys(const std::string& prefix)
         *
         * List the most current keys by an atomic broadcast
         *
         * @param prefix    Prefix, only the key matching this prefix will be returned. TODO: KT/VT provider should
         *                  provide their own prefix matching implementation,
         *                  Empty prefix matches all keys.
         *
         * @return a list of keys.
         */
        virtual std::vector<KT> multi_list_keys(const std::string& prefix) const = 0;

        /**
         * list_keys(const std::string& prefix, const persistent::version_t& ver, const bool stable)
         *
         * List keys at version.
         *
         * @param prefix    Prefix, only the key matching this prefix will be returned. TODO: KT/VT provider should
         *                  provide their own prefix matching implementation,
         *                  Empty prefix matches all keys.
         * @param ver       Version, if version  == CURRENT_VERSION, get the latest list of keys.
         *                  Please note that the current Persistent<T> in derecho will reconstruct the state at 'ver' from
         *                  the beginning of the log entry if 'ver' != CURRENT_VERSION, which is extremely inefficient.
         *                  TODO: use checkpoint cache to accelerate that process.
         * @param stable    
         *                  if stable == false, we only return the data reflecting the latest locally delivered atomic
         *                  broadcast. Otherwise, stable data will be returned, meaning that the persisted states returned
         *                  is safe: they will survive after whole system recovery.
         *
         * @return a list of keys.
         */
        virtual std::vector<KT> list_keys(const std::string& prefix, const persistent::version_t& ver, const bool stable) const = 0;

        /**
         * list_keys_by_time(const std::string&, const uint64_t&, const bool)
         *
         * List keys by timestamp
         *
         * Please note that the current Persistent<T> in derecho will reconstruct the state at 'ts_us' from the
         * beginning of the log entry, which is extremely inefficient. TODO: use checkpoint cache to accelerate that
         * process.
         *
         * @param prefix    Prefix, only the key matching this prefix will be returned. TODO: KT/VT provider should
         *                  provide their own prefix matching implementation,
         *                  Empty prefix matches all keys.
         * @param ts_us     timestamp in microsecond
         * @param stable    
         *                  if stable == false, we only return the data reflecting the latest locally delivered atomic
         *                  broadcast. Otherwise, stable data will be returned, meaning that the persisted states returned
         *                  is safe: they will survive after whole system recovery.
         *
         * @return a list of keys.
         */
        virtual std::vector<KT> list_keys_by_time(const std::string& prefix, const uint64_t& ts_us, const bool stable) const = 0;

        
        /**
         * multi_get_size(const KT&)
         *
         * Get size of the latest object using atomic broadcast.
         *
         * @param key   The key
         *
         * @return the size of serialized value.
         */
        virtual uint64_t multi_get_size(const KT& key) const = 0;

        /**
         * get_size(const KT&,const persistent::version_t&,bool)
         *
         * Get size by version
         *
         * @param key   The key
         * @param ver   Version, if version == CURRENT_VERSION, get the latest value.
         * @param stable
         * @param exact The exact match flag: this function try to return the value of that key at the 'ver'. If such a
         *              value does not exists and exact is true, it will throw an exception. If such a value does not
         *              exists and exact is false, it will return the latest state of the value for 'key' before 'ver'.
         *              The former case is very efficient but the latter one is not because of reconstructing the state.
         *              Please note that the current Persistent<T> in derecho will reconstruct the state at 'ver' from
         *              the beginning of the log entry if 'ver' != CURRENT_VERSION, which is extremely inefficient.
         *              TODO: use checkpoint cache to accelerate that process.
         *
         * @return the size of serialized value.
         */
        virtual uint64_t get_size(const KT& key, const persistent::version_t& ver, const bool stable, const bool exact=false) const = 0;

        /**
         * get_size_by_time(const KT&,const uint64_t&)
         *
         * Get size by timestamp
         *
         * Please note that the current Persistent<T> in derecho will reconstruct the state at 'ts_us' from the
         * beginning of the log entry, which is extremely inefficient. TODO: use checkpoint cache to accelerate that
         * process.
         *
         * @param key
         * @param ts_us - timestamp in microsecond
         *
         * @return the size of serialized value.
         */
        virtual uint64_t get_size_by_time(const KT& key, const uint64_t& ts_us, const bool stable) const = 0;

        /**
         * trigger_put(const VT& value)
         *
         * Put object as a trigger. This call will not cause a store but only trigger an off-critical data path
         * computation. Please note that this call should be handled in p2p processing thread.
         * 
         * @param value - the object to trig
         */
        virtual void trigger_put(const VT& value) const = 0;

#ifdef ENABLE_EVALUATION
        /**
         * dump_timestamp_log(const std::string& filename)
         *
         * Dump the timestamp log to a local file specified by "filename"
         *
         * @param filename - the name of the timestamp log.
         */
        virtual void dump_timestamp_log(const std::string& filename) const = 0;
#ifdef DUMP_TIMESTAMP_WORKAROUND
        /**
         * dump_timestamp_log_workaround(const std::string& filename)
         *
         * Dump the timestamp log to a local file specified by "filename"
         *
         * @param filename - the name of the timestamp log.
         */
        virtual void dump_timestamp_log_workaround(const std::string& filename) const = 0;
#endif

#endif//ENABLE_EVALUATION

    protected:
        /**
         * ordered_put
         * @param value
         * @return a tuple including version number (version_t) and a timestamp in microseconds.
         */
        virtual std::tuple<persistent::version_t,uint64_t> ordered_put(const VT& value) = 0;
        /**
         * ordered_put_and_forget
         * @param value
         */
        virtual void ordered_put_and_forget(const VT& value) = 0;
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
        virtual std::vector<KT> ordered_list_keys(const std::string& prefix) = 0;
        /**
         * ordered_get_size
         */
        virtual uint64_t ordered_get_size(const KT& key) = 0;
#ifdef ENABLE_EVALUATION
        /**
         * ordered_dump_timestamp_log(const std::string& filename)
         *
         * Dump the timestamp log to a local file specified by "filename"
         *
         * @param filename - the name of the timestamp log.
         */
        virtual void ordered_dump_timestamp_log(const std::string& filename) = 0;
#endif//ENABLE_EVALUATION
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
                                 public derecho::GroupReference,
                                 public derecho::NotificationSupport {
    private:
        bool internal_ordered_put(const VT& value);
#if defined(__i386__) || defined(__x86_64__) || defined(_M_AMD64) || defined(_M_IX86)
        mutable std::atomic<persistent::version_t> lockless_v1;
        mutable std::atomic<persistent::version_t> lockless_v2;
#else
#error The lockless reader/writer works only with TSO memory reordering. Please check https://en.wikipedia.org/wiki/Memory_ordering
#endif
    public:
        /* group reference */
        using derecho::GroupReference::group;
        /* volatile cascade store in memory */
        std::map<KT,VT> kv_map;
        /* record the version of latest update */
        persistent::version_t update_version;
        /* watcher */
        CriticalDataPathObserver<VolatileCascadeStore<KT,VT,IK,IV>>* cascade_watcher_ptr;
        /* cascade context */
        ICascadeContext* cascade_context_ptr;
        
        REGISTER_RPC_FUNCTIONS_WITH_NOTIFICATION(VolatileCascadeStore,
                               P2P_TARGETS(
                                   put,
                                   put_and_forget,
#ifdef ENABLE_EVALUATION
                                   perf_put,
#endif
                                   remove,
                                   get,
                                   multi_get,
                                   get_by_time,
                                   multi_list_keys,
                                   list_keys,
                                   list_keys_by_time,
                                   multi_get_size,
                                   get_size,
                                   get_size_by_time,
                                   trigger_put
#ifdef ENABLE_EVALUATION
                                   ,dump_timestamp_log
#ifdef DUMP_TIMESTAMP_WORKAROUND
                                   ,dump_timestamp_log_workaround
#endif
#endif//ENABLE_EVALUATION
                               ),
                               ORDERED_TARGETS(
                                   ordered_put,
                                   ordered_put_and_forget,
                                   ordered_remove,
                                   ordered_get,
                                   ordered_list_keys,
                                   ordered_get_size
#ifdef ENABLE_EVALUATION
                                   ,ordered_dump_timestamp_log
#endif//ENABLE_EVALUATION
                               ));
#ifdef ENABLE_EVALUATION
        virtual void dump_timestamp_log(const std::string& filename) const override;
#ifdef DUMP_TIMESTAMP_WORKAROUND
        virtual void dump_timestamp_log_workaround(const std::string& filename) const override;
#endif
#endif//ENABLE_EVALUATION
        virtual void trigger_put(const VT& value) const override;
        virtual std::tuple<persistent::version_t,uint64_t> put(const VT& value) const override;
#ifdef ENABLE_EVALUATION
        virtual double perf_put(const uint32_t max_payload_size,const uint64_t duration_sec) const override;
#endif//ENABLE_EVALUATION
        virtual void put_and_forget(const VT& value) const override;
        virtual std::tuple<persistent::version_t,uint64_t> remove(const KT& key) const override;
        virtual const VT get(const KT& key, const persistent::version_t& ver, const bool stable, bool exact=false) const override;
        virtual const VT multi_get(const KT& key) const override;
        virtual const VT get_by_time(const KT& key, const uint64_t& ts_us, const bool stable) const override;
        virtual std::vector<KT> multi_list_keys(const std::string& prefix) const override;
        virtual std::vector<KT> list_keys(const std::string& prefix, const persistent::version_t& ver, const bool stable) const override;
        virtual std::vector<KT> list_keys_by_time(const std::string& prefix, const uint64_t& ts_us, const bool stable) const override;
        virtual uint64_t multi_get_size(const KT& key) const override;
        virtual uint64_t get_size(const KT& key, const persistent::version_t& ver, const bool stable, bool exact=false) const override;
        virtual uint64_t get_size_by_time(const KT& key, const uint64_t& ts_us, const bool stable) const override;
        virtual std::tuple<persistent::version_t,uint64_t> ordered_put(const VT& value) override;
        virtual void ordered_put_and_forget(const VT& value) override;
        virtual std::tuple<persistent::version_t,uint64_t> ordered_remove(const KT& key) override;
        virtual const VT ordered_get(const KT& key) override;
        virtual std::vector<KT> ordered_list_keys(const std::string& prefix) override;
        virtual uint64_t ordered_get_size(const KT& key) override;
#ifdef ENABLE_EVALUATION
        virtual void ordered_dump_timestamp_log(const std::string& filename) override;
#endif//ENABLE_EVALUATION

        // serialization support
        DEFAULT_SERIALIZE(kv_map,update_version);

        static std::unique_ptr<VolatileCascadeStore> from_bytes(mutils::DeserializationManager* dsm, uint8_t const* buf);

        DEFAULT_DESERIALIZE_NOALLOC(VolatileCascadeStore);

        void ensure_registered(mutils::DeserializationManager&) {}

        /* constructors */
        VolatileCascadeStore(CriticalDataPathObserver<VolatileCascadeStore<KT,VT,IK,IV>>* cw=nullptr,
                             ICascadeContext* cc=nullptr);
        VolatileCascadeStore(const std::map<KT,VT>& _kvm,
                             persistent::version_t _uv,
                             CriticalDataPathObserver<VolatileCascadeStore<KT,VT,IK,IV>>* cw=nullptr,
                             ICascadeContext* cc=nullptr); // copy kv_map
        VolatileCascadeStore(std::map<KT,VT>&& _kvm,
                             persistent::version_t _uv,
                             CriticalDataPathObserver<VolatileCascadeStore<KT,VT,IK,IV>>* cw=nullptr,
                             ICascadeContext* cc=nullptr); // move kv_map
    };

    /**
     * Persistent Cascade Store Delta Support
     */
    template <typename KT, typename VT, KT* IK, VT* IV>
    class DeltaCascadeStoreCore : public mutils::ByteRepresentable,
                                  public persistent::IDeltaSupport<DeltaCascadeStoreCore<KT,VT,IK,IV>> {
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
            uint32_t    op;
            uint8_t     first_data_byte;
        };
        
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
        virtual void applyDelta(uint8_t const* const delta) override;
        static std::unique_ptr<DeltaCascadeStoreCore<KT,VT,IK,IV>> create(mutils::DeserializationManager* dm);
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
                                   public derecho::GroupReference,
                                   public derecho::NotificationSupport {
    private:
        bool internal_ordered_put(const VT& value);
    public:
        using derecho::GroupReference::group;
        persistent::Persistent<DeltaCascadeStoreCore<KT,VT,IK,IV>,ST> persistent_core;
        CriticalDataPathObserver<PersistentCascadeStore<KT,VT,IK,IV>>* cascade_watcher_ptr;
        /* cascade context */
        ICascadeContext* cascade_context_ptr;
        
        REGISTER_RPC_FUNCTIONS_WITH_NOTIFICATION(PersistentCascadeStore,
                               P2P_TARGETS(
                                   put,
                                   put_and_forget,
#ifdef ENABLE_EVALUATION
                                   perf_put,
#endif//ENABLE_EVALUATION
                                   remove,
                                   get,
                                   multi_get,
                                   get_by_time,
                                   multi_list_keys,
                                   list_keys,
                                   list_keys_by_time,
                                   multi_get_size,
                                   get_size,
                                   get_size_by_time,
                                   trigger_put
#ifdef ENABLE_EVALUATION
                                   ,dump_timestamp_log
#ifdef DUMP_TIMESTAMP_WORKAROUND
                                   ,dump_timestamp_log_workaround
#endif
#endif//ENABLE_EVALUATION
                               ),
                               ORDERED_TARGETS(
                                   ordered_put,
                                   ordered_put_and_forget,
                                   ordered_remove,
                                   ordered_get,
                                   ordered_list_keys,
                                   ordered_get_size
#ifdef ENABLE_EVALUATION
                                   ,ordered_dump_timestamp_log
#endif//ENABLE_EVALUATION
                                   ));
#ifdef ENABLE_EVALUATION
        virtual void dump_timestamp_log(const std::string& filename) const override;
#ifdef DUMP_TIMESTAMP_WORKAROUND
        virtual void dump_timestamp_log_workaround(const std::string& filename) const override;
#endif
#endif//ENABLE_EVALUATION
        virtual void trigger_put(const VT& value) const override;
        virtual std::tuple<persistent::version_t,uint64_t> put(const VT& value) const override;
        virtual void put_and_forget(const VT& value) const override;
#ifdef ENABLE_EVALUATION
        virtual double perf_put(const uint32_t max_payload_size,const uint64_t duration_sec) const override;
#endif//ENABLE_EVALUATION
        virtual std::tuple<persistent::version_t,uint64_t> remove(const KT& key) const override;
        virtual const VT get(const KT& key, const persistent::version_t& ver, const bool stable, bool exact=false) const override;
        virtual const VT multi_get(const KT& key) const override;
        virtual const VT get_by_time(const KT& key, const uint64_t& ts_us, const bool stable) const override;
        virtual std::vector<KT> multi_list_keys(const std::string& prefix) const override;
        virtual std::vector<KT> list_keys(const std::string& prefix, const persistent::version_t& ver, const bool stable) const override;
        virtual std::vector<KT> list_keys_by_time(const std::string& prefix, const uint64_t& ts_us, const bool stable) const override;
        virtual uint64_t multi_get_size(const KT& key) const override;
        virtual uint64_t get_size(const KT& key, const persistent::version_t& ver, const bool stable, bool exact=false) const override;
        virtual uint64_t get_size_by_time(const KT& key, const uint64_t& ts_us, const bool stable) const override;
        virtual std::tuple<persistent::version_t,uint64_t> ordered_put(const VT& value) override;
        virtual void ordered_put_and_forget(const VT& value) override;
        virtual std::tuple<persistent::version_t,uint64_t> ordered_remove(const KT& key) override;
        virtual const VT ordered_get(const KT& key) override;
        virtual std::vector<KT> ordered_list_keys(const std::string& prefix) override;
        virtual uint64_t ordered_get_size(const KT& key) override;
#ifdef ENABLE_EVALUATION
        virtual void ordered_dump_timestamp_log(const std::string& filename) override;
#endif//ENABLE_EVALUATION

        // serialization support
        DEFAULT_SERIALIZE(persistent_core);

        static std::unique_ptr<PersistentCascadeStore> from_bytes(mutils::DeserializationManager* dsm, uint8_t const* buf);

        DEFAULT_DESERIALIZE_NOALLOC(PersistentCascadeStore);

        void ensure_registered(mutils::DeserializationManager&) {}

        // constructors
        PersistentCascadeStore(persistent::PersistentRegistry *pr,
                               CriticalDataPathObserver<PersistentCascadeStore<KT,VT,IK,IV>>* cw=nullptr,
                               ICascadeContext* cc=nullptr);
        PersistentCascadeStore(persistent::Persistent<DeltaCascadeStoreCore<KT,VT,IK,IV>,ST>&& _persistent_core,
                               CriticalDataPathObserver<PersistentCascadeStore<KT,VT,IK,IV>>* cw=nullptr,
                               ICascadeContext* cc=nullptr); // move persistent_core
        PersistentCascadeStore();

        // destructor
        virtual ~PersistentCascadeStore();
    };

    /**
     * Interfaces for ValueTypes, derive them to enable corresponding features.
     */

    /**
     * create_null_object(const KT&)
     *
     * Users need to implement this function to create a null object of a given key.
     *
     * return a null VT object.
     */
    template <typename KT, typename VT, KT* IK, VT* IV>
    VT create_null_object_cb(const KT& key = *IK);

    /**
     * The VT template type of PersistentCascadeStore/VolatileCascadeStore must implement ICascadeObject interface.
     * We use both the concepts of null and valid object in Cascade. A null object precisely means 'no data'; while a
     * valid object literarily means an object is 'valid'. Technically, a null object has a valid key while invalid
     * object does not.
     */
    template <typename KT, typename VT>
    class ICascadeObject {
    public:
        /**
         * get_key_ref()
         *
         * Get a const reference to the key.
         *
         * @return a const reference to the key
         */
        virtual const KT& get_key_ref() const = 0;
        /**
         * is_null()
         *
         * Test if this Object is null or not.
         *
         * @return true for null object
         */
        virtual bool is_null() const = 0;
        /**
         * is_valid()
         *
         * Test if this Object is valid or not.
         *
         * @ return true for invalid object
         */
        virtual bool is_valid() const = 0;
        /**
         * copy_from()
         *
         * copy object from another object. This is very similar to a copy = operator. We disabled = operator to avoid
         * misuse. And we introduce copy_from operation in case such a copy is required.
         *
         * @param rhs   the other object
         */
        virtual void copy_from(const VT& rhs) = 0;
    };

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
    class IKeepPreviousVersion : public IKeepVersion {
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
     */
    class IVerifyPreviousVersion : public IKeepPreviousVersion {
    public:
        /**
         * verify_previous_version() is a callback on PresistentCascadeStore/VolatileCascadeStore::ordered_put();
         * @param prev_ver          The previous version
         * @param prev_ver_by_key   The previous version of the same key in VT object
         * @return If 'prev_ver' and 'prev_ver_by_key' are acceptable, it returns True, otherwise, false.
         */
        virtual bool verify_previous_version(persistent::version_t prev_ver, persistent::version_t prev_ver_by_key) const = 0;
    };

    /**
     * If the VT type for PersistentCascadeStore/VolatileCascadeStore implements IValidator inferface, its 'validate'
     * method will be called on 'ordered_put' with the current k/v map to verify if the object can be added to the
     * existing k/v map pool.
     *
     * For example, a VT object can override the default 'overwriting' behaviour by refusing an object whose key has
     * already existed in the kv_map.
     */
    template <typename KT, typename VT>
    class IValidator {
    public:
        virtual bool validate(const std::map<KT,VT>& kv_map) const = 0;
    };

#ifdef ENABLE_EVALUATION
    /**
     * TODO:
     * If the VT template of PersistentCascadeStore implements IKeepPreviousVersion interface, its 'set_message_id'
     * method is used to set an id dedicated for evaluation purpose, which is different from the key. The
     * 'get_message_id' methdo is used to retrieve its id.
     */
    class IHasMessageID {
    public:
        /**
         * set_message_id
         * @param message id
         */
        virtual void set_message_id(uint64_t id) const = 0;
        /**
         * get_message_id
         * @return message id
         */
        virtual uint64_t get_message_id() const = 0;
    };
#endif

    /**
     * Template for cascade trigger store
     *
     * @tparam KT   key type
     * @tparam VT   value type
     * @tparam IK   a pointer to invalid key
     * @tparam IV   a pointer to invalid value
     */
    template <typename KT, typename VT, KT* IK, VT* IV>
    class TriggerCascadeNoStore : public ICascadeStore<KT,VT,IK,IV>,
                                  public mutils::ByteRepresentable,
                                  public derecho::GroupReference,
                                  public derecho::NotificationSupport {
    public:
        using derecho::GroupReference::group;
        CriticalDataPathObserver<TriggerCascadeNoStore<KT,VT,IK,IV>>* cascade_watcher_ptr;
        /* cascade context */
        ICascadeContext* cascade_context_ptr;
        
        REGISTER_RPC_FUNCTIONS_WITH_NOTIFICATION(TriggerCascadeNoStore,
                               P2P_TARGETS(
                                   put,
                                   put_and_forget,
#ifdef ENABLE_EVALUATION
                                   perf_put,
#endif
                                   remove,
                                   get,
                                   multi_get,
                                   get_by_time,
                                   multi_list_keys,
                                   list_keys,
                                   list_keys_by_time,
                                   multi_get_size,
                                   get_size,
                                   get_size_by_time,
                                   trigger_put
#ifdef ENABLE_EVALUATION
                                   ,dump_timestamp_log
#ifdef DUMP_TIMESTAMP_WORKAROUND
                                   ,dump_timestamp_log_workaround
#endif
#endif
                                   ),
                               ORDERED_TARGETS(
                                   ordered_put,
                                   ordered_put_and_forget,
                                   ordered_remove,
                                   ordered_get,
                                   ordered_list_keys,
                                   ordered_get_size
#ifdef ENABLE_EVALUATION
                                   ,ordered_dump_timestamp_log
#endif
                                   ));
#ifdef ENABLE_EVALUATION
        virtual void dump_timestamp_log(const std::string& filename) const override;
#ifdef DUMP_TIMESTAMP_WORKAROUND
        virtual void dump_timestamp_log_workaround(const std::string& filename) const override;
#endif
#endif//ENABLE_EVALUATION
        virtual void trigger_put(const VT& value) const override;
        virtual std::tuple<persistent::version_t,uint64_t> put(const VT& value) const override;
        virtual void put_and_forget(const VT& value) const override;
#ifdef ENABLE_EVALUATION
        virtual double perf_put(const uint32_t max_payload_size,const uint64_t duration_sec) const override;
#endif//ENABLE_EVALUATION
        virtual std::tuple<persistent::version_t,uint64_t> remove(const KT& key) const override;
        virtual const VT get(const KT& key, const persistent::version_t& ver, const bool stable, bool exact=false) const override;
        virtual const VT multi_get(const KT& key) const override;
        virtual const VT get_by_time(const KT& key, const uint64_t& ts_us, const bool stable) const override;
        virtual std::vector<KT> multi_list_keys(const std::string& prefix) const override;
        virtual std::vector<KT> list_keys(const std::string& prefix, const persistent::version_t& ver, const bool stable) const override;
        virtual std::vector<KT> list_keys_by_time(const std::string& prefix, const uint64_t& ts_us, const bool stable) const override;
        virtual uint64_t multi_get_size(const KT& key) const override;
        virtual uint64_t get_size(const KT& key, const persistent::version_t& ver, const bool stable, bool exact=false) const override;
        virtual uint64_t get_size_by_time(const KT& key, const uint64_t& ts_us, const bool stable) const override;
        virtual std::tuple<persistent::version_t,uint64_t> ordered_put(const VT& value) override;
        virtual void ordered_put_and_forget(const VT& value) override;
        virtual std::tuple<persistent::version_t,uint64_t> ordered_remove(const KT& key) override;
        virtual const VT ordered_get(const KT& key) override;
        virtual std::vector<KT> ordered_list_keys(const std::string& prefix) override;
        virtual uint64_t ordered_get_size(const KT& key) override;
#ifdef ENABLE_EVALUATION
        virtual void ordered_dump_timestamp_log(const std::string& filename) override;
#endif//ENABLE_EVALUATION

        // serialization support
        virtual std::size_t to_bytes(uint8_t*) const override {return 0;}
        virtual void post_object(const std::function<void(uint8_t const* const, std::size_t)>&) const override {}
        virtual std::size_t bytes_size() const {return 0;}
        static std::unique_ptr<TriggerCascadeNoStore<KT,VT,IK,IV>> from_bytes(mutils::DeserializationManager*,const uint8_t*);
        static mutils::context_ptr<TriggerCascadeNoStore<KT,VT,IK,IV>> from_bytes_noalloc(mutils::DeserializationManager*,uint8_t const*);
        void ensure_registered(mutils::DeserializationManager&) {}

        // constructors
        TriggerCascadeNoStore(CriticalDataPathObserver<TriggerCascadeNoStore<KT,VT,IK,IV>>* cw=nullptr,
                       ICascadeContext* cc=nullptr);
    };

    /**
     * get_pathname(): retrieve the pathname, a.k.a prefix from a key. 
     * A pathname identifies the object pool this object belongs to.
     *
     * @tparam KeyType - Type of the Key
     * @param  key     - key
     *
     * @return pathname. An empty string returns for invalid key types and invalid keys. 
     */
    template <typename KeyType>
    inline std::string get_pathname(const KeyType& key);

} // namespace cascade
} // namespace derecho
#include "detail/cascade_impl.hpp"
