#pragma once

/**
 * @file    cascade_interface.hpp
 * @brief   Declarations of the interfaces of core Cascade types as Derecho Subgroup type.
 */

#include <cascade/config.h>

#include <derecho/core/derecho.hpp>
#include <derecho/mutils-serialization/SerializationSupport.hpp>
#include <derecho/persistent/Persistent.hpp>

#include <cstdint>
#include <string>
#include <tuple>
#include <vector>

namespace derecho {
/**
 * @namespace   cascade
 * @brief       The container for all cascade code.
 */
namespace cascade {

/**
 * @brief   The off-critical data path handler API
 */
class ICascadeContext : public derecho::DeserializationContext {};

#define CURRENT_VERSION (persistent::INVALID_VERSION)

/**
 * @brief CriticalDataPathObserver
 * The interface for critical data path observers.
 *
 * @tparam CascadeType  The CriticalDataPathObserver for corresponding cascade type.
 */
template <typename CascadeType>
class CriticalDataPathObserver : public derecho::DeserializationContext {
public:
    /**
     * The critical data path behaviour is defined here. The default behaviour is do nothing.
     *
     * @param[in]   subgroup_idx      The subgroup index
     * @param[in]   shard_idx         The shard index
     * @param[in]   sender_id         The node id of the sender of the K/V pair
     * @param[in]   key               The key of the K/V pair
     * @param[in]   value             The value of the K/V pair
     * @param[in]   cascade_ctxt      The cascade context to be used later
     * @param[in]   is_trigger        True for critical data path of `p2p_send`; otherwise, the critical data path of `ordered_send`.
     *
     * @return void
     */
    virtual void operator()(const uint32_t subgroup_idx,
                            const uint32_t shard_idx,
                            const node_id_t sender_id,
                            const typename CascadeType::KeyType& key,
                            const typename CascadeType::ObjectType& value,
                            ICascadeContext* cascade_ctxt,
                            bool is_trigger = false) {}
};

/**
 * A tuple including the version number of an update and its associated timestamp.
 * This is the return type of several CascadeStore functions.
 */
using version_tuple = std::tuple<persistent::version_t, uint64_t>;

/**
 * @brief   The cascade store interface.
 * This interface is for different Cascade Subgroup Types which provides different persistence guarantees.
 *
 * @tparam KT   The type of the key
 * @tparam VT   The type of the value must
 *              - includes a public and mutable field `ver` of type `std::tuple<version_t,uint64_t>` for its version and
 *              timestamp.
 *              - includes a public field `key` of type `KT` for the key
 *              - TODO: enforce this with 'concepts' in C++ 20?
 * @tparam IK A pointer to an invalid key (generally a static member of class `KT`)
 * @tparam IV A pointer to an invalid value (generally a static member of class `VT`)
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
     * @brief   put(const VT&, bool )
     *
     * Put a value. VT must implement ICascadeObject interface. The key is given in value and retrieved by
     * ICascadeObject::get_key_ref()
     *
     * @param[in]   value       The K/V pair value
     * @param[in]   as_trigger  The object will NOT be used to update the K/V state.
     *
     * @return      a tuple including version number (version_t) and a timestamp in microseconds.
     */
    virtual version_tuple put(const VT& value, bool as_trigger) const = 0;

    /**
     * @brief   put_and_forget(const VT&)
     *
     * Put a value. VT must implement ICascadeObject interface. The key is given in value and retrieved by
     * ICascadeObject::get_key_ref(). This function ignores any return value.
     *
     * @param[in]   value   The K/V pair value
     * @param[in]   as_trigger  The object will NOT be used to update the K/V state.
     *
     * @return      void
     */
    virtual void put_and_forget(const VT& value, bool as_trigger) const = 0;


    /**
     * @brief oob_send
     *
     * @param[in] data_addr Local memory address of data to write to remote node
     * @param[in] gpu_addr  Remote address to write to 
     * @param[in] rkey  Access key to the remote memory
     * @param[in] size The size of the remote allocated memory
     */
    virtual bool oob_send(uint64_t data_addr,uint64_t gpu_addr, uint64_t rkey,size_t size) const {
	dbg_default_warn("Calling unsupported func:{}", __PRETTY_FUNCTION__);
    	return false;
    }  

#ifdef ENABLE_EVALUATION
 /**
     * @brief   A function to evaluate the performance of an internal shard
     *
     * @param[in]   max_payload_size    The maximum size of the payload.
     * @param[in]   duration_sec        The duration of the test in seconds.
     *
     * @return Operations/sec.
     */
    virtual double perf_put(const uint32_t max_payload_size, const uint64_t duration_sec) const = 0;
#endif
    /**
     * @brief   remove(const KT&)
     *
     * Remove a value by key. The key will still be in the map with an empty value.
     *
     * @param[in]   key     The key of the K/V pair to be removed.
     *
     * @return a tuple including version number (version_t) and a timestamp in microseconds.
     */
    virtual version_tuple remove(const KT& key) const = 0;

    /**
     * @brief   get(const KT&,const persistent::version_t&)
     *
     * Get a value by key and version.
     *
     * @param[in]   key     The key of the K/V pair to be retrieved.
     * @param[in]   ver   Version: if `version == CURRENT_VERSION`, get the latest value.
     * @param[in]   stable
     *              If `stable == false`, we only return the data reflecting the latest locally delivered atomic
     *              broadcast. Otherwise, stable data will be returned, meaning that the persisted states returned
     *              is safe: they will survive after whole system recovery.
     * @param[in]   exact 
     *              The exact match flag: this function try to return the value of that key at the 'ver'. If such a
     *              value does not exists and exact is true, it will throw an exception. If such a value does not
     *              exists and exact is false, it will return the latest state of the value for 'key' before 'ver'.
     *              The former case is very efficient but the latter one is not because of reconstructing the state.
     *              Please note that the current Persistent<T> in derecho will reconstruct the state at 'ver' from
     *              the beginning of the log entry if 'ver' != CURRENT_VERSION, which is extremely inefficient.
     *              TODO: use checkpoint cache to accelerate that process.
     *
     * @return A value
     *
     * @throws std::runtime_error, if requested value is not found.
     */
    virtual const VT get(const KT& key, const persistent::version_t& ver, const bool stable, bool exact = false) const = 0;

    /**
     * @brief   multi_get(const KT&)
     *
     * Get a value by key. This is an ordered get that participates atomic broadcast, which reflecting the latest
     * global atomic broadcast.
     *
     * @param[in]   key
     *
     * @return a value
     */
    virtual const VT multi_get(const KT& key) const = 0;

    /**
     * @brief   get_by_time(const KT&, const uint64_t& ts_us)
     *
     * Get a value by key and timestamp.
     *
     * Please note that the current Persistent<T> in derecho will reconstruct the state at 'ts_us' from the
     * beginning of the log entry, which is extremely inefficient. TODO: use checkpoint cache to accelerate that
     * process.
     *
     * @param[in]   key
     * @param[in]   ts_us       Timestamp in microsecond
     * @param[in]   stable
     *              if stable == false, we only return the data reflecting the latest locally delivered atomic
     *              broadcast. Otherwise, stable data will be returned, meaning that the persisted states returned
     *              is safe: they will survive after whole system recovery.
     *
     * @return A value
     */
    virtual const VT get_by_time(const KT& key, const uint64_t& ts_us, const bool stable) const = 0;

    /**
     * @brief   multi_list_keys(const std::string& prefix)
     *
     * List the most current keys by an atomic broadcast
     *
     * @param[in]   prefix      Prefix, only the key matching this prefix will be returned. TODO: KT/VT provider should
     *                          provide their own prefix matching implementation,
     *                          Empty prefix matches all keys.
     *
     * @return  A list of keys.
     */
    virtual std::vector<KT> multi_list_keys(const std::string& prefix) const = 0;

    /**
     * @brief   list_keys(const std::string& prefix, const persistent::version_t& ver, const bool stable)
     *
     * List keys at version.
     *
     * @param[in]   prefix      Prefix, only the key matching this prefix will be returned. TODO: `KT`/`VT` provider
     *                          should provide their own prefix matching implementation,
     *                          Empty prefix matches all keys.
     * @param[in]   ver         The version, if `ver == CURRENT_VERSION`, get the latest list of keys.
     *                          Please note that the current Persistent<T> in derecho will reconstruct the state at
     *                          `ver` from the beginning of the log entry if `ver != CURRENT_VERSION`, which is
     *                          extremely inefficient.
     *                          TODO: use checkpoint cache to accelerate that process.
     * @param[in]   stable
     *                  If `stable == false`, we only return the data reflecting the latest locally delivered atomic
     *                  broadcast. Otherwise, stable data will be returned, meaning that the persisted states returned
     *                  is safe: they will survive after whole system recovery.
     *
     * @return  A list of keys.
     */
    virtual std::vector<KT> list_keys(const std::string& prefix, const persistent::version_t& ver, const bool stable) const = 0;

    /**
     * @brief   list_keys_by_time(const std::string&, const uint64_t&, const bool)
     *
     * List keys by timestamp
     *
     * Please note that the current Persistent<T> in derecho will reconstruct the state at 'ts_us' from the
     * beginning of the log entry, which is extremely inefficient. TODO: use checkpoint cache to accelerate that
     * process.
     *
     * @param[in]   prefix      The prefix, only the key matching this prefix will be returned. TODO: KT/VT provider should
     *                          provide their own prefix matching implementation,
     *                          Empty prefix matches all keys.
     * @param[in]   ts_us       The timestamp in microsecond
     * @param[in]   stable
     *                  If `stable == false`, we only return the data reflecting the latest locally delivered atomic
     *                  broadcast. Otherwise, stable data will be returned, meaning that the persisted states returned
     *                  is safe: they will survive after whole system recovery.
     *
     * @return  A list of keys.
     */
    virtual std::vector<KT> list_keys_by_time(const std::string& prefix, const uint64_t& ts_us, const bool stable) const = 0;

    /**
     * @brief multi_get_size(const KT&)
     *
     * Get size of the latest object using atomic broadcast.
     *
     * @param[in]   key     The key of the K/V pair.
     *
     * @return  The size of serialized value.
     */
    virtual uint64_t multi_get_size(const KT& key) const = 0;

    /**
     * @brief   get_size(const KT&,const persistent::version_t&,bool)
     *
     * Get size by version
     *
     * @param[in]   key     The key
     * @param[in]   ver     Version, if `ver == CURRENT_VERSION`, get the latest value.
     * @param[in]   stable
     * @param[in]   exact 
     *              The exact match flag: this function try to return the value of that key at the 'ver'. If such a
     *              value does not exists and exact is true, it will throw an exception. If such a value does not
     *              exists and exact is false, it will return the latest state of the value for 'key' before 'ver'.
     *              The former case is very efficient but the latter one is not because of reconstructing the state.
     *              Please note that the current Persistent<T> in derecho will reconstruct the state at 'ver' from
     *              the beginning of the log entry if `ver != CURRENT_VERSION`, which is extremely inefficient.
     *              TODO: use checkpoint cache to accelerate that process.
     *
     * @return  The size of serialized value.
     */
    virtual uint64_t get_size(const KT& key, const persistent::version_t& ver, const bool stable, const bool exact = false) const = 0;

    /**
     * @brief   get_size_by_time(const KT&,const uint64_t&)
     *
     * Get size by timestamp
     *
     * Please note that the current Persistent<T> in derecho will reconstruct the state at 'ts_us' from the
     * beginning of the log entry, which is extremely inefficient. TODO: use checkpoint cache to accelerate that
     * process.
     *
     * @param[in]   key     The key
     * @param[in]   ts_us   The timestamp in microsecond
     * @param[in]   stable  return the stablized data
     *
     * @return  The size of serialized value.
     */
    virtual uint64_t get_size_by_time(const KT& key, const uint64_t& ts_us, const bool stable) const = 0;

    /**
     * @brief   trigger_put(const VT& value)
     *
     * Put object as a trigger. This call will not cause a store but only trigger an off-critical data path
     * computation. Please note that this call should be handled in p2p processing thread.
     *
     * @param[in]   value       The K/V pair object to trigger some action.
     */
    virtual void trigger_put(const VT& value) const = 0;

#ifdef ENABLE_EVALUATION
    /**
     * @brief   dump_timestamp_log(const std::string& filename)
     *
     * Dump the timestamp log to a local file specified by `filename`
     *
     * @param[in]   filename    The name of the timestamp log file.
     */
    virtual void dump_timestamp_log(const std::string& filename) const = 0;
#ifdef DUMP_TIMESTAMP_WORKAROUND
    /**
     * @brief   dump_timestamp_log_workaround(const std::string& filename)
     *
     * Dump the timestamp log to a local file specified by `filename`
     *
     * @param[in]   filename    The name of the timestamp log.
     */
    virtual void dump_timestamp_log_workaround(const std::string& filename) const = 0;
#endif

#endif  // ENABLE_EVALUATION

protected:
    /**
     * @brief   ordered_put
     *
     * @param[in]   value       The K/V pair object.
     * @param[in]   as_trigger  If true, the value will NOT apply to the K/V state.
     *
     * @return  A tuple including version number (version_t) and a timestamp in microseconds.
     */
    virtual version_tuple ordered_put(const VT& value, bool as_trigger) = 0;

    /**
     * @brief   ordered_put_and_forget
     *
     * @param[in]   value       The K/V pair object.
     * @param[in]   as_trigger  If true, the value will NOT apply to the K/V state.
     */
    virtual void ordered_put_and_forget(const VT& value, bool as_trigger) = 0;

    /**
     * @brief   ordered_remove
     *
     * @param[in]   key     The key of the K/V pair object.
     *
     * @return  A tuple including version number (version_t) and a timestamp in microseconds.
     */
    virtual version_tuple ordered_remove(const KT& key) = 0;

    /**
     * @brief   ordered_get
     *
     * @param[in]   key     The key of the K/V pair object.
     *
     * @return  A K/V pair value.
     */
    virtual const VT ordered_get(const KT& key) = 0;
    /**
     * @brief   ordered_list_keys
     *
     * @param[in]   prefix  The prefix of a set of keys.
     *
     * @return  A list of keys.
     */
    virtual std::vector<KT> ordered_list_keys(const std::string& prefix) = 0;

    /**
     * @brief   ordered_get_size
     *
     * @param[in]   key     The key of the corresponding K/V pair object.
     *
     * @return  The size of the corresponding object.
     */
    virtual uint64_t ordered_get_size(const KT& key) = 0;

#ifdef ENABLE_EVALUATION
    /**
     * @brief   ordered_dump_timestamp_log(const std::string& filename)
     *
     * Dump the timestamp log to a local file specified by "filename"
     *
     * @param[in]   filename    The name of the timestamp log file.
     */
    virtual void ordered_dump_timestamp_log(const std::string& filename) = 0;
#endif  // ENABLE_EVALUATION
};

/**
 * Interfaces for ValueTypes, derive them to enable corresponding features.
 */

/**
 * @brief   create_null_object(const KT&)
 *
 * Users need to implement this function to create a null object of a given key.
 *
 * @tparam      KT      The key type.
 * @tparam      VT      The value type.
 * @tparam      IK      A pointer to a pre allocated `null` key object.
 * @tparam      IV      A pointer to a pre allocated `null` value object.
 * @param[in]   key     An optional key of the null object, defaulted to `null`.
 *
 * return   A `null` object of `VT` type.
 */
template <typename KT, typename VT, KT* IK, VT* IV>
VT create_null_object_cb(const KT& key = *IK);

/**
 * @brief The type interface for Cascade K/V pair objects.
 *
 * The VT template type of PersistentCascadeStore/VolatileCascadeStore must implement ICascadeObject interface.
 * We use both the concepts of null and valid object in Cascade. A null object precisely means 'no data'; while a
 * valid object literarily means an object is 'valid'. Technically, a null object has a valid key while invalid
 * object does not.
 * 
 * @tparam      KT      The key type.
 * @tparam      VT      The value type.
 */
template <typename KT, typename VT>
class ICascadeObject {
public:
    /**
     * @brief   get_key_ref()
     *
     * Get a const reference to the key.
     *
     * @return  A const reference to the key.
     */
    virtual const KT& get_key_ref() const = 0;
    /**
     * @brief   is_null()
     *
     * Test if this Object is null or not.
     *
     * @return  True for null object; otherwise, false is returned.
     */
    virtual bool is_null() const = 0;
    /**
     * @brief   is_valid()
     *
     * Test if this Object is valid or not.
     *
     * @return  True for valid object; otherwise, false is returned.
     */
    virtual bool is_valid() const = 0;
    /**
     * @brief   copy_from()
     *
     * Copy object from another object. This is very similar to a copy = operator. We disabled = operator to avoid
     * misuse. And we introduce copy_from operation in case such a copy is required.
     *
     * @param[in]   rhs   A const reference to the other object.
     */
    virtual void copy_from(const VT& rhs) = 0;
};

/**
 * @brief   An optional interface for Cascade objects to enable versioning.
 *
 * If the VT template type of PersistentCascadeStore/VolatileCascadeStore implements IKeepVersion interface, its
 * `set_version` method will be called on `ordered_put` or `ordered_remove` with the current version assigned to
 * this operation. The `VT` implementer may save this version in its states.
 */
class IKeepVersion {

    /**
     * @brief   The version setter
     *
     * A callback on PersistentCascadeStore/VolatileCascadeStore updates.
     *
     * @param[in]   ver     The current version to be set.
     */
    virtual void set_version(persistent::version_t ver) const = 0;

    /**
     * @brief   The version getter
     * 
     * Get the version
     *
     * @return  The K/V object's version.
     */
    virtual persistent::version_t get_version() const = 0;
};

/**
 * @brief   An optional interface for Cascade objects to enable timestamping.
 *
 * If the VT template type of PersistentCascadeStore/VolatileCascadeStore implements IKeepTimestamp interface, its
 * 'set_timestamp' method will be called on updates with the timestamp in microseconds assigned to this operation.
 * The `VT` implementer may save this timestamp in its states.
 */
class IKeepTimestamp {

    /**
     * @brief   A callback on PersistentCascadeStore/VolatileCascadeStore updates.
     *
     * @param[in]   ts_us   The timestamp in microseconds
     */
    virtual void set_timestamp(uint64_t ts_us) const = 0;

    /**
     * @brief   Get the timestamp
     *
     * @return  The K/V object's timestamp
     */
    virtual uint64_t get_timestamp() const = 0;
};

/**
 * @brief   An optional interface for Cascade objects to enable version tracing by key.
 *
 * If the VT template type of PersistentCascadeStore implements IKeepPreviousVersion interface, its
 * `set_previous_version` method will be called on `ordered_put` with the previous version in the shard as well as
 * the previous version of the same key. If this is the first value of that key, `set_previous_version` will be
 * called with `INVALID_VERSION`, meaning a genesis value. Therefore, the VT implementer must save the version in
 * its object and knows how to get them when they get a value from cascade.
 */
class IKeepPreviousVersion : public IKeepVersion {
public:

    /**
     * @brief   set_previous_version() is a callback on PersistentCascadeStore::ordered_put();
     *
     * @param[in]   prev_ver          The previous version
     * @param[in]   prev_ver_by_key   The previous version of the same key in VT object
     */
    virtual void set_previous_version(persistent::version_t prev_ver, persistent::version_t prev_ver_by_key) const = 0;
};

/**
 * @brief   An optional interface for Cascade objects to customize monotonic version checking.
 *
 * If the VT template of PersistentCascadeStore/VolatileCascadeStore implements IVerifyPreviousVersion interface,
 * its `verify_previous_version` will be called on `ordered_put` with the previous version in the shard as well as
 * the previous version of the same key. If this is the first value of that key, `verify_previous_version` will be
 * called with `INVALID_VERSION`, meaning a genesis value. The VT implementer must make sure if that satisfy
 * application semantics.
 *
 * For example, a VT object may compare if the given 'prev_ver' and 'prev_ver_key_key' match the previous versions
 * it saw (those versions might be VT members). If an application rejects writes from a client without knowing the
 * latest state of corresponding key, it can return false, meaning verify failed, if 'prev_ver_by_key' is greater
 * than the previous state cached in VT.
 */
class IVerifyPreviousVersion : public IKeepPreviousVersion {
public:

    /**
     * @brief   A callback on PresistentCascadeStore::ordered_put and VolatileCascadeStore::ordered_put.
     *
     * @param[in]   prev_ver          The previous version
     * @param[in]   prev_ver_by_key   The previous version of the same key in VT object
     *
     * @return If 'prev_ver' and 'prev_ver_by_key' are acceptable, returns `true`, otherwise, `false`.
     */
    virtual bool verify_previous_version(persistent::version_t prev_ver, persistent::version_t prev_ver_by_key) const = 0;
};

/**
 * @brief   An optional interface for Cascade objects to customize object validation behavior.
 *
 * If the VT type for PersistentCascadeStore/VolatileCascadeStore implements IValidator inferface, its 'validate'
 * method will be called on 'ordered_put' with the current k/v map to verify if the object can be added to the
 * existing k/v map pool.
 *
 * For example, a VT object can override the default 'overwriting' behaviour by refusing an object whose key has
 * already existed in the kv_map.
 *
 * @tparam  KT      The key type
 * @tparam  VT      The value type
 */
template <typename KT, typename VT>
class IValidator {
public:

    /**
     * @brief   A callback on PersistentCascadeStore::ordered_put or VolatileCascadeStore::ordered_put.
     *
     * @param[in]   kv_map      The reference to the current shard state as a map from `KT` to `VT`.
     *
     * @return  Returns `true` if validation is successful, otherwise, `false`.
     */
    virtual bool validate(const std::map<KT, VT>& kv_map) const = 0;
};

#ifdef ENABLE_EVALUATION
/**
 * @brief   An optional interface for Cascade objects to enalbing message ID.
 *
 * If the VT template of PersistentCascadeStore implements IKeepPreviousVersion interface, its 'set_message_id'
 * method is used to set an id dedicated for evaluation purpose, which is different from the key. The
 * 'get_message_id' methdo is used to retrieve its id.
 */
class IHasMessageID {
public:

    /**
     * @brief   Message ID setter
     *
     * @param[in]   id  The message id to be set.
     */
    virtual void set_message_id(uint64_t id) const = 0;

    /**
     * @brief   Message ID getter
     *
     * @return  The message id
     */
    virtual uint64_t get_message_id() const = 0;
};
#endif

}  // namespace cascade
}  // namespace derecho
