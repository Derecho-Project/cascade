#pragma once
#include "object.hpp"

namespace derecho {
namespace cascade {

using sharding_policy_t = enum sharding_policy_type {
    HASH,
    RANGE
};

template<typename... CascadeTypes>
class ObjectPoolMetadata : public mutils::ByteRepresentable,
                           public ICascadeObject<std::string>,
                           public IKeepTimestamp,
                           public IVerifyPreviousVersion {
public:
    mutable persistent::version_t               version;
    mutable uint64_t                            timestamp_us;
    mutable persistent::version_t               previous_version;
    mutable persistent::version_t               previous_version_by_key;
    std::string                                 pathname; //object pool is identified by a pathname starting with '/', just like an absolute path name in a file system.
    uint32_t                                    subgroup_type_index; // index of subgroup type into subgroup_type_order
    uint32_t                                    subgroup_index; // index of the subgroup of type subgroup_type_order[subgroup_type_index]
    sharding_policy_t                           sharding_policy; // the default sharding policy
    std::unordered_map<std::string,uint32_t>    object_locations; // the list of shards where a corresponding key is stored.
    bool                                        deleted; // is deleted

    // serialization support
    DEFAULT_SERIALIZATION_SUPPORT(ObjectPoolMetadata<CascadeTypes...>,
                                  version,
                                  timestamp_us,
                                  previous_version,
                                  previous_version_by_key,
                                  pathname,
                                  subgroup_type_index,
                                  subgroup_index,
                                  sharding_policy,
                                  object_locations,
                                  deleted);

    // constructor 0: default
    ObjectPoolMetadata():
        version(persistent::INVALID_VERSION),
        timestamp_us(0),
        previous_version(persistent::INVALID_VERSION),
        previous_version_by_key(persistent::INVALID_VERSION),
        pathname(""),
        subgroup_type_index(0),
        subgroup_index(0),
        sharding_policy(HASH),
        object_locations(),
        deleted(false) {}

    // constructor 1:
    ObjectPoolMetadata(const persistent::version_t _version,
                       const uint64_t _timestamp_us,
                       const persistent::version_t _previous_version,
                       const persistent::version_t _previous_version_by_key,
                       const std::string& _pathname,
                       uint32_t _subgroup_type_index,
                       uint32_t _subgroup_index,
                       sharding_policy_t _sharding_policy,
                       const std::unordered_map<std::string,uint32_t>& _object_locations,
                       bool _deleted):
        version(_version),
        timestamp_us(_timestamp_us),
        previous_version(_previous_version),
        previous_version_by_key(_previous_version_by_key),
        pathname(_pathname),
        subgroup_type_index(_subgroup_type_index),
        subgroup_index(_subgroup_index),
        sharding_policy(_sharding_policy),
        object_locations(_object_locations),
        deleted(_deleted) {}

    ObjectPoolMetadata(const std::string& _pathname,
                       uint32_t _subgroup_type_index,
                       uint32_t _subgroup_index,
                       sharding_policy_t _sharding_policy,
                       const std::unordered_map<std::string,uint32_t>& _object_locations,
                       bool _deleted):
        version(persistent::INVALID_VERSION),
        timestamp_us(0),
        previous_version(persistent::INVALID_VERSION),
        previous_version_by_key(persistent::INVALID_VERSION),
        pathname(_pathname),
        subgroup_type_index(_subgroup_type_index),
        subgroup_index(_subgroup_index),
        sharding_policy(_sharding_policy),
        object_locations(_object_locations),
        deleted(_deleted) {}

    // constructor 2: copy constructor
    ObjectPoolMetadata(const ObjectPoolMetadata& other):
        version(other.version),
        timestamp_us(other.timestamp_us),
        previous_version(other.previous_version),
        previous_version_by_key(other.previous_version_by_key),
        pathname(other.pathname),
        subgroup_type_index(other.subgroup_type_index),
        subgroup_index(other.subgroup_index),
        sharding_policy(other.sharding_policy),
        object_locations(other.object_locations),
        deleted(other.deleted) {}

    // constructor 3: move constructor
    ObjectPoolMetadata(ObjectPoolMetadata&& other):
        version(other.version),
        timestamp_us(other.timestamp_us),
        previous_version(other.previous_version),
        previous_version_by_key(other.previous_version_by_key),
        pathname(other.pathname),
        subgroup_type_index(other.subgroup_type_index),
        subgroup_index(other.subgroup_index),
        sharding_policy(other.sharding_policy),
        object_locations(std::move(other.object_locations)),
        deleted(other.deleted) {}

    void operator = (const ObjectPoolMetadata& other) {
        this->version = other.version;
        this->timestamp_us = other.timestamp_us;
        this->previous_version = other.previous_version;
        this->previous_version_by_key = other.previous_version_by_key;
        this->pathname = other.pathname;
        this->subgroup_type_index = other.subgroup_type_index;
        this->subgroup_index = other.subgroup_index;
        this->sharding_policy = other.sharding_policy;
        this->object_locations = other.object_locations;
        this->deleted = other.deleted;
    }

    virtual const std::string& get_key_ref() const override {
        return this->pathname;
    }

    virtual bool is_null() const override {
        return (subgroup_type_index == invalid_subgroup_type_index);
    }

    virtual bool is_valid() const override {
        return !this->pathname.empty() && (this->pathname.at(0) == '/');
    }

    virtual void set_version(persistent::version_t ver) const override {
        this->version = ver;
    }

    virtual persistent::version_t get_version() const override {
        return this->version;
    }

    virtual void set_timestamp(uint64_t ts_us) const override {
        this->timestamp_us = ts_us;
    }

    virtual uint64_t get_timestamp() const override {
        return this->timestamp_us;
    }

    virtual void set_previous_version(persistent::version_t prev_ver, persistent::version_t prev_ver_by_key) const override {
        this->previous_version = prev_ver;
        this->previous_version_by_key = prev_ver_by_key;
    }

    virtual bool verify_previous_version(persistent::version_t prev_ver,persistent::version_t prev_ver_by_key) const override {
        return ((this->previous_version == persistent::INVALID_VERSION)?true:(this->previous_version >= prev_ver)) &&
               ((this->previous_version_by_key == persistent::INVALID_VERSION)?true:(this->previous_version_by_key >= prev_ver_by_key));
    }

    /**
     * Find the shard for an object: key_to_shard_index
     *
     * @tparam KeyType type of the key.
     * @param  key
     * @param  num_shards
     * @param  check_object_locations - By default, we check the object location maps. In most cases, we can accelerate
     *                                  process by disabling it by setting it to false.
     * @return shard index.
     */
    template<typename KeyType>
    inline uint32_t key_to_shard_index(const KeyType& key, uint32_t num_shards, bool check_object_locations = true) const {
        if constexpr (std::is_convertible_v<KeyType,std::string>) {
            if (check_object_locations) {
                if (this->object_locations.find(key) != object_locations.end()) {
                    return object_locations.at(key);
                }
            }
            uint32_t shard_index = 0;
            switch (sharding_policy) {
            case HASH:
                shard_index = std::hash<std::string>{}(key) % num_shards;
                break;
            default:
                throw new derecho::derecho_exception(std::string("Unknown sharding_policy:") + std::to_string(sharding_policy));
            }
            return shard_index;
        } else {
            throw new derecho::derecho_exception(std::string{__PRETTY_FUNCTION__} + " failed with invalid Key Type:" + typeid(KeyType).name());
        }
    }

    static std::string IK;
    static ObjectPoolMetadata<CascadeTypes...> IV;

    // The type order vector.
    static const std::vector<std::type_index> subgroup_type_order;
    // The subgroup type reserved for metadata service
    static const uint32_t metadata_service_subgroup_type_index;
    // The subgroup index reserved for metadata service
    static const uint32_t metadata_service_subgroup_index;
    // The invalid subgroup type index;
    static const uint32_t invalid_subgroup_type_index;
    /**
     * get subgroup type index by type
     * @tparam SubgroupType type of the subgroup
     *
     * @return subgroup type index
     */
    template<typename SubgroupType>
    static inline uint32_t get_subgroup_type_index();
};

template<typename... CascadeTypes>
std::string ObjectPoolMetadata<CascadeTypes...>::IK;

template<typename... CascadeTypes>
ObjectPoolMetadata<CascadeTypes...> ObjectPoolMetadata<CascadeTypes...>::IV(
        persistent::INVALID_VERSION, // version
        0,                           //timestamp_us
        persistent::INVALID_VERSION, // previous_version
        persistent::INVALID_VERSION, // previous_version_by_key
        "",                          // ID
        invalid_subgroup_type_index, // subgroup_type_index
        0,                           // subgroup_index
        HASH,                        // HASH
        {},                          // object_locations
        false);                      // deleted

template<typename... CascadeTypes>
const std::vector<std::type_index> ObjectPoolMetadata<CascadeTypes...>::subgroup_type_order{std::type_index(typeid(CascadeTypes))...};

template<typename... CascadeTypes>
const uint32_t ObjectPoolMetadata<CascadeTypes...>::metadata_service_subgroup_type_index = 0;

template<typename... CascadeTypes>
const uint32_t ObjectPoolMetadata<CascadeTypes...>::metadata_service_subgroup_index = 0;

template<typename... CascadeTypes>
const uint32_t ObjectPoolMetadata<CascadeTypes...>::invalid_subgroup_type_index = 0xffffffff;

template<typename... CascadeTypes>
template<typename SubgroupType>
uint32_t ObjectPoolMetadata<CascadeTypes...>::get_subgroup_type_index() {
    uint32_t index = 0;
    while(index < subgroup_type_order.size()) {
        if (std::type_index(typeid(SubgroupType)) == subgroup_type_order.at(index)) {
            return index;
        }
        index ++;
    }
    return invalid_subgroup_type_index;
}

template<typename... CascadeTypes>
inline std::ostream& operator<<(std::ostream& out, const ObjectPoolMetadata<CascadeTypes...>& opm) {
    out << "object pool metadata@" << &opm << " is " << (opm.is_valid()?"valid":"invalid") << " and "
        << (opm.is_null()?"null":"not null.") << std::endl;
    if(opm.is_valid() && !opm.is_null()) {
        out << std::string{typeid(opm).name()} << "\n" <<
            "\tversion:" << opm.version << "\n" <<
            "\ttimestamp_us:" << opm.timestamp_us << "\n" <<
            "\tprevious_version:" << opm.previous_version << "\n" <<
            "\tprevious_version_by_key:" << opm.previous_version_by_key << "\n" <<
            "\tpathname:" << opm.pathname << "\n" <<
            "\tsubgroup_type:" << std::to_string(opm.subgroup_type_index) << "-->" << ObjectPoolMetadata<CascadeTypes...>::subgroup_type_order[opm.subgroup_type_index].name() << "\n" <<
            "\tsubgroup_index:" << std::to_string(opm.subgroup_index) << "\n" <<
            "\tsharding_policy:" << std::to_string(opm.sharding_policy) <<"\n" <<
            "\tobject_locations:[hidden]" << "\n" <<
            "\tis_deleted:" << std::to_string(opm.deleted) <<
            std::endl;
    }
    return out;
}

}
}
