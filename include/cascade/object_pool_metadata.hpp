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
    std::string                                 id; //object pool id
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
                                  id,
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
        id(""),
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
                       const std::string& _id,
                       uint32_t _subgroup_type_index,
                       uint32_t _subgroup_index,
                       sharding_policy_t _sharding_policy,
                       const std::unordered_map<std::string,uint32_t>& _object_locations,
                       bool _deleted):
        version(_version),
        timestamp_us(_timestamp_us),
        previous_version(_previous_version),
        previous_version_by_key(_previous_version_by_key),
        id(_id),
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
        id(other.id),
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
        id(other.id),
        subgroup_type_index(other.subgroup_type_index),
        subgroup_index(other.subgroup_index),
        sharding_policy(other.sharding_policy),
        object_locations(std::move(other.object_locations)),
        deleted(other.deleted) {}

    virtual const std::string& get_key_ref() const override {
        return this->id;
    }

    virtual bool is_null() const override {
        return !this->id.empty();
    }

    virtual bool is_valid() const override {
        return !this->id.empty();
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
ObjectPoolMetadata<CascadeTypes...> ObjectPoolMetadata<CascadeTypes...>::IV;

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
    out << std::string{typeid(opm).name()} << "\n" <<
        "\tversion:" << opm.version << "\n" <<
        "\ttimestamp_us:" << opm.timestamp_us << "\n" <<
        "\tprevious_version:" << opm.previous_version << "\n" <<
        "\tprevious_version_by_key:" << opm.previous_version_by_key << "\n" <<
        "\tid:" << opm.id << "\n" <<
        "\tsubgroup_type:" << std::to_string(opm.subgroup_type_index) << "-->" << ObjectPoolMetadata<CascadeTypes...>::subgroup_type_order[opm.subgroup_type_index].name() << "\n" <<
        "\tsubgroup_index:" << std::to_string(opm.subgroup_index) << "\n" <<
        "\tsharding_policy:" << std::to_string(opm.sharding_policy) <<"\n" <<
        "\tobject_locations:[hidden]" << "\n" <<
        "\tis_deleted:" << std::to_string(opm.deleted) <<
        std::endl;
    return out;
}



}
}
