#pragma once
#include "object.hpp"

namespace derecho {
namespace cascade {

using sharding_policy_t = enum sharding_policy_type {
    HASH,
    RANGE
};

template<typename... CascadeTypes>
class ObjectPoolMetadata : public mutils::ByteRepresentable {
public:
    std::string                                 id; //object pool id
    uint32_t                                    subgroup_type_index; // index of subgroup type into subgroup_type_order
    uint32_t                                    subgroup_index; // index of the subgroup of type subgroup_type_order[subgroup_type_index]
    sharding_policy_t                           sharding_policy; // the default sharding policy
    std::unordered_map<std::string,uint32_t>    object_locations; // the list of shards where a corresponding key is stored.
    bool                                        deleted; // is deleted

    // serialization support
    DEFAULT_SERIALIZATION_SUPPORT(ObjectPoolMetadata<CascadeTypes...>,id,subgroup_type_index,subgroup_index,sharding_policy,object_locations,deleted);

    // constructors
    ObjectPoolMetadata(): 
        id("invalid_object_pool"),
        subgroup_type_index(0),
        subgroup_index(0),
        sharding_policy(HASH),
        object_locations(),
        deleted(false) {}

    ObjectPoolMetadata(const std::string& _id,
                       uint32_t _subgroup_type_index,
                       uint32_t _subgroup_index,
                       sharding_policy_t _sharding_policy,
                       const std::unordered_map<std::string,uint32_t>& _object_locations,
                       bool _deleted):
        id(_id),
        subgroup_type_index(_subgroup_type_index),
        subgroup_index(_subgroup_index),
        sharding_policy(_sharding_policy),
        object_locations(_object_locations),
        deleted(_deleted) {}

    // The type order vector.
    static std::vector<std::type_index> subgroup_type_order;
};

template<typename... CascadeTypes>
std::vector<std::type_index> ObjectPoolMetadata<CascadeTypes...>::subgroup_type_order{std::type_index(typeid(CascadeTypes))...};

template<typename... CascadeTypes>
inline std::ostream& operator<<(std::ostream& out, const ObjectPoolMetadata<CascadeTypes...>& opm) {
    out << std::string{typeid(opm).name()} << "\n" <<
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
