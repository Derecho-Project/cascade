#include <cascade/delta_map.hpp>
#include <derecho/core/derecho.hpp>

#include <iostream>

using namespace derecho;
using derecho::cascade::DeltaMap;

class ObjectUsingDeltaMap : public mutils::ByteRepresentable,
                            public derecho::PersistsFields {
    static persistent::version_t INVALID_VERSION;
    persistent::Persistent<DeltaMap<persistent::version_t, persistent::version_t, &INVALID_VERSION>> version_map;

public:
    /** Standard persistent constructor */
    ObjectUsingDeltaMap(persistent::PersistentRegistry* registry) : version_map(registry) {}
    /** Deserialization/move constructor */
    ObjectUsingDeltaMap(persistent::Persistent<DeltaMap<persistent::version_t,
                                                        persistent::version_t,
                                                        &INVALID_VERSION>>& map)
            : version_map(std::move(map)) {}
    void put(persistent::version_t key_version, persistent::version_t value_version) {
        version_map->put(key_version, value_version);
    }
    persistent::version_t get(persistent::version_t key_version) const {
        return version_map->get(key_version);
    }
    void remove(persistent::version_t key_version) {
        version_map->remove(key_version);
    }
    void batch_put(const std::list<std::pair<persistent::version_t, persistent::version_t>>& kv_pairs) {
        for(const auto& pair : kv_pairs) {
            version_map->put(pair.first, pair.second);
        }
    }
    persistent::version_t find_version_before(persistent::version_t search_version) const {
        auto version_map_search = version_map->get_current_map().upper_bound(search_version);
        if(version_map_search != version_map->get_current_map().begin()) {
            version_map_search--;
            return version_map_search->second;
        } else {
            return INVALID_VERSION;
        }
    }

    DEFAULT_SERIALIZATION_SUPPORT(ObjectUsingDeltaMap, version_map);
    REGISTER_RPC_FUNCTIONS(ObjectUsingDeltaMap, ORDERED_TARGETS(put, remove, batch_put), P2P_TARGETS(get, find_version_before));
};

persistent::version_t ObjectUsingDeltaMap::INVALID_VERSION = persistent::INVALID_VERSION;

void standalone_map_test() {
    static std::string empty_str = "";

    using TestDeltaMapType = DeltaMap<int, std::string, &empty_str>;
    persistent::PersistentRegistry standalone_pr(nullptr, typeid(ObjectUsingDeltaMap), 0, 0);
    persistent::Persistent<DeltaMap<int, std::string, &empty_str>> standalone_delta_map(
            std::make_unique<DeltaMap<int, std::string, &empty_str>>, "PersistentDeltaMap",
            &standalone_pr, false);
    // Test put with delta (put, then create delta with version, then save delta with persist)
    standalone_delta_map->put(1, "a");
    standalone_delta_map.version(1);
    standalone_delta_map.persist();
    standalone_delta_map->put(2, "b");
    standalone_delta_map.version(2);
    standalone_delta_map.persist();
    // Test get, which should read the current version and should not create a delta
    std::string one_value = standalone_delta_map->get(1);
    std::cout << "In-memory get(1): " << one_value << std::endl;
    assert(one_value == "a");
    standalone_delta_map.version(3);
    standalone_delta_map.persist();
    // Change key 2 in the current version, then test getDelta on a previous version
    standalone_delta_map->put(2, "x");
    standalone_delta_map.version(4);
    standalone_delta_map.persist();
    std::string two_value = standalone_delta_map->get(2);
    std::cout << "In-memory get(2): " << two_value << std::endl;
    assert(two_value == "x");
    standalone_delta_map.template getDelta<TestDeltaMapType::DeltaType>(
            2, true, [](const TestDeltaMapType::DeltaType& delta) {
                std::cout << "DeltaMap.getDelta at version 2: " << delta.objects << " - by lambda" << std::endl;
                assert(delta.objects.at(2) == "b");
            });
    auto delta_ptr = standalone_delta_map.template getDelta<TestDeltaMapType::DeltaType>(2, true);
    std::cout << "DeltaMap.getDelta at version 2 : " << delta_ptr->objects << " - by copy" << std::endl;
    assert(delta_ptr->objects.at(2) == "b");
    // Test getDeltaByIndex
    standalone_delta_map->put(3, "c");
    standalone_delta_map.version(5);
    standalone_delta_map.persist();
    // After 4 puts, the latest index should be 3, and version 4 should be index 2 (the get doesn't create an index)
    std::cout << "DeltaMap latest index: " << standalone_delta_map.getLatestIndex() << std::endl;
    assert(standalone_delta_map.getLatestIndex() >= 3);
    standalone_delta_map.template getDeltaByIndex<TestDeltaMapType::DeltaType>(
        2, [](const TestDeltaMapType::DeltaType& delta){
            std::cout << "DeltaMap.getDeltaByIndex at index 2: " << delta.objects << " - by lambda" << std::endl;
        }
    );
    auto index_delta_ptr = standalone_delta_map.template getDeltaByIndex<TestDeltaMapType::DeltaType>(2);
    std::cout << "DeltaMap.getDeltaByIndex at index 2: " << index_delta_ptr->objects << " - by copy" << std::endl;
    // Test remove with delta
    standalone_delta_map->remove(1);
    standalone_delta_map.version(6);
    standalone_delta_map.persist();
    std::string deleted_value = standalone_delta_map->get(1);
    std::cout << "In-memory get(1) on a removed key: " << deleted_value << std::endl;
    assert(deleted_value == empty_str);
    std::cout << "Current map after delete: " << standalone_delta_map->get_current_map() << std::endl;
    // Test "slow" get-by-version, without delta
    std::cout << "Entire DeltaMap at version 2: " << standalone_delta_map.get(2)->get_current_map() << std::endl;
    standalone_delta_map.get(2, [](const TestDeltaMapType& past_map) {
        std::cout << "Entire DeltaMap at version 2, by lambda: " << past_map.get_current_map() << std::endl;
        std::map<int, std::string> expected_map{{1, "a"}, {2, "b"}};
        assert(past_map.get_current_map() == expected_map);
    });
    // Test "slow" get-by-index, without delta
    std::cout << "Entire DeltaMap at index 2: " << standalone_delta_map.getByIndex(2)->get_current_map() << std::endl;
    standalone_delta_map.getByIndex(2, [](const TestDeltaMapType& past_map) {
        std::cout << "Entire DeltaMap at index 2, by lambda: " << past_map.get_current_map() << std::endl;
        std::map<int, std::string> expected_map{{1, "a"}, {2, "x"}};
        assert(past_map.get_current_map() == expected_map);
    });

}

void group_map_test(uint32_t num_nodes) {
    derecho::SubgroupInfo subgroup_info(derecho::DefaultSubgroupAllocator(
            {{std::type_index(typeid(ObjectUsingDeltaMap)),
              derecho::one_subgroup_policy(derecho::fixed_even_shards(1, num_nodes))}}));
    derecho::Group<ObjectUsingDeltaMap> group(subgroup_info, [](persistent::PersistentRegistry* registry, derecho::subgroup_id_t sgid) {
        return std::make_unique<ObjectUsingDeltaMap>(registry);
    });
    std::cout << "Constructed a Group<ObjectUsingDeltaMap>" << std::endl;

    Replicated<ObjectUsingDeltaMap>& object_handle = group.get_subgroup<ObjectUsingDeltaMap>();
    // Send some updates
    uint32_t my_id = group.get_my_id();
    persistent::version_t my_key_version = my_id + 10;
    persistent::version_t my_value_version = my_id + 100;
    std::cout << "Sending put(" << my_key_version << ", " << my_value_version << ")" << std::endl;
    object_handle.ordered_send<RPC_NAME(put)>(my_key_version, my_value_version);
    // Read the current state
    uint32_t target_id = my_id + 1 % num_nodes;
    QueryResults<persistent::version_t> results = object_handle.p2p_send<RPC_NAME(get)>(target_id, my_key_version);
    for(auto& result : results.get()) {
        std::cout << "get(" << my_key_version << ") reply from " << result.first << ": " << result.second.get() << std::endl;
    }
    group.barrier_sync();
    group.leave(true);
}

int main(int argc, char** argv) {
    derecho::Conf::initialize(argc, argv);
    std::string test_type(argv[1]);
    if(test_type == "standalone") {
        standalone_map_test();
    } else if(test_type == "group") {
        const uint32_t num_nodes = std::stoi(argv[2]);
        group_map_test(num_nodes);
    }
}
