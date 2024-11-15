#include <cascade/delta_map.hpp>
#include <derecho/core/derecho.hpp>

#include <iostream>

using namespace derecho;
using derecho::cascade::DeltaMap;

class ObjectUsingDeltaMap : public mutils::ByteRepresentable,
                            public derecho::PersistsFields,
                            public derecho::GroupReference {
public:
    static int64_t INVALID_VALUE;

private:
    using derecho::GroupReference::group;
    persistent::Persistent<DeltaMap<int64_t, int64_t, &INVALID_VALUE>> number_map;
public:
    /** Standard persistent constructor */
    ObjectUsingDeltaMap(persistent::PersistentRegistry* registry) : number_map(registry) {}
    /** Deserialization/move constructor */
    ObjectUsingDeltaMap(persistent::Persistent<DeltaMap<int64_t, int64_t, &INVALID_VALUE>>& map)
            : number_map(std::move(map)) {}
    void put(int64_t key, int64_t value) {
        dbg_default_debug("ObjectUsingDeltaMap received put({}, {})", key, value);
        number_map->put(key, value);
    }
    int64_t get(int64_t key, persistent::version_t version = persistent::INVALID_VERSION) const;
    int64_t ordered_get(int64_t key) const {
        dbg_default_debug("ObjectUsingDeltaMap received ordered_get({})", key);
        return number_map->get(key);
    }
    std::map<int64_t, int64_t> get_all() const {
        return number_map->get_current_map();
    }
    void remove(int64_t key) {
        dbg_default_debug("ObjectUsingDeltaMap received remove({})", key);
        number_map->remove(key);
    }
    void batch_put(const std::list<std::pair<int64_t, int64_t>>& kv_pairs) {
        dbg_default_debug("ObjectUsingDeltaMap received batch_put({})", kv_pairs);
        for(const auto& pair : kv_pairs) {
            number_map->put(pair.first, pair.second);
        }
    }
    int64_t find_version_before(int64_t search_version) const {
        auto version_map_search = number_map->get_current_map().upper_bound(search_version);
        if(version_map_search != number_map->get_current_map().begin()) {
            version_map_search--;
            return version_map_search->second;
        } else {
            return INVALID_VALUE;
        }
    }

    DEFAULT_SERIALIZATION_SUPPORT(ObjectUsingDeltaMap, number_map);
    REGISTER_RPC_FUNCTIONS(ObjectUsingDeltaMap, ORDERED_TARGETS(put, ordered_get, remove, batch_put), P2P_TARGETS(get, get_all, find_version_before));
};

int64_t ObjectUsingDeltaMap::get(int64_t key, persistent::version_t version) const {
    dbg_default_debug("ObjectUsingDeltaMap received get({}, {})", key, version);
    // If the current version is requested, turn into an ordered_get
    if (version == persistent::INVALID_VERSION) {
        Replicated<ObjectUsingDeltaMap>& subgroup_handle = group->get_subgroup<ObjectUsingDeltaMap>();
        auto results = subgroup_handle.ordered_send<RPC_NAME(ordered_get)>(key);
        auto& replies = results.get();
        for(auto& reply_pair : replies) {
            reply_pair.second.wait();
        }
        return replies.begin()->second.get();
    } else {
        // This will reconstruct the map at the requested version using the stored deltas, so it is slow
        return number_map[version]->get(key);
    }
}


int64_t ObjectUsingDeltaMap::INVALID_VALUE = -1L;

void standalone_map_test() {
    static std::string empty_str = "";

    using TestDeltaMapType = DeltaMap<int, std::string, &empty_str>;
    persistent::PersistentRegistry standalone_pr(nullptr, typeid(ObjectUsingDeltaMap), 0, 0);
    persistent::Persistent<DeltaMap<int, std::string, &empty_str>> standalone_delta_map(
            std::make_unique<DeltaMap<int, std::string, &empty_str>>, "PersistentDeltaMap",
            &standalone_pr, false);
    // Test put with delta (put, then create delta with version, then save delta with persist)
    standalone_delta_map->put(1, "aaaaaaaaaaaaaaaaa");
    standalone_delta_map.version(1);
    standalone_delta_map.persist();
    standalone_delta_map->put(2, "bbbbbbbbbbbbbbbbb");
    standalone_delta_map.version(2);
    standalone_delta_map.persist();
    // Test get, which should read the current version and should not create a delta
    std::string one_value = standalone_delta_map->get(1);
    std::cout << "In-memory get(1): " << one_value << std::endl;
    assert(one_value == "aaaaaaaaaaaaaaaaa");
    standalone_delta_map.version(3);
    standalone_delta_map.persist();
    // Change key 2 in the current version, then test getDelta on a previous version
    standalone_delta_map->put(2, "xxxxxxxxxxxxxxxxx");
    standalone_delta_map.version(4);
    standalone_delta_map.persist();
    std::string two_value = standalone_delta_map->get(2);
    std::cout << "In-memory get(2): " << two_value << std::endl;
    assert(two_value == "xxxxxxxxxxxxxxxxx");
    standalone_delta_map.template getDelta<TestDeltaMapType::DeltaType>(
            2, true, [](const TestDeltaMapType::DeltaType& delta) {
                std::cout << "DeltaMap.getDelta at version 2: " << delta.objects << " - by lambda" << std::endl;
                assert(delta.objects.at(2) == "bbbbbbbbbbbbbbbbb");
            });
    auto delta_ptr = standalone_delta_map.template getDelta<TestDeltaMapType::DeltaType>(2, true);
    std::cout << "DeltaMap.getDelta at version 2 : " << delta_ptr->objects << " - by copy" << std::endl;
    assert(delta_ptr->objects.at(2) == "bbbbbbbbbbbbbbbbb");
    // Test getDeltaByIndex
    standalone_delta_map->put(3, "cccccccccccccccc");
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
        std::map<int, std::string> expected_map{{1, "aaaaaaaaaaaaaaaaa"}, {2, "bbbbbbbbbbbbbbbbb"}};
        assert(past_map.get_current_map() == expected_map);
    });
    // Test "slow" get-by-index, without delta
    std::cout << "Entire DeltaMap at index 2: " << standalone_delta_map.getByIndex(2)->get_current_map() << std::endl;
    standalone_delta_map.getByIndex(2, [](const TestDeltaMapType& past_map) {
        std::cout << "Entire DeltaMap at index 2, by lambda: " << past_map.get_current_map() << std::endl;
        std::map<int, std::string> expected_map{{1, "aaaaaaaaaaaaaaaaa"}, {2, "xxxxxxxxxxxxxxxxx"}};
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

    uint32_t my_id = group.get_my_id();
    Replicated<ObjectUsingDeltaMap>& object_handle = group.get_subgroup<ObjectUsingDeltaMap>();
    // Show the map's initial state, to test restarting from saved logs
    QueryResults<std::map<int64_t, int64_t>> initial_get_results = object_handle.p2p_send<RPC_NAME(get_all)>(my_id);
    std::map<int64_t, int64_t> initial_get_value = initial_get_results.get().get(my_id);
    std::cout << "Initial map value : {";
    for(const auto& pair : initial_get_value) {
        std::cout << "{" << pair.first << " => " << pair.second << "},";
    }
    std::cout << "}" << std::endl;
    // Send some updates
    int64_t my_key = my_id + 10;
    int64_t my_value = my_id + 100;
    const std::size_t num_updates = 1024;
    std::list<QueryResults<void>> put_query_results;
    for(std::size_t i = 0; i < num_updates; ++i) {
        std::cout << "Sending put(" << my_key << ", " << my_value + i << ")" << std::endl;
        put_query_results.push_back(object_handle.ordered_send<RPC_NAME(put)>(my_key, my_value + i));
    }
    // Wait for the last update to send, hoping it is also received shortly afterward
    put_query_results.back().get();
    // Read the current state from an adjacent node and test if it is equal to the last update
    uint32_t target_id = (my_id + 1) % num_nodes;
    QueryResults<int64_t> results = object_handle.p2p_send<RPC_NAME(get)>(target_id, my_key, persistent::INVALID_VERSION);
    // p2p_send should return only one response in the ReplyMap: the response from target_id
    int64_t read_result = results.get().get(target_id);
    std::cout << "get(" << my_key << ") reply from " << target_id << ": " << read_result << std::endl;
    int64_t expected_value = my_value + num_updates - 1;
    assert(read_result == expected_value);
    // Remove the key, then confirm it is gone
    std::cout << "Sending remove(" << my_key << ")" << std::endl;
    object_handle.ordered_send<RPC_NAME(remove)>(my_key);
    QueryResults<int64_t> removed_results = object_handle.p2p_send<RPC_NAME(get)>(my_id, my_key, persistent::INVALID_VERSION);
    auto removed_result_value = removed_results.get().get(my_id);
    std::cout << "get(" << my_key << ") at self returned " << removed_result_value << std::endl;
    assert(removed_result_value == ObjectUsingDeltaMap::INVALID_VALUE);
    // Wait for all nodes to finish their puts and removes
    group.barrier_sync();
    // Have the first node submit a batch update for all of the versions
    std::list<std::pair<int64_t, int64_t>> batch;
    for(uint32_t id = 0; id < num_nodes; ++id) {
        batch.push_back({id + 10, id + 100});
    }
    if(my_id == 0) {
        std::cout << "Sending batch_put(";
        for(const auto& pair : batch) {
            std::cout << "{" << pair.first << "," << pair.second << "}" << ",";
        }
        std::cout << ")" << std::endl;
        auto query_results = object_handle.ordered_send<RPC_NAME(batch_put)>(batch);
        query_results.get();
    }
    // Check to ensure the batch update was applied
    for(auto& kv_pair : batch) {
        QueryResults<int64_t> results = object_handle.p2p_send<RPC_NAME(get)>(target_id, kv_pair.first, persistent::INVALID_VERSION);
        int64_t result_value = results.get().get(target_id);
        std::cout << "get(" << kv_pair.first << ") at " << target_id << " returned " << result_value << std::endl;
    }
    // Get an old version. We'll have to guess at a version number; there ought to be at least num_nodes versions.
    QueryResults<int64_t> get_by_version_results = object_handle.p2p_send<RPC_NAME(get)>(target_id, my_key, num_nodes);
    int64_t get_by_version_value = get_by_version_results.get().get(target_id);
    std::cout << "get(" << my_key << ", " << num_nodes << ") returned " << get_by_version_value << std::endl;
    std::cout << "Done with the test" << std::endl;
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
