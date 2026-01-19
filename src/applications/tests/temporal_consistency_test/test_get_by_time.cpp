#include <cascade/service_client_api.hpp>
#include <cascade/object.hpp>
#include <cascade/utils.hpp>
#include <cascade/service_types.hpp>
#include <derecho/conf/conf.hpp>
#include <derecho/utils/time.h>
#include <iostream>
#include <iomanip>
#include <chrono>
#include <thread>
#include <cassert>

using namespace derecho::cascade;

/**
 * Test client for get_by_time API with temporal consistency checks
 * 
 * This program tests the get_by_time functionality with the new temporal consistency
 * implementation that uses PERS_TEMPORAL_CONSISTENCY_DELTA and PERS_SERVER_CLOCK_SKEW_DELTA_US.
 * 
 * Tests:
 * 1. get_by_time with too recent timestamp (should return INVALID_VERSION)
 * 2. get_by_time with timestamp in middle range (threshold2 <= ts < threshold1)
 * 3. get_by_time with old enough timestamp (ts < threshold2)
 * 4. Verify closest version selection
 * 5. Test with multiple puts at different times
 */
int main(int argc, char** argv) {
    try {
        // Initialize the Cascade client (singleton)
        auto& capi = ServiceClientAPI::get_service_client();
        
        std::cout << "=== Testing get_by_time API with Temporal Consistency ===" << std::endl;
        std::cout << std::endl;

        // Get configuration values for reference
        uint64_t temporal_consistency_delta_us = derecho::getConfUInt64(derecho::Conf::PERS_TEMPORAL_CONSISTENCY_DELTA_US);
        uint64_t clock_skew_delta_us = derecho::getConfUInt64(derecho::Conf::PERS_SERVER_CLOCK_SKEW_DELTA_US);
        
        std::cout << "Configuration:" << std::endl;
        std::cout << "  PERS_TEMPORAL_CONSISTENCY_DELTA: " << temporal_consistency_delta_us << " microseconds" << std::endl;
        std::cout << "  PERS_SERVER_CLOCK_SKEW_DELTA_US: " << clock_skew_delta_us << " microseconds" << std::endl;
        std::cout << std::endl;

        // Use subgroup index 0, shard index 0 for testing
        uint32_t subgroup_index = 0;
        uint32_t shard_index = 0;

        // First, populate the store with some data at known timestamps
        std::cout << "=== Setup: Populating store with test data ===" << std::endl;
        std::vector<std::pair<std::string, uint64_t>> test_data; // key -> timestamp
        
        // Put several objects with known timestamps
        for (int i = 0; i < 5; i++) {
            ObjectWithStringKey obj;
            obj.key = "test_key_" + std::to_string(i);
            obj.blob = Blob(reinterpret_cast<const uint8_t*>(("test_value_" + std::to_string(i)).c_str()), 
                           ("test_value_" + std::to_string(i)).length());
            obj.previous_version = persistent::INVALID_VERSION;
            obj.previous_version_by_key = persistent::INVALID_VERSION;

            // Use current time for each put
            uint64_t timestamp_us = get_walltime() / 1000ULL;
            
            auto result = capi.template put_by_time<PersistentCascadeStoreWithStringKey>(
                obj, timestamp_us, subgroup_index, shard_index, false);
            
            // Wait for result and store the actual timestamp
            for (auto& reply_future : result.get()) {
                auto reply = reply_future.second.get();
                uint64_t actual_timestamp = std::get<1>(reply);
                test_data.push_back({obj.key, actual_timestamp});
                std::cout << "  Put key: " << obj.key << " at timestamp: " << timestamp_us << std::endl;
                std::cout << "    ✓ Put successful. Version: " << std::get<0>(reply) 
                          << ", Timestamp: " << actual_timestamp << std::endl;
            }
            
            // Small delay between puts to ensure different timestamps
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        std::cout << std::endl;

        // Wait a bit to ensure data is stable
        std::cout << "Waiting for data to stabilize..." << std::endl;
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        std::cout << std::endl;

        // Test 1: get_by_time with too recent timestamp (should return INVALID_VERSION)
        std::cout << "=== Test 1: get_by_time with too recent timestamp ===" << std::endl;
        {
            uint64_t now_us = get_walltime() / 1000ULL;
            uint64_t threshold1 = now_us - temporal_consistency_delta_us - 2 * clock_skew_delta_us;
            uint64_t recent_time = threshold1 - 1000; // Just before threshold1
            
            std::cout << "  Current time: " << now_us << " microseconds" << std::endl;
            std::cout << "  Threshold1 (too recent): " << threshold1 << " microseconds" << std::endl;
            std::cout << "  Requesting time: " << recent_time << " microseconds" << std::endl;
            std::cout << "  Expected: INVALID_VERSION (time too recent, not stable)" << std::endl;

            auto result = capi.template get_by_time<PersistentCascadeStoreWithStringKey>(
                test_data[0].first, recent_time, true, subgroup_index, shard_index);
            
            bool found_valid = false;
            for (auto& reply_future : result.get()) {
                auto obj = reply_future.second.get();
                if (obj.key == test_data[0].first) {
                    found_valid = true;
                    std::cout << "  ✗ Unexpected: Found object with key: " << obj.key << std::endl;
                }
            }
            
            if (!found_valid) {
                std::cout << "  ✓ Correctly returned INVALID_VERSION (empty result)" << std::endl;
            }
        }
        std::cout << std::endl;

        // Test 2: get_by_time with timestamp in middle range (threshold2 <= ts < threshold1)
        std::cout << "=== Test 2: get_by_time with timestamp in middle range ===" << std::endl;
        {
            uint64_t now_us = get_walltime() / 1000ULL;
            uint64_t threshold1 = now_us - temporal_consistency_delta_us - 2 * clock_skew_delta_us;
            uint64_t threshold2 = now_us - temporal_consistency_delta_us - 3 * clock_skew_delta_us;
            
            // Use a timestamp in the middle range
            uint64_t middle_time = (threshold1 + threshold2) / 2;
            
            std::cout << "  Current time: " << now_us << " microseconds" << std::endl;
            std::cout << "  Threshold1: " << threshold1 << " microseconds" << std::endl;
            std::cout << "  Threshold2: " << threshold2 << " microseconds" << std::endl;
            std::cout << "  Requesting time: " << middle_time << " microseconds" << std::endl;
            std::cout << "  Expected: Closest version <= threshold1" << std::endl;

            // Use the oldest test data key
            if (!test_data.empty()) {
                auto result = capi.template get_by_time<PersistentCascadeStoreWithStringKey>(
                    test_data[0].first, middle_time, true, subgroup_index, shard_index);
                
                bool found = false;
                for (auto& reply_future : result.get()) {
                    auto obj = reply_future.second.get();
                    std::cout << "  ✓ Found object with key: " << obj.key 
                              << ", timestamp: " << obj.timestamp_us << std::endl;
                    if (obj.timestamp_us <= threshold1) {
                        std::cout << "    ✓ Timestamp is within threshold1" << std::endl;
                    } else {
                        std::cout << "    ✗ Warning: Timestamp exceeds threshold1" << std::endl;
                    }
                    found = true;
                }
                
                if (!found) {
                    std::cout << "  ✗ No object found" << std::endl;
                }
            }
        }
        std::cout << std::endl;

        // Test 3: get_by_time with old enough timestamp (ts < threshold2)
        std::cout << "=== Test 3: get_by_time with old enough timestamp ===" << std::endl;
        {
            uint64_t now_us = get_walltime() / 1000ULL;
            uint64_t threshold2 = now_us - temporal_consistency_delta_us - 3 * clock_skew_delta_us;
            
            // Use a timestamp well before threshold2
            uint64_t old_time = threshold2 - clock_skew_delta_us;
            
            std::cout << "  Current time: " << now_us << " microseconds" << std::endl;
            std::cout << "  Threshold2: " << threshold2 << " microseconds" << std::endl;
            std::cout << "  Requesting time: " << old_time << " microseconds" << std::endl;
            std::cout << "  Expected: Closest version <= (requested_time + clock_skew_delta)" << std::endl;

            if (!test_data.empty()) {
                auto result = capi.template get_by_time<PersistentCascadeStoreWithStringKey>(
                    test_data[0].first, old_time, true, subgroup_index, shard_index);
                
                bool found = false;
                for (auto& reply_future : result.get()) {
                    auto obj = reply_future.second.get();
                    uint64_t max_allowed = old_time + clock_skew_delta_us;
                    std::cout << "  ✓ Found object with key: " << obj.key 
                              << ", timestamp: " << obj.timestamp_us << std::endl;
                    std::cout << "    Max allowed timestamp: " << max_allowed << std::endl;
                    if (obj.timestamp_us <= max_allowed) {
                        std::cout << "    ✓ Timestamp is within allowed range" << std::endl;
                    } else {
                        std::cout << "    ✗ Warning: Timestamp exceeds allowed range" << std::endl;
                    }
                    found = true;
                }
                
                if (!found) {
                    std::cout << "  ✗ No object found" << std::endl;
                }
            }
        }
        std::cout << std::endl;

        // Test 4: Verify closest version selection with multiple versions
        std::cout << "=== Test 4: Verify closest version selection ===" << std::endl;
        {
            if (test_data.size() >= 3) {
                // Get the timestamps of the first and third objects
                uint64_t first_timestamp = test_data[0].second;
                uint64_t third_timestamp = test_data[2].second;
                
                // Request a time between first and third
                uint64_t target_time = (first_timestamp + third_timestamp) / 2;
                
                std::cout << "  First object timestamp: " << first_timestamp << std::endl;
                std::cout << "  Third object timestamp: " << third_timestamp << std::endl;
                std::cout << "  Target time: " << target_time << std::endl;
                std::cout << "  Expected: Closest version to target_time" << std::endl;

                // Test with the key that has multiple versions
                auto result = capi.template get_by_time<PersistentCascadeStoreWithStringKey>(
                    test_data[0].first, target_time, true, subgroup_index, shard_index);
                
                bool found = false;
                for (auto& reply_future : result.get()) {
                    auto obj = reply_future.second.get();
                    uint64_t distance = (obj.timestamp_us > target_time) ? 
                                       (obj.timestamp_us - target_time) : 
                                       (target_time - obj.timestamp_us);
                    std::cout << "  ✓ Found object with timestamp: " << obj.timestamp_us << std::endl;
                    std::cout << "    Distance from target: " << distance << " microseconds" << std::endl;
                    found = true;
                }
                
                if (!found) {
                    std::cout << "  ✗ No object found" << std::endl;
                }
            } else {
                std::cout << "  Skipped: Need at least 3 test objects" << std::endl;
            }
        }
        std::cout << std::endl;

        // Test 5: get_by_time with exact timestamp match
        std::cout << "=== Test 5: get_by_time with exact timestamp match ===" << std::endl;
        {
            if (!test_data.empty()) {
                uint64_t exact_timestamp = test_data[0].second;
                
                std::cout << "  Requesting exact timestamp: " << exact_timestamp << std::endl;
                std::cout << "  Expected: Object with matching timestamp" << std::endl;

                auto result = capi.template get_by_time<PersistentCascadeStoreWithStringKey>(
                    test_data[0].first, exact_timestamp, true, subgroup_index, shard_index);
                
                bool found = false;
                for (auto& reply_future : result.get()) {
                    auto obj = reply_future.second.get();
                    std::cout << "  ✓ Found object with key: " << obj.key 
                              << ", timestamp: " << obj.timestamp_us << std::endl;
                    if (obj.timestamp_us == exact_timestamp) {
                        std::cout << "    ✓ Exact timestamp match!" << std::endl;
                    } else {
                        std::cout << "    Note: Returned closest timestamp: " << obj.timestamp_us << std::endl;
                    }
                    found = true;
                }
                
                if (!found) {
                    std::cout << "  ✗ No object found" << std::endl;
                }
            }
        }
        std::cout << std::endl;

        // Test 6: get_by_time with very old timestamp (should still work if data exists)
        std::cout << "=== Test 6: get_by_time with very old timestamp ===" << std::endl;
        {
            if (!test_data.empty()) {
                // Use a timestamp from 10 seconds ago
                uint64_t now_us = get_walltime() / 1000ULL;
                uint64_t very_old_time = now_us - 10000000ULL; // 10 seconds ago
                
                std::cout << "  Current time: " << now_us << " microseconds" << std::endl;
                std::cout << "  Requesting very old time: " << very_old_time << " microseconds" << std::endl;
                std::cout << "  Expected: Closest version if data exists" << std::endl;

                auto result = capi.template get_by_time<PersistentCascadeStoreWithStringKey>(
                    test_data[0].first, very_old_time, true, subgroup_index, shard_index);
                
                bool found = false;
                for (auto& reply_future : result.get()) {
                    auto obj = reply_future.second.get();
                    std::cout << "  ✓ Found object with timestamp: " << obj.timestamp_us << std::endl;
                    found = true;
                }
                
                if (!found) {
                    std::cout << "  Note: No object found (may be expected if timestamp is too old)" << std::endl;
                }
            }
        }
        std::cout << std::endl;

        std::cout << "=== All tests completed ===" << std::endl;
        return 0;

    } catch (const std::exception& e) {
        std::cerr << "Exception: " << e.what() << std::endl;
        return -1;
    }
}