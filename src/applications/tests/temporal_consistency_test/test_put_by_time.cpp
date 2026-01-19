#include <cascade/service_client_api.hpp>
#include <cascade/object.hpp>
#include <cascade/utils.hpp>
#include <cascade/service_types.hpp>
#include <derecho/utils/time.h>
#include <iostream>
#include <iomanip>
#include <chrono>
#include <thread>

using namespace derecho::cascade;

/**
 * Test client for put_by_time API
 * 
 * This program tests the new put_by_time functionality using explicit subgroup/shard indices.
 * Tests:
 * 1. Valid timestamp test - should succeed
 * 2. Invalid (too old) timestamp test - should be rejected
 * 3. Comparison with regular put - verify timestamps are different
 */
int main(int argc, char** argv) {
    try {
        // Initialize the Cascade client (singleton)
        auto& capi = ServiceClientAPI::get_service_client();
        
        std::cout << "=== Testing put_by_time API ===" << std::endl;
        std::cout << std::endl;

        // Use subgroup index 0, shard index 0 for testing
        uint32_t subgroup_index = 0;
        uint32_t shard_index = 0;

        // Test 1: put_by_time with valid timestamp (current time)
        std::cout << "Test 1: put_by_time with current timestamp" << std::endl;
        {
            ObjectWithStringKey obj;
            obj.key = "test_key_1";
            obj.blob = Blob(reinterpret_cast<const uint8_t*>("test_value_1"), 12);
            obj.previous_version = persistent::INVALID_VERSION;
            obj.previous_version_by_key = persistent::INVALID_VERSION;

            // Get current time in microseconds
            uint64_t current_time_us = get_walltime() / 1000ULL;

            auto result = capi.template put_by_time<VolatileCascadeStoreWithStringKey>(obj, current_time_us, subgroup_index, shard_index, false);
            
            // Wait for result
            for (auto& reply_future : result.get()) {
                auto reply = reply_future.second.get();
                std::cout << "  ✓ Success! Version: " << std::get<0>(reply) 
                          << ", Timestamp: " << std::get<1>(reply) << " microseconds" << std::endl;
            }
            std::cout << "  Testing Current time: " << current_time_us << " microseconds" << std::endl;
        }
        std::cout << std::endl;

        // Test 2: put_by_time with future timestamp (should work)
        std::cout << "Test 2: put_by_time with future timestamp (within delta)" << std::endl;
        {
            ObjectWithStringKey obj;
            obj.key = "test_key_2";
            obj.blob = Blob(reinterpret_cast<const uint8_t*>("test_value_2"), 12);
            obj.previous_version = persistent::INVALID_VERSION;
            obj.previous_version_by_key = persistent::INVALID_VERSION;

            // Use a timestamp 500ms in the future (within 1 second delta)
            // Calculate and call immediately to avoid terminal output delays consuming the margin
            uint64_t current_time_us = get_walltime() / 1000ULL;
            uint64_t future_time_us = current_time_us + 500000ULL; // 500ms = 500000 microseconds
            auto result = capi.template put_by_time<VolatileCascadeStoreWithStringKey>(obj, future_time_us, subgroup_index, shard_index, false);
            
            // Wait for result
            for (auto& reply_future : result.get()) {
                auto reply = reply_future.second.get();
                std::cout << "  ✓ Success! Version: " << std::get<0>(reply) 
                          << ", Timestamp: " << std::get<1>(reply) << " microseconds" << std::endl;
            }

            // Print diagnostic info after the call
            std::cout << "  Testing Current time (at call): " << current_time_us << " microseconds" << std::endl;
            std::cout << "  Testing Future time: " << future_time_us << " microseconds" << std::endl;
        }
        std::cout << std::endl;

        // Test 3: put_by_time with too old timestamp (should be rejected)
        std::cout << "Test 3: put_by_time with too old timestamp (should be rejected)" << std::endl;
        {
            ObjectWithStringKey obj;
            obj.key = "test_key_3";
            obj.blob = Blob(reinterpret_cast<const uint8_t*>("test_value_3"), 12);
            obj.previous_version = persistent::INVALID_VERSION;
            obj.previous_version_by_key = persistent::INVALID_VERSION;

            // Use a timestamp 2 seconds in the past (older than 1 second delta)
            uint64_t current_time_us = get_walltime() / 1000ULL;
            uint64_t old_time_us = current_time_us - 2000000ULL; // 2 seconds = 2000000 microseconds
            try {
                auto result = capi.template put_by_time<VolatileCascadeStoreWithStringKey>(obj, old_time_us, subgroup_index, shard_index, false);
                // If we get here, the call didn't throw - check if result is empty
                bool has_results = false;
                for (auto& reply_future : result.get()) {
                    has_results = true;
                    auto reply = reply_future.second.get();
                    std::cout << "  ✗ Unexpected success! Version: " << std::get<0>(reply) 
                              << ", Timestamp: " << std::get<1>(reply) << " microseconds" << std::endl;
                }
                if (!has_results) {
                    std::cout << "  ✓ Correctly rejected (empty result)" << std::endl;
                }
            } catch (const derecho::derecho_exception& e) {
                std::cout << "  ✓ Correctly rejected with exception: " << e.what() << std::endl;
            }
            std::cout << "  Testing Current time: " << current_time_us << " microseconds" << std::endl;
            std::cout << "  Old time: " << old_time_us << " microseconds" << std::endl;
        }
        std::cout << std::endl;

        // Test 4: Compare put_by_time vs regular put
        std::cout << "Test 4: Compare put_by_time vs regular put" << std::endl;
        {
            // First, do a regular put
            ObjectWithStringKey obj1;
            obj1.key = "test_key_4a";
            obj1.blob = Blob(reinterpret_cast<const uint8_t*>("test_value_4a"), 13);
            obj1.previous_version = persistent::INVALID_VERSION;
            obj1.previous_version_by_key = persistent::INVALID_VERSION;

            std::cout << "  Regular put..." << std::endl;
            auto result1 = capi.template put<VolatileCascadeStoreWithStringKey>(obj1, subgroup_index, shard_index, false);
            uint64_t regular_put_timestamp = 0;
            for (auto& reply_future : result1.get()) {
                auto reply = reply_future.second.get();
                regular_put_timestamp = std::get<1>(reply);
                std::cout << "    Timestamp: " << regular_put_timestamp << " microseconds" << std::endl;
            }

            // Small delay to ensure different timestamps
            std::this_thread::sleep_for(std::chrono::milliseconds(10));

            // Now do put_by_time with a specific timestamp
            ObjectWithStringKey obj2;
            obj2.key = "test_key_4b";
            obj2.blob = Blob(reinterpret_cast<const uint8_t*>("test_value_4b"), 13);
            obj2.previous_version = persistent::INVALID_VERSION;
            obj2.previous_version_by_key = persistent::INVALID_VERSION;

            // Use a timestamp that's guaranteed to be higher than the current HLC
            // (regular_put_timestamp + 100ms to be safe)
            uint64_t custom_timestamp_us = regular_put_timestamp + 100000ULL; // 100ms ahead of regular put
            auto result2 = capi.template put_by_time<VolatileCascadeStoreWithStringKey>(obj2, custom_timestamp_us, subgroup_index, shard_index, false);
            uint64_t custom_put_timestamp = 0;
            for (auto& reply_future : result2.get()) {
                auto reply = reply_future.second.get();
                custom_put_timestamp = std::get<1>(reply);
                std::cout << "    Timestamp: " << custom_put_timestamp << " microseconds" << std::endl;
            }

            // Verify the custom timestamp was used
            if (custom_put_timestamp == custom_timestamp_us) {
                std::cout << "  ✓ Custom timestamp was correctly used!" << std::endl;
            } else {
                std::cout << "  ✗ Error: Custom timestamp mismatch. Expected: " << custom_timestamp_us 
                          << ", Got: " << custom_put_timestamp << std::endl;
            }
            std::cout << "  Testing put_by_time with custom timestamp: " << custom_timestamp_us << " microseconds" << std::endl;
            std::cout << "    (100ms ahead of regular put to ensure HLC advances)" << std::endl;
        }
        std::cout << std::endl;

        // Test 5: put_by_time with specific timestamp value
        std::cout << "Test 5: put_by_time with specific timestamp value" << std::endl;
        {
            ObjectWithStringKey obj;
            obj.key = "test_key_5";
            obj.blob = Blob(reinterpret_cast<const uint8_t*>("test_value_5"), 12);
            obj.previous_version = persistent::INVALID_VERSION;
            obj.previous_version_by_key = persistent::INVALID_VERSION;

            // Use a specific timestamp (e.g., 1000000000 microseconds = 1000 seconds)
            uint64_t specific_timestamp_us = 1000000000ULL;
            try {
                auto result = capi.template put_by_time<VolatileCascadeStoreWithStringKey>(obj, specific_timestamp_us, subgroup_index, shard_index, false);
                bool has_results = false;
                for (auto& reply_future : result.get()) {
                    has_results = true;
                    auto reply = reply_future.second.get();
                    std::cout << "  Result - Version: " << std::get<0>(reply) 
                              << ", Timestamp: " << std::get<1>(reply) << " microseconds" << std::endl;
                }
                if (!has_results) {
                    std::cout << "  Timestamp was rejected (empty result)" << std::endl;
                }
            } catch (const derecho::derecho_exception& e) {
                std::cout << "  Timestamp was rejected with exception: " << e.what() << std::endl;
            }
            std::cout << "  Using specific timestamp: " << specific_timestamp_us << " microseconds" << std::endl;
            std::cout << "  Note: This will be rejected if it's too old" << std::endl;
        }
        std::cout << std::endl;

        std::cout << "=== All tests completed ===" << std::endl;
        return 0;

    } catch (const std::exception& e) {
        std::cerr << "Exception: " << e.what() << std::endl;
        return -1;
    }
}

