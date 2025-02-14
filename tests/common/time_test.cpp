#include "common/time.h"

#include <thread>
#include <vector>

#include <gtest/gtest.h>

namespace pond::common {

TEST(HybridTimeTest, Construction) {
    // Test default constructor
    HybridTime t1;
    EXPECT_EQ(t1.encoded(), HybridTime::INVALID_TIME);

    // Test max hybrid time
    HybridTime t2 = MaxHybridTime();
    EXPECT_EQ(t2.encoded(), HybridTime::MAX_TIME);

    // Test explicit construction
    HybridTime t3(1000, 42);
    EXPECT_EQ(t3.physical_time(), 1000);
    EXPECT_EQ(t3.logical_counter(), 42);

    // Test encoded constructor
    uint64_t encoded = (1000ULL << HybridTime::LOGICAL_BITS) | 42;
    HybridTime t4(encoded);
    EXPECT_EQ(t4.physical_time(), 1000);
    EXPECT_EQ(t4.logical_counter(), 42);
}

TEST(HybridTimeTest, Comparison) {
    HybridTime t1(1000, 1);
    HybridTime t2(1000, 2);
    HybridTime t3(1001, 0);

    // Test equality
    EXPECT_EQ(t1, HybridTime(1000, 1));
    EXPECT_NE(t1, t2);

    // Test ordering
    EXPECT_LT(t1, t2);
    EXPECT_LT(t2, t3);
    EXPECT_GT(t3, t1);
    EXPECT_LE(t1, t2);
    EXPECT_LE(t1, HybridTime(1000, 1));
    EXPECT_GE(t3, t2);
}

TEST(HybridTimeTest, Limits) {
    // Test maximum logical counter
    HybridTime t1(1000, HybridTime::MAX_LOGICAL);
    EXPECT_EQ(t1.logical_counter(), HybridTime::MAX_LOGICAL);

    // Test physical time masking
    uint64_t large_physical = (1ULL << HybridTime::PHYSICAL_BITS) - 1;
    HybridTime t2(large_physical, 42);
    EXPECT_EQ(t2.physical_time(), large_physical);
    EXPECT_EQ(t2.logical_counter(), 42);
}

class HybridTimeManagerTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Reset the singleton instance before each test
        HybridTimeManager::Instance().Current();
    }
};

TEST_F(HybridTimeManagerTest, MonotonicIncrease) {
    auto& manager = HybridTimeManager::Instance();
    const uint64_t FIXED_TIME = manager.Current().physical_time() + 1;

    // Get initial timestamp
    HybridTime t1 = manager.NextWithTime(FIXED_TIME);
    EXPECT_EQ(t1.physical_time(), FIXED_TIME);
    EXPECT_EQ(t1.logical_counter(), 0);

    // Get next timestamp with same physical time
    HybridTime t2 = manager.NextWithTime(FIXED_TIME);
    EXPECT_EQ(t2.physical_time(), FIXED_TIME);
    EXPECT_EQ(t2.logical_counter(), 1);
    EXPECT_GT(t2, t1);

    // Get timestamp with increased physical time
    HybridTime t3 = manager.NextWithTime(FIXED_TIME + 1);
    EXPECT_EQ(t3.physical_time(), FIXED_TIME + 1);
    EXPECT_EQ(t3.logical_counter(), 0);
    EXPECT_GT(t3, t2);
}

TEST_F(HybridTimeManagerTest, BackwardsClockHandling) {
    auto& manager = HybridTimeManager::Instance();
    const uint64_t FIXED_TIME = manager.Current().physical_time() + 1;

    // Get initial timestamp
    HybridTime t1 = manager.NextWithTime(FIXED_TIME);
    EXPECT_EQ(t1.physical_time(), FIXED_TIME);
    EXPECT_EQ(t1.logical_counter(), 0);

    // Try to get timestamp with earlier physical time
    HybridTime t2 = manager.NextWithTime(FIXED_TIME - 100);
    EXPECT_EQ(t2.physical_time(), FIXED_TIME);  // Should use previous physical time
    EXPECT_EQ(t2.logical_counter(), 1);         // Should increment logical counter
    EXPECT_GT(t2, t1);
}

TEST_F(HybridTimeManagerTest, LogicalOverflow) {
    auto& manager = HybridTimeManager::Instance();
    const uint64_t FIXED_TIME = manager.Current().physical_time() + 1;

    // Get initial timestamp
    HybridTime t1 = manager.NextWithTime(FIXED_TIME);
    EXPECT_EQ(t1.physical_time(), FIXED_TIME);
    EXPECT_EQ(t1.logical_counter(), 0);

    // Request timestamps until we reach MAX_LOGICAL
    HybridTime prev = t1;
    for (uint32_t i = 0; i < HybridTime::MAX_LOGICAL - 1; i++) {
        HybridTime current = manager.NextWithTime(FIXED_TIME);
        EXPECT_GT(current, prev);
        prev = current;
    }

    // Next timestamp should overflow to next physical time
    HybridTime overflow = manager.NextWithTime(FIXED_TIME);
    EXPECT_EQ(overflow.physical_time(), FIXED_TIME + 1);
    EXPECT_EQ(overflow.logical_counter(), 0);
    EXPECT_GT(overflow, prev);
}

TEST_F(HybridTimeManagerTest, PhysicalTimeIncrement) {
    auto& manager = HybridTimeManager::Instance();
    const uint64_t FIXED_TIME = manager.Current().physical_time() + 1;

    // Get initial timestamp
    HybridTime t1 = manager.NextWithTime(FIXED_TIME);
    EXPECT_EQ(t1.physical_time(), FIXED_TIME);

    // Small increment should be allowed
    HybridTime t2 = manager.NextWithTime(FIXED_TIME + 1);
    EXPECT_EQ(t2.physical_time(), FIXED_TIME + 1);

    // Large increment within MAX_PHYSICAL_INCREMENT should be allowed
    HybridTime t3 = manager.NextWithTime(FIXED_TIME + HybridTimeManager::MAX_PHYSICAL_INCREMENT);
    EXPECT_EQ(t3.physical_time(), FIXED_TIME + HybridTimeManager::MAX_PHYSICAL_INCREMENT);

    // Increment larger than MAX_PHYSICAL_INCREMENT should throw
    EXPECT_THROW(manager.NextWithTime(t3.physical_time() + HybridTimeManager::MAX_PHYSICAL_INCREMENT + 1),
                 std::runtime_error);
}

TEST_F(HybridTimeManagerTest, ConcurrentAccess) {
    auto& manager = HybridTimeManager::Instance();
    constexpr int NUM_THREADS = 4;
    constexpr int ITERATIONS = 1000;

    std::vector<std::vector<HybridTime>> thread_times(NUM_THREADS);
    std::vector<std::thread> threads;

    // Launch multiple threads to get timestamps concurrently
    for (int i = 0; i < NUM_THREADS; i++) {
        threads.emplace_back([&manager, i, &thread_times]() {
            for (int j = 0; j < ITERATIONS; j++) {
                thread_times[i].push_back(manager.Next());
            }
        });
    }

    // Wait for all threads to complete
    for (auto& thread : threads) {
        thread.join();
    }

    // Collect all timestamps
    std::vector<HybridTime> all_times;
    for (const auto& thread_time : thread_times) {
        all_times.insert(all_times.end(), thread_time.begin(), thread_time.end());
    }

    // Sort timestamps
    std::sort(all_times.begin(), all_times.end());

    // Verify uniqueness and monotonicity
    for (size_t i = 1; i < all_times.size(); i++) {
        EXPECT_LT(all_times[i - 1], all_times[i]) << "Duplicate or non-monotonic timestamps detected at index " << i;
    }
}

TEST_F(HybridTimeManagerTest, CurrentTimestamp) {
    auto& manager = HybridTimeManager::Instance();
    const uint64_t FIXED_TIME = manager.Current().physical_time() + 1;

    // Get initial timestamp
    HybridTime t1 = manager.NextWithTime(FIXED_TIME);

    // Current() should return the last allocated timestamp
    EXPECT_EQ(manager.Current().encoded(), t1.encoded());

    // Multiple Current() calls should return the same timestamp
    EXPECT_EQ(manager.Current().encoded(), manager.Current().encoded());

    // After Next(), Current() should return the new timestamp
    HybridTime t2 = manager.NextWithTime(FIXED_TIME);
    EXPECT_EQ(manager.Current().encoded(), t2.encoded());
}

}  // namespace pond::common
