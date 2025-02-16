#include "common/time.h"

#include <thread>
#include <vector>

#include <gtest/gtest.h>

namespace pond::common {

TEST(TimeTest, TimestampToString) {
    // Test timestamp at 1970-01-01 00:20:34.567890
    Timestamp timestamp = 1234567890LL;  // 1,234,567,890 microseconds since epoch
    std::string result = TimestampToString(timestamp);

    // Format: YYYY/MM/DD HH:MM:SS.UUUUUU
    EXPECT_EQ(result.substr(0, 4), "1970");       // Year
    EXPECT_EQ(result.substr(5, 2), "01");         // Month
    EXPECT_EQ(result.substr(8, 2), "01");         // Day
    EXPECT_EQ(result.substr(11, 8), "00:20:34");  // HH:MM:SS
    EXPECT_EQ(result.substr(20), "567890");       // Microseconds

    // Test zero timestamp (epoch)
    std::string zero_result = TimestampToString(0);
    EXPECT_EQ(zero_result.substr(0, 4), "1970");
    EXPECT_EQ(zero_result.substr(5, 2), "01");
    EXPECT_EQ(zero_result.substr(8, 2), "01");
    EXPECT_EQ(zero_result.substr(11, 8), "00:00:00");
    EXPECT_EQ(zero_result.substr(20), "000000");

    // Test 1 hour after epoch
    Timestamp one_hour = 3600000000LL;  // 1 hour in microseconds
    std::string hour_result = TimestampToString(one_hour);
    EXPECT_EQ(hour_result.substr(0, 4), "1970");
    EXPECT_EQ(hour_result.substr(5, 2), "01");
    EXPECT_EQ(hour_result.substr(8, 2), "01");
    EXPECT_EQ(hour_result.substr(11, 8), "01:00:00");
    EXPECT_EQ(hour_result.substr(20), "000000");
}

TEST(TimeTest, TimeIntervalToString) {
    // Test various time intervals
    EXPECT_EQ(TimeIntervalToString(0), "0s");

    // Test individual units
    EXPECT_EQ(TimeIntervalToString(1000000), "1s");      // 1 second
    EXPECT_EQ(TimeIntervalToString(60000000), "1m");     // 1 minute
    EXPECT_EQ(TimeIntervalToString(3600000000), "1h");   // 1 hour
    EXPECT_EQ(TimeIntervalToString(86400000000), "1d");  // 1 day

    // Test fractional seconds
    EXPECT_EQ(TimeIntervalToString(1234567), "1.234567s");

    // Test combination of units
    TimeInterval interval = 1234567890;
    std::string result = TimeIntervalToString(interval);
    EXPECT_EQ(result, "20m34.567890s");

    // Test with all units including microseconds
    interval = 90061234567LL;  // 25h1m1.234567s
    result = TimeIntervalToString(interval);
    EXPECT_EQ(result, "1d1h1m1.234567s");
}

TEST(HybridTimeTest, Construction) {
    // Test default constructor
    HybridTime t1;
    EXPECT_EQ(t1.encoded(), HybridTime::INVALID_TIME);

    // Test max hybrid time
    HybridTime t2 = MaxHybridTime();
    EXPECT_EQ(t2.encoded(), HybridTime::MAX_TIME);

    // Test explicit construction
    HybridTime t3(1000);
    EXPECT_EQ(t3.physical_time(), 1000);
    EXPECT_EQ(t3.encoded(), 1000);

    // Test encoded constructor
    uint64_t encoded = 2000;
    HybridTime t4(encoded);
    EXPECT_EQ(t4.physical_time(), 2000);
    EXPECT_EQ(t4.encoded(), 2000);
}

TEST(HybridTimeTest, Comparison) {
    HybridTime t1(1000);
    HybridTime t2(2000);
    HybridTime t3(3000);

    // Test equality
    EXPECT_EQ(t1, HybridTime(1000));
    EXPECT_NE(t1, t2);

    // Test ordering
    EXPECT_LT(t1, t2);
    EXPECT_LT(t2, t3);
    EXPECT_GT(t3, t1);
    EXPECT_LE(t1, t2);
    EXPECT_LE(t1, HybridTime(1000));
    EXPECT_GE(t3, t2);
}

TEST(HybridTimeTest, Limits) {
    // Test minimum time
    HybridTime t1(HybridTime::MIN_TIME);
    EXPECT_EQ(t1.physical_time(), HybridTime::MIN_TIME);

    // Test maximum time
    HybridTime t2(HybridTime::MAX_TIME);
    EXPECT_EQ(t2.physical_time(), HybridTime::MAX_TIME);
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

    // Get next timestamp with same physical time
    HybridTime t2 = manager.NextWithTime(FIXED_TIME);
    EXPECT_GT(t2.physical_time(), t1.physical_time());
    EXPECT_GT(t2, t1);

    // Get timestamp with increased physical time
    HybridTime t3 = manager.NextWithTime(FIXED_TIME + 1);
    EXPECT_EQ(t3.physical_time(), FIXED_TIME + 2);  // + 1 has been used already
    EXPECT_GT(t3, t2);
}

TEST_F(HybridTimeManagerTest, BackwardsClockHandling) {
    auto& manager = HybridTimeManager::Instance();
    const uint64_t FIXED_TIME = manager.Current().physical_time() + 1;

    // Get initial timestamp
    HybridTime t1 = manager.NextWithTime(FIXED_TIME);
    EXPECT_EQ(t1.physical_time(), FIXED_TIME);

    // Try to get timestamp with earlier physical time
    HybridTime t2 = manager.NextWithTime(FIXED_TIME - 100);
    EXPECT_GT(t2.physical_time(), t1.physical_time());  // Should increment based on steady clock
    EXPECT_GT(t2, t1);
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
