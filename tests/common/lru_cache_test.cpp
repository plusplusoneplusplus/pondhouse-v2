#include "common/lru_cache.h"

#include <gtest/gtest.h>

#include <string>
#include <thread>
#include <vector>

using namespace pond::common;

namespace {

// Test structure to verify memory calculation
struct TestItem {
    std::string data;
    size_t size() const { return data.size(); }
};

class LRUCacheTest : public ::testing::Test {
protected:
    // Cache with custom memory calculator
    LRUCache<std::string, TestItem> cache_{1024,  // 1KB cache
                                         [](const TestItem& item) { return item.size(); }};

    // Helper to create test items of specific sizes
    TestItem MakeItem(size_t size) {
        return TestItem{.data = std::string(size, 'x')};
    }
};

TEST_F(LRUCacheTest, BasicOperations) {
    // Test Put and Get
    ASSERT_TRUE(cache_.Put("key1", MakeItem(100)).ok());
    auto result = cache_.Get("key1");
    ASSERT_TRUE(result.ok());
    ASSERT_TRUE(result.value().has_value());
    ASSERT_EQ(result.value()->size(), 100);

    // Test non-existent key
    result = cache_.Get("key2");
    ASSERT_TRUE(result.ok());
    ASSERT_FALSE(result.value().has_value());

    // Test Remove
    ASSERT_TRUE(cache_.Remove("key1").ok());
    result = cache_.Get("key1");
    ASSERT_TRUE(result.ok());
    ASSERT_FALSE(result.value().has_value());
}

TEST_F(LRUCacheTest, MemoryLimit) {
    // Try to insert item larger than cache
    ASSERT_FALSE(cache_.Put("big", MakeItem(2048)).ok());  // 2KB > 1KB cache size

    // Fill cache to capacity
    ASSERT_TRUE(cache_.Put("key1", MakeItem(512)).ok());  // 512B
    ASSERT_TRUE(cache_.Put("key2", MakeItem(512)).ok());  // Another 512B

    // Verify both items are present
    ASSERT_TRUE(cache_.Get("key1").value().has_value());
    ASSERT_TRUE(cache_.Get("key2").value().has_value());

    // Add another item, should evict key1 (LRU)
    ASSERT_TRUE(cache_.Put("key3", MakeItem(512)).ok());
    ASSERT_FALSE(cache_.Get("key1").value().has_value());
    ASSERT_TRUE(cache_.Get("key2").value().has_value());
    ASSERT_TRUE(cache_.Get("key3").value().has_value());
}

TEST_F(LRUCacheTest, LRUEvictionOrder) {
    // Fill cache with items that will force eviction (900B total > 1KB)
    ASSERT_TRUE(cache_.Put("key1", MakeItem(300)).ok());
    ASSERT_TRUE(cache_.Put("key2", MakeItem(300)).ok());
    ASSERT_TRUE(cache_.Put("key3", MakeItem(300)).ok());

    // Access key1 to make it most recently used
    ASSERT_TRUE(cache_.Get("key1").value().has_value());
    // Access key3 to make it second most recently used
    ASSERT_TRUE(cache_.Get("key3").value().has_value());

    // Add new item (300B), should evict key2 (least recently used)
    ASSERT_TRUE(cache_.Put("key4", MakeItem(300)).ok());
    
    // Verify eviction order:
    // key1 - most recently used, should exist
    // key2 - least recently used, should be evicted
    // key3 - second most recently used, should exist
    // key4 - just added, should exist
    ASSERT_TRUE(cache_.Get("key1").value().has_value());
    ASSERT_FALSE(cache_.Get("key2").value().has_value());
    ASSERT_TRUE(cache_.Get("key3").value().has_value());
    ASSERT_TRUE(cache_.Get("key4").value().has_value());
}

TEST_F(LRUCacheTest, UpdateExisting) {
    // Insert initial item
    TestItem item1 = MakeItem(100);
    ASSERT_TRUE(cache_.Put("key1", item1).ok());
    
    // Update with larger size
    TestItem item2 = MakeItem(200);
    ASSERT_TRUE(cache_.Put("key1", item2).ok());
    
    auto result = cache_.Get("key1");
    ASSERT_TRUE(result.ok());
    ASSERT_TRUE(result.value().has_value());
    ASSERT_EQ(result.value()->data.size(), 200);

    // Verify memory tracking
    auto stats = cache_.GetStats();
    ASSERT_EQ(stats.current_memory_bytes, 200);
}

TEST_F(LRUCacheTest, ConcurrentAccess) {
    const int num_threads = 4;
    const int ops_per_thread = 1000;
    std::vector<std::thread> threads;

    for (int i = 0; i < num_threads; i++) {
        threads.emplace_back([&, i]() {
            for (int j = 0; j < ops_per_thread; j++) {
                std::string key = "key" + std::to_string(i) + "_" + std::to_string(j);
                
                // Mix of operations
                switch (j % 3) {
                    case 0:  // Put
                        cache_.Put(key, MakeItem(50));
                        break;
                    case 1:  // Get
                        cache_.Get(key);
                        break;
                    case 2:  // Remove
                        cache_.Remove(key);
                        break;
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    // Verify cache is still functional
    ASSERT_TRUE(cache_.Put("test_key", MakeItem(100)).ok());
    auto result = cache_.Get("test_key");
    ASSERT_TRUE(result.ok());
    ASSERT_TRUE(result.value().has_value());
    ASSERT_EQ(result.value()->size(), 100);
}

TEST_F(LRUCacheTest, StatsTracking) {
    // Test hits
    ASSERT_TRUE(cache_.Put("key1", MakeItem(100)).ok());
    cache_.Get("key1");
    cache_.Get("key1");

    // Test misses
    cache_.Get("missing1");
    cache_.Get("missing2");

    // Test evictions
    for (int i = 0; i < 10; i++) {
        cache_.Put("fill" + std::to_string(i), MakeItem(200));
    }

    auto stats = cache_.GetStats();
    EXPECT_EQ(stats.hits, 2);
    EXPECT_EQ(stats.misses, 2);
    EXPECT_GT(stats.evictions, 0);
    EXPECT_EQ(stats.hit_rate(), 0.5);
    EXPECT_LE(stats.current_memory_bytes, 1024);  // Should not exceed cache size
}

TEST_F(LRUCacheTest, Clear) {
    // Add some items
    ASSERT_TRUE(cache_.Put("key1", MakeItem(100)).ok());
    ASSERT_TRUE(cache_.Put("key2", MakeItem(200)).ok());

    // Clear cache
    ASSERT_TRUE(cache_.Clear().ok());

    // Verify everything is cleared
    auto stats = cache_.GetStats();
    EXPECT_EQ(stats.current_memory_bytes, 0);
    EXPECT_EQ(stats.hits, 0);
    EXPECT_EQ(stats.misses, 0);
    EXPECT_EQ(stats.evictions, 0);

    // Verify items are gone
    ASSERT_FALSE(cache_.Get("key1").value().has_value());
    ASSERT_FALSE(cache_.Get("key2").value().has_value());
}

}  // namespace
