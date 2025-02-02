#include "kv/sstable_cache.h"

#include <thread>
#include <vector>

#include <gtest/gtest.h>

using namespace pond::common;

namespace pond::kv {

namespace {

class SSTableCacheTest : public ::testing::Test {
protected:
    SSTableCache cache_{1024 * 1024};  // 1MB cache for testing
};

TEST_F(SSTableCacheTest, BasicOperations) {
    BlockKey key1{.sstable_id = 1, .block_offset = 0};
    BlockKey key2{.sstable_id = 1, .block_offset = 1024};
    
    // Create test blocks
    IndexBlock block1{.data = std::vector<uint8_t>(100, 'A')};
    IndexBlock block2{.data = std::vector<uint8_t>(200, 'B')};

    // Test Put and Get
    ASSERT_TRUE(cache_.PutBlock(key1, block1).ok());
    auto result = cache_.GetBlock(key1);
    ASSERT_TRUE(result.ok());
    ASSERT_TRUE(result.value().has_value());
    ASSERT_EQ(result.value()->data, block1.data);

    // Test non-existent key
    result = cache_.GetBlock(key2);
    ASSERT_TRUE(result.ok());
    ASSERT_FALSE(result.value().has_value());

    // Test Remove
    ASSERT_TRUE(cache_.Remove(key1).ok());
    result = cache_.GetBlock(key1);
    ASSERT_TRUE(result.ok());
    ASSERT_FALSE(result.value().has_value());
}

TEST_F(SSTableCacheTest, MemoryBasedEviction) {
    // Create blocks that will fill the cache
    const size_t block_size = 300 * 1024;  // 300KB blocks
    std::vector<IndexBlock> blocks;
    std::vector<BlockKey> keys;

    // Add 4 blocks (total 1.2MB, more than cache capacity)
    for (int i = 0; i < 4; i++) {
        BlockKey key{.sstable_id = 1, .block_offset = static_cast<BlockOffset>(i * block_size)};
        IndexBlock block{.data = std::vector<uint8_t>(block_size, static_cast<uint8_t>('A' + i))};
        
        keys.push_back(key);
        blocks.push_back(block);
        ASSERT_TRUE(cache_.PutBlock(keys[i], blocks[i]).ok());
    }

    // First block should be evicted
    auto result = cache_.GetBlock(keys[0]);
    ASSERT_TRUE(result.ok());
    ASSERT_FALSE(result.value().has_value());

    // Last block should still be present
    result = cache_.GetBlock(keys[3]);
    ASSERT_TRUE(result.ok());
    ASSERT_TRUE(result.value().has_value());
    ASSERT_EQ(result.value()->data, blocks[3].data);
}

TEST_F(SSTableCacheTest, CacheStats) {
    BlockKey key{.sstable_id = 1, .block_offset = 0};
    IndexBlock block{.data = std::vector<uint8_t>(100, 'A')};

    // Add an entry
    ASSERT_TRUE(cache_.PutBlock(key, block).ok());

    // Test hits
    cache_.GetBlock(key);
    cache_.GetBlock(key);

    // Test misses
    BlockKey missing_key{.sstable_id = 2, .block_offset = 0};
    cache_.GetBlock(missing_key);
    cache_.GetBlock(missing_key);

    // Verify stats
    auto stats = cache_.GetStats();
    EXPECT_EQ(stats.hits, 2);
    EXPECT_EQ(stats.misses, 2);
    EXPECT_EQ(stats.hit_rate(), 0.5);
    EXPECT_EQ(stats.current_memory_bytes, 100);  // Size of our test block
}

TEST_F(SSTableCacheTest, ConcurrentAccess) {
    const int num_threads = 4;
    const int ops_per_thread = 1000;
    std::vector<std::thread> threads;

    for (int i = 0; i < num_threads; i++) {
        threads.emplace_back([&, i]() {
            for (int j = 0; j < ops_per_thread; j++) {
                BlockKey key{.sstable_id = static_cast<SSTableId>(i), 
                            .block_offset = static_cast<BlockOffset>(j)};
                IndexBlock block{.data = std::vector<uint8_t>(100, static_cast<uint8_t>('A' + i))};

                // Mix of operations
                if (j % 3 == 0) {
                    cache_.PutBlock(key, block);
                } else if (j % 3 == 1) {
                    cache_.GetBlock(key);
                } else {
                    cache_.Remove(key);
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    // Verify cache is still functional
    BlockKey test_key{.sstable_id = 100, .block_offset = 0};
    IndexBlock test_block{.data = std::vector<uint8_t>(100, 'X')};
    ASSERT_TRUE(cache_.PutBlock(test_key, test_block).ok());
    auto result = cache_.GetBlock(test_key);
    ASSERT_TRUE(result.ok());
    ASSERT_TRUE(result.value().has_value());
    ASSERT_EQ(result.value()->data, test_block.data);
}

}  // namespace
}  // namespace pond::kv
