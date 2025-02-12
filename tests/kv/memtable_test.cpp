#include "kv/memtable.h"

#include <random>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "kv/kv_entry.h"

namespace pond::kv {

class MemTableTest : public ::testing::Test {
protected:
    void SetUp() override { table = std::make_unique<MemTable>(); }

    std::unique_ptr<MemTable> table;

    common::DataChunk CreateTestValue(const std::string& str) {
        return common::DataChunk(reinterpret_cast<const uint8_t*>(str.data()), str.size());
    }
};

TEST_F(MemTableTest, BasicOperations) {
    // Test Put
    auto value = CreateTestValue("value1");
    auto result = table->Put("key1", value);
    EXPECT_TRUE(result.ok());

    // Test Get
    auto get_result = table->Get("key1");
    EXPECT_TRUE(get_result.ok());
    auto retrieved = std::move(get_result).value();
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(retrieved.Data()), retrieved.Size()), "value1");

    // Test non-existent key
    get_result = table->Get("key2");
    EXPECT_FALSE(get_result.ok());
}

TEST_F(MemTableTest, SizeLimits) {
    std::string large_key(MAX_KEY_SIZE + 1, 'x');
    auto value = CreateTestValue("value");

    // Test key size limit
    auto result = table->Put(large_key, value);
    EXPECT_FALSE(result.ok());

    // Test value size limit
    std::string large_value_str(MAX_VALUE_SIZE + 1, 'x');
    auto large_value = CreateTestValue(large_value_str);
    result = table->Put("key1", large_value);
    EXPECT_FALSE(result.ok());
}

TEST_F(MemTableTest, MemoryUsage) {
    const size_t initial_usage = table->ApproximateMemoryUsage();

    // Add entries and check memory usage increases
    auto value = CreateTestValue("value1");
    table->Put("key1", value);
    EXPECT_GT(table->ApproximateMemoryUsage(), initial_usage);

    // Add more entries until we reach the limit
    std::string large_value(1024 * 1024, 'x');  // 1MB value
    auto large_chunk = CreateTestValue(large_value);
    for (int i = 0; i < 64; i++) {  // Should fill up a 64MB memtable
        auto result = table->Put("key" + std::to_string(i), large_chunk);
        if (!result.ok()) {
            EXPECT_TRUE(table->ShouldFlush());
            break;
        }
    }
}

TEST_F(MemTableTest, IteratorBasicOperations) {
    // Insert some entries
    std::vector<std::string> keys = {"key1", "key2", "key3", "key4", "key5"};

    for (size_t i = 0; i < keys.size(); i++) {
        auto value = CreateTestValue("value" + std::to_string(i));
        EXPECT_TRUE(table->Put(keys[i], value).ok());
    }

    // Test iterator
    auto it = table->NewIterator();
    size_t count = 0;

    for (; it->Valid();) {
        auto key_result = it->key();
        EXPECT_TRUE(key_result.ok());
        EXPECT_EQ(key_result.value(), keys[count]);

        auto value_result = it->value();
        EXPECT_TRUE(value_result.ok());
        const common::DataChunk& value = value_result.value().get();
        EXPECT_EQ(std::string(reinterpret_cast<const char*>(value.Data()), value.Size()),
                  "value" + std::to_string(count));

        EXPECT_TRUE(it->Next().ok());
        count++;
    }
    EXPECT_EQ(count, keys.size());

    // Test seek
    auto seek_result = it->Seek("key3");
    EXPECT_TRUE(seek_result.ok());
    EXPECT_TRUE(it->Valid());
    auto key_result = it->key();
    EXPECT_TRUE(key_result.ok());
    EXPECT_EQ(key_result.value(), "key3");
}

TEST_F(MemTableTest, IteratorErrorHandling) {
    // Test operations on invalid iterator
    auto it = table->NewIterator();
    EXPECT_FALSE(it->Valid());

    // Test Next() on invalid iterator
    auto next_result = it->Next();
    EXPECT_FALSE(next_result.ok());
    EXPECT_EQ(next_result.error().code(), common::ErrorCode::InvalidOperation);

    // Test key() on invalid iterator
    auto key_result = it->key();
    EXPECT_FALSE(key_result.ok());
    EXPECT_EQ(key_result.error().code(), common::ErrorCode::InvalidOperation);

    // Test value() on invalid iterator
    auto value_result = it->value();
    EXPECT_FALSE(value_result.ok());
    EXPECT_EQ(value_result.error().code(), common::ErrorCode::InvalidOperation);

    // Test Seek with empty key
    auto seek_result = it->Seek("");
    EXPECT_FALSE(seek_result.ok());
    EXPECT_EQ(seek_result.error().code(), common::ErrorCode::InvalidArgument);
}

TEST_F(MemTableTest, IteratorConcurrency) {
    // Insert some entries
    std::vector<std::string> keys = {"key1", "key2", "key3", "key4", "key5"};
    for (size_t i = 0; i < keys.size(); i++) {
        auto value = CreateTestValue("value" + std::to_string(i));
        EXPECT_TRUE(table->Put(keys[i], value).ok());
    }

    // Create multiple threads that use the same iterator
    auto it = table->NewIterator();
    std::vector<std::thread> threads;
    std::atomic<int> successful_operations(0);

    for (int i = 0; i < 4; i++) {
        threads.emplace_back([&]() {
            for (int j = 0; j < 100; j++) {
                // Randomly choose between Next, Seek, and reading operations
                int op = j % 3;
                switch (op) {
                    case 0: {
                        auto next_result = it->Next();
                        if (next_result.ok())
                            successful_operations++;
                        break;
                    }
                    case 1: {
                        auto seek_result = it->Seek(keys[j % keys.size()]);
                        if (seek_result.ok())
                            successful_operations++;
                        break;
                    }
                    case 2: {
                        if (it->Valid()) {
                            auto key_result = it->key();
                            auto value_result = it->value();
                            if (key_result.ok() && value_result.ok())
                                successful_operations++;
                        }
                        break;
                    }
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    // Verify that some operations were successful
    EXPECT_GT(successful_operations.load(), 0);
}

TEST_F(MemTableTest, ConcurrentOperations) {
    const int num_threads = 4;
    const int num_ops = 1000;
    std::vector<std::thread> threads;

    // Concurrent insertions
    for (int i = 0; i < num_threads; i++) {
        threads.emplace_back([&, i]() {
            for (int j = 0; j < num_ops; j++) {
                std::string key = "key" + std::to_string(i) + "_" + std::to_string(j);
                auto value = CreateTestValue("value" + std::to_string(j));
                auto result = table->Put(key, value);
                if (!result.ok()) {
                    // Stop if memtable is full
                    break;
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    // Verify some entries
    for (int i = 0; i < num_threads; i++) {
        for (int j = 0; j < 10; j++) {  // Check first 10 entries from each thread
            std::string key = "key" + std::to_string(i) + "_" + std::to_string(j);
            auto result = table->Get(key);
            if (result.ok()) {
                auto value = std::move(result).value();
                EXPECT_EQ(std::string(reinterpret_cast<const char*>(value.Data()), value.Size()),
                          "value" + std::to_string(j));
            }
        }
    }
}

TEST_F(MemTableTest, DeleteOperations) {
    // Insert and then delete
    auto value = CreateTestValue("value1");
    EXPECT_TRUE(table->Put("key1", value).ok());
    EXPECT_TRUE(table->Delete("key1").ok());

    // Get should return NotFound
    auto get_result = table->Get("key1");
    EXPECT_FALSE(get_result.ok());
    EXPECT_EQ(get_result.error().code(), common::ErrorCode::NotFound);

    // Delete non-existent key should succeed
    EXPECT_TRUE(table->Delete("key2").ok());
}

}  // namespace pond::kv