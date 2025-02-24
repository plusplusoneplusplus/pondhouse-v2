#include "kv/memtable.h"

#include <random>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "kv/kv_entry.h"
#include "test_helper.h"

namespace pond::kv {

class MemTableTest : public ::testing::Test {
protected:
    void SetUp() override {
        table = std::make_unique<MemTable>();
        current_txn_id = 1;
    }

    std::unique_ptr<MemTable> table;
    uint64_t current_txn_id;

    common::DataChunk CreateTestValue(const std::string& str) {
        return common::DataChunk(reinterpret_cast<const uint8_t*>(str.data()), str.size());
    }

    uint64_t NextTxnId() { return current_txn_id++; }
};

TEST_F(MemTableTest, BasicVersionedOperations) {
    auto txn1 = NextTxnId();
    auto txn2 = NextTxnId();
    auto read_time = common::HybridTimeManager::Instance().Next();

    // Test Put with first transaction
    auto value1 = CreateTestValue("value1");
    auto result = table->Put("key1", value1, txn1);
    VERIFY_RESULT(result);

    // Test Get with old read time
    auto get_result = table->Get("key1", read_time);
    EXPECT_FALSE(get_result.ok());
    EXPECT_EQ(get_result.error().code(), common::ErrorCode::NotFound);
    EXPECT_TRUE(get_result.error().message().find("No visible version") != std::string::npos);

    auto get_result2 = table->Get("key1", common::GetNextHybridTime());
    VERIFY_RESULT(get_result2);
    auto retrieved = std::move(get_result2).value();
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(retrieved.Data()), retrieved.Size()), "value1");

    // Test Get with transaction ID
    auto txn_get_result = table->GetForTxn("key1", txn1);
    VERIFY_RESULT(txn_get_result);
    auto txn_retrieved = std::move(txn_get_result).value();
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(txn_retrieved.Data()), txn_retrieved.Size()), "value1");

    // Test non-existent key
    auto get_result3 = table->Get("key2", read_time);
    VERIFY_ERROR_CODE(get_result3, common::ErrorCode::NotFound);
}

TEST_F(MemTableTest, VersionChainOperations) {
    auto txn1 = NextTxnId();
    auto txn2 = NextTxnId();

    // Create initial version
    auto value1 = CreateTestValue("value1");
    VERIFY_RESULT(table->Put("key1", value1, txn1));
    auto time1 = common::HybridTimeManager::Instance().Current();

    // Create second version
    auto value2 = CreateTestValue("value2");
    VERIFY_RESULT(table->Put("key1", value2, txn2));
    auto time2 = common::HybridTimeManager::Instance().Current();

    // Test visibility at different timestamps
    auto result1 = table->Get("key1", time1);
    VERIFY_RESULT(result1);
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(result1.value().Data()), result1.value().Size()), "value1");

    auto result2 = table->Get("key1", time2);
    VERIFY_RESULT(result2);
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(result2.value().Data()), result2.value().Size()), "value2");

    // Test transaction visibility
    auto txn1_result = table->GetForTxn("key1", txn1);
    VERIFY_RESULT(txn1_result);
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(txn1_result.value().Data()), txn1_result.value().Size()),
              "value1");

    auto txn2_result = table->GetForTxn("key1", txn2);
    VERIFY_RESULT(txn2_result);
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(txn2_result.value().Data()), txn2_result.value().Size()),
              "value2");
}

TEST_F(MemTableTest, DeletionOperations) {
    auto txn1 = NextTxnId();
    auto txn2 = NextTxnId();

    // Create initial version
    auto value1 = CreateTestValue("value1");
    VERIFY_RESULT(table->Put("key1", value1, txn1));
    auto time1 = common::HybridTimeManager::Instance().Current();

    // Delete the key
    VERIFY_RESULT(table->Delete("key1", txn2));
    auto time2 = common::HybridTimeManager::Instance().Current();

    // Test visibility before deletion
    auto result1 = table->Get("key1", time1);
    VERIFY_RESULT(result1);
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(result1.value().Data()), result1.value().Size()), "value1");

    // Test visibility after deletion
    auto result2 = table->Get("key1", time2);
    VERIFY_ERROR_CODE(result2, common::ErrorCode::NotFound);

    // Test transaction visibility
    auto txn1_result = table->GetForTxn("key1", txn1);
    VERIFY_RESULT(txn1_result);
    auto txn2_result = table->GetForTxn("key1", txn2);
    // key has been deleted, so this should fail
    VERIFY_ERROR_CODE(txn2_result, common::ErrorCode::NotFound);
}

TEST_F(MemTableTest, ConcurrentVersioning) {
    const int num_threads = 4;
    const int num_ops = 100;
    std::vector<std::thread> threads;
    std::atomic<uint64_t> next_txn_id(1);

    // Concurrent insertions with different transaction IDs
    for (int i = 0; i < num_threads; i++) {
        threads.emplace_back([&, i]() {
            for (int j = 0; j < num_ops; j++) {
                std::string key = "key" + std::to_string(i);
                auto value = CreateTestValue("value" + std::to_string(j));
                uint64_t txn_id = next_txn_id.fetch_add(1);
                auto result = table->Put(key, value, txn_id);
                if (!result.ok()) {
                    break;
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    // Verify version chains
    for (int i = 0; i < num_threads; i++) {
        std::string key = "key" + std::to_string(i);
        auto current_time = common::HybridTimeManager::Instance().Current();
        auto result = table->Get(key, current_time);
        VERIFY_RESULT(result);
    }
}

TEST_F(MemTableTest, SizeLimits) {
    auto txn_id = NextTxnId();
    std::string large_key(MAX_KEY_SIZE + 1, 'x');
    auto value = CreateTestValue("value");

    // Test key size limit
    auto result = table->Put(large_key, value, txn_id);
    VERIFY_ERROR_CODE(result, common::ErrorCode::InvalidArgument);

    // Test value size limit
    std::string large_value_str(MAX_VALUE_SIZE + 1, 'x');
    auto large_value = CreateTestValue(large_value_str);
    result = table->Put("key1", large_value, txn_id);
    VERIFY_ERROR_CODE(result, common::ErrorCode::InvalidArgument);
}

TEST_F(MemTableTest, MemoryUsage) {
    auto txn_id = NextTxnId();
    const size_t initial_usage = table->ApproximateMemoryUsage();

    // Add entries and check memory usage increases
    auto value = CreateTestValue("value1");
    table->Put("key1", value, txn_id);
    EXPECT_GT(table->ApproximateMemoryUsage(), initial_usage);

    // Add more entries until we reach the limit
    std::string large_value(1024 * 1024, 'x');  // 1MB value
    auto large_chunk = CreateTestValue(large_value);
    for (int i = 0; i < 64; i++) {  // Should fill up a 64MB memtable
        auto result = table->Put("key" + std::to_string(i), large_chunk, txn_id);
        if (!result.ok()) {
            EXPECT_TRUE(table->ShouldFlush());
            break;
        }
    }
}

TEST_F(MemTableTest, InvalidOperations) {
    auto txn_id = NextTxnId();
    auto value = CreateTestValue("test_value");

    // Test empty key
    auto result = table->Put("", value, txn_id);
    VERIFY_ERROR_CODE(result, common::ErrorCode::InvalidArgument);

    // Test Get with empty key
    auto get_result = table->Get("", common::HybridTimeManager::Instance().Current());
    VERIFY_ERROR_CODE(get_result, common::ErrorCode::InvalidArgument);

    // Test GetForTxn with empty key
    auto txn_get_result = table->GetForTxn("", txn_id);
    VERIFY_ERROR_CODE(txn_get_result, common::ErrorCode::InvalidArgument);

    // Test null value
    auto null_value = common::DataChunk(nullptr, 0);
    result = table->Put("key1", null_value, txn_id);
    VERIFY_ERROR_CODE(result, common::ErrorCode::InvalidArgument);
}

TEST_F(MemTableTest, IteratorBasicOperations) {
    // Insert some entries
    std::vector<std::string> keys = {"key1", "key2", "key3", "key4", "key5"};

    for (size_t i = 0; i < keys.size(); i++) {
        auto value = CreateTestValue("value" + std::to_string(i));
        VERIFY_RESULT(table->Put(keys[i], value, 0 /* txn_id */));
    }

    // Test iterator
    auto it = table->NewIterator();
    size_t count = 0;

    for (; it->Valid();) {
        auto key = it->key();
        EXPECT_EQ(key, keys[count]);

        const auto& versioned_value = it->value().get();
        const common::DataChunk& value = versioned_value->value();
        EXPECT_EQ(std::string(reinterpret_cast<const char*>(value.Data()), value.Size()),
                  "value" + std::to_string(count));

        it->Next();
        count++;
    }
    EXPECT_EQ(count, keys.size());

    // Test seek
    it->Seek("key3");
    EXPECT_TRUE(it->Valid());

    auto key = it->key();
    EXPECT_EQ(key, "key3");
}

TEST_F(MemTableTest, IteratorErrorHandling) {
    // Test operations on invalid iterator
    auto it = table->NewIterator();
    EXPECT_FALSE(it->Valid());

    // Test Next() on invalid iterator
    EXPECT_THROW(it->Next(), std::runtime_error);

    // Test key() on invalid iterator
    EXPECT_THROW(it->key(), std::runtime_error);

    // Test value() on invalid iterator
    EXPECT_THROW(it->value(), std::runtime_error);

    // Test Seek with empty key
    it->Seek("");
    EXPECT_FALSE(it->Valid());
}

TEST_F(MemTableTest, IteratorConcurrency) {
    // Insert some entries
    std::vector<std::string> keys = {"key1", "key2", "key3", "key4", "key5"};
    for (size_t i = 0; i < keys.size(); i++) {
        auto value = CreateTestValue("value" + std::to_string(i));
        VERIFY_RESULT(table->Put(keys[i], value, 0 /* txn_id */));
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
                        try {
                            it->Next();
                            if (it->Valid()) {
                                successful_operations++;
                            }
                        } catch (const std::runtime_error& e) {
                            // Do nothing
                        }
                        break;
                    }
                    case 1: {
                        try {
                            it->Seek(keys[j % keys.size()]);
                            if (it->Valid()) {
                                successful_operations++;
                            }
                        } catch (const std::runtime_error& e) {
                            // Do nothing
                        }
                        break;
                    }
                    case 2: {
                        try {
                            if (it->Valid()) {
                                auto key = it->key();
                                auto value = it->value();
                                successful_operations++;
                            }
                        } catch (const std::runtime_error& e) {
                            // Do nothing
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
                auto result = table->Put(key, value, 0 /* txn_id */);
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
            auto result = table->Get(key, common::GetNextHybridTime());
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
    VERIFY_RESULT(table->Put("key1", value, 0 /* txn_id */));
    VERIFY_RESULT(table->Delete("key1", 0 /* txn_id */));

    // Get should return NotFound
    auto get_result = table->Get("key1", common::GetNextHybridTime());
    EXPECT_FALSE(get_result.ok());
    EXPECT_EQ(get_result.error().code(), common::ErrorCode::NotFound);

    // Delete non-existent key should succeed
    VERIFY_RESULT(table->Delete("key2", 0 /* txn_id */));
}

//
// Test Setup:
//      Insert multiple key-value pairs and delete some of them
//      Verify iterator behavior with deleted records
// Test Result:
//      Iterator should skip deleted records
//      Iterator should show the correct version of records
//
TEST_F(MemTableTest, IteratorWithDeletedRecords) {
    auto txn1 = NextTxnId();
    auto txn2 = NextTxnId();

    // Insert initial records
    VERIFY_RESULT(table->Put("key1", CreateTestValue("value1"), txn1));
    VERIFY_RESULT(table->Put("key2", CreateTestValue("value2"), txn1));
    VERIFY_RESULT(table->Put("key3", CreateTestValue("value3"), txn1));

    // Delete key2
    VERIFY_RESULT(table->Delete("key2", txn2));

    // Create iterator and verify it skips deleted records
    auto it = table->NewIterator();
    std::vector<std::string> seen_keys;
    std::vector<std::string> seen_values;

    for (; it->Valid();) {
        auto key = it->key();
        const auto& versioned_value = it->value().get();
        const common::DataChunk& value = versioned_value->value();
        seen_keys.push_back(key);
        seen_values.push_back(std::string(reinterpret_cast<const char*>(value.Data()), value.Size()));
        it->Next();
    }

    // Should only see key1 and key3, key2 should be skipped
    ASSERT_EQ(seen_keys.size(), 2);
    EXPECT_EQ(seen_keys[0], "key1");
    EXPECT_EQ(seen_keys[1], "key3");
    EXPECT_EQ(seen_values[0], "value1");
    EXPECT_EQ(seen_values[1], "value3");

    // Verify seeking behavior with deleted records
    it->Seek("key2");
    EXPECT_TRUE(it->Valid());
    EXPECT_EQ(it->key(), "key3");  // Should skip key2 and land on key3
}

//
// Test Setup:
//      Insert multiple key-value pairs and delete some of them
//      Verify iterator behavior with IncludeTombstones mode
// Test Result:
//      Iterator should include deleted records when in IncludeTombstones mode
//      Iterator should show the correct version of records including tombstones
//
TEST_F(MemTableTest, IteratorWithIncludeTombstones) {
    auto txn1 = NextTxnId();
    auto txn2 = NextTxnId();

    // Insert initial records
    VERIFY_RESULT(table->Put("key1", CreateTestValue("value1"), txn1));
    VERIFY_RESULT(table->Put("key2", CreateTestValue("value2"), txn1));
    VERIFY_RESULT(table->Put("key3", CreateTestValue("value3"), txn1));

    // Delete key2
    VERIFY_RESULT(table->Delete("key2", txn2));

    // Create iterator with IncludeTombstones mode
    auto it = table->NewIterator(common::IteratorMode::IncludeTombstones);
    std::vector<std::string> seen_keys;
    std::vector<std::string> seen_values;
    std::vector<bool> is_tombstone;

    for (; it->Valid();) {
        auto key = it->key();
        const auto& versioned_value = it->value().get();
        seen_keys.push_back(key);
        is_tombstone.push_back(it->IsTombstone());

        if (!it->IsTombstone()) {
            const common::DataChunk& value = versioned_value->value();
            seen_values.push_back(std::string(reinterpret_cast<const char*>(value.Data()), value.Size()));
        } else {
            seen_values.push_back("");  // Empty string for tombstone
        }
        it->Next();
    }

    // Should see all keys including the deleted one
    ASSERT_EQ(seen_keys.size(), 3);
    EXPECT_EQ(seen_keys[0], "key1");
    EXPECT_EQ(seen_keys[1], "key2");
    EXPECT_EQ(seen_keys[2], "key3");

    // Verify values and tombstone status
    EXPECT_EQ(seen_values[0], "value1");
    EXPECT_EQ(seen_values[1], "");  // Tombstone
    EXPECT_EQ(seen_values[2], "value3");

    EXPECT_FALSE(is_tombstone[0]);
    EXPECT_TRUE(is_tombstone[1]);
    EXPECT_FALSE(is_tombstone[2]);

    // Verify seeking behavior with tombstones
    it->Seek("key2");
    EXPECT_TRUE(it->Valid());
    EXPECT_EQ(it->key(), "key2");  // Should find the tombstone
    EXPECT_TRUE(it->IsTombstone());
}

}  // namespace pond::kv