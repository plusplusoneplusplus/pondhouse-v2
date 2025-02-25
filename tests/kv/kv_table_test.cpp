#include "kv/kv_table.h"

#include <thread>

#include <gtest/gtest.h>

#include "common/memory_append_only_fs.h"
#include "test_helper.h"

namespace pond::kv {

class KvTableTest : public ::testing::Test {
protected:
    void SetUp() override {
        fs_ = std::make_shared<common::MemoryAppendOnlyFileSystem>();
        table_ = std::make_unique<KvTable>(fs_, "test_table");
    }

    common::DataChunk CreateTestValue(const std::string& str) {
        return common::DataChunk(reinterpret_cast<const uint8_t*>(str.data()), str.size());
    }

    size_t GetMemTableSize() const { return table_->active_memtable_->GetEntryCount(); }

    std::shared_ptr<common::MemoryAppendOnlyFileSystem> fs_;
    std::unique_ptr<KvTable> table_;
};

TEST_F(KvTableTest, BasicOperations) {
    // Test Put
    auto value = CreateTestValue("value1");
    auto result = table_->Put("key1", value);
    EXPECT_TRUE(result.ok());

    // Test Get
    auto get_result = table_->Get("key1");
    EXPECT_TRUE(get_result.ok());
    auto retrieved = std::move(get_result).value();
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(retrieved.Data()), retrieved.Size()), "value1");

    // Test non-existent key
    get_result = table_->Get("key2");
    EXPECT_FALSE(get_result.ok());
}

TEST_F(KvTableTest, DeleteOperations) {
    // Insert and then delete
    auto value = CreateTestValue("value1");
    EXPECT_TRUE(table_->Put("key1", value).ok());
    EXPECT_TRUE(table_->Delete("key1").ok());

    // Get should return NotFound
    auto get_result = table_->Get("key1");
    EXPECT_FALSE(get_result.ok());
    EXPECT_EQ(get_result.error().code(), common::ErrorCode::NotFound);

    // Delete non-existent key should succeed
    EXPECT_TRUE(table_->Delete("key2").ok());
}

TEST_F(KvTableTest, BatchOperations) {
    // Prepare test data
    std::vector<std::pair<std::string, common::DataChunk>> entries;
    std::vector<std::string> keys;
    for (int i = 0; i < 5; i++) {
        std::string key = "key" + std::to_string(i);
        auto value = CreateTestValue("value" + std::to_string(i));
        entries.emplace_back(key, value);
        keys.push_back(std::move(key));
    }

    // Test BatchPut
    auto batch_put_result = table_->BatchPut(entries);
    EXPECT_TRUE(batch_put_result.ok());

    // Test BatchGet
    auto batch_get_result = table_->BatchGet(keys);
    EXPECT_TRUE(batch_get_result.ok());
    auto results = std::move(batch_get_result).value();
    EXPECT_EQ(results.size(), 5);
    for (size_t i = 0; i < results.size(); i++) {
        EXPECT_TRUE(results[i].ok());
        auto value = std::move(results[i]).value();
        EXPECT_EQ(std::string(reinterpret_cast<const char*>(value.Data()), value.Size()), "value" + std::to_string(i));
    }

    // Test BatchDelete
    auto batch_delete_result = table_->BatchDelete(keys);
    EXPECT_TRUE(batch_delete_result.ok());

    // Verify all keys are deleted
    batch_get_result = table_->BatchGet(keys);
    EXPECT_TRUE(batch_get_result.ok());
    results = std::move(batch_get_result).value();
    for (const auto& result : results) {
        EXPECT_FALSE(result.ok());
        EXPECT_EQ(result.error().code(), common::ErrorCode::NotFound);
    }
}

TEST_F(KvTableTest, FlushAndRecovery) {
    // Insert some data
    for (int i = 0; i < 10; i++) {
        auto value = CreateTestValue("value" + std::to_string(i));
        VERIFY_RESULT_MSG(table_->Put("key" + std::to_string(i), value), "key" + std::to_string(i));
    }

    // Flush to SSTable
    VERIFY_RESULT_MSG(table_->Flush(), "Flush memtable");

    LOG_STATUS("Recovering table - 1");
    table_ = std::make_unique<KvTable>(fs_, "test_table");
    EXPECT_EQ(GetMemTableSize(), 10);

    VERIFY_RESULT_MSG(table_->RotateWAL(), "Rotate WAL");
    LOG_STATUS("Recovering table - 2");
    table_ = std::make_unique<KvTable>(fs_, "test_table");
    EXPECT_EQ(GetMemTableSize(), 0);

    // Insert more data
    for (int i = 10; i < 20; i++) {
        auto value = CreateTestValue("value" + std::to_string(i));
        VERIFY_RESULT_MSG(table_->Put("key" + std::to_string(i), value), "key" + std::to_string(i));
    }

    // Create a new table instance (simulating restart)
    LOG_STATUS("Recovering table - 3");
    table_ = std::make_unique<KvTable>(fs_, "test_table");
    EXPECT_EQ(GetMemTableSize(), 10);

    // Verify all data is recovered
    for (int i = 0; i < 20; i++) {
        auto result = table_->Get("key" + std::to_string(i));
        VERIFY_RESULT_MSG(result, "key" + std::to_string(i));
        auto value = std::move(result).value();
        EXPECT_EQ(std::string(reinterpret_cast<const char*>(value.Data()), value.Size()), "value" + std::to_string(i));
    }
}

TEST_F(KvTableTest, LargeValues) {
    // Test with values of different sizes
    std::vector<size_t> sizes = {1024, 64 * 1024, 1024 * 1024};  // 1KB, 64KB, 1MB

    for (size_t size : sizes) {
        std::string value_str(size, 'x');
        auto value = CreateTestValue(value_str);
        std::string key = "key_" + std::to_string(size);

        // Put
        auto put_result = table_->Put(key, value);
        EXPECT_TRUE(put_result.ok());

        // Get and verify
        auto get_result = table_->Get(key);
        EXPECT_TRUE(get_result.ok());
        auto retrieved = std::move(get_result).value();
        EXPECT_EQ(retrieved.Size(), size);
        EXPECT_EQ(std::string(reinterpret_cast<const char*>(retrieved.Data()), retrieved.Size()), value_str);
    }
}

TEST_F(KvTableTest, ConcurrentOperations) {
    const int num_threads = 4;
    const int num_ops = 1000;
    std::vector<std::thread> threads;

    // Concurrent insertions
    for (int i = 0; i < num_threads; i++) {
        threads.emplace_back([&, i]() {
            for (int j = 0; j < num_ops; j++) {
                std::string key = "key" + std::to_string(i) + "_" + std::to_string(j);
                auto value = CreateTestValue("value" + std::to_string(j));
                auto result = table_->Put(key, value);
                if (!result.ok()) {
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
            auto result = table_->Get(key);
            if (result.ok()) {
                auto value = std::move(result).value();
                EXPECT_EQ(std::string(reinterpret_cast<const char*>(value.Data()), value.Size()),
                          "value" + std::to_string(j));
            }
        }
    }
}

TEST_F(KvTableTest, IteratorBasic) {
    // Insert data into memtable
    VERIFY_RESULT(table_->Put("key1", CreateTestValue("value1_v2")));  // Newer version
    VERIFY_RESULT(table_->Put("key2", CreateTestValue("value2")));
    VERIFY_RESULT(table_->Put("key3", CreateTestValue("value3")));

    // Flush to create SSTable
    VERIFY_RESULT(table_->Flush());

    // Add more data to memtable (including an update)
    VERIFY_RESULT(table_->Put("key1", CreateTestValue("value1_v3")));  // Newest version
    VERIFY_RESULT(table_->Put("key4", CreateTestValue("value4")));
    VERIFY_RESULT(table_->Put("key5", CreateTestValue("value5")));

    // Create iterator
    auto iter_result = table_->NewIterator();
    VERIFY_RESULT(iter_result);
    auto& iter = *iter_result.value();

    // Verify iteration order and values
    iter.Seek("");
    ASSERT_TRUE(iter.Valid());
    EXPECT_EQ(iter.key(), "key1");
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(iter.value().Data()), iter.value().Size()), "value1_v3");

    iter.Next();
    ASSERT_TRUE(iter.Valid());
    EXPECT_EQ(iter.key(), "key2");
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(iter.value().Data()), iter.value().Size()), "value2");

    iter.Next();
    ASSERT_TRUE(iter.Valid());
    EXPECT_EQ(iter.key(), "key3");
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(iter.value().Data()), iter.value().Size()), "value3");

    iter.Next();
    ASSERT_TRUE(iter.Valid());
    EXPECT_EQ(iter.key(), "key4");
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(iter.value().Data()), iter.value().Size()), "value4");

    iter.Next();
    ASSERT_TRUE(iter.Valid());
    EXPECT_EQ(iter.key(), "key5");
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(iter.value().Data()), iter.value().Size()), "value5");

    iter.Next();
    ASSERT_FALSE(iter.Valid());
}

TEST_F(KvTableTest, IteratorWithTombstones) {
    // Insert initial data
    VERIFY_RESULT(table_->Put("key1", CreateTestValue("value1")));
    VERIFY_RESULT(table_->Put("key2", CreateTestValue("value2")));
    VERIFY_RESULT(table_->Put("key3", CreateTestValue("value3")));

    // Flush to create SSTable
    VERIFY_RESULT(table_->Flush());

    // Delete key2 and add new data
    VERIFY_RESULT(table_->Delete("key2"));  // Create tombstone
    VERIFY_RESULT(table_->Put("key4", CreateTestValue("value4")));

    // Test default mode (should skip tombstones)
    {
        auto iter_result = table_->NewIterator();
        VERIFY_RESULT(iter_result);
        auto& iter = *iter_result.value();

        iter.Seek("");
        ASSERT_TRUE(iter.Valid());
        EXPECT_EQ(iter.key(), "key1");

        iter.Next();
        ASSERT_TRUE(iter.Valid());
        EXPECT_EQ(iter.key(), "key3");  // key2 should be skipped

        iter.Next();
        ASSERT_TRUE(iter.Valid());
        EXPECT_EQ(iter.key(), "key4");
    }

    // Test with IncludeTombstones mode
    {
        auto iter_result = table_->NewIterator(common::MaxHybridTime(), common::IteratorMode::IncludeTombstones);
        VERIFY_RESULT(iter_result);
        auto& iter = *iter_result.value();

        iter.Seek("");
        ASSERT_TRUE(iter.Valid());
        EXPECT_EQ(iter.key(), "key1");
        EXPECT_FALSE(iter.IsTombstone());

        iter.Next();
        ASSERT_TRUE(iter.Valid());
        EXPECT_EQ(iter.key(), "key2");
        EXPECT_TRUE(iter.IsTombstone());

        iter.Next();
        ASSERT_TRUE(iter.Valid());
        EXPECT_EQ(iter.key(), "key3");
        EXPECT_FALSE(iter.IsTombstone());

        iter.Next();
        ASSERT_TRUE(iter.Valid());
        EXPECT_EQ(iter.key(), "key4");
        EXPECT_FALSE(iter.IsTombstone());
    }
}

TEST_F(KvTableTest, IteratorVersionVisibility) {
    // Insert data with multiple versions
    VERIFY_RESULT(table_->Put("key1", CreateTestValue("value1_v1")));
    VERIFY_RESULT(table_->Put("key2", CreateTestValue("value2_v1")));

    common::HybridTime v1_time = common::GetNextHybridTime();

    // Flush to create first SSTable
    VERIFY_RESULT(table_->Flush());

    // Update values
    VERIFY_RESULT(table_->Put("key1", CreateTestValue("value1_v2")));
    VERIFY_RESULT(table_->Delete("key2"));  // Tombstone

    common::HybridTime v2_time = common::GetNextHybridTime();

    // Flush to create second SSTable
    VERIFY_RESULT(table_->Flush());

    // Add newest versions
    VERIFY_RESULT(table_->Put("key1", CreateTestValue("value1_v3")));
    VERIFY_RESULT(table_->Put("key2", CreateTestValue("value2_v2")));

    // Test at different timestamps with different modes
    {
        // Test default mode at early timestamp
        auto iter_result = table_->NewIterator(v1_time);
        VERIFY_RESULT(iter_result);
        auto& iter = *iter_result.value();

        iter.Seek("");
        ASSERT_TRUE(iter.Valid());
        EXPECT_EQ(iter.key(), "key1");
        EXPECT_EQ(iter.value().ToString(), "value1_v1");

        iter.Next();
        ASSERT_TRUE(iter.Valid());
        EXPECT_EQ(iter.key(), "key2");
        EXPECT_EQ(iter.value().ToString(), "value2_v1");
    }

    {
        // Test default mode at middle timestamp
        auto iter_result = table_->NewIterator(v2_time);
        VERIFY_RESULT(iter_result);
        auto& iter = *iter_result.value();

        iter.Seek("");
        ASSERT_TRUE(iter.Valid());
        EXPECT_EQ(iter.key(), "key1");
        EXPECT_EQ(iter.value().ToString(), "value1_v2");

        iter.Next();
        ASSERT_FALSE(iter.Valid());
    }

    {
        // Test IncludeTombstones mode at middle timestamp
        auto iter_result = table_->NewIterator(v2_time, common::IteratorMode::IncludeTombstones);
        VERIFY_RESULT(iter_result);
        auto& iter = *iter_result.value();

        iter.Seek("");
        ASSERT_TRUE(iter.Valid());
        EXPECT_EQ(iter.key(), "key1");
        EXPECT_EQ(iter.value().ToString(), "value1_v2");

        iter.Next();
        ASSERT_TRUE(iter.Valid());
        EXPECT_EQ(iter.key(), "key2");
        EXPECT_TRUE(iter.IsTombstone());  // Should see tombstone
    }

    {
        // Test IncludeAllVersions mode at latest timestamp
        auto iter_result = table_->NewIterator(common::MaxHybridTime(), common::IteratorMode::IncludeAllVersions);
        VERIFY_RESULT(iter_result);
        auto& iter = *iter_result.value();

        iter.Seek("");
        ASSERT_TRUE(iter.Valid());
        EXPECT_EQ(iter.key(), "key1");
        EXPECT_EQ(iter.value().ToString(), "value1_v3");

        iter.Next();
        ASSERT_TRUE(iter.Valid());
        EXPECT_EQ(iter.key(), "key1");
        EXPECT_EQ(iter.value().ToString(), "value1_v2");

        iter.Next();
        ASSERT_TRUE(iter.Valid());
        EXPECT_EQ(iter.key(), "key1");
        EXPECT_EQ(iter.value().ToString(), "value1_v1");

        iter.Next();
        ASSERT_TRUE(iter.Valid());
        EXPECT_EQ(iter.key(), "key2");
        EXPECT_EQ(iter.value().ToString(), "value2_v2");

        iter.Next();
        ASSERT_TRUE(iter.Valid());
        EXPECT_EQ(iter.key(), "key2");
        EXPECT_EQ(iter.value().ToString(), "value2_v1");
    }

    {
        // Test combined IncludeTombstones and IncludeAllVersions mode
        auto iter_result =
            table_->NewIterator(common::MaxHybridTime(),
                                common::IteratorMode::IncludeTombstones | common::IteratorMode::IncludeAllVersions);
        VERIFY_RESULT(iter_result);
        auto& iter = *iter_result.value();

        iter.Seek("");
        ASSERT_TRUE(iter.Valid());
        EXPECT_EQ(iter.key(), "key1");
        EXPECT_EQ(iter.value().ToString(), "value1_v3");

        iter.Next();
        ASSERT_TRUE(iter.Valid());
        EXPECT_EQ(iter.key(), "key1");
        EXPECT_EQ(iter.value().ToString(), "value1_v2");

        iter.Next();
        ASSERT_TRUE(iter.Valid());
        EXPECT_EQ(iter.key(), "key1");
        EXPECT_EQ(iter.value().ToString(), "value1_v1");

        iter.Next();
        ASSERT_TRUE(iter.Valid());
        EXPECT_EQ(iter.key(), "key2");
        EXPECT_EQ(iter.value().ToString(), "value2_v2");

        iter.Next();
        ASSERT_TRUE(iter.Valid());
        EXPECT_EQ(iter.key(), "key2");
        EXPECT_EQ(iter.IsTombstone(), true);
        EXPECT_EQ(iter.value().ToString(), "");

        iter.Next();
        ASSERT_TRUE(iter.Valid());
        EXPECT_EQ(iter.key(), "key2");
        EXPECT_EQ(iter.value().ToString(), "value2_v1");
    }
}

}  // namespace pond::kv