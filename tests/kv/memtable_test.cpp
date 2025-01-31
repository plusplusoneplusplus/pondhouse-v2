#include "kv/memtable.h"

#include <random>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

namespace pond::kv {

class MemTableTest : public ::testing::Test {
protected:
    void SetUp() override {
        std::vector<ColumnSchema> columns = {
            {"id", ColumnType::INT32, false}, {"name", ColumnType::STRING, true}, {"value", ColumnType::BINARY, true}};
        schema = std::make_shared<Schema>(columns);
        table = std::make_unique<MemTable>(schema);
    }

    std::shared_ptr<Schema> schema;
    std::unique_ptr<MemTable> table;

    std::unique_ptr<Record> CreateTestRecord(int32_t id, const std::string& name, const std::string& value) {
        auto record = std::make_unique<Record>(schema);
        record->Set(0, id);
        record->Set(1, name);
        record->Set(2, common::DataChunk(reinterpret_cast<const uint8_t*>(value.data()), value.size()));
        return record;
    }
};

TEST_F(MemTableTest, BasicOperations) {
    // Test Put
    auto record = CreateTestRecord(1, "test", "value1");
    auto result = table->Put("key1", record);
    EXPECT_TRUE(result.ok());

    // Test Get
    auto get_result = table->Get("key1");
    EXPECT_TRUE(get_result.ok());
    auto retrieved = std::move(get_result).value();
    EXPECT_EQ(retrieved->Get<int32_t>(0).value(), 1);
    EXPECT_EQ(retrieved->Get<std::string>(1).value(), "test");
    auto value_result = retrieved->Get<common::DataChunk>(2);
    EXPECT_TRUE(value_result.ok());
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(value_result.value().data()), value_result.value().size()),
              "value1");

    // Test non-existent key
    get_result = table->Get("key2");
    EXPECT_FALSE(get_result.ok());
}

TEST_F(MemTableTest, ColumnOperations) {
    // Insert initial record
    auto record = CreateTestRecord(1, "test", "value1");
    EXPECT_TRUE(table->Put("key1", record).ok());

    // Update single column
    EXPECT_TRUE(
        table->UpdateColumn("key1", "name", common::DataChunk(reinterpret_cast<const uint8_t*>("updated"), 7)).ok());

    // Verify update
    auto column_result = table->GetColumn("key1", "name");
    EXPECT_TRUE(column_result.ok());
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(column_result.value().data()), column_result.value().size()),
              "updated");

    // Other columns should remain unchanged
    auto get_result = table->Get("key1");
    EXPECT_TRUE(get_result.ok());
    auto retrieved = std::move(get_result).value();
    EXPECT_EQ(retrieved->Get<int32_t>(0).value(), 1);
    auto value_result = retrieved->Get<common::DataChunk>(2);
    EXPECT_TRUE(value_result.ok());
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(value_result.value().data()), value_result.value().size()),
              "value1");
}

TEST_F(MemTableTest, SizeLimits) {
    std::string large_key(MAX_KEY_SIZE + 1, 'x');
    auto record = CreateTestRecord(1, "test", "value");

    // Test key size limit
    auto result = table->Put(large_key, record);
    EXPECT_FALSE(result.ok());
}

TEST_F(MemTableTest, MemoryUsage) {
    const size_t initial_usage = table->ApproximateMemoryUsage();

    // Add entries and check memory usage increases
    auto record = CreateTestRecord(1, "test", "value1");
    table->Put("key1", record);
    EXPECT_GT(table->ApproximateMemoryUsage(), initial_usage);

    // Add more entries until we reach the limit
    std::string large_value(1024 * 1024, 'x');  // 1MB value
    for (int i = 0; i < 64; i++) {              // Should fill up a 64MB memtable
        auto large_record = CreateTestRecord(i, "test", large_value);
        auto result = table->Put("key" + std::to_string(i), large_record);
        if (!result.ok()) {
            std::cout << "MemTable size: " << table->ApproximateMemoryUsage() << std::endl;
            EXPECT_TRUE(table->ShouldFlush());
            break;
        }
    }
}

TEST_F(MemTableTest, Iterator) {
    // Insert some entries
    std::vector<std::string> keys = {"key1", "key2", "key3", "key4", "key5"};

    for (size_t i = 0; i < keys.size(); i++) {
        auto record = CreateTestRecord(i, "name" + std::to_string(i), "value" + std::to_string(i));
        EXPECT_TRUE(table->Put(keys[i], record).ok());
    }

    // Test iterator
    auto it = table->NewIterator();
    size_t count = 0;

    for (; it->Valid(); it->Next(), count++) {
        EXPECT_EQ(it->key(), keys[count]);
        const Record& record = it->record();
        EXPECT_EQ(record.Get<int32_t>(0).value(), count);
        EXPECT_EQ(record.Get<std::string>(1).value(), "name" + std::to_string(count));
        auto value_result = record.Get<common::DataChunk>(2);
        EXPECT_TRUE(value_result.ok());
        EXPECT_EQ(std::string(reinterpret_cast<const char*>(value_result.value().data()), value_result.value().size()),
                  "value" + std::to_string(count));
    }
    EXPECT_EQ(count, keys.size());

    // Test seek
    it->Seek("key3");
    EXPECT_TRUE(it->Valid());
    EXPECT_EQ(it->key(), "key3");
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
                auto record =
                    CreateTestRecord(i * num_ops + j, "name" + std::to_string(j), "value" + std::to_string(j));
                auto result = table->Put(key, record);
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

    // Verify insertions (until memtable is full)
    for (int i = 0; i < num_threads; i++) {
        for (int j = 0; j < num_ops; j++) {
            std::string key = "key" + std::to_string(i) + "_" + std::to_string(j);
            auto result = table->Get(key);
            if (!result.ok()) {
                // Stop verification if we hit entries that couldn't be inserted
                break;
            }

            auto record = std::move(result).value();
            EXPECT_EQ(record->Get<int32_t>(0).value(), i * num_ops + j);
            EXPECT_EQ(record->Get<std::string>(1).value(), "name" + std::to_string(j));
            auto value_result = record->Get<common::DataChunk>(2);
            EXPECT_TRUE(value_result.ok());
            EXPECT_EQ(
                std::string(reinterpret_cast<const char*>(value_result.value().data()), value_result.value().size()),
                "value" + std::to_string(j));
        }
    }
}

TEST_F(MemTableTest, DeleteOperations) {
    // Test delete then get
    auto record = CreateTestRecord(1, "test", "value1");
    EXPECT_TRUE(table->Put("key1", record).ok());
    EXPECT_TRUE(table->Delete("key1").ok());
    EXPECT_FALSE(table->Get("key1").ok());

    // Test delete non-existent key
    EXPECT_TRUE(table->Delete("key2").ok());

    // Test put after delete
    record = CreateTestRecord(2, "test2", "value2");
    EXPECT_TRUE(table->Put("key1", record).ok());
    auto result = table->Get("key1");
    EXPECT_TRUE(result.ok());
    auto retrieved = std::move(result).value();
    EXPECT_EQ(retrieved->Get<int32_t>(0).value(), 2);
    EXPECT_EQ(retrieved->Get<std::string>(1).value(), "test2");
    auto value_result = retrieved->Get<common::DataChunk>(2);
    EXPECT_TRUE(value_result.ok());
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(value_result.value().data()), value_result.value().size()),
              "value2");
}

}  // namespace pond::kv
