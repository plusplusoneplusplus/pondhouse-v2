#include "kv/sstable_manager.h"

#include <random>
#include <thread>

#include <gtest/gtest.h>

#include "common/memory_append_only_fs.h"
#include "kv/memtable.h"
#include "test_helper.h"

using namespace pond::common;
using namespace pond::kv;

namespace {

class SSTableManagerTest : public ::testing::Test {
protected:
    void SetUp() override {
        fs_ = std::make_shared<MemoryAppendOnlyFileSystem>();
        schema_ = std::make_shared<Schema>(std::vector<ColumnSchema>{
            {"value", ColumnType::STRING},
        });
        manager_ = std::make_unique<SSTableManager>(fs_, "test_db");
    }

    // Helper to create a MemTable with test data
    std::unique_ptr<MemTable> CreateTestMemTable(size_t start_key, size_t num_entries) {
        auto memtable = std::make_unique<MemTable>(schema_);
        for (size_t i = 0; i < num_entries; i++) {
            std::string key = pond::test::GenerateKey(start_key + i);
            std::string value = "value" + std::to_string(i);
            auto record = std::make_unique<Record>(schema_);
            record->Set(0, value);
            EXPECT_TRUE(memtable->Put(key, record).ok());
        }
        return memtable;
    }

    DataChunk FromRecord(const Record& record) { return record.Serialize(); }

    std::unique_ptr<Record> ToRecord(const DataChunk& data) {
        auto result = Record::Deserialize(data, schema_);
        EXPECT_TRUE(result.ok());
        return std::move(result).value();
    }

    std::shared_ptr<MemoryAppendOnlyFileSystem> fs_;
    std::unique_ptr<SSTableManager> manager_;
    std::shared_ptr<Schema> schema_;
};

TEST_F(SSTableManagerTest, BasicOperations) {
    // Create and flush a MemTable
    auto memtable = CreateTestMemTable(0, 100);
    auto result = manager_->CreateSSTableFromMemTable(*memtable);
    VERIFY_RESULT(result);
    EXPECT_FALSE(result.value().name.empty());
    EXPECT_GT(result.value().size, 0);

    // Verify stats
    auto stats = manager_->GetStats();
    EXPECT_EQ(stats.files_per_level[0], 1);  // One L0 file
    EXPECT_GT(stats.bytes_per_level[0], 0);
    EXPECT_EQ(stats.total_files, 1);

    // Read back values
    for (size_t i = 0; i < 100; i++) {
        std::string key = pond::test::GenerateKey(i);
        auto result = manager_->Get(key);
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(ToRecord(result.value())->Get<std::string>(0).value(), "value" + std::to_string(i));
    }

    // Try non-existent key
    auto get_result = manager_->Get(pond::test::GenerateKey(100));
    ASSERT_FALSE(get_result.ok());
    EXPECT_EQ(get_result.error().code(), ErrorCode::NotFound);
}

TEST_F(SSTableManagerTest, MultipleL0Files) {
    // Create multiple L0 files
    for (size_t i = 0; i < 3; i++) {
        auto memtable = CreateTestMemTable(i * 100, 100);
        auto result = manager_->CreateSSTableFromMemTable(*memtable);
        VERIFY_RESULT(result);
        EXPECT_FALSE(result.value().name.empty());
        EXPECT_GT(result.value().size, 0);
    }

    // Verify stats
    auto stats = manager_->GetStats();
    EXPECT_EQ(stats.files_per_level[0], 3);  // Three L0 files
    EXPECT_EQ(stats.total_files, 3);

    // Read back values (testing all files)
    for (size_t i = 0; i < 300; i++) {
        std::string key = pond::test::GenerateKey(i);
        auto result = manager_->Get(key);
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(ToRecord(result.value())->Get<std::string>(0).value(), "value" + std::to_string(i % 100));
    }
}

TEST_F(SSTableManagerTest, EmptyDirectory) {
    // Create new manager with empty directory
    auto empty_manager = std::make_unique<SSTableManager>(fs_, "empty_db");

    // Verify empty stats
    auto stats = empty_manager->GetStats();
    EXPECT_EQ(stats.total_files, 0);
    EXPECT_EQ(stats.total_bytes, 0);

    // Try to read non-existent key
    auto result = empty_manager->Get("key1");
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.error().code(), ErrorCode::NotFound);
}

TEST_F(SSTableManagerTest, DirectoryRecovery) {
    // Create some initial files
    std::vector<FileInfo> created_files;
    for (size_t i = 0; i < 2; i++) {
        auto memtable = CreateTestMemTable(i * 100, 100);
        auto result = manager_->CreateSSTableFromMemTable(*memtable);
        VERIFY_RESULT(result);
        created_files.push_back(result.value());
    }

    // Create new manager instance pointing to same directory
    auto recovered_manager = std::make_unique<SSTableManager>(fs_, "test_db");

    // Verify stats match
    auto original_stats = manager_->GetStats();
    auto recovered_stats = recovered_manager->GetStats();
    EXPECT_EQ(recovered_stats.files_per_level[0], original_stats.files_per_level[0]);
    EXPECT_EQ(recovered_stats.total_files, original_stats.total_files);
    EXPECT_EQ(recovered_stats.total_bytes, original_stats.total_bytes);

    // Verify data is readable
    for (size_t i = 0; i < 200; i++) {
        std::string key = pond::test::GenerateKey(i);
        auto result = recovered_manager->Get(key);
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(ToRecord(result.value())->Get<std::string>(0).value(), "value" + std::to_string(i % 100));
    }
}

TEST_F(SSTableManagerTest, ConcurrentReads) {
    // Create test data
    auto memtable = CreateTestMemTable(0, 1000);
    ASSERT_TRUE(manager_->CreateSSTableFromMemTable(*memtable).ok());

    // Test concurrent reads from multiple threads
    std::vector<std::thread> threads;
    std::atomic<size_t> success_count{0};

    for (size_t i = 0; i < 10; i++) {
        threads.emplace_back([this, &success_count]() {
            for (size_t j = 0; j < 100; j++) {
                size_t key_num = rand() % 1000;
                auto result = manager_->Get(pond::test::GenerateKey(key_num));
                if (result.ok()
                    && ToRecord(result.value())->Get<std::string>(0).value() == "value" + std::to_string(key_num)) {
                    success_count++;
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    EXPECT_EQ(success_count, 1000);  // All reads should succeed
}

TEST_F(SSTableManagerTest, LargeValues) {
    // Create MemTable with large values
    auto memtable = std::make_unique<MemTable>(schema_);
    std::string large_value(1024 * 1024, 'x');  // 1MB value

    for (size_t i = 0; i < 10; i++) {
        std::string key = pond::test::GenerateKey(i);
        auto record = std::make_unique<Record>(schema_);
        record->Set(0, large_value + std::to_string(i));
        ASSERT_TRUE(memtable->Put(key, record).ok());
    }

    // Flush to SSTable
    ASSERT_TRUE(manager_->CreateSSTableFromMemTable(*memtable).ok());

    // Verify large values can be read back
    for (size_t i = 0; i < 10; i++) {
        std::string key = pond::test::GenerateKey(i);
        auto result = manager_->Get(key);
        ASSERT_TRUE(result.ok());
        auto value = ToRecord(result.value())->Get<std::string>(0).value();
        EXPECT_EQ(value.size(), large_value.size() + std::to_string(i).size());
        EXPECT_EQ(value, large_value + std::to_string(i));
    }
}

TEST_F(SSTableManagerTest, InvalidOperations) {
    // Try to create SSTable from empty MemTable
    auto empty_memtable = std::make_unique<MemTable>(schema_);
    auto result = manager_->CreateSSTableFromMemTable(*empty_memtable);
    EXPECT_FALSE(result.ok());

    // Try to get with empty key
    auto get_result = manager_->Get("");
    EXPECT_FALSE(get_result.ok());
}

TEST_F(SSTableManagerTest, StressTest) {
    const size_t kNumMemTables = 5;
    const size_t kEntriesPerMemTable = 1000;

    // Create multiple MemTables with overlapping key ranges
    std::vector<std::unique_ptr<MemTable>> memtables;
    for (size_t i = 0; i < kNumMemTables; i++) {
        memtables.push_back(CreateTestMemTable(i * (kEntriesPerMemTable / 2), kEntriesPerMemTable));
        ASSERT_TRUE(manager_->CreateSSTableFromMemTable(*memtables.back()).ok());
    }

    // Verify all keys are readable
    std::set<std::string> all_keys;
    for (size_t i = 0; i < kNumMemTables; i++) {
        for (size_t j = 0; j < kEntriesPerMemTable; j++) {
            all_keys.insert(pond::test::GenerateKey(i * (kEntriesPerMemTable / 2) + j));
        }
    }

    for (const auto& key : all_keys) {
        auto result = manager_->Get(key);
        ASSERT_TRUE(result.ok()) << "Failed to read key: " << key;
    }

    // Verify stats
    auto stats = manager_->GetStats();
    EXPECT_EQ(stats.files_per_level[0], kNumMemTables);
    EXPECT_GT(stats.total_bytes, 0);
}

}  // namespace
