#include "kv/sstable_manager.h"

#include <random>
#include <thread>

#include <gtest/gtest.h>

#include "common/memory_append_only_fs.h"
#include "kv/memtable.h"
#include "kv/record.h"
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
        metadata_state_machine_ = std::make_shared<TableMetadataStateMachine>(fs_, "test_db_metadata");
        VERIFY_RESULT(metadata_state_machine_->Open());
        manager_ = std::make_unique<SSTableManager>(fs_, "test_db", metadata_state_machine_);
    }

    // Helper to create a MemTable with test data
    std::unique_ptr<MemTable> CreateTestMemTable(size_t start_key, size_t num_entries) {
        auto memtable = std::make_unique<MemTable>();
        for (size_t i = 0; i < num_entries; i++) {
            std::string key = pond::test::GenerateKey(start_key + i);
            std::string value = "value" + std::to_string(start_key + i);
            auto record = std::make_unique<Record>(schema_);
            record->Set(0, value);
            EXPECT_TRUE(memtable->Put(key, record->Serialize(), 0 /* txn_id */).ok());
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
    std::shared_ptr<TableMetadataStateMachine> metadata_state_machine_;
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
        EXPECT_EQ(ToRecord(result.value())->Get<std::string>(0).value(), "value" + std::to_string(i));
    }
}

TEST_F(SSTableManagerTest, EmptyDirectory) {
    // Create new manager with empty directory
    auto empty_manager = std::make_unique<SSTableManager>(fs_, "empty_db", metadata_state_machine_);

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
    auto recovered_manager = std::make_unique<SSTableManager>(fs_, "test_db", metadata_state_machine_);

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
        VERIFY_RESULT_MSG(result, "Failed to read key: " + key);
        EXPECT_EQ(ToRecord(result.value())->Get<std::string>(0).value(), "value" + std::to_string(i));
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
    auto memtable = std::make_unique<MemTable>();
    std::string large_value(1024 * 1024, 'x');  // 1MB value

    for (size_t i = 0; i < 10; i++) {
        std::string key = pond::test::GenerateKey(i);
        auto record = std::make_unique<Record>(schema_);
        record->Set(0, large_value + std::to_string(i));
        ASSERT_TRUE(memtable->Put(key, record->Serialize(), 0 /* txn_id */).ok());
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
    auto empty_memtable = std::make_unique<MemTable>();
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

TEST_F(SSTableManagerTest, MetadataCacheBasic) {
    // Create a MemTable with test data
    auto memtable = CreateTestMemTable(0, 100);

    // Create SSTable
    auto result = manager_->CreateSSTableFromMemTable(*memtable);
    VERIFY_RESULT(result);

    // Get initial stats
    auto initial_stats = manager_->GetStats();

    // Test key lookups that should be filtered by metadata cache
    // Key before range
    auto before_range = manager_->Get("kee");
    ASSERT_FALSE(before_range.ok());
    EXPECT_EQ(before_range.error().code(), ErrorCode::NotFound);

    // Key after range
    auto after_range = manager_->Get(pond::test::GenerateKey(100));
    ASSERT_FALSE(after_range.ok());
    EXPECT_EQ(after_range.error().code(), ErrorCode::NotFound);

    // Key within range should be found
    auto within_range = manager_->Get(pond::test::GenerateKey(50));
    ASSERT_TRUE(within_range.ok());
    EXPECT_EQ(ToRecord(within_range.value())->Get<std::string>(0).value(), "value50");

    // Verify cache statistics
    auto final_stats = manager_->GetStats();
    EXPECT_EQ(final_stats.metadata_filter_cache_hits - initial_stats.metadata_filter_cache_hits,
              2);  // Before and after range
    EXPECT_EQ(final_stats.metadata_filter_cache_misses - initial_stats.metadata_filter_cache_misses,
              1);  // Within range
    EXPECT_EQ(final_stats.physical_reads - initial_stats.physical_reads,
              1);  // Only one physical read for the found key
}

TEST_F(SSTableManagerTest, MetadataCacheMultipleLevels) {
    // Create multiple SSTables at different levels with non-overlapping ranges
    std::vector<std::pair<size_t, size_t>> ranges = {
        {0, 100},    // First SSTable
        {200, 300},  // Second SSTable
        {400, 500}   // Third SSTable
    };

    for (const auto& range : ranges) {
        auto memtable = CreateTestMemTable(range.first, range.second - range.first);
        ASSERT_TRUE(manager_->CreateSSTableFromMemTable(*memtable).ok());
    }

    // Test key lookups that should be filtered by metadata cache
    // Keys in gaps between ranges should be quickly rejected
    std::vector<size_t> gap_keys = {150, 350};
    for (size_t key : gap_keys) {
        auto result = manager_->Get(pond::test::GenerateKey(key));
        ASSERT_FALSE(result.ok()) << "Key " << key << " should not be found";
        EXPECT_EQ(result.error().code(), ErrorCode::NotFound);
    }

    // Keys within ranges should be found
    std::vector<size_t> existing_keys = {50, 250, 450};
    for (size_t key : existing_keys) {
        auto result = manager_->Get(pond::test::GenerateKey(key));
        ASSERT_TRUE(result.ok()) << "Key " << key << " should be found";
        EXPECT_EQ(ToRecord(result.value())->Get<std::string>(0).value(), "value" + std::to_string(key));
    }
}

TEST_F(SSTableManagerTest, MetadataCacheBloomFilter) {
    // Create a MemTable with specific test data
    auto memtable = std::make_unique<MemTable>();
    std::vector<std::string> test_keys = {"apple", "banana", "cherry", "date"};

    for (size_t i = 0; i < test_keys.size(); i++) {
        auto record = std::make_unique<Record>(schema_);
        record->Set(0, "value" + std::to_string(i));
        ASSERT_TRUE(memtable->Put(test_keys[i], record->Serialize(), 0 /* txn_id */).ok());
    }

    // Create SSTable with bloom filter
    ASSERT_TRUE(manager_->CreateSSTableFromMemTable(*memtable).ok());

    // Test keys that should be filtered out by the bloom filter
    std::vector<std::string> non_existent_keys = {"grape", "kiwi", "lemon", "mango"};
    for (const auto& key : non_existent_keys) {
        auto result = manager_->Get(key);
        ASSERT_FALSE(result.ok()) << "Key " << key << " should not be found";
        EXPECT_EQ(result.error().code(), ErrorCode::NotFound);
    }

    // Verify existing keys are still accessible
    for (size_t i = 0; i < test_keys.size(); i++) {
        auto result = manager_->Get(test_keys[i]);
        ASSERT_TRUE(result.ok()) << "Key " << test_keys[i] << " should be found";
        EXPECT_EQ(ToRecord(result.value())->Get<std::string>(0).value(), "value" + std::to_string(i));
    }
}

TEST_F(SSTableManagerTest, MetadataCacheConcurrentAccess) {
    // Create initial SSTable
    auto memtable = CreateTestMemTable(0, 1000);
    ASSERT_TRUE(manager_->CreateSSTableFromMemTable(*memtable).ok());

    // Get initial stats
    auto initial_stats = manager_->GetStats();

    // Start multiple threads doing concurrent reads
    std::vector<std::thread> threads;
    std::atomic<size_t> found_keys{0};
    std::atomic<size_t> total_ops{0};

    for (int i = 0; i < 4; i++) {
        threads.emplace_back([this, &found_keys, &total_ops]() {
            for (int j = 0; j < 250; j++) {
                total_ops++;

                // Try to get a mix of existing and non-existing keys
                size_t key_num = rand() % 2000;  // Half will be non-existent
                auto result = manager_->Get(pond::test::GenerateKey(key_num));

                if (key_num < 1000) {
                    // Should exist
                    ASSERT_TRUE(result.ok()) << "Key " << key_num << " should exist";
                    found_keys++;
                } else {
                    // Should not exist
                    ASSERT_FALSE(result.ok()) << "Key " << key_num << " should not exist";
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    // Get final stats
    auto final_stats = manager_->GetStats();

    // Verify operation counts
    EXPECT_EQ(total_ops, 1000);  // 4 threads * 250 ops
    EXPECT_GT(found_keys, 0);

    // Verify cache effectiveness
    size_t cache_hits = final_stats.metadata_filter_cache_hits - initial_stats.metadata_filter_cache_hits;
    size_t cache_misses = final_stats.metadata_filter_cache_misses - initial_stats.metadata_filter_cache_misses;
    size_t physical_reads = final_stats.physical_reads - initial_stats.physical_reads;

    // We expect roughly half of the operations to be cache hits (keys outside range)
    EXPECT_GT(cache_hits, total_ops / 4);     // At least 25% should be cache hits
    EXPECT_LT(physical_reads, total_ops);     // Should have fewer physical reads than operations
    EXPECT_EQ(cache_misses, physical_reads);  // Each cache miss should result in one physical read
}

TEST_F(SSTableManagerTest, MetadataCacheRecovery) {
    // Create initial SSTable
    {
        auto memtable = CreateTestMemTable(0, 100);
        ASSERT_TRUE(manager_->CreateSSTableFromMemTable(*memtable).ok());
    }

    // Create new manager instance pointing to same directory
    auto recovered_manager = std::make_unique<SSTableManager>(fs_, "test_db", metadata_state_machine_);

    // Verify metadata cache is working in recovered instance
    auto before_range = recovered_manager->Get(pond::test::GenerateKey(-1));
    ASSERT_FALSE(before_range.ok());
    EXPECT_EQ(before_range.error().code(), ErrorCode::NotFound);

    auto within_range = recovered_manager->Get(pond::test::GenerateKey(50));
    ASSERT_TRUE(within_range.ok());
    EXPECT_EQ(ToRecord(within_range.value())->Get<std::string>(0).value(), "value50");

    auto after_range = recovered_manager->Get(pond::test::GenerateKey(100));
    ASSERT_FALSE(after_range.ok());
    EXPECT_EQ(after_range.error().code(), ErrorCode::NotFound);
}

TEST_F(SSTableManagerTest, MetadataStateTracking) {
    // Create initial SSTable
    {
        auto memtable = CreateTestMemTable(0, 100);
        ASSERT_TRUE(manager_->CreateSSTableFromMemTable(*memtable).ok());
    }

    // Verify metadata state machine has recorded the operation
    const auto& sstable_files = metadata_state_machine_->GetSSTableFiles(0 /*level*/);
    ASSERT_EQ(sstable_files.size(), 1);
    EXPECT_TRUE(sstable_files[0].name.starts_with("L0_"));
    EXPECT_TRUE(sstable_files[0].name.ends_with(".sst"));

    // Create another SSTable
    {
        auto memtable = CreateTestMemTable(100, 100);
        ASSERT_TRUE(manager_->CreateSSTableFromMemTable(*memtable).ok());
    }

    // Verify both files are tracked
    EXPECT_EQ(metadata_state_machine_->GetSSTableFiles(0 /*level*/).size(), 2);
    EXPECT_GT(metadata_state_machine_->GetTotalSize(), 0);

    // Create new manager instance and verify state is recovered
    auto new_metadata_state_machine = std::make_shared<TableMetadataStateMachine>(fs_, "test_db_metadata");
    VERIFY_RESULT(new_metadata_state_machine->Open());
    auto new_manager = std::make_unique<SSTableManager>(fs_, "test_db", new_metadata_state_machine);

    // Verify state is consistent
    EXPECT_EQ(new_metadata_state_machine->GetSSTableFiles(0 /*level*/).size(), 2);
    EXPECT_EQ(new_metadata_state_machine->GetTotalSize(), metadata_state_machine_->GetTotalSize());

    // Verify files are accessible through new manager
    auto result = new_manager->Get(pond::test::GenerateKey(50));
    VERIFY_RESULT(result);
    EXPECT_EQ(ToRecord(result.value())->Get<std::string>(0).value(), "value50");

    result = new_manager->Get(pond::test::GenerateKey(150));
    VERIFY_RESULT(result);
    EXPECT_EQ(ToRecord(result.value())->Get<std::string>(0).value(), "value150");
}

TEST_F(SSTableManagerTest, MetadataStateCheckpointing) {
    // Create several SSTables to generate metadata entries
    for (int i = 0; i < 5; i++) {
        auto memtable = CreateTestMemTable(i * 100, 100);
        ASSERT_TRUE(manager_->CreateSSTableFromMemTable(*memtable).ok());
    }

    // Create a checkpoint
    ASSERT_TRUE(metadata_state_machine_->CreateCheckpoint().ok());

    // Verify checkpoint file exists
    auto list_result = fs_->List("test_db_metadata");
    ASSERT_TRUE(list_result.ok());
    bool found_checkpoint = false;
    for (const auto& file : list_result.value()) {
        if (file.starts_with(TableMetadataStateMachine::CHECKPOINT_FILE_PREFIX)) {
            found_checkpoint = true;
            break;
        }
    }
    ASSERT_TRUE(found_checkpoint);

    // Create new manager instance and verify it recovers from checkpoint
    auto new_metadata_state_machine = std::make_shared<TableMetadataStateMachine>(fs_, "test_db_metadata");
    ASSERT_TRUE(new_metadata_state_machine->Open().ok());
    auto new_manager = std::make_unique<SSTableManager>(fs_, "test_db", new_metadata_state_machine);

    // Verify state is recovered correctly
    EXPECT_EQ(new_metadata_state_machine->GetSSTableFiles(0 /*level*/).size(), 5);
    EXPECT_EQ(new_metadata_state_machine->GetTotalSize(), metadata_state_machine_->GetTotalSize());

    // Verify all data is accessible
    for (int i = 0; i < 5; i++) {
        auto key = pond::test::GenerateKey(i * 100 + 50);  // Test middle key from each range
        auto result = new_manager->Get(key);
        ASSERT_TRUE(result.ok());
        EXPECT_EQ(ToRecord(result.value())->Get<std::string>(0).value(), "value" + std::to_string(i * 100 + 50));
    }
}

}  // namespace
