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
        VERIFY_RESULT_MSG(result, "Failed to read key: " + key);
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

TEST_F(SSTableManagerTest, MetadataStateSnapshotting) {
    // Create several SSTables to generate metadata entries
    for (int i = 0; i < 5; i++) {
        auto memtable = CreateTestMemTable(i * 100, 100);
        ASSERT_TRUE(manager_->CreateSSTableFromMemTable(*memtable).ok());
    }

    // Create a snapshot
    ASSERT_TRUE(metadata_state_machine_->TriggerSnapshot().ok());

    // Verify snapshot file exists
    auto list_result = fs_->List("test_db_metadata");
    ASSERT_TRUE(list_result.ok());
    bool found_snapshot = false;
    for (const auto& file : list_result.value()) {
        if (file.ends_with(".snapshot")) {
            found_snapshot = true;
            break;
        }
    }
    ASSERT_TRUE(found_snapshot);

    // Create new manager instance and verify it recovers from snapshot
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

TEST_F(SSTableManagerTest, MergeL0ToL1) {
    // Create multiple L0 files with overlapping ranges and versions
    std::vector<FileInfo> l0_files;

    // First L0 file: keys 0-100
    {
        auto memtable = CreateTestMemTable(0, 100);
        auto result = manager_->CreateSSTableFromMemTable(*memtable);
        VERIFY_RESULT(result);
        l0_files.push_back(result.value());
    }

    // Second L0 file: keys 50-150 (overlapping with first file)
    {
        auto memtable = CreateTestMemTable(50, 100);
        auto result = manager_->CreateSSTableFromMemTable(*memtable);
        VERIFY_RESULT(result);
        l0_files.push_back(result.value());
    }

    // Third L0 file: keys 100-200 (overlapping with second file)
    {
        auto memtable = CreateTestMemTable(100, 100);
        auto result = manager_->CreateSSTableFromMemTable(*memtable);
        VERIFY_RESULT(result);
        l0_files.push_back(result.value());
    }

    // Verify initial state
    auto initial_stats = manager_->GetStats();
    EXPECT_EQ(initial_stats.files_per_level[0], 3);  // Three L0 files
    EXPECT_EQ(initial_stats.total_files, 3);
    EXPECT_GT(initial_stats.total_bytes, 0);

    // Verify L0 files in metadata state machine
    const auto& l0_metadata = metadata_state_machine_->GetSSTableFiles(0);
    EXPECT_EQ(l0_metadata.size(), 3);
    EXPECT_EQ(l0_metadata[0].level, 0);
    EXPECT_EQ(l0_metadata[1].level, 0);
    EXPECT_EQ(l0_metadata[2].level, 0);

    // Merge L0 to L1
    auto merge_result = manager_->MergeL0ToL1();
    VERIFY_RESULT(merge_result);

    // Verify the merged file info
    const auto& merged_file = merge_result.value();
    EXPECT_EQ(merged_file.level, 1);
    EXPECT_EQ(merged_file.smallest_key, pond::test::GenerateKey(0));   // First key overall
    EXPECT_EQ(merged_file.largest_key, pond::test::GenerateKey(199));  // Last key overall
    EXPECT_GT(merged_file.size, 0);

    // Verify final state
    auto final_stats = manager_->GetStats();
    EXPECT_EQ(final_stats.files_per_level[0], 0);  // No L0 files
    EXPECT_EQ(final_stats.files_per_level[1], 1);  // One L1 file
    EXPECT_EQ(final_stats.total_files, 1);
    EXPECT_LT(final_stats.total_bytes, initial_stats.total_bytes);  // Size should be smaller due to deduplication

    // Verify metadata state
    const auto& l0_files_after = metadata_state_machine_->GetSSTableFiles(0);
    EXPECT_TRUE(l0_files_after.empty());
    const auto& l1_files = metadata_state_machine_->GetSSTableFiles(1);
    ASSERT_EQ(l1_files.size(), 1);
    EXPECT_EQ(l1_files[0].level, 1);
    EXPECT_EQ(l1_files[0].smallest_key, pond::test::GenerateKey(0));
    EXPECT_EQ(l1_files[0].largest_key, pond::test::GenerateKey(199));

    // Verify all keys are still accessible with correct values
    for (size_t i = 0; i < 200; i++) {
        std::string key = pond::test::GenerateKey(i);
        auto result = manager_->Get(key);
        VERIFY_RESULT_MSG(result, "Failed to read key: " + key);
        EXPECT_EQ(ToRecord(result.value())->Get<std::string>(0).value(), "value" + std::to_string(i));
    }

    // Verify non-existent keys still return NotFound
    auto not_found_result = manager_->Get(pond::test::GenerateKey(200));
    EXPECT_FALSE(not_found_result.ok());
    EXPECT_EQ(not_found_result.error().code(), ErrorCode::NotFound);

    // Try to merge again with no L0 files
    auto empty_merge_result = manager_->MergeL0ToL1();
    EXPECT_FALSE(empty_merge_result.ok());
    EXPECT_EQ(empty_merge_result.error().code(), ErrorCode::InvalidOperation);
}

// Add a test for merging with updates to existing keys
TEST_F(SSTableManagerTest, MergeL0ToL1WithUpdates) {
    // Create first L0 file with initial values
    {
        auto memtable = std::make_unique<MemTable>();
        for (size_t i = 0; i < 100; i++) {
            std::string key = pond::test::GenerateKey(i);
            auto record = std::make_unique<Record>(schema_);
            record->Set(0, "old_value" + std::to_string(i));
            ASSERT_TRUE(memtable->Put(key, record->Serialize(), 1 /* older version */).ok());
        }
        VERIFY_RESULT(manager_->CreateSSTableFromMemTable(*memtable));
    }

    // Create second L0 file with updates to some keys
    {
        auto memtable = std::make_unique<MemTable>();
        for (size_t i = 0; i < 50; i++) {  // Update first half of keys
            std::string key = pond::test::GenerateKey(i);
            auto record = std::make_unique<Record>(schema_);
            record->Set(0, "new_value" + std::to_string(i));
            ASSERT_TRUE(memtable->Put(key, record->Serialize(), 2 /* newer version */).ok());
        }
        VERIFY_RESULT(manager_->CreateSSTableFromMemTable(*memtable));
    }

    // Verify initial state
    auto initial_stats = manager_->GetStats();
    EXPECT_EQ(initial_stats.files_per_level[0], 2);

    // Merge L0 to L1
    auto merge_result = manager_->MergeL0ToL1();
    VERIFY_RESULT(merge_result);

    // Verify final state
    auto final_stats = manager_->GetStats();
    EXPECT_EQ(final_stats.files_per_level[0], 0);
    EXPECT_EQ(final_stats.files_per_level[1], 1);

    // Verify merged data has correct versions
    for (size_t i = 0; i < 100; i++) {
        std::string key = pond::test::GenerateKey(i);
        auto result = manager_->Get(key);
        VERIFY_RESULT_MSG(result, "Failed to read key: " + key);
        std::string expected_value = i < 50 ? "new_value" + std::to_string(i)   // Updated keys
                                            : "old_value" + std::to_string(i);  // Non-updated keys
        EXPECT_EQ(ToRecord(result.value())->Get<std::string>(0).value(), expected_value);
    }
}

// Edge test cases for MergeL0ToL1
TEST_F(SSTableManagerTest, MergeL0ToL1_SingleFile) {
    auto memtable = CreateTestMemTable(0, 1);
    VERIFY_RESULT(manager_->CreateSSTableFromMemTable(*memtable));

    auto merge_result = manager_->MergeL0ToL1();
    VERIFY_RESULT(merge_result);
    EXPECT_EQ(merge_result.value().level, 1);
}

TEST_F(SSTableManagerTest, MergeL0ToL1_ManySmallFiles) {
    auto new_manager = std::make_unique<SSTableManager>(fs_, "many_files_test", metadata_state_machine_);
    // Create 100 L0 files with single entry each
    for (size_t i = 0; i < 100; i++) {
        auto memtable = std::make_unique<MemTable>();
        auto record = std::make_unique<Record>(schema_);
        record->Set(0, std::string("value") + std::to_string(i));  // Explicitly use std::string
        ASSERT_TRUE(memtable->Put("key" + std::to_string(i), record->Serialize(), 0).ok());
        VERIFY_RESULT(new_manager->CreateSSTableFromMemTable(*memtable));
    }

    auto merge_result = new_manager->MergeL0ToL1();
    VERIFY_RESULT(merge_result);

    // Verify all keys can be retrieved
    for (size_t i = 0; i < 100; i++) {
        auto get_result = new_manager->Get("key" + std::to_string(i));
        VERIFY_RESULT(get_result);
        EXPECT_EQ(ToRecord(get_result.value())->Get<std::string>(0).value(), "value" + std::to_string(i));
    }
}

TEST_F(SSTableManagerTest, MergeL0ToL1_VersionEdgeCases) {
    auto new_manager = std::make_unique<SSTableManager>(fs_, "version_test", metadata_state_machine_);

    // Create files with min and max version numbers
    auto memtable1 = std::make_unique<MemTable>();
    auto record1 = std::make_unique<Record>(schema_);
    record1->Set(0, std::string("oldest"));  // Explicitly use std::string
    ASSERT_TRUE(memtable1->Put("version_key", record1->Serialize(), 0).ok());
    VERIFY_RESULT(new_manager->CreateSSTableFromMemTable(*memtable1));

    auto memtable2 = std::make_unique<MemTable>();
    auto record2 = std::make_unique<Record>(schema_);
    record2->Set(0, std::string("newest"));  // Explicitly use std::string
    ASSERT_TRUE(memtable2->Put("version_key", record2->Serialize(), std::numeric_limits<uint64_t>::max()).ok());
    VERIFY_RESULT(new_manager->CreateSSTableFromMemTable(*memtable2));

    auto merge_result = new_manager->MergeL0ToL1();
    VERIFY_RESULT(merge_result);

    // Verify newest version is retrieved
    auto get_result = new_manager->Get("version_key");
    VERIFY_RESULT(get_result);
    EXPECT_EQ(ToRecord(get_result.value())->Get<std::string>(0).value(), "newest");
}

TEST_F(SSTableManagerTest, MergeL0ToL1_SpecialCharKeys) {
    auto new_manager = std::make_unique<SSTableManager>(fs_, "special_chars_test", metadata_state_machine_);
    auto memtable = std::make_unique<MemTable>();
    std::vector<std::string> special_keys = {
        std::string("\0key", 4),  // Null character
        "\n",                     // Newline
        "\t",                     // Tab
        "\xFF\xFE",               // UTF-16 BOM
        "key\xFF",                // High ASCII
        "../../path"              // Path traversal attempt
    };

    for (const auto& key : special_keys) {
        auto record = std::make_unique<Record>(schema_);
        record->Set(0, std::string("special"));  // Explicitly use std::string
        ASSERT_TRUE(memtable->Put(key, record->Serialize(), 0).ok());
    }
    VERIFY_RESULT(new_manager->CreateSSTableFromMemTable(*memtable));

    auto merge_result = new_manager->MergeL0ToL1();
    VERIFY_RESULT(merge_result);

    // Verify all special keys can be retrieved
    for (const auto& key : special_keys) {
        auto get_result = new_manager->Get(key);
        VERIFY_RESULT(get_result);
        EXPECT_EQ(ToRecord(get_result.value())->Get<std::string>(0).value(), "special");
    }
}

// Error test cases for MergeL0ToL1
TEST_F(SSTableManagerTest, MergeL0ToL1_NoL0Files) {
    auto empty_manager = std::make_unique<SSTableManager>(fs_, "empty_error_test", metadata_state_machine_);
    auto result = empty_manager->MergeL0ToL1();
    EXPECT_FALSE(result.ok());
    EXPECT_EQ(result.error().code(), ErrorCode::InvalidOperation);
}

TEST_F(SSTableManagerTest, MergeL0ToL1_CorruptedFile) {
    // Create a valid L0 file first
    auto memtable = CreateTestMemTable(0, 1);
    VERIFY_RESULT(manager_->CreateSSTableFromMemTable(*memtable));

    // Corrupt the file by appending invalid data
    auto file_result = fs_->OpenFile("test_db/L0_1.sst");
    VERIFY_RESULT(file_result);
    std::string corrupt_data = "corrupted data";
    VERIFY_RESULT(fs_->Append(file_result.value(),
                              DataChunk(reinterpret_cast<const uint8_t*>(corrupt_data.data()), corrupt_data.size())));

    // Attempt merge should fail gracefully
    auto merge_result = manager_->MergeL0ToL1();
    EXPECT_FALSE(merge_result.ok());
}

TEST_F(SSTableManagerTest, MergeL0ToL1_FSError) {
    // Create a new manager with a filesystem that will fail operations
    auto error_fs = std::make_shared<MemoryAppendOnlyFileSystem>();
    auto error_manager = std::make_unique<SSTableManager>(error_fs, "error_test", metadata_state_machine_);

    // Create a valid L0 file
    auto memtable = std::make_unique<MemTable>();
    auto record = std::make_unique<Record>(schema_);
    record->Set(0, std::string("test_value"));  // Explicitly use std::string
    VERIFY_RESULT(memtable->Put("test_key", record->Serialize(), 0));
    VERIFY_RESULT(error_manager->CreateSSTableFromMemTable(*memtable));

    // Delete the L1 directory to simulate filesystem errors during merge
    VERIFY_RESULT(error_fs->DeleteDirectory("error_test", true /* recursive */));

    // Merging should fail due to filesystem error
    auto merge_result = error_manager->MergeL0ToL1();
    EXPECT_FALSE(merge_result.ok());
}

TEST_F(SSTableManagerTest, MergeL0ToL1_WithOverlappingL1Files) {
    // First create L1 files with non-overlapping ranges
    std::vector<std::pair<size_t, size_t>> l1_ranges = {
        {0, 100},    // L1_1: keys 0-99
        {200, 300},  // L1_2: keys 200-299
        {400, 500}   // L1_3: keys 400-499
    };

    // Create L1 files first
    for (const auto& range : l1_ranges) {
        auto memtable = CreateTestMemTable(range.first, range.second - range.first);
        VERIFY_RESULT(manager_->CreateSSTableFromMemTable(*memtable));
        // Move the file to L1 manually
        auto merge_result = manager_->MergeL0ToL1();
        VERIFY_RESULT(merge_result);
    }

    // Verify initial L1 state
    auto initial_stats = manager_->GetStats();
    EXPECT_EQ(initial_stats.files_per_level[1], 3);  // Three L1 files
    EXPECT_EQ(initial_stats.files_per_level[0], 0);  // No L0 files

    // Now create L0 files with ranges that overlap L1 files
    std::vector<std::pair<size_t, size_t>> l0_ranges = {
        {50, 150},  // Overlaps with first L1 file and gap
        {400, 450}  // Overlaps with second and third L1 files
    };

    // Create L0 files with newer versions of some keys
    for (const auto& range : l0_ranges) {
        auto memtable = std::make_unique<MemTable>();
        for (size_t i = range.first; i < range.second; i++) {
            std::string key = pond::test::GenerateKey(i);
            auto record = std::make_unique<Record>(schema_);
            record->Set(0, "updated_value" + std::to_string(i));
            VERIFY_RESULT(memtable->Put(key, record->Serialize(), 0 /* txn_id */));
        }
        VERIFY_RESULT(manager_->CreateSSTableFromMemTable(*memtable));
    }

    // Verify L0 files were created
    auto pre_merge_stats = manager_->GetStats();
    EXPECT_EQ(pre_merge_stats.files_per_level[0], 2);  // Two L0 files
    EXPECT_EQ(pre_merge_stats.files_per_level[1], 3);  // Three L1 files

    // Perform merge
    auto merge_result = manager_->MergeL0ToL1();
    VERIFY_RESULT(merge_result);

    // Verify post-merge state
    auto post_merge_stats = manager_->GetStats();
    EXPECT_EQ(post_merge_stats.files_per_level[0], 0);  // No L0 files
    EXPECT_EQ(post_merge_stats.files_per_level[1], 1);  // Merged into one L1 file

    // Verify data integrity - check both updated and non-updated keys
    // 1. Keys that were updated in L0 should have new values
    for (const auto& range : l0_ranges) {
        for (size_t i = range.first; i < range.second; i++) {
            auto result = manager_->Get(pond::test::GenerateKey(i));
            VERIFY_RESULT_MSG(result, "Failed to read updated key: " + std::to_string(i));
            EXPECT_EQ(ToRecord(result.value())->Get<std::string>(0).value(), "updated_value" + std::to_string(i))
                << "Key " << i << " should have updated value";
        }
    }

    // 2. Keys that were only in L1 should retain their original values
    std::vector<size_t> non_updated_keys = {0, 25, 175, 350, 475};
    for (size_t key : non_updated_keys) {
        auto result = manager_->Get(pond::test::GenerateKey(key));
        if (key < 100 || (key >= 200 && key < 300) || (key >= 400 && key < 500)) {
            // Key should exist with original value
            VERIFY_RESULT_MSG(result, "Failed to read non-updated key: " + std::to_string(key));
            EXPECT_EQ(ToRecord(result.value())->Get<std::string>(0).value(), "value" + std::to_string(key))
                << "Key " << key << " should retain original value";
        } else {
            // Key should not exist
            EXPECT_FALSE(result.ok()) << "Key " << key << " should not exist";
            EXPECT_EQ(result.error().code(), ErrorCode::NotFound);
        }
    }
}

TEST_F(SSTableManagerTest, MergeL0ToL1_WithPartialOverlap) {
    // Create an L1 file with keys 100-200
    {
        auto memtable = CreateTestMemTable(100, 101);  // Keys 100-200
        VERIFY_RESULT(manager_->CreateSSTableFromMemTable(*memtable));
        VERIFY_RESULT(manager_->MergeL0ToL1());  // Move to L1
    }

    // Create L0 files with partial overlap
    std::vector<std::pair<size_t, size_t>> l0_ranges = {
        {50, 150},  // Partial overlap with L1 (lower bound)
        {175, 250}  // Partial overlap with L1 (upper bound)
    };

    for (const auto& range : l0_ranges) {
        auto memtable = std::make_unique<MemTable>();
        for (size_t i = range.first; i < range.second; i++) {
            std::string key = pond::test::GenerateKey(i);
            auto record = std::make_unique<Record>(schema_);
            record->Set(0, "l0_value" + std::to_string(i));
            ASSERT_TRUE(memtable->Put(key, record->Serialize(), 2 /* newer version */).ok());
        }
        VERIFY_RESULT(manager_->CreateSSTableFromMemTable(*memtable));
    }

    // Perform merge
    VERIFY_RESULT(manager_->MergeL0ToL1());

    // Verify results
    // 1. Keys before L1 range (50-99)
    for (size_t i = 50; i < 100; i++) {
        auto result = manager_->Get(pond::test::GenerateKey(i));
        VERIFY_RESULT_MSG(result, "Failed to read key before L1 range: " + std::to_string(i));
        EXPECT_EQ(ToRecord(result.value())->Get<std::string>(0).value(), "l0_value" + std::to_string(i));
    }

    // 2. Overlapping keys (100-150, 175-200) should have L0 values
    std::vector<std::pair<size_t, size_t>> overlap_ranges = {{100, 150}, {175, 200}};
    for (const auto& range : overlap_ranges) {
        for (size_t i = range.first; i < range.second; i++) {
            auto result = manager_->Get(pond::test::GenerateKey(i));
            VERIFY_RESULT_MSG(result, "Failed to read overlapping key: " + std::to_string(i));
            EXPECT_EQ(ToRecord(result.value())->Get<std::string>(0).value(), "l0_value" + std::to_string(i));
        }
    }

    // 3. Non-overlapping L1 keys (151-174) should retain original values
    for (size_t i = 151; i < 175; i++) {
        auto result = manager_->Get(pond::test::GenerateKey(i));
        VERIFY_RESULT_MSG(result, "Failed to read L1-only key: " + std::to_string(i));
        EXPECT_EQ(ToRecord(result.value())->Get<std::string>(0).value(), "value" + std::to_string(i));
    }

    // 4. Keys after L1 range (201-250)
    for (size_t i = 201; i < 250; i++) {
        auto result = manager_->Get(pond::test::GenerateKey(i));
        VERIFY_RESULT_MSG(result, "Failed to read key after L1 range: " + std::to_string(i));
        EXPECT_EQ(ToRecord(result.value())->Get<std::string>(0).value(), "l0_value" + std::to_string(i));
    }

    // verify the L1 file is removed
    auto post_merge_files = metadata_state_machine_->GetSSTableFiles(1);
    EXPECT_EQ(post_merge_files.size(), 1);
}

TEST_F(SSTableManagerTest, MergeL0ToL1_NoOverlappingL1Files) {
    // Create L1 files with ranges that don't overlap with upcoming L0 files
    std::vector<std::pair<size_t, size_t>> l1_ranges = {
        {0, 100},   // L1_1: keys 0-99
        {300, 400}  // L1_2: keys 300-399
    };

    for (const auto& range : l1_ranges) {
        auto memtable = CreateTestMemTable(range.first, range.second - range.first);
        VERIFY_RESULT(manager_->CreateSSTableFromMemTable(*memtable));
        VERIFY_RESULT(manager_->MergeL0ToL1());
    }

    // verify the L1 file is generated
    auto post_merge_files = metadata_state_machine_->GetSSTableFiles(1);
    EXPECT_EQ(post_merge_files.size(), 2);

    // Create L0 files in the gap between L1 files
    auto memtable = CreateTestMemTable(150, 100);  // Keys 150-249
    VERIFY_RESULT(manager_->CreateSSTableFromMemTable(*memtable));

    // Verify pre-merge state
    auto pre_merge_stats = manager_->GetStats();
    EXPECT_EQ(pre_merge_stats.files_per_level[0], 1);  // One L0 file
    EXPECT_EQ(pre_merge_stats.files_per_level[1], 2);  // Two L1 files

    // Perform merge
    auto merge_result = manager_->MergeL0ToL1();
    VERIFY_RESULT(merge_result);

    // Verify post-merge state
    auto post_merge_stats = manager_->GetStats();
    EXPECT_EQ(post_merge_stats.files_per_level[0], 0);  // No L0 files
    EXPECT_EQ(post_merge_stats.files_per_level[1], 3);  // Three L1 files

    // Verify data integrity
    // 1. Original L1 data should be unchanged
    for (const auto& range : l1_ranges) {
        for (size_t i = range.first; i < range.second; i++) {
            auto result = manager_->Get(pond::test::GenerateKey(i));
            VERIFY_RESULT_MSG(result, "Failed to read L1 key: " + std::to_string(i));
            EXPECT_EQ(ToRecord(result.value())->Get<std::string>(0).value(), "value" + std::to_string(i));
        }
    }

    // 2. Merged L0 data should be accessible
    for (size_t i = 150; i < 250; i++) {
        auto result = manager_->Get(pond::test::GenerateKey(i));
        VERIFY_RESULT_MSG(result, "Failed to read merged L0 key: " + std::to_string(i));
        EXPECT_EQ(ToRecord(result.value())->Get<std::string>(0).value(), "value" + std::to_string(i));
    }

    // 3. Gaps should still return NotFound
    std::vector<size_t> gap_keys = {100, 149, 250, 299};
    for (size_t key : gap_keys) {
        auto result = manager_->Get(pond::test::GenerateKey(key));
        EXPECT_FALSE(result.ok()) << "Key " << key << " should not exist";
        EXPECT_EQ(result.error().code(), ErrorCode::NotFound);
    }
}

TEST_F(SSTableManagerTest, MergeL0ToL1_VerifyL1FileRemoval) {
    // First create L1 files with specific file numbers we can track
    std::vector<std::pair<size_t, size_t>> l1_ranges = {
        {0, 100},    // L1_1
        {200, 300},  // L1_2
        {400, 500}   // L1_3
    };

    std::vector<FileInfo> l1_files;
    for (const auto& range : l1_ranges) {
        auto memtable = CreateTestMemTable(range.first, range.second - range.first);
        auto create_result = manager_->CreateSSTableFromMemTable(*memtable);
        VERIFY_RESULT(create_result);
        l1_files.push_back(create_result.value());
        VERIFY_RESULT(manager_->MergeL0ToL1());
    }

    // Create L0 file that overlaps with the second L1 file
    auto memtable = std::make_unique<MemTable>();
    for (size_t i = 150; i < 350; i++) {  // Overlaps with L1_2 and gaps
        std::string key = pond::test::GenerateKey(i);
        auto record = std::make_unique<Record>(schema_);
        record->Set(0, "l0_value" + std::to_string(i));
        ASSERT_TRUE(memtable->Put(key, record->Serialize(), 2 /* newer version */).ok());
    }
    VERIFY_RESULT(manager_->CreateSSTableFromMemTable(*memtable));

    // Get the L1 files before merge
    auto pre_merge_files = metadata_state_machine_->GetSSTableFiles(1);
    ASSERT_EQ(pre_merge_files.size(), 3);  // Should have all three L1 files

    // Perform merge
    auto merge_result = manager_->MergeL0ToL1();
    VERIFY_RESULT(merge_result);

    // Get the L1 files after merge
    auto post_merge_files = metadata_state_machine_->GetSSTableFiles(1);

    // Verify:
    // 1. The overlapped L1 file (L1_2) is removed
    // 2. Non-overlapping L1 files (L1_1 and L1_3) are preserved
    // 3. A new L1 file is added
    EXPECT_EQ(post_merge_files.size(), 3);  // 2 original + 1 new file

    // Verify the specific files that should still exist
    bool found_l1_1 = false, found_l1_3 = false;
    for (const auto& file : post_merge_files) {
        if (file.smallest_key == pond::test::GenerateKey(0) && file.largest_key == pond::test::GenerateKey(99)) {
            found_l1_1 = true;
        } else if (file.smallest_key == pond::test::GenerateKey(400)
                   && file.largest_key == pond::test::GenerateKey(499)) {
            found_l1_3 = true;
        }
    }
    EXPECT_TRUE(found_l1_1) << "L1_1 file should still exist";
    EXPECT_TRUE(found_l1_3) << "L1_3 file should still exist";

    // Verify data integrity
    // 1. Check L0's new values are preserved
    for (size_t i = 150; i < 350; i++) {
        auto result = manager_->Get(pond::test::GenerateKey(i));
        VERIFY_RESULT_MSG(result, "Failed to read updated key: " + std::to_string(i));
        EXPECT_EQ(ToRecord(result.value())->Get<std::string>(0).value(), "l0_value" + std::to_string(i));
    }

    // 2. Check non-overlapping L1 values are preserved
    for (size_t i = 0; i < 100; i++) {
        auto result = manager_->Get(pond::test::GenerateKey(i));
        VERIFY_RESULT_MSG(result, "Failed to read L1_1 key: " + std::to_string(i));
        EXPECT_EQ(ToRecord(result.value())->Get<std::string>(0).value(), "value" + std::to_string(i));
    }
    for (size_t i = 400; i < 500; i++) {
        auto result = manager_->Get(pond::test::GenerateKey(i));
        VERIFY_RESULT_MSG(result, "Failed to read L1_3 key: " + std::to_string(i));
        EXPECT_EQ(ToRecord(result.value())->Get<std::string>(0).value(), "value" + std::to_string(i));
    }
}

TEST_F(SSTableManagerTest, MergeL0ToL1_KeyRangeWithOverlap) {
    // Create L1 file with keys 100-300
    {
        auto memtable = CreateTestMemTable(100, 201);  // Keys 100-300
        VERIFY_RESULT(manager_->CreateSSTableFromMemTable(*memtable));
        VERIFY_RESULT(manager_->MergeL0ToL1());  // Move to L1
    }

    // Create L0 file with keys 200-400 (partial overlap with L1)
    {
        auto memtable = CreateTestMemTable(200, 201);  // Keys 200-400
        VERIFY_RESULT(manager_->CreateSSTableFromMemTable(*memtable));
    }

    // Verify initial state
    auto pre_merge_l1_files = metadata_state_machine_->GetSSTableFiles(1);
    ASSERT_EQ(pre_merge_l1_files.size(), 1);
    EXPECT_EQ(pre_merge_l1_files[0].smallest_key, pond::test::GenerateKey(100));
    EXPECT_EQ(pre_merge_l1_files[0].largest_key, pond::test::GenerateKey(300));

    // Perform merge
    auto merge_result = manager_->MergeL0ToL1();
    VERIFY_RESULT(merge_result);

    // Verify the merged file's key range spans the entire range
    auto post_merge_l1_files = metadata_state_machine_->GetSSTableFiles(1);
    ASSERT_EQ(post_merge_l1_files.size(), 1);
    EXPECT_EQ(post_merge_l1_files[0].smallest_key, pond::test::GenerateKey(100))
        << "Smallest key should be from L1 file";
    EXPECT_EQ(post_merge_l1_files[0].largest_key, pond::test::GenerateKey(400)) << "Largest key should be from L0 file";

    // Verify data integrity across the entire range
    // 1. Original L1-only range (100-199)
    for (size_t i = 100; i < 200; i++) {
        auto result = manager_->Get(pond::test::GenerateKey(i));
        VERIFY_RESULT_MSG(result, "Failed to read L1-only key: " + std::to_string(i));
        EXPECT_EQ(ToRecord(result.value())->Get<std::string>(0).value(), "value" + std::to_string(i));
    }

    // 2. Overlapping range (200-300)
    for (size_t i = 200; i <= 300; i++) {
        auto result = manager_->Get(pond::test::GenerateKey(i));
        VERIFY_RESULT_MSG(result, "Failed to read overlapping key: " + std::to_string(i));
        EXPECT_EQ(ToRecord(result.value())->Get<std::string>(0).value(), "value" + std::to_string(i));
    }

    // 3. L0-only range (301-400)
    for (size_t i = 301; i <= 400; i++) {
        auto result = manager_->Get(pond::test::GenerateKey(i));
        VERIFY_RESULT_MSG(result, "Failed to read L0-only key: " + std::to_string(i));
        EXPECT_EQ(ToRecord(result.value())->Get<std::string>(0).value(), "value" + std::to_string(i));
    }
}

TEST_F(SSTableManagerTest, VersionedReadAfterMerge) {
    // Create initial L1 file with version 1
    {
        auto memtable = std::make_unique<MemTable>();
        auto record = std::make_unique<Record>(schema_);
        record->Set(0, std::string("value1"));
        ASSERT_TRUE(memtable->Put("test_key", record->Serialize(), 0 /* txn_id */).ok());
        VERIFY_RESULT(manager_->CreateSSTableFromMemTable(*memtable));
        VERIFY_RESULT(manager_->MergeL0ToL1());  // Move to L1
    }

    HybridTime version0 = GetNextHybridTime();

    // Create L0 files with newer versions
    {
        // Version 2
        auto memtable = std::make_unique<MemTable>();
        auto record = std::make_unique<Record>(schema_);
        record->Set(0, std::string("value2"));
        ASSERT_TRUE(memtable->Put("test_key", record->Serialize(), 0 /* txn_id */).ok());
        VERIFY_RESULT(manager_->CreateSSTableFromMemTable(*memtable));
    }

    HybridTime version1 = GetNextHybridTime();

    {
        // Version 3
        auto memtable = std::make_unique<MemTable>();
        auto record = std::make_unique<Record>(schema_);
        record->Set(0, std::string("value3"));
        ASSERT_TRUE(memtable->Put("test_key", record->Serialize(), 0 /* txn_id */).ok());
        VERIFY_RESULT(manager_->CreateSSTableFromMemTable(*memtable));
    }

    HybridTime version2 = GetNextHybridTime();

    // Merge L0 to L1 (this will include the original L1 file)
    VERIFY_RESULT(manager_->MergeL0ToL1());

    // Verify reads after merge
    auto result_v3 = manager_->Get("test_key", version2);
    ASSERT_TRUE(result_v3.ok());
    EXPECT_EQ(ToRecord(result_v3.value())->Get<std::string>(0).value(), "value3");

    // Older versions should be found after merge
    auto result_v2 = manager_->Get("test_key", version1);
    EXPECT_TRUE(result_v2.ok());
    EXPECT_EQ(ToRecord(result_v2.value())->Get<std::string>(0).value(), "value2");

    auto result_v1 = manager_->Get("test_key", version0);
    EXPECT_TRUE(result_v1.ok());
    EXPECT_EQ(ToRecord(result_v1.value())->Get<std::string>(0).value(), "value1");

    // Verify physical read count
    auto stats = manager_->GetStats();
    EXPECT_EQ(stats.physical_reads, 3);  // Should only read the merged file
}

}  // namespace
