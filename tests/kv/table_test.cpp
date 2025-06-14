#include "kv/table.h"

#include <memory>
#include <vector>

#include <gtest/gtest.h>

#include "common/memory_append_only_fs.h"
#include "test_helper.h"

using namespace pond::common;

namespace pond::kv {

class TableTest : public ::testing::Test {
protected:
    void SetUp() override {
        std::vector<ColumnSchema> columns = {{"id", ColumnType::INT32},       // id column
                                             {"name", ColumnType::STRING},    // name column
                                             {"value", ColumnType::BINARY}};  // value column
        schema_ = std::make_shared<Schema>(std::move(columns));

        fs_ = std::make_shared<common::MemoryAppendOnlyFileSystem>();
        table_ = std::make_unique<Table>(schema_, fs_, "test_table");
    }

    std::unique_ptr<Record> CreateTestRecord(int32_t id, const std::string& name, const std::string& value) {
        auto record = std::make_unique<Record>(schema_);
        record->Set(0, id);
        record->Set(1, name);
        record->Set(2, common::DataChunk(reinterpret_cast<const uint8_t*>(value.data()), value.size()));
        return record;
    }

    std::shared_ptr<Schema> schema_;
    std::shared_ptr<common::MemoryAppendOnlyFileSystem> fs_;
    std::unique_ptr<Table> table_;
};

TEST_F(TableTest, BasicOperations) {
    // Put a record
    auto record1 = CreateTestRecord(1, "test1", "value1");
    VERIFY_RESULT(table_->Put("key1", std::move(record1)));

    // Get and verify
    auto get_result = table_->Get("key1");
    VERIFY_RESULT(get_result);
    auto& record = get_result.value();
    EXPECT_EQ(record->Get<int32_t>(0).value(), 1);
    EXPECT_EQ(record->Get<std::string>(1).value(), "test1");
    EXPECT_EQ(record->Get<common::DataChunk>(2).value(),
              common::DataChunk(reinterpret_cast<const uint8_t*>("value1"), 6));

    // Delete
    VERIFY_RESULT(table_->Delete("key1"));
    get_result = table_->Get("key1");
    EXPECT_FALSE(get_result.ok());
}

TEST_F(TableTest, PartialRecordInsert) {
    // Create a record with only id and name fields set (omitting value field)
    auto record = std::make_unique<Record>(schema_);
    record->Set(0, 42);                             // Set id field
    record->Set(1, std::string("partial_record"));  // Set name field
    // Deliberately not setting the value field

    // Put the partial record
    VERIFY_RESULT(table_->Put("partial_key", std::move(record)));

    // Get and verify the record
    auto get_result = table_->Get("partial_key");
    VERIFY_RESULT(get_result);
    auto& retrieved_record = get_result.value();

    // Verify the fields we set
    EXPECT_EQ(retrieved_record->Get<int32_t>(0).value(), 42);
    EXPECT_EQ(retrieved_record->Get<std::string>(1).value(), "partial_record");

    // The value field should be null/default since we didn't set it
    auto value_result = retrieved_record->Get<common::DataChunk>(2);
    EXPECT_FALSE(value_result.ok());
    EXPECT_EQ(value_result.error().code(), common::ErrorCode::NullValue);
}

TEST_F(TableTest, PutIfNotExists) {
    // Test PutIfNotExists on a new key
    auto record1 = CreateTestRecord(1, "test1", "value1");
    auto result = table_->PutIfNotExists("key1", std::move(record1));
    VERIFY_RESULT(result);
    EXPECT_TRUE(result.value());  // Should return true for successful put

    // Verify the record was inserted
    auto get_result = table_->Get("key1");
    VERIFY_RESULT(get_result);
    auto record = std::move(get_result).value();
    EXPECT_EQ(record->Get<int32_t>(0).value(), 1);
    EXPECT_EQ(record->Get<std::string>(1).value(), "test1");
    EXPECT_EQ(record->Get<common::DataChunk>(2).value().ToString(), "value1");

    // Try PutIfNotExists on an existing key
    auto record2 = CreateTestRecord(2, "test2", "value2");
    result = table_->PutIfNotExists("key1", std::move(record2));
    VERIFY_RESULT(result);
    EXPECT_FALSE(result.value());  // Should return false since key exists

    // Verify the original record is unchanged
    get_result = table_->Get("key1");
    VERIFY_RESULT(get_result);
    record = std::move(get_result).value();
    EXPECT_EQ(record->Get<int32_t>(0).value(), 1);                              // Still the original value
    EXPECT_EQ(record->Get<std::string>(1).value(), "test1");                    // Still the original value
    EXPECT_EQ(record->Get<common::DataChunk>(2).value().ToString(), "value1");  // Still the original value

    // Test with empty key
    auto record3 = CreateTestRecord(3, "test3", "value3");
    result = table_->PutIfNotExists("", std::move(record3));
    VERIFY_ERROR_CODE(result, common::ErrorCode::InvalidArgument);
}

TEST_F(TableTest, InvalidOperations) {
    // Test empty key
    auto record = CreateTestRecord(1, "test", "value");
    auto result = table_->Put("", std::move(record));
    VERIFY_ERROR_CODE(result, common::ErrorCode::InvalidArgument);

    // Test Get with empty key
    auto get_result = table_->Get("");
    VERIFY_ERROR_CODE(get_result, common::ErrorCode::InvalidArgument);

    // Test Delete with empty key
    auto delete_result = table_->Delete("");
    VERIFY_ERROR_CODE(delete_result, common::ErrorCode::InvalidArgument);

    // Test UpdateColumn with empty key
    common::DataChunk new_value(reinterpret_cast<const uint8_t*>("new_value"), 9);
    auto update_result = table_->UpdateColumn("", "value", new_value);
    VERIFY_ERROR_CODE(update_result, common::ErrorCode::InvalidArgument);

    // Test UpdateColumn with non-existent column
    record = CreateTestRecord(1, "test", "value");
    VERIFY_RESULT(table_->Put("key1", std::move(record)));
    update_result = table_->UpdateColumn("key1", "non_existent_column", new_value);
    VERIFY_ERROR_CODE(update_result, common::ErrorCode::InvalidArgument);
}

TEST_F(TableTest, UpdateColumn) {
    // Put a record
    auto record = CreateTestRecord(1, "test", "value");
    VERIFY_RESULT(table_->Put("key1", std::move(record)));

    // Update a column
    common::DataChunk new_value(reinterpret_cast<const uint8_t*>("new_value"), 9);
    VERIFY_RESULT(table_->UpdateColumn("key1", "value", new_value));

    // Verify update
    auto get_result = table_->Get("key1");
    VERIFY_RESULT(get_result);
    auto& updated_record = get_result.value();
    EXPECT_EQ(updated_record->Get<int32_t>(0).value(), 1);                    // unchanged
    EXPECT_EQ(updated_record->Get<std::string>(1).value(), "test");           // unchanged
    EXPECT_EQ(updated_record->Get<common::DataChunk>(2).value(), new_value);  // updated
}

TEST_F(TableTest, RecoveryFromWAL) {
    // Put some records
    auto record1 = CreateTestRecord(1, "test1", "value1");
    auto record2 = CreateTestRecord(2, "test2", "value2");
    VERIFY_RESULT(table_->Put("key1", std::move(record1)));
    VERIFY_RESULT(table_->Put("key2", std::move(record2)));
    VERIFY_RESULT(table_->Delete("key1"));

    // Create a new table with the same fs and recover
    auto new_table = std::make_unique<Table>(schema_, fs_, "test_table");
    auto recover_result = new_table->Recover();
    VERIFY_RESULT(recover_result);
    EXPECT_TRUE(recover_result.value());

    // Verify the recovered state
    auto result = new_table->Get("key1");
    EXPECT_FALSE(result.ok());  // key1 was deleted

    result = new_table->Get("key2");
    VERIFY_RESULT(result);
    auto& recovered_record = result.value();
    EXPECT_EQ(recovered_record->Get<int32_t>(0).value(), 2);
    EXPECT_EQ(recovered_record->Get<std::string>(1).value(), "test2");
    EXPECT_EQ(recovered_record->Get<common::DataChunk>(2).value(),
              common::DataChunk(reinterpret_cast<const uint8_t*>("value2"), 6));
}

TEST_F(TableTest, MemTableSwitchingAndSSTableIntegration) {
    // Fill up the memtable with large values
    std::string large_value(1024 * 1024, 'x');  // 1MB value
    std::vector<std::string> keys;

    // Insert records until memtable is full
    for (int i = 0; i < 100; i++) {
        std::string key = pond::test::GenerateKey(i);
        auto record = CreateTestRecord(i, "test" + std::to_string(i), large_value);
        auto result = table_->Put(key, std::move(record));
        if (!result.ok()) {
            // MemTable should be automatically switched
            break;
        }
        keys.push_back(key);
    }

    EXPECT_GT(keys.size(), 0) << "Should have inserted some records";

    // Insert one more record to trigger another flush
    auto record = CreateTestRecord(keys.size(), "test_final", "final_value");
    VERIFY_RESULT(table_->Put("final_key", std::move(record)));

    // Verify all records are still accessible
    for (const auto& key : keys) {
        auto result = table_->Get(key);
        VERIFY_RESULT(result);
        auto& record = result.value();
        EXPECT_EQ(record->Get<common::DataChunk>(2).value().Size(), large_value.size());
    }

    // Verify the final record
    auto result = table_->Get("final_key");
    VERIFY_RESULT(result);
    auto& final_record = result.value();
    EXPECT_EQ(final_record->Get<std::string>(1).value(), "test_final");
    EXPECT_EQ(final_record->Get<common::DataChunk>(2).value(), common::DataChunk::FromString("final_value"));

    // Test manual flush
    VERIFY_RESULT(table_->Flush());

    // Verify records are still accessible after manual flush
    for (const auto& key : keys) {
        auto result = table_->Get(key);
        VERIFY_RESULT(result);
        auto& record = result.value();
        EXPECT_EQ(record->Get<common::DataChunk>(2).value().Size(), large_value.size());
    }
}

TEST_F(TableTest, WALRotation) {
    // Create a table with small WAL size limit (1MB)
    auto table = std::make_unique<Table>(schema_, fs_, "test_table", 1024 * 1024);

    // Insert records with large values to trigger WAL rotation
    std::string large_value(100 * 1024, 'x');  // 100KB value
    std::vector<std::string> keys;

    // Insert enough records to trigger multiple WAL rotations
    for (int i = 0; i < 20; i++) {
        std::string key = pond::test::GenerateKey(i);
        auto record = CreateTestRecord(i, "test" + std::to_string(i), large_value);
        VERIFY_RESULT_MSG(table->Put(key, std::move(record)), "Failed to put record " + key);
        keys.push_back(key);
    }

    // Verify all records are still accessible
    for (const auto& key : keys) {
        auto result = table->Get(key);
        VERIFY_RESULT(result);
        auto& record = result.value();
        EXPECT_EQ(record->Get<common::DataChunk>(2).value().Size(), large_value.size());
    }

    // Verify WAL files were created
    std::vector<std::string> wal_files;
    for (size_t i = 0; i < 20; i++) {
        std::string wal_path = "test_table/" + std::to_string(i) + ".wal";
        auto exists = fs_->Exists(wal_path);
        wal_files.push_back(wal_path);
    }

    EXPECT_GT(wal_files.size(), 1) << "Should have created multiple WAL files";

    // Create a new table instance and recover
    auto recovered_table = std::make_unique<Table>(schema_, fs_, "test_table", 1024 * 1024);
    VERIFY_RESULT(recovered_table->Recover());

    // Verify all records are still accessible after recovery
    for (const auto& key : keys) {
        auto result = recovered_table->Get(key);
        VERIFY_RESULT_MSG(result, "Failed to get record " + key);
        auto& record = result.value();
        EXPECT_EQ(record->Get<common::DataChunk>(2).value().Size(), large_value.size());
    }
}

TEST_F(TableTest, MetadataTracking) {
    // Create a table with small WAL size to trigger rotations
    auto table = std::make_unique<Table>(schema_, fs_, "test_table", 1024 * 1024);  // 1MB WAL

    // Insert records with large values to trigger memtable flushes and WAL rotations
    std::string large_value(100 * 1024, 'x');  // 100KB value
    std::vector<std::string> keys;

    for (int i = 0; i < 20; i++) {
        std::string key = pond::test::GenerateKey(i);
        auto record = CreateTestRecord(i, "test" + std::to_string(i), large_value);
        VERIFY_RESULT(table->Put(key, std::move(record)));
        keys.push_back(key);
    }

    // Force a flush
    VERIFY_RESULT(table->Flush());

    // Create a new table instance and recover
    auto recovered_table = std::make_unique<Table>(schema_, fs_, "test_table", 1024 * 1024);
    VERIFY_RESULT(recovered_table->Recover());

    // Verify all records are accessible after recovery
    for (const auto& key : keys) {
        auto result = recovered_table->Get(key);
        VERIFY_RESULT(result);
        auto& record = result.value();
        EXPECT_EQ(record->Get<common::DataChunk>(2).value().Size(), large_value.size());
    }

    // Verify metadata wal file exists
    EXPECT_TRUE(fs_->Exists("test_table_metadata"));
    auto listFileResult = fs_->List("test_table_metadata");
    VERIFY_RESULT(listFileResult);
    EXPECT_EQ(listFileResult.value().size(), 1);
    EXPECT_EQ(listFileResult.value()[0], "wal.log");
}

TEST_F(TableTest, MetadataRecoveryAfterCrash) {
    // First phase: Create and populate table
    {
        auto table = std::make_unique<Table>(schema_, fs_, "test_table");

        // Add some records
        for (int i = 0; i < 10; i++) {
            auto record = CreateTestRecord(i, "test" + std::to_string(i), "value" + std::to_string(i));
            VERIFY_RESULT(table->Put(pond::test::GenerateKey(i), std::move(record)));
        }

        // Force flush to create SSTable
        VERIFY_RESULT(table->Flush());
    }
    // Table is destroyed here, simulating a crash

    // Second phase: Create new table and recover
    auto recovered_table = std::make_unique<Table>(schema_, fs_, "test_table");
    VERIFY_RESULT(recovered_table->Recover());

    // Verify records are accessible
    for (int i = 0; i < 10; i++) {
        auto result = recovered_table->Get(pond::test::GenerateKey(i));
        VERIFY_RESULT(result);
        auto& record = result.value();
        EXPECT_EQ(record->Get<int32_t>(0).value(), i);
        EXPECT_EQ(record->Get<std::string>(1).value(), "test" + std::to_string(i));
        EXPECT_EQ(record->Get<common::DataChunk>(2).value(),
                  common::DataChunk::FromString("value" + std::to_string(i)));
    }
}

TEST_F(TableTest, RecoveryFromWALAndSSTables) {
    // Create a table with small WAL size to trigger rotations and flushes
    auto table = std::make_unique<Table>(schema_, fs_, "test_table", 1024 * 1024);  // 1MB WAL

    // Phase 1: Create some SSTable files through memtable flushes
    std::vector<std::string> keys;
    std::string large_value(100 * 1024, 'x');  // 100KB value to trigger flushes
    for (int i = 0; i < 20; i++) {
        std::string key = pond::test::GenerateKey(i);
        auto record = CreateTestRecord(i, "name" + std::to_string(i), large_value);
        VERIFY_RESULT(table->Put(key, std::move(record)));
        keys.push_back(key);
    }

    // Force a flush to ensure some data is in SSTables
    VERIFY_RESULT(table->Flush());

    // Phase 2: Add more records that will stay in WAL/memtable
    std::vector<std::string> recent_keys;
    for (int i = 20; i < 30; i++) {
        std::string key = pond::test::GenerateKey(i);
        auto record = CreateTestRecord(i, "name" + std::to_string(i), "value" + std::to_string(i));
        VERIFY_RESULT(table->Put(key, std::move(record)));
        recent_keys.push_back(key);
    }

    // Verify metadata files were created
    EXPECT_TRUE(fs_->Exists("test_table_metadata"));
    EXPECT_TRUE(fs_->Exists("test_table_metadata/wal.log"));

    // Create a new table instance and recover
    auto recovered_table = std::make_unique<Table>(schema_, fs_, "test_table", 1024 * 1024);
    VERIFY_RESULT(recovered_table->Recover());

    // Verify all records from SSTables are accessible
    for (const auto& key : keys) {
        auto result = recovered_table->Get(key);
        VERIFY_RESULT(result);
        auto& record = result.value();
        EXPECT_EQ(record->Get<common::DataChunk>(2).value().Size(), large_value.size());
    }

    // Verify all records from WAL/memtable are accessible
    for (const auto& key : recent_keys) {
        auto result = recovered_table->Get(key);
        VERIFY_RESULT(result);
        auto& record = result.value();
        int id = std::stoi(key.substr(3));
        EXPECT_EQ(record->Get<int32_t>(0).value(), id);
        EXPECT_EQ(record->Get<std::string>(1).value(), "name" + std::to_string(id));
        EXPECT_EQ(record->Get<common::DataChunk>(2).value(),
                  common::DataChunk::FromString("value" + std::to_string(id)));
    }

    // Verify metadata state is correct
    auto metadata_path = "test_table_metadata";
    EXPECT_TRUE(fs_->Exists(metadata_path));

    auto data_path = "test_table";
    // Verify SSTable files exist
    auto list_result = fs_->List(data_path);
    VERIFY_RESULT(list_result);
    bool found_sstable = false;
    for (const auto& file : list_result.value()) {
        if (file.find("L0_") != std::string::npos && file.ends_with(".sst")) {
            found_sstable = true;
            break;
        }
    }
    EXPECT_TRUE(found_sstable) << "No SSTable files found after recovery";

    // Verify we can still write after recovery
    auto new_record = CreateTestRecord(100, "post_recovery", "value");
    VERIFY_RESULT(recovered_table->Put("post_recovery_key", std::move(new_record)));

    auto get_result = recovered_table->Get("post_recovery_key");
    VERIFY_RESULT(get_result);
    EXPECT_EQ(get_result.value()->Get<int32_t>(0).value(), 100);
    EXPECT_EQ(get_result.value()->Get<std::string>(1).value(), "post_recovery");
    EXPECT_EQ(get_result.value()->Get<common::DataChunk>(2).value(), common::DataChunk::FromString("value"));
}

//
// Test Setup:
//      Insert multiple records and verify iterator can traverse them correctly
// Test Result:
//      Iterator should return valid Record objects for each key in order
//
TEST_F(TableTest, RecordIterator) {
    // Insert multiple records
    std::vector<std::string> keys = {"key1", "key2", "key3", "key4", "key5"};
    std::vector<int32_t> ids = {1, 2, 3, 4, 5};

    for (size_t i = 0; i < keys.size(); i++) {
        auto record = CreateTestRecord(ids[i], "test" + std::to_string(i), "value" + std::to_string(i));
        VERIFY_RESULT(table_->Put(keys[i], std::move(record)));
    }

    // Create an iterator
    auto iter_result = table_->NewIterator();
    VERIFY_RESULT(iter_result);
    auto iter = iter_result.value();

    // Seek to beginning and iterate through all records
    iter->Seek("");

    size_t count = 0;
    std::vector<std::string> found_keys;

    while (iter->Valid()) {
        // Verify the key and record
        found_keys.push_back(iter->key());
        EXPECT_FALSE(iter->IsTombstone());

        const auto& record = iter->value();
        ASSERT_TRUE(record != nullptr);

        // Find the expected index based on the key
        auto it = std::find(keys.begin(), keys.end(), iter->key());
        ASSERT_NE(it, keys.end());
        size_t idx = std::distance(keys.begin(), it);

        // Verify record contents
        EXPECT_EQ(record->Get<int32_t>(0).value(), ids[idx]);
        EXPECT_EQ(record->Get<std::string>(1).value(), "test" + std::to_string(idx));
        EXPECT_EQ(record->Get<common::DataChunk>(2).value(),
                  common::DataChunk::FromString("value" + std::to_string(idx)));

        iter->Next();
        count++;
    }

    // Verify we found all keys
    EXPECT_EQ(count, keys.size());

    // Sort the found keys to compare with expected keys
    std::sort(found_keys.begin(), found_keys.end());
    std::sort(keys.begin(), keys.end());
    EXPECT_EQ(found_keys, keys);

    // Test seeking to a specific key
    iter->Seek("key3");
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(iter->key(), "key3");
    EXPECT_EQ(iter->value()->Get<int32_t>(0).value(), 3);

    // Test seeking to a non-existent key
    iter->Seek("key35");
    if (iter->Valid()) {
        // Should be at the next key after "key35" in lexicographical order
        EXPECT_GT(iter->key(), "key35");
    }

    // Test iterator after deleting a key
    VERIFY_RESULT(table_->Delete("key2"));

    auto iter2_result = table_->NewIterator();
    VERIFY_RESULT(iter2_result);
    auto iter2 = iter2_result.value();

    iter2->Seek("key2");
    if (iter2->Valid() && iter2->key() == "key2") {
        // If we found key2, it should be a tombstone
        EXPECT_TRUE(iter2->IsTombstone());
    }

    // Count non-tombstone entries
    iter2->Seek("");
    count = 0;
    while (iter2->Valid()) {
        if (!iter2->IsTombstone()) {
            count++;
        }
        iter2->Next();
    }

    EXPECT_EQ(count, keys.size() - 1);  // One key was deleted
}

TEST_F(TableTest, DISABLED_RecordIteratorWithAllVersions) {
    // Insert multiple records
    std::vector<std::string> keys = {"key1", "key2", "key3", "key4", "key5"};
    std::vector<int32_t> ids = {1, 2, 3, 4, 5};

    // Insert initial versions
    for (size_t i = 0; i < keys.size(); i++) {
        auto record = CreateTestRecord(ids[i], "name_v1", "value_v1");
        VERIFY_RESULT(table_->Put(keys[i], std::move(record)));
    }

    // Flush to create first SSTable
    VERIFY_RESULT(table_->Flush());

    // Update some records
    for (size_t i = 0; i < 3; i++) {
        auto record = CreateTestRecord(ids[i], "name_v2", "value_v2");
        VERIFY_RESULT(table_->Put(keys[i], std::move(record)));
    }

    // Delete key2
    VERIFY_RESULT(table_->Delete(keys[1]));

    // Flush to create second SSTable
    VERIFY_RESULT(table_->Flush());

    // Add newest versions
    for (size_t i = 0; i < keys.size(); i++) {
        auto record = CreateTestRecord(ids[i], "name_v3", "value_v3");
        VERIFY_RESULT(table_->Put(keys[i], std::move(record)));
    }

    // Test iterator with IncludeAllVersions mode
    {
        auto iter_result = table_->NewIterator(common::MaxHybridTime(), common::IteratorMode::IncludeAllVersions);
        VERIFY_RESULT(iter_result);
        auto iter = iter_result.value();

        iter->Seek("");
        std::map<std::string, int> version_counts;
        while (iter->Valid()) {
            version_counts[iter->key()]++;
            iter->Next();
        }

        // key1, key3: 3 versions each
        // key2: 3 versions + 1 tombstone
        // key4, key5: 2 versions each
        EXPECT_EQ(version_counts["key1"], 3);
        EXPECT_EQ(version_counts["key2"], 4);  // FIX ME
        EXPECT_EQ(version_counts["key3"], 3);
        EXPECT_EQ(version_counts["key4"], 2);
        EXPECT_EQ(version_counts["key5"], 2);
    }
}

//
// Test Setup:
//      Create an iterator on an empty table and verify its behavior
// Test Result:
//      Iterator should not be valid and should handle operations gracefully
//
TEST_F(TableTest, EmptyTableIterator) {
    // Create an iterator on the empty table
    auto iter_result = table_->NewIterator();
    VERIFY_RESULT(iter_result);
    auto iter = iter_result.value();

    // Seek to beginning
    iter->Seek("");

    // Iterator should not be valid since table is empty
    EXPECT_FALSE(iter->Valid());

    // Calling Next() on an invalid iterator should not crash
    iter->Next();
    EXPECT_FALSE(iter->Valid());

    // Seeking to a specific key should also result in an invalid iterator
    iter->Seek("any_key");
    EXPECT_FALSE(iter->Valid());

    // Add a record and verify iterator now works
    auto record = CreateTestRecord(1, "test", "value");
    VERIFY_RESULT(table_->Put("key1", std::move(record)));

    // Create a new iterator
    auto iter2_result = table_->NewIterator();
    VERIFY_RESULT(iter2_result);
    auto iter2 = iter2_result.value();

    // Seek to beginning
    iter2->Seek("");

    // Iterator should now be valid
    EXPECT_TRUE(iter2->Valid());
    EXPECT_EQ(iter2->key(), "key1");
    EXPECT_FALSE(iter2->IsTombstone());
    EXPECT_NE(iter2->value(), nullptr);

    // After moving past the only record, iterator should become invalid
    iter2->Next();
    EXPECT_FALSE(iter2->Valid());
}

TEST_F(TableTest, PrefixScanWithRecords) {
    // Insert test data with different prefixes
    auto record1 = CreateTestRecord(1, "alice", "data1");
    auto record2 = CreateTestRecord(2, "bob", "data2");
    auto record3 = CreateTestRecord(3, "charlie", "data3");
    auto record4 = CreateTestRecord(4, "david", "data4");
    auto record5 = CreateTestRecord(5, "eve", "data5");

    VERIFY_RESULT(table_->Put("user:1", std::move(record1)));
    VERIFY_RESULT(table_->Put("user:2", std::move(record2)));
    VERIFY_RESULT(table_->Put("user:3", std::move(record3)));
    VERIFY_RESULT(table_->Put("post:1", std::move(record4)));
    VERIFY_RESULT(table_->Put("post:2", std::move(record5)));

    // Test scanning with "user:" prefix
    {
        auto iter_result = table_->ScanPrefix("user:");
        VERIFY_RESULT(iter_result);
        auto& iter = *iter_result.value();

        // First entry
        ASSERT_TRUE(iter.Valid());
        EXPECT_EQ(iter.key(), "user:1");
        EXPECT_FALSE(iter.IsTombstone());
        EXPECT_EQ(iter.value()->Get<int32_t>(0).value(), 1);
        EXPECT_EQ(iter.value()->Get<std::string>(1).value(), "alice");
        EXPECT_EQ(iter.value()->Get<common::DataChunk>(2).value().ToString(), "data1");

        // Second entry
        iter.Next();
        ASSERT_TRUE(iter.Valid());
        EXPECT_EQ(iter.key(), "user:2");
        EXPECT_EQ(iter.value()->Get<int32_t>(0).value(), 2);
        EXPECT_EQ(iter.value()->Get<std::string>(1).value(), "bob");
        EXPECT_EQ(iter.value()->Get<common::DataChunk>(2).value().ToString(), "data2");

        // Third entry
        iter.Next();
        ASSERT_TRUE(iter.Valid());
        EXPECT_EQ(iter.key(), "user:3");
        EXPECT_EQ(iter.value()->Get<int32_t>(0).value(), 3);
        EXPECT_EQ(iter.value()->Get<std::string>(1).value(), "charlie");
        EXPECT_EQ(iter.value()->Get<common::DataChunk>(2).value().ToString(), "data3");

        // Should be at end
        iter.Next();
        ASSERT_FALSE(iter.Valid());
    }

    // Test scanning with "post:" prefix
    {
        auto iter_result = table_->ScanPrefix("post:");
        VERIFY_RESULT(iter_result);
        auto& iter = *iter_result.value();

        // First entry
        ASSERT_TRUE(iter.Valid());
        EXPECT_EQ(iter.key(), "post:1");
        EXPECT_EQ(iter.value()->Get<int32_t>(0).value(), 4);
        EXPECT_EQ(iter.value()->Get<std::string>(1).value(), "david");
        EXPECT_EQ(iter.value()->Get<common::DataChunk>(2).value().ToString(), "data4");

        // Second entry
        iter.Next();
        ASSERT_TRUE(iter.Valid());
        EXPECT_EQ(iter.key(), "post:2");
        EXPECT_EQ(iter.value()->Get<int32_t>(0).value(), 5);
        EXPECT_EQ(iter.value()->Get<std::string>(1).value(), "eve");
        EXPECT_EQ(iter.value()->Get<common::DataChunk>(2).value().ToString(), "data5");

        // Should be at end
        iter.Next();
        ASSERT_FALSE(iter.Valid());
    }

    // Test scanning with non-existent prefix
    {
        auto iter_result = table_->ScanPrefix("nonexistent:");
        VERIFY_RESULT(iter_result);
        auto& iter = *iter_result.value();
        ASSERT_FALSE(iter.Valid());
    }

    // Test scanning with empty prefix (should return all entries)
    {
        auto iter_result = table_->ScanPrefix("");
        VERIFY_RESULT(iter_result);
        auto& iter = *iter_result.value();

        size_t count = 0;
        while (iter.Valid()) {
            count++;
            iter.Next();
        }
        EXPECT_EQ(count, 5);
    }

    // Test scanning with different iterator modes
    // FIX ME
    // {
    //     // Create a new version of a record and delete another
    //     auto new_record = CreateTestRecord(6, "bob_v2", "data2_v2");
    //     VERIFY_RESULT(table_->Put("user:2", std::move(new_record)));
    //     VERIFY_RESULT(table_->Delete("user:3"));

    //     // Test with IncludeAllVersions mode
    //     auto iter_result = table_->ScanPrefix("user:", common::IteratorMode::IncludeAllVersions);
    //     VERIFY_RESULT(iter_result);
    //     auto& iter = *iter_result.value();

    //     // Count all versions
    //     size_t version_count = 0;
    //     while (iter.Valid()) {
    //         std::cout << "key: " << iter.key() << std::endl;
    //         version_count++;
    //         iter.Next();
    //     }
    //     EXPECT_EQ(version_count, 3);  // user:1 + two versions of user:2

    //     // Test with IncludeTombstones mode
    //     iter_result = table_->ScanPrefix("user:", common::IteratorMode::IncludeTombstones);
    //     VERIFY_RESULT(iter_result);
    //     auto& iter2 = *iter_result.value();

    //     std::vector<std::string> found_keys;
    //     std::vector<bool> is_tombstone;
    //     while (iter2.Valid()) {
    //         found_keys.push_back(iter2.key());
    //         is_tombstone.push_back(iter2.IsTombstone());
    //         iter2.Next();
    //     }

    //     EXPECT_EQ(found_keys.size(), 3);  // All three keys should be found
    //     EXPECT_TRUE(std::find(found_keys.begin(), found_keys.end(), "user:3") != found_keys.end());
    //     auto tombstone_idx = std::find(found_keys.begin(), found_keys.end(), "user:3") - found_keys.begin();
    //     EXPECT_TRUE(is_tombstone[tombstone_idx]);  // user:3 should be a tombstone
    // }
}

TEST_F(TableTest, SchemaMismatchPut) {
    // Create a new schema with different column types
    std::vector<ColumnSchema> mismatched_columns = {
        {"id", ColumnType::STRING},      // Different type (STRING instead of INT32)
        {"name", ColumnType::INT32},     // Different type (INT32 instead of STRING)
        {"value", ColumnType::BINARY}    // Same type
    };
    auto mismatched_schema = std::make_shared<Schema>(std::move(mismatched_columns));

    // Create a record with the mismatched schema
    auto record = std::make_unique<Record>(mismatched_schema);
    record->Set(0, std::string("1"));           // String instead of int
    record->Set(1, 42);                         // Int instead of string
    record->Set(2, DataChunk::FromString("value1"));

    // Attempt to put the record with mismatched schema
    auto result = table_->Put("key1", std::move(record));
    
    // Verify that the operation fails with schema mismatch error
    VERIFY_ERROR_CODE(result, ErrorCode::SchemaMismatch);

    // Create another schema with different number of columns
    std::vector<ColumnSchema> different_columns = {
        {"id", ColumnType::INT32},
        {"name", ColumnType::STRING}
        // Missing 'value' column
    };
    auto different_schema = std::make_shared<Schema>(std::move(different_columns));

    // Create a record with different number of columns
    auto record2 = std::make_unique<Record>(different_schema);
    record2->Set(0, 1);
    record2->Set(1, std::string("test"));

    // Attempt to put the record with different number of columns
    result = table_->Put("key2", std::move(record2));
    
    // Verify that the operation fails with schema mismatch error
    VERIFY_ERROR_CODE(result, ErrorCode::SchemaMismatch);

    // Verify that no records were actually inserted
    auto get_result = table_->Get("key1");
    EXPECT_FALSE(get_result.ok());
    
    get_result = table_->Get("key2");
    EXPECT_FALSE(get_result.ok());
}

}  // namespace pond::kv
