#include "common/wal.h"

#include <memory>
#include <vector>

#include <gtest/gtest.h>

#include "common/append_only_fs.h"
#include "common/memory_append_only_fs.h"
#include "common/time.h"
#include "kv/kv_entry.h"
#include "kv/memtable.h"
#include "kv/record.h"
#include "test_helper.h"

namespace pond::kv {

class MemTableWALTest : public ::testing::Test {
protected:
    void SetUp() override {
        std::vector<ColumnSchema> columns = {
            {"id", ColumnType::INT32}, {"name", ColumnType::STRING}, {"value", ColumnType::BINARY}};
        schema_ = std::make_shared<Schema>(std::move(columns));

        fs_ = std::make_shared<common::MemoryAppendOnlyFileSystem>();
        wal_ = std::make_shared<common::WAL<KvEntry>>(fs_);
        ASSERT_TRUE(wal_->open("test.wal").ok());

        table_ = std::make_unique<MemTable>(schema_, wal_);
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
    std::shared_ptr<common::WAL<KvEntry>> wal_;
    std::unique_ptr<MemTable> table_;
};

TEST_F(MemTableWALTest, PutOperation) {
    // Put a record
    auto record = CreateTestRecord(1, "test", "value");
    VERIFY_RESULT(table_->Put("key1", record));

    // Read back WAL entries
    auto entries = wal_->read(0);
    VERIFY_RESULT(entries);
    ASSERT_EQ(entries.value().size(), 1);

    // Verify entry
    const auto& entry = entries.value()[0];
    EXPECT_EQ(entry.key, "key1");
    EXPECT_EQ(entry.type, EntryType::Put);
}

TEST_F(MemTableWALTest, DeleteOperation) {
    // First put a record
    auto record = CreateTestRecord(1, "test", "value");
    VERIFY_RESULT(table_->Put("key1", record));

    // Then delete it
    VERIFY_RESULT(table_->Delete("key1"));

    // Read back WAL entries
    auto entries = wal_->read(0);
    VERIFY_RESULT(entries);
    ASSERT_EQ(entries.value().size(), 2);

    // Verify entries
    const auto& put_entry = entries.value()[0];
    EXPECT_EQ(put_entry.key, "key1");
    EXPECT_EQ(put_entry.type, EntryType::Put);

    const auto& delete_entry = entries.value()[1];
    EXPECT_EQ(delete_entry.key, "key1");
    EXPECT_EQ(delete_entry.type, EntryType::Delete);
}

TEST_F(MemTableWALTest, UpdateColumnOperation) {
    // First put a record
    auto record = CreateTestRecord(1, "test", "value");
    VERIFY_RESULT(table_->Put("key1", record));

    // Update a column
    common::DataChunk new_value(reinterpret_cast<const uint8_t*>("new_value"), 9);
    VERIFY_RESULT(table_->UpdateColumn("key1", "value", new_value));

    // Read back WAL entries
    auto entries = wal_->read(0);
    VERIFY_RESULT(entries);
    ASSERT_EQ(entries.value().size(), 2);

    // Verify entries
    const auto& put_entry = entries.value()[0];
    EXPECT_EQ(put_entry.key, "key1");
    EXPECT_EQ(put_entry.type, EntryType::Put);

    const auto& update_entry = entries.value()[1];
    EXPECT_EQ(update_entry.key, "key1");
    EXPECT_EQ(update_entry.type, EntryType::Put);  // UpdateColumn uses Put type
}

TEST_F(MemTableWALTest, RecoveryFromWAL) {
    // Put some records
    auto record1 = CreateTestRecord(1, "test1", "value1");
    auto record2 = CreateTestRecord(2, "test2", "value2");
    VERIFY_RESULT(table_->Put("key1", record1));
    VERIFY_RESULT(table_->Put("key2", record2));
    VERIFY_RESULT(table_->Delete("key1"));

    // Create a new MemTable with the same WAL and recover
    auto new_table = std::make_unique<MemTable>(schema_, wal_);
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

}  // namespace pond::kv
