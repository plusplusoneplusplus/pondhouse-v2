#include "kv/table.h"

#include <memory>
#include <vector>

#include <gtest/gtest.h>

#include "common/memory_append_only_fs.h"
#include "test_helper.h"

namespace pond::kv {

class TableTest : public ::testing::Test {
protected:
    void SetUp() override {
        std::vector<ColumnSchema> columns = {
            {"id", ColumnType::INT32}, {"name", ColumnType::STRING}, {"value", ColumnType::BINARY}};
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
    EXPECT_EQ(updated_record->Get<int32_t>(0).value(), 1);  // unchanged
    EXPECT_EQ(updated_record->Get<std::string>(1).value(), "test");  // unchanged
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

}  // namespace pond::kv
