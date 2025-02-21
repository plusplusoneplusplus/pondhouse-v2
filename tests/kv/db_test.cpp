#include "kv/db.h"

#include <memory>
#include <vector>

#include <gtest/gtest.h>

#include "common/memory_append_only_fs.h"
#include "test_helper.h"

using namespace pond::common;

namespace pond::kv {

class DBTest : public ::testing::Test {
protected:
    void SetUp() override {
        fs_ = std::make_shared<common::MemoryAppendOnlyFileSystem>();
        db_ = std::make_unique<DB>(fs_, "test_db");
    }

    std::shared_ptr<Schema> CreateTestSchema(const std::string& prefix = "") const {
        std::vector<ColumnSchema> columns = {{prefix + "id", ColumnType::INT32},
                                             {prefix + "name", ColumnType::STRING},
                                             {prefix + "value", ColumnType::BINARY}};
        return std::make_shared<Schema>(std::move(columns));
    }

    std::unique_ptr<Record> CreateTestRecord(const std::shared_ptr<Schema>& schema,
                                             int32_t id,
                                             const std::string& name,
                                             const std::string& value) {
        auto record = std::make_unique<Record>(schema);
        record->Set(0, id);
        record->Set(1, name);
        record->Set(2, DataChunk::FromString(value));
        return record;
    }

    std::shared_ptr<common::MemoryAppendOnlyFileSystem> fs_;
    std::unique_ptr<DB> db_;
};

TEST_F(DBTest, CreateAndGetTable) {
    // Create a table
    auto schema = CreateTestSchema();
    VERIFY_RESULT(db_->CreateTable("test_table", schema));

    // Get the table
    auto table_result = db_->GetTable("test_table");
    VERIFY_RESULT(table_result);
    auto table = table_result.value();
    EXPECT_EQ(table->schema()->num_columns(), schema->num_columns());

    // Try to create the same table again
    auto result = db_->CreateTable("test_table", schema);
    VERIFY_ERROR_CODE(result, ErrorCode::TableAlreadyOpen);

    // Try to get a non-existent table
    auto not_found = db_->GetTable("non_existent");
    VERIFY_ERROR_CODE(not_found, ErrorCode::TableNotFound);
}

TEST_F(DBTest, ListTables) {
    // Initially no tables
    auto list_result = db_->ListTables();
    VERIFY_RESULT(list_result);
    EXPECT_TRUE(list_result.value().empty());

    // Create some tables
    auto schema1 = CreateTestSchema("t1_");
    auto schema2 = CreateTestSchema("t2_");
    VERIFY_RESULT(db_->CreateTable("table1", schema1));
    VERIFY_RESULT(db_->CreateTable("table2", schema2));

    // List tables
    list_result = db_->ListTables();
    VERIFY_RESULT(list_result);
    auto tables = list_result.value();
    EXPECT_EQ(tables.size(), 2);
    EXPECT_TRUE(std::find(tables.begin(), tables.end(), "table1") != tables.end());
    EXPECT_TRUE(std::find(tables.begin(), tables.end(), "table2") != tables.end());
}

TEST_F(DBTest, DropTable) {
    // Create a table
    auto schema = CreateTestSchema();
    VERIFY_RESULT(db_->CreateTable("test_table", schema));

    // Drop the table
    VERIFY_RESULT(db_->DropTable("test_table"));

    // Try to get the dropped table
    auto result = db_->GetTable("test_table");
    VERIFY_ERROR_CODE(result, ErrorCode::TableNotFound);

    // Try to drop a non-existent table
    auto drop_result = db_->DropTable("non_existent");
    VERIFY_ERROR_CODE(drop_result, ErrorCode::TableNotFound);

    // Try to drop system table
    auto system_drop = db_->DropTable(DB::SYSTEM_TABLE);
    VERIFY_ERROR_CODE(system_drop, ErrorCode::InvalidArgument);
}

TEST_F(DBTest, TableOperations) {
    // Create a table
    auto schema = CreateTestSchema();
    VERIFY_RESULT(db_->CreateTable("test_table", schema));

    // Get the table
    auto table_result = db_->GetTable("test_table");
    VERIFY_RESULT(table_result);
    auto table = table_result.value();

    // Put a record
    auto record = CreateTestRecord(schema, 1, "test1", "value1");
    VERIFY_RESULT(table->Put("key1", std::move(record)));

    // Get and verify the record
    auto get_result = table->Get("key1");
    VERIFY_RESULT(get_result);
    auto& retrieved = get_result.value();
    EXPECT_EQ(retrieved->Get<int32_t>(0).value(), 1);
    EXPECT_EQ(retrieved->Get<std::string>(1).value(), "test1");
    EXPECT_EQ(retrieved->Get<DataChunk>(2).value(), DataChunk(reinterpret_cast<const uint8_t*>("value1"), 6));

    // Delete the record
    VERIFY_RESULT(table->Delete("key1"));
    get_result = table->Get("key1");
    EXPECT_FALSE(get_result.ok());
}

TEST_F(DBTest, Recovery) {
    // Create tables and add data
    auto schema1 = CreateTestSchema("t1_");
    auto schema2 = CreateTestSchema("t2_");
    VERIFY_RESULT(db_->CreateTable("table1", schema1));
    VERIFY_RESULT(db_->CreateTable("table2", schema2));

    // Get tables and add data
    auto table1_result = db_->GetTable("table1");
    auto table2_result = db_->GetTable("table2");
    VERIFY_RESULT(table1_result);
    VERIFY_RESULT(table2_result);

    auto table1 = table1_result.value();
    auto table2 = table2_result.value();

    auto record1 = CreateTestRecord(schema1, 1, "test1", "value1");
    auto record2 = CreateTestRecord(schema2, 2, "test2", "value2");
    VERIFY_RESULT(table1->Put("key1", std::move(record1)));
    VERIFY_RESULT(table2->Put("key2", std::move(record2)));

    // Flush to ensure data is persisted
    VERIFY_RESULT(db_->Flush());

    // Create a new DB instance with the same fs
    auto new_db = std::make_unique<DB>(fs_, "test_db");
    VERIFY_RESULT(new_db->Recover());

    // Verify tables were recovered
    auto list_result = new_db->ListTables();
    VERIFY_RESULT(list_result);
    auto tables = list_result.value();
    EXPECT_EQ(tables.size(), 2);

    // Verify data was recovered
    auto recovered_table1 = new_db->GetTable("table1");
    auto recovered_table2 = new_db->GetTable("table2");
    VERIFY_RESULT(recovered_table1);
    VERIFY_RESULT(recovered_table2);

    auto get1 = recovered_table1.value()->Get("key1");
    auto get2 = recovered_table2.value()->Get("key2");
    VERIFY_RESULT(get1);
    VERIFY_RESULT(get2);

    EXPECT_EQ(get1.value()->Get<int32_t>(0).value(), 1);
    EXPECT_EQ(get1.value()->Get<std::string>(1).value(), "test1");
    EXPECT_EQ(get2.value()->Get<int32_t>(0).value(), 2);
    EXPECT_EQ(get2.value()->Get<std::string>(1).value(), "test2");
}

TEST_F(DBTest, InvalidOperations) {
    auto schema = CreateTestSchema();

    // Try to create table with empty name
    auto result = db_->CreateTable("", schema);
    VERIFY_ERROR_CODE(result, ErrorCode::InvalidArgument);

    // Try to create table with null schema
    result = db_->CreateTable("test", nullptr);
    VERIFY_ERROR_CODE(result, ErrorCode::InvalidArgument);

    // Try to create system table
    result = db_->CreateTable(DB::SYSTEM_TABLE, schema);
    VERIFY_ERROR_CODE(result, ErrorCode::InvalidArgument);

    // Try to get table with empty name
    auto get_result = db_->GetTable("");
    VERIFY_ERROR_CODE(get_result, ErrorCode::InvalidArgument);

    // Try to drop table with empty name
    auto drop_result = db_->DropTable("");
    VERIFY_ERROR_CODE(drop_result, ErrorCode::InvalidArgument);
}

}  // namespace pond::kv