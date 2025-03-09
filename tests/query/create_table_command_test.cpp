#include <gtest/gtest.h>

#include "catalog/kv_catalog.h"
#include "common/memory_append_only_fs.h"
#include "common/schema.h"
#include "kv/db.h"
#include "query/executor/sql_table_executor.h"
#include "test_helper.h"

using namespace pond::common;
using namespace pond::query;
using namespace pond::catalog;
using namespace pond::kv;

namespace pond::test {

/**
 * @brief Test class dedicated to testing CREATE TABLE commands
 *
 * This class focuses on testing the behavior of the CREATE TABLE command
 * with different SQL syntax variants and edge cases.
 */
class CreateTableCommandTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create an in-memory filesystem
        fs_ = std::make_shared<MemoryAppendOnlyFileSystem>();

        // Create a database using the filesystem
        auto db_result = DB::Create(fs_, "test_db");
        ASSERT_TRUE(db_result.ok());
        db_ = std::move(db_result.value());

        // Create a catalog using the database
        catalog_ = std::make_shared<KVCatalog>(db_);

        // Create the SQL table executor
        executor_ = std::make_shared<SQLTableExecutor>(catalog_);
    }

    // Helper to execute a CREATE TABLE command and return the created table metadata
    Result<TableMetadata> ExecuteCreateTable(const std::string& sql) { return executor_->ExecuteCreateTable(sql); }

    // Helper to verify schema contains the expected column
    void VerifyColumnExists(const Schema& schema, const std::string& name, ColumnType type, Nullability nullability) {
        int idx = schema.GetColumnIndex(name);
        ASSERT_GE(idx, 0) << "Column " << name << " not found in schema";

        const auto& columns = schema.Columns();
        const auto& column = columns[idx];

        EXPECT_EQ(column.name, name);
        EXPECT_EQ(column.type, type);
        EXPECT_EQ(column.nullability, nullability);
    }

    // Helper to verify a column does not exist in the schema
    void VerifyColumnDoesNotExist(const Schema& schema, const std::string& name) {
        int idx = schema.GetColumnIndex(name);
        ASSERT_LT(idx, 0) << "Column " << name << " should not exist in schema";
    }

    std::shared_ptr<MemoryAppendOnlyFileSystem> fs_;
    std::shared_ptr<DB> db_;
    std::shared_ptr<KVCatalog> catalog_;
    std::shared_ptr<SQLTableExecutor> executor_;
};

//
// Test Setup:
//      Create a simple table with a minimal schema
// Test Result:
//      Table should be created with the specified columns
//
TEST_F(CreateTableCommandTest, SimpleCreateTable) {
    const std::string sql = "CREATE TABLE minimal (id INTEGER, name VARCHAR(100))";

    auto result = ExecuteCreateTable(sql);
    ASSERT_TRUE(result.ok()) << "Create table failed: " << result.error().message();

    // Verify the table exists in the catalog
    auto table_result = catalog_->LoadTable("minimal");
    ASSERT_TRUE(table_result.ok()) << "Table not found in catalog";

    // Verify the schema
    auto& schema = *table_result.value().schema;
    EXPECT_EQ(schema.NumColumns(), 2);
    VerifyColumnExists(schema, "id", ColumnType::INT32, Nullability::NULLABLE);
    VerifyColumnExists(schema, "name", ColumnType::STRING, Nullability::NULLABLE);
}

//
// Test Setup:
//      Create a table with a mix of nullable and non-nullable columns
// Test Result:
//      Table should have the correct nullability for each column
//
TEST_F(CreateTableCommandTest, NullabilityConstraints) {
    const std::string sql = "CREATE TABLE nullable_test ("
                            "required_id INTEGER NOT NULL, "
                            "optional_name VARCHAR(100) NULL, "
                            "also_required TEXT NOT NULL)";

    auto result = ExecuteCreateTable(sql);
    ASSERT_TRUE(result.ok()) << "Create table failed: " << result.error().message();

    // Verify the table exists
    auto table_result = catalog_->LoadTable("nullable_test");
    ASSERT_TRUE(table_result.ok()) << "Table not found in catalog";

    // Verify nullability constraints
    auto& schema = *table_result.value().schema;
    EXPECT_EQ(schema.NumColumns(), 3);
    VerifyColumnExists(schema, "required_id", ColumnType::INT32, Nullability::NOT_NULL);
    VerifyColumnExists(schema, "optional_name", ColumnType::STRING, Nullability::NULLABLE);
    VerifyColumnExists(schema, "also_required", ColumnType::STRING, Nullability::NOT_NULL);
}

//
// Test Setup:
//      Create a table with PRIMARY KEY constraint
// Test Result:
//      Primary key column should be NOT NULL
//
TEST_F(CreateTableCommandTest, PrimaryKeyConstraint) {
    const std::string sql = "CREATE TABLE primary_key_test ("
                            "id INTEGER PRIMARY KEY, "
                            "name VARCHAR(100))";

    auto result = ExecuteCreateTable(sql);
    ASSERT_TRUE(result.ok()) << "Create table failed: " << result.error().message();

    // Verify primary key is not nullable
    auto table_result = catalog_->LoadTable("primary_key_test");
    ASSERT_TRUE(table_result.ok()) << "Table not found in catalog";

    auto& schema = *table_result.value().schema;
    VerifyColumnExists(schema, "id", ColumnType::INT32, Nullability::NOT_NULL);
}

//
// Test Setup:
//      Create a table with all supported data types
// Test Result:
//      Each column should have the correct mapped data type
//
TEST_F(CreateTableCommandTest, AllDataTypes) {
    const std::string sql = "CREATE TABLE data_types ("
                            "int_col INTEGER, "
                            "bigint_col BIGINT, "
                            "float_col FLOAT, "
                            "double_col DOUBLE, "
                            "bool_col BOOLEAN, "
                            "varchar_col VARCHAR(255), "
                            "char_col CHAR(10), "
                            "text_col TEXT, "
                            "timestamp_col TIMESTAMP, "
                            "date_col DATE)";

    auto result = ExecuteCreateTable(sql);
    ASSERT_TRUE(result.ok()) << "Create table failed: " << result.error().message();

    // Verify data type mappings
    auto table_result = catalog_->LoadTable("data_types");
    ASSERT_TRUE(table_result.ok()) << "Table not found in catalog";

    auto& schema = *table_result.value().schema;
    VerifyColumnExists(schema, "int_col", ColumnType::INT32, Nullability::NULLABLE);
    VerifyColumnExists(schema, "bigint_col", ColumnType::INT64, Nullability::NULLABLE);
    VerifyColumnExists(schema, "float_col", ColumnType::FLOAT, Nullability::NULLABLE);
    VerifyColumnExists(schema, "double_col", ColumnType::DOUBLE, Nullability::NULLABLE);
    VerifyColumnExists(schema, "bool_col", ColumnType::BOOLEAN, Nullability::NULLABLE);
    VerifyColumnExists(schema, "varchar_col", ColumnType::STRING, Nullability::NULLABLE);
    VerifyColumnExists(schema, "char_col", ColumnType::STRING, Nullability::NULLABLE);
    VerifyColumnExists(schema, "text_col", ColumnType::STRING, Nullability::NULLABLE);
    VerifyColumnExists(schema, "timestamp_col", ColumnType::TIMESTAMP, Nullability::NULLABLE);
    VerifyColumnExists(schema, "date_col", ColumnType::TIMESTAMP, Nullability::NULLABLE);
}

//
// Test Setup:
//      Create a table with UNIQUE constraints
// Test Result:
//      Column should have the correct constraints
//
TEST_F(CreateTableCommandTest, UniqueConstraints) {
    const std::string sql = "CREATE TABLE unique_test ("
                            "id INTEGER PRIMARY KEY, "
                            "username VARCHAR(50) UNIQUE)";

    auto result = ExecuteCreateTable(sql);
    ASSERT_TRUE(result.ok()) << "Create table failed: " << result.error().message();

    // Verify the table exists
    auto table_result = catalog_->LoadTable("unique_test");
    ASSERT_TRUE(table_result.ok()) << "Table not found in catalog";

    // We don't directly verify uniqueness since it's not tracked in the schema
    // but we can verify the column exists
    auto& schema = *table_result.value().schema;
    VerifyColumnExists(schema, "username", ColumnType::STRING, Nullability::NULLABLE);
}

//
// Test Setup:
//      Attempt to create a table with syntax error
// Test Result:
//      Should fail with parsing error
//
TEST_F(CreateTableCommandTest, SyntaxError) {
    const std::string sql = "CREATE TABLE syntax_error (id INTEGER, name VARCHAR(50)";  // Missing closing parenthesis

    auto result = ExecuteCreateTable(sql);
    ASSERT_FALSE(result.ok()) << "Create table should have failed";
    EXPECT_TRUE(result.error().message().find("SQL parsing error") != std::string::npos);
}

//
// Test Setup:
//      Attempt to create a table that already exists
// Test Result:
//      Should fail with table already exists error
//
TEST_F(CreateTableCommandTest, TableAlreadyExists) {
    const std::string sql = "CREATE TABLE duplicate (id INTEGER)";

    // Create the table first time
    auto result1 = ExecuteCreateTable(sql);
    ASSERT_TRUE(result1.ok()) << "First create failed: " << result1.error().message();

    // Try to create it again
    auto result2 = ExecuteCreateTable(sql);
    ASSERT_FALSE(result2.ok()) << "Second create should have failed";
    EXPECT_TRUE(result2.error().message().find("already exists") != std::string::npos);
}

//
// Test Setup:
//      Create a table with a name that uses characters that need escaping
// Test Result:
//      Table should be created with the correct name
//
TEST_F(CreateTableCommandTest, SpecialTableName) {
    const std::string sql = "CREATE TABLE \"special-table-name\" (id INTEGER)";

    auto result = ExecuteCreateTable(sql);
    ASSERT_TRUE(result.ok()) << "Create table failed: " << result.error().message();

    // Verify the table exists with the special name
    auto table_result = catalog_->LoadTable("special-table-name");
    ASSERT_TRUE(table_result.ok()) << "Table not found in catalog";
}

}  // namespace pond::test