#include "query/executor/sql_table_executor.h"

#include <gtest/gtest.h>

#include "catalog/kv_catalog.h"
#include "common/memory_append_only_fs.h"
#include "common/schema.h"
#include "kv/db.h"
#include "test_helper.h"

using namespace pond::common;
using namespace pond::query;
using namespace pond::catalog;
using namespace pond::kv;

namespace pond::test {

class SQLTableExecutorTest : public ::testing::Test {
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

    // Helper method to verify column schema
    void VerifyColumn(const Schema& schema, const std::string& name, ColumnType type, Nullability nullability) {
        int idx = schema.GetColumnIndex(name);
        ASSERT_GE(idx, 0) << "Column " << name << " not found in schema";

        const auto& columns = schema.Columns();
        const auto& column = columns[idx];

        EXPECT_EQ(column.name, name);
        EXPECT_EQ(column.type, type);
        EXPECT_EQ(column.nullability, nullability);
    }

    std::shared_ptr<MemoryAppendOnlyFileSystem> fs_;
    std::shared_ptr<DB> db_;
    std::shared_ptr<KVCatalog> catalog_;
    std::shared_ptr<SQLTableExecutor> executor_;
};

//
// Test Setup:
//      Execute a basic CREATE TABLE SQL statement
// Test Result:
//      Table should be created with the correct schema
//
TEST_F(SQLTableExecutorTest, BasicCreateTable) {
    // Define a simple CREATE TABLE statement
    const std::string sql = "CREATE TABLE students ("
                            "id INTEGER PRIMARY KEY,"
                            "name VARCHAR(100) NOT NULL,"
                            "age INTEGER,"
                            "email VARCHAR(255) UNIQUE,"
                            "created_at TIMESTAMP"
                            ")";

    // Execute the statement
    auto result = executor_->ExecuteCreateTable(sql);
    ASSERT_TRUE(result.ok()) << "Create table failed: " << result.error().message();

    // Verify the table was created in the catalog
    auto table_result = catalog_->LoadTable("students");
    ASSERT_TRUE(table_result.ok()) << "Table not found in catalog";

    // Verify table schema
    const auto& metadata = table_result.value();
    ASSERT_NE(metadata.schema, nullptr);

    const auto& schema = *metadata.schema;
    EXPECT_EQ(schema.NumColumns(), 5);

    // Verify individual columns
    VerifyColumn(schema, "id", ColumnType::INT32, Nullability::NOT_NULL);
    VerifyColumn(schema, "name", ColumnType::STRING, Nullability::NOT_NULL);
    VerifyColumn(schema, "age", ColumnType::INT32, Nullability::NULLABLE);
    VerifyColumn(schema, "email", ColumnType::STRING, Nullability::NULLABLE);
    VerifyColumn(schema, "created_at", ColumnType::TIMESTAMP, Nullability::NULLABLE);
}

//
// Test Setup:
//      Execute a CREATE TABLE with various column types
// Test Result:
//      Table should be created with columns of all supported types
//
TEST_F(SQLTableExecutorTest, VariousColumnTypes) {
    // Define a CREATE TABLE with various column types
    const std::string sql = "CREATE TABLE data_types ("
                            "int_col INTEGER,"
                            "bigint_col BIGINT,"
                            "float_col FLOAT,"
                            "double_col DOUBLE,"
                            "string_col VARCHAR(255),"
                            "text_col TEXT,"
                            "bool_col BOOLEAN,"
                            "date_col DATE,"
                            "timestamp_col TIMESTAMP"
                            ")";

    // Execute the statement
    auto result = executor_->ExecuteCreateTable(sql);
    ASSERT_TRUE(result.ok()) << "Create table failed: " << result.error().message();

    // Verify the table was created in the catalog
    auto table_result = catalog_->LoadTable("data_types");
    ASSERT_TRUE(table_result.ok()) << "Table not found in catalog";

    // Verify table schema
    const auto& metadata = table_result.value();
    ASSERT_NE(metadata.schema, nullptr);

    const auto& schema = *metadata.schema;
    EXPECT_EQ(schema.NumColumns(), 9);

    // Verify column types
    VerifyColumn(schema, "int_col", ColumnType::INT32, Nullability::NULLABLE);
    VerifyColumn(schema, "bigint_col", ColumnType::INT64, Nullability::NULLABLE);
    VerifyColumn(schema, "float_col", ColumnType::FLOAT, Nullability::NULLABLE);
    VerifyColumn(schema, "double_col", ColumnType::DOUBLE, Nullability::NULLABLE);
    VerifyColumn(schema, "string_col", ColumnType::STRING, Nullability::NULLABLE);
    VerifyColumn(schema, "text_col", ColumnType::STRING, Nullability::NULLABLE);
    VerifyColumn(schema, "bool_col", ColumnType::BOOLEAN, Nullability::NULLABLE);
    VerifyColumn(schema, "date_col", ColumnType::TIMESTAMP, Nullability::NULLABLE);
    VerifyColumn(schema, "timestamp_col", ColumnType::TIMESTAMP, Nullability::NULLABLE);
}

//
// Test Setup:
//      Execute a SQL statement that is not a CREATE TABLE
// Test Result:
//      Should return an error
//
TEST_F(SQLTableExecutorTest, NonCreateTableStatement) {
    // Define a SELECT statement
    const std::string sql = "SELECT * FROM students";

    // Execute the statement (should fail)
    auto result = executor_->ExecuteCreateTable(sql);
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.error().code(), ErrorCode::InvalidArgument);
    EXPECT_TRUE(result.error().message().find("SQL must be a CREATE TABLE statement") != std::string::npos);
}

//
// Test Setup:
//      Execute an invalid SQL statement
// Test Result:
//      Should return a parsing error
//
TEST_F(SQLTableExecutorTest, InvalidSQLStatement) {
    // Define an invalid SQL statement
    const std::string sql = "CREATE TABLE missing_paren (id INTEGER, name VARCHAR(100)";

    // Execute the statement (should fail with parsing error)
    auto result = executor_->ExecuteCreateTable(sql);
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.error().code(), ErrorCode::InvalidArgument);
    EXPECT_TRUE(result.error().message().find("SQL parsing error") != std::string::npos);
}

}  // namespace pond::test