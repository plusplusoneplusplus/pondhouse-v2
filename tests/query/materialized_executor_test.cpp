#include "query/executor/materialized_executor.h"

#include <arrow/api.h>
#include <arrow/builder.h>
#include <arrow/record_batch.h>
#include <arrow/testing/gtest_util.h>
#include <gtest/gtest.h>

#include "catalog/data_ingestor.h"
#include "catalog/kv_catalog.h"
#include "common/memory_append_only_fs.h"
#include "kv/db.h"
#include "query/data/arrow_record_batch_builder.h"
#include "query/data/arrow_util.h"
#include "query/data/catalog_data_accessor.h"
#include "query/query_test_context.h"
#include "test_helper.h"

namespace pond::query {

class MaterializedExecutorTest : public QueryTestContext {
protected:
    MaterializedExecutorTest() : QueryTestContext("test_catalog") {}

    void SetUp() override {
        QueryTestContext::SetUp();

        // Create data accessor using the context's catalog
        data_accessor_ = std::make_shared<CatalogDataAccessor>(catalog_, fs_);

        // Create the executor
        executor_ = std::make_unique<MaterializedExecutor>(catalog_, data_accessor_);

        // Setup the test table and ingest data
        SetupTestTable();
    }

    void SetupTestTable() {
        // Get the table schema from the context
        auto schema_result = catalog_->LoadTable("users");
        ASSERT_TRUE(schema_result.ok()) << "Failed to load table schema: " << schema_result.error().message();
    }

    void IngestDefaultTestData() {
        // Create test data using ArrowRecordBatchBuilder
        auto batch_result = ArrowRecordBatchBuilder()
                                .AddInt32Column("id", {1, 2, 3, 4, 5})
                                .AddStringColumn("name", {"Alice", "Bob", "Charlie", "David", "Eve"})
                                .AddInt32Column("age", {25, 30, 35, 40, 45})
                                .AddDoubleColumn("salary", {75000.0, 85000.0, 95000.0, 105000.0, 115000.0})
                                .Build();

        VERIFY_RESULT(batch_result);

        // Ingest the batch
        IngestData("users", batch_result.value());
    }

    // Sets up multiple test files with sequential data
    void SetupMultipleFiles(int num_files, int rows_per_file) {
        // For each file, create a data ingestor and ingest a batch with unique data
        for (int file_id = 0; file_id < num_files; file_id++) {
            // Create vectors to hold the data for this file
            std::vector<int32_t> ids;
            std::vector<std::string> names;
            std::vector<int32_t> ages;
            std::vector<double> values;

            // Each file has rows_per_file rows with sequential IDs and file-specific data
            for (int i = 0; i < rows_per_file; i++) {
                int id = file_id * rows_per_file + i;
                std::string name = "user_" + std::to_string(file_id) + "_" + std::to_string(i);
                int age = 20 + ((file_id * rows_per_file + i) % 30);  // Ages between 20-49
                double value = (file_id * rows_per_file + i) * 1.5;

                ids.push_back(id);
                names.push_back(name);
                ages.push_back(age);
                values.push_back(value);
            }

            // Create record batch using ArrowRecordBatchBuilder
            auto batch_result = ArrowRecordBatchBuilder()
                                    .AddInt32Column("id", ids)
                                    .AddStringColumn("name", names)
                                    .AddInt32Column("age", ages)
                                    .AddDoubleColumn("salary", values)
                                    .Build();

            VERIFY_RESULT(batch_result);

            // Ingest the batch
            IngestData("multi_users", batch_result.value());
        }
    }

    // Creates a scan plan for multi-file table
    std::shared_ptr<PhysicalPlanNode> CreateMultiFileScanPlan() {
        // Create a simple sequential scan plan for the multi_users table
        return std::make_shared<PhysicalSequentialScanNode>("multi_users", *GetSchema("multi_users"));
    }

    std::shared_ptr<PhysicalPlanNode> CreateScanPlan() {
        // Create a simple sequential scan plan for the users table using the schema from the context
        return std::make_shared<PhysicalSequentialScanNode>("users", *GetSchema("users"));
    }

    // Helper method for testing sort operations
    void TestSortOperation(const std::vector<std::string>& columns,
                           const std::vector<SortDirection>& directions,
                           std::function<void(const ArrowDataBatchSharedPtr&)> validator,
                           const std::string& table_name = "") {
        auto schema = GetSchema(table_name.empty() ? "users" : table_name);

        // Create sort expressions
        std::vector<std::shared_ptr<common::Expression>> sort_exprs;
        for (const auto& col : columns) {
            sort_exprs.push_back(common::MakeColumnExpression("", col));
        }

        // Create scan plan
        auto scan_node =
            std::make_shared<PhysicalSequentialScanNode>(table_name.empty() ? "users" : table_name, *schema);

        // Create sort specs
        std::vector<SortSpec> sort_specs;
        for (size_t i = 0; i < sort_exprs.size(); i++) {
            sort_specs.push_back({sort_exprs[i], directions[i]});
        }
        auto sort_node = std::make_shared<PhysicalSortNode>(sort_specs, scan_node->OutputSchema());
        sort_node->AddChild(scan_node);

        // Execute
        auto result = executor_->ExecuteToCompletion(sort_node);
        VERIFY_RESULT(result);

        // Validate
        validator(result.value());
    }

    std::shared_ptr<DataAccessor> data_accessor_;
    std::unique_ptr<MaterializedExecutor> executor_;
};

//
// Test Setup:
//      Create a simple sequential scan plan for users table with 5 rows of data
// Test Result:
//      Execute returns a DataBatch with 5 rows and correct column values
//
TEST_F(MaterializedExecutorTest, SelectAllFromTable) {
    IngestDefaultTestData();

    // Create a simple scan plan
    auto plan = CreateScanPlan();

    // Execute the plan
    auto result = executor_->ExecuteToCompletion(plan);
    ASSERT_TRUE(result.ok()) << "Execution failed: " << result.error().message();

    // Get the batch
    auto batch = result.value();
    ASSERT_NE(batch, nullptr) << "Result batch should not be null";

    // Verify batch contents
    ASSERT_EQ(batch->num_rows(), 5) << "Batch should have 5 rows";
    ASSERT_EQ(batch->num_columns(), 4) << "Batch should have 4 columns";

    // Verify column names
    ASSERT_EQ(batch->schema()->field(0)->name(), "id");
    ASSERT_EQ(batch->schema()->field(1)->name(), "name");
    ASSERT_EQ(batch->schema()->field(2)->name(), "age");

    // Verify first row
    auto id_array = std::static_pointer_cast<arrow::Int32Array>(batch->column(0));
    auto name_array = std::static_pointer_cast<arrow::StringArray>(batch->column(1));
    auto age_array = std::static_pointer_cast<arrow::Int32Array>(batch->column(2));

    ASSERT_EQ(id_array->Value(0), 1);
    ASSERT_EQ(name_array->GetString(0), "Alice");
    ASSERT_EQ(age_array->Value(0), 25);
}

//
// Test Setup:
//      Create an empty table with no data and execute a scan plan on it
// Test Result:
//      Execute returns an empty DataBatch with the correct schema
//
TEST_F(MaterializedExecutorTest, SelectFromEmptyTable) {
    IngestDefaultTestData();

    // Create a new empty table using the context
    auto empty_schema = common::CreateSchemaBuilder()
                            .AddField("col1", common::ColumnType::INT32)
                            .AddField("col2", common::ColumnType::STRING)
                            .Build();

    auto create_result =
        catalog_->CreateTable("empty_table", empty_schema, partition_spec_, "test_catalog/empty_table");
    VERIFY_RESULT(create_result);

    // Create a scan plan for the empty table
    auto plan = std::make_shared<PhysicalSequentialScanNode>("empty_table", *empty_schema);

    // Execute the plan
    auto result = executor_->ExecuteToCompletion(plan);
    ASSERT_TRUE(result.ok()) << "Execution failed: " << result.error().message();

    // Get the batch
    auto batch = result.value();
    ASSERT_NE(batch, nullptr) << "Result batch should not be null";

    // Verify batch is empty
    ASSERT_EQ(batch->num_rows(), 0) << "Batch should have 0 rows";
}

//
// Test Setup:
//      Execute a scan plan on a table that doesn't exist
// Test Result:
//      Execute returns an error result
//
TEST_F(MaterializedExecutorTest, InvalidTable) {
    // Create a scan plan for a non-existent table
    auto plan = std::make_shared<PhysicalSequentialScanNode>("non_existent_table", *GetSchema("users"));

    // Execute the plan
    auto result = executor_->ExecuteToCompletion(plan);
    ASSERT_FALSE(result.ok()) << "Execution should fail for non-existent table";
}

//
// Test Setup:
//      Execute a SQL query "SELECT * FROM users" through the SQL parser,
//      logical planner, physical planner, and our executor
// Test Result:
//      Query execution returns the correct data batch with all 5 rows
//
TEST_F(MaterializedExecutorTest, ExecuteSQLQuery) {
    IngestDefaultTestData();

    // Create SQL query
    std::string sql_query = "SELECT * FROM users";

    // Plan the query and get the physical plan
    auto physical_plan_result = PlanPhysical(sql_query);
    ASSERT_TRUE(physical_plan_result.ok()) << "Physical planning failed: " << physical_plan_result.error().message();
    auto physical_plan = physical_plan_result.value();

    // Execute the physical plan
    auto result = executor_->ExecuteToCompletion(physical_plan);
    ASSERT_TRUE(result.ok()) << "Execution failed: " << result.error().message();

    // Get the batch
    auto batch = result.value();
    ASSERT_NE(batch, nullptr) << "Result batch should not be null";

    // Verify batch contents
    ASSERT_EQ(batch->num_rows(), 5) << "Batch should have 5 rows";
    ASSERT_EQ(batch->num_columns(), 4) << "Batch should have 4 columns";

    // Verify column names
    ASSERT_EQ(batch->schema()->field(0)->name(), "id");
    ASSERT_EQ(batch->schema()->field(1)->name(), "name");
    ASSERT_EQ(batch->schema()->field(2)->name(), "age");

    // Verify first row
    auto id_array = std::static_pointer_cast<arrow::Int32Array>(batch->column(0));
    auto name_array = std::static_pointer_cast<arrow::StringArray>(batch->column(1));
    auto age_array = std::static_pointer_cast<arrow::Int32Array>(batch->column(2));

    ASSERT_EQ(id_array->Value(0), 1);
    ASSERT_EQ(name_array->GetString(0), "Alice");
    ASSERT_EQ(age_array->Value(0), 25);

    // Verify last row
    ASSERT_EQ(id_array->Value(4), 5);
    ASSERT_EQ(name_array->GetString(4), "Eve");
    ASSERT_EQ(age_array->Value(4), 45);
}

//
// Test Setup:
//      Execute a SQL query with a WHERE clause to filter results
// Test Result:
//      Execution should yield filtered results (only rows with age > 30)
//
TEST_F(MaterializedExecutorTest, ExecuteSQLQueryWithFilter) {
    IngestDefaultTestData();

    // Create SQL query with filter
    std::string sql_query = "SELECT * FROM users WHERE age > 30";

    // Plan the query and get the physical plan
    auto physical_plan_result = PlanPhysical(sql_query);
    ASSERT_TRUE(physical_plan_result.ok()) << "Physical planning failed: " << physical_plan_result.error().message();
    auto physical_plan = physical_plan_result.value();

    // Print the plan to understand its structure
    std::cout << "Physical Plan Structure:" << std::endl;
    std::cout << "Node Type: " << static_cast<int>(physical_plan->Type()) << std::endl;

    // Check if the filter has been pushed down or optimized away
    bool has_filter_node = false;
    bool has_scan_node = false;

    // Examine the plan structure
    std::function<void(const std::shared_ptr<PhysicalPlanNode>&)> inspect_plan =
        [&](const std::shared_ptr<PhysicalPlanNode>& node) {
            if (node->Type() == PhysicalNodeType::Filter) {
                has_filter_node = true;
                std::cout << "Found Filter Node" << std::endl;
            } else if (node->Type() == PhysicalNodeType::SequentialScan) {
                has_scan_node = true;
                auto scan_node = dynamic_cast<PhysicalSequentialScanNode*>(node.get());
                if (scan_node && scan_node->Predicate()) {
                    std::cout << "Sequential Scan with predicate (filter pushed down)" << std::endl;
                } else {
                    std::cout << "Sequential Scan without predicate" << std::endl;
                }
            }

            for (const auto& child : node->Children()) {
                inspect_plan(child);
            }
        };

    inspect_plan(physical_plan);

    // Execute the plan
    auto result = executor_->ExecuteToCompletion(physical_plan);
    ASSERT_TRUE(result.ok()) << "Execution failed: " << result.error().message();

    // Get the batch
    auto batch = result.value();
    ASSERT_NE(batch, nullptr) << "Result batch should not be null";

    // Print batch info
    std::cout << "Result batch has " << batch->num_rows() << " rows (should be filtered)" << std::endl;

    // Verify that filtering was applied - should only have rows with age > 30
    ASSERT_GT(batch->num_rows(), 0) << "Filtered result should have at least one row";
    ASSERT_LT(batch->num_rows(), 5) << "Filtered result should have fewer rows than the original 5";

    auto age_array = std::static_pointer_cast<arrow::Int32Array>(batch->column(2));
    std::cout << "Age values: ";
    for (int i = 0; i < batch->num_rows(); i++) {
        std::cout << age_array->Value(i) << " ";
        // Verify each row satisfies the filter condition
        ASSERT_GT(age_array->Value(i), 30) << "Row " << i << " doesn't satisfy filter condition";
    }
    std::cout << std::endl;

    // Check that we have expected values in the result
    auto name_array = std::static_pointer_cast<arrow::StringArray>(batch->column(1));
    bool has_charlie = false;
    bool has_david = false;
    bool has_eve = false;

    for (int i = 0; i < batch->num_rows(); i++) {
        auto name = name_array->GetString(i);
        if (name == "Charlie")
            has_charlie = true;
        if (name == "David")
            has_david = true;
        if (name == "Eve")
            has_eve = true;
    }

    ASSERT_TRUE(has_charlie) << "Charlie (age 35) should be in the filtered result";
    ASSERT_TRUE(has_david) << "David (age 40) should be in the filtered result";
    ASSERT_TRUE(has_eve) << "Eve (age 45) should be in the filtered result";
}

//
// Test Setup:
//      Create multiple files with sequential data and execute a query that spans all files
// Test Result:
//      Execute returns a DataBatch with data from all files with correct concatenation
//
TEST_F(MaterializedExecutorTest, MultiFileQuery) {
    IngestDefaultTestData();

    // Setup 3 files with 5 rows each
    SetupMultipleFiles(3, 5);

    // Create a simple scan plan
    auto plan = CreateMultiFileScanPlan();
    auto schema = GetSchema("multi_users");

    // Execute the plan
    auto result = executor_->ExecuteToCompletion(plan);
    ASSERT_TRUE(result.ok()) << "Execution failed: " << result.error().message();

    // Get the batch
    auto batch = result.value();
    ASSERT_NE(batch, nullptr) << "Result batch should not be null";

    // Verify batch contents - should have 15 rows (3 files * 5 rows)
    ASSERT_EQ(batch->num_rows(), 15) << "Batch should have 15 rows";
    ASSERT_EQ(batch->num_columns(), schema->FieldCount()) << "Batch should have " << schema->FieldCount() << " columns";

    // Verify the id column (assuming first column is 'id') has sequential values
    auto id_array = std::static_pointer_cast<arrow::Int32Array>(batch->column(0));
    for (int i = 0; i < 15; i++) {
        ASSERT_EQ(id_array->Value(i), i) << "ID at index " << i << " should be " << i;
    }
}

//
// Test Setup:
//      Create multiple files and execute a query with a filter predicate
// Test Result:
//      Execute returns a DataBatch with only the rows matching the filter from all files
//
TEST_F(MaterializedExecutorTest, MultiFileQueryWithFilter) {
    // Setup 3 files with 5 rows each
    SetupMultipleFiles(3, 5);

    // Add a filter: id > 7 (should return the last 7 rows)
    auto id_column = common::MakeColumnExpression("", "id");
    auto constant = common::MakeIntegerConstant(7);
    auto predicate = common::MakeComparisonExpression(common::BinaryOpType::Greater, id_column, constant);

    auto schema = GetSchema("multi_users");

    // Create a scan plan
    auto scan_node = std::make_shared<PhysicalSequentialScanNode>("multi_users", *schema, predicate);

    // Execute the plan
    auto result = executor_->ExecuteToCompletion(scan_node);
    VERIFY_RESULT(result);

    // Get the batch
    auto batch = result.value();
    ASSERT_NE(batch, nullptr) << "Result batch should not be null";

    // Verify batch contents - should have 7 rows (id > 7 from total of 15 rows)
    ASSERT_EQ(batch->num_rows(), 7) << "Batch should have 7 rows";
    ASSERT_EQ(batch->num_columns(), schema->FieldCount()) << "Batch should have " << schema->FieldCount() << " columns";

    // Verify data - IDs should be 8-14
    auto id_array = std::static_pointer_cast<arrow::Int32Array>(batch->column(0));
    for (int i = 0; i < 7; i++) {
        ASSERT_EQ(id_array->Value(i), i + 8) << "ID at index " << i << " should be " << (i + 8);
    }
}

//
// Test Setup:
//      Create a test table and execute a query with a projection
// Test Result:
//      Execute returns a DataBatch with only the selected columns
//
TEST_F(MaterializedExecutorTest, ProjectionTest) {
    IngestDefaultTestData();

    auto schema = GetSchema("users");

    // Create a scan plan
    auto scan_node = std::make_shared<PhysicalSequentialScanNode>("users", *schema);

    // Add a projection to select only id and name columns
    std::vector<std::shared_ptr<common::Expression>> projections = {common::MakeColumnExpression("users", "id"),
                                                                    common::MakeColumnExpression("users", "name")};

    // Create a schema for the projection
    auto proj_schema = std::make_shared<common::Schema>(
        std::vector<common::ColumnSchema>{{"id", common::ColumnType::INT32}, {"name", common::ColumnType::STRING}});

    auto projection_node = std::make_shared<PhysicalProjectionNode>(projections, *proj_schema);
    projection_node->AddChild(scan_node);

    // Execute the plan
    auto result = executor_->ExecuteToCompletion(projection_node);
    ASSERT_TRUE(result.ok()) << "Execution failed: " << result.error().message();

    // Get the batch
    auto batch = result.value();
    ASSERT_NE(batch, nullptr) << "Result batch should not be null";

    // Verify batch contents
    ASSERT_EQ(batch->num_rows(), 5) << "Batch should have 5 rows";
    ASSERT_EQ(batch->num_columns(), 2) << "Batch should have 2 columns";

    // Verify column names
    ASSERT_EQ(batch->schema()->field(0)->name(), "id");
    ASSERT_EQ(batch->schema()->field(1)->name(), "name");

    // Verify data
    auto id_array = std::static_pointer_cast<arrow::Int32Array>(batch->column(0));
    auto name_array = std::static_pointer_cast<arrow::StringArray>(batch->column(1));

    std::vector<std::pair<int, std::string>> expected_data = {
        {1, "Alice"}, {2, "Bob"}, {3, "Charlie"}, {4, "David"}, {5, "Eve"}};

    for (int i = 0; i < batch->num_rows(); i++) {
        ASSERT_EQ(id_array->Value(i), expected_data[i].first);
        ASSERT_EQ(name_array->GetString(i), expected_data[i].second);
    }
}

//
// Test Setup:
//      Create a test table and execute a query with a projection on a single column
// Test Result:
//      Execute returns a DataBatch with only the selected column
//
TEST_F(MaterializedExecutorTest, ProjectionSingleColumnTest) {
    IngestDefaultTestData();

    auto schema = GetSchema("users");

    // Create a scan plan
    auto scan_node = std::make_shared<PhysicalSequentialScanNode>("users", *schema);

    // Add a projection to select only the age column
    std::vector<std::shared_ptr<common::Expression>> projections = {common::MakeColumnExpression("", "age")};

    // Create a schema for the projection
    auto proj_schema =
        std::make_shared<common::Schema>(std::vector<common::ColumnSchema>{{"age", common::ColumnType::INT32}});

    auto projection_node = std::make_shared<PhysicalProjectionNode>(projections, *proj_schema);
    projection_node->AddChild(scan_node);

    // Execute the plan
    auto result = executor_->ExecuteToCompletion(projection_node);
    ASSERT_TRUE(result.ok()) << "Execution failed: " << result.error().message();

    // Get the batch
    auto batch = result.value();
    ASSERT_NE(batch, nullptr) << "Result batch should not be null";

    // Verify batch contents
    ASSERT_EQ(batch->num_rows(), 5) << "Batch should have 5 rows";
    ASSERT_EQ(batch->num_columns(), 1) << "Batch should have 1 column";

    // Verify column name
    ASSERT_EQ(batch->schema()->field(0)->name(), "age");

    // Verify data
    auto age_array = std::static_pointer_cast<arrow::Int32Array>(batch->column(0));

    std::vector<int> expected_data = {25, 30, 35, 40, 45};

    for (int i = 0; i < batch->num_rows(); i++) {
        ASSERT_EQ(age_array->Value(i), expected_data[i]);
    }
}

//
// Test Setup:
//      Create a test table and execute a query with projection after filter
// Test Result:
//      Execute returns a filtered DataBatch with only the selected columns
//
TEST_F(MaterializedExecutorTest, ProjectionAfterFilterTest) {
    IngestDefaultTestData();

    auto schema = GetSchema("users");

    // Create a scan plan
    auto scan_node = std::make_shared<PhysicalSequentialScanNode>("users", *schema);

    // Add a filter: age > 30
    auto age_column = common::MakeColumnExpression("users", "age");
    auto constant = common::MakeIntegerConstant(30);
    auto predicate = common::MakeComparisonExpression(common::BinaryOpType::Greater, age_column, constant);

    // Create a filter node
    auto filter_node = std::make_shared<PhysicalFilterNode>(predicate, scan_node->OutputSchema());
    filter_node->AddChild(scan_node);

    // Add a projection to select id and name columns
    std::vector<std::shared_ptr<common::Expression>> projections = {common::MakeColumnExpression("users", "id"),
                                                                    common::MakeColumnExpression("users", "name")};

    // Create a schema for the projection
    auto proj_schema = std::make_shared<common::Schema>(
        std::vector<common::ColumnSchema>{{"id", common::ColumnType::INT32}, {"name", common::ColumnType::STRING}});

    auto projection_node = std::make_shared<PhysicalProjectionNode>(projections, *proj_schema);
    projection_node->AddChild(filter_node);

    // Execute the plan
    auto result = executor_->ExecuteToCompletion(projection_node);
    ASSERT_TRUE(result.ok()) << "Execution failed: " << result.error().message();

    // Get the batch
    auto batch = result.value();
    ASSERT_NE(batch, nullptr) << "Result batch should not be null";

    // Verify batch contents - should only have Eve (age 45) since she's the only one with age > 30
    ASSERT_EQ(batch->num_rows(), 3) << "Batch should have 3 rows";
    ASSERT_EQ(batch->num_columns(), 2) << "Batch should have 2 columns";

    // Verify column names
    ASSERT_EQ(batch->schema()->field(0)->name(), "id");
    ASSERT_EQ(batch->schema()->field(1)->name(), "name");

    // Verify data
    auto id_array = std::static_pointer_cast<arrow::Int32Array>(batch->column(0));
    auto name_array = std::static_pointer_cast<arrow::StringArray>(batch->column(1));

    std::vector<std::pair<int, std::string>> expected_data = {{3, "Charlie"}, {4, "David"}, {5, "Eve"}};

    for (int i = 0; i < batch->num_rows(); i++) {
        ASSERT_EQ(id_array->Value(i), expected_data[i].first);
        ASSERT_EQ(name_array->GetString(i), expected_data[i].second);
    }
}

//
// Test Setup:
//      Create multiple files and execute a query with projection
// Test Result:
//      Execute returns a DataBatch with only the selected columns from all files
//
TEST_F(MaterializedExecutorTest, MultiFileQueryWithProjection) {
    IngestDefaultTestData();

    // Setup 3 files with 5 rows each
    SetupMultipleFiles(3, 5);
    auto schema = GetSchema("multi_users");

    // Create a scan plan
    auto scan_node = std::make_shared<PhysicalSequentialScanNode>("multi_users", *schema);

    // Add a projection to select only id and name columns
    std::vector<std::shared_ptr<common::Expression>> projections = {common::MakeColumnExpression("", "id"),
                                                                    common::MakeColumnExpression("", "name")};

    // Create a schema for the projection
    auto proj_schema = std::make_shared<common::Schema>(
        std::vector<common::ColumnSchema>{{"id", common::ColumnType::INT32}, {"name", common::ColumnType::STRING}});

    auto projection_node = std::make_shared<PhysicalProjectionNode>(projections, *proj_schema);
    projection_node->AddChild(scan_node);

    // Execute the plan
    auto result = executor_->ExecuteToCompletion(projection_node);
    ASSERT_TRUE(result.ok()) << "Execution failed: " << result.error().message();

    // Get the batch
    auto batch = result.value();
    ASSERT_NE(batch, nullptr) << "Result batch should not be null";

    // Verify batch contents
    ASSERT_EQ(batch->num_rows(), 15) << "Batch should have 15 rows";
    ASSERT_EQ(batch->num_columns(), 2) << "Batch should have 2 columns";

    // Verify column names
    ASSERT_EQ(batch->schema()->field(0)->name(), "id");
    ASSERT_EQ(batch->schema()->field(1)->name(), "name");

    // Verify data - check a few sample points
    auto id_array = std::static_pointer_cast<arrow::Int32Array>(batch->column(0));
    auto name_array = std::static_pointer_cast<arrow::StringArray>(batch->column(1));

    // Check first row
    ASSERT_EQ(id_array->Value(0), 0);
    ASSERT_EQ(name_array->GetString(0), "user_0_0");

    // Check middle row (file 1, row 2)
    ASSERT_EQ(id_array->Value(7), 7);
    ASSERT_EQ(name_array->GetString(7), "user_1_2");

    // Check last row
    ASSERT_EQ(id_array->Value(14), 14);
    ASSERT_EQ(name_array->GetString(14), "user_2_4");
}

//
// Test Setup:
//      Create a test table and execute a query with invalid column in projection
// Test Result:
//      Execute returns an error
//
TEST_F(MaterializedExecutorTest, ProjectionInvalidColumnTest) {
    IngestDefaultTestData();

    auto schema = GetSchema("users");

    // Create a scan plan
    auto scan_node = std::make_shared<PhysicalSequentialScanNode>("users", *schema);

    // Add a projection with a non-existent column
    std::vector<std::shared_ptr<common::Expression>> projections = {
        common::MakeColumnExpression("", "id"), common::MakeColumnExpression("", "non_existent_column")};

    // Create a schema for the projection (with a non-existent column)
    auto proj_schema = std::make_shared<common::Schema>(std::vector<common::ColumnSchema>{
        {"id", common::ColumnType::INT32}, {"non_existent_column", common::ColumnType::STRING}});

    auto projection_node = std::make_shared<PhysicalProjectionNode>(projections, *proj_schema);
    projection_node->AddChild(scan_node);

    // Execute the plan
    auto result = executor_->ExecuteToCompletion(projection_node);

    // Should fail because of the invalid column
    VERIFY_ERROR_CODE(result, common::ErrorCode::InvalidArgument);
    ASSERT_TRUE(result.error().message().find("Column not found") != std::string::npos);
}

//
// Test Setup:
//      Create a sequential scan plan with a projection list
//      Scan node has projections directly (not a separate projection node)
// Test Result:
//      Should return only the projected columns in the result
//
TEST_F(MaterializedExecutorTest, SequentialScanWithProjections) {
    IngestDefaultTestData();

    // Set up expected schema for the projected columns
    auto projected_schema = std::make_shared<common::Schema>(
        std::vector<common::ColumnSchema>{{"id", common::ColumnType::INT32}, {"name", common::ColumnType::STRING}});

    // Create a sequential scan node
    auto scan_node = std::make_shared<PhysicalSequentialScanNode>(
        "users", *GetSchema("users"), nullptr /* prediate */, *projected_schema);

    // Execute the plan
    auto result = executor_->ExecuteToCompletion(scan_node);
    VERIFY_RESULT(result);

    auto batch = result.value();

    // Verify the schema has only the projected columns
    ASSERT_EQ(2, batch->schema()->num_fields());
    EXPECT_EQ("id", batch->schema()->field(0)->name());
    EXPECT_EQ("name", batch->schema()->field(1)->name());

    // Verify we have data in the result
    ASSERT_GT(batch->num_rows(), 0);

    // Verify the batch contains the expected data types
    EXPECT_TRUE(batch->column(0)->type()->Equals(arrow::int32()));
    EXPECT_TRUE(batch->column(1)->type()->Equals(arrow::utf8()));

    // Print the first few rows for verification
    LOG_STATUS("First row values - id: %d, name: %s",
               std::static_pointer_cast<arrow::Int32Array>(batch->column(0))->Value(0),
               std::static_pointer_cast<arrow::StringArray>(batch->column(1))->GetString(0).c_str());
}

//
// Test Setup:
//      Execute a scan plan and use the returned iterator to fetch results
// Test Result:
//      Iterator correctly provides the data batch on first call and nullptr on subsequent calls
//
TEST_F(MaterializedExecutorTest, BatchIteratorTest) {
    IngestDefaultTestData();

    // Create a simple scan plan
    auto plan = CreateScanPlan();

    // Execute the plan to get an iterator
    auto iterator_result = executor_->Execute(plan);
    ASSERT_TRUE(iterator_result.ok()) << "Execution failed: " << iterator_result.error().message();

    auto iterator = std::move(iterator_result).value();
    ASSERT_NE(iterator, nullptr) << "Iterator should not be null";

    // Check that HasNext returns true before we call Next
    ASSERT_TRUE(iterator->HasNext()) << "Iterator should have data available";

    // Call Next to get the batch
    auto batch_result = iterator->Next();
    ASSERT_TRUE(batch_result.ok()) << "Next call failed: " << batch_result.error().message();

    auto batch = batch_result.value();
    ASSERT_NE(batch, nullptr) << "Result batch should not be null";

    // Verify batch contents
    ASSERT_EQ(batch->num_rows(), 5) << "Batch should have 5 rows";
    ASSERT_EQ(batch->num_columns(), 4) << "Batch should have 4 columns";

    // Check that HasNext now returns false
    ASSERT_FALSE(iterator->HasNext()) << "Iterator should not have more data";

    // Call Next again - should return nullptr
    auto empty_result = iterator->Next();
    ASSERT_TRUE(empty_result.ok()) << "Second Next call failed: " << empty_result.error().message();
    ASSERT_EQ(empty_result.value(), nullptr) << "Second call to Next should return nullptr";
}

//
// Test Setup:
//      Execute a query with COUNT(*) aggregation
// Test Result:
//      Returns a single row with the count of all rows
//
TEST_F(MaterializedExecutorTest, SimpleCount) {
    IngestDefaultTestData();

    // Create SQL query with COUNT
    std::string sql_query = "SELECT COUNT(*) AS row_count FROM users";

    // Plan the query and get the physical plan
    auto physical_plan_result = PlanPhysical(sql_query);
    ASSERT_TRUE(physical_plan_result.ok()) << "Physical planning failed: " << physical_plan_result.error().message();
    auto physical_plan = physical_plan_result.value();

    // Execute the plan
    auto result = executor_->ExecuteToCompletion(physical_plan);
    VERIFY_RESULT(result);

    // Get the batch
    auto batch = result.value();
    ASSERT_NE(batch, nullptr) << "Result batch should not be null";

    // Verify batch contents
    ASSERT_EQ(batch->num_rows(), 1) << "Batch should have 1 row";
    ASSERT_EQ(batch->num_columns(), 1) << "Batch should have 1 column";

    // Verify column names
    ASSERT_EQ(batch->schema()->field(0)->name(), "row_count");

    // Verify count value
    auto count_array = std::static_pointer_cast<arrow::Int64Array>(batch->column(0));
    ASSERT_EQ(count_array->Value(0), 5) << "Count should be 5";
}

//
// Test Setup:
//      Execute a query with SUM aggregation
// Test Result:
//      Returns a single row with the sum of the age column
//
TEST_F(MaterializedExecutorTest, SimpleSum) {
    IngestDefaultTestData();

    // Create SQL query with SUM
    std::string sql_query = "SELECT SUM(age) FROM users";

    // Plan the query and get the physical plan
    auto physical_plan_result = PlanPhysical(sql_query);
    ASSERT_TRUE(physical_plan_result.ok()) << "Physical planning failed: " << physical_plan_result.error().message();
    auto physical_plan = physical_plan_result.value();

    // Execute the plan
    auto result = executor_->ExecuteToCompletion(physical_plan);
    VERIFY_RESULT(result);

    // Get the batch
    auto batch = result.value();
    ASSERT_NE(batch, nullptr) << "Result batch should not be null";

    // Verify batch contents
    ASSERT_EQ(batch->num_rows(), 1) << "Batch should have 1 row";
    ASSERT_EQ(batch->num_columns(), 1) << "Batch should have 1 column";

    // Verify column names
    ASSERT_EQ(batch->schema()->field(0)->name(), "sum_age");

    // Verify sum value (25 + 30 + 35 + 40 + 45 = 175)
    auto sum_array = std::static_pointer_cast<arrow::Int32Array>(batch->column(0));
    ASSERT_EQ(sum_array->Value(0), 175) << "Sum should be 175";
}

//
// Test Setup:
//      Execute a query with AVG aggregation
// Test Result:
//      Returns a single row with the average of the age column
//
TEST_F(MaterializedExecutorTest, SimpleAvg) {
    IngestDefaultTestData();

    // Create SQL query with AVG
    std::string sql_query = "SELECT AVG(age) FROM users";

    // Plan the query and get the physical plan
    auto physical_plan_result = PlanPhysical(sql_query);
    ASSERT_TRUE(physical_plan_result.ok()) << "Physical planning failed: " << physical_plan_result.error().message();
    auto physical_plan = physical_plan_result.value();

    // Execute the plan
    auto result = executor_->ExecuteToCompletion(physical_plan);
    VERIFY_RESULT(result);

    // Get the batch
    auto batch = result.value();
    ASSERT_NE(batch, nullptr) << "Result batch should not be null";

    // Verify batch contents
    ASSERT_EQ(batch->num_rows(), 1) << "Batch should have 1 row";
    ASSERT_EQ(batch->num_columns(), 1) << "Batch should have 1 column";

    // Verify column names
    ASSERT_EQ(batch->schema()->field(0)->name(), "avg_age");

    // Verify average value (25 + 30 + 35 + 40 + 45) / 5 = 35
    auto avg_array = std::static_pointer_cast<arrow::DoubleArray>(batch->column(0));
    ASSERT_DOUBLE_EQ(avg_array->Value(0), 35.0) << "Average should be 35.0";
}

//
// Test Setup:
//      Execute a query with MIN and MAX aggregation
// Test Result:
//      Returns a single row with the min and max of the age column
//
TEST_F(MaterializedExecutorTest, SimpleMinMax) {
    IngestDefaultTestData();

    // Create SQL query with MIN and MAX
    std::string sql_query = "SELECT MIN(age) AS age_min, MAX(age) AS age_max FROM users";

    // Plan the query and get the physical plan
    auto physical_plan_result = PlanPhysical(sql_query);
    ASSERT_TRUE(physical_plan_result.ok()) << "Physical planning failed: " << physical_plan_result.error().message();
    auto physical_plan = physical_plan_result.value();

    // Execute the plan
    auto result = executor_->ExecuteToCompletion(physical_plan);
    VERIFY_RESULT(result);

    // Get the batch
    auto batch = result.value();
    ASSERT_NE(batch, nullptr) << "Result batch should not be null";

    // Verify batch contents
    ASSERT_EQ(batch->num_rows(), 1) << "Batch should have 1 row";
    ASSERT_EQ(batch->num_columns(), 2) << "Batch should have 2 columns";

    // Verify column names
    ASSERT_EQ(batch->schema()->field(0)->name(), "age_min");
    ASSERT_EQ(batch->schema()->field(1)->name(), "age_max");

    // Verify min/max values
    auto min_array = std::static_pointer_cast<arrow::Int32Array>(batch->column(0));
    auto max_array = std::static_pointer_cast<arrow::Int32Array>(batch->column(1));
    ASSERT_EQ(min_array->Value(0), 25) << "Min age should be 25";
    ASSERT_EQ(max_array->Value(0), 45) << "Max age should be 45";
}

//
// Test Setup:
//      Execute a query with GROUP BY and aggregation
// Test Result:
//      Returns multiple rows grouped by age range with aggregates
//      Returns multiple rows grouped by gender with aggregates
//
TEST_F(MaterializedExecutorTest, GroupByWithAggregates) {
    IngestDefaultTestData();

    // Create SQL query with GROUP BY and multiple aggregates
    std::string sql_query = "SELECT name, COUNT(*) AS count, AVG(age) AS avg_age, MIN(age) AS min_age, MAX(age) AS "
                            "max_age FROM users GROUP BY name";

    auto logical_plan_result = PlanLogical(sql_query);
    VERIFY_RESULT(logical_plan_result);
    auto logical_plan = logical_plan_result.value();

    // Plan the query and get the physical plan
    auto physical_plan_result = PlanPhysical(sql_query);
    ASSERT_TRUE(physical_plan_result.ok()) << "Physical planning failed: " << physical_plan_result.error().message();
    auto physical_plan = physical_plan_result.value();

    // Execute the plan
    auto result = executor_->ExecuteToCompletion(physical_plan);
    VERIFY_RESULT(result);

    // Get the batch
    auto batch = result.value();
    ASSERT_NE(batch, nullptr) << "Result batch should not be null";

    // Verify batch contents - should have 5 rows (one for each user)
    ASSERT_EQ(batch->num_rows(), 5) << "Batch should have 5 rows (one for each user)";
    for (int i = 0; i < batch->num_columns(); i++) {
        std::cout << "Column " << i << ": " << batch->schema()->field(i)->name() << std::endl;
    }
    ASSERT_EQ(batch->num_columns(), 5) << "Batch should have 5 columns";

    // Verify column names
    ASSERT_EQ(batch->schema()->field(0)->name(), "name");
    ASSERT_EQ(batch->schema()->field(1)->name(), "count");
    ASSERT_EQ(batch->schema()->field(2)->name(), "avg_age");
    ASSERT_EQ(batch->schema()->field(3)->name(), "min_age");
    ASSERT_EQ(batch->schema()->field(4)->name(), "max_age");

    // Get the name array to identify which row is which
    auto name_array = std::static_pointer_cast<arrow::StringArray>(batch->column(0));
    auto count_array = std::static_pointer_cast<arrow::Int64Array>(batch->column(1));
    auto avg_array = std::static_pointer_cast<arrow::DoubleArray>(batch->column(2));
    auto min_array = std::static_pointer_cast<arrow::Int32Array>(batch->column(3));
    auto max_array = std::static_pointer_cast<arrow::Int32Array>(batch->column(4));

    // Verify each user's metrics
    for (int i = 0; i < batch->num_rows(); i++) {
        std::string name = name_array->GetString(i);
        ASSERT_EQ(count_array->Value(i), 1) << "Count should be 1 for each user";

        if (name == "Alice") {
            ASSERT_DOUBLE_EQ(avg_array->Value(i), 25.0) << "Alice avg age should be 25.0";
            ASSERT_EQ(min_array->Value(i), 25) << "Alice min age should be 25";
            ASSERT_EQ(max_array->Value(i), 25) << "Alice max age should be 25";
        } else if (name == "Bob") {
            ASSERT_DOUBLE_EQ(avg_array->Value(i), 30.0) << "Bob avg age should be 30.0";
            ASSERT_EQ(min_array->Value(i), 30) << "Bob min age should be 30";
            ASSERT_EQ(max_array->Value(i), 30) << "Bob max age should be 30";
        } else if (name == "Charlie") {
            ASSERT_DOUBLE_EQ(avg_array->Value(i), 35.0) << "Charlie avg age should be 35.0";
            ASSERT_EQ(min_array->Value(i), 35) << "Charlie min age should be 35";
            ASSERT_EQ(max_array->Value(i), 35) << "Charlie max age should be 35";
        } else if (name == "Dave") {
            ASSERT_DOUBLE_EQ(avg_array->Value(i), 40.0) << "Dave avg age should be 40.0";
            ASSERT_EQ(min_array->Value(i), 40) << "Dave min age should be 40";
            ASSERT_EQ(max_array->Value(i), 40) << "Dave max age should be 40";
        } else if (name == "Eve") {
            ASSERT_DOUBLE_EQ(avg_array->Value(i), 45.0) << "Eve avg age should be 45.0";
            ASSERT_EQ(min_array->Value(i), 45) << "Eve min age should be 45";
            ASSERT_EQ(max_array->Value(i), 45) << "Eve max age should be 45";
        }
    }
}

//
// Test Setup:
//      Create test data with mixed values and nulls, execute sort with single column
// Test Result:
//      Verify data is sorted correctly in ascending/descending order with proper null handling
//
TEST_F(MaterializedExecutorTest, SortSingleColumn) {
    IngestDefaultTestData();

    // Test ascending sort with nulls first
    TestSortOperation({"age"}, {SortDirection::Ascending}, [](const ArrowDataBatchSharedPtr& batch) {
        std::cout << "Sorting ascending" << std::endl;
        std::cout << "Batch Result: " << ArrowUtil::BatchToString(batch) << std::endl;
        auto ages = std::static_pointer_cast<arrow::Int32Array>(batch->column(2));
        ASSERT_EQ(ages->Value(0), 25);
        ASSERT_EQ(ages->Value(1), 30);
        ASSERT_EQ(ages->Value(2), 35);
        ASSERT_EQ(ages->Value(3), 40);
        ASSERT_EQ(ages->Value(4), 45);
    });

    // Test descending sort with nulls last
    TestSortOperation({"age"}, {SortDirection::Descending}, [](const ArrowDataBatchSharedPtr& batch) {
        std::cout << "Sorting descending" << std::endl;
        std::cout << "Batch Result: " << ArrowUtil::BatchToString(batch) << std::endl;
        auto ages = std::static_pointer_cast<arrow::Int32Array>(batch->column(2));
        ASSERT_EQ(ages->Value(0), 45);
        ASSERT_EQ(ages->Value(1), 40);
        ASSERT_EQ(ages->Value(2), 35);
        ASSERT_EQ(ages->Value(3), 30);
        ASSERT_EQ(ages->Value(4), 25);
    });
}

//
// Test Setup:
//      Create test data with duplicate keys, execute multi-column sort
// Test Result:
//      Verify data is sorted correctly with secondary sort keys
//
TEST_F(MaterializedExecutorTest, SortMultipleColumns) {
    // Create test data with duplicate ages
    auto batch_result = ArrowRecordBatchBuilder()
                            .AddInt32Column("id", {1, 2, 3, 4, 5})
                            .AddStringColumn("name", {"Bob", "Alice", "Charlie", "David", "Eve"})
                            .AddInt32Column("age", {30, 30, 25, 25, 35})
                            .AddDoubleColumn("salary", {100000, 90000, 80000, 70000, 60000})
                            .Build();

    VERIFY_RESULT(batch_result);

    // Ingest the batch
    IngestData("users", batch_result.value());

    // Sort by age ascending, then name descending
    TestSortOperation({"age", "name"},
                      {SortDirection::Ascending, SortDirection::Descending},
                      [](const ArrowDataBatchSharedPtr& batch) {
                          auto names = std::static_pointer_cast<arrow::StringArray>(batch->column(1));
                          auto ages = std::static_pointer_cast<arrow::Int32Array>(batch->column(2));

                          std::cout << "Batch Result: " << ArrowUtil::BatchToString(batch) << std::endl;

                          // First sort by age (25, 25, 30, 30, 35)
                          ASSERT_EQ(ages->Value(0), 25);
                          ASSERT_EQ(ages->Value(1), 25);
                          ASSERT_EQ(ages->Value(2), 30);
                          ASSERT_EQ(ages->Value(3), 30);
                          ASSERT_EQ(ages->Value(4), 35);

                          // Then sort names descending within same ages
                          ASSERT_EQ(names->GetString(0), "David");  // 25s sorted descending
                          ASSERT_EQ(names->GetString(1), "Charlie");
                          ASSERT_EQ(names->GetString(2), "Bob");  // 30s sorted descending
                          ASSERT_EQ(names->GetString(3), "Alice");
                          ASSERT_EQ(names->GetString(4), "Eve");  // 35
                      });
}

//
// Test Setup:
//      Create empty batch and execute sort
// Test Result:
//      Should return empty batch without errors
//
TEST_F(MaterializedExecutorTest, SortEmptyBatch) {
    // Create empty table
    auto empty_schema = common::CreateSchemaBuilder().AddField("id", common::ColumnType::INT32).Build();
    catalog_->CreateTable("empty_table", empty_schema, partition_spec_, "test_catalog/empty_table");

    auto scan_node = std::make_shared<PhysicalSequentialScanNode>("empty_table", *empty_schema);

    // Create sort specs
    std::vector<SortSpec> sort_specs{{common::MakeColumnExpression("", "id"), SortDirection::Ascending}};

    auto sort_node = std::make_shared<PhysicalSortNode>(sort_specs, scan_node->OutputSchema());
    sort_node->AddChild(scan_node);

    auto result = executor_->ExecuteToCompletion(sort_node);
    VERIFY_RESULT(result);

    auto batch = result.value();
    ASSERT_EQ(batch->num_rows(), 0);
}

//
// Test Setup:
//      Sort by unsupported column type (bool)
// Test Result:
//      Should handle boolean type correctly
//
TEST_F(MaterializedExecutorTest, SortBooleanType) {
    CreateNewTable("bool_test",
                   common::CreateSchemaBuilder()
                       .AddField("active", common::ColumnType::BOOLEAN)
                       .AddField("id", common::ColumnType::INT32)
                       .Build());

    // Create test data with boolean values
    auto batch_result = ArrowRecordBatchBuilder()
                            .AddBooleanColumn("active", {true, false, true, false, true})
                            .AddInt32Column("id", {3, 1, 4, 2, 5})
                            .Build();

    VERIFY_RESULT(batch_result);

    // Ingest the batch
    IngestData("bool_test", batch_result.value());

    // Test sort by active (descending) and id (ascending)
    TestSortOperation(
        {"active", "id"},
        {SortDirection::Descending, SortDirection::Ascending},
        [](const ArrowDataBatchSharedPtr& batch) {
            auto active_result = std::static_pointer_cast<arrow::BooleanArray>(batch->column(0));
            auto ids_result = std::static_pointer_cast<arrow::Int32Array>(batch->column(1));

            // Verify sort order
            ASSERT_TRUE(active_result->Value(0));
            ASSERT_TRUE(active_result->Value(1));
            ASSERT_TRUE(active_result->Value(2));
            ASSERT_FALSE(active_result->Value(3));
            ASSERT_FALSE(active_result->Value(4));

            // Verify IDs within active groups
            ASSERT_EQ(ids_result->Value(0), 3);
            ASSERT_EQ(ids_result->Value(1), 4);
            ASSERT_EQ(ids_result->Value(2), 5);
            ASSERT_EQ(ids_result->Value(3), 1);
            ASSERT_EQ(ids_result->Value(4), 2);
        },
        "bool_test");
}

//
// Test Setup:
//      Apply limit with offset to batch containing null values
// Test Result:
//      Verify output batch preserves null values in the selected range
//
TEST_F(MaterializedExecutorTest, LimitWithNullValues) {
    // Create test data with null values
    auto batch_result =
        ArrowRecordBatchBuilder()
            .AddInt32Column("id", {1, 2, 3, 4, 5})
            .AddStringColumn("name", {"Alice", "", "Charlie", "", "Eve"}, {true, false, true, false, true})
            .AddInt32Column("age", {25, 30, 35, 40, 45}, {true, true, false, true, true})
            .AddDoubleColumn("salary", {100000, 90000, 80000, 70000, 60000})
            .Build();
    VERIFY_RESULT(batch_result);
    IngestData("nullable_users", batch_result.value());

    // Create scan plan
    auto scan_node = std::make_shared<PhysicalSequentialScanNode>("nullable_users", *GetSchema("nullable_users"));

    // Create limit node with offset
    auto limit_node = std::make_shared<PhysicalLimitNode>(2, 1, scan_node->OutputSchema());
    limit_node->AddChild(scan_node);

    // Execute the plan
    auto result = executor_->ExecuteToCompletion(limit_node);
    VERIFY_RESULT(result);

    auto output_batch = result.value();
    std::cout << "Output batch: " << ArrowUtil::BatchToString(output_batch) << std::endl;
    ASSERT_NE(output_batch, nullptr);
    ASSERT_EQ(output_batch->num_rows(), 2);

    // Verify data and null values
    auto name_array = std::static_pointer_cast<arrow::StringArray>(output_batch->column(1));
    auto age_array = std::static_pointer_cast<arrow::Int32Array>(output_batch->column(2));

    EXPECT_TRUE(name_array->IsNull(0));  // Second row from original (null)
    EXPECT_EQ(name_array->GetString(1), "Charlie");
    EXPECT_TRUE(age_array->IsValid(0));
    EXPECT_EQ(age_array->Value(0), 30);
    EXPECT_TRUE(age_array->IsNull(1));
}

//
// Test Setup:
//      Apply limit with large offset that exceeds batch size
// Test Result:
//      Verify empty batch is returned with correct schema
//
TEST_F(MaterializedExecutorTest, LimitWithLargeOffset) {
    IngestDefaultTestData();

    // Create scan plan
    auto scan_node = std::make_shared<PhysicalSequentialScanNode>("users", *GetSchema("users"));

    // Create limit node with offset beyond data size
    auto limit_node = std::make_shared<PhysicalLimitNode>(5, 10, scan_node->OutputSchema());
    limit_node->AddChild(scan_node);

    // Execute the plan
    auto result = executor_->ExecuteToCompletion(limit_node);
    VERIFY_RESULT(result);

    auto output_batch = result.value();
    ASSERT_NE(output_batch, nullptr);
    ASSERT_EQ(output_batch->num_rows(), 0);
    ASSERT_EQ(output_batch->num_columns(), 4);  // Schema should be preserved
}

//
// Test Setup:
//      Apply limit after filter operation
// Test Result:
//      Verify limit is correctly applied to filtered results
//
TEST_F(MaterializedExecutorTest, LimitAfterFilter) {
    IngestDefaultTestData();

    // Create scan plan
    auto scan_node = std::make_shared<PhysicalSequentialScanNode>("users", *GetSchema("users"));

    // Add filter: age > 30
    auto age_column = common::MakeColumnExpression("", "age");
    auto constant = common::MakeIntegerConstant(30);
    auto predicate = common::MakeComparisonExpression(common::BinaryOpType::Greater, age_column, constant);
    auto filter_node = std::make_shared<PhysicalFilterNode>(predicate, scan_node->OutputSchema());
    filter_node->AddChild(scan_node);

    // Add limit: first 2 rows
    auto limit_node = std::make_shared<PhysicalLimitNode>(2, 0, filter_node->OutputSchema());
    limit_node->AddChild(filter_node);

    // Execute the plan
    auto result = executor_->ExecuteToCompletion(limit_node);
    VERIFY_RESULT(result);

    auto output_batch = result.value();
    ASSERT_NE(output_batch, nullptr);
    ASSERT_EQ(output_batch->num_rows(), 2);

    // Verify we got the first two rows where age > 30
    auto age_array = std::static_pointer_cast<arrow::Int32Array>(output_batch->column(2));
    EXPECT_EQ(age_array->Value(0), 35);  // Charlie
    EXPECT_EQ(age_array->Value(1), 40);  // David
}

//
// Test Setup:
//      Apply limit with zero rows requested
// Test Result:
//      Verify empty batch is returned with correct schema
//
TEST_F(MaterializedExecutorTest, LimitZeroRows) {
    IngestDefaultTestData();

    // Create scan plan
    auto scan_node = std::make_shared<PhysicalSequentialScanNode>("users", *GetSchema("users"));

    // Create limit node with zero rows
    auto limit_node = std::make_shared<PhysicalLimitNode>(0, 0, scan_node->OutputSchema());
    limit_node->AddChild(scan_node);

    // Execute the plan
    auto result = executor_->ExecuteToCompletion(limit_node);
    VERIFY_RESULT(result);

    auto output_batch = result.value();
    ASSERT_NE(output_batch, nullptr);
    ASSERT_EQ(output_batch->num_rows(), 0);
    ASSERT_EQ(output_batch->num_columns(), 4);  // Schema should be preserved
}

//
// Test Setup:
//      - Create 'users' and 'orders' tables
//      - users: id, name
//      - orders: order_id, user_id, amount
//      - Ingest sample data with matching user IDs
// Test Result:
//      - Joined result should have correct columns from both tables
//      - Number of rows should match expected joins
//
TEST_F(MaterializedExecutorTest, HashJoinBasic) {
    // Setup users table
    auto user_schema = std::make_shared<common::Schema>();
    user_schema->AddField("id", common::ColumnType::INT32);
    user_schema->AddField("name", common::ColumnType::STRING);
    CreateNewTable("join_users", user_schema);

    // Setup orders table
    auto order_schema = std::make_shared<common::Schema>();
    order_schema->AddField("order_id", common::ColumnType::INT32);
    order_schema->AddField("user_id", common::ColumnType::INT32);
    order_schema->AddField("amount", common::ColumnType::DOUBLE);
    CreateNewTable("join_orders", order_schema);

    // Ingest sample data
    auto user_batch = ArrowRecordBatchBuilder()
                          .AddInt32Column("id", {1, 2, 3})
                          .AddStringColumn("name", {"Alice", "Bob", "Charlie"})
                          .Build()
                          .value();
    IngestData("join_users", user_batch);

    auto order_batch = ArrowRecordBatchBuilder()
                           .AddInt32Column("order_id", {100, 101, 102})
                           .AddInt32Column("user_id", {1, 2, 1})
                           .AddDoubleColumn("amount", {50.0, 100.0, 75.0})
                           .Build()
                           .value();
    IngestData("join_orders", order_batch);

    // Create join plan
    auto left_plan = std::make_shared<PhysicalSequentialScanNode>(
        "join_users", *user_schema, nullptr, std::nullopt, std::nullopt, std::nullopt);
    auto right_plan = std::make_shared<PhysicalSequentialScanNode>(
        "join_orders", *order_schema, nullptr, std::nullopt, std::nullopt, std::nullopt);

    auto join_condition =
        std::make_shared<common::BinaryExpression>(common::BinaryOpType::Equal,
                                                   std::make_shared<common::ColumnExpression>("", "id"),
                                                   std::make_shared<common::ColumnExpression>("", "user_id"));

    common::Schema output_schema;
    output_schema.AddField("id", common::ColumnType::INT32);
    output_schema.AddField("name", common::ColumnType::STRING);
    output_schema.AddField("order_id", common::ColumnType::INT32);
    output_schema.AddField("user_id", common::ColumnType::INT32);
    output_schema.AddField("amount", common::ColumnType::DOUBLE);

    auto join_node = std::make_shared<PhysicalHashJoinNode>(join_condition, output_schema, common::JoinType::Inner);
    join_node->AddChild(left_plan);
    join_node->AddChild(right_plan);

    // Execute
    auto result = executor_->ExecuteToCompletion(join_node);
    std::cout << "Result: " << ArrowUtil::BatchToString(result.value()) << std::endl;
    VERIFY_RESULT(result);

    auto batch = result.value();
    EXPECT_EQ(batch->num_rows(), 3);
    EXPECT_EQ(batch->num_columns(), 5);

    auto sorted_batch = ArrowUtil::SortBatch(batch, {"order_id"}).value();

    // Verify first row in sorted batch
    auto id_array = std::static_pointer_cast<arrow::Int32Array>(sorted_batch->column(0));
    auto name_array = std::static_pointer_cast<arrow::StringArray>(sorted_batch->column(1));
    auto order_array = std::static_pointer_cast<arrow::Int32Array>(sorted_batch->column(2));
    auto user_id_array = std::static_pointer_cast<arrow::Int32Array>(sorted_batch->column(3));
    auto amount_array = std::static_pointer_cast<arrow::DoubleArray>(sorted_batch->column(4));

    EXPECT_EQ(id_array->Value(0), 1);
    EXPECT_EQ(name_array->GetString(0), "Alice");
    EXPECT_EQ(order_array->Value(0), 100);
    EXPECT_EQ(user_id_array->Value(0), 1);
    EXPECT_EQ(amount_array->Value(0), 50.0);
}

//
// Test Setup:
//      - Create two tables with non-overlapping join keys
// Test Result:
//      - Joined result should be empty
//
TEST_F(MaterializedExecutorTest, HashJoinNoMatches) {
    // Setup users table
    auto user_schema = std::make_shared<common::Schema>();
    user_schema->AddField("id", common::ColumnType::INT32);
    user_schema->AddField("name", common::ColumnType::STRING);
    CreateNewTable("no_match_users", user_schema);

    // Setup orders table
    auto order_schema = std::make_shared<common::Schema>();
    order_schema->AddField("user_id", common::ColumnType::INT32);
    order_schema->AddField("order_id", common::ColumnType::INT32);
    CreateNewTable("no_match_orders", order_schema);

    // Ingest non-matching data
    auto user_batch = ArrowRecordBatchBuilder()
                          .AddInt32Column("id", {1, 2, 3})
                          .AddStringColumn("name", {"Alice", "Bob", "Charlie"})
                          .Build()
                          .value();
    IngestData("no_match_users", user_batch);

    auto order_batch = ArrowRecordBatchBuilder()
                           .AddInt32Column("user_id", {4, 5, 6})
                           .AddInt32Column("order_id", {100, 101, 102})
                           .Build()
                           .value();
    IngestData("no_match_orders", order_batch);

    // Create join plan
    auto left_plan = std::make_shared<PhysicalSequentialScanNode>(
        "no_match_users", *user_schema, nullptr, std::nullopt, std::nullopt, std::nullopt);
    auto right_plan = std::make_shared<PhysicalSequentialScanNode>(
        "no_match_orders", *order_schema, nullptr, std::nullopt, std::nullopt, std::nullopt);

    auto join_condition =
        std::make_shared<common::BinaryExpression>(common::BinaryOpType::Equal,
                                                   std::make_shared<common::ColumnExpression>("", "id"),
                                                   std::make_shared<common::ColumnExpression>("", "user_id"));

    common::Schema output_schema;
    output_schema.AddField("id", common::ColumnType::INT32);
    output_schema.AddField("name", common::ColumnType::STRING);
    output_schema.AddField("user_id", common::ColumnType::INT32);
    output_schema.AddField("order_id", common::ColumnType::INT32);

    auto join_node = std::make_shared<PhysicalHashJoinNode>(join_condition, output_schema, common::JoinType::Inner);
    join_node->AddChild(left_plan);
    join_node->AddChild(right_plan);

    // Execute
    auto result = executor_->ExecuteToCompletion(join_node);
    VERIFY_RESULT(result);

    auto batch = result.value();
    EXPECT_EQ(batch->num_rows(), 0);
    EXPECT_EQ(batch->num_columns(), 4);
}

//
// Test Setup:
//      - Create tables with NULL values in join columns
// Test Result:
//      - Rows with NULL join keys should not be matched
//
TEST_F(MaterializedExecutorTest, HashJoinWithNulls) {
    // Create nullable tables
    auto left_schema = std::make_shared<common::Schema>();
    left_schema->AddField("id", common::ColumnType::INT32, common::Nullability::NULLABLE);
    left_schema->AddField("data", common::ColumnType::STRING);
    CreateNewTable("nullable_left", left_schema);

    auto right_schema = std::make_shared<common::Schema>();
    right_schema->AddField("id", common::ColumnType::INT32, common::Nullability::NULLABLE);
    right_schema->AddField("info", common::ColumnType::STRING);
    CreateNewTable("nullable_right", right_schema);

    // Ingest data with nulls
    auto left_batch = ArrowRecordBatchBuilder()
                          .AddInt32Column("id", {1, 2, -1}, {true, true, false})
                          .AddStringColumn("data", {"A", "B", "C"})
                          .Build()
                          .value();
    IngestData("nullable_left", left_batch);

    auto right_batch = ArrowRecordBatchBuilder()
                           .AddInt32Column("id", {1, -1, 3}, {true, false, true})
                           .AddStringColumn("info", {"X", "Y", "Z"})
                           .Build()
                           .value();
    IngestData("nullable_right", right_batch);

    // Create join plan
    auto left_plan = std::make_shared<PhysicalSequentialScanNode>(
        "nullable_left", *left_schema, nullptr, std::nullopt, std::nullopt, std::nullopt);
    auto right_plan = std::make_shared<PhysicalSequentialScanNode>(
        "nullable_right", *right_schema, nullptr, std::nullopt, std::nullopt, std::nullopt);

    auto join_condition =
        std::make_shared<common::BinaryExpression>(common::BinaryOpType::Equal,
                                                   std::make_shared<common::ColumnExpression>("", "id"),
                                                   std::make_shared<common::ColumnExpression>("", "id"));

    common::Schema output_schema;
    output_schema.AddField("id", common::ColumnType::INT32);
    output_schema.AddField("data", common::ColumnType::STRING);
    output_schema.AddField("info", common::ColumnType::STRING);

    auto join_node = std::make_shared<PhysicalHashJoinNode>(join_condition, output_schema, common::JoinType::Inner);
    join_node->AddChild(left_plan);
    join_node->AddChild(right_plan);

    // Execute
    auto result = executor_->ExecuteToCompletion(join_node);
    VERIFY_RESULT(result);

    std::cout << "Result: " << ArrowUtil::BatchToString(result.value()) << std::endl;

    auto batch = result.value();
    EXPECT_EQ(batch->num_rows(), 1);  // Only ID=1 should match
    EXPECT_EQ(batch->num_columns(), 3);
}

//
// Test Setup:
//      - Create tables with duplicate join keys
// Test Result:
//      - Should produce Cartesian product of matching rows
//
TEST_F(MaterializedExecutorTest, HashJoinDuplicateKeys) {
    // Setup tables
    auto left_schema = std::make_shared<common::Schema>();
    left_schema->AddField("key", common::ColumnType::INT32);
    CreateNewTable("dup_left", left_schema);

    auto right_schema = std::make_shared<common::Schema>();
    right_schema->AddField("key", common::ColumnType::INT32);
    CreateNewTable("dup_right", right_schema);

    // Ingest duplicate data
    auto left_batch = ArrowRecordBatchBuilder().AddInt32Column("key", {1, 1, 2}).Build().value();
    IngestData("dup_left", left_batch);

    auto right_batch = ArrowRecordBatchBuilder().AddInt32Column("key", {1, 1, 3}).Build().value();
    IngestData("dup_right", right_batch);

    // Create join plan
    auto left_plan = std::make_shared<PhysicalSequentialScanNode>(
        "dup_left", *left_schema, nullptr, std::nullopt, std::nullopt, std::nullopt);
    auto right_plan = std::make_shared<PhysicalSequentialScanNode>(
        "dup_right", *right_schema, nullptr, std::nullopt, std::nullopt, std::nullopt);

    auto join_condition =
        std::make_shared<common::BinaryExpression>(common::BinaryOpType::Equal,
                                                   std::make_shared<common::ColumnExpression>("", "key"),
                                                   std::make_shared<common::ColumnExpression>("", "key"));

    common::Schema output_schema;
    output_schema.AddField("key", common::ColumnType::INT32);

    auto join_node = std::make_shared<PhysicalHashJoinNode>(join_condition, output_schema, common::JoinType::Inner);
    join_node->AddChild(left_plan);
    join_node->AddChild(right_plan);

    // Execute
    auto result = executor_->ExecuteToCompletion(join_node);
    VERIFY_RESULT(result);

    auto batch = result.value();
    EXPECT_EQ(batch->num_rows(), 4);  // 2 left * 2 right matches
}

//
// Test Setup:
//      - Use non-equality condition in join
// Test Result:
//      - Should return NOT_IMPLEMENTED error
//
TEST_F(MaterializedExecutorTest, HashJoinInvalidCondition) {
    // Create schemas for left and right tables
    auto left_schema = std::make_shared<common::Schema>();
    left_schema->AddField("a", common::ColumnType::INT32);
    CreateNewTable("left_table", left_schema);

    auto right_schema = std::make_shared<common::Schema>();
    right_schema->AddField("b", common::ColumnType::INT32);
    CreateNewTable("right_table", right_schema);

    // Create scan plans
    auto left_plan = std::make_shared<PhysicalSequentialScanNode>(
        "left_table", *left_schema, nullptr, std::nullopt, std::nullopt, std::nullopt);
    auto right_plan = std::make_shared<PhysicalSequentialScanNode>(
        "right_table", *right_schema, nullptr, std::nullopt, std::nullopt, std::nullopt);

    // Create join condition with greater than operator
    auto join_condition =
        std::make_shared<common::BinaryExpression>(common::BinaryOpType::Greater,
                                                   std::make_shared<common::ColumnExpression>("", "a"),
                                                   std::make_shared<common::ColumnExpression>("", "b"));

    // Create output schema
    common::Schema output_schema;
    output_schema.AddField("a", common::ColumnType::INT32);
    output_schema.AddField("b", common::ColumnType::INT32);

    // Create join node
    auto join_node = std::make_shared<PhysicalHashJoinNode>(join_condition, output_schema, common::JoinType::Inner);
    join_node->AddChild(left_plan);
    join_node->AddChild(right_plan);

    // Execute
    auto result = executor_->ExecuteToCompletion(join_node);

    // Verify error
    EXPECT_FALSE(result.ok());
    EXPECT_EQ(result.error().code(), common::ErrorCode::NotImplemented);
}

}  // namespace pond::query