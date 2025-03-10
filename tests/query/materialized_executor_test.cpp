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
        IngestTestData();
    }

    void SetupTestTable() {
        // Get the table schema from the context
        auto schema_result = catalog_->LoadTable("users");
        ASSERT_TRUE(schema_result.ok()) << "Failed to load table schema: " << schema_result.error().message();
    }

    void IngestTestData() {
        // Create test data
        arrow::Int32Builder id_builder;
        arrow::StringBuilder name_builder;
        arrow::Int32Builder age_builder;
        arrow::DoubleBuilder salary_builder;

        ASSERT_OK(id_builder.AppendValues({1, 2, 3, 4, 5}));
        ASSERT_OK(name_builder.AppendValues({"Alice", "Bob", "Charlie", "David", "Eve"}));
        ASSERT_OK(age_builder.AppendValues({25, 30, 35, 40, 45}));
        ASSERT_OK(salary_builder.AppendValues({75000.0, 85000.0, 95000.0, 105000.0, 115000.0}));

        std::shared_ptr<arrow::Array> id_array;
        std::shared_ptr<arrow::Array> name_array;
        std::shared_ptr<arrow::Array> age_array;
        std::shared_ptr<arrow::Array> salary_array;

        ASSERT_OK(id_builder.Finish(&id_array));
        ASSERT_OK(name_builder.Finish(&name_array));
        ASSERT_OK(age_builder.Finish(&age_array));
        ASSERT_OK(salary_builder.Finish(&salary_array));

        auto record_batch =
            arrow::RecordBatch::Make(GetArrowSchema("users"), 5, {id_array, name_array, age_array, salary_array});

        // Ingest the batch
        IngestData("users", record_batch);
    }

    // Sets up multiple test files with sequential data
    void SetupMultipleFiles(int num_files, int rows_per_file) {
        // For each file, create a data ingestor and ingest a batch with unique data
        for (int file_id = 0; file_id < num_files; file_id++) {
            // Create record batch with data for this file
            arrow::Int32Builder id_builder;
            arrow::StringBuilder name_builder;
            arrow::Int32Builder age_builder;
            arrow::DoubleBuilder value_builder;

            // Each file has rows_per_file rows with sequential IDs and file-specific data
            for (int i = 0; i < rows_per_file; i++) {
                int id = file_id * rows_per_file + i;
                std::string name = "user_" + std::to_string(file_id) + "_" + std::to_string(i);
                int age = 20 + ((file_id * rows_per_file + i) % 30);  // Ages between 20-49
                double value = (file_id * rows_per_file + i) * 1.5;

                ASSERT_OK(id_builder.Append(id));
                ASSERT_OK(name_builder.Append(name));
                ASSERT_OK(age_builder.Append(age));
                ASSERT_OK(value_builder.Append(value));
            }

            std::shared_ptr<arrow::Array> id_array = id_builder.Finish().ValueOrDie();
            std::shared_ptr<arrow::Array> name_array = name_builder.Finish().ValueOrDie();
            std::shared_ptr<arrow::Array> age_array = age_builder.Finish().ValueOrDie();
            std::shared_ptr<arrow::Array> value_array = value_builder.Finish().ValueOrDie();

            auto record_batch = arrow::RecordBatch::Make(
                GetArrowSchema("multi_users"), rows_per_file, {id_array, name_array, age_array, value_array});

            // Ingest the batch
            IngestData("multi_users", record_batch);
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
    auto id_column = common::MakeColumn("", "id");
    auto constant = common::MakeIntegerConstant(7);
    auto predicate = common::MakeComparison(common::BinaryOpType::Greater, id_column, constant);

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
    auto schema = GetSchema("users");

    // Create a scan plan
    auto scan_node = std::make_shared<PhysicalSequentialScanNode>("users", *schema);

    // Add a projection to select only id and name columns
    std::vector<std::shared_ptr<common::Expression>> projections = {common::MakeColumn("users", "id"),
                                                                    common::MakeColumn("users", "name")};

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
    auto schema = GetSchema("users");

    // Create a scan plan
    auto scan_node = std::make_shared<PhysicalSequentialScanNode>("users", *schema);

    // Add a projection to select only the age column
    std::vector<std::shared_ptr<common::Expression>> projections = {common::MakeColumn("", "age")};

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
    auto schema = GetSchema("users");

    // Create a scan plan
    auto scan_node = std::make_shared<PhysicalSequentialScanNode>("users", *schema);

    // Add a filter: age > 30
    auto age_column = common::MakeColumn("users", "age");
    auto constant = common::MakeIntegerConstant(30);
    auto predicate = common::MakeComparison(common::BinaryOpType::Greater, age_column, constant);

    // Create a filter node
    auto filter_node = std::make_shared<PhysicalFilterNode>(predicate, scan_node->OutputSchema());
    filter_node->AddChild(scan_node);

    // Add a projection to select id and name columns
    std::vector<std::shared_ptr<common::Expression>> projections = {common::MakeColumn("users", "id"),
                                                                    common::MakeColumn("users", "name")};

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
    // Setup 3 files with 5 rows each
    SetupMultipleFiles(3, 5);
    auto schema = GetSchema("multi_users");

    // Create a scan plan
    auto scan_node = std::make_shared<PhysicalSequentialScanNode>("multi_users", *schema);

    // Add a projection to select only id and name columns
    std::vector<std::shared_ptr<common::Expression>> projections = {common::MakeColumn("", "id"),
                                                                    common::MakeColumn("", "name")};

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
    auto schema = GetSchema("users");

    // Create a scan plan
    auto scan_node = std::make_shared<PhysicalSequentialScanNode>("users", *schema);

    // Add a projection with a non-existent column
    std::vector<std::shared_ptr<common::Expression>> projections = {common::MakeColumn("", "id"),
                                                                    common::MakeColumn("", "non_existent_column")};

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

}  // namespace pond::query