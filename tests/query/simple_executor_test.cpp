#include "query/executor/simple_executor.h"

#include <arrow/api.h>
#include <arrow/builder.h>
#include <arrow/record_batch.h>
#include <arrow/testing/gtest_util.h>
#include <gtest/gtest.h>

#include "catalog/data_ingestor.h"
#include "catalog/kv_catalog.h"
#include "common/memory_append_only_fs.h"
#include "kv/db.h"
#include "query/data/catalog_data_accessor.h"
#include "query_test_helper.h"
#include "test_helper.h"

namespace pond::query {

class SimpleExecutorTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create the query test context
        query_context_ = std::make_unique<QueryTestContext>("test_catalog");

        // Create data accessor using the context's catalog
        data_accessor_ = std::make_shared<CatalogDataAccessor>(query_context_->catalog_, query_context_->fs_);

        // Create the executor
        executor_ = std::make_unique<SimpleExecutor>(query_context_->catalog_, data_accessor_);

        // Setup the test table and ingest data
        SetupTestTable();
        IngestTestData();
    }

    void SetupTestTable() {
        // Use the QueryTestContext to set up the users table with the schema we need
        query_context_->SetupUsersTable();

        // Get the table schema from the context
        auto schema_result = query_context_->catalog_->LoadTable("users");
        ASSERT_TRUE(schema_result.ok()) << "Failed to load table schema: " << schema_result.error().message();
        schema_ = schema_result.value().schema;
    }

    void IngestTestData() {
        // Create data ingestor using the context's catalog
        auto ingestor_result = catalog::DataIngestor::Create(query_context_->catalog_, query_context_->fs_, "users");
        VERIFY_RESULT(ingestor_result);
        auto ingestor = std::move(ingestor_result).value();

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

        auto schema = arrow::schema({arrow::field("id", arrow::int32()),
                                     arrow::field("name", arrow::utf8()),
                                     arrow::field("age", arrow::int32()),
                                     arrow::field("salary", arrow::float64())});

        auto record_batch = arrow::RecordBatch::Make(schema, 5, {id_array, name_array, age_array, salary_array});

        // Ingest the batch
        auto ingest_result = ingestor->IngestBatch(record_batch);
        VERIFY_RESULT(ingest_result);
    }

    std::shared_ptr<PhysicalPlanNode> CreateScanPlan() {
        // Create a simple sequential scan plan for the users table using the schema from the context
        return std::make_shared<PhysicalSequentialScanNode>("users", *schema_);
    }

    std::shared_ptr<DataAccessor> data_accessor_;
    std::unique_ptr<SimpleExecutor> executor_;
    std::shared_ptr<common::Schema> schema_;
    std::unique_ptr<QueryTestContext> query_context_;
};

//
// Test Setup:
//      Create a simple sequential scan plan for users table with 5 rows of data
// Test Result:
//      Execute returns a DataBatch with 5 rows and correct column values
//
TEST_F(SimpleExecutorTest, SelectAllFromTable) {
    // Create a simple scan plan
    auto plan = CreateScanPlan();

    // Execute the plan
    auto result = executor_->execute(plan);
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
TEST_F(SimpleExecutorTest, SelectFromEmptyTable) {
    // Create a new empty table using the context
    auto empty_schema = common::CreateSchemaBuilder()
                            .AddField("col1", common::ColumnType::INT32)
                            .AddField("col2", common::ColumnType::STRING)
                            .Build();

    auto create_result = query_context_->catalog_->CreateTable(
        "empty_table", empty_schema, query_context_->partition_spec_, "test_catalog/empty_table");
    VERIFY_RESULT(create_result);

    // Create a scan plan for the empty table
    auto plan = std::make_shared<PhysicalSequentialScanNode>("empty_table", *empty_schema);

    // Execute the plan
    auto result = executor_->execute(plan);
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
TEST_F(SimpleExecutorTest, InvalidTable) {
    // Create a scan plan for a non-existent table
    auto plan = std::make_shared<PhysicalSequentialScanNode>("non_existent_table", *schema_);

    // Execute the plan
    auto result = executor_->execute(plan);
    ASSERT_FALSE(result.ok()) << "Execution should fail for non-existent table";
}

//
// Test Setup:
//      Execute a SQL query "SELECT * FROM users" through the SQL parser,
//      logical planner, physical planner, and our executor
// Test Result:
//      Query execution returns the correct data batch with all 5 rows
//
TEST_F(SimpleExecutorTest, ExecuteSQLQuery) {
    // Create SQL query
    std::string sql_query = "SELECT * FROM users";

    // Plan the query and get the physical plan
    auto physical_plan_result = query_context_->PlanPhysical(sql_query);
    ASSERT_TRUE(physical_plan_result.ok()) << "Physical planning failed: " << physical_plan_result.error().message();
    auto physical_plan = physical_plan_result.value();

    // Execute the physical plan
    auto result = executor_->execute(physical_plan);
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
TEST_F(SimpleExecutorTest, ExecuteSQLQueryWithFilter) {
    // Create SQL query with filter
    std::string sql_query = "SELECT * FROM users WHERE age > 30";

    // Plan the query and get the physical plan
    auto physical_plan_result = query_context_->PlanPhysical(sql_query);
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
    auto result = executor_->execute(physical_plan);
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

}  // namespace pond::query