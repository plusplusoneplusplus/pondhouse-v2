#include "query/executor/vectorized_executor.h"

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

/**
 * @brief Test fixture for VectorizedExecutor tests
 *
 * Provides test setup with a memory filesystem, catalog, and sample data
 */
class VectorizedExecutorTest : public QueryTestContext {
protected:
    VectorizedExecutorTest() : QueryTestContext("test_catalog") {}

    void SetUp() override {
        QueryTestContext::SetUp();

        // Create data accessor using the context's catalog
        data_accessor_ = std::make_shared<CatalogDataAccessor>(catalog_, fs_);

        // Create the executor
        executor_ = std::make_unique<VectorizedExecutor>(catalog_, data_accessor_);
    }

    void IngestTestData(int num_files, int num_rows) {
        auto batches = GetSampleBatches("users", num_rows, num_files);
        for (const auto& batch : batches) {
            IngestData("users", batch);
        }
    }

    std::shared_ptr<PhysicalPlanNode> CreateScanPlan(std::shared_ptr<common::Expression> predicate = nullptr) {
        return std::make_shared<PhysicalSequentialScanNode>("users", *GetSchema("users"), predicate);
    }

    std::shared_ptr<DataAccessor> data_accessor_;
    std::unique_ptr<VectorizedExecutor> executor_;
};

//
// Test Setup:
//      Execute a simple scan query using the iterator model
// Test Result:
//      Iterator should return batches as expected
//
TEST_F(VectorizedExecutorTest, SimpleScanWithIterator) {
    IngestTestData(1, 5);  // 1 file, 5 rows

    // Create a scan plan
    auto scan_node = CreateScanPlan();

    // Execute and get iterator
    auto iterator_result = executor_->Execute(scan_node);
    VERIFY_RESULT(iterator_result);
    auto iterator = std::move(iterator_result).value();

    // Verify we have data
    ASSERT_TRUE(iterator->HasNext());

    // Get the first batch
    auto batch_result = iterator->Next();
    VERIFY_RESULT(batch_result);
    auto batch = batch_result.value();

    // Verify the batch
    ASSERT_NE(nullptr, batch);
    ASSERT_GT(batch->num_rows(), 0);
    ASSERT_EQ((*GetSchema("users")).NumColumns(), batch->num_columns());

    // Verify schema
    auto schema = batch->schema();
    ASSERT_EQ("id", schema->field(0)->name());
    ASSERT_EQ("name", schema->field(1)->name());
    ASSERT_EQ("age", schema->field(2)->name());

    // Verify we don't have more data (the test table is small)
    ASSERT_FALSE(iterator->HasNext());
}

//
// Test Setup:
//      Execute a simple scan query using both ExecuteToCompletion and Execute methods
// Test Result:
//      Both methods should return the same data
//
TEST_F(VectorizedExecutorTest, ExecuteVsExecuteToCompletion) {
    // Set up multiple files with a larger dataset for this test
    IngestTestData(3, 50);  // 3 files, 50 rows each

    // Create a scan plan for the multi-file table
    auto scan_node = CreateScanPlan();

    // Execute the plan using ExecuteToCompletion
    auto complete_result = executor_->ExecuteToCompletion(scan_node);
    VERIFY_RESULT(complete_result);
    auto complete_batch = complete_result.value();

    // Execute the plan using Execute and collect results
    auto iterator_result = executor_->Execute(scan_node);
    VERIFY_RESULT(iterator_result);
    auto iterator = std::move(iterator_result).value();

    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    int total_rows = 0;

    while (iterator->HasNext()) {
        auto batch_result = iterator->Next();
        VERIFY_RESULT(batch_result);
        auto batch = batch_result.value();
        ASSERT_NE(nullptr, batch);

        batches.push_back(batch);
        total_rows += batch->num_rows();
    }

    // Verify the total number of rows is the same
    ASSERT_EQ(complete_batch->num_rows(), total_rows);
    ASSERT_EQ(150, total_rows);  // 3 files * 50 rows

    // Verify batches were actually split (we should have multiple batches)
    ASSERT_GT(batches.size(), 1);

    LOG_STATUS("Collected %zu batches with total %d rows", batches.size(), total_rows);
}

//
// Test Setup:
//      Execute a scan with filter query using the iterator model
// Test Result:
//      Iterator should return filtered batches as expected
//
TEST_F(VectorizedExecutorTest, ScanWithFilterIterator) {
    // Set up multiple files with a larger dataset for this test
    IngestTestData(3, 50);  // 3 files, 50 rows each

    // Add a filter (age > 50)
    auto age_column = common::MakeColumn("", "age");
    auto fifty = common::MakeIntegerConstant(50);
    auto predicate = common::MakeComparison(common::BinaryOpType::Greater, age_column, fifty);

    // Create a scan node
    auto scan_node = CreateScanPlan(predicate);

    // Execute and get iterator
    auto iterator_result = executor_->Execute(scan_node);
    VERIFY_RESULT(iterator_result);
    auto iterator = std::move(iterator_result).value();

    // Verify we have data
    ASSERT_TRUE(iterator->HasNext());

    // Collect and verify all batches
    int total_rows = 0;
    int batch_count = 0;

    while (iterator->HasNext()) {
        auto batch_result = iterator->Next();
        VERIFY_RESULT(batch_result);
        auto batch = batch_result.value();
        ASSERT_NE(nullptr, batch);

        // Verify each row has age > 50
        auto age_array = std::static_pointer_cast<arrow::Int32Array>(batch->column(2));
        for (int i = 0; i < batch->num_rows(); i++) {
            ASSERT_GT(age_array->Value(i), 50);
        }

        total_rows += batch->num_rows();
        batch_count++;
    }

    // Verify we got some data but not all (due to the filter)
    ASSERT_GT(total_rows, 0);
    ASSERT_LT(total_rows, 150);  // Less than the total 150 rows

    LOG_STATUS("Filter kept %d rows in %d batches", total_rows, batch_count);
}

//
// Test Setup:
//      Execute a scan with projection query using the iterator model
// Test Result:
//      Iterator should return projected batches as expected
//
TEST_F(VectorizedExecutorTest, ScanWithProjectionIterator) {
    IngestTestData(1, 5);

    // Add projections for only id and name
    std::vector<std::shared_ptr<common::Expression>> projections = {common::MakeColumn("", "id"),
                                                                    common::MakeColumn("", "name")};

    // Create projection node with scan as child
    auto projection_schema = common::CreateSchemaBuilder()
                                 .AddField("id", common::ColumnType::INT32)
                                 .AddField("name", common::ColumnType::STRING)
                                 .Build();

    // Create a scan node
    auto scan_node = CreateScanPlan();

    auto projection_node = std::make_shared<PhysicalProjectionNode>(projections, *projection_schema);
    projection_node->AddChild(scan_node);

    // Execute and get iterator
    auto iterator_result = executor_->Execute(projection_node);
    VERIFY_RESULT(iterator_result);
    auto iterator = std::move(iterator_result).value();

    // Verify we have data
    ASSERT_TRUE(iterator->HasNext());

    // Get the batch
    auto batch_result = iterator->Next();
    VERIFY_RESULT(batch_result);
    auto batch = batch_result.value();

    // Verify the projection worked
    ASSERT_NE(nullptr, batch);
    ASSERT_GT(batch->num_rows(), 0);
    ASSERT_EQ(2, batch->num_columns());

    // Verify schema
    auto schema = batch->schema();
    ASSERT_EQ("id", schema->field(0)->name());
    ASSERT_EQ("name", schema->field(1)->name());
}

//
// Test Setup:
//      Execute a complex query with scan + filter + projection
// Test Result:
//      Iterator should return filtered and projected batches as expected
//
TEST_F(VectorizedExecutorTest, ComplexQuery) {
    // Set up multiple files with a larger dataset
    IngestTestData(3, 50);  // 3 files, 50 rows each

    // Create a scan node
    auto scan_node = CreateScanPlan();

    // Add a filter (age > 50)
    auto age_column = common::MakeColumn("", "age");
    auto fifty = common::MakeIntegerConstant(50);
    auto predicate = common::MakeComparison(common::BinaryOpType::Greater, age_column, fifty);

    auto filter_node = std::make_shared<PhysicalFilterNode>(predicate, *GetSchema("users"));
    filter_node->AddChild(scan_node);

    // Add projections for id and name
    std::vector<std::shared_ptr<common::Expression>> projections = {common::MakeColumn("", "id"),
                                                                    common::MakeColumn("", "name")};

    // Create projection node with filter as child
    auto projection_schema = common::CreateSchemaBuilder()
                                 .AddField("id", common::ColumnType::INT32)
                                 .AddField("name", common::ColumnType::STRING)
                                 .Build();

    auto projection_node = std::make_shared<PhysicalProjectionNode>(projections, *projection_schema);
    projection_node->AddChild(filter_node);

    // Execute and get iterator
    auto iterator_result = executor_->Execute(projection_node);
    VERIFY_RESULT(iterator_result);
    auto iterator = std::move(iterator_result).value();

    // Verify we have data
    ASSERT_TRUE(iterator->HasNext());

    // Collect and verify all batches
    int total_rows = 0;
    int batch_count = 0;

    while (iterator->HasNext()) {
        auto batch_result = iterator->Next();
        VERIFY_RESULT(batch_result);
        auto batch = batch_result.value();
        ASSERT_NE(nullptr, batch);

        // Verify schema (should have id and name only)
        ASSERT_EQ(2, batch->num_columns());
        auto schema = batch->schema();
        ASSERT_EQ("id", schema->field(0)->name());
        ASSERT_EQ("name", schema->field(1)->name());

        total_rows += batch->num_rows();
        batch_count++;
    }

    // Verify we got some data but not all (due to the filter)
    ASSERT_GT(total_rows, 0);
    ASSERT_LT(total_rows, 150);  // Less than the total 150 rows

    LOG_STATUS("Complex query returned %d rows in %d batches", total_rows, batch_count);
}

//
// Test Setup:
//      Check handling of null plan
// Test Result:
//      Should return an error
//
TEST_F(VectorizedExecutorTest, NullPlanHandling) {
    // Execute null plan
    auto result = executor_->Execute(nullptr);
    VERIFY_ERROR_CODE(result, common::ErrorCode::InvalidArgument);

    auto complete_result = executor_->ExecuteToCompletion(nullptr);
    VERIFY_ERROR_CODE(complete_result, common::ErrorCode::InvalidArgument);
}

//
// Test Setup:
//      Execute a query against a non-existent table
// Test Result:
//      Should return an error
//
TEST_F(VectorizedExecutorTest, NonExistentTable) {
    // Create a scan plan for a non-existent table
    auto scan_node = std::make_shared<PhysicalSequentialScanNode>("non_existent_table", *GetSchema("users"), nullptr);

    // Execute and check error
    auto result = executor_->Execute(scan_node);
    ASSERT_FALSE(result.ok());
}

}  // namespace pond::query