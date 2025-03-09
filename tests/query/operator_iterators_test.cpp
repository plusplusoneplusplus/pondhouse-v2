#include "query/executor/operator_iterators.h"

#include <memory>

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
 * @brief Mock operator iterator for testing
 *
 * Provides pre-defined batches for testing parent operators
 */
class MockOperatorIterator : public BaseOperatorIterator {
public:
    MockOperatorIterator(const common::Schema& schema, const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches)
        : BaseOperatorIterator(schema), batches_(batches), current_batch_(0) {}

    common::Result<ArrowDataBatchSharedPtr> Next() override {
        if (!initialized_) {
            auto init_result = Initialize();
            if (!init_result.ok()) {
                return common::Result<ArrowDataBatchSharedPtr>::failure(init_result.error());
            }
        }

        if (!has_next_) {
            return common::Result<ArrowDataBatchSharedPtr>::success(nullptr);
        }

        auto batch = batches_[current_batch_++];
        has_next_ = (current_batch_ < batches_.size());

        return common::Result<ArrowDataBatchSharedPtr>::success(batch);
    }

protected:
    common::Result<bool> InitializeImpl() override {
        has_next_ = !batches_.empty();
        return common::Result<bool>::success(has_next_);
    }

private:
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches_;
    size_t current_batch_;
};

/**
 * @brief Test fixture for operator iterators
 *
 * Provides test setup with a memory filesystem, catalog, and sample data
 */
class OperatorIteratorsTest : public QueryTestContext {
protected:
    OperatorIteratorsTest() : QueryTestContext("test_catalog") {}

    void SetUp() override {
        QueryTestContext::SetUp();

        // Create data accessor using the context's catalog
        data_accessor_ = std::make_shared<CatalogDataAccessor>(catalog_, fs_);

        // Create sample data for tests
        sample_batches_ = GetSampleBatches("users");
        for (auto& batch : sample_batches_) {
            IngestData("users", batch);
        }
    }

    std::unique_ptr<MockOperatorIterator> NewMockIterator() {
        return std::make_unique<MockOperatorIterator>(*GetSchema("users"), sample_batches_);
    }

    std::shared_ptr<DataAccessor> data_accessor_;
    std::vector<std::shared_ptr<arrow::RecordBatch>> sample_batches_;
};

//
// Test Setup:
//      Create and use a MockOperatorIterator
// Test Result:
//      Should return batches as expected
//
TEST_F(OperatorIteratorsTest, MockOperatorIterator) {
    // Create a mock iterator with our sample batches
    auto mock_iterator = NewMockIterator();

    // Initialize the iterator
    auto init_result = mock_iterator->Initialize();
    ASSERT_TRUE(init_result.ok());
    ASSERT_TRUE(init_result.value());

    // Verify HasNext works
    ASSERT_TRUE(mock_iterator->HasNext());

    // Get schema and verify
    auto schema_result = mock_iterator->GetSchema();
    ASSERT_TRUE(schema_result.ok());
    ASSERT_EQ(4, schema_result.value().FieldCount());

    // Get all batches
    int batch_count = 0;
    int total_rows = 0;

    while (mock_iterator->HasNext()) {
        auto batch_result = mock_iterator->Next();
        ASSERT_TRUE(batch_result.ok());
        auto batch = batch_result.value();
        ASSERT_NE(nullptr, batch);

        batch_count++;
        total_rows += batch->num_rows();
    }

    // Verify we got all batches
    ASSERT_EQ(3, batch_count);
    ASSERT_EQ(15, total_rows);  // 3 batches * 5 rows

    // Verify HasNext is now false
    ASSERT_FALSE(mock_iterator->HasNext());

    // Next should return nullptr when no more data
    auto batch_result = mock_iterator->Next();
    ASSERT_TRUE(batch_result.ok());
    ASSERT_EQ(nullptr, batch_result.value());
}

//
// Test Setup:
//      Create a FilterIterator with a child MockOperatorIterator
// Test Result:
//      Should filter rows based on predicate
//
TEST_F(OperatorIteratorsTest, FilterIterator) {
    // Create a predicate (age > 25)
    auto age_column = common::MakeColumn("", "age");
    auto twenty_five = common::MakeIntegerConstant(25);
    auto predicate = common::MakeComparison(common::BinaryOpType::Greater, age_column, twenty_five);

    // Create a mock iterator as the child
    auto mock_iterator = NewMockIterator();

    // Create the filter iterator
    auto filter_iterator = std::make_unique<FilterIterator>(std::move(mock_iterator), predicate, *GetSchema("users"));

    // Initialize the iterator
    auto init_result = filter_iterator->Initialize();
    ASSERT_TRUE(init_result.ok());
    ASSERT_TRUE(init_result.value());

    // Verify HasNext works
    ASSERT_TRUE(filter_iterator->HasNext());

    // Collect all batches
    std::vector<std::shared_ptr<arrow::RecordBatch>> filtered_batches;
    int total_rows = 0;

    while (filter_iterator->HasNext()) {
        auto batch_result = filter_iterator->Next();
        ASSERT_TRUE(batch_result.ok());
        auto batch = batch_result.value();
        ASSERT_NE(nullptr, batch);

        // Verify each row has age > 25
        auto age_array = std::static_pointer_cast<arrow::Int32Array>(batch->column(2));
        for (int i = 0; i < batch->num_rows(); i++) {
            ASSERT_GT(age_array->Value(i), 25);
        }

        filtered_batches.push_back(batch);
        total_rows += batch->num_rows();
    }

    // Verify we filtered correctly: batches 1 and 2 should remain
    // Batch 0 has ages 20-24, batch 1 has 30-34, batch 2 has 40-44
    ASSERT_GT(filtered_batches.size(), 0);
    ASSERT_EQ(10, total_rows);  // 2 batches * 5 rows = 10 total rows
}

//
// Test Setup:
//      Create a ProjectionIterator with a child MockOperatorIterator
// Test Result:
//      Should project only selected columns
//
TEST_F(OperatorIteratorsTest, ProjectionIterator) {
    // Create projections for only id and name
    std::vector<std::shared_ptr<common::Expression>> projections = {common::MakeColumn("", "id"),
                                                                    common::MakeColumn("", "name")};

    // Create output schema for the projection
    auto projection_schema = common::CreateSchemaBuilder()
                                 .AddField("id", common::ColumnType::INT32)
                                 .AddField("name", common::ColumnType::STRING)
                                 .Build();

    // Create a mock iterator as the child
    auto mock_iterator = NewMockIterator();

    // Create the projection iterator
    auto projection_iterator =
        std::make_unique<ProjectionIterator>(std::move(mock_iterator), projections, *projection_schema);

    // Initialize the iterator
    auto init_result = projection_iterator->Initialize();
    ASSERT_TRUE(init_result.ok());
    ASSERT_TRUE(init_result.value());

    // Verify HasNext works
    ASSERT_TRUE(projection_iterator->HasNext());

    // Get schema and verify
    auto schema_result = projection_iterator->GetSchema();
    ASSERT_TRUE(schema_result.ok());
    ASSERT_EQ(2, schema_result.value().FieldCount());

    // Collect all batches
    std::vector<std::shared_ptr<arrow::RecordBatch>> projected_batches;

    while (projection_iterator->HasNext()) {
        auto batch_result = projection_iterator->Next();
        ASSERT_TRUE(batch_result.ok());
        auto batch = batch_result.value();
        ASSERT_NE(nullptr, batch);

        // Verify schema has only id and name columns
        ASSERT_EQ(2, batch->num_columns());
        auto schema = batch->schema();
        ASSERT_EQ("id", schema->field(0)->name());
        ASSERT_EQ("name", schema->field(1)->name());

        projected_batches.push_back(batch);
    }

    // Verify we got all batches with correct projection
    ASSERT_EQ(3, projected_batches.size());
    ASSERT_EQ(5, projected_batches[0]->num_rows());
}

//
// Test Setup:
//      Create a SequentialScanIterator to scan a table
// Test Result:
//      Should read data from files with correct batching
//
TEST_F(OperatorIteratorsTest, SequentialScanIterator) {
    // Set up a separate table with multiple files for scanning
    auto schema = GetSchema("users");

    // Create "scan_test" table
    catalog::PartitionSpec spec;  // empty partition spec
    auto create_result = catalog_->CreateTable("scan_test", schema, spec, "/scan_test");
    VERIFY_RESULT(create_result);

    // Create data ingestor
    auto ingestor_result = catalog::DataIngestor::Create(catalog_, fs_, "scan_test");
    VERIFY_RESULT(ingestor_result);
    auto ingestor = std::move(ingestor_result).value();

    ingestor->IngestBatches(sample_batches_);

    // Create a scan iterator for the table
    auto scan_iterator = std::make_unique<SequentialScanIterator>(
        data_accessor_, "scan_test", std::shared_ptr<common::Expression>(), std::vector<std::string>{}, *schema);

    // Initialize the iterator
    auto init_result = scan_iterator->Initialize();
    ASSERT_TRUE(init_result.ok());
    ASSERT_TRUE(init_result.value());

    // Verify HasNext works
    ASSERT_TRUE(scan_iterator->HasNext());

    // Collect all batches
    std::vector<std::shared_ptr<arrow::RecordBatch>> scanned_batches;
    int total_rows = 0;

    while (scan_iterator->HasNext()) {
        auto batch_result = scan_iterator->Next();
        ASSERT_TRUE(batch_result.ok());
        auto batch = batch_result.value();
        ASSERT_NE(nullptr, batch);

        scanned_batches.push_back(batch);
        total_rows += batch->num_rows();
    }

    // Verify we read all data correctly
    ASSERT_GT(scanned_batches.size(), 0);
    ASSERT_EQ(15, total_rows);  // 3 batches * 5 rows
}

//
// Test Setup:
//      Create a SequentialScanIterator with specific column projections
// Test Result:
//      Should return data with only the projected columns
//
TEST_F(OperatorIteratorsTest, SequentialScanWithProjection) {
    // We'll reuse the "scan_test" table already created in SequentialScanIterator test
    // This table has schema: id (INT32), name (STRING), age (INT32)
    // And contains 3 batches of data (total 15 rows)

    // Test 1: Project only id and name columns
    {
        // Create a projection schema with only id and name
        auto projection_schema = common::CreateSchemaBuilder()
                                     .AddField("id", common::ColumnType::INT32)
                                     .AddField("name", common::ColumnType::STRING)
                                     .Build();

        // Create scan iterator with projection
        auto scan_iterator = std::make_unique<SequentialScanIterator>(data_accessor_,
                                                                      "users",
                                                                      std::shared_ptr<common::Expression>(),
                                                                      std::vector<std::string>{"id", "name"},
                                                                      *projection_schema);

        // Initialize the iterator
        auto init_result = scan_iterator->Initialize();
        VERIFY_RESULT(init_result);
        ASSERT_TRUE(init_result.value());

        // Verify HasNext works
        ASSERT_TRUE(scan_iterator->HasNext());

        // Collect all batches
        std::vector<std::shared_ptr<arrow::RecordBatch>> result_batches;
        int total_rows = 0;

        while (scan_iterator->HasNext()) {
            auto batch_result = scan_iterator->Next();
            ASSERT_TRUE(batch_result.ok());
            auto batch = batch_result.value();
            ASSERT_NE(nullptr, batch);

            // Verify each batch has only the projected columns
            ASSERT_EQ(2, batch->num_columns());
            ASSERT_EQ("id", batch->schema()->field(0)->name());
            ASSERT_EQ("name", batch->schema()->field(1)->name());

            result_batches.push_back(batch);
            total_rows += batch->num_rows();
        }

        // Verify we got all the data
        ASSERT_EQ(15, total_rows);  // 3 batches * 5 rows

        // Verify data in the first batch
        if (!result_batches.empty()) {
            auto first_batch = result_batches[0];
            auto id_array = std::static_pointer_cast<arrow::Int32Array>(first_batch->column(0));
            auto name_array = std::static_pointer_cast<arrow::StringArray>(first_batch->column(1));

            // Check first row of the first batch
            ASSERT_EQ(1, id_array->Value(0));
            ASSERT_EQ("User1", name_array->GetString(0));
        }
    }

    // Test 2: Project only id and age columns
    {
        // Create a projection schema with only id and age
        auto projection_schema = common::CreateSchemaBuilder()
                                     .AddField("id", common::ColumnType::INT32)
                                     .AddField("age", common::ColumnType::INT32)
                                     .Build();

        // Create scan iterator with projection
        auto scan_iterator = std::make_unique<SequentialScanIterator>(data_accessor_,
                                                                      "users",
                                                                      std::shared_ptr<common::Expression>(),
                                                                      std::vector<std::string>{"id", "age"},
                                                                      *projection_schema);

        // Initialize the iterator
        auto init_result = scan_iterator->Initialize();
        ASSERT_TRUE(init_result.ok());
        ASSERT_TRUE(init_result.value());

        // Verify HasNext works
        ASSERT_TRUE(scan_iterator->HasNext());

        // Get the first batch
        auto batch_result = scan_iterator->Next();
        ASSERT_TRUE(batch_result.ok());
        auto batch = batch_result.value();

        // Verify the batch has only the projected columns
        ASSERT_EQ(2, batch->num_columns());
        ASSERT_EQ("id", batch->schema()->field(0)->name());
        ASSERT_EQ("age", batch->schema()->field(1)->name());

        // Verify content of the first batch
        auto id_array = std::static_pointer_cast<arrow::Int32Array>(batch->column(0));
        auto age_array = std::static_pointer_cast<arrow::Int32Array>(batch->column(1));

        // Check data in the first batch
        for (int i = 0; i < batch->num_rows(); i++) {
            // Based on the sample data creation, first batch has ids 1-5 and ages 20-24
            ASSERT_EQ(i + 1, id_array->Value(i));
            ASSERT_EQ(20 + i, age_array->Value(i));
        }
    }
}

//
// Test Setup:
//      Chain multiple iterators together (scan + filter + project)
// Test Result:
//      Should correctly process data through the entire pipeline
//
TEST_F(OperatorIteratorsTest, ChainedIterators) {
    // Set up a separate table with test data
    auto schema = GetSchema("users");

    // Create "chain_test" table
    catalog::PartitionSpec spec;  // empty partition spec
    auto create_result = catalog_->CreateTable("chain_test", schema, spec, "/chain_test");
    VERIFY_RESULT(create_result);

    // Create data ingestor
    auto ingestor_result = catalog::DataIngestor::Create(catalog_, fs_, "chain_test");
    VERIFY_RESULT(ingestor_result);
    auto ingestor = std::move(ingestor_result).value();

    // Create and ingest multiple batches
    for (int i = 0; i < 3; i++) {
        // Use sample batches we already created
        auto ingest_result = ingestor->IngestBatch(sample_batches_[i]);
        VERIFY_RESULT(ingest_result);

        // Commit after each batch to create multiple files
        auto commit_result = ingestor->Commit();
        VERIFY_RESULT(commit_result);
    }

    // Step 1: Create a scan iterator
    auto scan_iterator = std::make_unique<SequentialScanIterator>(
        data_accessor_, "chain_test", std::shared_ptr<common::Expression>(), std::vector<std::string>{}, *schema);

    // Step 2: Create a filter iterator (age >= 30)
    auto age_column = common::MakeColumn("", "age");
    auto thirty = common::MakeIntegerConstant(30);
    auto predicate = common::MakeComparison(common::BinaryOpType::GreaterEqual, age_column, thirty);

    auto filter_iterator = std::make_unique<FilterIterator>(std::move(scan_iterator), predicate, *schema);

    // Step 3: Create a projection iterator (id and name only)
    std::vector<std::shared_ptr<common::Expression>> projections = {common::MakeColumn("", "id"),
                                                                    common::MakeColumn("", "name")};

    auto projection_schema = common::CreateSchemaBuilder()
                                 .AddField("id", common::ColumnType::INT32)
                                 .AddField("name", common::ColumnType::STRING)
                                 .Build();

    auto projection_iterator =
        std::make_unique<ProjectionIterator>(std::move(filter_iterator), projections, *projection_schema);

    // Initialize the iterator chain
    auto init_result = projection_iterator->Initialize();
    ASSERT_TRUE(init_result.ok());

    // Collect and verify the results
    std::vector<std::shared_ptr<arrow::RecordBatch>> result_batches;
    int total_rows = 0;

    while (projection_iterator->HasNext()) {
        auto batch_result = projection_iterator->Next();
        ASSERT_TRUE(batch_result.ok());
        auto batch = batch_result.value();
        ASSERT_NE(nullptr, batch);

        // Verify schema has only id and name
        ASSERT_EQ(2, batch->num_columns());
        auto schema = batch->schema();
        ASSERT_EQ("id", schema->field(0)->name());
        ASSERT_EQ("name", schema->field(1)->name());

        result_batches.push_back(batch);
        total_rows += batch->num_rows();
    }

    // Verify we got the correct results
    // Only batches 1 and 2 should pass the filter (ages 30-34 and 40-44)
    ASSERT_GT(result_batches.size(), 0);
    ASSERT_EQ(10, total_rows);  // 2 batches * 5 rows

    LOG_STATUS("Chain processed %d rows across %zu batches", total_rows, result_batches.size());
}

//
// Test Setup:
//      Test error handling in iterators
// Test Result:
//      Should propagate errors correctly
//
TEST_F(OperatorIteratorsTest, ErrorHandling) {
    // Create a scan iterator for a non-existent table
    auto scan_iterator = std::make_unique<SequentialScanIterator>(data_accessor_,
                                                                  "non_existent_table",
                                                                  std::shared_ptr<common::Expression>(),
                                                                  std::vector<std::string>{},
                                                                  *GetSchema("users"));

    // Initialize should fail
    auto init_result = scan_iterator->Initialize();
    ASSERT_FALSE(init_result.ok());

    // HasNext should be false
    ASSERT_FALSE(scan_iterator->HasNext());

    // Next should return an error
    auto next_result = scan_iterator->Next();
    ASSERT_FALSE(next_result.ok());
}

}  // namespace pond::query