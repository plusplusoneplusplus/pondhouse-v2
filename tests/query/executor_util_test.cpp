#include "query/executor/executor_util.h"

#include <arrow/testing/gtest_util.h>
#include <gtest/gtest.h>

#include "query/data/arrow_record_batch_builder.h"
#include "query/data/arrow_util.h"
#include "test_helper.h"

namespace pond::query {

class ExecutorUtilTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create a sample input batch with multiple columns using ArrowRecordBatchBuilder
        auto batch_result = ArrowRecordBatchBuilder()
                                .AddInt32Column("id", {1, 2, 3, 4, 5})
                                .AddStringColumn("name", {"Alice", "Bob", "Charlie", "David", "Eve"})
                                .AddInt32Column("age", {25, 30, 35, 40, 45})
                                .AddDoubleColumn("salary", {50000.0, 60000.0, 70000.0, 80000.0, 90000.0})
                                .Build();

        VERIFY_RESULT(batch_result);
        input_batch_ = batch_result.value();
    }

    std::shared_ptr<arrow::RecordBatch> input_batch_;
};

//
// Test Setup:
//      Project single column from input batch
// Test Result:
//      Verify output batch contains only the projected column
//
TEST_F(ExecutorUtilTest, SingleColumnProjection) {
    // Create projection node with single column
    std::vector<std::shared_ptr<Expression>> projections = {std::make_shared<ColumnExpression>("", "name")};
    common::Schema output_schema(
        {common::ColumnSchema("name", common::ColumnType::STRING, common::Nullability::NULLABLE)});
    auto projection_node = std::make_shared<PhysicalProjectionNode>(std::move(projections), std::move(output_schema));

    // Execute projection
    auto result = ExecutorUtil::CreateProjectionBatch(*projection_node, input_batch_);
    VERIFY_RESULT(result);

    auto output_batch = result.value();
    ASSERT_NE(output_batch, nullptr);

    // Verify output schema
    ASSERT_EQ(output_batch->num_columns(), 1);
    ASSERT_EQ(output_batch->schema()->field(0)->name(), "name");

    // Verify data
    auto name_array = std::static_pointer_cast<arrow::StringArray>(output_batch->column(0));
    ASSERT_EQ(name_array->length(), 5);
    EXPECT_EQ(name_array->GetString(0), "Alice");
    EXPECT_EQ(name_array->GetString(4), "Eve");
}

//
// Test Setup:
//      Project multiple columns from input batch
// Test Result:
//      Verify output batch contains all projected columns in correct order
//
TEST_F(ExecutorUtilTest, MultipleColumnProjection) {
    // Create projection node with multiple columns
    std::vector<std::shared_ptr<Expression>> projections = {std::make_shared<ColumnExpression>("", "age"),
                                                            std::make_shared<ColumnExpression>("", "salary"),
                                                            std::make_shared<ColumnExpression>("", "name")};
    common::Schema output_schema(
        {common::ColumnSchema("age", common::ColumnType::INT32, common::Nullability::NULLABLE),
         common::ColumnSchema("salary", common::ColumnType::DOUBLE, common::Nullability::NULLABLE),
         common::ColumnSchema("name", common::ColumnType::STRING, common::Nullability::NULLABLE)});
    auto projection_node = std::make_shared<PhysicalProjectionNode>(std::move(projections), std::move(output_schema));

    // Execute projection
    auto result = ExecutorUtil::CreateProjectionBatch(*projection_node, input_batch_);
    VERIFY_RESULT(result);

    auto output_batch = result.value();
    ASSERT_NE(output_batch, nullptr);

    // Verify output schema
    ASSERT_EQ(output_batch->num_columns(), 3);
    EXPECT_EQ(output_batch->schema()->field(0)->name(), "age");
    EXPECT_EQ(output_batch->schema()->field(1)->name(), "salary");
    EXPECT_EQ(output_batch->schema()->field(2)->name(), "name");

    // Verify data
    auto age_array = std::static_pointer_cast<arrow::Int32Array>(output_batch->column(0));
    auto salary_array = std::static_pointer_cast<arrow::DoubleArray>(output_batch->column(1));
    auto name_array = std::static_pointer_cast<arrow::StringArray>(output_batch->column(2));

    ASSERT_EQ(age_array->length(), 5);
    EXPECT_EQ(age_array->Value(0), 25);
    EXPECT_EQ(salary_array->Value(0), 50000.0);
    EXPECT_EQ(name_array->GetString(0), "Alice");
}

//
// Test Setup:
//      Project non-existent column
// Test Result:
//      Verify error is returned
//
TEST_F(ExecutorUtilTest, NonExistentColumnProjection) {
    std::vector<std::shared_ptr<Expression>> projections = {std::make_shared<ColumnExpression>("", "non_existent")};
    common::Schema output_schema(
        {common::ColumnSchema("non_existent", common::ColumnType::STRING, common::Nullability::NULLABLE)});
    auto projection_node = std::make_shared<PhysicalProjectionNode>(std::move(projections), std::move(output_schema));

    auto result = ExecutorUtil::CreateProjectionBatch(*projection_node, input_batch_);
    EXPECT_FALSE(result.ok());
    EXPECT_EQ(result.error().code(), ErrorCode::InvalidArgument);
    EXPECT_TRUE(result.error().message().find("Column not found") != std::string::npos);
}

//
// Test Setup:
//      Project with aggregate expression
// Test Result:
//      Verify output batch contains aggregate result
//
TEST_F(ExecutorUtilTest, AggregateProjection) {
    // Create a test batch with just the id column
    auto test_batch_result = ArrowRecordBatchBuilder().AddInt32Column("count_id", {1, 2, 3, 4, 5}).Build();

    VERIFY_RESULT(test_batch_result);
    auto test_batch = test_batch_result.value();

    // Create projection node with COUNT aggregate
    auto col_expr = std::make_shared<ColumnExpression>("", "id");
    auto agg_expr = std::make_shared<AggregateExpression>(AggregateType::Count, col_expr);
    std::vector<std::shared_ptr<Expression>> projections = {agg_expr};
    common::Schema output_schema(
        {common::ColumnSchema("count_id", common::ColumnType::UINT64, common::Nullability::NOT_NULL)});
    auto projection_node = std::make_shared<PhysicalProjectionNode>(std::move(projections), std::move(output_schema));

    // Execute projection
    auto result = ExecutorUtil::CreateProjectionBatch(*projection_node, test_batch);
    VERIFY_RESULT(result);

    auto output_batch = result.value();
    ASSERT_NE(output_batch, nullptr);

    // Verify output schema
    ASSERT_EQ(output_batch->num_columns(), 1);
    EXPECT_EQ(output_batch->schema()->field(0)->name(), "count_id");
    EXPECT_EQ(output_batch->schema()->field(0)->type()->id(), arrow::Type::UINT64);
}

//
// Test Setup:
//      Project with empty input batch
// Test Result:
//      Verify empty output batch is created with correct schema
//
TEST_F(ExecutorUtilTest, EmptyBatchProjection) {
    // Create an empty batch with same schema
    auto empty_batch_result = ArrowRecordBatchBuilder()
                                  .AddInt32Column("id", {})
                                  .AddStringColumn("name", {})
                                  .AddInt32Column("age", {})
                                  .AddDoubleColumn("salary", {})
                                  .Build();

    VERIFY_RESULT(empty_batch_result);
    auto empty_batch = empty_batch_result.value();

    // Create projection node
    std::vector<std::shared_ptr<Expression>> projections = {std::make_shared<ColumnExpression>("", "name"),
                                                            std::make_shared<ColumnExpression>("", "age")};
    common::Schema output_schema(
        {common::ColumnSchema("name", common::ColumnType::STRING, common::Nullability::NULLABLE),
         common::ColumnSchema("age", common::ColumnType::INT32, common::Nullability::NULLABLE)});
    auto projection_node = std::make_shared<PhysicalProjectionNode>(std::move(projections), std::move(output_schema));

    // Execute projection
    auto result = ExecutorUtil::CreateProjectionBatch(*projection_node, empty_batch);
    VERIFY_RESULT(result);

    auto output_batch = result.value();
    ASSERT_NE(output_batch, nullptr);
    EXPECT_EQ(output_batch->num_rows(), 0);
    EXPECT_EQ(output_batch->num_columns(), 2);
    EXPECT_EQ(output_batch->schema()->field(0)->name(), "name");
    EXPECT_EQ(output_batch->schema()->field(1)->name(), "age");
}

//
// Test Setup:
//      Sort a single column in ascending order
// Test Result:
//      Verify output batch has rows sorted by the specified column
//
TEST_F(ExecutorUtilTest, SingleColumnSort) {
    // Create sort specs for age column
    std::vector<SortSpec> sort_specs{{std::make_shared<ColumnExpression>("", "age"), SortDirection::Ascending}};

    // Execute sort
    auto result = ExecutorUtil::CreateSortBatch(input_batch_, sort_specs);
    VERIFY_RESULT(result);

    auto output_batch = result.value();
    ASSERT_NE(output_batch, nullptr);

    // Verify output has same schema and number of rows
    ASSERT_EQ(output_batch->num_columns(), input_batch_->num_columns());
    ASSERT_EQ(output_batch->num_rows(), input_batch_->num_rows());

    // Verify data is sorted by age
    auto age_array = std::static_pointer_cast<arrow::Int32Array>(output_batch->column(2));
    for (int i = 1; i < age_array->length(); i++) {
        EXPECT_LE(age_array->Value(i - 1), age_array->Value(i));
    }

    // Verify first and last values
    EXPECT_EQ(age_array->Value(0), 25);  // Youngest
    EXPECT_EQ(age_array->Value(4), 45);  // Oldest
}

//
// Test Setup:
//      Sort on string column (name) and then int column (age)
// Test Result:
//      Verify output batch is sorted by name first, then age
//
TEST_F(ExecutorUtilTest, MultiColumnSortStringThenInt) {
    // Create sort specs for name and age columns
    std::vector<SortSpec> sort_specs{{std::make_shared<ColumnExpression>("", "name"), SortDirection::Ascending},
                                     {std::make_shared<ColumnExpression>("", "age"), SortDirection::Ascending}};

    // Execute sort
    auto result = ExecutorUtil::CreateSortBatch(input_batch_, sort_specs);
    VERIFY_RESULT(result);

    auto output_batch = result.value();
    ASSERT_NE(output_batch, nullptr);

    // Verify output has same schema and number of rows
    ASSERT_EQ(output_batch->num_columns(), input_batch_->num_columns());
    ASSERT_EQ(output_batch->num_rows(), input_batch_->num_rows());

    // Get the sorted arrays
    auto name_array = std::static_pointer_cast<arrow::StringArray>(output_batch->column(1));
    auto age_array = std::static_pointer_cast<arrow::Int32Array>(output_batch->column(2));

    // Verify data is sorted by name first, then age
    for (int i = 1; i < name_array->length(); i++) {
        std::string prev_name = name_array->GetString(i - 1);
        std::string curr_name = name_array->GetString(i);

        // If names are equal, verify ages are in order
        if (prev_name == curr_name) {
            EXPECT_LE(age_array->Value(i - 1), age_array->Value(i));
        } else {
            // Otherwise verify names are in order
            EXPECT_LT(prev_name, curr_name);
        }
    }

    // Verify first and last values
    EXPECT_EQ(name_array->GetString(0), "Alice");
    EXPECT_EQ(age_array->Value(0), 25);
    EXPECT_EQ(name_array->GetString(4), "Eve");
    EXPECT_EQ(age_array->Value(4), 45);
}

//
// Test Setup:
//      Sort with null values
// Test Result:
//      Verify null values are handled correctly in sorting
//
TEST_F(ExecutorUtilTest, SortWithNulls) {
    // Create a batch with null values
    auto batch_result =
        ArrowRecordBatchBuilder()
            .AddInt32Column("id", {1, 2, 3, 4, 5})
            .AddStringColumn("name", {"Alice", "Bob", "", "David", "Eve"}, {true, true, false, true, true})
            .AddInt32Column("age", {25, -1, 35, -1, 45}, {true, false, true, false, true})
            .Build();

    VERIFY_RESULT(batch_result);
    auto batch_with_nulls = batch_result.value();

    // Create sort specs for age and name
    std::vector<SortSpec> sort_specs{{std::make_shared<ColumnExpression>("", "age"), SortDirection::Ascending},
                                     {std::make_shared<ColumnExpression>("", "name"), SortDirection::Ascending}};

    // Execute sort
    auto result = ExecutorUtil::CreateSortBatch(batch_with_nulls, sort_specs);
    VERIFY_RESULT(result);

    auto output_batch = result.value();
    ASSERT_NE(output_batch, nullptr);

    // Verify output has same schema and number of rows
    ASSERT_EQ(output_batch->num_columns(), batch_with_nulls->num_columns());
    ASSERT_EQ(output_batch->num_rows(), batch_with_nulls->num_rows());

    // Verify null handling - nulls should be first in ascending order
    auto sorted_age_array = std::static_pointer_cast<arrow::Int32Array>(output_batch->column(2));
    auto sorted_name_array = std::static_pointer_cast<arrow::StringArray>(output_batch->column(1));

    // First two ages should be null
    EXPECT_TRUE(sorted_age_array->IsNull(0));
    EXPECT_TRUE(sorted_age_array->IsNull(1));

    // Remaining ages should be in ascending order
    for (int i = 3; i < sorted_age_array->length(); i++) {
        if (!sorted_age_array->IsNull(i - 1) && !sorted_age_array->IsNull(i)) {
            EXPECT_LE(sorted_age_array->Value(i - 1), sorted_age_array->Value(i));
        }
    }
}

//
// Test Setup:
//      Sort empty batch
// Test Result:
//      Verify empty batch is handled correctly
//
TEST_F(ExecutorUtilTest, SortEmptyBatch) {
    // Create an empty batch with same schema
    auto empty_batch_result = ArrowRecordBatchBuilder()
                                  .AddInt32Column("id", {})
                                  .AddStringColumn("name", {})
                                  .AddInt32Column("age", {})
                                  .AddDoubleColumn("salary", {})
                                  .Build();

    VERIFY_RESULT(empty_batch_result);
    auto empty_batch = empty_batch_result.value();

    std::vector<SortSpec> sort_specs{{std::make_shared<ColumnExpression>("", "age"), SortDirection::Ascending}};

    // Execute sort
    auto result = ExecutorUtil::CreateSortBatch(empty_batch, sort_specs);
    VERIFY_RESULT(result);

    auto output_batch = result.value();
    ASSERT_NE(output_batch, nullptr);
    EXPECT_EQ(output_batch->num_rows(), 0);
    EXPECT_EQ(output_batch->num_columns(), empty_batch->num_columns());
}

}  // namespace pond::query