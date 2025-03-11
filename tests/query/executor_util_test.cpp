#include "query/executor/executor_util.h"

#include <arrow/testing/gtest_util.h>
#include <gtest/gtest.h>

#include "query/data/arrow_util.h"
#include "test_helper.h"

namespace pond::query {

class ExecutorUtilTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create a sample input batch with multiple columns
        std::vector<std::shared_ptr<arrow::Field>> fields = {arrow::field("id", arrow::int32()),
                                                             arrow::field("name", arrow::utf8()),
                                                             arrow::field("age", arrow::int32()),
                                                             arrow::field("salary", arrow::float64())};
        auto schema = arrow::schema(fields);

        // Create arrays for each column
        arrow::Int32Builder id_builder;
        ASSERT_OK(id_builder.AppendValues({1, 2, 3, 4, 5}));
        std::shared_ptr<arrow::Array> id_array;
        ASSERT_OK(id_builder.Finish(&id_array));

        arrow::StringBuilder name_builder;
        ASSERT_OK(name_builder.AppendValues({"Alice", "Bob", "Charlie", "David", "Eve"}));
        std::shared_ptr<arrow::Array> name_array;
        ASSERT_OK(name_builder.Finish(&name_array));

        arrow::Int32Builder age_builder;
        ASSERT_OK(age_builder.AppendValues({25, 30, 35, 40, 45}));
        std::shared_ptr<arrow::Array> age_array;
        ASSERT_OK(age_builder.Finish(&age_array));

        arrow::DoubleBuilder salary_builder;
        ASSERT_OK(salary_builder.AppendValues({50000.0, 60000.0, 70000.0, 80000.0, 90000.0}));
        std::shared_ptr<arrow::Array> salary_array;
        ASSERT_OK(salary_builder.Finish(&salary_array));

        // Create the record batch
        input_batch_ = arrow::RecordBatch::Make(schema, 5, {id_array, name_array, age_array, salary_array});
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
    // Create a new input batch with just the id column for testing
    std::vector<std::shared_ptr<arrow::Field>> fields = {arrow::field("count_id", arrow::int32())};
    auto schema = arrow::schema(fields);

    arrow::Int32Builder id_builder;
    ASSERT_OK(id_builder.AppendValues({1, 2, 3, 4, 5}));
    std::shared_ptr<arrow::Array> id_array;
    ASSERT_OK(id_builder.Finish(&id_array));

    auto test_batch = arrow::RecordBatch::Make(schema, 5, {id_array});

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
    // Create an empty input batch with same schema
    // Create empty arrays using builders
    arrow::Int32Builder id_builder;
    std::shared_ptr<arrow::Array> id_array;
    ASSERT_OK(id_builder.Finish(&id_array));

    arrow::StringBuilder name_builder;
    std::shared_ptr<arrow::Array> name_array;
    ASSERT_OK(name_builder.Finish(&name_array));

    arrow::Int32Builder age_builder;
    std::shared_ptr<arrow::Array> age_array;
    ASSERT_OK(age_builder.Finish(&age_array));

    arrow::DoubleBuilder salary_builder;
    std::shared_ptr<arrow::Array> salary_array;
    ASSERT_OK(salary_builder.Finish(&salary_array));

    auto empty_batch =
        arrow::RecordBatch::Make(input_batch_->schema(), 0, {id_array, name_array, age_array, salary_array});

    std::vector<std::shared_ptr<Expression>> projections = {std::make_shared<ColumnExpression>("", "name"),
                                                            std::make_shared<ColumnExpression>("", "age")};
    common::Schema output_schema(
        {common::ColumnSchema("name", common::ColumnType::STRING, common::Nullability::NULLABLE),
         common::ColumnSchema("age", common::ColumnType::INT32, common::Nullability::NULLABLE)});
    auto projection_node = std::make_shared<PhysicalProjectionNode>(std::move(projections), std::move(output_schema));

    auto result = ExecutorUtil::CreateProjectionBatch(*projection_node, empty_batch);
    VERIFY_RESULT(result);

    auto output_batch = result.value();
    ASSERT_NE(output_batch, nullptr);
    EXPECT_EQ(output_batch->num_rows(), 0);
    EXPECT_EQ(output_batch->num_columns(), 2);
    EXPECT_EQ(output_batch->schema()->field(0)->name(), "name");
    EXPECT_EQ(output_batch->schema()->field(1)->name(), "age");
}

}  // namespace pond::query