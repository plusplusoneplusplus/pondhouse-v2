#include "query/data/arrow_predicate.h"

#include <arrow/api.h>
#include <arrow/builder.h>
#include <arrow/testing/gtest_util.h>
#include <gtest/gtest.h>

#include "common/result.h"
#include "common/schema.h"
#include "query/data/arrow_util.h"
#include "test_helper.h"

namespace pond::query {

using namespace pond::common;

class ArrowPredicateTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create a test schema with different types
        schema_ = common::Schema({
            {"int32_col", common::ColumnType::INT32},
            {"int64_col", common::ColumnType::INT64},
            {"float_col", common::ColumnType::FLOAT},
            {"double_col", common::ColumnType::DOUBLE},
            {"bool_col", common::ColumnType::BOOLEAN},
            {"string_col", common::ColumnType::STRING},
            {"timestamp_col", common::ColumnType::TIMESTAMP},
            {"binary_col", common::ColumnType::BINARY},
        });
    }

    common::Schema schema_;
};

TEST_F(ArrowPredicateTest, ApplyPredicateEmptyBatch) {
    //
    // Test Setup:
    //      Create an empty batch and apply a predicate to it
    // Test Result:
    //      Predicate should apply successfully and return an empty batch
    //

    // Create an empty batch first
    auto batch_result = ArrowUtil::CreateEmptyBatch(schema_);
    ASSERT_TRUE(batch_result.ok());
    auto batch = batch_result.value();

    // Create a simple predicate (int32_col > 5)
    auto col_expr = common::MakeColumnExpression("", "int32_col");
    auto const_expr = common::MakeIntegerConstant(5);
    auto predicate = common::MakeComparisonExpression(common::BinaryOpType::Greater, col_expr, const_expr);

    // Apply predicate directly via ArrowPredicate
    auto result = ArrowPredicate::Apply(batch, predicate);
    ASSERT_TRUE(result.ok()) << "Failed with error: " << result.error().message();

    auto filtered_batch = result.value();
    EXPECT_EQ(filtered_batch->num_rows(), 0);
    EXPECT_EQ(filtered_batch->num_columns(), schema_.Fields().size());
}

TEST_F(ArrowPredicateTest, ApplyPredicateNullPredicate) {
    //
    // Test Setup:
    //      Create a batch and apply a null predicate to it
    // Test Result:
    //      Should return the original batch unchanged
    //

    // Create an empty batch
    auto batch_result = ArrowUtil::CreateEmptyBatch(schema_);
    ASSERT_TRUE(batch_result.ok());
    auto batch = batch_result.value();

    // Apply null predicate (should return original batch)
    auto result = ArrowPredicate::Apply(batch, nullptr);
    ASSERT_TRUE(result.ok());
    EXPECT_EQ(result.value(), batch);
}

TEST_F(ArrowPredicateTest, ApplyPredicateInvalidColumn) {
    //
    // Test Setup:
    //      Create a batch and apply a predicate with non-existent column
    // Test Result:
    //      Should return an error
    //

    // Create an empty batch
    auto batch_result = ArrowUtil::CreateEmptyBatch(schema_);
    ASSERT_TRUE(batch_result.ok());
    auto batch = batch_result.value();

    // Create predicate with non-existent column
    auto col_expr = common::MakeColumnExpression("", "non_existent_col");
    auto const_expr = common::MakeIntegerConstant(5);
    auto predicate = common::MakeComparisonExpression(common::BinaryOpType::Greater, col_expr, const_expr);

    // Apply predicate
    auto result = ArrowPredicate::Apply(batch, predicate);
    EXPECT_FALSE(result.ok());
}

TEST_F(ArrowPredicateTest, ApplyPredicateUnsupportedExpression) {
    //
    // Test Setup:
    //      Create a batch and apply an unsupported expression type
    // Test Result:
    //      Should return an error
    //

    // Create an empty batch
    auto batch_result = ArrowUtil::CreateEmptyBatch(schema_);
    ASSERT_TRUE(batch_result.ok());
    auto batch = batch_result.value();

    // Create an unsupported expression type (e.g., a star expression)
    auto star_expr = common::MakeStarExpression();

    // Apply predicate
    auto result = ArrowPredicate::Apply(batch, star_expr);
    EXPECT_FALSE(result.ok());
}

TEST_F(ArrowPredicateTest, ApplyPredicateViaArrowUtil) {
    //
    // Test Setup:
    //      Verify that the ArrowPredicate class works correctly with different filters
    // Test Result:
    //      Should apply predicates correctly
    //

    // Create an empty batch
    auto batch_result = ArrowUtil::CreateEmptyBatch(schema_);
    ASSERT_TRUE(batch_result.ok());
    auto batch = batch_result.value();

    // Create a simple predicate (int32_col > 5)
    auto col_expr = common::MakeColumnExpression("", "int32_col");
    auto const_expr = common::MakeIntegerConstant(5);
    auto predicate = common::MakeComparisonExpression(common::BinaryOpType::Greater, col_expr, const_expr);

    // Apply predicate directly
    auto result = ArrowPredicate::Apply(batch, predicate);

    ASSERT_TRUE(result.ok());
    EXPECT_EQ(result.value()->num_rows(), 0);
    EXPECT_EQ(result.value()->num_columns(), schema_.Fields().size());
}

TEST_F(ArrowPredicateTest, ApplyPredicateWithData) {
    //
    // Test Setup:
    //      Create a batch with actual data and apply a valid predicate to filter rows
    // Test Result:
    //      Predicate should correctly filter the batch based on the condition
    //

    // Create a test batch with some data
    arrow::Int32Builder int_builder;
    arrow::StringBuilder str_builder;

    ASSERT_OK(int_builder.AppendValues({1, 5, 10, 15, 20}));
    ASSERT_OK(str_builder.AppendValues({"low", "low", "medium", "high", "high"}));

    auto int_array = int_builder.Finish().ValueOrDie();
    auto str_array = str_builder.Finish().ValueOrDie();

    auto schema = arrow::schema({arrow::field("int_val", arrow::int32()), arrow::field("category", arrow::utf8())});

    auto batch = arrow::RecordBatch::Make(schema, 5, {int_array, str_array});

    // Test integer comparison (int_val > 10)
    {
        auto col_expr = common::MakeColumnExpression("", "int_val");
        auto const_expr = common::MakeIntegerConstant(10);
        auto predicate = common::MakeComparisonExpression(common::BinaryOpType::Greater, col_expr, const_expr);

        auto result = ArrowPredicate::Apply(batch, predicate);
        ASSERT_TRUE(result.ok());

        auto filtered = result.value();
        EXPECT_EQ(filtered->num_rows(), 2);  // Should only keep rows with values 15 and 20

        auto filtered_ints = std::static_pointer_cast<arrow::Int32Array>(filtered->column(0));
        EXPECT_EQ(filtered_ints->Value(0), 15);
        EXPECT_EQ(filtered_ints->Value(1), 20);
    }

    // Test string comparison (category = "high")
    {
        auto col_expr = common::MakeColumnExpression("", "category");
        auto const_expr = common::MakeStringConstant("high");
        auto predicate = common::MakeComparisonExpression(common::BinaryOpType::Equal, col_expr, const_expr);

        auto result = ArrowPredicate::Apply(batch, predicate);
        ASSERT_TRUE(result.ok());

        auto filtered = result.value();
        EXPECT_EQ(filtered->num_rows(), 2);  // Should only keep "high" rows

        auto filtered_strs = std::static_pointer_cast<arrow::StringArray>(filtered->column(1));
        EXPECT_EQ(filtered_strs->GetString(0), "high");
        EXPECT_EQ(filtered_strs->GetString(1), "high");
    }
}

}  // namespace pond::query