#include "query/data/arrow_util.h"

#include <gtest/gtest.h>

#include "common/schema.h"

namespace pond::query {

class ArrowUtilTest : public ::testing::Test {
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

TEST_F(ArrowUtilTest, CreateEmptyArrayForAllTypes) {
    // Test creating empty arrays for each supported type
    std::vector<common::ColumnType> types = {
        common::ColumnType::INT32,
        common::ColumnType::INT64,
        common::ColumnType::FLOAT,
        common::ColumnType::DOUBLE,
        common::ColumnType::BOOLEAN,
        common::ColumnType::STRING,
        common::ColumnType::TIMESTAMP,
        common::ColumnType::BINARY,
    };

    for (const auto& type : types) {
        auto result = ArrowUtil::CreateEmptyArray(type);
        ASSERT_TRUE(result.ok()) << "Failed to create empty array for type: " << static_cast<int>(type);
        auto array = result.ValueOrDie();
        EXPECT_EQ(array->length(), 0);
        EXPECT_EQ(array->null_count(), 0);
    }
}

TEST_F(ArrowUtilTest, CreateEmptyArrayInvalidType) {
    auto result = ArrowUtil::CreateEmptyArray(common::ColumnType::INVALID);
    EXPECT_FALSE(result.ok());
    EXPECT_TRUE(result.status().IsInvalid());
}

TEST_F(ArrowUtilTest, CreateEmptyBatch) {
    auto result = ArrowUtil::CreateEmptyBatch(schema_);
    ASSERT_TRUE(result.ok());

    auto batch = result.value();
    EXPECT_EQ(batch->num_rows(), 0);
    EXPECT_EQ(batch->num_columns(), schema_.Fields().size());

    // Verify schema matches
    auto arrow_schema = batch->schema();
    EXPECT_EQ(arrow_schema->num_fields(), schema_.Fields().size());

    for (size_t i = 0; i < schema_.Fields().size(); ++i) {
        EXPECT_EQ(arrow_schema->field(i)->name(), schema_.Fields()[i].name);
    }
}

TEST_F(ArrowUtilTest, ApplyPredicateEmptyBatch) {
    // Create an empty batch first
    auto batch_result = ArrowUtil::CreateEmptyBatch(schema_);
    ASSERT_TRUE(batch_result.ok());
    auto batch = batch_result.value();

    // Create a simple predicate (int32_col > 5)
    auto col_expr = common::MakeColumn("", "int32_col");
    auto const_expr = common::MakeIntegerConstant(5);
    auto predicate = common::MakeComparison(common::BinaryOpType::Greater, col_expr, const_expr);

    // Apply predicate
    auto result = ArrowUtil::ApplyPredicate(batch, predicate);
    ASSERT_TRUE(result.ok()) << "Failed with error: " << result.error().message();

    auto filtered_batch = result.value();
    EXPECT_EQ(filtered_batch->num_rows(), 0);
    EXPECT_EQ(filtered_batch->num_columns(), schema_.Fields().size());
}

TEST_F(ArrowUtilTest, ApplyPredicateNullPredicate) {
    // Create an empty batch
    auto batch_result = ArrowUtil::CreateEmptyBatch(schema_);
    ASSERT_TRUE(batch_result.ok());
    auto batch = batch_result.value();

    // Apply null predicate (should return original batch)
    auto result = ArrowUtil::ApplyPredicate(batch, nullptr);
    ASSERT_TRUE(result.ok());
    EXPECT_EQ(result.value(), batch);
}

TEST_F(ArrowUtilTest, ApplyPredicateInvalidColumn) {
    // Create an empty batch
    auto batch_result = ArrowUtil::CreateEmptyBatch(schema_);
    ASSERT_TRUE(batch_result.ok());
    auto batch = batch_result.value();

    // Create predicate with non-existent column
    auto col_expr = common::MakeColumn("", "non_existent_col");
    auto const_expr = common::MakeIntegerConstant(5);
    auto predicate = common::MakeComparison(common::BinaryOpType::Greater, col_expr, const_expr);

    // Apply predicate
    auto result = ArrowUtil::ApplyPredicate(batch, predicate);
    EXPECT_FALSE(result.ok());
}

TEST_F(ArrowUtilTest, ApplyPredicateUnsupportedExpression) {
    // Create an empty batch
    auto batch_result = ArrowUtil::CreateEmptyBatch(schema_);
    ASSERT_TRUE(batch_result.ok());
    auto batch = batch_result.value();

    // Create an unsupported expression type (e.g., a star expression)
    auto star_expr = common::MakeStar();

    // Apply predicate
    auto result = ArrowUtil::ApplyPredicate(batch, star_expr);
    EXPECT_FALSE(result.ok());
}

}  // namespace pond::query