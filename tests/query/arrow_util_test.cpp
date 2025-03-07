#include "query/data/arrow_util.h"

#include <arrow/api.h>
#include <arrow/testing/gtest_util.h>
#include <gtest/gtest.h>

#include "common/result.h"
#include "common/schema.h"

namespace pond::query {

using namespace pond::common;

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

TEST_F(ArrowUtilTest, ConcatenateBatchesEmpty) {
    // Test with empty batch vector
    std::vector<std::shared_ptr<arrow::RecordBatch>> empty_batches;
    auto result = ArrowUtil::ConcatenateBatches(empty_batches);

    ASSERT_TRUE(result.ok());
    ASSERT_NE(result.value(), nullptr);
    ASSERT_EQ(result.value()->num_rows(), 0);
    ASSERT_EQ(result.value()->num_columns(), 0);
}

TEST_F(ArrowUtilTest, ConcatenateBatchesSingle) {
    // Create a test batch
    arrow::Int32Builder id_builder;
    arrow::StringBuilder name_builder;
    arrow::DoubleBuilder value_builder;

    ASSERT_OK(id_builder.AppendValues({1, 2, 3}));
    ASSERT_OK(name_builder.AppendValues({"a", "b", "c"}));
    ASSERT_OK(value_builder.AppendValues({1.1, 2.2, 3.3}));

    auto id_array = id_builder.Finish().ValueOrDie();
    auto name_array = name_builder.Finish().ValueOrDie();
    auto value_array = value_builder.Finish().ValueOrDie();

    auto schema = arrow::schema({arrow::field("id", arrow::int32()),
                                 arrow::field("name", arrow::utf8()),
                                 arrow::field("value", arrow::float64())});

    auto batch = arrow::RecordBatch::Make(schema, 3, {id_array, name_array, value_array});

    // Test with single batch
    std::vector<std::shared_ptr<arrow::RecordBatch>> single_batch = {batch};
    auto result = ArrowUtil::ConcatenateBatches(single_batch);

    ASSERT_TRUE(result.ok());
    ASSERT_EQ(result.value(), batch);  // Should return the same batch without copying
}

TEST_F(ArrowUtilTest, ConcatenateBatchesMultiple) {
    // Create first test batch
    arrow::Int32Builder id_builder1;
    arrow::StringBuilder name_builder1;
    arrow::DoubleBuilder value_builder1;

    ASSERT_OK(id_builder1.AppendValues({1, 2}));
    ASSERT_OK(name_builder1.AppendValues({"a", "b"}));
    ASSERT_OK(value_builder1.AppendValues({1.1, 2.2}));

    auto id_array1 = id_builder1.Finish().ValueOrDie();
    auto name_array1 = name_builder1.Finish().ValueOrDie();
    auto value_array1 = value_builder1.Finish().ValueOrDie();

    // Create second test batch
    arrow::Int32Builder id_builder2;
    arrow::StringBuilder name_builder2;
    arrow::DoubleBuilder value_builder2;

    ASSERT_OK(id_builder2.AppendValues({3, 4, 5}));
    ASSERT_OK(name_builder2.AppendValues({"c", "d", "e"}));
    ASSERT_OK(value_builder2.AppendValues({3.3, 4.4, 5.5}));

    auto id_array2 = id_builder2.Finish().ValueOrDie();
    auto name_array2 = name_builder2.Finish().ValueOrDie();
    auto value_array2 = value_builder2.Finish().ValueOrDie();

    auto schema = arrow::schema({arrow::field("id", arrow::int32()),
                                 arrow::field("name", arrow::utf8()),
                                 arrow::field("value", arrow::float64())});

    auto batch1 = arrow::RecordBatch::Make(schema, 2, {id_array1, name_array1, value_array1});
    auto batch2 = arrow::RecordBatch::Make(schema, 3, {id_array2, name_array2, value_array2});

    // Test with multiple batches
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches = {batch1, batch2};
    auto result = ArrowUtil::ConcatenateBatches(batches);

    ASSERT_TRUE(result.ok());
    ASSERT_NE(result.value(), nullptr);
    ASSERT_EQ(result.value()->num_rows(), 5);  // 2 + 3 = 5 rows total
    ASSERT_EQ(result.value()->num_columns(), 3);

    // Verify the concatenated data
    auto concat_batch = result.value();

    auto id_array = std::static_pointer_cast<arrow::Int32Array>(concat_batch->column(0));
    auto name_array = std::static_pointer_cast<arrow::StringArray>(concat_batch->column(1));
    auto value_array = std::static_pointer_cast<arrow::DoubleArray>(concat_batch->column(2));

    // Check first batch data
    ASSERT_EQ(id_array->Value(0), 1);
    ASSERT_EQ(name_array->GetString(0), "a");
    ASSERT_DOUBLE_EQ(value_array->Value(0), 1.1);

    ASSERT_EQ(id_array->Value(1), 2);
    ASSERT_EQ(name_array->GetString(1), "b");
    ASSERT_DOUBLE_EQ(value_array->Value(1), 2.2);

    // Check second batch data
    ASSERT_EQ(id_array->Value(2), 3);
    ASSERT_EQ(name_array->GetString(2), "c");
    ASSERT_DOUBLE_EQ(value_array->Value(2), 3.3);

    ASSERT_EQ(id_array->Value(3), 4);
    ASSERT_EQ(name_array->GetString(3), "d");
    ASSERT_DOUBLE_EQ(value_array->Value(3), 4.4);

    ASSERT_EQ(id_array->Value(4), 5);
    ASSERT_EQ(name_array->GetString(4), "e");
    ASSERT_DOUBLE_EQ(value_array->Value(4), 5.5);
}

TEST_F(ArrowUtilTest, ConcatenateBatchesDifferentSchema) {
    // Create first test batch
    arrow::Int32Builder id_builder1;
    arrow::StringBuilder name_builder1;

    ASSERT_OK(id_builder1.AppendValues({1, 2}));
    ASSERT_OK(name_builder1.AppendValues({"a", "b"}));

    auto id_array1 = id_builder1.Finish().ValueOrDie();
    auto name_array1 = name_builder1.Finish().ValueOrDie();

    auto schema1 = arrow::schema({arrow::field("id", arrow::int32()), arrow::field("name", arrow::utf8())});

    // Create second test batch with different schema
    arrow::Int32Builder id_builder2;
    arrow::DoubleBuilder value_builder2;  // Different column type

    ASSERT_OK(id_builder2.AppendValues({3, 4}));
    ASSERT_OK(value_builder2.AppendValues({3.3, 4.4}));

    auto id_array2 = id_builder2.Finish().ValueOrDie();
    auto value_array2 = value_builder2.Finish().ValueOrDie();

    auto schema2 = arrow::schema({
        arrow::field("id", arrow::int32()), arrow::field("value", arrow::float64())  // Different column name
    });

    auto batch1 = arrow::RecordBatch::Make(schema1, 2, {id_array1, name_array1});
    auto batch2 = arrow::RecordBatch::Make(schema2, 2, {id_array2, value_array2});

    // Test with batches that have different schemas
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches = {batch1, batch2};
    auto result = ArrowUtil::ConcatenateBatches(batches);

    // Should fail due to schema mismatch
    ASSERT_FALSE(result.ok());
    ASSERT_EQ(result.error().code(), ErrorCode::InvalidArgument);
    ASSERT_NE(result.error().message().find("different schemas"), std::string::npos);
}

}  // namespace pond::query