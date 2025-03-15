#include "query/data/arrow_util.h"

#include <arrow/api.h>
#include <arrow/builder.h>
#include <arrow/testing/gtest_util.h>
#include <gtest/gtest.h>

#include "common/result.h"
#include "common/schema.h"
#include "test_helper.h"

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
        common::ColumnType::UINT32,
        common::ColumnType::UINT64,
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

TEST_F(ArrowUtilTest, CreateArrayBuilderForAllTypes) {
    // Test creating array builders for each supported type
    std::vector<common::ColumnType> types = {
        common::ColumnType::INT32,
        common::ColumnType::INT64,
        common::ColumnType::UINT32,
        common::ColumnType::UINT64,
        common::ColumnType::FLOAT,
        common::ColumnType::DOUBLE,
        common::ColumnType::BOOLEAN,
        common::ColumnType::STRING,
        common::ColumnType::TIMESTAMP,
        common::ColumnType::BINARY,
    };

    for (const auto& type : types) {
        auto result = ArrowUtil::CreateArrayBuilder(type);
        ASSERT_TRUE(result.ok()) << "Failed to create array builder for type: " << static_cast<int>(type);
        auto builder = result.value();
        EXPECT_NE(builder, nullptr);
    }
}

TEST_F(ArrowUtilTest, CreateArrayBuilderInvalidType) {
    auto result = ArrowUtil::CreateArrayBuilder(common::ColumnType::INVALID);
    EXPECT_FALSE(result.ok());
    EXPECT_EQ(result.error().code(), common::ErrorCode::NotImplemented);
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
    auto col_expr = common::MakeColumnExpression("", "int32_col");
    auto const_expr = common::MakeIntegerConstant(5);
    auto predicate = common::MakeComparisonExpression(common::BinaryOpType::Greater, col_expr, const_expr);

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
    auto col_expr = common::MakeColumnExpression("", "non_existent_col");
    auto const_expr = common::MakeIntegerConstant(5);
    auto predicate = common::MakeComparisonExpression(common::BinaryOpType::Greater, col_expr, const_expr);

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
    auto star_expr = common::MakeStarExpression();

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

//
// Test Setup:
//      Test with empty JSON array
// Test Result:
//      Should return an empty record batch with the correct schema
//
TEST_F(ArrowUtilTest, JsonToRecordBatchEmptyArray) {
    std::string json_str = "[]";
    auto result = ArrowUtil::JsonToRecordBatch(json_str, schema_);
    ASSERT_TRUE(result.ok()) << "Failed with error: " << result.error().message();

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

//
// Test Setup:
//      Test with invalid JSON
// Test Result:
//      Should return an error
//
TEST_F(ArrowUtilTest, JsonToRecordBatchInvalidJson) {
    std::string invalid_json = "{not valid json}";
    auto result = ArrowUtil::JsonToRecordBatch(invalid_json, schema_);
    EXPECT_FALSE(result.ok());
    EXPECT_EQ(result.error().code(), common::ErrorCode::InvalidArgument);
}

//
// Test Setup:
//      Test with JSON that is not an array
// Test Result:
//      Should return an error
//
TEST_F(ArrowUtilTest, JsonToRecordBatchNotAnArray) {
    std::string json_str = R"({"this": "is an object", "not": "an array"})";
    auto result = ArrowUtil::JsonToRecordBatch(json_str, schema_);
    EXPECT_FALSE(result.ok());
    EXPECT_EQ(result.error().code(), common::ErrorCode::InvalidArgument);
}

//
// Test Setup:
//      Test with a single row of valid data
// Test Result:
//      Should correctly convert to a record batch
//
TEST_F(ArrowUtilTest, JsonToRecordBatchSingleRow) {
    std::string json_str = R"([
        {
            "int32_col": 42,
            "int64_col": 9223372036854775807,
            "float_col": 3.14,
            "double_col": 2.71828,
            "bool_col": true,
            "string_col": "hello world",
            "timestamp_col": 1609459200000,
            "binary_col": "binary data"
        }
    ])";

    auto result = ArrowUtil::JsonToRecordBatch(json_str, schema_);
    ASSERT_TRUE(result.ok()) << "Failed with error: " << result.error().message();

    auto batch = result.value();
    EXPECT_EQ(batch->num_rows(), 1);
    EXPECT_EQ(batch->num_columns(), schema_.Fields().size());

    // Verify data for each column
    EXPECT_EQ(std::static_pointer_cast<arrow::Int32Array>(batch->column(0))->Value(0), 42);
    EXPECT_EQ(std::static_pointer_cast<arrow::Int64Array>(batch->column(1))->Value(0), 9223372036854775807);
    EXPECT_FLOAT_EQ(std::static_pointer_cast<arrow::FloatArray>(batch->column(2))->Value(0), 3.14f);
    EXPECT_DOUBLE_EQ(std::static_pointer_cast<arrow::DoubleArray>(batch->column(3))->Value(0), 2.71828);
    EXPECT_EQ(std::static_pointer_cast<arrow::BooleanArray>(batch->column(4))->Value(0), true);
    EXPECT_EQ(std::static_pointer_cast<arrow::StringArray>(batch->column(5))->GetString(0), "hello world");
    EXPECT_EQ(std::static_pointer_cast<arrow::TimestampArray>(batch->column(6))->Value(0), 1609459200000);

    auto binary_array = std::static_pointer_cast<arrow::BinaryArray>(batch->column(7));
    EXPECT_EQ(binary_array->GetView(0), std::string_view("binary data"));
}

//
// Test Setup:
//      Test with multiple rows of data
// Test Result:
//      Should correctly convert to a record batch with multiple rows
//
TEST_F(ArrowUtilTest, JsonToRecordBatchMultipleRows) {
    std::string json_str = R"([
        {
            "int32_col": 1,
            "int64_col": 1000000000000,
            "float_col": 1.1,
            "double_col": 1.01,
            "bool_col": true,
            "string_col": "row 1",
            "timestamp_col": 1609459200000,
            "binary_col": "binary 1"
        },
        {
            "int32_col": 2,
            "int64_col": 2000000000000,
            "float_col": 2.2,
            "double_col": 2.02,
            "bool_col": false,
            "string_col": "row 2",
            "timestamp_col": 1609545600000,
            "binary_col": "binary 2"
        },
        {
            "int32_col": 3,
            "int64_col": 3000000000000,
            "float_col": 3.3,
            "double_col": 3.03,
            "bool_col": true,
            "string_col": "row 3",
            "timestamp_col": 1609632000000,
            "binary_col": "binary 3"
        }
    ])";

    auto result = ArrowUtil::JsonToRecordBatch(json_str, schema_);
    ASSERT_TRUE(result.ok()) << "Failed with error: " << result.error().message();

    auto batch = result.value();
    EXPECT_EQ(batch->num_rows(), 3);
    EXPECT_EQ(batch->num_columns(), schema_.Fields().size());

    // Verify data for a few columns across rows
    auto int32_array = std::static_pointer_cast<arrow::Int32Array>(batch->column(0));
    EXPECT_EQ(int32_array->Value(0), 1);
    EXPECT_EQ(int32_array->Value(1), 2);
    EXPECT_EQ(int32_array->Value(2), 3);

    auto string_array = std::static_pointer_cast<arrow::StringArray>(batch->column(5));
    EXPECT_EQ(string_array->GetString(0), "row 1");
    EXPECT_EQ(string_array->GetString(1), "row 2");
    EXPECT_EQ(string_array->GetString(2), "row 3");

    auto bool_array = std::static_pointer_cast<arrow::BooleanArray>(batch->column(4));
    EXPECT_EQ(bool_array->Value(0), true);
    EXPECT_EQ(bool_array->Value(1), false);
    EXPECT_EQ(bool_array->Value(2), true);
}

//
// Test Setup:
//      Test handling of null values in JSON
// Test Result:
//      Should correctly handle null values for nullable columns and reject for non-nullable
//
TEST_F(ArrowUtilTest, JsonToRecordBatchNullValues) {
    // Create a schema with nullable columns
    common::Schema nullable_schema({{"int32_col", common::ColumnType::INT32, common::Nullability::NULLABLE},
                                    {"string_col", common::ColumnType::STRING, common::Nullability::NULLABLE}});

    std::string json_str = R"([
        {
            "int32_col": 1,
            "string_col": "not null"
        },
        {
            "int32_col": null,
            "string_col": "int is null"
        },
        {
            "int32_col": 3,
            "string_col": null
        },
        {
            "string_col": "int missing"
        },
        {
            "int32_col": 5
        }
    ])";

    auto result = ArrowUtil::JsonToRecordBatch(json_str, nullable_schema);
    ASSERT_TRUE(result.ok()) << "Failed with error: " << result.error().message();

    auto batch = result.value();
    EXPECT_EQ(batch->num_rows(), 5);
    EXPECT_EQ(batch->num_columns(), 2);

    // Verify data and nullity
    auto int32_array = std::static_pointer_cast<arrow::Int32Array>(batch->column(0));
    auto string_array = std::static_pointer_cast<arrow::StringArray>(batch->column(1));

    // Row 0: both values present
    EXPECT_FALSE(int32_array->IsNull(0));
    EXPECT_EQ(int32_array->Value(0), 1);
    EXPECT_FALSE(string_array->IsNull(0));
    EXPECT_EQ(string_array->GetString(0), "not null");

    // Row 1: int32 is null
    EXPECT_TRUE(int32_array->IsNull(1));
    EXPECT_FALSE(string_array->IsNull(1));
    EXPECT_EQ(string_array->GetString(1), "int is null");

    // Row 2: string is null
    EXPECT_FALSE(int32_array->IsNull(2));
    EXPECT_EQ(int32_array->Value(2), 3);
    EXPECT_TRUE(string_array->IsNull(2));

    // Row 3: int32 missing (treated as null)
    EXPECT_TRUE(int32_array->IsNull(3));
    EXPECT_FALSE(string_array->IsNull(3));
    EXPECT_EQ(string_array->GetString(3), "int missing");

    // Row 4: string missing (treated as null)
    EXPECT_FALSE(int32_array->IsNull(4));
    EXPECT_EQ(int32_array->Value(4), 5);
    EXPECT_TRUE(string_array->IsNull(4));
}

//
// Test Setup:
//      Test handling of null values in JSON
// Test Result:
//      Should correctly handle null values for nullable columns and reject for non-nullable
//
TEST_F(ArrowUtilTest, JsonToRecordBatchNonNullableError) {
    // Create a schema with non-nullable columns
    common::Schema non_nullable_schema({{"int32_col", common::ColumnType::INT32, common::Nullability::NOT_NULL},
                                        {"string_col", common::ColumnType::STRING, common::Nullability::NOT_NULL}});

    // JSON with null value for a non-nullable column
    std::string json_str = R"([
        {
            "int32_col": 1,
            "string_col": "not null"
        },
        {
            "int32_col": null,
            "string_col": "this should fail"
        }
    ])";

    auto result = ArrowUtil::JsonToRecordBatch(json_str, non_nullable_schema);
    EXPECT_FALSE(result.ok());
    EXPECT_EQ(result.error().code(), common::ErrorCode::InvalidArgument);
    EXPECT_TRUE(result.error().message().find("Non-nullable column") != std::string::npos);
}

//
// Test Setup:
//      Test handling of type mismatches
// Test Result:
//      Should reject values with incorrect types
//
TEST_F(ArrowUtilTest, JsonToRecordBatchTypeMismatch) {
    // JSON with type mismatches
    std::string json_str = R"([
        {
            "int32_col": "not a number",
            "int64_col": 1000000000000,
            "float_col": 1.1,
            "double_col": 1.01,
            "bool_col": true,
            "string_col": "row 1",
            "timestamp_col": 1609459200000,
            "binary_col": "binary 1"
        }
    ])";

    auto result = ArrowUtil::JsonToRecordBatch(json_str, schema_);
    EXPECT_FALSE(result.ok());
    EXPECT_EQ(result.error().code(), common::ErrorCode::InvalidArgument);
    EXPECT_TRUE(result.error().message().find("not an integer") != std::string::npos);
}

//
// Test Setup:
//      Test AppendGroupValue with different Arrow array types
// Test Result:
//      Values are correctly appended to builders for each type
//
TEST_F(ArrowUtilTest, AppendGroupValue) {
    // Test Int32
    {
        auto builder = std::make_shared<arrow::Int32Builder>();
        std::shared_ptr<arrow::Array> array;
        ASSERT_OK(builder->AppendValues({1, 2, 3}));
        ASSERT_OK(builder->Finish(&array));

        auto output_builder = std::make_shared<arrow::Int32Builder>();
        auto result = ArrowUtil::AppendGroupValue(array, output_builder, 1);
        ASSERT_TRUE(result.ok());

        std::shared_ptr<arrow::Array> output_array;
        ASSERT_OK(output_builder->Finish(&output_array));
        auto typed_array = std::static_pointer_cast<arrow::Int32Array>(output_array);
        ASSERT_EQ(typed_array->Value(0), 2);
    }

    // Test Int64
    {
        auto builder = std::make_shared<arrow::Int64Builder>();
        std::shared_ptr<arrow::Array> array;
        ASSERT_OK(builder->AppendValues({100L, 200L, 300L}));
        ASSERT_OK(builder->Finish(&array));

        auto output_builder = std::make_shared<arrow::Int64Builder>();
        auto result = ArrowUtil::AppendGroupValue(array, output_builder, 1);
        ASSERT_TRUE(result.ok());

        std::shared_ptr<arrow::Array> output_array;
        ASSERT_OK(output_builder->Finish(&output_array));
        auto typed_array = std::static_pointer_cast<arrow::Int64Array>(output_array);
        ASSERT_EQ(typed_array->Value(0), 200L);
    }

    // Test Double
    {
        auto builder = std::make_shared<arrow::DoubleBuilder>();
        std::shared_ptr<arrow::Array> array;
        ASSERT_OK(builder->AppendValues({1.1, 2.2, 3.3}));
        ASSERT_OK(builder->Finish(&array));

        auto output_builder = std::make_shared<arrow::DoubleBuilder>();
        auto result = ArrowUtil::AppendGroupValue(array, output_builder, 1);
        ASSERT_TRUE(result.ok());

        std::shared_ptr<arrow::Array> output_array;
        ASSERT_OK(output_builder->Finish(&output_array));
        auto typed_array = std::static_pointer_cast<arrow::DoubleArray>(output_array);
        ASSERT_DOUBLE_EQ(typed_array->Value(0), 2.2);
    }

    // Test String
    {
        auto builder = std::make_shared<arrow::StringBuilder>();
        std::shared_ptr<arrow::Array> array;
        ASSERT_OK(builder->AppendValues({"a", "b", "c"}));
        ASSERT_OK(builder->Finish(&array));

        auto output_builder = std::make_shared<arrow::StringBuilder>();
        auto result = ArrowUtil::AppendGroupValue(array, output_builder, 1);
        ASSERT_TRUE(result.ok());

        std::shared_ptr<arrow::Array> output_array;
        ASSERT_OK(output_builder->Finish(&output_array));
        auto typed_array = std::static_pointer_cast<arrow::StringArray>(output_array);
        ASSERT_EQ(typed_array->GetString(0), "b");
    }

    // Test with nulls
    {
        auto builder = std::make_shared<arrow::Int32Builder>();
        std::shared_ptr<arrow::Array> array;
        ASSERT_OK(builder->AppendNull());
        ASSERT_OK(builder->Append(2));
        ASSERT_OK(builder->AppendNull());
        ASSERT_OK(builder->Finish(&array));

        auto output_builder = std::make_shared<arrow::Int32Builder>();
        auto result = ArrowUtil::AppendGroupValue(array, output_builder, 1);
        ASSERT_TRUE(result.ok());

        std::shared_ptr<arrow::Array> output_array;
        ASSERT_OK(output_builder->Finish(&output_array));
        auto typed_array = std::static_pointer_cast<arrow::Int32Array>(output_array);
        ASSERT_EQ(typed_array->Value(0), 2);
    }
}

//
// Test Setup:
//      Test ComputeSum with different numeric types and edge cases
// Test Result:
//      Sums are correctly computed for each type
//
TEST_F(ArrowUtilTest, ComputeSum) {
    // Test Int32
    {
        auto builder = std::make_shared<arrow::Int32Builder>();
        std::shared_ptr<arrow::Array> array;
        ASSERT_OK(builder->AppendValues({1, 2, 3, 4, 5}));
        ASSERT_OK(builder->Finish(&array));

        auto output_builder = std::make_shared<arrow::Int32Builder>();
        std::vector<int> indices = {0, 2, 4};  // Sum 1 + 3 + 5 = 9
        auto result = ArrowUtil::ComputeSum(array, indices, output_builder);
        ASSERT_TRUE(result.ok());

        std::shared_ptr<arrow::Array> output_array;
        ASSERT_OK(output_builder->Finish(&output_array));
        auto typed_array = std::static_pointer_cast<arrow::Int32Array>(output_array);
        ASSERT_EQ(typed_array->Value(0), 9);
    }

    // Test Double with nulls
    {
        auto builder = std::make_shared<arrow::DoubleBuilder>();
        std::shared_ptr<arrow::Array> array;
        ASSERT_OK(builder->AppendNull());
        ASSERT_OK(builder->Append(2.5));
        ASSERT_OK(builder->AppendNull());
        ASSERT_OK(builder->Append(7.5));
        ASSERT_OK(builder->Finish(&array));

        auto output_builder = std::make_shared<arrow::DoubleBuilder>();
        std::vector<int> indices = {0, 1, 2, 3};  // Sum null + 2.5 + null + 7.5 = 10.0
        auto result = ArrowUtil::ComputeSum(array, indices, output_builder);
        ASSERT_TRUE(result.ok());

        std::shared_ptr<arrow::Array> output_array;
        ASSERT_OK(output_builder->Finish(&output_array));
        auto typed_array = std::static_pointer_cast<arrow::DoubleArray>(output_array);
        ASSERT_DOUBLE_EQ(typed_array->Value(0), 10.0);
    }

    // Test empty indices
    {
        auto builder = std::make_shared<arrow::Int32Builder>();
        std::shared_ptr<arrow::Array> array;
        ASSERT_OK(builder->AppendValues({1, 2, 3}));
        ASSERT_OK(builder->Finish(&array));

        auto output_builder = std::make_shared<arrow::Int64Builder>();
        std::vector<int> indices;
        auto result = ArrowUtil::ComputeSum(array, indices, output_builder);
        ASSERT_TRUE(result.ok());

        std::shared_ptr<arrow::Array> output_array;
        ASSERT_OK(output_builder->Finish(&output_array));
        ASSERT_EQ(output_array->length(), 1);
        ASSERT_TRUE(output_array->IsNull(0));
    }
}

//
// Test Setup:
//      Test ComputeAverage with different numeric types and edge cases
// Test Result:
//      Averages are correctly computed for each type
//
TEST_F(ArrowUtilTest, ComputeAverage) {
    // Test Int32
    {
        auto builder = std::make_shared<arrow::Int32Builder>();
        std::shared_ptr<arrow::Array> array;
        ASSERT_OK(builder->AppendValues({1, 2, 3, 4, 5}));
        ASSERT_OK(builder->Finish(&array));

        auto output_builder = std::make_shared<arrow::DoubleBuilder>();
        std::vector<int> indices = {0, 2, 4};  // Avg of 1, 3, 5 = 3.0
        auto result = ArrowUtil::ComputeAverage(array, indices, output_builder);
        ASSERT_TRUE(result.ok());

        std::shared_ptr<arrow::Array> output_array;
        ASSERT_OK(output_builder->Finish(&output_array));
        auto typed_array = std::static_pointer_cast<arrow::DoubleArray>(output_array);
        ASSERT_DOUBLE_EQ(typed_array->Value(0), 3.0);
    }

    // Test Double with nulls
    {
        auto builder = std::make_shared<arrow::DoubleBuilder>();
        std::shared_ptr<arrow::Array> array;
        ASSERT_OK(builder->AppendNull());
        ASSERT_OK(builder->Append(2.0));
        ASSERT_OK(builder->AppendNull());
        ASSERT_OK(builder->Append(4.0));
        ASSERT_OK(builder->Finish(&array));

        auto output_builder = std::make_shared<arrow::DoubleBuilder>();
        std::vector<int> indices = {0, 1, 2, 3};  // Avg of null, 2.0, null, 4.0 = 3.0
        auto result = ArrowUtil::ComputeAverage(array, indices, output_builder);
        ASSERT_TRUE(result.ok());

        std::shared_ptr<arrow::Array> output_array;
        ASSERT_OK(output_builder->Finish(&output_array));
        auto typed_array = std::static_pointer_cast<arrow::DoubleArray>(output_array);
        ASSERT_DOUBLE_EQ(typed_array->Value(0), 3.0);
    }

    // Test all nulls
    {
        auto builder = std::make_shared<arrow::Int32Builder>();
        std::shared_ptr<arrow::Array> array;
        ASSERT_OK(builder->AppendNulls(3));
        ASSERT_OK(builder->Finish(&array));

        auto output_builder = std::make_shared<arrow::DoubleBuilder>();
        std::vector<int> indices = {0, 1, 2};
        auto result = ArrowUtil::ComputeAverage(array, indices, output_builder);
        ASSERT_TRUE(result.ok());

        std::shared_ptr<arrow::Array> output_array;
        ASSERT_OK(output_builder->Finish(&output_array));
        ASSERT_EQ(output_array->length(), 1);
        ASSERT_TRUE(output_array->IsNull(0));
    }

    // Test empty indices
    {
        auto builder = std::make_shared<arrow::Int32Builder>();
        std::shared_ptr<arrow::Array> array;
        ASSERT_OK(builder->AppendValues({1, 2, 3}));
        ASSERT_OK(builder->Finish(&array));

        auto output_builder = std::make_shared<arrow::DoubleBuilder>();
        std::vector<int> indices;
        auto result = ArrowUtil::ComputeAverage(array, indices, output_builder);
        ASSERT_TRUE(result.ok());

        std::shared_ptr<arrow::Array> output_array;
        ASSERT_OK(output_builder->Finish(&output_array));
        ASSERT_EQ(output_array->length(), 1);
        ASSERT_TRUE(output_array->IsNull(0));
    }
}

//
// Test Setup:
//      Test ComputeMin with different types and edge cases
// Test Result:
//      Minimum values are correctly computed for each type
//
TEST_F(ArrowUtilTest, ComputeMin) {
    // Test Int32
    {
        auto builder = std::make_shared<arrow::Int32Builder>();
        std::shared_ptr<arrow::Array> array;
        ASSERT_OK(builder->AppendValues({5, 2, 8, 1, 9}));
        ASSERT_OK(builder->Finish(&array));

        auto output_builder = std::make_shared<arrow::Int32Builder>();
        std::vector<int> indices = {0, 2, 4};  // Min of 5, 8, 9 = 5
        auto result = ArrowUtil::ComputeMin(array, indices, output_builder);
        ASSERT_TRUE(result.ok());

        std::shared_ptr<arrow::Array> output_array;
        ASSERT_OK(output_builder->Finish(&output_array));
        auto typed_array = std::static_pointer_cast<arrow::Int32Array>(output_array);
        ASSERT_EQ(typed_array->Value(0), 5);
    }

    // Test Double with nulls
    {
        auto builder = std::make_shared<arrow::DoubleBuilder>();
        std::shared_ptr<arrow::Array> array;
        ASSERT_OK(builder->AppendNull());
        ASSERT_OK(builder->Append(2.5));
        ASSERT_OK(builder->AppendNull());
        ASSERT_OK(builder->Append(1.5));
        ASSERT_OK(builder->Finish(&array));

        auto output_builder = std::make_shared<arrow::DoubleBuilder>();
        std::vector<int> indices = {0, 1, 2, 3};  // Min of null, 2.5, null, 1.5 = 1.5
        auto result = ArrowUtil::ComputeMin(array, indices, output_builder);
        ASSERT_TRUE(result.ok());

        std::shared_ptr<arrow::Array> output_array;
        ASSERT_OK(output_builder->Finish(&output_array));
        auto typed_array = std::static_pointer_cast<arrow::DoubleArray>(output_array);
        ASSERT_DOUBLE_EQ(typed_array->Value(0), 1.5);
    }

    // Test String
    {
        auto builder = std::make_shared<arrow::StringBuilder>();
        std::shared_ptr<arrow::Array> array;
        ASSERT_OK(builder->AppendValues({"zebra", "apple", "banana"}));
        ASSERT_OK(builder->Finish(&array));

        auto output_builder = std::make_shared<arrow::StringBuilder>();
        std::vector<int> indices = {0, 1, 2};  // Min of "zebra", "apple", "banana" = "apple"
        auto result = ArrowUtil::ComputeMin(array, indices, output_builder);
        VERIFY_RESULT(result);

        std::shared_ptr<arrow::Array> output_array;
        ASSERT_OK(output_builder->Finish(&output_array));
        auto typed_array = std::static_pointer_cast<arrow::StringArray>(output_array);
        ASSERT_EQ(typed_array->GetString(0), "apple");
    }

    // Test all nulls
    {
        auto builder = std::make_shared<arrow::Int32Builder>();
        std::shared_ptr<arrow::Array> array;
        ASSERT_OK(builder->AppendNulls(3));
        ASSERT_OK(builder->Finish(&array));

        auto output_builder = std::make_shared<arrow::Int32Builder>();
        std::vector<int> indices = {0, 1, 2};
        auto result = ArrowUtil::ComputeMin(array, indices, output_builder);
        VERIFY_RESULT(result);

        std::shared_ptr<arrow::Array> output_array;
        ASSERT_OK(output_builder->Finish(&output_array));
        ASSERT_EQ(output_array->length(), 1);
        ASSERT_TRUE(output_array->IsNull(0));
    }

    // Test empty indices
    {
        auto builder = std::make_shared<arrow::Int32Builder>();
        std::shared_ptr<arrow::Array> array;
        ASSERT_OK(builder->AppendValues({1, 2, 3}));
        ASSERT_OK(builder->Finish(&array));

        auto output_builder = std::make_shared<arrow::Int32Builder>();
        std::vector<int> indices;
        auto result = ArrowUtil::ComputeMin(array, indices, output_builder);
        VERIFY_RESULT(result);

        std::shared_ptr<arrow::Array> output_array;
        ASSERT_OK(output_builder->Finish(&output_array));
        ASSERT_EQ(output_array->length(), 1);
        ASSERT_TRUE(output_array->IsNull(0));
    }
}

//
// Test Setup:
//      Test ComputeMax with different types and edge cases
// Test Result:
//      Maximum values are correctly computed for each type
//
TEST_F(ArrowUtilTest, ComputeMax) {
    // Test Int32
    {
        auto builder = std::make_shared<arrow::Int32Builder>();
        std::shared_ptr<arrow::Array> array;
        ASSERT_OK(builder->AppendValues({5, 2, 8, 1, 9}));
        ASSERT_OK(builder->Finish(&array));

        auto output_builder = std::make_shared<arrow::Int32Builder>();
        std::vector<int> indices = {0, 2, 4};  // Max of 5, 8, 9 = 9
        auto result = ArrowUtil::ComputeMax(array, indices, output_builder);
        VERIFY_RESULT(result);

        std::shared_ptr<arrow::Array> output_array;
        ASSERT_OK(output_builder->Finish(&output_array));
        auto typed_array = std::static_pointer_cast<arrow::Int32Array>(output_array);
        ASSERT_EQ(typed_array->Value(0), 9);
    }

    // Test Double with nulls
    {
        auto builder = std::make_shared<arrow::DoubleBuilder>();
        std::shared_ptr<arrow::Array> array;
        ASSERT_OK(builder->AppendNull());
        ASSERT_OK(builder->Append(2.5));
        ASSERT_OK(builder->AppendNull());
        ASSERT_OK(builder->Append(3.5));
        ASSERT_OK(builder->Finish(&array));

        auto output_builder = std::make_shared<arrow::DoubleBuilder>();
        std::vector<int> indices = {0, 1, 2, 3};  // Max of null, 2.5, null, 3.5 = 3.5
        auto result = ArrowUtil::ComputeMax(array, indices, output_builder);
        VERIFY_RESULT(result);

        std::shared_ptr<arrow::Array> output_array;
        ASSERT_OK(output_builder->Finish(&output_array));
        auto typed_array = std::static_pointer_cast<arrow::DoubleArray>(output_array);
        ASSERT_DOUBLE_EQ(typed_array->Value(0), 3.5);
    }

    // Test String
    {
        auto builder = std::make_shared<arrow::StringBuilder>();
        std::shared_ptr<arrow::Array> array;
        ASSERT_OK(builder->AppendValues({"zebra", "apple", "banana"}));
        ASSERT_OK(builder->Finish(&array));

        auto output_builder = std::make_shared<arrow::StringBuilder>();
        std::vector<int> indices = {0, 1, 2};  // Max of "zebra", "apple", "banana" = "zebra"
        auto result = ArrowUtil::ComputeMax(array, indices, output_builder);
        VERIFY_RESULT(result);

        std::shared_ptr<arrow::Array> output_array;
        ASSERT_OK(output_builder->Finish(&output_array));
        auto typed_array = std::static_pointer_cast<arrow::StringArray>(output_array);
        ASSERT_EQ(typed_array->GetString(0), "zebra");
    }

    // Test all nulls
    {
        auto builder = std::make_shared<arrow::Int32Builder>();
        std::shared_ptr<arrow::Array> array;
        ASSERT_OK(builder->AppendNulls(3));
        ASSERT_OK(builder->Finish(&array));

        auto output_builder = std::make_shared<arrow::Int32Builder>();
        std::vector<int> indices = {0, 1, 2};
        auto result = ArrowUtil::ComputeMax(array, indices, output_builder);
        VERIFY_RESULT(result);

        std::shared_ptr<arrow::Array> output_array;
        ASSERT_OK(output_builder->Finish(&output_array));
        ASSERT_EQ(output_array->length(), 1);
        ASSERT_TRUE(output_array->IsNull(0));
    }

    // Test empty indices
    {
        auto builder = std::make_shared<arrow::Int32Builder>();
        std::shared_ptr<arrow::Array> array;
        ASSERT_OK(builder->AppendValues({1, 2, 3}));
        ASSERT_OK(builder->Finish(&array));

        auto output_builder = std::make_shared<arrow::Int32Builder>();
        std::vector<int> indices;
        auto result = ArrowUtil::ComputeMax(array, indices, output_builder);
        VERIFY_RESULT(result);

        std::shared_ptr<arrow::Array> output_array;
        ASSERT_OK(output_builder->Finish(&output_array));
        ASSERT_EQ(output_array->length(), 1);
        ASSERT_TRUE(output_array->IsNull(0));
    }
}

//
// Test Setup:
//      Test ComputeCount with different scenarios
// Test Result:
//      Counts are correctly computed for various cases
//
TEST_F(ArrowUtilTest, ComputeCount) {
    // Test regular count
    {
        auto builder = std::make_shared<arrow::Int32Builder>();
        std::shared_ptr<arrow::Array> array;
        ASSERT_OK(builder->AppendValues({1, 2, 3, 4, 5}));
        ASSERT_OK(builder->Finish(&array));

        auto output_builder = std::make_shared<arrow::Int64Builder>();
        std::vector<int> indices = {0, 2, 4};  // Count of 3 elements
        auto result = ArrowUtil::ComputeCount(array, indices, output_builder);
        VERIFY_RESULT(result);

        std::shared_ptr<arrow::Array> output_array;
        ASSERT_OK(output_builder->Finish(&output_array));
        auto typed_array = std::static_pointer_cast<arrow::Int64Array>(output_array);
        ASSERT_EQ(typed_array->Value(0), 3);
    }

    // Test count with nulls
    {
        auto builder = std::make_shared<arrow::Int32Builder>();
        std::shared_ptr<arrow::Array> array;
        ASSERT_OK(builder->AppendNull());
        ASSERT_OK(builder->Append(2));
        ASSERT_OK(builder->AppendNull());
        ASSERT_OK(builder->Append(4));
        ASSERT_OK(builder->Finish(&array));

        auto output_builder = std::make_shared<arrow::Int64Builder>();
        std::vector<int> indices = {0, 1, 2, 3};  // Count should be 4 (including nulls)
        auto result = ArrowUtil::ComputeCount(array, indices, output_builder);
        VERIFY_RESULT(result);

        std::shared_ptr<arrow::Array> output_array;
        ASSERT_OK(output_builder->Finish(&output_array));
        auto typed_array = std::static_pointer_cast<arrow::Int64Array>(output_array);
        ASSERT_EQ(typed_array->Value(0), 4);
    }

    // Test count with all nulls
    {
        auto builder = std::make_shared<arrow::Int32Builder>();
        std::shared_ptr<arrow::Array> array;
        ASSERT_OK(builder->AppendNulls(3));
        ASSERT_OK(builder->Finish(&array));

        auto output_builder = std::make_shared<arrow::Int64Builder>();
        std::vector<int> indices = {0, 1, 2};  // Count should be 3
        auto result = ArrowUtil::ComputeCount(array, indices, output_builder);
        VERIFY_RESULT(result);

        std::shared_ptr<arrow::Array> output_array;
        ASSERT_OK(output_builder->Finish(&output_array));
        auto typed_array = std::static_pointer_cast<arrow::Int64Array>(output_array);
        ASSERT_EQ(typed_array->Value(0), 3);
    }

    // Test count with empty indices
    {
        auto builder = std::make_shared<arrow::Int32Builder>();
        std::shared_ptr<arrow::Array> array;
        ASSERT_OK(builder->AppendValues({1, 2, 3}));
        ASSERT_OK(builder->Finish(&array));

        auto output_builder = std::make_shared<arrow::Int64Builder>();
        std::vector<int> indices;  // Count should be 0
        auto result = ArrowUtil::ComputeCount(array, indices, output_builder);
        VERIFY_RESULT(result);

        std::shared_ptr<arrow::Array> output_array;
        ASSERT_OK(output_builder->Finish(&output_array));
        auto typed_array = std::static_pointer_cast<arrow::Int64Array>(output_array);
        ASSERT_EQ(typed_array->Value(0), 0);
    }

    // Test count with different data types
    {
        // String array
        auto string_builder = std::make_shared<arrow::StringBuilder>();
        std::shared_ptr<arrow::Array> string_array;
        ASSERT_OK(string_builder->AppendValues({"a", "b", "c"}));
        ASSERT_OK(string_builder->Finish(&string_array));

        auto output_builder = std::make_shared<arrow::Int64Builder>();
        std::vector<int> indices = {0, 1, 2};
        auto result = ArrowUtil::ComputeCount(string_array, indices, output_builder);
        VERIFY_RESULT(result);

        std::shared_ptr<arrow::Array> output_array;
        ASSERT_OK(output_builder->Finish(&output_array));
        auto typed_array = std::static_pointer_cast<arrow::Int64Array>(output_array);
        ASSERT_EQ(typed_array->Value(0), 3);
    }

    // Test count with mixed null and non-null values
    {
        auto builder = std::make_shared<arrow::Int32Builder>();
        std::shared_ptr<arrow::Array> array;
        ASSERT_OK(builder->Append(1));
        ASSERT_OK(builder->AppendNull());
        ASSERT_OK(builder->Append(3));
        ASSERT_OK(builder->AppendNull());
        ASSERT_OK(builder->Append(5));
        ASSERT_OK(builder->Finish(&array));

        auto output_builder = std::make_shared<arrow::Int64Builder>();
        std::vector<int> indices = {0, 1, 2, 3, 4};  // Count should be 5
        auto result = ArrowUtil::ComputeCount(array, indices, output_builder);
        VERIFY_RESULT(result);

        std::shared_ptr<arrow::Array> output_array;
        ASSERT_OK(output_builder->Finish(&output_array));
        auto typed_array = std::static_pointer_cast<arrow::Int64Array>(output_array);
        ASSERT_EQ(typed_array->Value(0), 5);
    }
}

//
// Test Setup:
//      Create a record batch with 3 columns (int, string, double)
//      Insert 4 rows with some duplicate values
// Test Result:
//      Single column grouping should return 3 unique keys
//      Multiple column grouping should return 3 unique combinations
//
TEST_F(ArrowUtilTest, ExtractGroupKeysBasic) {
    common::Schema schema({{"int_col", common::ColumnType::INT32, common::Nullability::NULLABLE},
                           {"str_col", common::ColumnType::STRING, common::Nullability::NULLABLE},
                           {"double_col", common::ColumnType::DOUBLE, common::Nullability::NULLABLE}});

    // Create test data
    std::string json = R"([
        {"int_col": 1, "str_col": "a", "double_col": 1.1},
        {"int_col": 1, "str_col": "a", "double_col": 1.1},
        {"int_col": 2, "str_col": "b", "double_col": 2.2},
        {"int_col": null, "str_col": "c", "double_col": 3.3}
    ])";

    auto batch_result = ArrowUtil::JsonToRecordBatch(json, schema);
    ASSERT_TRUE(batch_result.ok());
    auto batch = batch_result.value();

    // Test single column grouping
    {
        auto result = ArrowUtil::ExtractGroupKeys(batch, {"int_col"});
        ASSERT_TRUE(result.ok());
        auto keys = result.value();
        ASSERT_EQ(keys.size(), 3);  // 1, 2, null are unique values
    }

    // Test multiple column grouping
    {
        auto result = ArrowUtil::ExtractGroupKeys(batch, {"int_col", "str_col"});
        ASSERT_TRUE(result.ok());
        auto keys = result.value();
        ASSERT_EQ(keys.size(), 3);  // (1,a), (2,b), (null,c) are unique combinations
    }
}

//
// Test Setup:
//      Create a record batch with all supported data types
//      Insert 2 rows with different values for each column
// Test Result:
//      Grouping by all columns should return 2 unique combinations
//      Verify handling of all supported data types
//
TEST_F(ArrowUtilTest, ExtractGroupKeysAllTypes) {
    common::Schema schema({{"int32_col", common::ColumnType::INT32, common::Nullability::NULLABLE},
                           {"int64_col", common::ColumnType::INT64, common::Nullability::NULLABLE},
                           {"uint32_col", common::ColumnType::UINT32, common::Nullability::NULLABLE},
                           {"uint64_col", common::ColumnType::UINT64, common::Nullability::NULLABLE},
                           {"float_col", common::ColumnType::FLOAT, common::Nullability::NULLABLE},
                           {"double_col", common::ColumnType::DOUBLE, common::Nullability::NULLABLE},
                           {"bool_col", common::ColumnType::BOOLEAN, common::Nullability::NULLABLE},
                           {"string_col", common::ColumnType::STRING, common::Nullability::NULLABLE}});

    // Create test data with all types
    std::string json = R"([
        {
            "int32_col": 1,
            "int64_col": 1000000000000,
            "uint32_col": 1,
            "uint64_col": 1000000000000,
            "float_col": 1.1,
            "double_col": 1.1,
            "bool_col": true,
            "string_col": "a"
        },
        {
            "int32_col": 2,
            "int64_col": 2000000000000,
            "uint32_col": 2,
            "uint64_col": 2000000000000,
            "float_col": 2.2,
            "double_col": 2.2,
            "bool_col": false,
            "string_col": "b"
        }
    ])";

    auto batch_result = ArrowUtil::JsonToRecordBatch(json, schema);
    VERIFY_RESULT(batch_result);
    auto batch = batch_result.value();

    // Test grouping by all columns
    auto result = ArrowUtil::ExtractGroupKeys(
        batch,
        {"int32_col", "int64_col", "uint32_col", "uint64_col", "float_col", "double_col", "bool_col", "string_col"});

    ASSERT_TRUE(result.ok());
    auto keys = result.value();
    ASSERT_EQ(keys.size(), 2);  // Two unique combinations
}

//
// Test Setup:
//      Create a record batch with 2 nullable columns
//      Insert rows with various null combinations
// Test Result:
//      Should identify 4 unique combinations
//      Verify correct handling of null values in grouping
//
TEST_F(ArrowUtilTest, ExtractGroupKeysNullHandling) {
    common::Schema schema({{"int_col", common::ColumnType::INT32, common::Nullability::NULLABLE},
                           {"str_col", common::ColumnType::STRING, common::Nullability::NULLABLE}});

    // Create test data with various null combinations
    std::string json = R"([
        {"int_col": 1, "str_col": "a"},
        {"int_col": null, "str_col": "a"},
        {"int_col": 1, "str_col": null},
        {"int_col": null, "str_col": null},
        {"int_col": null, "str_col": null}
    ])";

    auto batch_result = ArrowUtil::JsonToRecordBatch(json, schema);
    ASSERT_TRUE(batch_result.ok());
    auto batch = batch_result.value();

    auto result = ArrowUtil::ExtractGroupKeys(batch, {"int_col", "str_col"});
    ASSERT_TRUE(result.ok());
    auto keys = result.value();
    ASSERT_EQ(keys.size(), 4);  // (1,a), (null,a), (1,null), (null,null)
}

//
// Test Setup:
//      Create an empty record batch with a single column
// Test Result:
//      Should return an empty vector of group keys
//      Operation should complete successfully
//
TEST_F(ArrowUtilTest, ExtractGroupKeysEmptyBatch) {
    common::Schema schema({{"int_col", common::ColumnType::INT32, common::Nullability::NULLABLE}});

    auto empty_batch_result = ArrowUtil::CreateEmptyBatch(schema);
    ASSERT_TRUE(empty_batch_result.ok());
    auto empty_batch = empty_batch_result.value();

    auto result = ArrowUtil::ExtractGroupKeys(empty_batch, {"int_col"});
    ASSERT_TRUE(result.ok());
    ASSERT_TRUE(result.value().empty());
}

//
// Test Setup:
//      Create various invalid inputs: null batch, empty column list, non-existent column
// Test Result:
//      Should return appropriate error codes for each invalid input
//      Verify error handling for all edge cases
//
TEST_F(ArrowUtilTest, ExtractGroupKeysErrorCases) {
    common::Schema schema({{"int_col", common::ColumnType::INT32, common::Nullability::NULLABLE}});

    auto batch_result = ArrowUtil::JsonToRecordBatch(R"([{"int_col": 1}])", schema);
    ASSERT_TRUE(batch_result.ok());
    auto batch = batch_result.value();

    // Test null batch
    {
        auto result = ArrowUtil::ExtractGroupKeys(nullptr, {"int_col"});
        ASSERT_FALSE(result.ok());
        ASSERT_EQ(result.error().code(), common::ErrorCode::InvalidArgument);
    }

    // Test empty column list
    {
        auto result = ArrowUtil::ExtractGroupKeys(batch, {});
        ASSERT_FALSE(result.ok());
        ASSERT_EQ(result.error().code(), common::ErrorCode::InvalidArgument);
    }

    // Test non-existent column
    {
        auto result = ArrowUtil::ExtractGroupKeys(batch, {"non_existent"});
        ASSERT_FALSE(result.ok());
        ASSERT_EQ(result.error().code(), common::ErrorCode::InvalidArgument);
    }
}

//
// Test Setup:
//      Create group keys with different values and null combinations
//      Test sorting and comparison operations
// Test Result:
//      Null values should sort first
//      Keys should maintain consistent ordering
//      Equality comparison should work correctly
//
TEST_F(ArrowUtilTest, GroupKeyComparison) {
    common::Schema schema({{"int_col", common::ColumnType::INT32, common::Nullability::NULLABLE},
                           {"str_col", common::ColumnType::STRING, common::Nullability::NULLABLE}});

    std::string json = R"([
        {"int_col": 1, "str_col": "a"},
        {"int_col": 2, "str_col": "b"},
        {"int_col": null, "str_col": "c"},
        {"int_col": 1, "str_col": null}
    ])";

    auto batch_result = ArrowUtil::JsonToRecordBatch(json, schema);
    ASSERT_TRUE(batch_result.ok());
    auto batch = batch_result.value();

    auto result = ArrowUtil::ExtractGroupKeys(batch, {"int_col", "str_col"});
    ASSERT_TRUE(result.ok());
    auto keys = result.value();

    // Test ordering
    std::vector<GroupKey> sorted_keys = keys;
    std::sort(sorted_keys.begin(), sorted_keys.end());

    // Verify null ordering (nulls should come first)
    ASSERT_TRUE(sorted_keys[0].ToString().find("null") != std::string::npos);

    // Test equality
    for (size_t i = 0; i < keys.size(); i++) {
        ASSERT_EQ(keys[i], keys[i]);
        for (size_t j = i + 1; j < keys.size(); j++) {
            ASSERT_NE(keys[i], keys[j]);
        }
    }
}

//
// Test Setup:
//      Create group keys with both regular and null values
//      Test string representation generation
// Test Result:
//      Regular values should be properly formatted with separators
//      Null values should be represented as "NULL"
//
TEST_F(ArrowUtilTest, GroupKeyToString) {
    common::Schema schema({{"int_col", common::ColumnType::INT32, common::Nullability::NULLABLE},
                           {"str_col", common::ColumnType::STRING, common::Nullability::NULLABLE}});

    std::string json = R"([
        {"int_col": 1, "str_col": "a"},
        {"int_col": null, "str_col": null}
    ])";

    auto batch_result = ArrowUtil::JsonToRecordBatch(json, schema);
    ASSERT_TRUE(batch_result.ok());
    auto batch = batch_result.value();

    auto result = ArrowUtil::ExtractGroupKeys(batch, {"int_col", "str_col"});
    ASSERT_TRUE(result.ok());
    auto keys = result.value();

    // Verify string representation
    std::set<std::string> expected = {"1,\"a\"", "null,null"};
    std::set<std::string> actual;
    for (const auto& key : keys) {
        actual.insert(key.ToString());
    }
    ASSERT_EQ(actual, expected);
}

//
// Test Setup:
//      Create a record batch with 3 columns and insert 4 rows with some duplicates
// Test Result:
//      When grouping by col1, should get 3 unique groups with correct aggregates
//
TEST_F(ArrowUtilTest, HashAggregateBasic) {
    std::vector<std::shared_ptr<arrow::Array>> arrays;

    auto col1_builder = std::make_shared<arrow::Int32Builder>();
    ASSERT_OK(col1_builder->AppendValues({1, 1, 2, 3}));
    std::shared_ptr<arrow::Array> col1;
    ASSERT_OK(col1_builder->Finish(&col1));

    auto col2_builder = std::make_shared<arrow::DoubleBuilder>();
    ASSERT_OK(col2_builder->AppendValues({10.0, 20.0, 30.0, 40.0}));
    std::shared_ptr<arrow::Array> col2;
    ASSERT_OK(col2_builder->Finish(&col2));

    auto col3_builder = std::make_shared<arrow::StringBuilder>();
    ASSERT_OK(col3_builder->AppendValues({"a", "b", "c", "d"}));
    std::shared_ptr<arrow::Array> col3;
    ASSERT_OK(col3_builder->Finish(&col3));

    arrays.push_back(col1);
    arrays.push_back(col2);
    arrays.push_back(col3);

    auto schema = arrow::schema({arrow::field("col1", arrow::int32()),
                                 arrow::field("col2", arrow::float64()),
                                 arrow::field("col3", arrow::utf8())});

    auto batch = arrow::RecordBatch::Make(schema, 4, arrays);

    // Test grouping by col1 and aggregating col2 with SUM and AVG
    auto result = ArrowUtil::HashAggregate(
        batch, {"col1"}, {"col2", "col2"}, {common::AggregateType::Sum, common::AggregateType::Avg});

    ASSERT_TRUE(result.ok());
    auto agg_batch = result.value();

    // Verify output schema
    auto output_schema = agg_batch->schema();
    ASSERT_EQ(output_schema->num_fields(), 3);
    ASSERT_EQ(output_schema->field(0)->name(), "col1");
    ASSERT_TRUE(output_schema->field(0)->type()->Equals(arrow::int32()));
    ASSERT_EQ(output_schema->field(1)->name(), "col2_sum");
    ASSERT_TRUE(output_schema->field(1)->type()->Equals(arrow::float64()));
    ASSERT_EQ(output_schema->field(2)->name(), "col2_avg");
    ASSERT_TRUE(output_schema->field(2)->type()->Equals(arrow::float64()));

    ASSERT_EQ(agg_batch->num_rows(), 3);  // Should have 3 groups (1, 2, 3)

    // Verify group by column values
    auto group_col = std::static_pointer_cast<arrow::Int32Array>(agg_batch->column(0));
    ASSERT_EQ(group_col->Value(0), 1);
    ASSERT_EQ(group_col->Value(1), 2);
    ASSERT_EQ(group_col->Value(2), 3);

    // Verify sum values
    auto sum_col = std::static_pointer_cast<arrow::DoubleArray>(agg_batch->column(1));
    ASSERT_DOUBLE_EQ(sum_col->Value(0), 30.0);  // 10.0 + 20.0
    ASSERT_DOUBLE_EQ(sum_col->Value(1), 30.0);  // 30.0
    ASSERT_DOUBLE_EQ(sum_col->Value(2), 40.0);  // 40.0

    // Verify average values
    auto avg_col = std::static_pointer_cast<arrow::DoubleArray>(agg_batch->column(2));
    ASSERT_DOUBLE_EQ(avg_col->Value(0), 15.0);  // (10.0 + 20.0) / 2
    ASSERT_DOUBLE_EQ(avg_col->Value(1), 30.0);  // 30.0 / 1
    ASSERT_DOUBLE_EQ(avg_col->Value(2), 40.0);  // 40.0 / 1
}

//
// Test Setup:
//      Create a batch with multiple columns and test grouping by multiple columns
// Test Result:
//      Correct groups and aggregates when grouping by multiple columns
TEST_F(ArrowUtilTest, HashAggregateMultipleGroupBy) {
    std::vector<std::shared_ptr<arrow::Array>> arrays;

    auto col1_builder = std::make_shared<arrow::Int32Builder>();
    ASSERT_OK(col1_builder->AppendValues({1, 1, 1, 1}));
    std::shared_ptr<arrow::Array> col1;
    ASSERT_OK(col1_builder->Finish(&col1));

    auto col2_builder = std::make_shared<arrow::StringBuilder>();
    ASSERT_OK(col2_builder->AppendValues({"a", "a", "b", "b"}));
    std::shared_ptr<arrow::Array> col2;
    ASSERT_OK(col2_builder->Finish(&col2));

    auto col3_builder = std::make_shared<arrow::DoubleBuilder>();
    ASSERT_OK(col3_builder->AppendValues({10.0, 20.0, 30.0, 40.0}));
    std::shared_ptr<arrow::Array> col3;
    ASSERT_OK(col3_builder->Finish(&col3));

    arrays.push_back(col1);
    arrays.push_back(col2);
    arrays.push_back(col3);

    auto schema = arrow::schema({arrow::field("col1", arrow::int32()),
                                 arrow::field("col2", arrow::utf8()),
                                 arrow::field("col3", arrow::float64())});

    auto batch = arrow::RecordBatch::Make(schema, 4, arrays);

    auto result = ArrowUtil::HashAggregate(batch,
                                           {"col1", "col2"},  // Group by both col1 and col2
                                           {"col3"},
                                           {common::AggregateType::Sum});

    ASSERT_TRUE(result.ok());
    auto agg_batch = result.value();
    ASSERT_EQ(agg_batch->num_rows(), 2);  // Should have 2 groups: (1,a) and (1,b)

    // Verify group values
    auto group_col1 = std::static_pointer_cast<arrow::Int32Array>(agg_batch->column(0));
    auto group_col2 = std::static_pointer_cast<arrow::StringArray>(agg_batch->column(1));
    ASSERT_EQ(group_col1->Value(0), 1);
    ASSERT_EQ(group_col1->Value(1), 1);
    ASSERT_EQ(group_col2->GetString(0), "a");
    ASSERT_EQ(group_col2->GetString(1), "b");

    // Verify sum values
    auto sum_col = std::static_pointer_cast<arrow::DoubleArray>(agg_batch->column(2));
    ASSERT_DOUBLE_EQ(sum_col->Value(0), 30.0);  // 10.0 + 20.0
    ASSERT_DOUBLE_EQ(sum_col->Value(1), 70.0);  // 30.0 + 40.0
}

//
// Test Setup:
//      Create a batch with null values and test how aggregates handle them
// Test Result:
//      Nulls should be handled correctly in both grouping and aggregation
//
TEST_F(ArrowUtilTest, HashAggregateNullHandling) {
    std::vector<std::shared_ptr<arrow::Array>> arrays;

    auto col1_builder = std::make_shared<arrow::Int32Builder>();
    ASSERT_OK(col1_builder->AppendValues({1, 1, 2, 2}, {true, true, true, false}));
    std::shared_ptr<arrow::Array> col1;
    ASSERT_OK(col1_builder->Finish(&col1));

    auto col2_builder = std::make_shared<arrow::DoubleBuilder>();
    ASSERT_OK(col2_builder->AppendValues({10.0, 20.0, 30.0, 40.0}, {true, false, true, true}));
    std::shared_ptr<arrow::Array> col2;
    ASSERT_OK(col2_builder->Finish(&col2));

    arrays.push_back(col1);
    arrays.push_back(col2);

    auto schema = arrow::schema({
        arrow::field("col1", arrow::int32(), true),   // Nullable
        arrow::field("col2", arrow::float64(), true)  // Nullable
    });

    auto batch = arrow::RecordBatch::Make(schema, 4, arrays);

    auto result = ArrowUtil::HashAggregate(
        batch,
        {"col1"},
        {"col2", "col2", "col2"},
        {common::AggregateType::Sum, common::AggregateType::Avg, common::AggregateType::Count});

    ASSERT_TRUE(result.ok());
    auto agg_batch = result.value();
    std::cout << ArrowUtil::BatchToString(agg_batch) << std::endl;
    ASSERT_EQ(agg_batch->num_rows(), 3);  // Should have 3 groups: 1, 2, and null

    // Verify group by column values
    auto group_col = std::static_pointer_cast<arrow::Int32Array>(agg_batch->column(0));
    ASSERT_EQ(group_col->Value(0), 0);  // null
    ASSERT_EQ(group_col->Value(1), 1);
    ASSERT_EQ(group_col->Value(2), 2);

    // Verify sum values
    auto sum_col = std::static_pointer_cast<arrow::DoubleArray>(agg_batch->column(1));
    ASSERT_DOUBLE_EQ(sum_col->Value(0), 40.0);  // null
    ASSERT_DOUBLE_EQ(sum_col->Value(1), 10.0);
    ASSERT_DOUBLE_EQ(sum_col->Value(2), 30.0);

    // Verify average values
    auto avg_col = std::static_pointer_cast<arrow::DoubleArray>(agg_batch->column(2));
    ASSERT_DOUBLE_EQ(avg_col->Value(0), 40.0);  // null
    ASSERT_DOUBLE_EQ(avg_col->Value(1), 10.0);
    ASSERT_DOUBLE_EQ(avg_col->Value(2), 30.0);

    // Verify count values
    auto count_col = std::static_pointer_cast<arrow::Int64Array>(agg_batch->column(3));
    ASSERT_EQ(count_col->Value(0), 1);  // null
    ASSERT_EQ(count_col->Value(1), 2);
    ASSERT_EQ(count_col->Value(2), 1);
}

//
// Test Setup:
//      Create a batch with all supported types and test aggregation
// Test Result:
//      All supported types should work correctly for grouping and aggregation
//
TEST_F(ArrowUtilTest, HashAggregateAllTypes) {
    std::vector<std::shared_ptr<arrow::Array>> arrays;

    // Add arrays for all supported types...
    auto int32_builder = std::make_shared<arrow::Int32Builder>();
    ASSERT_OK(int32_builder->AppendValues({1, 2, 1, 2}));
    std::shared_ptr<arrow::Array> int32_array;
    ASSERT_OK(int32_builder->Finish(&int32_array));
    arrays.push_back(int32_array);

    auto int64_builder = std::make_shared<arrow::Int64Builder>();
    ASSERT_OK(int64_builder->AppendValues({100, 200, 300, 400}));
    std::shared_ptr<arrow::Array> int64_array;
    ASSERT_OK(int64_builder->Finish(&int64_array));
    arrays.push_back(int64_array);

    auto double_builder = std::make_shared<arrow::DoubleBuilder>();
    ASSERT_OK(double_builder->AppendValues({1.1, 2.2, 3.3, 4.4}));
    std::shared_ptr<arrow::Array> double_array;
    ASSERT_OK(double_builder->Finish(&double_array));
    arrays.push_back(double_array);

    auto schema = arrow::schema({arrow::field("int32_col", arrow::int32()),
                                 arrow::field("int64_col", arrow::int64()),
                                 arrow::field("double_col", arrow::float64())});

    auto batch = arrow::RecordBatch::Make(schema, 4, arrays);

    // Test grouping by int32 and aggregating other columns
    auto result = ArrowUtil::HashAggregate(
        batch, {"int32_col"}, {"int64_col", "double_col"}, {common::AggregateType::Sum, common::AggregateType::Avg});

    ASSERT_TRUE(result.ok());
    auto agg_batch = result.value();
    ASSERT_EQ(agg_batch->num_rows(), 2);  // Should have 2 groups

    // Verify int64 sum
    auto sum_col = std::static_pointer_cast<arrow::Int64Array>(agg_batch->column(1));
    ASSERT_EQ(sum_col->Value(0), 400);  // 100 + 300
    ASSERT_EQ(sum_col->Value(1), 600);  // 200 + 400

    // Verify double average
    auto avg_col = std::static_pointer_cast<arrow::DoubleArray>(agg_batch->column(2));
    ASSERT_DOUBLE_EQ(avg_col->Value(0), 2.2);  // (1.1 + 3.3) / 2
    ASSERT_DOUBLE_EQ(avg_col->Value(1), 3.3);  // (2.2 + 4.4) / 2
}

//
// Test Setup:
//      Create an empty batch and test aggregation
// Test Result:
//      Should return an empty batch with correct schema
//
TEST_F(ArrowUtilTest, HashAggregateEmptyBatch) {
    auto schema = arrow::schema({arrow::field("col1", arrow::int32()), arrow::field("col2", arrow::float64())});

    std::vector<std::shared_ptr<arrow::Array>> arrays;
    auto col1_builder = std::make_shared<arrow::Int32Builder>();
    std::shared_ptr<arrow::Array> col1;
    ASSERT_OK(col1_builder->Finish(&col1));
    arrays.push_back(col1);

    auto col2_builder = std::make_shared<arrow::DoubleBuilder>();
    std::shared_ptr<arrow::Array> col2;
    ASSERT_OK(col2_builder->Finish(&col2));
    arrays.push_back(col2);

    auto batch = arrow::RecordBatch::Make(schema, 0, arrays);

    auto result = ArrowUtil::HashAggregate(batch, {"col1"}, {"col2"}, {common::AggregateType::Sum});

    ASSERT_TRUE(result.ok());
    auto agg_batch = result.value();
    ASSERT_EQ(agg_batch->num_rows(), 0);
    ASSERT_EQ(agg_batch->num_columns(), 2);
}

//
// Test Setup:
//      Test various error cases
// Test Result:
//      Should return appropriate errors for invalid inputs
//
TEST_F(ArrowUtilTest, HashAggregateErrorCases) {
    auto schema = arrow::schema({arrow::field("col1", arrow::int32()), arrow::field("col2", arrow::float64())});

    std::vector<std::shared_ptr<arrow::Array>> arrays;
    auto col1_builder = std::make_shared<arrow::Int32Builder>();
    ASSERT_OK(col1_builder->AppendValues({1, 2, 3}));
    std::shared_ptr<arrow::Array> col1;
    ASSERT_OK(col1_builder->Finish(&col1));
    arrays.push_back(col1);

    auto col2_builder = std::make_shared<arrow::DoubleBuilder>();
    ASSERT_OK(col2_builder->AppendValues({1.0, 2.0, 3.0}));
    std::shared_ptr<arrow::Array> col2;
    ASSERT_OK(col2_builder->Finish(&col2));
    arrays.push_back(col2);

    auto batch = arrow::RecordBatch::Make(schema, 3, arrays);

    // Test: Null batch
    auto result1 = ArrowUtil::HashAggregate(nullptr, {"col1"}, {"col2"}, {common::AggregateType::Sum});
    ASSERT_FALSE(result1.ok());
    ASSERT_EQ(result1.error().code(), common::ErrorCode::InvalidArgument);

    // Test: Empty group by columns
    auto result2 = ArrowUtil::HashAggregate(batch, {}, {"col2"}, {common::AggregateType::Sum});
    ASSERT_FALSE(result2.ok());
    ASSERT_EQ(result2.error().code(), common::ErrorCode::InvalidArgument);

    // Test: Mismatched agg columns and types
    auto result3 =
        ArrowUtil::HashAggregate(batch, {"col1"}, {"col2"}, {common::AggregateType::Sum, common::AggregateType::Avg}
                                 // More agg types than columns
        );
    ASSERT_FALSE(result3.ok());
    ASSERT_EQ(result3.error().code(), common::ErrorCode::InvalidArgument);

    // Test: Non-existent column
    auto result4 = ArrowUtil::HashAggregate(batch, {"non_existent_col"}, {"col2"}, {common::AggregateType::Sum});
    ASSERT_FALSE(result4.ok());
    ASSERT_EQ(result4.error().code(), common::ErrorCode::InvalidArgument);
}

}  // namespace pond::query