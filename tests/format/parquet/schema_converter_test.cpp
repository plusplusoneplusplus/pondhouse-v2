#include "format/parquet/schema_converter.h"

#include <arrow/type.h>
#include <gtest/gtest.h>

namespace pond::format {

class SchemaConverterTest : public ::testing::Test {
protected:
    std::shared_ptr<common::Schema> CreateTestSchema() {
        return common::CreateSchemaBuilder()
            .AddField("int32_col", common::ColumnType::INT32)
            .AddField("int64_col", common::ColumnType::INT64)
            .AddField("float_col", common::ColumnType::FLOAT)
            .AddField("double_col", common::ColumnType::DOUBLE)
            .AddField("string_col", common::ColumnType::STRING, true)
            .AddField("binary_col", common::ColumnType::BINARY)
            .AddField("bool_col", common::ColumnType::BOOLEAN)
            .AddField("timestamp_col", common::ColumnType::TIMESTAMP)
            .AddField("uuid_col", common::ColumnType::UUID)
            .Build();
    }
};

TEST_F(SchemaConverterTest, TestColumnTypeConversion) {
    // Test conversion from Pond to Arrow
    EXPECT_EQ(SchemaConverter::ToArrowDataType(common::ColumnType::INT32)->id(), arrow::Type::INT32);
    EXPECT_EQ(SchemaConverter::ToArrowDataType(common::ColumnType::INT64)->id(), arrow::Type::INT64);
    EXPECT_EQ(SchemaConverter::ToArrowDataType(common::ColumnType::FLOAT)->id(), arrow::Type::FLOAT);
    EXPECT_EQ(SchemaConverter::ToArrowDataType(common::ColumnType::DOUBLE)->id(), arrow::Type::DOUBLE);
    EXPECT_EQ(SchemaConverter::ToArrowDataType(common::ColumnType::STRING)->id(), arrow::Type::STRING);
    EXPECT_EQ(SchemaConverter::ToArrowDataType(common::ColumnType::BINARY)->id(), arrow::Type::BINARY);
    EXPECT_EQ(SchemaConverter::ToArrowDataType(common::ColumnType::BOOLEAN)->id(), arrow::Type::BOOL);
    EXPECT_EQ(SchemaConverter::ToArrowDataType(common::ColumnType::TIMESTAMP)->id(), arrow::Type::TIMESTAMP);
    EXPECT_EQ(SchemaConverter::ToArrowDataType(common::ColumnType::UUID)->id(), arrow::Type::FIXED_SIZE_BINARY);

    // Test conversion from Arrow to Pond
    EXPECT_EQ(SchemaConverter::FromArrowDataType(arrow::int32()).value(), common::ColumnType::INT32);
    EXPECT_EQ(SchemaConverter::FromArrowDataType(arrow::int64()).value(), common::ColumnType::INT64);
    EXPECT_EQ(SchemaConverter::FromArrowDataType(arrow::float32()).value(), common::ColumnType::FLOAT);
    EXPECT_EQ(SchemaConverter::FromArrowDataType(arrow::float64()).value(), common::ColumnType::DOUBLE);
    EXPECT_EQ(SchemaConverter::FromArrowDataType(arrow::utf8()).value(), common::ColumnType::STRING);
    EXPECT_EQ(SchemaConverter::FromArrowDataType(arrow::binary()).value(), common::ColumnType::BINARY);
    EXPECT_EQ(SchemaConverter::FromArrowDataType(arrow::boolean()).value(), common::ColumnType::BOOLEAN);
    EXPECT_EQ(SchemaConverter::FromArrowDataType(arrow::timestamp(arrow::TimeUnit::MICRO)).value(),
              common::ColumnType::TIMESTAMP);
    EXPECT_EQ(SchemaConverter::FromArrowDataType(arrow::fixed_size_binary(16)).value(), common::ColumnType::UUID);
}

TEST_F(SchemaConverterTest, TestSchemaRoundTrip) {
    auto original_schema = CreateTestSchema();

    // Convert Pond schema to Arrow schema
    auto arrow_schema_result = SchemaConverter::ToArrowSchema(*original_schema);
    ASSERT_TRUE(arrow_schema_result.ok());
    auto arrow_schema = arrow_schema_result.value();

    // Convert Arrow schema back to Pond schema
    auto pond_schema_result = SchemaConverter::FromArrowSchema(arrow_schema);
    ASSERT_TRUE(pond_schema_result.ok());
    auto converted_schema = pond_schema_result.value();

    // Verify schemas are equivalent
    ASSERT_EQ(original_schema->num_columns(), converted_schema->num_columns());

    for (size_t i = 0; i < original_schema->num_columns(); i++) {
        const auto& original_col = original_schema->columns()[i];
        const auto& converted_col = converted_schema->columns()[i];

        EXPECT_EQ(original_col, converted_col);

        EXPECT_EQ(original_col.name, converted_col.name);
        EXPECT_EQ(original_col.type, converted_col.type);
        EXPECT_EQ(original_col.nullability, converted_col.nullability);
    }
}

TEST_F(SchemaConverterTest, TestInvalidArrowType) {
    // Test conversion of unsupported Arrow type
    auto invalid_type = arrow::dictionary(arrow::int8(), arrow::utf8());
    auto result = SchemaConverter::FromArrowDataType(invalid_type);
    EXPECT_FALSE(result.ok());
}

TEST_F(SchemaConverterTest, TestNullability) {
    common::ColumnSchema nullable_col("test", common::ColumnType::INT32, common::Nullability::NULLABLE);
    common::ColumnSchema not_null_col("test", common::ColumnType::INT32, common::Nullability::NOT_NULL);

    auto nullable_field = SchemaConverter::ToArrowField(nullable_col);
    auto not_null_field = SchemaConverter::ToArrowField(not_null_col);

    EXPECT_TRUE(nullable_field->nullable());
    EXPECT_FALSE(not_null_field->nullable());

    auto back_to_nullable = SchemaConverter::FromArrowField(nullable_field);
    auto back_to_not_null = SchemaConverter::FromArrowField(not_null_field);

    ASSERT_TRUE(back_to_nullable.ok());
    ASSERT_TRUE(back_to_not_null.ok());

    EXPECT_EQ(back_to_nullable.value().nullability, common::Nullability::NULLABLE);
    EXPECT_EQ(back_to_not_null.value().nullability, common::Nullability::NOT_NULL);
}

}  // namespace pond::format