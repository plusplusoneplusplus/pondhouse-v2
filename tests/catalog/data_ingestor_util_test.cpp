#include <arrow/compute/api.h>
#include <arrow/util/formatting.h>
#include <gtest/gtest.h>

#include "shared.h"

namespace pond::catalog {

class DataIngestorUtilTest : public DataIngestorTestBase {};

//
// Test Setup:
//      Create table with identity partition on "id" field
//      Create a batch with valid data
// Test Result:
//      Should extract correct partition values using identity transform
//
TEST_F(DataIngestorUtilTest, TestExtractPartitionValuesIdentity) {
    // Create schema with partition field
    auto schema = common::CreateSchemaBuilder()
                      .AddField("id", common::ColumnType::INT32)
                      .AddField("name", common::ColumnType::STRING)
                      .AddField("value", common::ColumnType::DOUBLE)
                      .Build();

    // Create table with partition spec
    PartitionSpec partition_spec;
    partition_spec.spec_id = 1;
    partition_spec.fields.emplace_back(0, 0, "id_part", Transform::IDENTITY);

    TableMetadata metadata;
    metadata.schema = schema;
    metadata.partition_specs.push_back(partition_spec);

    // Create test batch
    auto batch = CreateTestBatch();

    // Extract partition values
    auto result = DataIngestorUtil::ExtractPartitionValues(metadata, batch);
    ASSERT_TRUE(result.ok());

    auto partition_values = result.value();
    ASSERT_EQ(partition_values.size(), 1);
    ASSERT_EQ(partition_values["id_part"], "1");  // First row has id=1
}

//
// Test Setup:
//      Create table with bucket partition on "id" field
//      Create a batch with valid data
// Test Result:
//      Should extract correct partition values using bucket transform
//
TEST_F(DataIngestorUtilTest, TestExtractPartitionValuesBucket) {
    // Create schema with partition field
    auto schema = common::CreateSchemaBuilder()
                      .AddField("id", common::ColumnType::INT32)
                      .AddField("name", common::ColumnType::STRING)
                      .AddField("value", common::ColumnType::DOUBLE)
                      .Build();

    // Create table with partition spec
    PartitionSpec partition_spec;
    partition_spec.spec_id = 1;
    partition_spec.fields.emplace_back(0, 0, "id_bucket", Transform::BUCKET, 10);  // 10 buckets

    TableMetadata metadata;
    metadata.schema = schema;
    metadata.partition_specs.push_back(partition_spec);

    // Create test batch
    auto batch = CreateTestBatch();

    // Extract partition values
    auto result = DataIngestorUtil::ExtractPartitionValues(metadata, batch);
    ASSERT_TRUE(result.ok());

    auto partition_values = result.value();
    ASSERT_EQ(partition_values.size(), 1);
    // The exact bucket value depends on the hash function implementation
    ASSERT_FALSE(partition_values["id_bucket"].empty());
    // Check it's a valid bucket number (0-9)
    int bucket = std::stoi(partition_values["id_bucket"]);
    ASSERT_GE(bucket, 0);
    ASSERT_LT(bucket, 10);
}

//
// Test Setup:
//      Create table with truncate partition on "value" field
//      Create a batch with valid data
// Test Result:
//      Should extract correct partition values using truncate transform
//
TEST_F(DataIngestorUtilTest, TestExtractPartitionValuesTruncate) {
    // Create schema with partition field
    auto schema = common::CreateSchemaBuilder()
                      .AddField("id", common::ColumnType::INT32)
                      .AddField("name", common::ColumnType::STRING)
                      .AddField("value", common::ColumnType::DOUBLE)
                      .Build();

    // Create table with partition spec
    PartitionSpec partition_spec;
    partition_spec.spec_id = 1;
    partition_spec.fields.emplace_back(2, 0, "value_trunc", Transform::TRUNCATE, 1);  // Truncate to nearest 1

    TableMetadata metadata;
    metadata.schema = schema;
    metadata.partition_specs.push_back(partition_spec);

    // Create test batch
    auto batch = CreateTestBatch();  // First row has value=1.1

    // Extract partition values
    auto result = DataIngestorUtil::ExtractPartitionValues(metadata, batch);
    ASSERT_TRUE(result.ok());

    auto partition_values = result.value();
    ASSERT_EQ(partition_values.size(), 1);
    ASSERT_EQ(partition_values["value_trunc"], "1");  // 1.1 truncated to 1
}

//
// Test Setup:
//      Create table with string partition field
//      Create a batch with valid string data
// Test Result:
//      Should extract correct string partition values
//
TEST_F(DataIngestorUtilTest, TestExtractPartitionValuesString) {
    // Create schema with partition field
    auto schema = common::CreateSchemaBuilder()
                      .AddField("id", common::ColumnType::INT32)
                      .AddField("name", common::ColumnType::STRING)
                      .AddField("value", common::ColumnType::DOUBLE)
                      .Build();

    // Create table with partition spec
    PartitionSpec partition_spec;
    partition_spec.spec_id = 1;
    partition_spec.fields.emplace_back(1, 0, "name_part", Transform::IDENTITY);

    TableMetadata metadata;
    metadata.schema = schema;
    metadata.partition_specs.push_back(partition_spec);

    // Create test batch
    auto batch = CreateTestBatch();  // First row has name="one"

    // Extract partition values
    auto result = DataIngestorUtil::ExtractPartitionValues(metadata, batch);
    ASSERT_TRUE(result.ok());

    auto partition_values = result.value();
    ASSERT_EQ(partition_values.size(), 1);
    ASSERT_EQ(partition_values["name_part"], "one");
}

//
// Test Setup:
//      Create table with multiple partition fields
//      Create a batch with valid data
// Test Result:
//      Should extract correct partition values for all partition fields
//
TEST_F(DataIngestorUtilTest, TestExtractPartitionValuesMultipleFields) {
    // Create schema with partition fields
    auto schema = common::CreateSchemaBuilder()
                      .AddField("id", common::ColumnType::INT32)
                      .AddField("name", common::ColumnType::STRING)
                      .AddField("value", common::ColumnType::DOUBLE)
                      .Build();

    // Create table with partition spec for multiple fields
    PartitionSpec partition_spec;
    partition_spec.spec_id = 1;
    partition_spec.fields.emplace_back(0, 0, "id_part", Transform::IDENTITY);
    partition_spec.fields.emplace_back(1, 1, "name_part", Transform::IDENTITY);

    TableMetadata metadata;
    metadata.schema = schema;
    metadata.partition_specs.push_back(partition_spec);

    // Create test batch
    auto batch = CreateTestBatch();

    // Extract partition values
    auto result = DataIngestorUtil::ExtractPartitionValues(metadata, batch);
    ASSERT_TRUE(result.ok());

    auto partition_values = result.value();
    ASSERT_EQ(partition_values.size(), 2);
    ASSERT_EQ(partition_values["id_part"], "1");
    ASSERT_EQ(partition_values["name_part"], "one");
}

//
// Test Setup:
//      Create table with partition specs but empty batch
// Test Result:
//      Should return empty partition values map
//
TEST_F(DataIngestorUtilTest, TestExtractPartitionValuesEmptyBatch) {
    // Create schema with partition field
    auto schema = common::CreateSchemaBuilder()
                      .AddField("id", common::ColumnType::INT32)
                      .AddField("name", common::ColumnType::STRING)
                      .AddField("value", common::ColumnType::DOUBLE)
                      .Build();

    // Create table with partition spec
    PartitionSpec partition_spec;
    partition_spec.spec_id = 1;
    partition_spec.fields.emplace_back(0, 0, "id_part", Transform::IDENTITY);

    TableMetadata metadata;
    metadata.schema = schema;
    metadata.partition_specs.push_back(partition_spec);

    // Create empty Arrow schema and batch
    auto arrow_schema = format::SchemaConverter::ToArrowSchema(*schema).value();
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    arrow::Int32Builder id_builder;
    arrays.push_back(id_builder.Finish().ValueOrDie());
    arrow::StringBuilder name_builder;
    arrays.push_back(name_builder.Finish().ValueOrDie());
    arrow::DoubleBuilder value_builder;
    arrays.push_back(value_builder.Finish().ValueOrDie());
    auto empty_batch = arrow::RecordBatch::Make(arrow_schema, 0, arrays);

    // Extract partition values
    auto result = DataIngestorUtil::ExtractPartitionValues(metadata, empty_batch);
    ASSERT_TRUE(result.ok());

    auto partition_values = result.value();
    ASSERT_TRUE(partition_values.empty());
}

//
// Test Setup:
//      Create table without partition specs
// Test Result:
//      Should return empty partition values map
//
TEST_F(DataIngestorUtilTest, TestExtractPartitionValuesNoPartitionSpecs) {
    // Create schema without partition field
    auto schema = common::CreateSchemaBuilder()
                      .AddField("id", common::ColumnType::INT32)
                      .AddField("name", common::ColumnType::STRING)
                      .AddField("value", common::ColumnType::DOUBLE)
                      .Build();

    // Create table without partition spec
    TableMetadata metadata;
    metadata.schema = schema;
    // No partition_specs added

    // Create test batch
    auto batch = CreateTestBatch();

    // Extract partition values
    auto result = DataIngestorUtil::ExtractPartitionValues(metadata, batch);
    ASSERT_TRUE(result.ok());

    auto partition_values = result.value();
    ASSERT_TRUE(partition_values.empty());
}

//
// Test Setup:
//      Create table with invalid partition field (missing transform param)
// Test Result:
//      Should return error for invalid partition field
//
TEST_F(DataIngestorUtilTest, TestExtractPartitionValuesInvalidField) {
    // Create schema with partition field
    auto schema = common::CreateSchemaBuilder()
                      .AddField("id", common::ColumnType::INT32)
                      .AddField("name", common::ColumnType::STRING)
                      .AddField("value", common::ColumnType::DOUBLE)
                      .Build();

    // Create table with invalid partition spec (BUCKET without param)
    PartitionSpec partition_spec;
    partition_spec.spec_id = 1;
    partition_spec.fields.emplace_back(0, 0, "id_bucket", Transform::BUCKET);  // Missing transform param

    TableMetadata metadata;
    metadata.schema = schema;
    metadata.partition_specs.push_back(partition_spec);

    // Create test batch
    auto batch = CreateTestBatch();

    // Extract partition values
    auto result = DataIngestorUtil::ExtractPartitionValues(metadata, batch);
    ASSERT_FALSE(result.ok());
    ASSERT_EQ(result.error().code(), common::ErrorCode::InvalidOperation);
}

//
// Test Setup:
//      Create table with partition on non-existent field
// Test Result:
//      Should return error for missing field
//
TEST_F(DataIngestorUtilTest, TestExtractPartitionValuesFieldNotFound) {
    // Create schema with partition field
    auto schema = common::CreateSchemaBuilder()
                      .AddField("id", common::ColumnType::INT32)
                      .AddField("name", common::ColumnType::STRING)
                      .AddField("value", common::ColumnType::DOUBLE)
                      .Build();

    // Create table with partition spec on non-existent field
    PartitionSpec partition_spec;
    partition_spec.spec_id = 1;
    // Use invalid source_id that doesn't exist in schema
    partition_spec.fields.emplace_back(10, 0, "missing_field", Transform::IDENTITY);

    TableMetadata metadata;
    metadata.schema = schema;
    metadata.partition_specs.push_back(partition_spec);

    // Create test batch
    auto batch = CreateTestBatch();

    // Extract partition values
    auto result = DataIngestorUtil::ExtractPartitionValues(metadata, batch);
    ASSERT_FALSE(result.ok());
    ASSERT_EQ(result.error().code(), common::ErrorCode::SchemaMismatch);
    ASSERT_TRUE(result.error().message().find("Partition field not found in input") != std::string::npos);
}

//
// Test Setup:
//      Create table with partition field that doesn't match input schema
// Test Result:
//      Should return schema mismatch error
//
TEST_F(DataIngestorUtilTest, TestExtractPartitionValuesSchemaMismatch) {
    // Create schema with partition field
    auto schema = common::CreateSchemaBuilder()
                      .AddField("id", common::ColumnType::INT32)
                      .AddField("name", common::ColumnType::STRING)
                      .AddField("value", common::ColumnType::DOUBLE)
                      .Build();

    // Create table with partition spec
    PartitionSpec partition_spec;
    partition_spec.spec_id = 1;
    partition_spec.fields.emplace_back(0, 0, "id_part", Transform::IDENTITY);

    TableMetadata metadata;
    metadata.schema = schema;
    metadata.partition_specs.push_back(partition_spec);

    // Create a batch with different schema
    auto different_schema =
        arrow::schema({arrow::field("different_id", arrow::int32()), arrow::field("different_name", arrow::utf8())});

    arrow::Int32Builder id_builder;
    ASSERT_TRUE(id_builder.AppendValues({1, 2, 3}).ok());
    auto id_array = id_builder.Finish().ValueOrDie();

    arrow::StringBuilder name_builder;
    ASSERT_TRUE(name_builder.AppendValues({"one", "two", "three"}).ok());
    auto name_array = name_builder.Finish().ValueOrDie();

    auto batch = arrow::RecordBatch::Make(different_schema, 3, {id_array, name_array});

    // Extract partition values
    auto result = DataIngestorUtil::ExtractPartitionValues(metadata, batch);
    ASSERT_FALSE(result.ok());
    ASSERT_EQ(result.error().code(), common::ErrorCode::SchemaMismatch);
}

//
// Test Setup:
//      Create table with partition on nullable field with null values
// Test Result:
//      Should return error for null partition field
//
TEST_F(DataIngestorUtilTest, TestExtractPartitionValuesNullField) {
    // Create schema with partition field
    auto schema = common::CreateSchemaBuilder()
                      .AddField("id", common::ColumnType::INT32)
                      .AddField("name", common::ColumnType::STRING)
                      .AddField("value", common::ColumnType::DOUBLE)
                      .Build();

    // Create table with partition spec
    PartitionSpec partition_spec;
    partition_spec.spec_id = 1;
    partition_spec.fields.emplace_back(0, 0, "id_part", Transform::IDENTITY);

    TableMetadata metadata;
    metadata.schema = schema;
    metadata.partition_specs.push_back(partition_spec);

    // Create a batch with null value in partition field
    auto arrow_schema = format::SchemaConverter::ToArrowSchema(*schema).value();

    // Create ID column with null
    arrow::Int32Builder id_builder;
    ASSERT_TRUE(id_builder.AppendNull().ok());
    auto id_array = id_builder.Finish().ValueOrDie();

    // Create other columns
    arrow::StringBuilder name_builder;
    ASSERT_TRUE(name_builder.Append("test").ok());
    auto name_array = name_builder.Finish().ValueOrDie();

    arrow::DoubleBuilder value_builder;
    ASSERT_TRUE(value_builder.Append(1.0).ok());
    auto value_array = value_builder.Finish().ValueOrDie();

    auto batch = arrow::RecordBatch::Make(arrow_schema, 1, {id_array, name_array, value_array});

    // Extract partition values
    auto result = DataIngestorUtil::ExtractPartitionValues(metadata, batch);
    ASSERT_FALSE(result.ok());
    ASSERT_EQ(result.error().code(), common::ErrorCode::InvalidOperation);
    ASSERT_TRUE(result.error().message().find("Partition field cannot be null") != std::string::npos);
}

//
// Test Setup:
//      Create table with unsupported partition field type
// Test Result:
//      Should return not implemented error
//
TEST_F(DataIngestorUtilTest, TestExtractPartitionValuesUnsupportedType) {
    // Create custom schema with binary field
    auto arrow_schema = arrow::schema({arrow::field("id", arrow::int32()),
                                       arrow::field("binary_field", arrow::binary()),
                                       arrow::field("value", arrow::float64())});

    // Create batch with binary data
    arrow::Int32Builder id_builder;
    ASSERT_TRUE(id_builder.Append(1).ok());
    auto id_array = id_builder.Finish().ValueOrDie();

    arrow::BinaryBuilder binary_builder;
    std::string binary_data = "binary data";
    ASSERT_TRUE(binary_builder.Append(binary_data).ok());
    auto binary_array = binary_builder.Finish().ValueOrDie();

    arrow::DoubleBuilder value_builder;
    ASSERT_TRUE(value_builder.Append(1.0).ok());
    auto value_array = value_builder.Finish().ValueOrDie();

    auto batch = arrow::RecordBatch::Make(arrow_schema, 1, {id_array, binary_array, value_array});

    // Create common::Schema equivalent (simplified for test)
    auto schema = common::CreateSchemaBuilder()
                      .AddField("id", common::ColumnType::INT32)
                      .AddField("binary_field", common::ColumnType::BINARY)
                      .AddField("value", common::ColumnType::DOUBLE)
                      .Build();

    // Create partition spec on binary field
    PartitionSpec partition_spec;
    partition_spec.spec_id = 1;
    partition_spec.fields.emplace_back(1, 0, "binary_part", Transform::IDENTITY);

    TableMetadata metadata;
    metadata.schema = schema;
    metadata.partition_specs.push_back(partition_spec);

    // Extract partition values - should fail with unsupported type
    auto result = DataIngestorUtil::ExtractPartitionValues(metadata, batch);
    ASSERT_FALSE(result.ok());
    ASSERT_EQ(result.error().code(), common::ErrorCode::NotImplemented);
}

//
// Test Setup:
//      Attempt to use unsupported transform type
// Test Result:
//      Should return not implemented error
//
TEST_F(DataIngestorUtilTest, TestExtractPartitionValuesUnsupportedTransform) {
    // Create schema with partition field
    auto schema = common::CreateSchemaBuilder()
                      .AddField("id", common::ColumnType::INT32)
                      .AddField("name", common::ColumnType::STRING)
                      .AddField("value", common::ColumnType::DOUBLE)
                      .Build();

    // Create table with invalid transform type (using underlying enum value beyond valid range)
    PartitionSpec partition_spec;
    partition_spec.spec_id = 1;
    // Cast invalid transform value
    Transform invalid_transform = static_cast<Transform>(999);
    partition_spec.fields.emplace_back(0, 0, "id_part", invalid_transform);

    TableMetadata metadata;
    metadata.schema = schema;
    metadata.partition_specs.push_back(partition_spec);

    // Create test batch
    auto batch = CreateTestBatch();

    // Extract partition values
    auto result = DataIngestorUtil::ExtractPartitionValues(metadata, batch);
    ASSERT_FALSE(result.ok());
    ASSERT_EQ(result.error().code(), common::ErrorCode::NotImplemented);
}

//
// Test Setup:
//      Try to apply TRUNCATE transform to a string field
// Test Result:
//      Should return invalid operation error
//
TEST_F(DataIngestorUtilTest, TestExtractPartitionValuesInvalidTransformForType) {
    // Create schema with partition field
    auto schema = common::CreateSchemaBuilder()
                      .AddField("id", common::ColumnType::INT32)
                      .AddField("name", common::ColumnType::STRING)
                      .AddField("value", common::ColumnType::DOUBLE)
                      .Build();

    // Create table with invalid transform for field type (TRUNCATE on string)
    PartitionSpec partition_spec;
    partition_spec.spec_id = 1;
    partition_spec.fields.emplace_back(1, 0, "name_trunc", Transform::TRUNCATE, 10);

    TableMetadata metadata;
    metadata.schema = schema;
    metadata.partition_specs.push_back(partition_spec);

    // Create test batch
    auto batch = CreateTestBatch();

    // Extract partition values
    auto result = DataIngestorUtil::ExtractPartitionValues(metadata, batch);
    ASSERT_FALSE(result.ok());
    ASSERT_EQ(result.error().code(), common::ErrorCode::InvalidOperation);
    ASSERT_TRUE(result.error().message().find("TRUNCATE transform only applies to numeric types") != std::string::npos);
}

//
// Test Setup:
//      Create a record batch with rows that belong to different partitions
//      Apply PartitionRecordBatch to divide it by year
// Test Result:
//      Records should be correctly partitioned into separate batches
//
TEST_F(DataIngestorUtilTest, TestPartitionRecordBatchByYear) {
    // Create schema with a date field
    auto schema = common::CreateSchemaBuilder()
                      .AddField("id", common::ColumnType::INT32)
                      .AddField("date", common::ColumnType::STRING)  // Using string for simplicity
                      .AddField("value", common::ColumnType::DOUBLE)
                      .Build();

    // Create table with year partition spec
    PartitionSpec partition_spec;
    partition_spec.spec_id = 1;
    partition_spec.fields.emplace_back(1, 0, "year", Transform::YEAR);

    TableMetadata metadata;
    metadata.schema = schema;
    metadata.partition_specs.push_back(partition_spec);

    // Create a batch with multiple rows spanning different years
    auto arrow_schema = format::SchemaConverter::ToArrowSchema(*schema).value();

    arrow::Int32Builder id_builder;
    ASSERT_TRUE(id_builder.AppendValues({1, 2, 3, 4}).ok());
    auto id_array = id_builder.Finish().ValueOrDie();

    arrow::StringBuilder date_builder;
    ASSERT_TRUE(date_builder.AppendValues({"2020-01-15", "2020-06-20", "2021-03-10", "2021-11-30"}).ok());
    auto date_array = date_builder.Finish().ValueOrDie();

    arrow::DoubleBuilder value_builder;
    ASSERT_TRUE(value_builder.AppendValues({10.0, 20.0, 30.0, 40.0}).ok());
    auto value_array = value_builder.Finish().ValueOrDie();

    auto batch = arrow::RecordBatch::Make(arrow_schema, 4, {id_array, date_array, value_array});

    // Partition the batch
    auto result = DataIngestorUtil::PartitionRecordBatch(metadata, batch);
    VERIFY_RESULT(result);

    auto partitioned_batches = result.value();
    ASSERT_EQ(partitioned_batches.size(), 2);  // Should have 2 partitions (2020, 2021)

    // Check partition values
    bool found_2020 = false;
    bool found_2021 = false;

    for (const auto& partitioned_batch : partitioned_batches) {
        ASSERT_EQ(partitioned_batch.partition_values.size(), 1);
        ASSERT_TRUE(partitioned_batch.partition_values.find("year") != partitioned_batch.partition_values.end());

        if (partitioned_batch.partition_values.at("year") == "2020") {
            found_2020 = true;
            ASSERT_EQ(partitioned_batch.batch->num_rows(), 2);

            // Check first row
            auto id_arr = std::static_pointer_cast<arrow::Int32Array>(partitioned_batch.batch->column(0));
            auto date_arr = std::static_pointer_cast<arrow::StringArray>(partitioned_batch.batch->column(1));
            auto value_arr = std::static_pointer_cast<arrow::DoubleArray>(partitioned_batch.batch->column(2));

            ASSERT_EQ(id_arr->Value(0), 1);
            ASSERT_EQ(date_arr->GetString(0), "2020-01-15");
            ASSERT_DOUBLE_EQ(value_arr->Value(0), 10.0);

            ASSERT_EQ(id_arr->Value(1), 2);
            ASSERT_EQ(date_arr->GetString(1), "2020-06-20");
            ASSERT_DOUBLE_EQ(value_arr->Value(1), 20.0);
        } else if (partitioned_batch.partition_values.at("year") == "2021") {
            found_2021 = true;
            ASSERT_EQ(partitioned_batch.batch->num_rows(), 2);

            // Check first row
            auto id_arr = std::static_pointer_cast<arrow::Int32Array>(partitioned_batch.batch->column(0));
            auto date_arr = std::static_pointer_cast<arrow::StringArray>(partitioned_batch.batch->column(1));
            auto value_arr = std::static_pointer_cast<arrow::DoubleArray>(partitioned_batch.batch->column(2));

            ASSERT_EQ(id_arr->Value(0), 3);
            ASSERT_EQ(date_arr->GetString(0), "2021-03-10");
            ASSERT_DOUBLE_EQ(value_arr->Value(0), 30.0);

            ASSERT_EQ(id_arr->Value(1), 4);
            ASSERT_EQ(date_arr->GetString(1), "2021-11-30");
            ASSERT_DOUBLE_EQ(value_arr->Value(1), 40.0);
        }
    }

    ASSERT_TRUE(found_2020);
    ASSERT_TRUE(found_2021);
}

//
// Test Setup:
//      Create a record batch with rows that belong to different partitions
//      Apply PartitionRecordBatch with identity and bucket transforms
// Test Result:
//      Records should be correctly partitioned into separate batches
//
TEST_F(DataIngestorUtilTest, TestPartitionRecordBatchMultipleFields) {
    // Create schema
    auto schema = common::CreateSchemaBuilder()
                      .AddField("id", common::ColumnType::INT32)
                      .AddField("region", common::ColumnType::STRING)
                      .AddField("value", common::ColumnType::DOUBLE)
                      .Build();

    // Create table with multiple partition fields
    PartitionSpec partition_spec;
    partition_spec.spec_id = 1;
    partition_spec.fields.emplace_back(1, 0, "region_part", Transform::IDENTITY);  // Region (identity)
    partition_spec.fields.emplace_back(0, 1, "id_mod", Transform::BUCKET, 2);      // ID (bucket mod 2)

    TableMetadata metadata;
    metadata.schema = schema;
    metadata.partition_specs.push_back(partition_spec);

    // Create a batch with multiple rows
    auto arrow_schema = format::SchemaConverter::ToArrowSchema(*schema).value();

    arrow::Int32Builder id_builder;
    ASSERT_TRUE(id_builder.AppendValues({1, 2, 3, 4, 5, 6, 7, 8}).ok());
    auto id_array = id_builder.Finish().ValueOrDie();

    arrow::StringBuilder region_builder;
    ASSERT_TRUE(region_builder.AppendValues({"east", "east", "east", "east", "west", "west", "west", "west"}).ok());
    auto region_array = region_builder.Finish().ValueOrDie();

    arrow::DoubleBuilder value_builder;
    ASSERT_TRUE(value_builder.AppendValues({10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0}).ok());
    auto value_array = value_builder.Finish().ValueOrDie();

    auto batch = arrow::RecordBatch::Make(arrow_schema, 8, {id_array, region_array, value_array});

    // Partition the batch
    auto result = DataIngestorUtil::PartitionRecordBatch(metadata, batch);
    VERIFY_RESULT(result);

    auto partitioned_batches = result.value();
    ASSERT_EQ(partitioned_batches.size(), 4);  // Should have 4 partitions (east/west x even/odd)

    // Map to track which combinations we've found
    std::map<std::pair<std::string, std::string>, bool> found_combinations;

    for (const auto& partitioned_batch : partitioned_batches) {
        ASSERT_EQ(partitioned_batch.partition_values.size(), 2);
        ASSERT_TRUE(partitioned_batch.partition_values.find("region_part") != partitioned_batch.partition_values.end());
        ASSERT_TRUE(partitioned_batch.partition_values.find("id_mod") != partitioned_batch.partition_values.end());

        std::string region = partitioned_batch.partition_values.at("region_part");
        std::string id_mod = partitioned_batch.partition_values.at("id_mod");

        // Mark this combination as found
        found_combinations[{region, id_mod}] = true;

        // Check that all rows in this batch have the correct region
        auto region_arr = std::static_pointer_cast<arrow::StringArray>(partitioned_batch.batch->column(1));
        for (int64_t i = 0; i < partitioned_batch.batch->num_rows(); i++) {
            ASSERT_EQ(region_arr->GetString(i), region);
        }

        // For id_mod (bucket), we need to compute the expected bucket for each ID
        auto id_arr = std::static_pointer_cast<arrow::Int32Array>(partitioned_batch.batch->column(0));
        for (int64_t i = 0; i < partitioned_batch.batch->num_rows(); i++) {
            int32_t id = id_arr->Value(i);
            size_t hash_value = std::hash<int32_t>{}(id);
            size_t expected_bucket = hash_value % 2;
            ASSERT_EQ(std::to_string(expected_bucket), id_mod);
        }
    }

    // Check that we found all expected combinations
    ASSERT_EQ(found_combinations.size(), 4);
}

//
// Test Setup:
//      Create an empty record batch
//      Apply PartitionRecordBatch
// Test Result:
//      Should return a single empty batch with no partition values
//
TEST_F(DataIngestorUtilTest, TestPartitionRecordBatchEmptyBatch) {
    // Create schema
    auto schema = common::CreateSchemaBuilder()
                      .AddField("id", common::ColumnType::INT32)
                      .AddField("name", common::ColumnType::STRING)
                      .AddField("value", common::ColumnType::DOUBLE)
                      .Build();

    // Create table with partition spec
    PartitionSpec partition_spec;
    partition_spec.spec_id = 1;
    partition_spec.fields.emplace_back(0, 0, "id_part", Transform::IDENTITY);

    TableMetadata metadata;
    metadata.schema = schema;
    metadata.partition_specs.push_back(partition_spec);

    // Create empty batch
    auto arrow_schema = format::SchemaConverter::ToArrowSchema(*schema).value();
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    arrow::Int32Builder id_builder;
    arrays.push_back(id_builder.Finish().ValueOrDie());
    arrow::StringBuilder name_builder;
    arrays.push_back(name_builder.Finish().ValueOrDie());
    arrow::DoubleBuilder value_builder;
    arrays.push_back(value_builder.Finish().ValueOrDie());
    auto empty_batch = arrow::RecordBatch::Make(arrow_schema, 0, arrays);

    // Partition the batch
    auto result = DataIngestorUtil::PartitionRecordBatch(metadata, empty_batch);
    ASSERT_TRUE(result.ok());

    auto partitioned_batches = result.value();
    ASSERT_EQ(partitioned_batches.size(), 1);
    ASSERT_EQ(partitioned_batches[0].batch->num_rows(), 0);
    ASSERT_TRUE(partitioned_batches[0].partition_values.empty());
}

//
// Test Setup:
//      Create a record batch with no partition spec in metadata
//      Apply PartitionRecordBatch
// Test Result:
//      Should return original batch with empty partition values
//
TEST_F(DataIngestorUtilTest, TestPartitionRecordBatchNoPartitionSpecs) {
    // Create schema with no partition
    auto schema = common::CreateSchemaBuilder()
                      .AddField("id", common::ColumnType::INT32)
                      .AddField("name", common::ColumnType::STRING)
                      .AddField("value", common::ColumnType::DOUBLE)
                      .Build();

    // Create table without partition spec
    TableMetadata metadata;
    metadata.schema = schema;
    // No partition_specs added

    // Create test batch
    auto batch = CreateTestBatch();

    // Partition the batch
    auto result = DataIngestorUtil::PartitionRecordBatch(metadata, batch);
    ASSERT_TRUE(result.ok());

    auto partitioned_batches = result.value();
    ASSERT_EQ(partitioned_batches.size(), 1);
    ASSERT_EQ(partitioned_batches[0].batch->num_rows(), batch->num_rows());
    ASSERT_TRUE(partitioned_batches[0].partition_values.empty());
}

//
// Test Setup:
//      Create a record batch with invalid partition field
//      Apply PartitionRecordBatch
// Test Result:
//      Should return error
//
TEST_F(DataIngestorUtilTest, TestPartitionRecordBatchInvalidField) {
    // Create schema
    auto schema = common::CreateSchemaBuilder()
                      .AddField("id", common::ColumnType::INT32)
                      .AddField("name", common::ColumnType::STRING)
                      .AddField("value", common::ColumnType::DOUBLE)
                      .Build();

    // Create table with invalid partition spec (BUCKET without param)
    PartitionSpec partition_spec;
    partition_spec.spec_id = 1;
    partition_spec.fields.emplace_back(0, 0, "id_bucket", Transform::BUCKET);  // Missing transform param

    TableMetadata metadata;
    metadata.schema = schema;
    metadata.partition_specs.push_back(partition_spec);

    // Create test batch
    auto batch = CreateTestBatch();

    // Partition the batch
    auto result = DataIngestorUtil::PartitionRecordBatch(metadata, batch);
    ASSERT_FALSE(result.ok());
    ASSERT_EQ(result.error().code(), common::ErrorCode::InvalidArgument);
}

std::shared_ptr<arrow::RecordBatch> BuildTimestampRecordBatch(const std::vector<std::string>& dates) {
    auto timestamp_type = arrow::timestamp(arrow::TimeUnit::MILLI);
    arrow::TimestampBuilder ts_builder(timestamp_type, arrow::default_memory_pool());
    for (const auto& date_str : dates) {
        if (date_str.empty()) {
            auto status = ts_builder.AppendNull();
            if (!status.ok()) {
                throw std::runtime_error("Failed to append null: " + status.ToString());
            }
            continue;
        }

        std::tm tm = {};
        std::istringstream ss(date_str);
        ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
        // Convert to UTC time using gmtime instead of mktime
        std::time_t epoch_seconds = timegm(&tm);  // timegm uses UTC

        // Parse milliseconds from the input string if present
        size_t dot_pos = date_str.find('.');
        int64_t milliseconds = 0;
        if (dot_pos != std::string::npos && date_str.length() > dot_pos + 1) {
            std::string ms_str = date_str.substr(dot_pos + 1);
            milliseconds = std::stol(ms_str);
            // Ensure milliseconds are within 0-999 range
            milliseconds = std::min(999LL, std::max(0LL, milliseconds));
        }

        int64_t epoch_milliseconds = static_cast<int64_t>(epoch_seconds) * 1000 + milliseconds;
        auto status = ts_builder.Append(epoch_milliseconds);
        if (!status.ok()) {
            throw std::runtime_error("Failed to append timestamp: " + status.ToString());
        }
    }

    auto timestamp_array = ts_builder.Finish().ValueOrDie();

    auto batch = arrow::RecordBatch::Make(
        arrow::schema({arrow::field("ts", timestamp_type)}), timestamp_array->length(), {timestamp_array});

    return batch;
}

//
// Test Setup:
//      Test ExtractPartitionValuesVectorized with YEAR transform on timestamp column
// Test Result:
//      Should correctly extract years from timestamp values
//
TEST_F(DataIngestorUtilTest, ExtractPartitionValuesVectorizedYear) {
    // Create schema with timestamp field
    auto schema = std::make_shared<common::Schema>();
    schema->AddField("ts", common::ColumnType::TIMESTAMP);

    // Create partition spec with YEAR transform
    PartitionSpec spec(1);
    spec.fields.emplace_back(0, 100, "year", Transform::YEAR);

    std::vector<std::string> dates = {"2020-01-01 00:00:00",
                                      "2020-06-15 12:30:45.123",
                                      "2021-03-20 15:45:30",
                                      "2021-12-31 23:59:59",
                                      "2022-01-01 00:00:00.000"};

    auto batch = BuildTimestampRecordBatch(dates);

    // Extract partition values
    auto result = DataIngestorUtil::ExtractPartitionValuesVectorized(spec.fields[0], batch->column(0));
    VERIFY_RESULT(result);

    // Verify results
    std::shared_ptr<arrow::StringArray> values = result.value();
    ASSERT_EQ(values->length(), 5);
    ASSERT_EQ(values->GetString(0), "2020");
    ASSERT_EQ(values->GetString(1), "2020");
    ASSERT_EQ(values->GetString(2), "2021");
    ASSERT_EQ(values->GetString(3), "2021");
    ASSERT_EQ(values->GetString(4), "2022");
}

//
// Test Setup:
//      Test ExtractPartitionValuesVectorized with MONTH transform on timestamp column
// Test Result:
//      Should correctly extract months from timestamp values
//
TEST_F(DataIngestorUtilTest, ExtractPartitionValuesVectorizedMonth) {
    // Create schema with timestamp field
    auto schema = std::make_shared<common::Schema>();
    schema->AddField("ts", common::ColumnType::TIMESTAMP);

    // Create partition spec with MONTH transform
    PartitionSpec spec(1);
    spec.fields.emplace_back(0, 100, "month", Transform::MONTH);

    // Create test data with timestamps from different months
    std::vector<std::string> dates = {
        "2023-01-15 10:30:00", "2023-02-20 14:45:00", "2023-03-25 09:15:00", "2023-04-30 16:20:00"};

    auto batch = BuildTimestampRecordBatch(dates);

    // Extract partition values
    auto result = DataIngestorUtil::ExtractPartitionValuesVectorized(spec.fields[0], batch->column(0));
    VERIFY_RESULT(result);

    // Verify results
    std::shared_ptr<arrow::StringArray> values = result.value();
    ASSERT_EQ(values->length(), 4);
    EXPECT_EQ(values->GetString(0), "1");  // January
    EXPECT_EQ(values->GetString(1), "2");  // February
    EXPECT_EQ(values->GetString(2), "3");  // March
    EXPECT_EQ(values->GetString(3), "4");  // April
}

//
// Test Setup:
//      Test ExtractPartitionValuesVectorized with DAY transform on timestamp column
// Test Result:
//      Should correctly extract days from timestamp values
//
TEST_F(DataIngestorUtilTest, ExtractPartitionValuesVectorizedDay) {
    // Create schema with timestamp field
    auto schema = std::make_shared<common::Schema>();
    schema->AddField("ts", common::ColumnType::TIMESTAMP);

    // Create partition spec with DAY transform
    PartitionSpec spec(1);
    spec.fields.emplace_back(0, 100, "day", Transform::DAY);

    std::vector<std::string> dates = {
        "2023-03-01 08:00:00", "2023-03-05 12:30:00", "2023-03-10 15:45:00", "2023-03-15 18:20:00"};

    auto batch = BuildTimestampRecordBatch(dates);

    // Extract partition values
    auto result = DataIngestorUtil::ExtractPartitionValuesVectorized(spec.fields[0], batch->column(0));
    VERIFY_RESULT(result);

    // Verify results
    std::shared_ptr<arrow::StringArray> values = result.value();
    ASSERT_EQ(values->length(), 4);
    EXPECT_EQ(values->GetString(0), "1");   // First day of March
    EXPECT_EQ(values->GetString(1), "5");   // Fifth day of March
    EXPECT_EQ(values->GetString(2), "10");  // Tenth day of March
    EXPECT_EQ(values->GetString(3), "15");  // Fifteenth day of March
}

//
// Test Setup:
//      Test ExtractPartitionValuesVectorized with HOUR transform on timestamp column
// Test Result:
//      Should correctly extract hours from timestamp values
//
TEST_F(DataIngestorUtilTest, ExtractPartitionValuesVectorizedHour) {
    // Create schema with timestamp field
    auto schema = std::make_shared<common::Schema>();
    schema->AddField("ts", common::ColumnType::TIMESTAMP);

    // Create partition spec with HOUR transform
    PartitionSpec spec(1);
    spec.fields.emplace_back(0, 100, "hour", Transform::HOUR);

    std::vector<std::string> dates = {
        "2023-03-15 09:00:00", "2023-03-15 10:30:00", "2023-03-15 11:45:00", "2023-03-15 12:15:00"};

    auto batch = BuildTimestampRecordBatch(dates);

    // Extract partition values
    auto result = DataIngestorUtil::ExtractPartitionValuesVectorized(spec.fields[0], batch->column(0));
    VERIFY_RESULT(result);

    // Verify results
    std::shared_ptr<arrow::StringArray> values = result.value();
    ASSERT_EQ(values->length(), 4);
    EXPECT_EQ(values->GetString(0), "9");   // 9 AM
    EXPECT_EQ(values->GetString(1), "10");  // 10 AM
    EXPECT_EQ(values->GetString(2), "11");  // 11 AM
    EXPECT_EQ(values->GetString(3), "12");  // 12 PM
}

//
// Test Setup:
//      Test ExtractPartitionValuesVectorized with null values in timestamp column
// Test Result:
//      Should handle null values correctly
//
TEST_F(DataIngestorUtilTest, ExtractPartitionValuesVectorizedWithNulls) {
    // Create schema with timestamp field
    auto schema = std::make_shared<common::Schema>();
    schema->AddField("ts", common::ColumnType::TIMESTAMP);

    // Create partition spec with YEAR transform
    PartitionSpec spec(1);
    spec.fields.emplace_back(0, 100, "year", Transform::YEAR);

    // Add two valid timestamps for 2023
    std::vector<std::string> valid_dates = {"",  // null
                                            "",  // null
                                            "2023-01-01 00:00:00",
                                            "2023-12-31 23:59:59"};

    auto batch = BuildTimestampRecordBatch(valid_dates);

    // Extract partition values
    auto result = DataIngestorUtil::ExtractPartitionValuesVectorized(spec.fields[0], batch->column(0));
    VERIFY_RESULT(result);

    // Verify results - should use first non-null value
    std::shared_ptr<arrow::StringArray> values = result.value();
    ASSERT_EQ(values->length(), 4);
    EXPECT_EQ(values->GetString(0), "");
    EXPECT_EQ(values->GetString(1), "");
    EXPECT_EQ(values->GetString(2), "2023");
    EXPECT_EQ(values->GetString(3), "2023");
}

//
// Test Setup:
//      Test ExtractPartitionValuesVectorized with invalid transform type
// Test Result:
//      Should return appropriate error
//
TEST_F(DataIngestorUtilTest, ExtractPartitionValuesVectorizedInvalidTransform) {
    // Create schema with timestamp field
    auto schema = std::make_shared<common::Schema>();
    schema->AddField("ts", common::ColumnType::TIMESTAMP);

    // Create partition spec with invalid transform
    PartitionSpec spec(1);
    spec.fields.emplace_back(0, 100, "invalid", static_cast<Transform>(999));  // Invalid transform

    auto batch = BuildTimestampRecordBatch({"2023-01-01 00:00:00"});

    // Extract partition values - should fail with invalid transform
    auto result = DataIngestorUtil::ExtractPartitionValuesVectorized(spec.fields[0], batch->column(0));
    VERIFY_ERROR_CODE(result, common::ErrorCode::InvalidOperation);
}

//
// Test Setup:
//      Test ExtractPartitionValuesVectorized with BUCKET transform on numeric types
// Test Result:
//      Should correctly bucket numeric values consistently
//
TEST_F(DataIngestorUtilTest, ExtractPartitionValuesVectorizedBucketNumeric) {
    // Create arrays with different numeric types
    arrow::Int32Builder int32_builder;
    EXPECT_TRUE(int32_builder.AppendValues({1, 11, 21, 31, 41}).ok());
    auto int32_array = int32_builder.Finish().ValueOrDie();

    arrow::Int64Builder int64_builder;
    EXPECT_TRUE(int64_builder.AppendValues({100, 200, 300, 400, 500}).ok());
    auto int64_array = int64_builder.Finish().ValueOrDie();

    arrow::DoubleBuilder double_builder;
    EXPECT_TRUE(double_builder.AppendValues({1.5, 2.5, 3.5, 4.5, 5.5}).ok());
    auto double_array = double_builder.Finish().ValueOrDie();

    // Create partition specs with BUCKET transform (3 buckets)
    PartitionSpec spec(1);
    spec.fields.emplace_back(0, 100, "bucket", Transform::BUCKET, 3);

    // Test with Int32 array
    auto result_int32 = DataIngestorUtil::ExtractPartitionValuesVectorized(spec.fields[0], int32_array);
    VERIFY_RESULT(result_int32);
    auto values_int32 = result_int32.value();
    ASSERT_EQ(values_int32->length(), 5);

    // Verify specific bucket values for Int32 array
    std::vector<std::string> expected_int32_buckets = {"1", "2", "0", "1", "2"};
    for (int i = 0; i < values_int32->length(); i++) {
        EXPECT_EQ(values_int32->GetString(i), expected_int32_buckets[i]);
    }

    // Test with Int64 array
    auto result_int64 = DataIngestorUtil::ExtractPartitionValuesVectorized(spec.fields[0], int64_array);
    VERIFY_RESULT(result_int64);
    auto values_int64 = result_int64.value();
    ASSERT_EQ(values_int64->length(), 5);

    // Verify specific bucket values for Int64 array
    std::vector<std::string> expected_int64_buckets = {"1", "2", "0", "1", "2"};
    for (int i = 0; i < values_int64->length(); i++) {
        EXPECT_EQ(values_int64->GetString(i), expected_int64_buckets[i]);
    }

    // Test with Double array
    auto result_double = DataIngestorUtil::ExtractPartitionValuesVectorized(spec.fields[0], double_array);
    VERIFY_RESULT(result_double);
    auto values_double = result_double.value();
    ASSERT_EQ(values_double->length(), 5);

    // Verify specific bucket values for Double array
    std::vector<std::string> expected_double_buckets = {"1", "2", "0", "1", "2"};
    for (int i = 0; i < values_double->length(); i++) {
        EXPECT_EQ(values_double->GetString(i), expected_double_buckets[i]);
    }
}

//
// Test Setup:
//      Test ExtractPartitionValuesVectorized with TRUNCATE transform
// Test Result:
//      Should correctly truncate numeric and string values
//
TEST_F(DataIngestorUtilTest, ExtractPartitionValuesVectorizedTruncate) {
    // Test numeric truncation
    arrow::Int64Builder int_builder;
    EXPECT_TRUE(int_builder.AppendValues({12345, 67890, 11111, 22222}).ok());
    auto int_array = int_builder.Finish().ValueOrDie();

    // Test string truncation
    arrow::StringBuilder str_builder;
    EXPECT_TRUE(str_builder.AppendValues({"hello", "world", "test", "long_string"}).ok());
    auto str_array = str_builder.Finish().ValueOrDie();

    // Create partition specs with TRUNCATE transform
    PartitionSpec spec_num(1);
    spec_num.fields.emplace_back(0, 100, "trunc_num", Transform::TRUNCATE, 3);  // Truncate to 1000s

    PartitionSpec spec_str(1);
    spec_str.fields.emplace_back(0, 100, "trunc_str", Transform::TRUNCATE, 4);  // Truncate to 4 chars

    // Test numeric truncation
    auto result_num = DataIngestorUtil::ExtractPartitionValuesVectorized(spec_num.fields[0], int_array);
    VERIFY_RESULT(result_num);
    auto values_num = result_num.value();
    ASSERT_EQ(values_num->length(), 4);
    EXPECT_EQ(values_num->GetString(0), "12000");
    EXPECT_EQ(values_num->GetString(1), "67000");
    EXPECT_EQ(values_num->GetString(2), "11000");
    EXPECT_EQ(values_num->GetString(3), "22000");

    // Test string truncation
    auto result_str = DataIngestorUtil::ExtractPartitionValuesVectorized(spec_str.fields[0], str_array);
    VERIFY_RESULT(result_str);
    auto values_str = result_str.value();
    ASSERT_EQ(values_str->length(), 4);
    EXPECT_EQ(values_str->GetString(0), "hell");
    EXPECT_EQ(values_str->GetString(1), "worl");
    EXPECT_EQ(values_str->GetString(2), "test");
    EXPECT_EQ(values_str->GetString(3), "long");
}

//
// Test Setup:
//      Test ExtractPartitionValuesVectorized with IDENTITY transform on different types
// Test Result:
//      Should correctly preserve values while converting to strings
//
TEST_F(DataIngestorUtilTest, ExtractPartitionValuesVectorizedIdentity) {
    // Create arrays with different types
    arrow::Int32Builder int_builder;
    EXPECT_TRUE(int_builder.AppendValues({123, 456, 789}).ok());
    auto int_array = int_builder.Finish().ValueOrDie();

    arrow::DoubleBuilder double_builder;
    EXPECT_TRUE(double_builder.AppendValues({1.23, 4.56, 7.89}).ok());
    auto double_array = double_builder.Finish().ValueOrDie();

    arrow::StringBuilder string_builder;
    EXPECT_TRUE(string_builder.AppendValues({"abc", "def", "ghi"}).ok());
    auto string_array = string_builder.Finish().ValueOrDie();

    // Create partition spec with IDENTITY transform
    PartitionSpec spec(1);
    spec.fields.emplace_back(0, 100, "identity", Transform::IDENTITY);

    // Test with Int32 array
    auto result_int = DataIngestorUtil::ExtractPartitionValuesVectorized(spec.fields[0], int_array);
    VERIFY_RESULT(result_int);
    auto values_int = result_int.value();
    ASSERT_EQ(values_int->length(), 3);
    EXPECT_EQ(values_int->GetString(0), "123");
    EXPECT_EQ(values_int->GetString(1), "456");
    EXPECT_EQ(values_int->GetString(2), "789");

    // Test with Double array
    auto result_double = DataIngestorUtil::ExtractPartitionValuesVectorized(spec.fields[0], double_array);
    VERIFY_RESULT(result_double);
    auto values_double = result_double.value();
    ASSERT_EQ(values_double->length(), 3);
    EXPECT_EQ(values_double->GetString(0), "1.23");
    EXPECT_EQ(values_double->GetString(1), "4.56");
    EXPECT_EQ(values_double->GetString(2), "7.89");

    // Test with String array
    auto result_string = DataIngestorUtil::ExtractPartitionValuesVectorized(spec.fields[0], string_array);
    VERIFY_RESULT(result_string);
    auto values_string = result_string.value();
    ASSERT_EQ(values_string->length(), 3);
    EXPECT_EQ(values_string->GetString(0), "abc");
    EXPECT_EQ(values_string->GetString(1), "def");
    EXPECT_EQ(values_string->GetString(2), "ghi");
}

}  // namespace pond::catalog
