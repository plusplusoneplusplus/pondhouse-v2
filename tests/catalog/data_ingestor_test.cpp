#include "catalog/data_ingestor.h"

#include <arrow/table.h>
#include <gtest/gtest.h>

#include "catalog/kv_catalog.h"
#include "common/memory_append_only_fs.h"
#include "common/schema.h"
#include "format/parquet/parquet_reader.h"
#include "format/parquet/schema_converter.h"
#include "test_helper.h"

namespace pond::catalog {

class DataIngestorTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create test schema
        schema_ = common::CreateSchemaBuilder()
                      .AddField("id", common::ColumnType::INT32)
                      .AddField("name", common::ColumnType::STRING)
                      .AddField("value", common::ColumnType::DOUBLE)
                      .Build();

        fs_ = std::make_shared<common::MemoryAppendOnlyFileSystem>();
        auto db_result = kv::DB::Create(fs_, "test_catalog");
        VERIFY_RESULT(db_result);
        db_ = std::move(db_result.value());

        catalog_ = std::make_shared<KVCatalog>(db_);

        // Create test table
        auto result = catalog_->CreateTable("test_table",
                                            schema_,
                                            PartitionSpec{},  // No partitioning
                                            "/tmp/test_table",
                                            {});
        ASSERT_TRUE(result.ok());
        table_metadata_ = result.value();
    }

    std::shared_ptr<common::Schema> schema_;
    std::shared_ptr<common::IAppendOnlyFileSystem> fs_;
    std::shared_ptr<kv::DB> db_;
    std::shared_ptr<Catalog> catalog_;
    TableMetadata table_metadata_;

    std::shared_ptr<arrow::RecordBatch> CreateTestBatch() {
        // Create test data
        std::vector<std::shared_ptr<arrow::Array>> arrays;

        // Create ID column
        arrow::Int32Builder id_builder;
        EXPECT_TRUE(id_builder.AppendValues({1, 2, 3}).ok());
        arrays.push_back(id_builder.Finish().ValueOrDie());

        // Create name column
        arrow::StringBuilder name_builder;
        EXPECT_TRUE(name_builder.AppendValues({"one", "two", "three"}).ok());
        arrays.push_back(name_builder.Finish().ValueOrDie());

        // Create value column
        arrow::DoubleBuilder value_builder;
        EXPECT_TRUE(value_builder.AppendValues({1.1, 2.2, 3.3}).ok());
        arrays.push_back(value_builder.Finish().ValueOrDie());

        auto arrow_schema = format::SchemaConverter::ToArrowSchema(*schema_).value();
        return arrow::RecordBatch::Make(arrow_schema, 3, arrays);
    }

    // Add new helper method to verify data in a Parquet file
    void VerifyParquetFileContents(const std::string& file_path,
                                   const std::vector<int32_t>& expected_ids,
                                   const std::vector<std::string>& expected_names,
                                   const std::vector<double>& expected_values) {
        auto reader_result = format::ParquetReader::create(fs_, file_path);
        ASSERT_TRUE(reader_result.ok());
        auto reader = std::move(reader_result).value();

        // Read the data
        auto table_result = reader->read();
        ASSERT_TRUE(table_result.ok());
        auto table = table_result.value();

        // Verify number of rows
        ASSERT_EQ(table->num_rows(), expected_ids.size());

        // Get columns
        auto id_array = std::static_pointer_cast<arrow::Int32Array>(table->column(0)->chunk(0));
        auto name_array = std::static_pointer_cast<arrow::StringArray>(table->column(1)->chunk(0));
        auto value_array = std::static_pointer_cast<arrow::DoubleArray>(table->column(2)->chunk(0));

        // Verify contents
        for (int64_t i = 0; i < table->num_rows(); i++) {
            EXPECT_EQ(id_array->Value(i), expected_ids[i]);
            EXPECT_EQ(name_array->GetString(i), expected_names[i]);
            EXPECT_DOUBLE_EQ(value_array->Value(i), expected_values[i]);
        }
    }
};

//
// Test Setup:
//      Create ingestor and prepare a test record batch
//      Ingest the batch and verify the commit
// Test Result:
//      Verify data file is created and metadata is updated
//
TEST_F(DataIngestorTest, TestIngestBatch) {
    auto ingestor_result = DataIngestor::Create(catalog_, fs_, "test_table");
    VERIFY_RESULT(ingestor_result);
    auto ingestor = std::move(ingestor_result).value();

    auto batch = CreateTestBatch();
    auto ingest_result = ingestor->IngestBatch(batch);
    VERIFY_RESULT(ingest_result);
    auto data_file = std::move(ingest_result).value();
    EXPECT_EQ(data_file.file_path, "/tmp/test_table/data/part_0_0.parquet");

    // Verify the data file was created
    auto files_result = catalog_->ListDataFiles("test_table");
    ASSERT_TRUE(files_result.ok());
    ASSERT_EQ(files_result.value().size(), 1);
    EXPECT_EQ(files_result.value()[0].record_count, 3);
}

//
// Test Setup:
//      Create ingestor and prepare multiple test record batches
//      Ingest batches without immediate commits
// Test Result:
//      Verify all data is written and committed together
//
TEST_F(DataIngestorTest, TestMultipleBatches) {
    auto ingestor_result = DataIngestor::Create(catalog_, fs_, "test_table");
    VERIFY_RESULT(ingestor_result);
    auto ingestor = std::move(ingestor_result).value();

    auto batch = CreateTestBatch();
    ASSERT_TRUE(ingestor->IngestBatch(batch, false).ok());
    ASSERT_TRUE(ingestor->IngestBatch(batch, false).ok());
    ASSERT_TRUE(ingestor->Commit().ok());

    // Verify the data files were created
    auto files_result = catalog_->ListDataFiles("test_table");
    ASSERT_TRUE(files_result.ok());
    ASSERT_EQ(files_result.value().size(), 2);
    EXPECT_EQ(files_result.value()[0].record_count, 3);
    EXPECT_EQ(files_result.value()[1].record_count, 3);
}

//
// Test Setup:
//      Create ingestor, ingest data, and verify the data is written to filesystem
// Test Result:
//      Data should be correctly written to Parquet file and readable back
//
TEST_F(DataIngestorTest, TestDataPersistence) {
    auto ingestor_result = DataIngestor::Create(catalog_, fs_, "test_table");
    VERIFY_RESULT(ingestor_result);
    auto ingestor = std::move(ingestor_result).value();

    // Create test data
    auto batch = CreateTestBatch();
    ASSERT_TRUE(ingestor->IngestBatch(batch).ok());

    // Get the list of files from the catalog
    auto files_result = catalog_->ListDataFiles("test_table");
    VERIFY_RESULT(files_result);
    ASSERT_EQ(files_result.value().size(), 1);

    // Verify the file exists in the filesystem
    const auto& file_path = files_result.value()[0].file_path;
    ASSERT_TRUE(fs_->Exists(file_path));

    // Verify the contents of the Parquet file
    std::vector<int32_t> expected_ids = {1, 2, 3};
    std::vector<std::string> expected_names = {"one", "two", "three"};
    std::vector<double> expected_values = {1.1, 2.2, 3.3};
    VerifyParquetFileContents(file_path, expected_ids, expected_names, expected_values);
}

//
// Test Setup:
//      Ingest multiple batches without committing, then commit all at once
// Test Result:
//      All data should be correctly written to separate Parquet files
//
TEST_F(DataIngestorTest, TestMultipleBatchesPersistence) {
    auto ingestor_result = DataIngestor::Create(catalog_, fs_, "test_table");
    VERIFY_RESULT(ingestor_result);
    auto ingestor = std::move(ingestor_result).value();

    // Create and ingest first batch
    auto batch1 = CreateTestBatch();  // Contains {1, 2, 3}
    ASSERT_TRUE(ingestor->IngestBatch(batch1, false).ok());

    // Create and ingest second batch with different values
    std::vector<std::shared_ptr<arrow::Array>> arrays;

    arrow::Int32Builder id_builder;
    ASSERT_TRUE(id_builder.AppendValues({4, 5, 6}).ok());
    arrays.push_back(id_builder.Finish().ValueOrDie());

    arrow::StringBuilder name_builder;
    ASSERT_TRUE(name_builder.AppendValues({"four", "five", "six"}).ok());
    arrays.push_back(name_builder.Finish().ValueOrDie());

    arrow::DoubleBuilder value_builder;
    ASSERT_TRUE(value_builder.AppendValues({4.4, 5.5, 6.6}).ok());
    arrays.push_back(value_builder.Finish().ValueOrDie());

    auto arrow_schema = format::SchemaConverter::ToArrowSchema(*schema_).value();
    auto batch2 = arrow::RecordBatch::Make(arrow_schema, 3, arrays);
    ASSERT_TRUE(ingestor->IngestBatch(batch2, false).ok());

    // Commit all changes
    ASSERT_TRUE(ingestor->Commit().ok());

    // Get the list of files from the catalog
    auto files_result = catalog_->ListDataFiles("test_table");
    ASSERT_TRUE(files_result.ok());
    ASSERT_EQ(files_result.value().size(), 2);

    // Verify first file
    const auto& file1_path = files_result.value()[0].file_path;
    ASSERT_TRUE(fs_->Exists(file1_path));
    std::vector<int32_t> expected_ids1 = {1, 2, 3};
    std::vector<std::string> expected_names1 = {"one", "two", "three"};
    std::vector<double> expected_values1 = {1.1, 2.2, 3.3};
    VerifyParquetFileContents(file1_path, expected_ids1, expected_names1, expected_values1);

    // Verify second file
    const auto& file2_path = files_result.value()[1].file_path;
    ASSERT_TRUE(fs_->Exists(file2_path));
    std::vector<int32_t> expected_ids2 = {4, 5, 6};
    std::vector<std::string> expected_names2 = {"four", "five", "six"};
    std::vector<double> expected_values2 = {4.4, 5.5, 6.6};
    VerifyParquetFileContents(file2_path, expected_ids2, expected_names2, expected_values2);
}

//
// Test Setup:
//      Attempt to read from a non-existent file
// Test Result:
//      Should return appropriate error
//
TEST_F(DataIngestorTest, TestNonExistentFile) {
    auto reader_result = format::ParquetReader::create(fs_, "/tmp/non_existent.parquet");
    EXPECT_FALSE(reader_result.ok());
    EXPECT_EQ(reader_result.error().code(), common::ErrorCode::FileOpenFailed);
}

//
// Test Setup:
//      Create ingestor and try to ingest data with mismatched schema
// Test Result:
//      Should fail with appropriate schema mismatch errors
//
TEST_F(DataIngestorTest, TestSchemaMismatch) {
    auto ingestor_result = DataIngestor::Create(catalog_, fs_, "test_table");
    VERIFY_RESULT(ingestor_result);
    auto ingestor = std::move(ingestor_result).value();

    // Create batch with wrong field name
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    arrow::Int32Builder id_builder;
    ASSERT_TRUE(id_builder.AppendValues({1, 2, 3}).ok());
    arrays.push_back(id_builder.Finish().ValueOrDie());

    arrow::StringBuilder wrong_name_builder;
    ASSERT_TRUE(wrong_name_builder.AppendValues({"one", "two", "three"}).ok());
    arrays.push_back(wrong_name_builder.Finish().ValueOrDie());

    arrow::DoubleBuilder value_builder;
    ASSERT_TRUE(value_builder.AppendValues({1.1, 2.2, 3.3}).ok());
    arrays.push_back(value_builder.Finish().ValueOrDie());

    auto wrong_schema = arrow::schema({arrow::field("id", arrow::int32()),
                                       arrow::field("wrong_name", arrow::utf8()),  // Wrong field name
                                       arrow::field("value", arrow::float64())});

    auto batch_wrong_name = arrow::RecordBatch::Make(wrong_schema, 3, arrays);
    auto result = ingestor->IngestBatch(batch_wrong_name);
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.error().code(), common::ErrorCode::SchemaMismatch);
    EXPECT_TRUE(result.error().message().find("Field name mismatch") != std::string::npos);

    // Create batch with wrong type
    auto wrong_type_schema = arrow::schema({
        arrow::field("id", arrow::int32()),
        arrow::field("name", arrow::utf8()),
        arrow::field("value", arrow::int64())  // Wrong type
    });

    arrow::Int64Builder wrong_type_builder;
    ASSERT_TRUE(wrong_type_builder.AppendValues({1, 2, 3}).ok());
    arrays[2] = wrong_type_builder.Finish().ValueOrDie();

    auto batch_wrong_type = arrow::RecordBatch::Make(wrong_type_schema, 3, arrays);
    result = ingestor->IngestBatch(batch_wrong_type);
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.error().code(), common::ErrorCode::SchemaMismatch);
    EXPECT_TRUE(result.error().message().find("Field type mismatch") != std::string::npos);

    // Create batch with wrong number of fields
    auto fewer_fields_schema = arrow::schema({arrow::field("id", arrow::int32()), arrow::field("name", arrow::utf8())});

    arrays.pop_back();
    auto batch_fewer_fields = arrow::RecordBatch::Make(fewer_fields_schema, 3, arrays);
    result = ingestor->IngestBatch(batch_fewer_fields);
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.error().code(), common::ErrorCode::SchemaMismatch);
    EXPECT_TRUE(result.error().message().find("Schema field count mismatch") != std::string::npos);
}

//
// Test Setup:
//      Create ingestor and try to ingest data with nullability mismatches
// Test Result:
//      Should handle nullability appropriately
//
TEST_F(DataIngestorTest, TestNullabilityHandling) {
    auto ingestor_result = DataIngestor::Create(catalog_, fs_, "test_table");
    VERIFY_RESULT(ingestor_result);
    auto ingestor = std::move(ingestor_result).value();

    // Create batch with non-nullable fields
    auto strict_schema = arrow::schema({arrow::field("id", arrow::int32(), false),
                                        arrow::field("name", arrow::utf8(), false),
                                        arrow::field("value", arrow::float64(), false)});

    auto batch = CreateTestBatch();
    auto result = ingestor->IngestBatch(batch);
    VERIFY_RESULT(result);

    // The reverse case (nullable input for non-nullable table) would be tested
    // if we had any non-nullable fields in our table schema
}

//
// Test Setup:
//      Create table with identity partition on "id" field
//      Create a batch with valid data
// Test Result:
//      Should extract correct partition values using identity transform
//
TEST_F(DataIngestorTest, TestExtractPartitionValuesIdentity) {
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
    auto result = DataIngestor::ExtractPartitionValues(metadata, batch);
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
TEST_F(DataIngestorTest, TestExtractPartitionValuesBucket) {
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
    auto result = DataIngestor::ExtractPartitionValues(metadata, batch);
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
TEST_F(DataIngestorTest, TestExtractPartitionValuesTruncate) {
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
    auto result = DataIngestor::ExtractPartitionValues(metadata, batch);
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
TEST_F(DataIngestorTest, TestExtractPartitionValuesString) {
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
    auto result = DataIngestor::ExtractPartitionValues(metadata, batch);
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
TEST_F(DataIngestorTest, TestExtractPartitionValuesMultipleFields) {
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
    auto result = DataIngestor::ExtractPartitionValues(metadata, batch);
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
TEST_F(DataIngestorTest, TestExtractPartitionValuesEmptyBatch) {
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
    auto result = DataIngestor::ExtractPartitionValues(metadata, empty_batch);
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
TEST_F(DataIngestorTest, TestExtractPartitionValuesNoPartitionSpecs) {
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
    auto result = DataIngestor::ExtractPartitionValues(metadata, batch);
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
TEST_F(DataIngestorTest, TestExtractPartitionValuesInvalidField) {
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
    auto result = DataIngestor::ExtractPartitionValues(metadata, batch);
    ASSERT_FALSE(result.ok());
    ASSERT_EQ(result.error().code(), common::ErrorCode::InvalidOperation);
}

//
// Test Setup:
//      Create table with partition on non-existent field
// Test Result:
//      Should return error for missing field
//
TEST_F(DataIngestorTest, TestExtractPartitionValuesFieldNotFound) {
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
    auto result = DataIngestor::ExtractPartitionValues(metadata, batch);
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
TEST_F(DataIngestorTest, TestExtractPartitionValuesSchemaMismatch) {
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
    auto result = DataIngestor::ExtractPartitionValues(metadata, batch);
    ASSERT_FALSE(result.ok());
    ASSERT_EQ(result.error().code(), common::ErrorCode::SchemaMismatch);
}

//
// Test Setup:
//      Create table with partition on nullable field with null values
// Test Result:
//      Should return error for null partition field
//
TEST_F(DataIngestorTest, TestExtractPartitionValuesNullField) {
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
    auto result = DataIngestor::ExtractPartitionValues(metadata, batch);
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
TEST_F(DataIngestorTest, TestExtractPartitionValuesUnsupportedType) {
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
    auto result = DataIngestor::ExtractPartitionValues(metadata, batch);
    ASSERT_FALSE(result.ok());
    ASSERT_EQ(result.error().code(), common::ErrorCode::NotImplemented);
}

//
// Test Setup:
//      Attempt to use unsupported transform type
// Test Result:
//      Should return not implemented error
//
TEST_F(DataIngestorTest, TestExtractPartitionValuesUnsupportedTransform) {
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
    auto result = DataIngestor::ExtractPartitionValues(metadata, batch);
    ASSERT_FALSE(result.ok());
    ASSERT_EQ(result.error().code(), common::ErrorCode::NotImplemented);
}

//
// Test Setup:
//      Try to apply TRUNCATE transform to a string field
// Test Result:
//      Should return invalid operation error
//
TEST_F(DataIngestorTest, TestExtractPartitionValuesInvalidTransformForType) {
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
    auto result = DataIngestor::ExtractPartitionValues(metadata, batch);
    ASSERT_FALSE(result.ok());
    ASSERT_EQ(result.error().code(), common::ErrorCode::InvalidOperation);
    ASSERT_TRUE(result.error().message().find("TRUNCATE transform only applies to numeric types") != std::string::npos);
}

}  // namespace pond::catalog