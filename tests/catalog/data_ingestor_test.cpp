#include "shared.h"

namespace pond::catalog {

class DataIngestorTest : public DataIngestorTestBase {};

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

    // First test: Non-nullable input for nullable table schema
    // Create batch with non-nullable fields
    auto strict_schema = arrow::schema({arrow::field("id", arrow::int32(), false),
                                        arrow::field("name", arrow::utf8(), false),
                                        arrow::field("value", arrow::float64(), false)});

    auto batch = CreateTestBatch();
    auto result = ingestor->IngestBatch(batch);
    VERIFY_RESULT(result);

    // Second test: Nullable input for nullable table schema
    // Check if table schema has nullable fields
    bool has_nullable_fields = false;
    for (const auto& field : schema_->Fields()) {
        if (field.nullability == common::Nullability::NULLABLE) {
            has_nullable_fields = true;
            break;
        }
    }

    if (has_nullable_fields) {
        // Create batch with null values in nullable fields
        arrow::Int32Builder id_builder;
        ASSERT_TRUE(id_builder.AppendValues({1, 2, 3}).ok());
        ASSERT_TRUE(id_builder.AppendNull().ok());
        auto id_array = id_builder.Finish().ValueOrDie();

        arrow::StringBuilder name_builder;
        ASSERT_TRUE(name_builder.AppendValues({"one", "two", "three"}).ok());
        ASSERT_TRUE(name_builder.AppendNull().ok());
        auto name_array = name_builder.Finish().ValueOrDie();

        arrow::DoubleBuilder value_builder;
        ASSERT_TRUE(value_builder.AppendValues({1.1, 2.2, 3.3}).ok());
        ASSERT_TRUE(value_builder.AppendNull().ok());
        auto value_array = value_builder.Finish().ValueOrDie();

        auto batch_with_nulls = arrow::RecordBatch::Make(strict_schema, 4, {id_array, name_array, value_array});
        result = ingestor->IngestBatch(batch_with_nulls);
        VERIFY_RESULT(result);
    }

    // Third test: Nullable input for non-nullable table schema
    // Create a new table with non-nullable fields
    auto non_nullable_schema = common::CreateSchemaBuilder()
                                   .AddField("id", common::ColumnType::INT32, false)
                                   .AddField("name", common::ColumnType::STRING, false)
                                   .AddField("value", common::ColumnType::DOUBLE, false)
                                   .Build();

    auto create_result = catalog_->CreateTable(
        "non_nullable_table", non_nullable_schema, PartitionSpec{}, "/tmp/non_nullable_table", {});
    VERIFY_RESULT(create_result);

    // Create ingestor for non-nullable table
    auto non_nullable_ingestor_result = DataIngestor::Create(catalog_, fs_, "non_nullable_table");
    VERIFY_RESULT(non_nullable_ingestor_result);
    auto non_nullable_ingestor = std::move(non_nullable_ingestor_result).value();

    // Try to ingest batch with null values
    arrow::Int32Builder id_builder2;
    ASSERT_TRUE(id_builder2.AppendValues({1, 2, 3}).ok());
    ASSERT_TRUE(id_builder2.AppendNull().ok());
    auto id_array2 = id_builder2.Finish().ValueOrDie();

    arrow::StringBuilder name_builder2;
    ASSERT_TRUE(name_builder2.AppendValues({"one", "two", "three"}).ok());
    ASSERT_TRUE(name_builder2.AppendNull().ok());
    auto name_array2 = name_builder2.Finish().ValueOrDie();

    arrow::DoubleBuilder value_builder2;
    ASSERT_TRUE(value_builder2.AppendValues({1.1, 2.2, 3.3}).ok());
    ASSERT_TRUE(value_builder2.AppendNull().ok());
    auto value_array2 = value_builder2.Finish().ValueOrDie();

    auto batch_with_nulls2 = arrow::RecordBatch::Make(strict_schema, 4, {id_array2, name_array2, value_array2});
    result = non_nullable_ingestor->IngestBatch(batch_with_nulls2);
    VERIFY_ERROR_CODE(result, common::ErrorCode::ParquetInvalidNullability);
}

//
// Test Setup:
//      Create a table with year partition spec
//      Create a batch with data from multiple years
//      Ingest the batch
// Test Result:
//      Data should be partitioned into separate files by year
//
TEST_F(DataIngestorTest, TestIngestPartitionedBatch) {
    // Create schema with date field
    auto schema = common::CreateSchemaBuilder()
                      .AddField("id", common::ColumnType::INT32)
                      .AddField("date", common::ColumnType::STRING)  // Using string for simplicity
                      .AddField("value", common::ColumnType::DOUBLE)
                      .Build();

    // Create a partition spec on the date field by year
    PartitionSpec partition_spec;
    partition_spec.spec_id = 1;
    partition_spec.fields.emplace_back(1, 0, "year", Transform::YEAR);

    // Create table with year partition
    auto create_result =
        catalog_->CreateTable("partitioned_table", schema, partition_spec, "/tmp/partitioned_table", {});
    VERIFY_RESULT(create_result);
    auto table_metadata = create_result.value();

    // Create a record batch with data spanning multiple years
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

    // Create ingestor
    auto ingestor_result = DataIngestor::Create(catalog_, fs_, "partitioned_table");
    VERIFY_RESULT(ingestor_result);
    auto ingestor = std::move(ingestor_result).value();

    // Ingest the batch
    auto ingest_result = ingestor->IngestBatch(batch);
    VERIFY_RESULT(ingest_result);

    // List files to verify partitioning
    auto files_result = catalog_->ListDataFiles("partitioned_table");
    VERIFY_RESULT(files_result);

    // Should have 2 files (one for 2020 and one for 2021)
    ASSERT_EQ(files_result.value().size(), 2);

    // Verify partition values
    bool found_2020 = false;
    bool found_2021 = false;

    for (const auto& file : files_result.value()) {
        ASSERT_EQ(file.partition_values.size(), 1);
        ASSERT_TRUE(file.partition_values.find("year") != file.partition_values.end());

        if (file.partition_values.at("year") == "2020") {
            found_2020 = true;
            ASSERT_EQ(file.record_count, 2);  // 2 records from 2020

            // Verify the file path contains the partition info
            ASSERT_TRUE(file.file_path.find("year=2020") != std::string::npos);

            // Verify file exists
            ASSERT_TRUE(fs_->Exists(file.file_path));

            // Verify file contents
            std::vector<int32_t> expected_ids = {1, 2};
            std::vector<std::string> expected_dates = {"2020-01-15", "2020-06-20"};
            std::vector<double> expected_values = {10.0, 20.0};
            VerifyParquetFileContents(file.file_path, expected_ids, expected_dates, expected_values);
        } else if (file.partition_values.at("year") == "2021") {
            found_2021 = true;
            ASSERT_EQ(file.record_count, 2);  // 2 records from 2021

            // Verify the file path contains the partition info
            ASSERT_TRUE(file.file_path.find("year=2021") != std::string::npos);

            // Verify file exists
            ASSERT_TRUE(fs_->Exists(file.file_path));

            // Verify file contents
            std::vector<int32_t> expected_ids = {3, 4};
            std::vector<std::string> expected_dates = {"2021-03-10", "2021-11-30"};
            std::vector<double> expected_values = {30.0, 40.0};
            VerifyParquetFileContents(file.file_path, expected_ids, expected_dates, expected_values);
        }
    }

    ASSERT_TRUE(found_2020);
    ASSERT_TRUE(found_2021);
}

}  // namespace pond::catalog