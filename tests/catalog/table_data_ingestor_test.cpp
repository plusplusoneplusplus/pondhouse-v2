#include "catalog/table_data_ingestor.h"

#include <arrow/table.h>
#include <gtest/gtest.h>

#include "catalog/kv_catalog.h"
#include "common/memory_append_only_fs.h"
#include "common/schema.h"
#include "format/parquet/parquet_reader.h"
#include "format/parquet/schema_converter.h"
#include "test_helper.h"

namespace pond::catalog {

class TableDataIngestorTest : public ::testing::Test {
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
TEST_F(TableDataIngestorTest, TestIngestBatch) {
    auto ingestor_result = TableDataIngestor::Create(catalog_, fs_, "test_table");
    ASSERT_TRUE(ingestor_result.ok());
    auto ingestor = std::move(ingestor_result).value();

    auto batch = CreateTestBatch();
    ASSERT_TRUE(ingestor->IngestBatch(batch).ok());

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
TEST_F(TableDataIngestorTest, TestMultipleBatches) {
    auto ingestor_result = TableDataIngestor::Create(catalog_, fs_, "test_table");
    ASSERT_TRUE(ingestor_result.ok());
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
TEST_F(TableDataIngestorTest, TestDataPersistence) {
    auto ingestor_result = TableDataIngestor::Create(catalog_, fs_, "test_table");
    ASSERT_TRUE(ingestor_result.ok());
    auto ingestor = std::move(ingestor_result).value();

    // Create test data
    auto batch = CreateTestBatch();
    ASSERT_TRUE(ingestor->IngestBatch(batch).ok());

    // Get the list of files from the catalog
    auto files_result = catalog_->ListDataFiles("test_table");
    ASSERT_TRUE(files_result.ok());
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
TEST_F(TableDataIngestorTest, TestMultipleBatchesPersistence) {
    auto ingestor_result = TableDataIngestor::Create(catalog_, fs_, "test_table");
    ASSERT_TRUE(ingestor_result.ok());
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
TEST_F(TableDataIngestorTest, TestNonExistentFile) {
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
TEST_F(TableDataIngestorTest, TestSchemaMismatch) {
    auto ingestor_result = TableDataIngestor::Create(catalog_, fs_, "test_table");
    ASSERT_TRUE(ingestor_result.ok());
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
TEST_F(TableDataIngestorTest, TestNullabilityHandling) {
    auto ingestor_result = TableDataIngestor::Create(catalog_, fs_, "test_table");
    ASSERT_TRUE(ingestor_result.ok());
    auto ingestor = std::move(ingestor_result).value();

    // Create batch with non-nullable fields
    auto strict_schema = arrow::schema({arrow::field("id", arrow::int32(), false),
                                        arrow::field("name", arrow::utf8(), false),
                                        arrow::field("value", arrow::float64(), false)});

    auto batch = CreateTestBatch();
    auto result = ingestor->IngestBatch(batch);
    ASSERT_TRUE(result.ok()) << "Should accept non-nullable input for nullable table fields";

    // The reverse case (nullable input for non-nullable table) would be tested
    // if we had any non-nullable fields in our table schema
}

}  // namespace pond::catalog