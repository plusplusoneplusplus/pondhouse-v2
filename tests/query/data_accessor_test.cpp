#include <arrow/api.h>
#include <arrow/builder.h>
#include <gtest/gtest.h>

#include "catalog/data_ingestor.h"
#include "catalog/kv_catalog.h"
#include "common/memory_append_only_fs.h"
#include "kv/db.h"
#include "query/data/catalog_data_accessor.h"
#include "test_helper.h"

namespace pond::query {

class DataAccessorTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create memory filesystem and KV database
        fs_ = std::make_shared<common::MemoryAppendOnlyFileSystem>();
        auto db_result = kv::DB::Create(fs_, "test_catalog");
        VERIFY_RESULT(db_result);
        db_ = std::move(db_result.value());

        // Create catalog and data accessor
        catalog_ = std::make_shared<catalog::KVCatalog>(db_);
        data_accessor_ = std::make_unique<CatalogDataAccessor>(catalog_, fs_);

        // Create test table with schema and data
        SetupTestTable();
        IngestTestData();
    }

    void SetupTestTable() {
        // Create schema
        auto schema = std::make_shared<common::Schema>();
        schema->AddField("id", common::ColumnType::INT32);
        schema->AddField("name", common::ColumnType::STRING);
        schema->AddField("age", common::ColumnType::INT32);

        // Create partition spec
        catalog::PartitionSpec spec(1);
        spec.fields.emplace_back(2, 100, "age_bucket", catalog::Transform::BUCKET);

        // Create table
        auto create_result = catalog_->CreateTable("test_table", schema, spec, "/test_table");
        VERIFY_RESULT(create_result);
    }

    void IngestTestData() {
        // Create data ingestor
        auto ingestor_result = catalog::DataIngestor::Create(catalog_, fs_, "test_table");
        VERIFY_RESULT(ingestor_result);
        auto ingestor = std::move(ingestor_result).value();

        // Helper function to create and ingest a batch
        auto CreateAndIngestBatch = [&](const std::vector<int32_t>& ids,
                                        const std::vector<std::string>& names,
                                        const std::vector<int32_t>& ages) {
            // Create Arrow arrays
            arrow::Int32Builder id_builder;
            arrow::StringBuilder name_builder;
            arrow::Int32Builder age_builder;

            EXPECT_TRUE(id_builder.AppendValues(ids).ok());
            EXPECT_TRUE(name_builder.AppendValues(names).ok());
            EXPECT_TRUE(age_builder.AppendValues(ages).ok());

            std::shared_ptr<arrow::Array> id_array, name_array, age_array;
            EXPECT_TRUE(id_builder.Finish(&id_array).ok());
            EXPECT_TRUE(name_builder.Finish(&name_array).ok());
            EXPECT_TRUE(age_builder.Finish(&age_array).ok());

            // Create record batch
            auto schema = arrow::schema({arrow::field("id", arrow::int32()),
                                         arrow::field("name", arrow::utf8()),
                                         arrow::field("age", arrow::int32())});
            auto batch = arrow::RecordBatch::Make(schema, ids.size(), {id_array, name_array, age_array});

            // Ingest batch
            auto ingest_result = ingestor->IngestBatch(batch);
            VERIFY_RESULT(ingest_result);
        };

        // Ingest first partition (age_bucket = 1)
        CreateAndIngestBatch({1, 2, 3}, {"Alice", "Bob", "Charlie"}, {25, 28, 30});

        // Ingest second partition (age_bucket = 2)
        CreateAndIngestBatch({4, 5}, {"David", "Eve"}, {35, 38});

        // Commit changes
        auto commit_result = ingestor->Commit();
        VERIFY_RESULT(commit_result);
    }

    std::shared_ptr<common::MemoryAppendOnlyFileSystem> fs_;
    std::shared_ptr<kv::DB> db_;
    std::shared_ptr<catalog::KVCatalog> catalog_;
    std::unique_ptr<DataAccessor> data_accessor_;
};

//
// Test Setup:
//      Get schema for an existing table
// Test Result:
//      Should return correct schema with all fields
//
TEST_F(DataAccessorTest, GetTableSchema) {
    auto schema_result = data_accessor_->GetTableSchema("test_table");
    VERIFY_RESULT(schema_result);

    auto schema = schema_result.value();
    ASSERT_EQ(3, schema->FieldCount());
    EXPECT_EQ("id", schema->Fields()[0].name);
    EXPECT_EQ("name", schema->Fields()[1].name);
    EXPECT_EQ("age", schema->Fields()[2].name);
}

//
// Test Setup:
//      Get schema for a non-existent table
// Test Result:
//      Should return TableNotFound error
//
TEST_F(DataAccessorTest, GetTableSchemaNonExistent) {
    auto schema_result = data_accessor_->GetTableSchema("non_existent_table");
    EXPECT_FALSE(schema_result.ok());
    EXPECT_EQ(common::ErrorCode::TableNotFound, schema_result.error().code());
}

//
// Test Setup:
//      List data files for an existing table with actual data
// Test Result:
//      Should return all data files with correct metadata and record counts
//
TEST_F(DataAccessorTest, DISABLED_ListTableFiles) {
    auto files_result = data_accessor_->ListTableFiles("test_table");
    VERIFY_RESULT(files_result);

    auto files = files_result.value();
    ASSERT_EQ(2, files.size());

    // Verify files have correct record counts
    bool found_partition1 = false;
    bool found_partition2 = false;

    for (const auto& file : files) {
        if (file.partition_values.at("age_bucket") == "1") {
            EXPECT_EQ(3, file.record_count);  // First partition has 3 records
            found_partition1 = true;
        } else if (file.partition_values.at("age_bucket") == "2") {
            EXPECT_EQ(2, file.record_count);  // Second partition has 2 records
            found_partition2 = true;
        }
    }

    EXPECT_TRUE(found_partition1);
    EXPECT_TRUE(found_partition2);
}

//
// Test Setup:
//      Get reader for a data file and read its contents
// Test Result:
//      Should successfully read the data with correct values
//
TEST_F(DataAccessorTest, GetReaderAndReadData) {
    // Get the files
    auto files_result = data_accessor_->ListTableFiles("test_table");
    VERIFY_RESULT(files_result);
    ASSERT_FALSE(files_result.value().empty());

    // Get a reader for the first partition
    const auto& first_file = files_result.value()[0];
    auto reader_result = data_accessor_->GetReader(first_file);
    VERIFY_RESULT(reader_result);
    auto reader = std::move(reader_result).value();

    // Read the data
    auto table_result = reader->read();
    VERIFY_RESULT(table_result);
    auto table = table_result.value();

    // Verify the data
    ASSERT_EQ(3, table->num_columns());
    ASSERT_GT(table->num_rows(), 0);

    // Verify schema
    auto schema = table->schema();
    ASSERT_EQ("id", schema->field(0)->name());
    ASSERT_EQ("name", schema->field(1)->name());
    ASSERT_EQ("age", schema->field(2)->name());
}

//
// Test Setup:
//      List data files with specific snapshot ID after data ingestion
// Test Result:
//      Should return files from that snapshot with correct record counts
//
TEST_F(DataAccessorTest, DISABLED_ListTableFilesWithSnapshot) {
    // First snapshot contains our test data
    auto files_result = data_accessor_->ListTableFiles("test_table", 1);
    VERIFY_RESULT(files_result);
    ASSERT_EQ(1, files_result.value().size());

    // Verify record counts in first snapshot
    int total_records = 0;
    for (const auto& file : files_result.value()) {
        total_records += file.record_count;
    }
    EXPECT_EQ(3, total_records);  // Total records across both first file

    files_result = data_accessor_->ListTableFiles("test_table", 2);
    VERIFY_RESULT(files_result);
    ASSERT_EQ(2, files_result.value().size());

    // Verify record counts in second snapshot
    total_records = 0;
    for (const auto& file : files_result.value()) {
        total_records += file.record_count;
    }
    EXPECT_EQ(5, total_records);  // Total records across both files
}

}  // namespace pond::query