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
        schema_ = common::CreateSchemaBuilder()
                      .AddField("id", common::ColumnType::INT32)
                      .AddField("name", common::ColumnType::STRING)
                      .AddField("age", common::ColumnType::INT32)
                      .AddField("year", common::ColumnType::INT32)  // Added for partitioning
                      .Build();

        spec_ = catalog::PartitionSpec(1);
        spec_.fields.emplace_back(2, 100, "age_bucket", catalog::Transform::BUCKET, 10);  // Partition by age bucket
        spec_.fields.emplace_back(3, 100, "year", catalog::Transform::IDENTITY);          // Partition by year

        // Create test table with schema and data
        SetupTestTable();
        IngestTestData();
    }

    void SetupTestTable() {
        // Create table
        auto create_result = catalog_->CreateTable("test_table", schema_, spec_, "/test_table");
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
                                        const std::vector<int32_t>& ages,
                                        const std::vector<int32_t>& years) {
            // Create Arrow arrays
            arrow::Int32Builder id_builder;
            arrow::StringBuilder name_builder;
            arrow::Int32Builder age_builder;
            arrow::Int32Builder year_builder;

            EXPECT_TRUE(id_builder.AppendValues(ids).ok());
            EXPECT_TRUE(name_builder.AppendValues(names).ok());
            EXPECT_TRUE(age_builder.AppendValues(ages).ok());
            EXPECT_TRUE(year_builder.AppendValues(years).ok());

            std::shared_ptr<arrow::Array> id_array, name_array, age_array, year_array;
            EXPECT_TRUE(id_builder.Finish(&id_array).ok());
            EXPECT_TRUE(name_builder.Finish(&name_array).ok());
            EXPECT_TRUE(age_builder.Finish(&age_array).ok());
            EXPECT_TRUE(year_builder.Finish(&year_array).ok());

            // Create record batch
            auto schema = arrow::schema({arrow::field("id", arrow::int32()),
                                         arrow::field("name", arrow::utf8()),
                                         arrow::field("age", arrow::int32()),
                                         arrow::field("year", arrow::int32())});
            auto batch = arrow::RecordBatch::Make(schema, ids.size(), {id_array, name_array, age_array, year_array});

            // Ingest batch
            auto ingest_result = ingestor->IngestBatch(batch);
            VERIFY_RESULT(ingest_result);
        };

        // Ingest data for different partitions
        // Partition 1: age_bucket=1, year=2023
        CreateAndIngestBatch({1, 2, 3}, {"Alice", "Bob", "Charlie"}, {21, 31, 41}, {2023, 2023, 2023});

        // Partition 2: age_bucket=2, year=2023
        CreateAndIngestBatch({4, 5}, {"David", "Eve"}, {32, 42}, {2023, 2023});

        // Partition 3: age_bucket=1, year=2024
        CreateAndIngestBatch({6, 7}, {"Frank", "Grace"}, {51, 61}, {2024, 2024});

        // Commit changes
        auto commit_result = ingestor->Commit();
        VERIFY_RESULT(commit_result);
    }

    std::shared_ptr<common::MemoryAppendOnlyFileSystem> fs_;
    std::shared_ptr<kv::DB> db_;
    std::shared_ptr<catalog::KVCatalog> catalog_;
    std::unique_ptr<DataAccessor> data_accessor_;
    std::shared_ptr<common::Schema> schema_;
    catalog::PartitionSpec spec_;
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
    ASSERT_EQ(4, schema->FieldCount());
    EXPECT_EQ("id", schema->Fields()[0].name);
    EXPECT_EQ("name", schema->Fields()[1].name);
    EXPECT_EQ("age", schema->Fields()[2].name);
    EXPECT_EQ("year", schema->Fields()[3].name);
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
//      List data files for an existing table with partitioned data
// Test Result:
//      Should return all data files with correct partition values and record counts
//
TEST_F(DataAccessorTest, ListTableFiles) {
    auto files_result = data_accessor_->ListTableFiles("test_table");
    VERIFY_RESULT(files_result);

    auto files = files_result.value();
    ASSERT_EQ(3, files.size());

    // Verify files have correct partition values and record counts
    bool found_partition1 = false;  // age_bucket=1, year=2023
    bool found_partition2 = false;  // age_bucket=2, year=2023
    bool found_partition3 = false;  // age_bucket=1, year=2024

    for (const auto& file : files) {
        const auto& partition_values = file.partition_values;
        if (partition_values.at("age_bucket") == "1" && partition_values.at("year") == "2023") {
            EXPECT_EQ(3, file.record_count);  // First partition has 3 records
            found_partition1 = true;
        } else if (partition_values.at("age_bucket") == "2" && partition_values.at("year") == "2023") {
            EXPECT_EQ(2, file.record_count);  // Second partition has 2 records
            found_partition2 = true;
        } else if (partition_values.at("age_bucket") == "1" && partition_values.at("year") == "2024") {
            EXPECT_EQ(2, file.record_count);  // Third partition has 2 records
            found_partition3 = true;
        }
    }

    EXPECT_TRUE(found_partition1);
    EXPECT_TRUE(found_partition2);
    EXPECT_TRUE(found_partition3);
}

//
// Test Setup:
//      Get reader for a data file and read its contents
// Test Result:
//      Should successfully read the data with correct values and partition values
//
TEST_F(DataAccessorTest, GetReaderAndReadData) {
    // Get the files
    auto files_result = data_accessor_->ListTableFiles("test_table");
    VERIFY_RESULT(files_result);
    ASSERT_FALSE(files_result.value().empty());

    // Get a reader for the first partition (age_bucket=1, year=2023)
    const auto& first_file = files_result.value()[0];
    LOG_VERBOSE("Reading file: %s", first_file.file_path.c_str());
    auto reader_result = data_accessor_->GetReader(first_file);
    VERIFY_RESULT(reader_result);
    auto reader = std::move(reader_result).value();

    // Read the data
    auto table_result = reader->Read();
    VERIFY_RESULT(table_result);
    auto table = table_result.value();

    // Verify the data
    ASSERT_EQ(4, table->num_columns());  // id, name, age, year
    ASSERT_EQ(3, table->num_rows());     // First partition has 3 records

    // Verify schema
    auto schema = table->schema();
    ASSERT_EQ("id", schema->field(0)->name());
    ASSERT_EQ("name", schema->field(1)->name());
    ASSERT_EQ("age", schema->field(2)->name());
    ASSERT_EQ("year", schema->field(3)->name());

    // Verify partition values
    EXPECT_EQ("1", first_file.partition_values.at("age_bucket"));
    EXPECT_EQ("2023", first_file.partition_values.at("year"));
}

//
// Test Setup:
//      List data files with specific snapshot ID after data ingestion
// Test Result:
//      Should return files from that snapshot with correct partition values and record counts
//
TEST_F(DataAccessorTest, ListTableFilesWithSnapshot) {
    // First snapshot contains our test data
    auto files_result = data_accessor_->ListTableFiles("test_table", 1);
    VERIFY_RESULT(files_result);
    ASSERT_EQ(1, files_result.value().size());

    // Verify record counts and partition values in first snapshot
    int total_records = 0;
    for (const auto& file : files_result.value()) {
        total_records += file.record_count;
        // Verify partition values are present
        EXPECT_TRUE(file.partition_values.find("age_bucket") != file.partition_values.end());
        EXPECT_TRUE(file.partition_values.find("year") != file.partition_values.end());
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
    EXPECT_EQ(5, total_records);  // Total records in second snapshot files

    files_result = data_accessor_->ListTableFiles("test_table", 3);
    VERIFY_RESULT(files_result);
    ASSERT_EQ(3, files_result.value().size());

    // Verify record counts in third snapshot
    total_records = 0;
    for (const auto& file : files_result.value()) {
        total_records += file.record_count;
    }
    EXPECT_EQ(7, total_records);  // Total records in third snapshot files
}

//
// Test Setup:
//      Create a large volume of data (thousands of rows) and ingest it in batches
//      Data is partitioned by age_bucket and year to test partitioning at scale
// Test Result:
//      All data should be correctly ingested and retrievable
//      File count, record counts, and partition values should match expectations
//
TEST_F(DataAccessorTest, DataIngestorStressTest) {
    // Create a new table for stress testing
    auto create_result = catalog_->CreateTable("stress_test_table", schema_, spec_, "/stress_test_table");
    VERIFY_RESULT(create_result);

    // Create a new DataIngestor for stress testing
    auto ingestor_result = catalog::DataIngestor::Create(catalog_, fs_, "stress_test_table");
    VERIFY_RESULT(ingestor_result);
    auto ingestor = std::move(ingestor_result).value();

    // Configuration for stress test
    const int kNumBatches = 10;
    const int kRowsPerBatch = 500;  // 5,000 rows total
    const int kYearStart = 2020;
    const int kYearEnd = 2023;
    const int kMaxAge = 80;

    // Keep track of expected counts per partition
    std::unordered_map<std::string, std::unordered_map<std::string, int>> expected_partition_counts;
    int total_rows = 0;

    // Generate and ingest batches
    LOG_STATUS("Starting stress test with %d batches of %d rows each", kNumBatches, kRowsPerBatch);
    for (int batch_idx = 0; batch_idx < kNumBatches; batch_idx++) {
        std::vector<int32_t> ids;
        std::vector<std::string> names;
        std::vector<int32_t> ages;
        std::vector<int32_t> years;

        // Generate data for this batch
        for (int row_idx = 0; row_idx < kRowsPerBatch; row_idx++) {
            int id = batch_idx * kRowsPerBatch + row_idx + 1;
            std::string name = "User" + std::to_string(id);
            int age = id % kMaxAge + 1;  // Age from 1 to kMaxAge
            int year = kYearStart + (id % (kYearEnd - kYearStart + 1));

            ids.push_back(id);
            names.push_back(name);
            ages.push_back(age);
            years.push_back(year);

            // Calculate expected partition
            int age_bucket = age % 10;  // Using modulo 10 to match partition spec
            std::string bucket_str = std::to_string(age_bucket);
            std::string year_str = std::to_string(year);

            // Update expected counts
            expected_partition_counts[bucket_str][year_str]++;
            total_rows++;
        }

        // Create Arrow arrays
        arrow::Int32Builder id_builder;
        arrow::StringBuilder name_builder;
        arrow::Int32Builder age_builder;
        arrow::Int32Builder year_builder;

        EXPECT_TRUE(id_builder.AppendValues(ids).ok());
        EXPECT_TRUE(name_builder.AppendValues(names).ok());
        EXPECT_TRUE(age_builder.AppendValues(ages).ok());
        EXPECT_TRUE(year_builder.AppendValues(years).ok());

        std::shared_ptr<arrow::Array> id_array, name_array, age_array, year_array;
        EXPECT_TRUE(id_builder.Finish(&id_array).ok());
        EXPECT_TRUE(name_builder.Finish(&name_array).ok());
        EXPECT_TRUE(age_builder.Finish(&age_array).ok());
        EXPECT_TRUE(year_builder.Finish(&year_array).ok());

        // Create record batch
        auto schema = arrow::schema({arrow::field("id", arrow::int32()),
                                     arrow::field("name", arrow::utf8()),
                                     arrow::field("age", arrow::int32()),
                                     arrow::field("year", arrow::int32())});
        auto batch = arrow::RecordBatch::Make(schema, ids.size(), {id_array, name_array, age_array, year_array});

        // Ingest batch without committing yet
        auto ingest_result = ingestor->IngestBatch(batch, false);
        VERIFY_RESULT(ingest_result);

        LOG_STATUS("Ingested batch %d/%d with %d rows", batch_idx + 1, kNumBatches, kRowsPerBatch);
    }

    // Commit all changes at once
    auto commit_result = ingestor->Commit();
    VERIFY_RESULT(commit_result);
    LOG_STATUS("Committed all changes");

    // Now verify using data accessor
    auto files_result = data_accessor_->ListTableFiles("stress_test_table");
    VERIFY_RESULT(files_result);
    auto files = files_result.value();

    // Estimate how many files we should have based on distinct partitions
    size_t expected_partition_count = 0;
    for (const auto& bucket_entry : expected_partition_counts) {
        expected_partition_count += bucket_entry.second.size();
    }

    LOG_STATUS("Found %zu data files across %zu partitions", files.size(), expected_partition_count);
    EXPECT_GE(files.size(), expected_partition_count) << "Should have at least as many files as distinct partitions";

    // Verify total record count
    int total_records = 0;
    for (const auto& file : files) {
        total_records += file.record_count;
    }
    EXPECT_EQ(total_rows, total_records) << "Total records should match ingested row count";

    // Verify reading from a random file
    if (!files.empty()) {
        const auto& random_file = files[files.size() / 2];  // Pick middle file
        LOG_STATUS("Reading file: %s", random_file.file_path.c_str());
        auto reader_result = data_accessor_->GetReader(random_file);
        VERIFY_RESULT(reader_result);
        auto reader = std::move(reader_result).value();

        // Read the data
        auto table_result = reader->Read();
        VERIFY_RESULT(table_result);
        auto table = table_result.value();

        // Verify the data has expected schema
        ASSERT_EQ(4, table->num_columns());  // id, name, age, year
        ASSERT_GT(table->num_rows(), 0);     // Should have at least 1 row

        // Verify schema
        auto schema = table->schema();
        ASSERT_EQ("id", schema->field(0)->name());
        ASSERT_EQ("name", schema->field(1)->name());
        ASSERT_EQ("age", schema->field(2)->name());
        ASSERT_EQ("year", schema->field(3)->name());
    }

    LOG_STATUS("Stress test completed successfully with %d total rows", total_rows);
}

}  // namespace pond::query