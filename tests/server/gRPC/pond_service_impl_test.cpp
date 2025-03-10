#include "server/gRPC/pond_service_impl.h"

#include <chrono>
#include <memory>
#include <string>
#include <thread>

#include <gtest/gtest.h>

#include "catalog/data_ingestor.h"
#include "catalog/kv_catalog.h"
#include "catalog/metadata.h"
#include "common/memory_append_only_fs.h"
#include "common/result.h"
#include "common/schema.h"
#include "kv/db.h"

namespace pond::server {

class PondServiceImplTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create a memory file system
        fs_ = std::make_shared<common::MemoryAppendOnlyFileSystem>();

        // Create a real DB with the memory file system
        auto db_result = pond::kv::DB::Create(fs_, "test_db");
        ASSERT_TRUE(db_result.ok()) << "Failed to create DB: " << db_result.error().message();
        db_ = db_result.value();

        // Create the service implementation
        service_ = std::make_unique<PondServiceImpl>(db_, fs_);

        // The PondServiceImpl constructor should create a catalog_db_ and catalog_
        // Wait a short time to ensure everything is initialized
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        // Create a test table
        auto catalog = service_->catalog_;  // Direct access via friend relationship
        ASSERT_TRUE(catalog != nullptr) << "Catalog is null";

        auto create_result = catalog->CreateTable("test_table",
                                                  CreateTestSchema(),
                                                  {},  // empty partition spec
                                                  "/tables/test_table");

        ASSERT_TRUE(create_result.ok()) << "Failed to create test table: " << create_result.error().message();
    }

    // Helper to set up a simple test schema
    std::shared_ptr<common::Schema> CreateTestSchema() {
        return std::make_shared<common::Schema>(std::vector<common::ColumnSchema>{
            {"int_col", common::ColumnType::INT32}, {"string_col", common::ColumnType::STRING}});
    }

    std::shared_ptr<common::MemoryAppendOnlyFileSystem> fs_;
    std::shared_ptr<pond::kv::DB> db_;
    std::unique_ptr<PondServiceImpl> service_;
};

//
// Test Setup:
//      Test the successful ingestion of JSON data
// Test Result:
//      Should return success with the correct number of rows ingested
//
TEST_F(PondServiceImplTest, IngestJsonData_Success) {
    // Set up request and response objects
    grpc::ServerContext context;
    pond::proto::IngestJsonDataRequest request;
    pond::proto::IngestJsonDataResponse response;

    // Set up the request
    request.set_table_name("test_table");
    request.set_json_data(R"([
        {"int_col": 1, "string_col": "value1"},
        {"int_col": 2, "string_col": "value2"},
        {"int_col": 3, "string_col": "value3"}
    ])");

    // Call the method
    auto status = service_->IngestJsonData(&context, &request, &response);

    // Verify results
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(response.success()) << "Error: " << response.error_message();
    EXPECT_EQ(3, response.rows_ingested());
    EXPECT_TRUE(response.error_message().empty());

    // Verify data files were created by checking table snapshots
    auto catalog = service_->catalog_;  // Direct access via friend relationship
    auto table_result = catalog->LoadTable("test_table");
    ASSERT_TRUE(table_result.ok()) << "Failed to load table: " << table_result.error().message();

    // Get data files in the latest snapshot
    auto files_result = catalog->ListDataFiles("test_table");
    ASSERT_TRUE(files_result.ok()) << "Failed to list data files: " << files_result.error().message();
    EXPECT_FALSE(files_result.value().empty()) << "Expected at least one data file";

    // Check record count matches
    int total_records = 0;
    for (const auto& file : files_result.value()) {
        total_records += file.record_count;
    }
    EXPECT_EQ(3, total_records) << "Expected 3 total records";
}

//
// Test Setup:
//      Test ingestion when the table is not found
// Test Result:
//      Should return failure with an appropriate error message
//
TEST_F(PondServiceImplTest, IngestJsonData_TableNotFound) {
    // Set up request and response objects
    grpc::ServerContext context;
    pond::proto::IngestJsonDataRequest request;
    pond::proto::IngestJsonDataResponse response;

    // Set up the request with a non-existent table
    request.set_table_name("nonexistent_table");
    request.set_json_data(R"([{"int_col": 1, "string_col": "test"}])");

    // Call the method
    auto status = service_->IngestJsonData(&context, &request, &response);

    // Verify results
    EXPECT_TRUE(status.ok());          // gRPC call itself succeeds
    EXPECT_FALSE(response.success());  // But the operation fails
    EXPECT_TRUE(response.error_message().find("Failed to load table") != std::string::npos)
        << "Error message doesn't contain expected text. Got: " << response.error_message();
}

//
// Test Setup:
//      Test ingestion with invalid JSON data
// Test Result:
//      Should return failure with an error about invalid JSON
//
TEST_F(PondServiceImplTest, IngestJsonData_InvalidJson) {
    // Set up request and response objects
    grpc::ServerContext context;
    pond::proto::IngestJsonDataRequest request;
    pond::proto::IngestJsonDataResponse response;

    // Set up the request with invalid JSON
    request.set_table_name("test_table");
    request.set_json_data(R"({"this is not valid json":)");  // Missing closing brace

    // Call the method
    auto status = service_->IngestJsonData(&context, &request, &response);

    // Verify results
    EXPECT_TRUE(status.ok());          // gRPC call itself succeeds
    EXPECT_FALSE(response.success());  // But the operation fails
    EXPECT_TRUE(response.error_message().find("JSON") != std::string::npos)
        << "Error message doesn't contain expected text. Got: " << response.error_message();
}

//
// Test Setup:
//      Test ingestion with JSON data that doesn't match the schema
// Test Result:
//      Should return failure with an error about schema mismatch
//
TEST_F(PondServiceImplTest, IngestJsonData_SchemaTypeMismatch) {
    // Set up request and response objects
    grpc::ServerContext context;
    pond::proto::IngestJsonDataRequest request;
    pond::proto::IngestJsonDataResponse response;

    // Set up the request with type mismatch (string instead of int)
    request.set_table_name("test_table");
    request.set_json_data(R"([{"int_col": "not_an_int", "string_col": "test"}])");

    // Call the method
    auto status = service_->IngestJsonData(&context, &request, &response);

    // Verify results
    EXPECT_TRUE(status.ok());          // gRPC call itself succeeds
    EXPECT_FALSE(response.success());  // But the operation fails
    EXPECT_TRUE(response.error_message().find("not an integer") != std::string::npos)
        << "Error message doesn't contain expected text. Got: " << response.error_message();
}

//
// Test Setup:
//      Test ingestion with an empty JSON array
// Test Result:
//      Should return success with 0 rows ingested
//
TEST_F(PondServiceImplTest, IngestJsonData_EmptyJsonArray) {
    // Set up request and response objects
    grpc::ServerContext context;
    pond::proto::IngestJsonDataRequest request;
    pond::proto::IngestJsonDataResponse response;

    // Set up the request with an empty JSON array
    request.set_table_name("test_table");
    request.set_json_data("[]");

    // Call the method
    auto status = service_->IngestJsonData(&context, &request, &response);

    // Verify results
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(response.success()) << "Error: " << response.error_message();
    EXPECT_EQ(0, response.rows_ingested());

    // No files should be created for empty data
    auto catalog = service_->catalog_;  // Direct access via friend relationship
    auto files_result = catalog->ListDataFiles("test_table");
    ASSERT_TRUE(files_result.ok()) << "Failed to list data files: " << files_result.error().message();
    // Since DataIngestor handles this, we don't know if it will create empty files or not
    // Just make sure we don't have any records
    int total_records = 0;
    for (const auto& file : files_result.value()) {
        total_records += file.record_count;
    }
    EXPECT_EQ(0, total_records) << "Expected no records for empty JSON array";
}

//
// Test Setup:
//      Test ingesting multiple batches to the same table
// Test Result:
//      Should return success for both ingestions and properly accumulate files
//
TEST_F(PondServiceImplTest, IngestJsonData_MultipleBatches) {
    // First batch
    {
        grpc::ServerContext context;
        pond::proto::IngestJsonDataRequest request;
        pond::proto::IngestJsonDataResponse response;

        request.set_table_name("test_table");
        request.set_json_data(R"([
            {"int_col": 1, "string_col": "first_batch_1"},
            {"int_col": 2, "string_col": "first_batch_2"}
        ])");

        auto status = service_->IngestJsonData(&context, &request, &response);

        EXPECT_TRUE(status.ok());
        EXPECT_TRUE(response.success()) << "Error: " << response.error_message();
        EXPECT_EQ(2, response.rows_ingested());
    }

    // Second batch
    {
        grpc::ServerContext context;
        pond::proto::IngestJsonDataRequest request;
        pond::proto::IngestJsonDataResponse response;

        request.set_table_name("test_table");
        request.set_json_data(R"([
            {"int_col": 3, "string_col": "second_batch_1"},
            {"int_col": 4, "string_col": "second_batch_2"},
            {"int_col": 5, "string_col": "second_batch_3"}
        ])");

        auto status = service_->IngestJsonData(&context, &request, &response);

        EXPECT_TRUE(status.ok());
        EXPECT_TRUE(response.success()) << "Error: " << response.error_message();
        EXPECT_EQ(3, response.rows_ingested());
    }

    // Verify records were added correctly
    auto catalog = service_->catalog_;  // Direct access via friend relationship
    auto files_result = catalog->ListDataFiles("test_table");
    ASSERT_TRUE(files_result.ok()) << "Failed to list data files: " << files_result.error().message();

    // Calculate total records - should be 5 from both batches
    int total_records = 0;
    for (const auto& file : files_result.value()) {
        total_records += file.record_count;
    }
    EXPECT_EQ(5, total_records) << "Expected 5 total records from both batches";
}

}  // namespace pond::server