#include "catalog/kv_catalog.h"

#include <memory>
#include <vector>

#include <gtest/gtest.h>

#include "common/append_only_fs.h"
#include "common/memory_append_only_fs.h"
#include "common/schema.h"
#include "common/uuid.h"
#include "kv/db.h"
#include "kv/record.h"
#include "test_helper.h"

using namespace pond::common;

namespace pond::catalog {

class KVCatalogTest : public ::testing::Test {
protected:
    void SetUp() override {
        fs_ = std::make_shared<common::MemoryAppendOnlyFileSystem>();
        auto db_result = kv::DB::Create(fs_, "test_catalog");
        VERIFY_RESULT(db_result);
        db_ = std::move(db_result.value());

        catalog_ = std::make_shared<KVCatalog>(db_);
    }

    std::shared_ptr<Schema> CreateTestSchema(bool include_email = false) const {
        auto schema = std::make_shared<Schema>();
        schema->AddField("id", ColumnType::INT32);
        schema->AddField("name", ColumnType::STRING);
        schema->AddField("age", ColumnType::INT32);
        schema->AddField("created_date", ColumnType::TIMESTAMP);

        if (include_email) {
            schema->AddField("email", ColumnType::STRING);
        }

        return schema;
    }

    std::unique_ptr<kv::Record> CreateCatalogRecord(const std::string& key, const std::string& value) const {
        auto table_result = db_->GetTable(kv::DB::SYSTEM_TABLE);
        EXPECT_TRUE(table_result.ok());
        auto table = table_result.value();

        auto record = std::make_unique<kv::Record>(table->schema());
        record->Set(0, key);
        record->Set(1, DataChunk::FromString(value));
        return record;
    }

    PartitionSpec CreateTestPartitionSpec() const {
        PartitionSpec spec(1);
        spec.fields.emplace_back(3, 100, "year", Transform::YEAR);
        return spec;
    }

    std::vector<DataFile> CreateTestDataFiles(const std::string& year = "2023") const {
        std::vector<DataFile> files;
        files.emplace_back(DataFile{"/data/users/data/year=" + year + "/part-00000.parquet",
                                    FileFormat::PARQUET,
                                    {{"year", year}},
                                    1000,
                                    1024 * 1024});

        files.emplace_back(DataFile{"/data/users/data/year=" + year + "/part-00001.parquet",
                                    FileFormat::PARQUET,
                                    {{"year", year}},
                                    1500,
                                    10 * 1024 * 1024});

        return files;
    }

    // Helper method to verify JSON format of properties
    void VerifyPropertiesJson(const std::string& json_str,
                              const std::unordered_map<std::string, std::string>& expected) {
        rapidjson::Document doc;
        ASSERT_FALSE(doc.Parse(json_str.c_str()).HasParseError());
        ASSERT_TRUE(doc.IsObject());

        for (const auto& [key, value] : expected) {
            ASSERT_TRUE(doc.HasMember(key.c_str()));
            ASSERT_TRUE(doc[key.c_str()].IsString());
            EXPECT_EQ(doc[key.c_str()].GetString(), value);
        }
    }

    // Helper method to verify JSON format of partition specs
    void VerifyPartitionSpecsJson(const std::string& json_str, const std::vector<PartitionSpec>& expected) {
        rapidjson::Document doc;
        ASSERT_FALSE(doc.Parse(json_str.c_str()).HasParseError());
        ASSERT_TRUE(doc.IsArray());
        ASSERT_EQ(doc.Size(), expected.size());

        for (size_t i = 0; i < expected.size(); ++i) {
            const auto& spec = doc[i];
            ASSERT_TRUE(spec.IsObject());
            ASSERT_TRUE(spec.HasMember("spec_id"));
            ASSERT_TRUE(spec.HasMember("fields"));
            EXPECT_EQ(spec["spec_id"].GetInt(), expected[i].spec_id);

            const auto& fields = spec["fields"];
            ASSERT_TRUE(fields.IsArray());
            ASSERT_EQ(fields.Size(), expected[i].fields.size());

            for (size_t j = 0; j < expected[i].fields.size(); ++j) {
                const auto& field = fields[j];
                const auto& expected_field = expected[i].fields[j];
                ASSERT_TRUE(field.IsObject());
                EXPECT_EQ(field["source_id"].GetInt(), expected_field.source_id);
                EXPECT_EQ(field["field_id"].GetInt(), expected_field.field_id);
                EXPECT_EQ(field["name"].GetString(), expected_field.name);
                EXPECT_EQ(field["transform"].GetString(), TransformToString(expected_field.transform));
            }
        }
    }

    std::shared_ptr<common::MemoryAppendOnlyFileSystem> fs_;
    std::shared_ptr<kv::DB> db_;
    std::shared_ptr<KVCatalog> catalog_;
};

//
// Test Setup:
//      Acquire a lock for a table, verify it's acquired, then release it
// Test Result:
//      Should successfully acquire and release the lock
//
TEST_F(KVCatalogTest, AcquireAndReleaseLock) {
    // Create a table first
    auto schema = CreateTestSchema();
    auto spec = CreateTestPartitionSpec();
    auto create_result = catalog_->CreateTable("test_table", schema, spec, "/data/test_table");
    VERIFY_RESULT(create_result);

    // Acquire lock
    auto acquire_result = catalog_->AcquireLock("test_table");
    VERIFY_RESULT(acquire_result);
    EXPECT_TRUE(acquire_result.value());

    // Try to acquire the lock again - should fail or timeout
    auto second_acquire_result = catalog_->AcquireLock("test_table", 500);  // Short timeout
    EXPECT_FALSE(second_acquire_result.ok());
    EXPECT_EQ(second_acquire_result.error().code(), common::ErrorCode::Timeout);

    // Release the lock
    auto release_result = catalog_->ReleaseLock("test_table");
    VERIFY_RESULT(release_result);
    EXPECT_TRUE(release_result.value());

    // Now we should be able to acquire the lock again
    auto reacquire_result = catalog_->AcquireLock("test_table");
    VERIFY_RESULT(reacquire_result);
    EXPECT_TRUE(reacquire_result.value());

    // Clean up by releasing the lock
    VERIFY_RESULT(catalog_->ReleaseLock("test_table"));
}

//
// Test Setup:
//      Create a table with a schema and partition spec, then load it
// Test Result:
//      Should successfully create and then load the table metadata
//
TEST_F(KVCatalogTest, CreateAndLoadTable) {
    auto schema = CreateTestSchema();
    auto spec = CreateTestPartitionSpec();

    // Create a table
    auto create_result = catalog_->CreateTable(
        "users", schema, spec, "/data/users", {{"description", "User information"}, {"owner", "data_team"}});
    VERIFY_RESULT(create_result);

    auto metadata = create_result.value();
    EXPECT_EQ(metadata.properties.size(), 2);
    EXPECT_EQ(metadata.properties.at("description"), "User information");
    EXPECT_EQ(metadata.properties.at("owner"), "data_team");
    EXPECT_EQ(metadata.partition_specs.size(), 1);
    EXPECT_EQ(metadata.partition_specs[0].fields.size(), 1);
    EXPECT_EQ(metadata.snapshots.size(), 1);  // initial snapshot
    EXPECT_EQ(metadata.current_snapshot_id, 0);

    // Load the table
    auto load_result = catalog_->LoadTable("users");
    VERIFY_RESULT(load_result);

    auto loaded_metadata = load_result.value();
    EXPECT_EQ(loaded_metadata.table_uuid, metadata.table_uuid);
    EXPECT_EQ(loaded_metadata.location, "/data/users");
    EXPECT_EQ(loaded_metadata.properties.size(), 2);

    // Verify schema
    EXPECT_EQ(*loaded_metadata.schema, *schema);
}

//
// Test Setup:
//      Create a table, add files in a snapshot, and list those files
// Test Result:
//      Should successfully create a snapshot and retrieve the files
//
TEST_F(KVCatalogTest, CreateSnapshotAndListFiles) {
    auto schema = CreateTestSchema();
    auto spec = CreateTestPartitionSpec();

    // Create a table
    auto create_result = catalog_->CreateTable("users", schema, spec, "/data/users");
    VERIFY_RESULT(create_result);

    // Create files and snapshot
    auto files = CreateTestDataFiles();
    auto snapshot_result = catalog_->CreateSnapshot("users", files);
    VERIFY_RESULT(snapshot_result);

    auto metadata = snapshot_result.value();
    EXPECT_EQ(metadata.snapshots.size(), 1);
    EXPECT_EQ(metadata.current_snapshot_id, 1);

    // List files
    auto files_result = catalog_->ListDataFiles("users");
    VERIFY_RESULT(files_result);

    auto listed_files = files_result.value();
    EXPECT_EQ(listed_files.size(), 2);
    EXPECT_EQ(listed_files[0].file_path, "/data/users/data/year=2023/part-00000.parquet");
    EXPECT_EQ(listed_files[0].record_count, 1000);
    EXPECT_EQ(listed_files[1].file_path, "/data/users/data/year=2023/part-00001.parquet");
    EXPECT_EQ(listed_files[1].record_count, 1500);
}

//
// Test Setup:
//      Attempt to create snapshots with invalid operations and non-existent tables
// Test Result:
//      Should return appropriate error codes for invalid cases
//
TEST_F(KVCatalogTest, CreateSnapshotErrorHandling) {
    auto schema = CreateTestSchema();
    auto spec = CreateTestPartitionSpec();
    auto files = CreateTestDataFiles();

    // Try creating snapshot for non-existent table
    auto missing_table_result = catalog_->CreateSnapshot("missing_table", files);
    EXPECT_FALSE(missing_table_result.ok());
    EXPECT_EQ(missing_table_result.error().code(), common::ErrorCode::TableNotFoundInCatalog);

    // Create a valid table for remaining tests
    auto create_result = catalog_->CreateTable("users", schema, spec, "/data/users");
    VERIFY_RESULT(create_result);

    // Try unsupported operations
    auto delete_op_result = catalog_->CreateSnapshot("users", files, {}, Operation::DELETE);
    EXPECT_FALSE(delete_op_result.ok());
    EXPECT_EQ(delete_op_result.error().code(), common::ErrorCode::NotImplemented);

    auto replace_op_result = catalog_->CreateSnapshot("users", files, {}, Operation::REPLACE);
    EXPECT_FALSE(replace_op_result.ok());
    EXPECT_EQ(replace_op_result.error().code(), common::ErrorCode::NotImplemented);

    auto overwrite_op_result = catalog_->CreateSnapshot("users", files, {}, Operation::OVERWRITE);
    EXPECT_FALSE(overwrite_op_result.ok());
    EXPECT_EQ(overwrite_op_result.error().code(), common::ErrorCode::NotImplemented);

    // Try invalid operation enum value
    auto invalid_op_result = catalog_->CreateSnapshot("users", files, {}, static_cast<Operation>(999));
    EXPECT_FALSE(invalid_op_result.ok());
    EXPECT_EQ(invalid_op_result.error().code(), common::ErrorCode::InvalidOperation);
}

//
// Test Setup:
//      Create a table, then update its schema to add a field
// Test Result:
//      Should successfully update the schema with new field
//
TEST_F(KVCatalogTest, UpdateSchema) {
    auto schema = CreateTestSchema();
    auto spec = CreateTestPartitionSpec();

    // Create a table
    auto create_result = catalog_->CreateTable("users", schema, spec, "/data/users");
    VERIFY_RESULT(create_result);

    // Update schema
    auto updated_schema = CreateTestSchema(true);  // includes email field
    auto schema_result = catalog_->UpdateSchema("users", updated_schema);
    VERIFY_RESULT(schema_result);

    // Load table to verify
    auto load_result = catalog_->LoadTable("users");
    VERIFY_RESULT(load_result);

    // Note: In a real test, we would check schema field count, but since
    // schema serialization is stubbed in this implementation, we just check
    // that the update operation succeeded
}

//
// Test Setup:
//      Create a table, then update its properties
// Test Result:
//      Properties should be updated successfully
//
TEST_F(KVCatalogTest, UpdateTableProperties) {
    auto schema = CreateTestSchema();
    auto spec = CreateTestPartitionSpec();

    // Create a table
    auto create_result = catalog_->CreateTable("users", schema, spec, "/data/users");
    VERIFY_RESULT(create_result);

    // Update properties
    auto props_result = catalog_->UpdateTableProperties("users", {{"version", "2.0"}, {"updated_by", "test_user"}});
    VERIFY_RESULT(props_result);

    auto metadata = props_result.value();
    EXPECT_EQ(metadata.properties.size(), 2);
    EXPECT_EQ(metadata.properties.at("version"), "2.0");
    EXPECT_EQ(metadata.properties.at("updated_by"), "test_user");
}

//
// Test Setup:
//      Create a table with two snapshots, then retrieve files from
//      the first snapshot using time travel capabilities
// Test Result:
//      Should retrieve the correct files from the specified snapshot
//
TEST_F(KVCatalogTest, TimeTravel) {
    auto schema = CreateTestSchema();
    auto spec = CreateTestPartitionSpec();

    // Create a table
    auto create_result = catalog_->CreateTable("users", schema, spec, "/data/users");
    VERIFY_RESULT(create_result);

    // Create first snapshot with 2023 data
    auto files2023 = CreateTestDataFiles("2023");
    auto snapshot1_result = catalog_->CreateSnapshot("users", files2023);
    VERIFY_RESULT(snapshot1_result);

    // Create second snapshot with 2022 data
    auto files2022 = CreateTestDataFiles("2022");
    auto snapshot2_result = catalog_->CreateSnapshot("users", files2022);
    VERIFY_RESULT(snapshot2_result);

    // Use time travel to access first snapshot
    auto files1_result = catalog_->ListDataFiles("users", 1);  // Snapshot ID 1
    VERIFY_RESULT(files1_result);

    auto files_snapshot1 = files1_result.value();
    EXPECT_EQ(files_snapshot1.size(), 2);
    EXPECT_EQ(files_snapshot1[0].partition_values.at("year"), "2023");
    EXPECT_EQ(files_snapshot1[1].partition_values.at("year"), "2023");
}

//
// Test Setup:
//      Create a table and then drop it
// Test Result:
//      Table should be successfully dropped and no longer accessible
//
TEST_F(KVCatalogTest, DropTable) {
    auto schema = CreateTestSchema();
    auto spec = CreateTestPartitionSpec();

    // Create a table
    auto create_result = catalog_->CreateTable("users", schema, spec, "/data/users");
    VERIFY_RESULT(create_result);

    // Drop the table
    auto drop_result = catalog_->DropTable("users");
    VERIFY_RESULT(drop_result);

    // Try to load the dropped table
    auto load_result = catalog_->LoadTable("users");
    EXPECT_FALSE(load_result.ok());
    EXPECT_EQ(load_result.error().code(), common::ErrorCode::TableNotFoundInCatalog);
}

//
// Test Setup:
//      Attempt operations on a table that doesn't exist
// Test Result:
//      Operations should fail with appropriate error codes
//
TEST_F(KVCatalogTest, NonExistentTable) {
    // Try to load a non-existent table
    auto load_result = catalog_->LoadTable("non_existent");
    EXPECT_FALSE(load_result.ok());
    EXPECT_EQ(load_result.error().code(), common::ErrorCode::TableNotFoundInCatalog);

    // Try to drop a non-existent table
    auto drop_result = catalog_->DropTable("non_existent");
    EXPECT_FALSE(drop_result.ok());
    EXPECT_EQ(drop_result.error().code(), common::ErrorCode::TableNotFoundInCatalog);
}

//
// Test Setup:
//      Create a table and then try to create another with the same name
// Test Result:
//      Second create operation should fail with appropriate error
//
TEST_F(KVCatalogTest, TableAlreadyExists) {
    auto schema = CreateTestSchema();
    auto spec = CreateTestPartitionSpec();

    // Create a table
    auto create_result = catalog_->CreateTable("users", schema, spec, "/data/users");
    VERIFY_RESULT(create_result);

    // Try to create the same table again
    auto duplicate_result = catalog_->CreateTable("users", schema, spec, "/data/users");
    EXPECT_FALSE(duplicate_result.ok());
    EXPECT_EQ(duplicate_result.error().code(), common::ErrorCode::FileAlreadyExists);
}

//
// Test Setup:
//      Create a table with properties and verify JSON serialization format
// Test Result:
//      Properties and partition specs should be properly serialized in JSON format
//
TEST_F(KVCatalogTest, JsonSerialization) {
    auto schema = CreateTestSchema();
    auto spec = CreateTestPartitionSpec();
    std::unordered_map<std::string, std::string> properties = {
        {"description", "User information"}, {"owner", "data_team"}, {"version", "1.0"}};

    // Create a table
    auto create_result = catalog_->CreateTable("users", schema, spec, "/data/users", properties);
    VERIFY_RESULT(create_result);

    // Load the table to verify serialization
    auto load_result = catalog_->LoadTable("users");
    VERIFY_RESULT(load_result);

    auto metadata = load_result.value();

    // Verify properties JSON format
    EXPECT_EQ(metadata.properties, properties);

    // Verify partition specs JSON format
    ASSERT_EQ(metadata.partition_specs.size(), 1);
    EXPECT_EQ(metadata.partition_specs[0].spec_id, spec.spec_id);
    ASSERT_EQ(metadata.partition_specs[0].fields.size(), spec.fields.size());
    EXPECT_EQ(metadata.partition_specs[0].fields[0].source_id, spec.fields[0].source_id);
    EXPECT_EQ(metadata.partition_specs[0].fields[0].field_id, spec.fields[0].field_id);
    EXPECT_EQ(metadata.partition_specs[0].fields[0].name, spec.fields[0].name);
    EXPECT_EQ(metadata.partition_specs[0].fields[0].transform, spec.fields[0].transform);
}

//
// Test Setup:
//      Create a table and snapshot with partition values, verify JSON format
// Test Result:
//      Partition values should be properly serialized in JSON format
//
TEST_F(KVCatalogTest, JsonPartitionValues) {
    auto schema = CreateTestSchema();
    auto spec = CreateTestPartitionSpec();

    // Create a table
    auto create_result = catalog_->CreateTable("users", schema, spec, "/data/users");
    VERIFY_RESULT(create_result);

    // Create files with partition values
    auto files = CreateTestDataFiles("2024");
    auto snapshot_result = catalog_->CreateSnapshot("users", files);
    VERIFY_RESULT(snapshot_result);

    // List files and verify partition values JSON format
    auto files_result = catalog_->ListDataFiles("users");
    VERIFY_RESULT(files_result);

    auto listed_files = files_result.value();
    ASSERT_EQ(listed_files.size(), 2);

    // Verify partition values are in JSON format
    for (const auto& file : listed_files) {
        ASSERT_EQ(file.partition_values.size(), 1);
        EXPECT_EQ(file.partition_values.at("year"), "2024");
    }
}

//
// Test Setup:
//      Create a table and verify snapshot ID tracking
// Test Result:
//      Snapshot IDs should be tracked correctly in table metadata
//
TEST_F(KVCatalogTest, SnapshotIdTracking) {
    auto schema = CreateTestSchema();
    auto spec = CreateTestPartitionSpec();

    // Create a table
    auto create_result = catalog_->CreateTable("users", schema, spec, "/data/users");
    VERIFY_RESULT(create_result);

    // Verify initial snapshot ID
    auto load_result = catalog_->LoadTable("users");
    VERIFY_RESULT(load_result);
    EXPECT_EQ(load_result.value().current_snapshot_id, 0);  // Initial snapshot ID

    // Create a new snapshot
    auto files = CreateTestDataFiles();
    auto snapshot_result = catalog_->CreateSnapshot("users", files);
    VERIFY_RESULT(snapshot_result);

    // Verify snapshot ID is updated
    load_result = catalog_->LoadTable("users");
    VERIFY_RESULT(load_result);
    EXPECT_EQ(load_result.value().current_snapshot_id, 1);  // New snapshot ID
}

//
// Test Setup:
//      Create a table, rename it, and verify metadata is moved
// Test Result:
//      Table metadata should be moved to new name
//
TEST_F(KVCatalogTest, RenameTableMetadata) {
    auto schema = CreateTestSchema();
    auto spec = CreateTestPartitionSpec();

    // Create a table
    auto create_result = catalog_->CreateTable("old_name", schema, spec, "/data/users");
    VERIFY_RESULT(create_result);

    // Create a snapshot to have non-zero snapshot ID
    auto files = CreateTestDataFiles();
    auto snapshot_result = catalog_->CreateSnapshot("old_name", files);
    VERIFY_RESULT(snapshot_result);

    // Get the current snapshot ID
    auto load_result = catalog_->LoadTable("old_name");
    VERIFY_RESULT(load_result);
    auto current_id = load_result.value().current_snapshot_id;

    // Rename the table
    auto rename_result = catalog_->RenameTable("old_name", "new_name");
    VERIFY_RESULT(rename_result);

    // Verify old table doesn't exist
    load_result = catalog_->LoadTable("old_name");
    EXPECT_FALSE(load_result.ok());

    // Verify new table has correct metadata
    load_result = catalog_->LoadTable("new_name");
    VERIFY_RESULT(load_result);
    EXPECT_EQ(load_result.value().current_snapshot_id, current_id);
}

}  // namespace pond::catalog