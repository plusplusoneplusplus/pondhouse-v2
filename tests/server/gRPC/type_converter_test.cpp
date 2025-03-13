#include "server/gRPC/type_converter.h"

#include <gtest/gtest.h>

#include "catalog/metadata.h"
#include "common/schema.h"
#include "test_helper.h"

using namespace pond::common;
using namespace pond::catalog;

namespace pond::server {

class TypeConverterTest : public ::testing::Test {
protected:
    std::shared_ptr<Schema> CreateTestSchema() {
        return std::make_shared<Schema>(std::vector<ColumnSchema>{
            {"id", ColumnType::INT32},
            {"name", ColumnType::STRING},
            {"age", ColumnType::INT32},
            {"email", ColumnType::STRING, Nullability::NULLABLE}
        });
    }

    PartitionSpec CreateTestPartitionSpec() {
        PartitionSpec spec(1);  // spec_id = 1
        
        // Add identity transform
        PartitionField id_field(0, 1, "id", Transform::IDENTITY);
        spec.fields.push_back(std::move(id_field));
        
        // Add year transform with source column 3 (email)
        PartitionField year_field(3, 2, "year", Transform::YEAR);
        spec.fields.push_back(std::move(year_field));
        
        // Add bucket transform with 4 buckets
        PartitionField bucket_field(1, 3, "name_bucket", Transform::BUCKET);
        bucket_field.transform_param = 4;  // 4 buckets
        spec.fields.push_back(std::move(bucket_field));

        return spec;
    }

    std::vector<DataFile> CreateTestDataFiles() {
        std::vector<DataFile> files;
        
        // File 1
        std::unordered_map<std::string, std::string> partition_values1 = {
            {"id", "1"},
            {"year", "2024"},
            {"name_bucket", "2"}
        };
        files.emplace_back("data/part1.parquet", FileFormat::PARQUET, partition_values1, 100, 1024);
        
        // File 2
        std::unordered_map<std::string, std::string> partition_values2 = {
            {"id", "2"},
            {"year", "2023"},
            {"name_bucket", "1"}
        };
        files.emplace_back("data/part2.parquet", FileFormat::PARQUET, partition_values2, 150, 2048);
        
        return files;
    }
};

//
// Test Setup:
//      Create a TableMetadata object with schema, partition spec, and properties
// Test Result:
//      Verify all fields are correctly converted to protobuf format
//
TEST_F(TypeConverterTest, ConvertToTableMetadataInfo_Basic) {
    // Create test data
    auto schema = CreateTestSchema();
    auto partition_spec = CreateTestPartitionSpec();
    std::unordered_map<std::string, std::string> properties = {
        {"description", "Test table"},
        {"owner", "test_user"}
    };

    // Create table metadata
    TableMetadata metadata(
        "test-uuid",
        "test_table",
        "/test/location",
        schema,
        properties
    );
    metadata.last_updated_time = 1234567890;
    metadata.partition_specs.push_back(partition_spec);

    // Convert to protobuf
    auto pb_metadata = TypeConverter::ConvertToTableMetadataInfo(metadata);

    // Verify basic fields
    EXPECT_EQ(pb_metadata.name(), "test_table");
    EXPECT_EQ(pb_metadata.location(), "/test/location");
    EXPECT_EQ(pb_metadata.last_updated_time(), 1234567890);

    // Verify schema columns
    ASSERT_EQ(pb_metadata.columns_size(), 4);
    EXPECT_EQ(pb_metadata.columns(0).name(), "id");
    EXPECT_EQ(pb_metadata.columns(0).type(), "INT32");
    EXPECT_EQ(pb_metadata.columns(1).name(), "name");
    EXPECT_EQ(pb_metadata.columns(1).type(), "STRING");
    EXPECT_EQ(pb_metadata.columns(2).name(), "age");
    EXPECT_EQ(pb_metadata.columns(2).type(), "INT32");
    EXPECT_EQ(pb_metadata.columns(3).name(), "email");
    EXPECT_EQ(pb_metadata.columns(3).type(), "STRING");

    // Verify partition columns
    ASSERT_EQ(pb_metadata.partition_columns_size(), 3);
    EXPECT_EQ(pb_metadata.partition_columns(0), "id");
    EXPECT_EQ(pb_metadata.partition_columns(1), "year");
    EXPECT_EQ(pb_metadata.partition_columns(2), "name_bucket");

    // Verify partition spec
    ASSERT_TRUE(pb_metadata.has_partition_spec());
    const auto& pb_spec = pb_metadata.partition_spec();
    ASSERT_EQ(pb_spec.fields_size(), 3);

    // Check identity transform field
    const auto& id_field = pb_spec.fields(0);
    EXPECT_EQ(id_field.source_id(), 0);
    EXPECT_EQ(id_field.field_id(), 1);
    EXPECT_EQ(id_field.name(), "id");
    EXPECT_EQ(id_field.transform(), "identity");
    EXPECT_TRUE(id_field.transform_param().empty());

    // Check year transform field
    const auto& year_field = pb_spec.fields(1);
    EXPECT_EQ(year_field.source_id(), 3);
    EXPECT_EQ(year_field.field_id(), 2);
    EXPECT_EQ(year_field.name(), "year");
    EXPECT_EQ(year_field.transform(), "year");
    EXPECT_TRUE(year_field.transform_param().empty());

    // Check bucket transform field
    const auto& bucket_field = pb_spec.fields(2);
    EXPECT_EQ(bucket_field.source_id(), 1);
    EXPECT_EQ(bucket_field.field_id(), 3);
    EXPECT_EQ(bucket_field.name(), "name_bucket");
    EXPECT_EQ(bucket_field.transform(), "bucket");
    EXPECT_EQ(bucket_field.transform_param(), "4");

    // Verify properties
    ASSERT_EQ(pb_metadata.properties_size(), 2);
    EXPECT_EQ(pb_metadata.properties().at("description"), "Test table");
    EXPECT_EQ(pb_metadata.properties().at("owner"), "test_user");
}

//
// Test Setup:
//      Create TableMetadata with multiple partition specs and verify latest is used
// Test Result:
//      Verify the most recent partition spec is used in the conversion
//
TEST_F(TypeConverterTest, ConvertToTableMetadataInfo_MultipleSpecs) {
    auto schema = CreateTestSchema();
    auto first_spec = CreateTestPartitionSpec();
    
    // Create a second partition spec
    PartitionSpec second_spec(2);
    second_spec.fields.push_back(PartitionField(2, 1, "age", Transform::IDENTITY));
    
    TableMetadata metadata("test-uuid", "test_table", "/test/location", schema);
    metadata.partition_specs.push_back(first_spec);
    metadata.partition_specs.push_back(second_spec);

    auto pb_metadata = TypeConverter::ConvertToTableMetadataInfo(metadata);

    // Verify the latest spec (second_spec) is used
    ASSERT_EQ(pb_metadata.partition_columns_size(), 1);
    EXPECT_EQ(pb_metadata.partition_columns(0), "age");

    ASSERT_TRUE(pb_metadata.has_partition_spec());
    const auto& pb_spec = pb_metadata.partition_spec();
    ASSERT_EQ(pb_spec.fields_size(), 1);
    
    const auto& age_field = pb_spec.fields(0);
    EXPECT_EQ(age_field.source_id(), 2);
    EXPECT_EQ(age_field.field_id(), 1);
    EXPECT_EQ(age_field.name(), "age");
    EXPECT_EQ(age_field.transform(), "identity");
}

//
// Test Setup:
//      Create TableMetadata with data files and verify conversion
// Test Result:
//      Verify data files are correctly converted with all attributes
//
TEST_F(TypeConverterTest, AddDataFilesToTableMetadataInfo) {
    auto schema = CreateTestSchema();
    auto files = CreateTestDataFiles();
    
    TableMetadata metadata("test-uuid", "test_table", "/test/location", schema);
    auto pb_metadata = TypeConverter::ConvertToTableMetadataInfo(metadata);
    
    // Add data files
    TypeConverter::AddDataFilesToTableMetadataInfo(&pb_metadata, files);

    // Verify data files
    ASSERT_EQ(pb_metadata.data_files_size(), 2);

    // Check first file
    const auto& file1 = pb_metadata.data_files(0);
    EXPECT_EQ(file1.path(), "data/part1.parquet");
    EXPECT_EQ(file1.content_length(), 1024);
    EXPECT_EQ(file1.record_count(), 100);
    ASSERT_EQ(file1.partition_values_size(), 3);
    EXPECT_EQ(file1.partition_values().at("id"), "1");
    EXPECT_EQ(file1.partition_values().at("year"), "2024");
    EXPECT_EQ(file1.partition_values().at("name_bucket"), "2");

    // Check second file
    const auto& file2 = pb_metadata.data_files(1);
    EXPECT_EQ(file2.path(), "data/part2.parquet");
    EXPECT_EQ(file2.content_length(), 2048);
    EXPECT_EQ(file2.record_count(), 150);
    ASSERT_EQ(file2.partition_values_size(), 3);
    EXPECT_EQ(file2.partition_values().at("id"), "2");
    EXPECT_EQ(file2.partition_values().at("year"), "2023");
    EXPECT_EQ(file2.partition_values().at("name_bucket"), "1");
}

}  // namespace pond::server 