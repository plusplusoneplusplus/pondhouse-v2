#include "catalog/kv_catalog_util.h"

#include <memory>
#include <vector>

#include <gtest/gtest.h>
#include <rapidjson/document.h>

#include "catalog/metadata.h"
#include "catalog/types.h"
#include "common/uuid.h"
#include "test_helper.h"

using namespace pond::common;

namespace pond::catalog {

class KVCatalogUtilTest : public ::testing::Test {
protected:
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

    // Helper method to create test partition specs
    std::vector<PartitionSpec> CreateTestPartitionSpecs() {
        std::vector<PartitionSpec> specs;

        // First spec
        PartitionSpec spec1(1);
        spec1.fields.emplace_back(3, 100, "year", Transform::YEAR);
        specs.push_back(spec1);

        // Second spec with multiple fields
        PartitionSpec spec2(2);
        spec2.fields.emplace_back(3, 100, "year", Transform::YEAR);
        spec2.fields.emplace_back(4, 101, "month", Transform::MONTH);
        spec2.fields.emplace_back(5, 102, "day", Transform::DAY);
        specs.push_back(spec2);

        return specs;
    }

    // Helper method to create test snapshots
    std::vector<Snapshot> CreateTestSnapshots() {
        std::vector<Snapshot> snapshots;

        // First snapshot
        std::unordered_map<std::string, std::string> summary1 = {{"added-files", "10"}, {"total-records", "1000"}};
        Snapshot snapshot1(1, 1648176000000, Operation::APPEND, "metadata/manifests/snap-1.avro", summary1);
        snapshots.push_back(snapshot1);

        // Second snapshot with parent
        std::unordered_map<std::string, std::string> summary2 = {
            {"added-files", "5"}, {"deleted-files", "2"}, {"total-records", "1500"}};
        Snapshot snapshot2(2, 1648262400000, Operation::REPLACE, "metadata/manifests/snap-2.avro", summary2);
        snapshot2.parent_snapshot_id = 1;
        snapshots.push_back(snapshot2);

        return snapshots;
    }

    // Helper method to create test data files
    std::vector<DataFile> CreateTestDataFiles() {
        std::vector<DataFile> files;

        // First file
        std::unordered_map<std::string, std::string> partition1 = {{"year", "2023"}, {"month", "01"}};
        files.emplace_back("data/part-00000.parquet", FileFormat::PARQUET, partition1, 1000, 1024 * 1024);

        // Second file
        std::unordered_map<std::string, std::string> partition2 = {{"year", "2023"}, {"month", "02"}};
        files.emplace_back("data/part-00001.parquet", FileFormat::PARQUET, partition2, 1500, 1.5 * 1024 * 1024);

        return files;
    }

    // Helper method to create test table metadata
    TableMetadata CreateTestTableMetadata() {
        auto schema = std::make_shared<Schema>();
        schema->AddField("id", ColumnType::INT32);
        schema->AddField("name", ColumnType::STRING);
        schema->AddField("created_date", ColumnType::TIMESTAMP);

        std::unordered_map<std::string, std::string> properties = {
            {"description", "Test table"}, {"owner", "test_user"}, {"version", "1.0"}};

        TableMetadata metadata(UUID::NewUUID().ToString(), "/data/test_table", schema, properties);
        metadata.format_version = 1;
        metadata.current_snapshot_id = 2;
        metadata.last_updated_time = 1648262400000;
        metadata.last_sequence_number = 2;

        // Add partition specs
        metadata.partition_specs = CreateTestPartitionSpecs();

        // Add snapshots
        metadata.snapshots = CreateTestSnapshots();

        return metadata;
    }
};

//
// Test Setup:
//      Create a properties map and test serialization/deserialization
// Test Result:
//      Properties should be correctly serialized to JSON and deserialized back
//
TEST_F(KVCatalogUtilTest, SerializeDeserializeProperties) {
    std::unordered_map<std::string, std::string> properties = {
        {"description", "Test table"},
        {"owner", "test_user"},
        {"version", "1.0"},
        {"created_by", "system"},
        {"special_chars", "value with spaces, commas, and \"quotes\""}};

    // Serialize
    std::string json = SerializeProperties(properties);

    // Verify JSON format
    VerifyPropertiesJson(json, properties);

    // Deserialize
    auto result = DeserializeProperties(json);
    VERIFY_RESULT(result);

    // Verify deserialized properties
    auto deserialized = result.value();
    EXPECT_EQ(deserialized.size(), properties.size());
    for (const auto& [key, value] : properties) {
        ASSERT_TRUE(deserialized.find(key) != deserialized.end());
        EXPECT_EQ(deserialized[key], value);
    }
}

//
// Test Setup:
//      Create partition specs and test serialization/deserialization
// Test Result:
//      Partition specs should be correctly serialized to JSON and deserialized back
//
TEST_F(KVCatalogUtilTest, SerializeDeserializePartitionSpecs) {
    // Create test partition specs
    auto specs = CreateTestPartitionSpecs();

    // Serialize
    std::string json = SerializePartitionSpecs(specs);

    // Verify JSON format
    VerifyPartitionSpecsJson(json, specs);

    // Deserialize
    auto result = DeserializePartitionSpecs(json);
    VERIFY_RESULT(result);

    // Verify deserialized specs
    auto deserialized = result.value();
    ASSERT_EQ(deserialized.size(), specs.size());

    for (size_t i = 0; i < specs.size(); i++) {
        EXPECT_EQ(deserialized[i].spec_id, specs[i].spec_id);
        ASSERT_EQ(deserialized[i].fields.size(), specs[i].fields.size());

        for (size_t j = 0; j < specs[i].fields.size(); j++) {
            EXPECT_EQ(deserialized[i].fields[j].source_id, specs[i].fields[j].source_id);
            EXPECT_EQ(deserialized[i].fields[j].field_id, specs[i].fields[j].field_id);
            EXPECT_EQ(deserialized[i].fields[j].name, specs[i].fields[j].name);
            EXPECT_EQ(deserialized[i].fields[j].transform, specs[i].fields[j].transform);
        }
    }
}

//
// Test Setup:
//      Create snapshots and test serialization/deserialization
// Test Result:
//      Snapshots should be correctly serialized to JSON and deserialized back
//
TEST_F(KVCatalogUtilTest, SerializeDeserializeSnapshots) {
    auto snapshots = CreateTestSnapshots();

    // Serialize
    std::string json = SerializeSnapshots(snapshots);

    // Verify JSON format
    rapidjson::Document doc;
    ASSERT_FALSE(doc.Parse(json.c_str()).HasParseError());
    ASSERT_TRUE(doc.IsArray());
    ASSERT_EQ(doc.Size(), snapshots.size());

    // Deserialize
    auto result = DeserializeSnapshots(json);
    VERIFY_RESULT(result);

    // Verify deserialized snapshots
    auto deserialized = result.value();
    ASSERT_EQ(deserialized.size(), snapshots.size());

    for (size_t i = 0; i < snapshots.size(); i++) {
        EXPECT_EQ(deserialized[i].snapshot_id, snapshots[i].snapshot_id);
        EXPECT_EQ(deserialized[i].timestamp_ms, snapshots[i].timestamp_ms);
        EXPECT_EQ(deserialized[i].operation, snapshots[i].operation);
        EXPECT_EQ(deserialized[i].manifest_list, snapshots[i].manifest_list);
        EXPECT_EQ(deserialized[i].parent_snapshot_id, snapshots[i].parent_snapshot_id);

        // Verify summary
        ASSERT_EQ(deserialized[i].summary.size(), snapshots[i].summary.size());
        for (const auto& [key, value] : snapshots[i].summary) {
            ASSERT_TRUE(deserialized[i].summary.find(key) != deserialized[i].summary.end());
            EXPECT_EQ(deserialized[i].summary[key], value);
        }
    }
}

//
// Test Setup:
//      Create data files and test serialization/deserialization
// Test Result:
//      Data files should be correctly serialized to JSON and deserialized back
//
TEST_F(KVCatalogUtilTest, SerializeDeserializeDataFileList) {
    auto files = CreateTestDataFiles();

    // Serialize
    std::string json = SerializeDataFileList(files);

    // Verify JSON format
    rapidjson::Document doc;
    ASSERT_FALSE(doc.Parse(json.c_str()).HasParseError());
    ASSERT_TRUE(doc.IsArray());
    ASSERT_EQ(doc.Size(), files.size());

    // Deserialize
    auto result = DeserializeDataFileList(json);
    VERIFY_RESULT(result);

    // Verify deserialized files
    auto deserialized = result.value();
    ASSERT_EQ(deserialized.size(), files.size());

    for (size_t i = 0; i < files.size(); i++) {
        EXPECT_EQ(deserialized[i].file_path, files[i].file_path);
        EXPECT_EQ(deserialized[i].format, files[i].format);
        EXPECT_EQ(deserialized[i].record_count, files[i].record_count);
        EXPECT_EQ(deserialized[i].file_size_bytes, files[i].file_size_bytes);

        // Verify partition values
        ASSERT_EQ(deserialized[i].partition_values.size(), files[i].partition_values.size());
        for (const auto& [key, value] : files[i].partition_values) {
            ASSERT_TRUE(deserialized[i].partition_values.find(key) != deserialized[i].partition_values.end());
            EXPECT_EQ(deserialized[i].partition_values[key], value);
        }
    }
}

//
// Test Setup:
//      Create partition values and test serialization/deserialization
// Test Result:
//      Partition values should be correctly serialized to JSON and deserialized back
//
TEST_F(KVCatalogUtilTest, SerializeDeserializePartitionValues) {
    std::unordered_map<std::string, std::string> partition_values = {
        {"year", "2023"}, {"month", "01"}, {"day", "15"}, {"region", "us-west"}};

    // Serialize
    std::string json = SerializePartitionValues(partition_values);

    // Verify JSON format
    VerifyPropertiesJson(json, partition_values);

    // Deserialize
    auto result = DeserializePartitionValues(json);
    VERIFY_RESULT(result);

    // Verify deserialized partition values
    auto deserialized = result.value();
    EXPECT_EQ(deserialized.size(), partition_values.size());
    for (const auto& [key, value] : partition_values) {
        ASSERT_TRUE(deserialized.find(key) != deserialized.end());
        EXPECT_EQ(deserialized[key], value);
    }
}

//
// Test Setup:
//      Create table metadata and test serialization/deserialization
// Test Result:
//      Table metadata should be correctly serialized to JSON and deserialized back
//

TEST_F(KVCatalogUtilTest, SerializeDeserializeTableMetadata) {
    auto metadata = CreateTestTableMetadata();

    // Serialize
    std::string json = SerializeTableMetadata(metadata);

    // Verify JSON format
    rapidjson::Document doc;
    ASSERT_FALSE(doc.Parse(json.c_str()).HasParseError());
    ASSERT_TRUE(doc.IsObject());

    // Deserialize
    auto result = DeserializeTableMetadata(json);
    VERIFY_RESULT(result);

    // Verify deserialized metadata
    auto deserialized = result.value();
    EXPECT_EQ(deserialized.table_uuid, metadata.table_uuid);
    EXPECT_EQ(deserialized.format_version, metadata.format_version);
    EXPECT_EQ(deserialized.location, metadata.location);
    EXPECT_EQ(deserialized.current_snapshot_id, metadata.current_snapshot_id);
    EXPECT_EQ(deserialized.last_updated_time, metadata.last_updated_time);

    // Verify properties
    ASSERT_EQ(deserialized.properties.size(), metadata.properties.size());
    for (const auto& [key, value] : metadata.properties) {
        ASSERT_TRUE(deserialized.properties.find(key) != deserialized.properties.end());
        EXPECT_EQ(deserialized.properties[key], value);
    }

    // Verify partition specs
    ASSERT_EQ(deserialized.partition_specs.size(), metadata.partition_specs.size());
    for (size_t i = 0; i < metadata.partition_specs.size(); i++) {
        EXPECT_EQ(deserialized.partition_specs[i].spec_id, metadata.partition_specs[i].spec_id);
        ASSERT_EQ(deserialized.partition_specs[i].fields.size(), metadata.partition_specs[i].fields.size());
    }
}

//
// Test Setup:
//      Test deserialization with invalid JSON inputs
// Test Result:
//      Deserialization should fail with appropriate error codes
//
TEST_F(KVCatalogUtilTest, InvalidJsonDeserialization) {
    // Invalid properties JSON
    auto props_result = DeserializeProperties("not a json");
    EXPECT_FALSE(props_result.ok());
    EXPECT_EQ(props_result.error().code(), ErrorCode::DeserializationError);

    // Invalid partition specs JSON
    auto specs_result = DeserializePartitionSpecs("not a json");
    EXPECT_FALSE(specs_result.ok());
    EXPECT_EQ(specs_result.error().code(), ErrorCode::DeserializationError);

    // Invalid snapshots JSON
    auto snapshots_result = DeserializeSnapshots("not a json");
    EXPECT_FALSE(snapshots_result.ok());
    EXPECT_EQ(snapshots_result.error().code(), ErrorCode::DeserializationError);

    // Invalid data files JSON
    auto files_result = DeserializeDataFileList("not a json");
    EXPECT_FALSE(files_result.ok());
    EXPECT_EQ(files_result.error().code(), ErrorCode::DeserializationError);

    // Invalid table metadata JSON
    auto metadata_result = DeserializeTableMetadata("not a json");
    EXPECT_FALSE(metadata_result.ok());
    EXPECT_EQ(metadata_result.error().code(), ErrorCode::DeserializationError);
}

//
// Test Setup:
//      Test deserialization with malformed but valid JSON inputs
// Test Result:
//      Deserialization should fail with appropriate error codes
//
TEST_F(KVCatalogUtilTest, MalformedJsonDeserialization) {
    // Properties with non-string values
    auto props_result = DeserializeProperties(R"({"key": 123})", false /* allow_non_string_values */);
    EXPECT_FALSE(props_result.ok());

    // Partition specs with missing fields
    auto specs_result = DeserializePartitionSpecs(R"([{"spec_id": 1}])");
    EXPECT_FALSE(specs_result.ok());

    // Snapshots with missing required fields
    auto snapshots_result = DeserializeSnapshots(R"([{"snapshot_id": 1}])");
    EXPECT_FALSE(snapshots_result.ok());

    // Data files with missing required fields
    auto files_result = DeserializeDataFileList(R"([{"file_path": "test.parquet"}])");
    EXPECT_FALSE(files_result.ok());

    // Table metadata with missing required fields
    auto metadata_result = DeserializeTableMetadata(R"({"table_uuid": "123"})");
    EXPECT_FALSE(metadata_result.ok());
}

//
// Test Setup:
//      Test deserialization with empty JSON inputs
// Test Result:
//      Empty collections should be properly deserialized
//
TEST_F(KVCatalogUtilTest, EmptyCollectionsDeserialization) {
    // Empty properties
    auto empty_props_result = DeserializeProperties("{}");
    VERIFY_RESULT(empty_props_result);
    EXPECT_TRUE(empty_props_result.value().empty());

    // Empty partition specs
    auto empty_specs_result = DeserializePartitionSpecs("[]");
    VERIFY_RESULT(empty_specs_result);
    EXPECT_TRUE(empty_specs_result.value().empty());

    // Empty snapshots
    auto empty_snapshots_result = DeserializeSnapshots("[]");
    VERIFY_RESULT(empty_snapshots_result);
    EXPECT_TRUE(empty_snapshots_result.value().empty());

    // Empty data files
    auto empty_files_result = DeserializeDataFileList("[]");
    VERIFY_RESULT(empty_files_result);
    EXPECT_TRUE(empty_files_result.value().empty());

    // Empty partition values
    auto empty_values_result = DeserializePartitionValues("{}");
    VERIFY_RESULT(empty_values_result);
    EXPECT_TRUE(empty_values_result.value().empty());
}

//
// Test Setup:
//      Test serialization with empty collections
// Test Result:
//      Empty collections should be properly serialized as empty JSON objects/arrays
//
TEST_F(KVCatalogUtilTest, EmptyCollectionsSerialization) {
    // Empty properties
    std::unordered_map<std::string, std::string> empty_props;
    std::string props_json = SerializeProperties(empty_props);
    rapidjson::Document props_doc;
    ASSERT_FALSE(props_doc.Parse(props_json.c_str()).HasParseError());
    ASSERT_TRUE(props_doc.IsObject());
    EXPECT_EQ(props_doc.MemberCount(), 0);

    // Empty partition specs
    std::vector<PartitionSpec> empty_specs;
    std::string specs_json = SerializePartitionSpecs(empty_specs);
    rapidjson::Document specs_doc;
    ASSERT_FALSE(specs_doc.Parse(specs_json.c_str()).HasParseError());
    ASSERT_TRUE(specs_doc.IsArray());
    EXPECT_EQ(specs_doc.Size(), 0);

    // Empty partition values
    std::unordered_map<std::string, std::string> empty_values;
    std::string values_json = SerializePartitionValues(empty_values);
    rapidjson::Document values_doc;
    ASSERT_FALSE(values_doc.Parse(values_json.c_str()).HasParseError());
    ASSERT_TRUE(values_doc.IsObject());
    EXPECT_EQ(values_doc.MemberCount(), 0);
}

//
// Test Setup:
//      Test with extremely large values in JSON
// Test Result:
//      Large values should be properly serialized and deserialized
//
TEST_F(KVCatalogUtilTest, LargeValuesSerialization) {
    // Create a very large string value
    std::string large_value(100000, 'X');

    // Properties with large value
    std::unordered_map<std::string, std::string> large_props;
    large_props["large_key"] = large_value;

    std::string props_json = SerializeProperties(large_props);
    auto props_result = DeserializeProperties(props_json);
    VERIFY_RESULT(props_result);
    auto& props_map = props_result.value();
    ASSERT_TRUE(props_map.find("large_key") != props_map.end());
    EXPECT_EQ(props_map.at("large_key"), large_value);

    // Partition values with large value
    std::unordered_map<std::string, std::string> large_values;
    large_values["large_partition"] = large_value;

    std::string values_json = SerializePartitionValues(large_values);
    auto values_result = DeserializePartitionValues(values_json);
    VERIFY_RESULT(values_result);
    auto& values_map = values_result.value();
    ASSERT_TRUE(values_map.find("large_partition") != values_map.end());
    EXPECT_EQ(values_map.at("large_partition"), large_value);
}

//
// Test Setup:
//      Test with special characters in JSON strings
// Test Result:
//      Special characters should be properly escaped and preserved
//
TEST_F(KVCatalogUtilTest, SpecialCharactersSerialization) {
    // Create strings with special characters
    std::string special_chars = "\"\\\/\b\f\n\r\t";
    std::string unicode_chars = "Unicode: \u00A9 \u00AE \u2122";
    std::string emoji = "Emoji: 😀 🚀 🌍";

    // Properties with special characters
    std::unordered_map<std::string, std::string> special_props;
    special_props["special"] = special_chars;
    special_props["unicode"] = unicode_chars;
    special_props["emoji"] = emoji;

    std::string props_json = SerializeProperties(special_props);
    auto props_result = DeserializeProperties(props_json);
    VERIFY_RESULT(props_result);
    auto& props_map = props_result.value();
    ASSERT_TRUE(props_map.find("special") != props_map.end());
    ASSERT_TRUE(props_map.find("unicode") != props_map.end());
    ASSERT_TRUE(props_map.find("emoji") != props_map.end());
    EXPECT_EQ(props_map.at("special"), special_chars);
    EXPECT_EQ(props_map.at("unicode"), unicode_chars);
    EXPECT_EQ(props_map.at("emoji"), emoji);
}

//
// Test Setup:
//      Test with deeply nested partition specs
// Test Result:
//      Complex nested structures should be properly serialized and deserialized
//
TEST_F(KVCatalogUtilTest, ComplexPartitionSpecsSerialization) {
    // Create a complex partition spec with many fields
    PartitionSpec complex_spec(999);
    for (int i = 0; i < 50; i++) {
        PartitionField field(i, i + 100, "field_" + std::to_string(i), Transform::IDENTITY);
        if (i % 2 == 0) {
            field.transform_param = i * 10;
        }
        complex_spec.fields.push_back(field);
    }

    std::vector<PartitionSpec> specs = {complex_spec};
    std::string specs_json = SerializePartitionSpecs(specs);

    auto specs_result = DeserializePartitionSpecs(specs_json);
    VERIFY_RESULT(specs_result);
    ASSERT_EQ(specs_result.value().size(), 1);
    EXPECT_EQ(specs_result.value()[0].spec_id, complex_spec.spec_id);
    ASSERT_EQ(specs_result.value()[0].fields.size(), complex_spec.fields.size());

    // Verify all fields were preserved
    for (size_t i = 0; i < complex_spec.fields.size(); i++) {
        EXPECT_EQ(specs_result.value()[0].fields[i].source_id, complex_spec.fields[i].source_id);
        EXPECT_EQ(specs_result.value()[0].fields[i].field_id, complex_spec.fields[i].field_id);
        EXPECT_EQ(specs_result.value()[0].fields[i].name, complex_spec.fields[i].name);
        EXPECT_EQ(specs_result.value()[0].fields[i].transform, complex_spec.fields[i].transform);
        EXPECT_EQ(specs_result.value()[0].fields[i].transform_param, complex_spec.fields[i].transform_param);
    }
}

//
// Test Setup:
//      Test with malformed JSON that has incorrect types but valid syntax
// Test Result:
//      Deserialization should fail with appropriate error codes
//
TEST_F(KVCatalogUtilTest, TypeMismatchJsonDeserialization) {
    // Properties with array instead of object
    auto props_array_result = DeserializeProperties("[]");
    EXPECT_FALSE(props_array_result.ok());
    EXPECT_EQ(props_array_result.error().code(), ErrorCode::DeserializationError);

    // Partition specs with object instead of array
    auto specs_object_result = DeserializePartitionSpecs("{}");
    EXPECT_FALSE(specs_object_result.ok());
    EXPECT_EQ(specs_object_result.error().code(), ErrorCode::DeserializationError);

    // Snapshots with object instead of array
    auto snapshots_object_result = DeserializeSnapshots("{}");
    EXPECT_FALSE(snapshots_object_result.ok());
    EXPECT_EQ(snapshots_object_result.error().code(), ErrorCode::DeserializationError);

    // Data files with object instead of array
    auto files_object_result = DeserializeDataFileList("{}");
    EXPECT_FALSE(files_object_result.ok());
    EXPECT_EQ(files_object_result.error().code(), ErrorCode::DeserializationError);

    // Partition values with array instead of object
    auto values_array_result = DeserializePartitionValues("[]");
    EXPECT_FALSE(values_array_result.ok());
    EXPECT_EQ(values_array_result.error().code(), ErrorCode::DeserializationError);
}

//
// Test Setup:
//      Test with truncated JSON inputs
// Test Result:
//      Deserialization should fail with appropriate error codes
//
TEST_F(KVCatalogUtilTest, TruncatedJsonDeserialization) {
    // Truncated properties JSON
    auto truncated_props_result = DeserializeProperties(R"({"key": "value)");
    EXPECT_FALSE(truncated_props_result.ok());
    EXPECT_EQ(truncated_props_result.error().code(), ErrorCode::DeserializationError);

    // Truncated partition specs JSON
    auto truncated_specs_result = DeserializePartitionSpecs(R"([{"spec_id": 1, "fields": [{"source_id": 1)");
    EXPECT_FALSE(truncated_specs_result.ok());
    EXPECT_EQ(truncated_specs_result.error().code(), ErrorCode::DeserializationError);

    // Truncated snapshots JSON
    auto truncated_snapshots_result = DeserializeSnapshots(R"([{"snapshot_id": 1, "timestamp_ms": 123456789)");
    EXPECT_FALSE(truncated_snapshots_result.ok());
    EXPECT_EQ(truncated_snapshots_result.error().code(), ErrorCode::DeserializationError);
}

//
// Test Setup:
//      Test with JSON containing unexpected fields
// Test Result:
//      Extra fields should be ignored during deserialization
//
TEST_F(KVCatalogUtilTest, ExtraFieldsJsonDeserialization) {
    // Properties with extra fields
    auto props_result = DeserializeProperties(R"({"key": "value", "extra_field": true, "another_extra": 123})");
    VERIFY_RESULT(props_result);
    auto& props_map = props_result.value();
    EXPECT_EQ(props_map.size(), 1);
    ASSERT_TRUE(props_map.find("key") != props_map.end());
    EXPECT_EQ(props_map.at("key"), "value");

    // Partition specs with extra fields
    auto specs_result = DeserializePartitionSpecs(
        R"([{"spec_id": 1, "extra_field": true, "fields": [{"source_id": 1, "field_id": 100, "name": "test", "transform": "identity", "extra_field": "ignored"}]}])");
    VERIFY_RESULT(specs_result);
    ASSERT_EQ(specs_result.value().size(), 1);
    EXPECT_EQ(specs_result.value()[0].spec_id, 1);
    ASSERT_EQ(specs_result.value()[0].fields.size(), 1);
    EXPECT_EQ(specs_result.value()[0].fields[0].source_id, 1);
    EXPECT_EQ(specs_result.value()[0].fields[0].field_id, 100);
    EXPECT_EQ(specs_result.value()[0].fields[0].name, "test");
    EXPECT_EQ(specs_result.value()[0].fields[0].transform, Transform::IDENTITY);
}

//
// Test Setup:
//      Test with boundary values for numeric fields
// Test Result:
//      Boundary values should be properly serialized and deserialized
//
TEST_F(KVCatalogUtilTest, BoundaryValuesSerialization) {
    // Create snapshots with boundary values
    std::vector<Snapshot> boundary_snapshots;

    // Max int64_t values
    std::unordered_map<std::string, std::string> summary1 = {{"count", "9223372036854775807"}};
    Snapshot max_snapshot(std::numeric_limits<int64_t>::max(),
                          std::numeric_limits<int64_t>::max(),
                          Operation::APPEND,
                          "max.avro",
                          summary1);
    max_snapshot.parent_snapshot_id = std::numeric_limits<int64_t>::max() - 1;
    boundary_snapshots.push_back(max_snapshot);

    // Min int64_t values
    std::unordered_map<std::string, std::string> summary2 = {{"count", "-9223372036854775808"}};
    Snapshot min_snapshot(std::numeric_limits<int64_t>::min(),
                          std::numeric_limits<int64_t>::min(),
                          Operation::DELETE,
                          "min.avro",
                          summary2);
    min_snapshot.parent_snapshot_id = std::numeric_limits<int64_t>::min() + 1;
    boundary_snapshots.push_back(min_snapshot);

    // Serialize and deserialize
    std::string json = SerializeSnapshots(boundary_snapshots);
    auto result = DeserializeSnapshots(json);
    VERIFY_RESULT(result);

    // Verify max values
    ASSERT_EQ(result.value().size(), 2);
    EXPECT_EQ(result.value()[0].snapshot_id, std::numeric_limits<int64_t>::max());
    EXPECT_EQ(result.value()[0].timestamp_ms, std::numeric_limits<int64_t>::max());
    EXPECT_EQ(*result.value()[0].parent_snapshot_id, std::numeric_limits<int64_t>::max() - 1);

    // Verify min values
    EXPECT_EQ(result.value()[1].snapshot_id, std::numeric_limits<int64_t>::min());
    EXPECT_EQ(result.value()[1].timestamp_ms, std::numeric_limits<int64_t>::min());
    EXPECT_EQ(*result.value()[1].parent_snapshot_id, std::numeric_limits<int64_t>::min() + 1);
}

}  // namespace pond::catalog