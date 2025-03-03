#include "catalog/kv_catalog_util.h"

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include "catalog/types.h"
#include "common/uuid.h"

using namespace pond::common;

namespace pond::catalog {

// JSON keys for table metadata
static constexpr const char* KEY_TABLE_UUID = "table_uuid";
static constexpr const char* KEY_FORMAT_VERSION = "format_version";
static constexpr const char* KEY_LOCATION = "location";
static constexpr const char* KEY_CURRENT_SNAPSHOT_ID = "current_snapshot_id";
static constexpr const char* KEY_LAST_UPDATED_TIME = "last_updated_time";
static constexpr const char* KEY_PROPERTIES = "properties";
static constexpr const char* KEY_SCHEMA = "schema";
static constexpr const char* KEY_PARTITION_SPECS = "partition_specs";
static constexpr const char* KEY_SNAPSHOTS = "snapshots";

// JSON keys for partition specs
static constexpr const char* KEY_SPEC_ID = "spec_id";
static constexpr const char* KEY_FIELDS = "fields";
static constexpr const char* KEY_SOURCE_ID = "source_id";
static constexpr const char* KEY_FIELD_ID = "field_id";
static constexpr const char* KEY_NAME = "name";
static constexpr const char* KEY_TRANSFORM = "transform";
static constexpr const char* KEY_TRANSFORM_PARAM = "transform_param";

// JSON keys for snapshots
static constexpr const char* KEY_SNAPSHOT_ID = "snapshot_id";
static constexpr const char* KEY_PARENT_SNAPSHOT_ID = "parent_snapshot_id";
static constexpr const char* KEY_TIMESTAMP_MS = "timestamp_ms";
static constexpr const char* KEY_OPERATION = "operation";
static constexpr const char* KEY_FILES = "files";
static constexpr const char* KEY_SUMMARY = "summary";

// JSON keys for data files
static constexpr const char* KEY_FILE_PATH = "file_path";
static constexpr const char* KEY_FORMAT = "format";
static constexpr const char* KEY_RECORD_COUNT = "record_count";
static constexpr const char* KEY_FILE_SIZE_BYTES = "file_size_bytes";
static constexpr const char* KEY_PARTITION_VALUES = "partition_values";

#define CSTR_TO_VALUE(str) rapidjson::Value(str, allocator)

rapidjson::Value SerializeSnapshot(const Snapshot& snapshot, rapidjson::Document::AllocatorType& allocator) {
    rapidjson::Value snap_json(rapidjson::kObjectType);

    // Add required fields - create Value objects for the keys
    rapidjson::Value snapshot_id_key(KEY_SNAPSHOT_ID, allocator);
    rapidjson::Value timestamp_ms_key(KEY_TIMESTAMP_MS, allocator);
    rapidjson::Value operation_key(KEY_OPERATION, allocator);
    rapidjson::Value files_key(KEY_FILES, allocator);

    snap_json.AddMember(snapshot_id_key, snapshot.snapshot_id, allocator);
    snap_json.AddMember(timestamp_ms_key, snapshot.timestamp_ms, allocator);
    snap_json.AddMember(
        operation_key, rapidjson::Value(OperationToString(snapshot.operation).c_str(), allocator), allocator);

    // Serialize files array
    rapidjson::Value files_array(rapidjson::kArrayType);
    for (const auto& file : snapshot.files) {
        rapidjson::Value file_json(rapidjson::kObjectType);

        // Add file fields
        rapidjson::Value file_path_key(KEY_FILE_PATH, allocator);
        rapidjson::Value format_key(KEY_FORMAT, allocator);
        rapidjson::Value record_count_key(KEY_RECORD_COUNT, allocator);
        rapidjson::Value file_size_bytes_key(KEY_FILE_SIZE_BYTES, allocator);

        file_json.AddMember(file_path_key, rapidjson::Value(file.file_path.c_str(), allocator), allocator);
        file_json.AddMember(
            format_key, rapidjson::Value(FileFormatToString(file.format).c_str(), allocator), allocator);
        file_json.AddMember(record_count_key, file.record_count, allocator);
        file_json.AddMember(file_size_bytes_key, file.file_size_bytes, allocator);

        // Add partition values
        if (!file.partition_values.empty()) {
            rapidjson::Value partition_values(rapidjson::kObjectType);
            for (const auto& [key, value] : file.partition_values) {
                rapidjson::Value key_val(key.c_str(), allocator);
                partition_values.AddMember(key_val, rapidjson::Value(value.c_str(), allocator), allocator);
            }
            rapidjson::Value partition_values_key("partition_values", allocator);
            file_json.AddMember(partition_values_key, partition_values, allocator);
        }

        files_array.PushBack(file_json, allocator);
    }

    snap_json.AddMember(files_key, files_array, allocator);

    // Add optional parent_snapshot_id if present
    if (snapshot.parent_snapshot_id) {
        rapidjson::Value parent_id_key(KEY_PARENT_SNAPSHOT_ID, allocator);
        snap_json.AddMember(parent_id_key, *snapshot.parent_snapshot_id, allocator);
    }

    // Serialize summary map
    rapidjson::Value summary_json(rapidjson::kObjectType);
    for (const auto& [key, value] : snapshot.summary) {
        rapidjson::Value key_val(key.c_str(), allocator);
        summary_json.AddMember(key_val, rapidjson::Value(value.c_str(), allocator), allocator);
    }

    rapidjson::Value summary_key(KEY_SUMMARY, allocator);
    snap_json.AddMember(summary_key, summary_json, allocator);
    return snap_json;
}

// Serialization
std::string SerializeTableMetadata(const TableMetadata& metadata) {
    rapidjson::Document doc;
    doc.SetObject();
    auto& allocator = doc.GetAllocator();

    doc.AddMember("format_version", metadata.format_version, allocator);
    doc.AddMember("table_uuid", rapidjson::Value(metadata.table_uuid.c_str(), allocator), allocator);
    doc.AddMember("location", rapidjson::Value(metadata.location.c_str(), allocator), allocator);
    doc.AddMember("last_sequence_number", metadata.last_sequence_number, allocator);
    doc.AddMember("last_updated_time", metadata.last_updated_time, allocator);
    doc.AddMember("current_snapshot_id", metadata.current_snapshot_id, allocator);

    // Serialize snapshots
    rapidjson::Value snapshots(rapidjson::kArrayType);
    for (const auto& snapshot : metadata.snapshots) {
        rapidjson::Value snap = SerializeSnapshot(snapshot, allocator);

        snapshots.PushBack(snap, allocator);
    }
    doc.AddMember("snapshots", snapshots, allocator);

    // Serialize partition specs
    rapidjson::Value specs(rapidjson::kArrayType);
    for (const auto& spec : metadata.partition_specs) {
        rapidjson::Value spec_json(rapidjson::kObjectType);

        rapidjson::Value spec_id_key(KEY_SPEC_ID, allocator);
        spec_json.AddMember(spec_id_key, spec.spec_id, allocator);

        rapidjson::Value fields(rapidjson::kArrayType);
        for (const auto& field : spec.fields) {
            rapidjson::Value field_json(rapidjson::kObjectType);

            rapidjson::Value source_id_key(KEY_SOURCE_ID, allocator);
            rapidjson::Value field_id_key(KEY_FIELD_ID, allocator);
            rapidjson::Value name_key(KEY_NAME, allocator);
            rapidjson::Value transform_key(KEY_TRANSFORM, allocator);
            rapidjson::Value transform_param_key(KEY_TRANSFORM_PARAM, allocator);

            field_json.AddMember(source_id_key, field.source_id, allocator);
            field_json.AddMember(field_id_key, field.field_id, allocator);
            field_json.AddMember(name_key, rapidjson::Value(field.name.c_str(), allocator), allocator);
            field_json.AddMember(
                transform_key, rapidjson::Value(TransformToString(field.transform).c_str(), allocator), allocator);

            if (field.transform_param) {
                field_json.AddMember(transform_param_key, *field.transform_param, allocator);
            }

            fields.PushBack(field_json, allocator);
        }

        rapidjson::Value fields_key(KEY_FIELDS, allocator);
        spec_json.AddMember(fields_key, fields, allocator);
        specs.PushBack(spec_json, allocator);
    }

    rapidjson::Value partition_specs_key(KEY_PARTITION_SPECS, allocator);
    doc.AddMember(partition_specs_key, specs, allocator);

    // Serialize schema (placeholder)
    rapidjson::Value schema_key(KEY_SCHEMA, allocator);
    doc.AddMember(schema_key, "schema_placeholder", allocator);

    // Serialize properties
    rapidjson::Value properties(rapidjson::kObjectType);
    for (const auto& [key, value] : metadata.properties) {
        properties.AddMember(
            rapidjson::Value(key.c_str(), allocator), rapidjson::Value(value.c_str(), allocator), allocator);
    }
    rapidjson::Value properties_key(KEY_PROPERTIES, allocator);
    doc.AddMember(properties_key, properties, allocator);

    // Convert to string
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    doc.Accept(writer);

    return buffer.GetString();
}

// Deserialize a single snapshot from a JSON object
Result<Snapshot> DeserializeSnapshot(const rapidjson::Value& snapshot_json) {
    // Validate that snapshot_json is an object
    if (!snapshot_json.IsObject()) {
        return Result<Snapshot>::failure(ErrorCode::DeserializationError, "Snapshot must be an object");
    }

    // Validate required fields
    if (!snapshot_json.HasMember(KEY_SNAPSHOT_ID) || !snapshot_json.HasMember(KEY_TIMESTAMP_MS)
        || !snapshot_json.HasMember(KEY_OPERATION) || !snapshot_json.HasMember(KEY_FILES)) {
        return Result<Snapshot>::failure(ErrorCode::DeserializationError, "Missing required snapshot fields");
    }

    // Create a snapshot object and populate it
    Snapshot snapshot(snapshot_json[KEY_SNAPSHOT_ID].GetInt64(),
                      snapshot_json[KEY_TIMESTAMP_MS].GetInt64(),
                      OperationFromString(snapshot_json[KEY_OPERATION].GetString()),
                      {},  // Will populate files below
                      {}   // summary will be populated below
    );

    // Set parent_snapshot_id if present
    if (snapshot_json.HasMember(KEY_PARENT_SNAPSHOT_ID)) {
        snapshot.parent_snapshot_id = snapshot_json[KEY_PARENT_SNAPSHOT_ID].GetInt64();
    }

    // Deserialize files
    const auto& files_json = snapshot_json[KEY_FILES];
    if (!files_json.IsArray()) {
        return Result<Snapshot>::failure(ErrorCode::DeserializationError, "Files must be an array");
    }

    for (const auto& file_json : files_json.GetArray()) {
        if (!file_json.IsObject() || !file_json.HasMember(KEY_FILE_PATH) || !file_json.HasMember(KEY_FORMAT)
            || !file_json.HasMember(KEY_RECORD_COUNT) || !file_json.HasMember(KEY_FILE_SIZE_BYTES)) {
            return Result<Snapshot>::failure(ErrorCode::DeserializationError, "Missing required file fields");
        }

        // Initialize partition values map
        std::unordered_map<std::string, std::string> partition_values;

        // Parse partition values if present
        if (file_json.HasMember(KEY_PARTITION_VALUES)) {
            const auto& pv_json = file_json[KEY_PARTITION_VALUES];
            if (!pv_json.IsObject()) {
                return Result<Snapshot>::failure(ErrorCode::DeserializationError, "Partition values must be an object");
            }

            for (auto it = pv_json.MemberBegin(); it != pv_json.MemberEnd(); ++it) {
                if (!it->value.IsString()) {
                    return Result<Snapshot>::failure(ErrorCode::DeserializationError,
                                                     "Partition value must be a string");
                }
                partition_values[it->name.GetString()] = it->value.GetString();
            }
        }

        // Parse file format
        FileFormat format;
        try {
            format = FileFormatFromString(file_json[KEY_FORMAT].GetString());
        } catch (const std::exception& e) {
            return Result<Snapshot>::failure(ErrorCode::DeserializationError,
                                             std::string("Invalid file format: ") + e.what());
        }

        // Create data file and add to snapshot
        DataFile data_file(file_json[KEY_FILE_PATH].GetString(),
                           format,
                           partition_values,
                           file_json[KEY_RECORD_COUNT].GetInt64(),
                           file_json[KEY_FILE_SIZE_BYTES].GetInt64());
        snapshot.files.push_back(std::move(data_file));
    }

    // Deserialize summary if present
    if (snapshot_json.HasMember(KEY_SUMMARY)) {
        const auto& summary_json = snapshot_json[KEY_SUMMARY];
        if (!summary_json.IsObject()) {
            return Result<Snapshot>::failure(ErrorCode::DeserializationError, "Summary must be an object");
        }

        for (auto it = summary_json.MemberBegin(); it != summary_json.MemberEnd(); ++it) {
            if (!it->value.IsString()) {
                return Result<Snapshot>::failure(ErrorCode::DeserializationError, "Summary value must be a string");
            }
            snapshot.summary[it->name.GetString()] = it->value.GetString();
        }
    }

    return Result<Snapshot>::success(std::move(snapshot));
}

Result<TableMetadata> DeserializeTableMetadata(const std::string& data) {
    rapidjson::Document doc;
    if (doc.Parse(data.c_str()).HasParseError()) {
        return Result<TableMetadata>::failure(ErrorCode::DeserializationError, "Failed to parse table metadata JSON");
    }

    // Validate required fields
    if (!doc.HasMember(KEY_TABLE_UUID) || !doc.HasMember(KEY_FORMAT_VERSION) || !doc.HasMember(KEY_LOCATION)
        || !doc.HasMember(KEY_CURRENT_SNAPSHOT_ID) || !doc.HasMember(KEY_LAST_UPDATED_TIME)
        || !doc.HasMember(KEY_PROPERTIES) || !doc.HasMember(KEY_SCHEMA) || !doc.HasMember(KEY_PARTITION_SPECS)) {
        return Result<TableMetadata>::failure(ErrorCode::DeserializationError,
                                              "Missing required fields in table metadata JSON");
    }

    // Extract basic fields
    TableMetadata metadata(doc[KEY_TABLE_UUID].GetString(),
                           doc[KEY_LOCATION].GetString(),
                           nullptr,  // Schema will be set later
                           {}        // Properties will be set later
    );

    metadata.format_version = doc[KEY_FORMAT_VERSION].GetInt();
    metadata.current_snapshot_id = doc[KEY_CURRENT_SNAPSHOT_ID].GetInt64();
    metadata.last_updated_time = doc[KEY_LAST_UPDATED_TIME].GetInt64();

    // Extract properties
    const auto& properties = doc[KEY_PROPERTIES];
    if (!properties.IsObject()) {
        return Result<TableMetadata>::failure(ErrorCode::DeserializationError, "Properties must be an object");
    }
    for (auto it = properties.MemberBegin(); it != properties.MemberEnd(); ++it) {
        if (!it->value.IsString()) {
            return Result<TableMetadata>::failure(ErrorCode::DeserializationError, "Property values must be strings");
        }
        metadata.properties[it->name.GetString()] = it->value.GetString();
    }

    // TODO: Deserialize schema
    // const auto& schema = doc[KEY_SCHEMA];

    // Extract partition specs
    const auto& specs = doc[KEY_PARTITION_SPECS];
    if (!specs.IsArray()) {
        return Result<TableMetadata>::failure(ErrorCode::DeserializationError, "Partition specs must be an array");
    }

    for (const auto& spec : specs.GetArray()) {
        if (!spec.IsObject() || !spec.HasMember(KEY_SPEC_ID) || !spec.HasMember(KEY_FIELDS)) {
            return Result<TableMetadata>::failure(ErrorCode::DeserializationError, "Invalid partition spec format");
        }

        PartitionSpec partition_spec(spec[KEY_SPEC_ID].GetInt());
        const auto& fields = spec[KEY_FIELDS];
        if (!fields.IsArray()) {
            return Result<TableMetadata>::failure(ErrorCode::DeserializationError,
                                                  "Partition spec fields must be an array");
        }

        for (const auto& field : fields.GetArray()) {
            if (!field.IsObject() || !field.HasMember(KEY_SOURCE_ID) || !field.HasMember(KEY_FIELD_ID)
                || !field.HasMember(KEY_NAME) || !field.HasMember(KEY_TRANSFORM)) {
                return Result<TableMetadata>::failure(ErrorCode::DeserializationError,
                                                      "Invalid partition field format");
            }

            PartitionField partition_field(field[KEY_SOURCE_ID].GetInt(),
                                           field[KEY_FIELD_ID].GetInt(),
                                           field[KEY_NAME].GetString(),
                                           TransformFromString(field[KEY_TRANSFORM].GetString()));

            if (field.HasMember(KEY_TRANSFORM_PARAM)) {
                partition_field.transform_param = field[KEY_TRANSFORM_PARAM].GetInt();
            }

            partition_spec.fields.push_back(std::move(partition_field));
        }

        metadata.partition_specs.push_back(std::move(partition_spec));
    }

    // Deserialize snapshots if present
    if (doc.HasMember(KEY_SNAPSHOTS)) {
        const auto& snapshots_json = doc[KEY_SNAPSHOTS];
        if (!snapshots_json.IsArray()) {
            return Result<TableMetadata>::failure(ErrorCode::DeserializationError, "Snapshots must be an array");
        }

        for (const auto& snapshot_json : snapshots_json.GetArray()) {
            auto result = DeserializeSnapshot(snapshot_json);
            if (!result.ok()) {
                return Result<TableMetadata>::failure(
                    ErrorCode::DeserializationError,
                    "Failed to deserialize snapshot: " + std::string(result.error().message()));
            }
            metadata.snapshots.push_back(std::move(result.value()));
        }
    }

    return Result<TableMetadata>::success(std::move(metadata));
}

// Serialize a list of data files
std::string SerializeDataFiles(const std::vector<DataFile>& files) {
    rapidjson::Document doc;
    doc.SetArray();
    auto& allocator = doc.GetAllocator();

    for (const auto& file : files) {
        rapidjson::Value file_json(rapidjson::kObjectType);

        rapidjson::Value file_path_key(KEY_FILE_PATH, allocator);
        rapidjson::Value format_key(KEY_FORMAT, allocator);
        rapidjson::Value record_count_key(KEY_RECORD_COUNT, allocator);
        rapidjson::Value file_size_bytes_key(KEY_FILE_SIZE_BYTES, allocator);

        file_json.AddMember(file_path_key, rapidjson::Value(file.file_path.c_str(), allocator), allocator);
        file_json.AddMember(
            format_key, rapidjson::Value(FileFormatToString(file.format).c_str(), allocator), allocator);
        file_json.AddMember(record_count_key, file.record_count, allocator);
        file_json.AddMember(file_size_bytes_key, file.file_size_bytes, allocator);

        // Serialize partition values
        rapidjson::Value partition_values(rapidjson::kObjectType);
        for (const auto& [key, value] : file.partition_values) {
            partition_values.AddMember(
                rapidjson::Value(key.c_str(), allocator), rapidjson::Value(value.c_str(), allocator), allocator);
        }
        file_json.AddMember("partition_values", partition_values, allocator);

        doc.PushBack(file_json, allocator);
    }

    // Convert to string
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    doc.Accept(writer);

    return buffer.GetString();
}

// Deserialize a list of data files
Result<std::vector<DataFile>> DeserializeDataFiles(const std::string& data) {
    try {
        rapidjson::Document doc;
        doc.Parse(data.c_str());

        if (doc.HasParseError() || !doc.IsArray()) {
            return Result<std::vector<DataFile>>::failure(ErrorCode::DeserializationError,
                                                          "Failed to parse JSON data files array");
        }

        std::vector<DataFile> files;

        for (const auto& file_json : doc.GetArray()) {
            // Get partition values
            std::unordered_map<std::string, std::string> partition_values;
            if (file_json.HasMember(KEY_PARTITION_VALUES) && file_json[KEY_PARTITION_VALUES].IsObject()) {
                for (auto it = file_json[KEY_PARTITION_VALUES].MemberBegin();
                     it != file_json[KEY_PARTITION_VALUES].MemberEnd();
                     ++it) {
                    if (it->name.IsString() && it->value.IsString()) {
                        partition_values[it->name.GetString()] = it->value.GetString();
                    }
                }
            }

            DataFile file(file_json[KEY_FILE_PATH].GetString(),
                          FileFormatFromString(file_json[KEY_FORMAT].GetString()),
                          std::move(partition_values),
                          file_json[KEY_RECORD_COUNT].GetInt64(),
                          file_json[KEY_FILE_SIZE_BYTES].GetInt64());

            files.push_back(file);
        }

        return Result<std::vector<DataFile>>::success(files);
    } catch (const std::exception& e) {
        return Result<std::vector<DataFile>>::failure(ErrorCode::DeserializationError,
                                                      "Failed to deserialize data files: " + std::string(e.what()));
    }
}

// JSON conversion helpers
std::string SerializeProperties(const std::unordered_map<std::string, std::string>& properties) {
    rapidjson::Document doc;
    doc.SetObject();
    auto& allocator = doc.GetAllocator();

    for (const auto& [key, value] : properties) {
        rapidjson::Value key_val(key.c_str(), allocator);
        doc.AddMember(key_val, rapidjson::Value(value.c_str(), allocator), allocator);
    }

    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    doc.Accept(writer);
    return buffer.GetString();
}

Result<std::unordered_map<std::string, std::string>> DeserializeProperties(const std::string& json,
                                                                           bool allow_non_string_values) {
    try {
        rapidjson::Document doc;
        doc.Parse(json.c_str());

        if (doc.HasParseError() || !doc.IsObject()) {
            return Result<std::unordered_map<std::string, std::string>>::failure(ErrorCode::DeserializationError,
                                                                                 "Invalid properties JSON");
        }

        std::unordered_map<std::string, std::string> properties;
        for (auto it = doc.MemberBegin(); it != doc.MemberEnd(); ++it) {
            if (it->name.IsString() && it->value.IsString()) {
                properties[it->name.GetString()] = it->value.GetString();
            } else if (!allow_non_string_values) {
                return Result<std::unordered_map<std::string, std::string>>::failure(ErrorCode::DeserializationError,
                                                                                     "Property values must be strings");
            }
        }
        return Result<std::unordered_map<std::string, std::string>>::success(properties);
    } catch (const std::exception& e) {
        return Result<std::unordered_map<std::string, std::string>>::failure(
            ErrorCode::DeserializationError, "Failed to parse properties JSON: " + std::string(e.what()));
    }
}

std::string SerializePartitionSpecs(const std::vector<PartitionSpec>& specs) {
    rapidjson::Document doc;
    doc.SetArray();
    auto& allocator = doc.GetAllocator();

    for (const auto& spec : specs) {
        rapidjson::Value spec_json(rapidjson::kObjectType);

        // Create key values
        rapidjson::Value spec_id_key(KEY_SPEC_ID, allocator);
        rapidjson::Value fields_key(KEY_FIELDS, allocator);

        spec_json.AddMember(spec_id_key, spec.spec_id, allocator);

        rapidjson::Value fields(rapidjson::kArrayType);
        for (const auto& field : spec.fields) {
            rapidjson::Value field_json(rapidjson::kObjectType);

            // Create key values for field properties
            rapidjson::Value source_id_key(KEY_SOURCE_ID, allocator);
            rapidjson::Value field_id_key(KEY_FIELD_ID, allocator);
            rapidjson::Value name_key(KEY_NAME, allocator);
            rapidjson::Value transform_key(KEY_TRANSFORM, allocator);
            rapidjson::Value transform_param_key(KEY_TRANSFORM_PARAM, allocator);

            field_json.AddMember(source_id_key, field.source_id, allocator);
            field_json.AddMember(field_id_key, field.field_id, allocator);
            field_json.AddMember(name_key, rapidjson::Value(field.name.c_str(), allocator), allocator);
            field_json.AddMember(
                transform_key, rapidjson::Value(TransformToString(field.transform).c_str(), allocator), allocator);
            if (field.transform_param) {
                field_json.AddMember(transform_param_key, *field.transform_param, allocator);
            }
            fields.PushBack(field_json, allocator);
        }
        spec_json.AddMember(fields_key, fields, allocator);
        doc.PushBack(spec_json, allocator);
    }

    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    doc.Accept(writer);
    return buffer.GetString();
}

Result<std::vector<PartitionSpec>> DeserializePartitionSpecs(const std::string& json) {
    try {
        rapidjson::Document doc;
        doc.Parse(json.c_str());

        if (doc.HasParseError() || !doc.IsArray()) {
            return Result<std::vector<PartitionSpec>>::failure(ErrorCode::DeserializationError,
                                                               "Invalid partition specs JSON");
        }

        std::vector<PartitionSpec> specs;
        for (const auto& spec_json : doc.GetArray()) {
            if (!spec_json.IsObject())
                continue;

            PartitionSpec spec(spec_json[KEY_SPEC_ID].GetInt());

            if (spec_json.HasMember(KEY_FIELDS) && spec_json[KEY_FIELDS].IsArray()) {
                for (const auto& field_json : spec_json[KEY_FIELDS].GetArray()) {
                    std::optional<int32_t> transform_param;
                    if (field_json.HasMember(KEY_TRANSFORM_PARAM)) {
                        transform_param = field_json[KEY_TRANSFORM_PARAM].GetInt();
                    }

                    PartitionField field(field_json[KEY_SOURCE_ID].GetInt(),
                                         field_json[KEY_FIELD_ID].GetInt(),
                                         field_json[KEY_NAME].GetString(),
                                         TransformFromString(field_json[KEY_TRANSFORM].GetString()),
                                         transform_param);
                    spec.fields.push_back(field);
                }
            } else {
                return Result<std::vector<PartitionSpec>>::failure(ErrorCode::DeserializationError,
                                                                   "Invalid partition spec format");
            }
            specs.push_back(spec);
        }
        return Result<std::vector<PartitionSpec>>::success(specs);
    } catch (const std::exception& e) {
        return Result<std::vector<PartitionSpec>>::failure(
            ErrorCode::DeserializationError, "Failed to parse partition specs JSON: " + std::string(e.what()));
    }
}

std::string SerializePartitionValues(const std::unordered_map<std::string, std::string>& values) {
    // Use the same JSON format as properties
    return SerializeProperties(values);
}

Result<std::unordered_map<std::string, std::string>> DeserializePartitionValues(const std::string& json) {
    // Use the same JSON format as properties
    return DeserializeProperties(json);
}

std::string SerializeSnapshots(const std::vector<Snapshot>& snapshots) {
    rapidjson::Document doc;
    doc.SetArray();
    auto& allocator = doc.GetAllocator();

    for (const auto& snapshot : snapshots) {
        rapidjson::Value snap_json = SerializeSnapshot(snapshot, allocator);
        doc.PushBack(snap_json, allocator);
    }

    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    doc.Accept(writer);
    return buffer.GetString();
}

Result<std::vector<Snapshot>> DeserializeSnapshots(const std::string& data) {
    rapidjson::Document doc;
    if (doc.Parse(data.c_str()).HasParseError()) {
        return Result<std::vector<Snapshot>>::failure(ErrorCode::DeserializationError, "Invalid JSON format");
    }

    if (!doc.IsArray()) {
        return Result<std::vector<Snapshot>>::failure(ErrorCode::DeserializationError,
                                                      "Expected an array of snapshots");
    }

    std::vector<Snapshot> snapshots;
    for (const auto& snapshot_json : doc.GetArray()) {
        auto result = DeserializeSnapshot(snapshot_json);
        if (!result.ok()) {
            return Result<std::vector<Snapshot>>::failure(
                ErrorCode::DeserializationError,
                "Failed to deserialize snapshot: " + std::string(result.error().message()));
        }
        snapshots.push_back(std::move(result.value()));
    }

    return Result<std::vector<Snapshot>>::success(std::move(snapshots));
}

std::string SerializeDataFileList(const std::vector<DataFile>& files) {
    rapidjson::Document doc;
    doc.SetArray();
    auto& allocator = doc.GetAllocator();

    for (const auto& file : files) {
        rapidjson::Value file_json(rapidjson::kObjectType);

        rapidjson::Value file_path_key(KEY_FILE_PATH, allocator);
        rapidjson::Value format_key(KEY_FORMAT, allocator);
        rapidjson::Value record_count_key(KEY_RECORD_COUNT, allocator);
        rapidjson::Value file_size_bytes_key(KEY_FILE_SIZE_BYTES, allocator);

        // Add required fields
        file_json.AddMember(file_path_key, CSTR_TO_VALUE(file.file_path.c_str()), allocator);
        file_json.AddMember(format_key, CSTR_TO_VALUE(FileFormatToString(file.format).c_str()), allocator);
        file_json.AddMember(record_count_key, file.record_count, allocator);
        file_json.AddMember(file_size_bytes_key, file.file_size_bytes, allocator);

        // Add partition values
        rapidjson::Value partition_values(rapidjson::kObjectType);
        for (const auto& [key, value] : file.partition_values) {
            partition_values.AddMember(CSTR_TO_VALUE(key.c_str()), CSTR_TO_VALUE(value.c_str()), allocator);
        }
        file_json.AddMember(CSTR_TO_VALUE(KEY_PARTITION_VALUES), partition_values, allocator);

        // Add the file to the document array
        doc.PushBack(file_json, allocator);
    }

    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    doc.Accept(writer);
    return buffer.GetString();
}

Result<std::vector<DataFile>> DeserializeDataFileList(const std::string& data) {
    rapidjson::Document doc;
    if (doc.Parse(data.c_str()).HasParseError()) {
        return Result<std::vector<DataFile>>::failure(ErrorCode::DeserializationError,
                                                      "Failed to parse data files JSON");
    }

    if (!doc.IsArray()) {
        return Result<std::vector<DataFile>>::failure(ErrorCode::DeserializationError, "Data files must be an array");
    }

    std::vector<DataFile> files;
    for (const auto& file : doc.GetArray()) {
        if (!file.IsObject() || !file.HasMember(KEY_FILE_PATH) || !file.HasMember(KEY_FORMAT)
            || !file.HasMember(KEY_RECORD_COUNT) || !file.HasMember(KEY_FILE_SIZE_BYTES)
            || !file.HasMember(KEY_PARTITION_VALUES)) {
            return Result<std::vector<DataFile>>::failure(ErrorCode::DeserializationError, "Invalid data file format");
        }

        std::unordered_map<std::string, std::string> partition_values;
        const auto& values = file[KEY_PARTITION_VALUES];
        if (!values.IsObject()) {
            return Result<std::vector<DataFile>>::failure(ErrorCode::DeserializationError,
                                                          "Partition values must be an object");
        }

        for (auto it = values.MemberBegin(); it != values.MemberEnd(); ++it) {
            if (!it->value.IsString()) {
                return Result<std::vector<DataFile>>::failure(ErrorCode::DeserializationError,
                                                              "Partition values must be strings");
            }
            partition_values[it->name.GetString()] = it->value.GetString();
        }

        files.emplace_back(file[KEY_FILE_PATH].GetString(),
                           FileFormatFromString(file[KEY_FORMAT].GetString()),
                           std::move(partition_values),
                           file[KEY_RECORD_COUNT].GetInt64(),
                           file[KEY_FILE_SIZE_BYTES].GetInt64());
    }

    return Result<std::vector<DataFile>>::success(std::move(files));
}

std::shared_ptr<common::Schema> GetTablesTableSchema() {
    static auto schema = common::CreateSchemaBuilder()
                             .AddField(TABLE_NAME_FIELD, common::ColumnType::STRING)
                             .AddField(TABLE_UUID_FIELD, common::ColumnType::STRING)
                             .AddField(FORMAT_VERSION_FIELD, common::ColumnType::INT32)
                             .AddField(LOCATION_FIELD, common::ColumnType::STRING)
                             .AddField(CURRENT_SNAPSHOT_ID_FIELD, common::ColumnType::INT64)
                             .AddField(LAST_UPDATED_TIME_FIELD, common::ColumnType::INT64)
                             .AddField(PROPERTIES_FIELD, common::ColumnType::STRING)
                             .AddField(SCHEMA_FIELD, common::ColumnType::BINARY)
                             .AddField(PARTITION_SPECS_FIELD, common::ColumnType::STRING)
                             .Build();
    return schema;
}

std::shared_ptr<common::Schema> GetSnapshotsTableSchema() {
    static auto schema = common::CreateSchemaBuilder()
                             .AddField(TABLE_NAME_FIELD, common::ColumnType::STRING)
                             .AddField(SNAPSHOT_ID_FIELD, common::ColumnType::INT64)
                             .AddField(PARENT_SNAPSHOT_ID_FIELD, common::ColumnType::INT64)
                             .AddField(TIMESTAMP_MS_FIELD, common::ColumnType::INT64)
                             .AddField(OPERATION_FIELD, common::ColumnType::STRING)
                             .AddField(FILES_FIELD, common::ColumnType::BINARY)
                             .AddField(SUMMARY_FIELD, common::ColumnType::BINARY)
                             .Build();
    return schema;
}

std::shared_ptr<common::Schema> GetFilesTableSchema() {
    static auto schema = common::CreateSchemaBuilder()
                             .AddField(TABLE_NAME_FIELD, common::ColumnType::STRING)
                             .AddField(SNAPSHOT_ID_FIELD, common::ColumnType::INT64)
                             .AddField(FILE_PATH_FIELD, common::ColumnType::STRING)
                             .AddField(FILE_FORMAT_FIELD, common::ColumnType::STRING)
                             .AddField(RECORD_COUNT_FIELD, common::ColumnType::INT64)
                             .AddField(FILE_SIZE_FIELD, common::ColumnType::INT64)
                             .AddField(PARTITION_VALUES_FIELD, common::ColumnType::BINARY)
                             .Build();
    return schema;
}

std::unique_ptr<kv::Record> CreateTableMetadataRecord(const std::string& name, const TableMetadata& metadata) {
    auto record = std::make_unique<kv::Record>(GetTablesTableSchema());
    record->Set(0, name);                                               // TABLE_NAME_FIELD
    record->Set(1, metadata.table_uuid);                                // TABLE_UUID_FIELD
    record->Set(2, metadata.format_version);                            // FORMAT_VERSION_FIELD
    record->Set(3, metadata.location);                                  // LOCATION_FIELD
    record->Set(4, metadata.current_snapshot_id);                       // CURRENT_SNAPSHOT_ID_FIELD
    record->Set(5, metadata.last_updated_time);                         // LAST_UPDATED_TIME_FIELD
    record->Set(6, SerializePartitionValues(metadata.properties));      // PROPERTIES_FIELD
    record->Set(7, metadata.schema->Serialize());                       // SCHEMA_FIELD
    record->Set(8, SerializePartitionSpecs(metadata.partition_specs));  // PARTITION_SPECS_FIELD

    return record;
}

}  // namespace pond::catalog