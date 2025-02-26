#include "catalog/kv_catalog.h"

#include <chrono>
#include <sstream>

#include "catalog/kv_catalog_util.h"
#include "common/error.h"
#include "common/log.h"
#include "common/uuid.h"
#include "kv/record.h"

namespace pond::catalog {

using namespace std::chrono;
using namespace pond::common;

namespace {
constexpr const char* TABLES_TABLE = "__tables";
constexpr const char* SNAPSHOTS_TABLE = "__snapshots";
constexpr const char* FILES_TABLE = "__files";

// Tables metadata schema
constexpr const char* TABLE_NAME_FIELD = "table_name";                    // STRING (primary key)
constexpr const char* TABLE_UUID_FIELD = "table_uuid";                    // STRING
constexpr const char* FORMAT_VERSION_FIELD = "format_version";            // INT32
constexpr const char* LOCATION_FIELD = "location";                        // STRING
constexpr const char* CURRENT_SNAPSHOT_ID_FIELD = "current_snapshot_id";  // INT64
constexpr const char* LAST_UPDATED_MS_FIELD = "last_updated_ms";          // INT64
constexpr const char* PROPERTIES_FIELD = "properties";                    // BINARY (serialized map)
constexpr const char* SCHEMA_FIELD = "schema";                            // BINARY (serialized schema)
constexpr const char* PARTITION_SPECS_FIELD = "partition_specs";          // BINARY (serialized specs)

// Snapshots metadata schema
constexpr const char* SNAPSHOT_ID_FIELD = "snapshot_id";                // INT64
constexpr const char* PARENT_SNAPSHOT_ID_FIELD = "parent_snapshot_id";  // INT64
constexpr const char* TIMESTAMP_MS_FIELD = "timestamp_ms";              // INT64
constexpr const char* OPERATION_FIELD = "operation";                    // STRING
constexpr const char* MANIFEST_LIST_FIELD = "manifest_list";            // STRING
constexpr const char* SUMMARY_FIELD = "summary";                        // BINARY (serialized map)

// Files metadata schema
constexpr const char* FILE_PATH_FIELD = "file_path";                // STRING
constexpr const char* FILE_FORMAT_FIELD = "format";                 // STRING
constexpr const char* RECORD_COUNT_FIELD = "record_count";          // INT64
constexpr const char* FILE_SIZE_FIELD = "file_size_bytes";          // INT64
constexpr const char* PARTITION_VALUES_FIELD = "partition_values";  // BINARY (serialized map)

std::shared_ptr<common::Schema> CreateTablesTableSchema() {
    static auto schema = common::CreateSchemaBuilder()
                             .AddField(TABLE_NAME_FIELD, ColumnType::STRING)
                             .AddField(TABLE_UUID_FIELD, ColumnType::STRING)
                             .AddField(FORMAT_VERSION_FIELD, ColumnType::INT32)
                             .AddField(LOCATION_FIELD, ColumnType::STRING)
                             .AddField(CURRENT_SNAPSHOT_ID_FIELD, ColumnType::INT64)
                             .AddField(LAST_UPDATED_MS_FIELD, ColumnType::INT64)
                             .AddField(PROPERTIES_FIELD, ColumnType::BINARY)
                             .AddField(SCHEMA_FIELD, ColumnType::BINARY)
                             .AddField(PARTITION_SPECS_FIELD, ColumnType::BINARY)
                             .Build();
    return schema;
}

std::shared_ptr<common::Schema> CreateSnapshotsTableSchema() {
    static auto schema = common::CreateSchemaBuilder()
                             .AddField(TABLE_NAME_FIELD, ColumnType::STRING)
                             .AddField(SNAPSHOT_ID_FIELD, ColumnType::INT64)
                             .AddField(PARENT_SNAPSHOT_ID_FIELD, ColumnType::INT64)
                             .AddField(TIMESTAMP_MS_FIELD, ColumnType::INT64)
                             .AddField(OPERATION_FIELD, ColumnType::STRING)
                             .AddField(MANIFEST_LIST_FIELD, ColumnType::STRING)
                             .AddField(SUMMARY_FIELD, ColumnType::BINARY)
                             .Build();
    return schema;
}

std::shared_ptr<common::Schema> CreateFilesTableSchema() {
    static auto schema = common::CreateSchemaBuilder()
                             .AddField(TABLE_NAME_FIELD, ColumnType::STRING)
                             .AddField(SNAPSHOT_ID_FIELD, ColumnType::INT64)
                             .AddField(FILE_PATH_FIELD, ColumnType::STRING)
                             .AddField(FILE_FORMAT_FIELD, ColumnType::STRING)
                             .AddField(RECORD_COUNT_FIELD, ColumnType::INT64)
                             .AddField(FILE_SIZE_FIELD, ColumnType::INT64)
                             .AddField(PARTITION_VALUES_FIELD, ColumnType::BINARY)
                             .Build();
    return schema;
}
}  // namespace

// Constructor
KVCatalog::KVCatalog(std::shared_ptr<pond::kv::DB> db) : db_(std::move(db)) {
    // Initialize the catalog
    auto result = Initialize();
    if (!result.ok()) {
        LOG_ERROR("Failed to initialize KVCatalog: %s", result.error().message().c_str());
    }
}

// Initialize the catalog
common::Result<void> KVCatalog::Initialize() {
    // Create tables table if it doesn't exist
    auto tables_result = db_->GetTable(TABLES_TABLE);
    if (!tables_result.ok()) {
        auto create_result = db_->CreateTable(TABLES_TABLE, CreateTablesTableSchema());
        if (!create_result.ok()) {
            return common::Result<void>::failure(create_result.error());
        }
        tables_result = db_->GetTable(TABLES_TABLE);
        if (!tables_result.ok()) {
            return common::Result<void>::failure(tables_result.error());
        }
    }
    tables_table_ = tables_result.value();

    // Create snapshots table if it doesn't exist
    auto snapshots_result = db_->GetTable(SNAPSHOTS_TABLE);
    if (!snapshots_result.ok()) {
        auto create_result = db_->CreateTable(SNAPSHOTS_TABLE, CreateSnapshotsTableSchema());
        if (!create_result.ok()) {
            return common::Result<void>::failure(create_result.error());
        }
        snapshots_result = db_->GetTable(SNAPSHOTS_TABLE);
        if (!snapshots_result.ok()) {
            return common::Result<void>::failure(snapshots_result.error());
        }
    }
    snapshots_table_ = snapshots_result.value();

    // Create files table if it doesn't exist
    auto files_result = db_->GetTable(FILES_TABLE);
    if (!files_result.ok()) {
        auto create_result = db_->CreateTable(FILES_TABLE, CreateFilesTableSchema());
        if (!create_result.ok()) {
            return common::Result<void>::failure(create_result.error());
        }
        files_result = db_->GetTable(FILES_TABLE);
        if (!files_result.ok()) {
            return common::Result<void>::failure(files_result.error());
        }
    }
    files_table_ = files_result.value();

    return common::Result<void>::success();
}

// Helper method to create and store a record
common::Result<void> KVCatalog::PutRecord(const std::string& key, const std::string& value) {
    auto record = std::make_unique<kv::Record>(tables_table_->schema());
    record->Set(0, key);
    record->Set(1, common::DataChunk::FromString(value));

    return tables_table_->Put(key, std::move(record));
}

// Helper method to get a record's value
common::Result<std::string> KVCatalog::GetRecordValue(const std::string& key) {
    auto get_result = tables_table_->Get(key);
    if (!get_result.ok()) {
        return common::Result<std::string>::failure(get_result.error());
    }

    auto& record = get_result.value();
    auto value_result = record->Get<common::DataChunk>(1);
    if (!value_result.ok()) {
        return common::Result<std::string>::failure(value_result.error());
    }

    return common::Result<std::string>::success(value_result.value().ToString());
}

// Create a new table
common::Result<TableMetadata> KVCatalog::CreateTable(const std::string& name,
                                                     std::shared_ptr<common::Schema> schema,
                                                     const PartitionSpec& spec,
                                                     const std::string& location,
                                                     const std::unordered_map<std::string, std::string>& properties) {
    std::lock_guard<std::mutex> lock(mutex_);

    // Check if table already exists
    auto get_result = tables_table_->Get(name);
    if (get_result.ok()) {
        return common::Result<TableMetadata>::failure(common::ErrorCode::FileAlreadyExists,
                                                      "Table '" + name + "' already exists");
    }

    // Generate a UUID for the table
    TableId table_uuid = GenerateUuid();

    // Create initial table metadata
    TableMetadata metadata(table_uuid, location, schema, properties);
    metadata.last_updated_ms = GetCurrentTimeMillis();
    metadata.partition_specs.push_back(spec);

    // Create a record for the tables table
    auto record = std::make_unique<kv::Record>(tables_table_->schema());
    record->Set(0, name);                          // TABLE_NAME_FIELD
    record->Set(1, table_uuid);                    // TABLE_UUID_FIELD
    record->Set(2, metadata.format_version);       // FORMAT_VERSION_FIELD
    record->Set(3, location);                      // LOCATION_FIELD
    record->Set(4, metadata.current_snapshot_id);  // CURRENT_SNAPSHOT_ID_FIELD
    record->Set(5, metadata.last_updated_ms);      // LAST_UPDATED_MS_FIELD

    // Serialize properties to binary
    rapidjson::Document doc;
    doc.SetObject();
    auto& allocator = doc.GetAllocator();
    for (const auto& [key, value] : properties) {
        doc.AddMember(rapidjson::Value(key.c_str(), allocator), rapidjson::Value(value.c_str(), allocator), allocator);
    }
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    doc.Accept(writer);
    record->Set(6, common::DataChunk::FromString(buffer.GetString()));  // PROPERTIES_FIELD

    // Serialize schema to binary (placeholder for now)
    record->Set(7, common::DataChunk::FromString("schema_placeholder"));  // SCHEMA_FIELD

    // Serialize partition specs to binary
    rapidjson::Document specs_doc;
    specs_doc.SetArray();
    for (const auto& spec : metadata.partition_specs) {
        rapidjson::Value spec_json(rapidjson::kObjectType);
        spec_json.AddMember("spec_id", spec.spec_id, allocator);

        rapidjson::Value fields(rapidjson::kArrayType);
        for (const auto& field : spec.fields) {
            rapidjson::Value field_json(rapidjson::kObjectType);
            field_json.AddMember("source_id", field.source_id, allocator);
            field_json.AddMember("field_id", field.field_id, allocator);
            field_json.AddMember("name", rapidjson::Value(field.name.c_str(), allocator), allocator);
            field_json.AddMember(
                "transform", rapidjson::Value(TransformToString(field.transform).c_str(), allocator), allocator);
            if (field.transform_param) {
                field_json.AddMember("transform_param", *field.transform_param, allocator);
            }
            fields.PushBack(field_json, allocator);
        }
        spec_json.AddMember("fields", fields, allocator);
        specs_doc.PushBack(spec_json, allocator);
    }
    rapidjson::StringBuffer specs_buffer;
    rapidjson::Writer<rapidjson::StringBuffer> specs_writer(specs_buffer);
    specs_doc.Accept(specs_writer);
    record->Set(8, common::DataChunk::FromString(specs_buffer.GetString()));  // PARTITION_SPECS_FIELD

    // Save the table record
    auto put_result = tables_table_->Put(name, std::move(record));
    if (!put_result.ok()) {
        return common::Result<TableMetadata>::failure(put_result.error());
    }

    return common::Result<TableMetadata>::success(metadata);
}

// Load a table
common::Result<TableMetadata> KVCatalog::LoadTable(const std::string& name) {
    std::lock_guard<std::mutex> lock(mutex_);

    // Get the table record
    auto get_result = tables_table_->Get(name);
    if (!get_result.ok()) {
        return common::Result<TableMetadata>::failure(common::ErrorCode::FileNotFound,
                                                      "Table '" + name + "' not found");
    }

    auto& record = get_result.value();

    // Create a dummy schema for now (will be replaced with deserialized schema)
    auto schema = std::make_shared<common::Schema>();

    // Get properties
    auto properties_result = record->Get<common::DataChunk>(6);  // PROPERTIES_FIELD
    if (!properties_result.ok()) {
        return common::Result<TableMetadata>::failure(properties_result.error());
    }

    std::unordered_map<std::string, std::string> properties;
    rapidjson::Document doc;
    doc.Parse(properties_result.value().ToString().c_str());
    if (!doc.HasParseError() && doc.IsObject()) {
        for (auto it = doc.MemberBegin(); it != doc.MemberEnd(); ++it) {
            if (it->name.IsString() && it->value.IsString()) {
                properties[it->name.GetString()] = it->value.GetString();
            }
        }
    }

    // Create metadata object
    TableMetadata metadata(record->Get<std::string>(1).value(),  // TABLE_UUID_FIELD
                           record->Get<std::string>(3).value(),  // LOCATION_FIELD
                           schema,
                           properties);

    metadata.format_version = record->Get<int32_t>(2).value();       // FORMAT_VERSION_FIELD
    metadata.current_snapshot_id = record->Get<int64_t>(4).value();  // CURRENT_SNAPSHOT_ID_FIELD
    metadata.last_updated_ms = record->Get<int64_t>(5).value();      // LAST_UPDATED_MS_FIELD

    // Deserialize partition specs
    auto specs_result = record->Get<common::DataChunk>(8);  // PARTITION_SPECS_FIELD
    if (specs_result.ok()) {
        auto specs = DeserializePartitionSpecs(specs_result.value().ToString());
        if (!specs.ok()) {
            return common::Result<TableMetadata>::failure(specs.error());
        }
        metadata.partition_specs = specs.value();
    }

    // Load snapshots for this table
    auto snapshots_prefix = name + "/";  // We'll use table name as prefix for snapshots
    // TODO: Implement scan with prefix in Table interface
    // For now, we'll leave snapshots empty

    return common::Result<TableMetadata>::success(metadata);
}

// Commit a transaction
common::Result<bool> KVCatalog::CommitTransaction(const std::string& name,
                                                  const TableMetadata& base,
                                                  const TableMetadata& updated) {
    // Acquire lock for the table
    auto lock_result = AcquireLock(name);
    if (!lock_result.ok() || !lock_result.value()) {
        return common::Result<bool>::failure(common::ErrorCode::Failure,
                                             "Failed to acquire lock for table '" + name + "'");
    }

    // Verify the base state matches the current state (optimistic concurrency)
    auto current_id_result = GetCurrentSnapshotId(name);
    if (!current_id_result.ok()) {
        ReleaseLock(name);
        return common::Result<bool>::failure(current_id_result.error());
    }

    if (current_id_result.value() != base.current_snapshot_id) {
        ReleaseLock(name);
        return common::Result<bool>::failure(
            common::ErrorCode::InvalidOperation,
            "Concurrent modification detected. Table was modified since the transaction began.");
    }

    // Save the updated metadata
    auto save_result = SaveTableMetadata(updated);
    if (!save_result.ok()) {
        ReleaseLock(name);
        return common::Result<bool>::failure(save_result.error());
    }

    // Update the current pointer
    auto current_key = GetTableCurrentKey(name);
    auto put_result = PutRecord(current_key, std::to_string(updated.current_snapshot_id));
    if (!put_result.ok()) {
        ReleaseLock(name);
        return common::Result<bool>::failure(put_result.error());
    }

    // Release the lock
    auto release_result = ReleaseLock(name);
    if (!release_result.ok() || !release_result.value()) {
        return common::Result<bool>::failure(common::ErrorCode::Failure,
                                             "Failed to release lock for table '" + name + "'");
    }

    return common::Result<bool>::success(true);
}

// Save table metadata
common::Result<void> KVCatalog::SaveTableMetadata(const TableMetadata& metadata) {
    std::string metadata_key = GetTableMetadataKey(metadata.table_uuid, metadata.current_snapshot_id);
    std::string serialized = SerializeTableMetadata(metadata);

    return PutRecord(metadata_key, serialized);
}

// Get table metadata
common::Result<TableMetadata> KVCatalog::GetTableMetadata(const std::string& name, SnapshotId snapshot_id) {
    std::string metadata_key = GetTableMetadataKey(name, snapshot_id);
    auto get_result = GetRecordValue(metadata_key);
    if (!get_result.ok()) {
        return common::Result<TableMetadata>::failure(get_result.error());
    }

    return DeserializeTableMetadata(get_result.value());
}

// Get current snapshot ID
common::Result<SnapshotId> KVCatalog::GetCurrentSnapshotId(const std::string& name) {
    auto current_key = GetTableCurrentKey(name);
    auto get_result = GetRecordValue(current_key);
    if (!get_result.ok()) {
        return common::Result<SnapshotId>::failure(get_result.error());
    }

    try {
        return common::Result<SnapshotId>::success(std::stoll(get_result.value()));
    } catch (const std::exception& e) {
        return common::Result<SnapshotId>::failure(common::ErrorCode::DeserializationError,
                                                   "Failed to parse snapshot ID: " + std::string(e.what()));
    }
}

// Lock management
common::Result<bool> KVCatalog::AcquireLock(const std::string& name, int64_t timeout_ms) {
    auto lock_key = GetTableLockKey(name);
    auto start_time = GetCurrentTimeMillis();

    while (GetCurrentTimeMillis() - start_time < timeout_ms) {
        // Try to create the lock record
        auto record = std::make_unique<kv::Record>(tables_table_->schema());
        record->Set(0, lock_key);
        record->Set(1, common::DataChunk::FromString("locked"));

        auto put_result = tables_table_->Put(lock_key, std::move(record)
                                             // TODO: FIXME
                                             //, /* only_if_absent */ true
        );
        if (put_result.ok()) {
            return common::Result<bool>::success(true);
        }

        // If the error is not that the key already exists, return the error
        if (put_result.error().code() != common::ErrorCode::FileAlreadyExists) {
            return common::Result<bool>::failure(put_result.error());
        }

        // Wait and retry
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    return common::Result<bool>::failure(common::ErrorCode::Failure,
                                         "Timeout waiting for lock on table '" + name + "'");
}

common::Result<bool> KVCatalog::ReleaseLock(const std::string& name) {
    auto lock_key = GetTableLockKey(name);
    auto delete_result = tables_table_->Delete(lock_key);
    if (!delete_result.ok()) {
        return common::Result<bool>::failure(delete_result.error());
    }

    return common::Result<bool>::success(true);
}

// Key construction
std::string KVCatalog::GetTableMetadataKey(const std::string& name, SnapshotId snapshot_id) {
    return TABLE_PREFIX + name + META_SUFFIX + std::to_string(snapshot_id);
}

std::string KVCatalog::GetTableCurrentKey(const std::string& name) {
    return TABLE_PREFIX + name + CURRENT_SUFFIX;
}

std::string KVCatalog::GetTableLockKey(const std::string& name) {
    return TABLE_PREFIX + name + LOCK_SUFFIX;
}

std::string KVCatalog::GetTableFilesKey(const std::string& name, SnapshotId snapshot_id) {
    return TABLE_PREFIX + name + FILES_SUFFIX + std::to_string(snapshot_id);
}

// Utilities
int64_t KVCatalog::GetCurrentTimeMillis() {
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

TableId KVCatalog::GenerateUuid() {
    common::UUID uuid = common::UUID::NewUUID();
    return uuid.ToString();
}

// Create a snapshot for a table
common::Result<TableMetadata> KVCatalog::CreateSnapshot(const std::string& name,
                                                        const std::vector<DataFile>& added_files,
                                                        const std::vector<DataFile>& deleted_files,
                                                        Operation op) {
    std::lock_guard<std::mutex> lock(mutex_);

    // Load the current table metadata
    auto load_result = LoadTable(name);
    if (!load_result.ok()) {
        return load_result;
    }

    TableMetadata metadata = load_result.value();

    // Generate a new snapshot ID (increment the current one)
    SnapshotId new_snapshot_id = metadata.current_snapshot_id + 1;

    // Create the manifest list path
    std::string manifest_list =
        metadata.location + "/metadata/manifest-list-" + std::to_string(new_snapshot_id) + ".json";

    // Create a summary of the snapshot
    std::unordered_map<std::string, std::string> summary;
    summary["added-files"] = std::to_string(added_files.size());
    summary["deleted-files"] = std::to_string(deleted_files.size());
    summary["total-files"] = std::to_string(added_files.size() - deleted_files.size());

    // Create a record for the snapshots table
    auto snapshot_record = std::make_unique<kv::Record>(snapshots_table_->schema());
    snapshot_record->Set(0, name);             // TABLE_NAME_FIELD
    snapshot_record->Set(1, new_snapshot_id);  // SNAPSHOT_ID_FIELD
    if (metadata.current_snapshot_id >= 0) {
        snapshot_record->Set(2, metadata.current_snapshot_id);  // PARENT_SNAPSHOT_ID_FIELD
    }
    snapshot_record->Set(3, GetCurrentTimeMillis());  // TIMESTAMP_MS_FIELD
    snapshot_record->Set(4, OperationToString(op));   // OPERATION_FIELD
    snapshot_record->Set(5, manifest_list);           // MANIFEST_LIST_FIELD

    // Serialize summary to binary
    rapidjson::Document summary_doc;
    summary_doc.SetObject();
    auto& allocator = summary_doc.GetAllocator();
    for (const auto& [key, value] : summary) {
        summary_doc.AddMember(
            rapidjson::Value(key.c_str(), allocator), rapidjson::Value(value.c_str(), allocator), allocator);
    }
    rapidjson::StringBuffer summary_buffer;
    rapidjson::Writer<rapidjson::StringBuffer> summary_writer(summary_buffer);
    summary_doc.Accept(summary_writer);
    snapshot_record->Set(6, common::DataChunk::FromString(summary_buffer.GetString()));  // SUMMARY_FIELD

    // Save the snapshot record
    std::string snapshot_key = name + "/" + std::to_string(new_snapshot_id);
    auto snapshot_put_result = snapshots_table_->Put(snapshot_key, std::move(snapshot_record));
    if (!snapshot_put_result.ok()) {
        return common::Result<TableMetadata>::failure(snapshot_put_result.error());
    }

    // Store the files
    for (const auto& file : added_files) {
        auto file_record = std::make_unique<kv::Record>(files_table_->schema());
        file_record->Set(0, name);                             // TABLE_NAME_FIELD
        file_record->Set(1, new_snapshot_id);                  // SNAPSHOT_ID_FIELD
        file_record->Set(2, file.file_path);                   // FILE_PATH_FIELD
        file_record->Set(3, FileFormatToString(file.format));  // FILE_FORMAT_FIELD
        file_record->Set(4, file.record_count);                // RECORD_COUNT_FIELD
        file_record->Set(5, file.file_size_bytes);             // FILE_SIZE_FIELD

        // Serialize partition values to binary
        rapidjson::Document partition_doc;
        partition_doc.SetObject();
        for (const auto& [key, value] : file.partition_values) {
            partition_doc.AddMember(
                rapidjson::Value(key.c_str(), allocator), rapidjson::Value(value.c_str(), allocator), allocator);
        }
        rapidjson::StringBuffer partition_buffer;
        rapidjson::Writer<rapidjson::StringBuffer> partition_writer(partition_buffer);
        partition_doc.Accept(partition_writer);
        file_record->Set(6, common::DataChunk::FromString(partition_buffer.GetString()));  // PARTITION_VALUES_FIELD

        // Save the file record
        std::string file_key = name + "/" + std::to_string(new_snapshot_id) + "/" + file.file_path;
        auto file_put_result = files_table_->Put(file_key, std::move(file_record));
        if (!file_put_result.ok()) {
            LOG_ERROR("Failed to save file record: %s", file_put_result.error().message().c_str());
            // Continue with other files
        }
    }

    // Update the table metadata with the new snapshot
    metadata.current_snapshot_id = new_snapshot_id;
    metadata.last_updated_ms = GetCurrentTimeMillis();
    metadata.last_sequence_number++;

    // Update the table record
    auto table_record = std::make_unique<kv::Record>(tables_table_->schema());
    table_record->Set(0, name);                          // TABLE_NAME_FIELD
    table_record->Set(1, metadata.table_uuid);           // TABLE_UUID_FIELD
    table_record->Set(2, metadata.format_version);       // FORMAT_VERSION_FIELD
    table_record->Set(3, metadata.location);             // LOCATION_FIELD
    table_record->Set(4, metadata.current_snapshot_id);  // CURRENT_SNAPSHOT_ID_FIELD
    table_record->Set(5, metadata.last_updated_ms);      // LAST_UPDATED_MS_FIELD

    // Re-serialize properties and other fields (reuse from LoadTable)
    auto get_result = tables_table_->Get(name);
    if (get_result.ok()) {
        auto& existing_record = get_result.value();
        table_record->Set(6, existing_record->Get<common::DataChunk>(6).value());  // PROPERTIES_FIELD
        table_record->Set(7, existing_record->Get<common::DataChunk>(7).value());  // SCHEMA_FIELD
        table_record->Set(8, existing_record->Get<common::DataChunk>(8).value());  // PARTITION_SPECS_FIELD
    }

    auto table_put_result = tables_table_->Put(name, std::move(table_record));
    if (!table_put_result.ok()) {
        return common::Result<TableMetadata>::failure(table_put_result.error());
    }

    return common::Result<TableMetadata>::success(metadata);
}

// List data files for a specific snapshot
common::Result<std::vector<DataFile>> KVCatalog::ListDataFiles(const std::string& name,
                                                               const std::optional<SnapshotId>& snapshot_id) {
    std::lock_guard<std::mutex> lock(mutex_);

    // Load the table metadata to get the current snapshot ID if none specified
    auto metadata_result = LoadTable(name);
    if (!metadata_result.ok()) {
        return common::Result<std::vector<DataFile>>::failure(metadata_result.error());
    }

    TableMetadata metadata = metadata_result.value();
    SnapshotId target_snapshot_id = snapshot_id.value_or(metadata.current_snapshot_id);

    // Verify the snapshot exists
    std::string snapshot_key = name + "/" + std::to_string(target_snapshot_id);
    auto snapshot_get = snapshots_table_->Get(snapshot_key);
    if (!snapshot_get.ok()) {
        return common::Result<std::vector<DataFile>>::failure(
            common::ErrorCode::NotFound,
            "Snapshot ID " + std::to_string(target_snapshot_id) + " not found for table '" + name + "'");
    }

    // List all files for this snapshot
    // TODO: Implement scan with prefix in Table interface
    // For now, we'll return an empty vector
    std::vector<DataFile> files;

    return common::Result<std::vector<DataFile>>::success(files);
}

// Update the schema for a table
common::Result<TableMetadata> KVCatalog::UpdateSchema(const std::string& name,
                                                      std::shared_ptr<common::Schema> new_schema) {
    // Load the current table metadata
    auto current_result = LoadTable(name);
    if (!current_result.ok()) {
        return current_result;
    }

    TableMetadata current = current_result.value();
    TableMetadata updated = current;

    // Update the schema
    updated.schema = new_schema;
    updated.last_updated_ms = GetCurrentTimeMillis();

    // Commit the transaction
    auto commit_result = CommitTransaction(name, current, updated);
    if (!commit_result.ok()) {
        return common::Result<TableMetadata>::failure(commit_result.error());
    }

    return common::Result<TableMetadata>::success(updated);
}

// Update the partition spec for a table
common::Result<TableMetadata> KVCatalog::UpdatePartitionSpec(const std::string& name, const PartitionSpec& new_spec) {
    // Load the current table metadata
    auto current_result = LoadTable(name);
    if (!current_result.ok()) {
        return current_result;
    }

    TableMetadata current = current_result.value();
    TableMetadata updated = current;

    // Add the new partition spec (we keep history of all partition specs)
    updated.partition_specs.push_back(new_spec);
    updated.last_updated_ms = GetCurrentTimeMillis();

    // Commit the transaction
    auto commit_result = CommitTransaction(name, current, updated);
    if (!commit_result.ok()) {
        return common::Result<TableMetadata>::failure(commit_result.error());
    }

    return common::Result<TableMetadata>::success(updated);
}

// Drop a table
common::Result<bool> KVCatalog::DropTable(const std::string& name) {
    std::lock_guard<std::mutex> lock(mutex_);

    // Check if the table exists
    auto current_id_result = GetCurrentSnapshotId(name);
    if (!current_id_result.ok()) {
        return common::Result<bool>::failure(common::ErrorCode::FileNotFound, "Table '" + name + "' not found");
    }

    // Get table metadata
    auto metadata_result = GetTableMetadata(name, current_id_result.value());
    if (!metadata_result.ok()) {
        return common::Result<bool>::failure(metadata_result.error());
    }

    // Delete current pointer
    auto current_key = GetTableCurrentKey(name);
    auto delete_current = PutRecord(current_key, "");
    if (!delete_current.ok()) {
        return common::Result<bool>::failure(delete_current.error());
    }

    // Delete all metadata records for all snapshots
    // Note: In a real system, you might want to keep these for some time for recovery
    for (const auto& snapshot : metadata_result.value().snapshots) {
        std::string metadata_key = GetTableMetadataKey(name, snapshot.snapshot_id);
        auto delete_metadata = PutRecord(metadata_key, "");
        if (!delete_metadata.ok()) {
            // Log but continue with deletion
            LOG_ERROR("Failed to delete metadata for table '%s' snapshot %ld: %s",
                      name.c_str(),
                      snapshot.snapshot_id,
                      delete_metadata.error().message().c_str());
        }

        std::string files_key = GetTableFilesKey(name, snapshot.snapshot_id);
        auto delete_files = PutRecord(files_key, "");
        if (!delete_files.ok()) {
            // Log but continue with deletion
            LOG_ERROR("Failed to delete files for table '%s' snapshot %ld: %s",
                      name.c_str(),
                      snapshot.snapshot_id,
                      delete_files.error().message().c_str());
        }
    }

    return common::Result<bool>::success(true);
}

// Rename a table
common::Result<bool> KVCatalog::RenameTable(const std::string& name, const std::string& new_name) {
    // Make sure the new name doesn't already exist
    auto new_current_id_result = GetCurrentSnapshotId(new_name);
    if (new_current_id_result.ok()) {
        return common::Result<bool>::failure(common::ErrorCode::FileAlreadyExists,
                                             "Table '" + new_name + "' already exists");
    }

    // Load the current table metadata
    auto current_result = LoadTable(name);
    if (!current_result.ok()) {
        return common::Result<bool>::failure(current_result.error());
    }

    // Acquire locks for both tables
    auto lock_result = AcquireLock(name);
    if (!lock_result.ok() || !lock_result.value()) {
        return common::Result<bool>::failure(common::ErrorCode::Failure,
                                             "Failed to acquire lock for table '" + name + "'");
    }

    auto lock_new_result = AcquireLock(new_name);
    if (!lock_new_result.ok() || !lock_new_result.value()) {
        // Release the first lock
        ReleaseLock(name);
        return common::Result<bool>::failure(common::ErrorCode::Failure,
                                             "Failed to acquire lock for new table name '" + new_name + "'");
    }

    TableMetadata metadata = current_result.value();
    SnapshotId current_id = metadata.current_snapshot_id;

    // Get the current metadata key
    std::string metadata_key = GetTableMetadataKey(name, current_id);
    auto metadata_get = GetRecordValue(metadata_key);
    if (!metadata_get.ok()) {
        ReleaseLock(name);
        ReleaseLock(new_name);
        return common::Result<bool>::failure(metadata_get.error());
    }

    // Save metadata with new name
    std::string new_metadata_key = GetTableMetadataKey(new_name, current_id);
    auto put_result = PutRecord(new_metadata_key, metadata_get.value());
    if (!put_result.ok()) {
        ReleaseLock(name);
        ReleaseLock(new_name);
        return common::Result<bool>::failure(put_result.error());
    }

    // Update the current pointer for the new name
    std::string new_current_key = GetTableCurrentKey(new_name);
    auto put_current = PutRecord(new_current_key, std::to_string(current_id));
    if (!put_current.ok()) {
        ReleaseLock(name);
        ReleaseLock(new_name);
        return common::Result<bool>::failure(put_current.error());
    }

    // Delete the old current pointer
    std::string old_current_key = GetTableCurrentKey(name);
    auto delete_current = PutRecord(old_current_key, "");
    if (!delete_current.ok()) {
        // Log but continue
        LOG_ERROR("Failed to delete old current pointer for table '%s': %s",
                  name.c_str(),
                  delete_current.error().message().c_str());
    }

    // Copy file lists for all snapshots
    for (const auto& snapshot : metadata.snapshots) {
        std::string old_files_key = GetTableFilesKey(name, snapshot.snapshot_id);
        auto get_files = GetRecordValue(old_files_key);
        if (get_files.ok()) {
            std::string new_files_key = GetTableFilesKey(new_name, snapshot.snapshot_id);
            auto put_files = PutRecord(new_files_key, get_files.value());
            if (!put_files.ok()) {
                // Log but continue
                LOG_ERROR("Failed to copy files for table '%s' snapshot %ld: %s",
                          name.c_str(),
                          snapshot.snapshot_id,
                          put_files.error().message().c_str());
            }
        }
    }

    // Release the locks
    ReleaseLock(name);
    ReleaseLock(new_name);

    return common::Result<bool>::success(true);
}

// Update table properties
common::Result<TableMetadata> KVCatalog::UpdateTableProperties(
    const std::string& name, const std::unordered_map<std::string, std::string>& updates) {
    // Load the current table metadata
    auto current_result = LoadTable(name);
    if (!current_result.ok()) {
        return current_result;
    }

    TableMetadata current = current_result.value();
    TableMetadata updated = current;

    // Update properties
    for (const auto& [key, value] : updates) {
        updated.properties[key] = value;
    }

    updated.last_updated_ms = GetCurrentTimeMillis();

    // Commit the transaction
    auto commit_result = CommitTransaction(name, current, updated);
    if (!commit_result.ok()) {
        return common::Result<TableMetadata>::failure(commit_result.error());
    }

    return common::Result<TableMetadata>::success(updated);
}

}  // namespace pond::catalog
