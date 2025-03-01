#include "catalog/kv_catalog.h"

#include <chrono>
#include <sstream>

#include "catalog/kv_catalog_util.h"
#include "common/error.h"
#include "common/log.h"
#include "common/scope_exit.h"
#include "common/uuid.h"
#include "kv/record.h"

namespace pond::catalog {

using namespace std::chrono;
using namespace pond::common;

namespace {
constexpr const char* TABLES_TABLE = "__tables";
constexpr const char* SNAPSHOTS_TABLE = "__snapshots";
constexpr const char* FILES_TABLE = "__files";
constexpr const char* CURRENT_TABLE = "__current";

}  // namespace

// Constructor
KVCatalog::KVCatalog(std::shared_ptr<pond::kv::DB> db) : db_(std::move(db)) {
    // Initialize the catalog
    auto result = Initialize();
    if (!result.ok()) {
        LOG_ERROR("Failed to initialize KVCatalog: %s", result.error().message().c_str());
    }

    LOG_STATUS("==== KVCatalog initialized ====");
}

// Initialize the catalog
common::Result<void> KVCatalog::Initialize() {
    // Create tables table if it doesn't exist
    auto tables_result = db_->GetTable(TABLES_TABLE);
    if (!tables_result.ok()) {
        auto create_result = db_->CreateTable(TABLES_TABLE, GetTablesTableSchema());
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
        auto create_result = db_->CreateTable(SNAPSHOTS_TABLE, GetSnapshotsTableSchema());
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
        auto create_result = db_->CreateTable(FILES_TABLE, GetFilesTableSchema());
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

    LOG_VERBOSE("Putting record into tables table for key '%s' with value '%s'", key.c_str(), value.c_str());

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

common::Result<void> KVCatalog::PutTableMetadata(bool create_if_not_exists,
                                                 const std::string& name,
                                                 const TableMetadata& metadata) {
    // Create a record for the tables table
    auto record = CreateTableMetadataRecord(name, metadata);

    if (create_if_not_exists) {
        auto put_result = tables_table_->PutIfNotExists(name, std::move(record));
        RETURN_IF_ERROR_T(common::Result<void>, put_result);

        if (!put_result.value()) {
            return common::Result<void>::failure(common::ErrorCode::FileAlreadyExists,
                                                 "Table '" + name + "' already exists");
        }

        LOG_STATUS("Created table '%s' in catalog.", name.c_str());
    } else {
        auto put_result = tables_table_->Put(name, std::move(record));
        RETURN_IF_ERROR_T(common::Result<void>, put_result);

        LOG_STATUS("Updated table '%s' in catalog.", name.c_str());
    }

    return common::Result<void>::success();
}

// Create a new table
common::Result<TableMetadata> KVCatalog::CreateTable(const std::string& name,
                                                     std::shared_ptr<common::Schema> schema,
                                                     const PartitionSpec& spec,
                                                     const std::string& location,
                                                     const std::unordered_map<std::string, std::string>& properties) {
    using ReturnType = common::Result<TableMetadata>;

    auto lock = std::unique_lock(mutex_);

    // Generate a UUID for the table
    TableId table_uuid = GenerateUuid();

    {
        // Create initial table metadata
        TableMetadata initial_metadata(table_uuid, location, schema, properties);
        initial_metadata.last_updated_time = GetCurrentTime();
        initial_metadata.partition_specs.push_back(spec);
        initial_metadata.table_uuid = table_uuid;
        initial_metadata.format_version = 1;
        initial_metadata.current_snapshot_id = -1;
        initial_metadata.location = location;
        initial_metadata.properties = properties;
        initial_metadata.schema = schema;
        initial_metadata.partition_specs = {spec};

        auto put_result = PutTableMetadata(true /* create_if_not_exists */, name, initial_metadata);
        RETURN_IF_ERROR_T(ReturnType, put_result);
    }

    // Create an initial empty snapshot for the table using the existing CreateSnapshot function
    // This establishes the baseline state for the table
    std::vector<DataFile> empty_files;  // No files in the initial snapshot
    auto snapshot_result = CreateSnapshot(name, empty_files, {}, Operation::CREATE);
    if (!snapshot_result.ok()) {
        // If snapshot creation fails, we should clean up the table record
        auto delete_result = tables_table_->Delete(name);
        if (!delete_result.ok()) {
            LOG_ERROR("Failed to clean up table record after snapshot creation failure: %s",
                      delete_result.error().message().c_str());
        }
        return common::Result<TableMetadata>::failure(snapshot_result.error());
    }

    return common::Result<TableMetadata>::success(snapshot_result.value());
}

// Load a table
common::Result<TableMetadata> KVCatalog::LoadTable(const std::string& name) {
    auto lock = std::unique_lock(mutex_);

    // Get the table record
    auto get_result = tables_table_->Get(name);
    if (!get_result.ok()) {
        return common::Result<TableMetadata>::failure(common::ErrorCode::TableNotFoundInCatalog,
                                                      "Table '" + name + "' not found");
    }

    auto& record = get_result.value();

    // Deserialize schema
    auto schema_result = record->Get<common::DataChunk>(7);  // SCHEMA_FIELD
    if (!schema_result.ok()) {
        return common::Result<TableMetadata>::failure(schema_result.error());
    }

    std::shared_ptr<common::Schema> schema = std::make_shared<common::Schema>();
    bool deserialize_success = schema->Deserialize(schema_result.value());
    if (!deserialize_success) {
        return common::Result<TableMetadata>::failure(common::ErrorCode::DeserializationError,
                                                      "Failed to deserialize schema");
    }

    // Get properties
    auto properties_result = record->Get<std::string>(6);  // PROPERTIES_FIELD
    if (!properties_result.ok()) {
        return common::Result<TableMetadata>::failure(properties_result.error());
    }

    auto properties = DeserializePartitionValues(properties_result.value());
    if (!properties.ok()) {
        return common::Result<TableMetadata>::failure(properties.error());
    }

    // Create metadata object
    TableMetadata metadata(record->Get<std::string>(1).value(),  // TABLE_UUID_FIELD
                           record->Get<std::string>(3).value(),  // LOCATION_FIELD
                           schema,
                           properties.value());

    metadata.format_version = record->Get<int32_t>(2).value();               // FORMAT_VERSION_FIELD
    metadata.current_snapshot_id = record->Get<int64_t>(4).value();          // CURRENT_SNAPSHOT_ID_FIELD
    metadata.last_updated_time = record->Get<common::Timestamp>(5).value();  // LAST_UPDATED_TIME_FIELD

    // Deserialize partition specs
    auto specs_result = record->Get<std::string>(8);  // PARTITION_SPECS_FIELD
    if (specs_result.ok()) {
        auto specs = DeserializePartitionSpecs(specs_result.value());
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
        LOG_ERROR("Failed to acquire lock for table '%s': %s", name.c_str(), lock_result.error().message().c_str());
        return common::Result<bool>::failure(common::ErrorCode::Failure,
                                             "Failed to acquire lock for table '" + name + "'");
    }

    auto release_lock = common::ScopeExit([&]() { ReleaseLock(name); });

    // Load current metadata to verify base state
    auto current_result = LoadTable(name);
    if (!current_result.ok()) {
        LOG_ERROR("Failed to load current metadata for table '%s': %s",
                  name.c_str(),
                  current_result.error().message().c_str());
        return common::Result<bool>::failure(current_result.error());
    }

    if (current_result.value().current_snapshot_id != base.current_snapshot_id) {
        LOG_ERROR("Concurrent modification detected. Table was modified since the transaction began.");
        return common::Result<bool>::failure(
            common::ErrorCode::InvalidOperation,
            "Concurrent modification detected. Table was modified since the transaction began.");
    }

    // Save the updated metadata
    auto save_result = PutTableMetadata(false /* create_if_not_exists */, name, updated);
    if (!save_result.ok()) {
        LOG_ERROR(
            "Failed to save table metadata for table '%s': %s", name.c_str(), save_result.error().message().c_str());
        return common::Result<bool>::failure(save_result.error());
    }

    return common::Result<bool>::success(true);
}

// Get table metadata
common::Result<TableMetadata> KVCatalog::GetTableMetadata(const std::string& name, SnapshotId snapshot_id) {
    std::string metadata_key = GetTableMetadataKey(name, snapshot_id);
    auto get_result = GetRecordValue(metadata_key);
    if (!get_result.ok()) {
        LOG_ERROR("Failed to get table metadata for table '%s' key '%s': %s",
                  name.c_str(),
                  metadata_key.c_str(),
                  get_result.error().message().c_str());
        return common::Result<TableMetadata>::failure(get_result.error());
    }

    return DeserializeTableMetadata(get_result.value());
}

// Lock management
common::Result<bool> KVCatalog::AcquireLock(const std::string& name, int64_t timeout_ms) {
    auto lock_key = GetTableLockKey(name);
    auto start_time = GetCurrentTime();

    while (GetCurrentTime() - start_time < timeout_ms) {
        // Try to create the lock record
        auto record = std::make_unique<kv::Record>(tables_table_->schema());
        record->Set(0, lock_key);
        record->Set(1, common::DataChunk::FromString("locked"));

        auto put_result = tables_table_->PutIfNotExists(lock_key, std::move(record));
        RETURN_IF_ERROR_T(common::Result<bool>, put_result);

        // If the error is not that the key already exists, return the error
        if (!put_result.value()) {
            // Wait and retry
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            continue;
        }

        return common::Result<bool>::success(true);
    }

    return common::Result<bool>::failure(common::ErrorCode::Timeout,
                                         "Timeout waiting for lock on table '" + name + "'");
}

common::Result<bool> KVCatalog::ReleaseLock(const std::string& name) {
    auto lock_key = GetTableLockKey(name);
    auto delete_result = tables_table_->Delete(lock_key);
    LOG_CHECK(delete_result.ok(), "Failed to release lock for table '" + name + "'");
    return common::Result<bool>::success(true);
}

// Key construction
std::string KVCatalog::GetTableMetadataKey(const std::string& name, SnapshotId snapshot_id) {
    return TABLE_PREFIX + name + META_SUFFIX + std::to_string(snapshot_id);
}

std::string KVCatalog::GetTableLockKey(const std::string& name) {
    return TABLE_PREFIX + name + LOCK_SUFFIX;
}

std::string KVCatalog::GetTableFilesKey(const std::string& name, SnapshotId snapshot_id) {
    return TABLE_PREFIX + name + FILES_SUFFIX + std::to_string(snapshot_id);
}

// Utilities
Timestamp KVCatalog::GetCurrentTime() {
    return now();
}

TableId KVCatalog::GenerateUuid() {
    common::UUID uuid = common::UUID::NewUUID();
    return uuid.ToString();
}

common::Result<void> KVCatalog::AddFilesToSnapshot(const std::string& name,
                                                   SnapshotId snapshot_id,
                                                   const std::vector<DataFile>& files) {
    using ReturnType = common::Result<void>;

    // Store the files
    for (const auto& file : files) {
        auto file_record = std::make_unique<kv::Record>(files_table_->schema());
        file_record->Set(0, name);                             // TABLE_NAME_FIELD
        file_record->Set(1, snapshot_id);                      // SNAPSHOT_ID_FIELD
        file_record->Set(2, file.file_path);                   // FILE_PATH_FIELD
        file_record->Set(3, FileFormatToString(file.format));  // FILE_FORMAT_FIELD
        file_record->Set(4, file.record_count);                // RECORD_COUNT_FIELD
        file_record->Set(5, file.file_size_bytes);             // FILE_SIZE_FIELD

        // Serialize partition values to binary
        rapidjson::Document partition_doc;
        auto& allocator = partition_doc.GetAllocator();
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
        std::string file_key = name + "/" + std::to_string(snapshot_id) + "/" + file.file_path;
        auto file_put_result = files_table_->Put(file_key, std::move(file_record));
        RETURN_IF_ERROR_T(ReturnType, file_put_result);
    }

    return ReturnType::success();
}

std::string GetSummary(const std::vector<DataFile>& added_files, const std::vector<DataFile>& deleted_files) {
    std::unordered_map<std::string, std::string> summary;
    summary["added-files"] = std::to_string(added_files.size());
    summary["deleted-files"] = std::to_string(deleted_files.size());
    summary["total-files"] = std::to_string(added_files.size() - deleted_files.size());

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

    return summary_buffer.GetString();
}

// Create a snapshot for a table
common::Result<TableMetadata> KVCatalog::CreateSnapshot(const std::string& name,
                                                        const std::vector<DataFile>& added_files,
                                                        const std::vector<DataFile>& deleted_files,
                                                        Operation op) {
    using ReturnType = common::Result<TableMetadata>;

    // validate the operation
    switch (op) {
        case Operation::APPEND:
            break;
        case Operation::CREATE:
            break;
        case Operation::DELETE:
        case Operation::OVERWRITE:
        case Operation::REPLACE:
            return ReturnType::failure(common::ErrorCode::NotImplemented, "Operation not implemented");
        default:
            return ReturnType::failure(common::ErrorCode::InvalidOperation, "Invalid operation");
    }

    auto lock = std::unique_lock(mutex_);

    // Load the current table metadata
    auto load_result = LoadTable(name);
    RETURN_IF_ERROR_T(ReturnType, load_result);

    TableMetadata metadata = load_result.value();

    // Generate a new snapshot ID (increment the current one)
    SnapshotId new_snapshot_id = metadata.current_snapshot_id + 1;

    // Create the manifest list path
    std::string manifest_list =
        metadata.location + "/metadata/manifest-list-" + std::to_string(new_snapshot_id) + ".json";

    // Create a record for the snapshots table
    auto snapshot_record = std::make_unique<kv::Record>(snapshots_table_->schema());
    snapshot_record->Set(0, name);                          // TABLE_NAME_FIELD
    snapshot_record->Set(1, new_snapshot_id);               // SNAPSHOT_ID_FIELD
    snapshot_record->Set(2, metadata.current_snapshot_id);  // PARENT_SNAPSHOT_ID_FIELD
    snapshot_record->Set(3, GetCurrentTime());              // TIMESTAMP_MS_FIELD
    snapshot_record->Set(4, OperationToString(op));         // OPERATION_FIELD
    snapshot_record->Set(5, manifest_list);                 // MANIFEST_LIST_FIELD
    snapshot_record->Set(6, common::DataChunk::FromString(GetSummary(added_files, deleted_files)));  // SUMMARY_FIELD

    // Save the snapshot record
    std::string snapshot_key = name + "/" + std::to_string(new_snapshot_id);
    auto snapshot_put_result = snapshots_table_->Put(snapshot_key, std::move(snapshot_record));
    RETURN_IF_ERROR_T(ReturnType, snapshot_put_result);

    // Add the files to the snapshot
    auto add_files_result = AddFilesToSnapshot(name, new_snapshot_id, added_files);
    RETURN_IF_ERROR_T(ReturnType, add_files_result);

    // Update the table metadata with the new snapshot
    metadata.current_snapshot_id = new_snapshot_id;
    metadata.last_updated_time = GetCurrentTime();
    metadata.last_sequence_number++;

    if (op == Operation::CREATE || op == Operation::APPEND) {
        metadata.snapshots.push_back(Snapshot(new_snapshot_id, GetCurrentTime(), op, manifest_list, {}));
    }

    auto table_put_result = PutTableMetadata(false /* create_if_not_exists */, name, metadata);
    RETURN_IF_ERROR_T(ReturnType, table_put_result);

    return common::Result<TableMetadata>::success(metadata);
}

// List data files for a specific snapshot
common::Result<std::vector<DataFile>> KVCatalog::ListDataFiles(const std::string& name,
                                                               const std::optional<SnapshotId>& snapshot_id) {
    auto lock = std::unique_lock(mutex_);

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

    // List all files for this snapshot using prefix scan
    std::string files_prefix = name + "/" + std::to_string(target_snapshot_id) + "/";
    auto iter_result = files_table_->ScanPrefix(files_prefix);
    if (!iter_result.ok()) {
        return common::Result<std::vector<DataFile>>::failure(iter_result.error());
    }

    std::vector<DataFile> files;
    auto iter = iter_result.value();
    for (iter->Seek(files_prefix); iter->Valid(); iter->Next()) {
        const auto& record = iter->value();

        DataFile file;
        file.file_path = record->Get<std::string>(2).value();                     // FILE_PATH_FIELD
        file.format = FileFormatFromString(record->Get<std::string>(3).value());  // FILE_FORMAT_FIELD
        file.record_count = record->Get<int64_t>(4).value();                      // RECORD_COUNT_FIELD
        file.file_size_bytes = record->Get<int64_t>(5).value();                   // FILE_SIZE_FIELD

        // Get partition values
        auto partition_values_result = record->Get<common::DataChunk>(6);  // PARTITION_VALUES_FIELD
        if (partition_values_result.ok()) {
            file.partition_values = DeserializePartitionValues(partition_values_result.value().ToString()).value();
        }

        files.push_back(std::move(file));
    }

    return common::Result<std::vector<DataFile>>::success(files);
}

// Update the schema for a table
common::Result<TableMetadata> KVCatalog::UpdateSchema(const std::string& name,
                                                      std::shared_ptr<common::Schema> new_schema) {
    using ReturnType = common::Result<TableMetadata>;
    // Load the current table metadata
    auto current_result = LoadTable(name);
    if (!current_result.ok()) {
        LOG_ERROR(
            "Failed to load table metadata for table '%s': %s", name.c_str(), current_result.error().message().c_str());
        return current_result;
    }

    TableMetadata current = current_result.value();
    TableMetadata updated = current;

    // Update the schema
    updated.schema = new_schema;
    updated.last_updated_time = GetCurrentTime();

    // Commit the transaction
    auto commit_result = CommitTransaction(name, current, updated);
    RETURN_IF_ERROR_T(ReturnType, commit_result);

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
    updated.last_updated_time = GetCurrentTime();

    // Commit the transaction
    auto commit_result = CommitTransaction(name, current, updated);
    if (!commit_result.ok()) {
        return common::Result<TableMetadata>::failure(commit_result.error());
    }

    return common::Result<TableMetadata>::success(updated);
}

// Drop a table
common::Result<bool> KVCatalog::DropTable(const std::string& name) {
    using ReturnType = common::Result<bool>;

    auto lock = std::unique_lock(mutex_);

    // Load the table metadata
    auto metadata_result = LoadTable(name);
    if (!metadata_result.ok()) {
        return common::Result<bool>::failure(common::ErrorCode::TableNotFoundInCatalog,
                                             "Table '" + name + "' not found");
    }

    // Delete the main table record
    auto delete_table = tables_table_->Delete(name);
    RETURN_IF_ERROR_T(ReturnType, delete_table);

    // Delete all snapshot records
    for (const auto& snapshot : metadata_result.value().snapshots) {
        std::string snapshot_key = name + "/" + std::to_string(snapshot.snapshot_id);
        auto delete_snapshot = snapshots_table_->Delete(snapshot_key);
        if (!delete_snapshot.ok()) {
            LOG_ERROR("Failed to delete snapshot record: %s", delete_snapshot.error().message().c_str());
        }
    }

    // Delete all file records
    std::string files_prefix = name + "/";
    auto iter_result = files_table_->ScanPrefix(files_prefix);
    if (iter_result.ok()) {
        auto iter = iter_result.value();
        for (iter->Seek(files_prefix); iter->Valid(); iter->Next()) {
            auto delete_file = files_table_->Delete(iter->key());
            if (!delete_file.ok()) {
                LOG_ERROR("Failed to delete file record: %s", delete_file.error().message().c_str());
            }
        }
    }

    return common::Result<bool>::success(true);
}

// Rename a table
common::Result<bool> KVCatalog::RenameTable(const std::string& name, const std::string& new_name) {
    using ReturnType = common::Result<bool>;

    // Make sure the new name doesn't already exist
    auto new_table_result = LoadTable(new_name);
    if (new_table_result.ok()) {
        return common::Result<bool>::failure(common::ErrorCode::FileAlreadyExists,
                                             "Table '" + new_name + "' already exists");
    }

    // Load the current table metadata
    auto current_result = LoadTable(name);
    RETURN_IF_ERROR_T(ReturnType, current_result);

    // Acquire locks for both tables
    auto lock_result = AcquireLock(name);
    if (!lock_result.ok() || !lock_result.value()) {
        return common::Result<bool>::failure(common::ErrorCode::Failure,
                                             "Failed to acquire lock for table '" + name + "'");
    }

    auto release_lock = common::ScopeExit([&]() { ReleaseLock(name); });

    auto lock_new_result = AcquireLock(new_name);
    if (!lock_new_result.ok() || !lock_new_result.value()) {
        return common::Result<bool>::failure(common::ErrorCode::Failure,
                                             "Failed to acquire lock for new table name '" + new_name + "'");
    }

    auto release_lock_new = common::ScopeExit([&]() { ReleaseLock(new_name); });

    // Save metadata with new name
    auto metadata = current_result.value();
    auto save_result = PutTableMetadata(false /* create_if_not_exists */, new_name, metadata);
    RETURN_IF_ERROR_T(ReturnType, save_result);

    // Delete old table entry
    auto delete_result = tables_table_->Delete(name);
    RETURN_IF_ERROR_T(ReturnType, delete_result);

    return common::Result<bool>::success(true);
}

// Update table properties
common::Result<TableMetadata> KVCatalog::UpdateTableProperties(
    const std::string& name, const std::unordered_map<std::string, std::string>& updates) {
    using ReturnType = common::Result<TableMetadata>;
    // Load the current table metadata
    auto current_result = LoadTable(name);
    RETURN_IF_ERROR_T(ReturnType, current_result);

    const TableMetadata& current = current_result.value();
    TableMetadata updated = current;

    // Update properties
    for (const auto& [key, value] : updates) {
        updated.properties[key] = value;
    }

    updated.last_updated_time = GetCurrentTime();

    // Commit the transaction
    auto commit_result = CommitTransaction(name, current, updated);
    RETURN_IF_ERROR_T(ReturnType, commit_result);

    return common::Result<TableMetadata>::success(updated);
}

}  // namespace pond::catalog
