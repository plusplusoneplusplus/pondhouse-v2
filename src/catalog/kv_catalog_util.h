#pragma once

#include "catalog/catalog.h"
#include "catalog/metadata.h"
#include "common/column_type.h"
#include "common/error.h"
#include "common/result.h"
#include "kv/record.h"

namespace pond::catalog {

//
// JSON conversion helpers
//

/**
 * Serializes a properties map to a JSON string.
 *
 * @param properties The map of property key-value pairs to serialize
 * @return A JSON string representation of the properties
 */
std::string SerializeProperties(const std::unordered_map<std::string, std::string>& properties);

/**
 * Deserializes a JSON string into a properties map.
 *
 * @param json The JSON string to deserialize
 * @param allow_non_string_values Whether to allow non-string values in the JSON (defaults to true)
 * @return A Result containing either the deserialized properties map or an error
 */
common::Result<std::unordered_map<std::string, std::string>> DeserializeProperties(const std::string& json,
                                                                                   bool allow_non_string_values = true);

/**
 * Serializes a vector of partition specifications to a JSON string.
 *
 * @param specs The partition specifications to serialize
 * @return A JSON string representation of the partition specifications
 */
std::string SerializePartitionSpecs(const std::vector<PartitionSpec>& specs);

/**
 * Deserializes a JSON string into a vector of partition specifications.
 *
 * @param json The JSON string to deserialize
 * @return A Result containing either the deserialized partition specifications or an error
 */
common::Result<std::vector<PartitionSpec>> DeserializePartitionSpecs(const std::string& json);

/**
 * Serializes a vector of snapshots to a JSON string.
 *
 * @param snapshots The snapshots to serialize
 * @return A JSON string representation of the snapshots
 */
std::string SerializeSnapshots(const std::vector<Snapshot>& snapshots);

/**
 * Deserializes a JSON string into a vector of snapshots.
 *
 * @param json The JSON string to deserialize
 * @return A Result containing either the deserialized snapshots or an error
 */
common::Result<std::vector<Snapshot>> DeserializeSnapshots(const std::string& json);

/**
 * Serializes a vector of data files to a JSON string.
 *
 * @param files The data files to serialize
 * @return A JSON string representation of the data files
 */
std::string SerializeDataFileList(const std::vector<DataFile>& files);

/**
 * Deserializes a JSON string into a vector of data files.
 *
 * @param json The JSON string to deserialize
 * @return A Result containing either the deserialized data files or an error
 */
common::Result<std::vector<DataFile>> DeserializeDataFileList(const std::string& json);

/**
 * Serializes a map of partition values to a JSON string.
 *
 * @param values The partition values to serialize
 * @return A JSON string representation of the partition values
 */
std::string SerializePartitionValues(const std::unordered_map<std::string, std::string>& values);

/**
 * Deserializes a JSON string into a map of partition values.
 *
 * @param json The JSON string to deserialize
 * @return A Result containing either the deserialized partition values or an error
 */
common::Result<std::unordered_map<std::string, std::string>> DeserializePartitionValues(const std::string& json);

/**
 * Serializes table metadata to a JSON string.
 *
 * @param metadata The table metadata to serialize
 * @return A JSON string representation of the table metadata
 */
std::string SerializeTableMetadata(const TableMetadata& metadata);

/**
 * Deserializes a JSON string into table metadata.
 *
 * @param data The JSON string to deserialize
 * @return A Result containing either the deserialized table metadata or an error
 */
common::Result<TableMetadata> DeserializeTableMetadata(const std::string& data);

/**
 * Serializes a vector of data files to a JSON string.
 *
 * @param files The data files to serialize
 * @return A JSON string representation of the data files
 */
std::string SerializeDataFiles(const std::vector<DataFile>& files);

/**
 * Deserializes a JSON string into a vector of data files.
 *
 * @param data The JSON string to deserialize
 * @return A Result containing either the deserialized data files or an error
 */
common::Result<std::vector<DataFile>> DeserializeDataFiles(const std::string& data);

//
// kv/Record helpers
//

// Tables metadata schema
constexpr const char* TABLE_NAME_FIELD = "table_name";                    // STRING (primary key)
constexpr const char* TABLE_UUID_FIELD = "table_uuid";                    // STRING
constexpr const char* FORMAT_VERSION_FIELD = "format_version";            // INT32
constexpr const char* LOCATION_FIELD = "location";                        // STRING
constexpr const char* CURRENT_SNAPSHOT_ID_FIELD = "current_snapshot_id";  // INT64
constexpr const char* LAST_UPDATED_TIME_FIELD = "last_updated_time";      // INT64
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

std::shared_ptr<common::Schema> GetTablesTableSchema();

std::shared_ptr<common::Schema> GetSnapshotsTableSchema();

std::shared_ptr<common::Schema> GetFilesTableSchema();

/**
 * Creates a record for a table metadata.
 *
 * @param name The name of the table
 * @param metadata The table metadata
 * @return A Result containing either the created record or an error
 */
std::unique_ptr<kv::Record> CreateTableMetadataRecord(const std::string& name, const TableMetadata& metadata);

}  // namespace pond::catalog
