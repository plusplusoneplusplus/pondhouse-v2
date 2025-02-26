#pragma once

#include "catalog/catalog.h"
#include "catalog/metadata.h"
#include "common/error.h"
#include "common/result.h"

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

}  // namespace pond::catalog
