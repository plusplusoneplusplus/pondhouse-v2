#pragma once

#include <cstdint>
#include <string>

namespace pond::catalog {

// Basic types
using TableId = std::string;  // UUID string
using SnapshotId = int64_t;
using FieldId = int32_t;
using SchemaId = int32_t;
using PartitionSpecId = int32_t;

//
// NOTE: Update cpp file to reflect the new types
//

// Operation types for snapshots
enum class Operation { CREATE, APPEND, REPLACE, DELETE, OVERWRITE };

// Partition transforms
enum class Transform {
    // individual fields
    IDENTITY,
    // date/time fields
    YEAR,
    MONTH,
    DAY,
    HOUR,
    // bucket fields
    BUCKET,
    // truncate fields
    TRUNCATE
};

// File format types
enum class FileFormat { UNKNOWN, PARQUET };

// Convert enums to string for serialization
std::string OperationToString(Operation op);
Operation OperationFromString(const std::string& str);

std::string TransformToString(Transform transform);
Transform TransformFromString(const std::string& str);

std::string FileFormatToString(FileFormat format);
FileFormat FileFormatFromString(const std::string& str);

}  // namespace pond::catalog