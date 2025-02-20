#pragma once

#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "catalog/types.h"
#include "common/schema.h"

namespace pond::catalog {

struct PartitionField {
    FieldId source_id;
    FieldId field_id;
    std::string name;
    Transform transform;
    std::optional<int32_t> transform_param;  // For BUCKET/TRUNCATE

    // Constructor for convenience
    PartitionField(FieldId source_id_,
                   FieldId field_id_,
                   std::string name_,
                   Transform transform_,
                   std::optional<int32_t> transform_param_ = std::nullopt)
        : source_id(source_id_),
          field_id(field_id_),
          name(std::move(name_)),
          transform(transform_),
          transform_param(transform_param_) {}
};

struct PartitionSpec {
    PartitionSpecId spec_id;
    std::vector<PartitionField> fields;

    explicit PartitionSpec(PartitionSpecId spec_id_) : spec_id(spec_id_) {}
};

struct DataFile {
    std::string file_path;
    FileFormat format;
    std::unordered_map<std::string, std::string> partition_values;
    int64_t record_count;
    int64_t file_size_bytes;
    // TODO: Add more fields like column-level stats later

    DataFile(std::string file_path_,
             FileFormat format_,
             std::unordered_map<std::string, std::string> partition_values_,
             int64_t record_count_,
             int64_t file_size_bytes_)
        : file_path(std::move(file_path_)),
          format(format_),
          partition_values(std::move(partition_values_)),
          record_count(record_count_),
          file_size_bytes(file_size_bytes_) {}
};

struct Snapshot {
    SnapshotId snapshot_id;
    int64_t timestamp_ms;
    Operation operation;
    std::string manifest_list;  // Path to manifest list file
    std::unordered_map<std::string, std::string> summary;
    std::optional<SnapshotId> parent_snapshot_id;

    Snapshot(SnapshotId snapshot_id_,
             int64_t timestamp_ms_,
             Operation operation_,
             std::string manifest_list_,
             std::unordered_map<std::string, std::string> summary_,
             std::optional<SnapshotId> parent_snapshot_id_ = std::nullopt)
        : snapshot_id(snapshot_id_),
          timestamp_ms(timestamp_ms_),
          operation(operation_),
          manifest_list(std::move(manifest_list_)),
          summary(std::move(summary_)),
          parent_snapshot_id(parent_snapshot_id_) {}
};

struct TableMetadata {
    static constexpr int32_t kCurrentFormatVersion = 2;

    int32_t format_version;
    TableId table_uuid;
    std::string location;
    int64_t last_sequence_number;
    int64_t last_updated_ms;
    SnapshotId current_snapshot_id;
    std::vector<Snapshot> snapshots;
    std::vector<PartitionSpec> partition_specs;
    std::shared_ptr<common::Schema> schema;
    std::unordered_map<std::string, std::string> properties;

    TableMetadata(TableId table_uuid_,
                  std::string location_,
                  std::shared_ptr<common::Schema> schema_,
                  std::unordered_map<std::string, std::string> properties_ = {})
        : format_version(kCurrentFormatVersion),
          table_uuid(std::move(table_uuid_)),
          location(std::move(location_)),
          last_sequence_number(0),
          last_updated_ms(0),       // Will be set when committing
          current_snapshot_id(-1),  // No snapshot yet
          schema(std::move(schema_)),
          properties(std::move(properties_)) {}
};

}  // namespace pond::catalog