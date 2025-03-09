#pragma once

#include <memory>
#include <string>
#include <unordered_map>

#include "catalog/metadata.h"
#include "common/result.h"

namespace pond::catalog {

class Catalog {
public:
    virtual ~Catalog() = default;

    // Table operations
    virtual common::Result<TableMetadata> CreateTable(
        const std::string& name,
        std::shared_ptr<common::Schema> schema,
        const PartitionSpec& spec,
        const std::string& location,
        const std::unordered_map<std::string, std::string>& properties = {}) = 0;

    virtual common::Result<TableMetadata> LoadTable(const std::string& name) = 0;

    virtual common::Result<bool> DropTable(const std::string& name) = 0;

    virtual common::Result<bool> RenameTable(const std::string& name, const std::string& new_name) = 0;

    virtual common::Result<TableMetadata> UpdateTableProperties(
        const std::string& name, const std::unordered_map<std::string, std::string>& updates) = 0;

    // Transaction support
    virtual common::Result<bool> CommitTransaction(const std::string& name,
                                                   const TableMetadata& base,
                                                   const TableMetadata& updated) = 0;

    // Snapshot management
    virtual common::Result<TableMetadata> CreateSnapshot(const std::string& name,
                                                         const std::vector<DataFile>& added_files,
                                                         const std::vector<DataFile>& deleted_files = {},
                                                         Operation op = Operation::APPEND) = 0;

    virtual common::Result<std::vector<DataFile>> ListDataFiles(
        const std::string& name, const std::optional<SnapshotId>& snapshot_id = std::nullopt) = 0;

    // Schema evolution
    virtual common::Result<TableMetadata> UpdateSchema(const std::string& name,
                                                       std::shared_ptr<common::Schema> new_schema) = 0;

    // Partition spec evolution
    virtual common::Result<TableMetadata> UpdatePartitionSpec(const std::string& name,
                                                              const PartitionSpec& new_spec) = 0;

    // Table listing
    virtual common::Result<std::vector<std::string>> ListTables() = 0;
};

}  // namespace pond::catalog