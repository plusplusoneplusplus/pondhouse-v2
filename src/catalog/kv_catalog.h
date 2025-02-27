#pragma once

#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "catalog/catalog.h"
#include "catalog/metadata.h"
#include "common/error.h"
#include "common/result.h"
#include "kv/db.h"
#include "kv/table.h"

namespace pond::catalog {

/**
 * KVCatalog is an implementation of the Catalog interface that uses a key-value database
 * for persistent storage. It provides an Iceberg-like transactional catalog with snapshot
 * isolation and schema evolution capabilities.
 */
class KVCatalog : public Catalog {
    FRIEND_TEST(KVCatalogTest, AcquireAndReleaseLock);

public:
    static constexpr const char* TABLE_PREFIX = "table/";
    static constexpr const char* META_SUFFIX = "/meta/";
    static constexpr const char* CURRENT_SUFFIX = "/current";
    static constexpr const char* LOCK_SUFFIX = "/lock";
    static constexpr const char* FILES_SUFFIX = "/files/";

    /**
     * Constructor for KVCatalog
     * @param db The key-value database to use for storage
     */
    explicit KVCatalog(std::shared_ptr<pond::kv::DB> db);

    ~KVCatalog() override = default;

    // Table operations
    common::Result<TableMetadata> CreateTable(
        const std::string& name,
        std::shared_ptr<common::Schema> schema,
        const PartitionSpec& spec,
        const std::string& location,
        const std::unordered_map<std::string, std::string>& properties = {}) override;

    common::Result<TableMetadata> LoadTable(const std::string& name) override;

    common::Result<bool> DropTable(const std::string& name) override;

    common::Result<bool> RenameTable(const std::string& name, const std::string& new_name) override;

    common::Result<TableMetadata> UpdateTableProperties(
        const std::string& name, const std::unordered_map<std::string, std::string>& updates) override;

    // Transaction support
    common::Result<bool> CommitTransaction(const std::string& name,
                                           const TableMetadata& base,
                                           const TableMetadata& updated) override;

    // Snapshot management
    common::Result<TableMetadata> CreateSnapshot(const std::string& name,
                                                 const std::vector<DataFile>& added_files,
                                                 const std::vector<DataFile>& deleted_files = {},
                                                 Operation op = Operation::APPEND) override;

    common::Result<std::vector<DataFile>> ListDataFiles(
        const std::string& name, const std::optional<SnapshotId>& snapshot_id = std::nullopt) override;

    // Schema evolution
    common::Result<TableMetadata> UpdateSchema(const std::string& name,
                                               std::shared_ptr<common::Schema> new_schema) override;

    // Partition spec evolution
    common::Result<TableMetadata> UpdatePartitionSpec(const std::string& name, const PartitionSpec& new_spec) override;

protected:
    // Initialize the catalog tables
    common::Result<void> Initialize();

    // Helper methods for record operations
    common::Result<void> PutRecord(const std::string& key, const std::string& value);
    common::Result<std::string> GetRecordValue(const std::string& key);

    // Table metadata operations
    common::Result<void> SaveTableMetadata(const TableMetadata& metadata);
    common::Result<TableMetadata> GetTableMetadata(const std::string& name, SnapshotId snapshot_id);
    common::Result<SnapshotId> GetCurrentSnapshotId(const std::string& name);

    // Lock management
    common::Result<bool> AcquireLock(const std::string& name, int64_t timeout_ms = 5000);
    common::Result<bool> ReleaseLock(const std::string& name);

    // Key construction
    std::string GetTableMetadataKey(const std::string& name, SnapshotId snapshot_id);
    std::string GetTableCurrentKey(const std::string& name);
    std::string GetTableLockKey(const std::string& name);
    std::string GetTableFilesKey(const std::string& name, SnapshotId snapshot_id);

    // Utilities
    common::Timestamp GetCurrentTime();
    TableId GenerateUuid();

    // Member variables
    std::shared_ptr<pond::kv::DB> db_;
    std::shared_ptr<pond::kv::Table> tables_table_;
    std::shared_ptr<pond::kv::Table> snapshots_table_;
    std::shared_ptr<pond::kv::Table> files_table_;
    std::recursive_mutex mutex_;
};

}  // namespace pond::catalog