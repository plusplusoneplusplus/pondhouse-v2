#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "common/append_only_fs.h"
#include "common/data_chunk.h"
#include "common/result.h"
#include "common/wal.h"
#include "common/wal_state_machine.h"
#include "kv/kv_entry.h"
#include "kv/memtable.h"
#include "kv/sstable_manager.h"
#include "kv/table_metadata.h"

namespace pond::kv {

// Constants
static constexpr size_t DEFAULT_WAL_SIZE = 64 * 1024 * 1024;  // 64MB

/**
 * KvTable provides a schema-agnostic key-value store interface.
 * It uses string keys and DataChunk values, providing raw byte storage
 * without schema validation or column awareness.
 */
class KvTable {
public:
    explicit KvTable(std::shared_ptr<common::IAppendOnlyFileSystem> fs,
                     const std::string& table_name,
                     size_t max_wal_size = DEFAULT_WAL_SIZE);
    ~KvTable() = default;

    // Core operations
    common::Result<void> Put(const std::string& key, const common::DataChunk& value, bool acquire_lock = true);
    common::Result<common::DataChunk> Get(const std::string& key, bool acquire_lock = true) const;
    common::Result<void> Delete(const std::string& key, bool acquire_lock = true);

    // Batch operations
    common::Result<void> BatchPut(const std::vector<std::pair<std::string, common::DataChunk>>& entries);
    common::Result<std::vector<common::Result<common::DataChunk>>> BatchGet(const std::vector<std::string>& keys) const;
    common::Result<void> BatchDelete(const std::vector<std::string>& keys);

    // Recovery and maintenance
    common::Result<bool> Recover();
    common::Result<void> Flush();

private:
    // Write entry to WAL and return LSN
    common::Result<common::LSN> WriteToWAL(KvEntry& entry);

    // Switch to a new MemTable, flushing the current one if needed
    common::Result<void> SwitchMemTable();

    // Create a new WAL file
    common::Result<void> RotateWAL();

    // Get the WAL file path with sequence number
    std::string GetWALPath(size_t sequence_number) const;

    // Track metadata operation
    common::Result<void> TrackMetadataOp(MetadataOpType op_type, const std::vector<FileInfo>& files = {});

    std::shared_ptr<common::IAppendOnlyFileSystem> fs_;
    std::string table_name_;
    std::unique_ptr<MemTable> active_memtable_;
    std::shared_ptr<common::WAL<KvEntry>> wal_;
    std::unique_ptr<SSTableManager> sstable_manager_;
    std::shared_ptr<TableMetadataStateMachine> metadata_state_machine_;
    mutable std::mutex mutex_;  // For thread-safe MemTable switching
    size_t max_wal_size_;
    size_t current_wal_sequence_{0};
};

}  // namespace pond::kv