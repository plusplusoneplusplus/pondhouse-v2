#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "common/append_only_fs.h"
#include "common/result.h"
#include "common/wal.h"
#include "kv/kv_entry.h"
#include "kv/memtable.h"
#include "kv/record.h"
#include "kv/sstable_manager.h"

namespace pond::kv {

// Constants
static constexpr size_t DEFAULT_WAL_SIZE = 64 * 1024 * 1024;  // 64MB

class Table {
public:
    explicit Table(std::shared_ptr<Schema> schema,
                   std::shared_ptr<common::IAppendOnlyFileSystem> fs,
                   const std::string& table_name,
                   size_t max_wal_size = DEFAULT_WAL_SIZE);
    ~Table() = default;

    // Core operations
    common::Result<void> Put(const Key& key, std::unique_ptr<Record> record, bool acquire_lock = true);
    common::Result<std::unique_ptr<Record>> Get(const Key& key, bool acquire_lock = true) const;
    common::Result<void> Delete(const Key& key);
    common::Result<void> UpdateColumn(const Key& key, const std::string& column_name, const common::DataChunk& value);

    // Recovery
    common::Result<bool> Recover();

    // Flush operations
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

    std::shared_ptr<Schema> schema_;
    std::shared_ptr<common::IAppendOnlyFileSystem> fs_;
    std::string table_name_;
    std::unique_ptr<MemTable> active_memtable_;
    std::shared_ptr<common::WAL<KvEntry>> wal_;
    std::unique_ptr<SSTableManager> sstable_manager_;
    mutable std::mutex mutex_;  // For thread-safe MemTable switching
    size_t max_wal_size_;
    size_t current_wal_sequence_{0};
};

}  // namespace pond::kv
