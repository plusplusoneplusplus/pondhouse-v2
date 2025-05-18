#pragma once

#include <cstddef>  // For std::size_t
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "common/append_only_fs.h"
#include "common/data_chunk.h"
#include "common/result.h"
#include "common/wal.h"
#include "common/wal_state_machine.h"
#include "kv/i_kv_table.h"
#include "kv/kv_entry.h"
#include "kv/kv_table_iterator.h"
#include "kv/memtable.h"
#include "kv/sstable_manager.h"
#include "kv/table_metadata.h"

namespace pond::kv {

// Constants
static constexpr std::size_t DEFAULT_WAL_SIZE = 64 * 1024 * 1024;  // 64MB

/**
 * BasicKvTable provides a schema-agnostic key-value store implementation.
 * It uses string keys and DataChunk values, providing raw byte storage
 * without schema validation or column awareness.
 */
class BasicKvTable : public IKvTable {
    friend class KvTableTest;

public:
    using Iterator = KvTableIterator;

    explicit BasicKvTable(std::shared_ptr<common::IAppendOnlyFileSystem> fs,
                          const std::string& table_name,
                          std::size_t max_wal_size = DEFAULT_WAL_SIZE);
    ~BasicKvTable() override = default;

    // Core operations
    common::Result<void> Put(const Key& key, const Value& value, bool acquire_lock = true) override;
    common::Result<Value> Get(const Key& key, bool acquire_lock = true) const override;
    common::Result<void> Delete(const Key& key, bool acquire_lock = true) override;

    // Prefix scan operation
    common::Result<std::shared_ptr<Iterator>> ScanPrefix(
        const std::string& prefix, common::IteratorMode mode = common::IteratorMode::Default) const override;

    // Batch operations
    common::Result<void> BatchPut(const std::vector<std::pair<Key, Value>>& entries) override;
    common::Result<std::vector<common::Result<Value>>> BatchGet(const std::vector<Key>& keys) const override;
    common::Result<void> BatchDelete(const std::vector<Key>& keys) override;

    // Recovery and maintenance
    common::Result<bool> Recover() override;
    common::Result<void> Flush() override;
    common::Result<void> RotateWAL() override;

    // Iterator creation
    common::Result<std::shared_ptr<Iterator>> NewIterator(common::HybridTime read_time = common::MaxHybridTime(),
                                                          common::IteratorMode mode = common::IteratorMode::Default,
                                                          const std::string& prefix = "") const override;

protected:
    // Write entry to WAL and return LSN
    common::Result<common::LSN> WriteToWAL(KvEntry& entry);

    // Switch to a new MemTable, flushing the current one if needed
    common::Result<void> SwitchMemTable();

    // Get the WAL file path with sequence number
    std::string GetWALPath(std::size_t sequence_number) const;

    // Track metadata operation
    common::Result<void> TrackMetadataOp(MetadataOpType op_type, const std::vector<FileInfo>& files = {});

    std::shared_ptr<common::IAppendOnlyFileSystem> fs_;
    std::string table_name_;
    std::unique_ptr<MemTable> active_memtable_;
    std::shared_ptr<common::WAL<KvEntry>> wal_;
    std::unique_ptr<SSTableManager> sstable_manager_;
    std::shared_ptr<TableMetadataStateMachine> metadata_state_machine_;
    mutable std::mutex mutex_;  // For thread-safe MemTable switching
    std::size_t max_wal_size_;
    std::size_t next_wal_sequence_{0};
};

}  // namespace pond::kv