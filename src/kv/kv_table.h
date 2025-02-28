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

// Forward declaration
class KvTableIterator;

/**
 * KvTable provides a schema-agnostic key-value store interface.
 * It uses string keys and DataChunk values, providing raw byte storage
 * without schema validation or column awareness.
 */
class KvTable {
    friend class KvTableTest;

public:
    using Iterator = KvTableIterator;

    explicit KvTable(std::shared_ptr<common::IAppendOnlyFileSystem> fs,
                     const std::string& table_name,
                     size_t max_wal_size = DEFAULT_WAL_SIZE);
    ~KvTable() = default;

    // Core operations
    common::Result<void> Put(const std::string& key, const common::DataChunk& value, bool acquire_lock = true);
    common::Result<common::DataChunk> Get(const std::string& key, bool acquire_lock = true) const;
    common::Result<void> Delete(const std::string& key, bool acquire_lock = true);

    // Prefix scan operation
    common::Result<std::shared_ptr<Iterator>> ScanPrefix(
        const std::string& prefix, common::IteratorMode mode = common::IteratorMode::Default) const;

    // Batch operations
    common::Result<void> BatchPut(const std::vector<std::pair<std::string, common::DataChunk>>& entries);
    common::Result<std::vector<common::Result<common::DataChunk>>> BatchGet(const std::vector<std::string>& keys) const;
    common::Result<void> BatchDelete(const std::vector<std::string>& keys);

    // Recovery and maintenance
    common::Result<bool> Recover();
    common::Result<void> Flush();
    common::Result<void> RotateWAL();

    // Iterator creation
    common::Result<std::shared_ptr<Iterator>> NewIterator(common::HybridTime read_time = common::MaxHybridTime(),
                                                          common::IteratorMode mode = common::IteratorMode::Default,
                                                          const std::string& prefix = "") const;

private:
    // Write entry to WAL and return LSN
    common::Result<common::LSN> WriteToWAL(KvEntry& entry);

    // Switch to a new MemTable, flushing the current one if needed
    common::Result<void> SwitchMemTable();

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
    size_t next_wal_sequence_{0};
};

/**
 * KvTableIterator combines the current memtable iterator with the SSTableManager's
 * union iterator to provide a unified view of all data in the KV table.
 * It follows the same versioning and tombstone rules as the underlying iterators.
 */
class KvTableIterator : public common::SnapshotIterator<std::string, common::DataChunk> {
public:
    KvTableIterator(std::shared_ptr<MemTable::SnapshotIterator> memtable_iter,
                    std::shared_ptr<SSTableManager::Iterator> sstable_iter,
                    common::HybridTime read_time,
                    common::IteratorMode mode,
                    const std::string& prefix = "")
        : SnapshotIterator<std::string, common::DataChunk>(read_time, mode) {
        // Set up L0 iterators (just the memtable)
        std::vector<std::shared_ptr<common::SnapshotIterator<std::string, common::DataChunk>>> l0_iters;
        l0_iters.push_back(memtable_iter);

        // Set up L1+ iterators (the sstable iterator is already a union of all SSTables)
        std::vector<std::vector<std::shared_ptr<common::SnapshotIterator<std::string, common::DataChunk>>>> level_iters;
        std::vector<std::shared_ptr<common::SnapshotIterator<std::string, common::DataChunk>>> l1_iters;
        l1_iters.push_back(sstable_iter);
        level_iters.push_back(l1_iters);

        // Create the union iterator with prefix support
        union_iter_ = std::make_unique<common::UnionIterator<std::string, common::DataChunk>>(
            l0_iters, level_iters, read_time, mode, prefix);
    }

    // All iterator methods simply delegate to the union iterator
    void Seek(const std::string& target) override { union_iter_->Seek(target); }
    void Next() override { union_iter_->Next(); }
    bool Valid() const override { return union_iter_->Valid(); }
    const std::string& key() const override { return union_iter_->key(); }
    const common::DataChunk& value() const override { return union_iter_->value(); }
    bool IsTombstone() const override { return union_iter_->IsTombstone(); }
    common::HybridTime version() const override { return union_iter_->version(); }

private:
    std::unique_ptr<common::UnionIterator<std::string, common::DataChunk>> union_iter_;
};

}  // namespace pond::kv