#include "kv/kv_table.h"

#include <sstream>

#include "common/time.h"

namespace pond::kv {

KvTable::KvTable(std::shared_ptr<common::IAppendOnlyFileSystem> fs, const std::string& table_name, size_t max_wal_size)
    : fs_(std::move(fs)), table_name_(table_name), max_wal_size_(max_wal_size) {
    // Initialize WAL
    wal_ = std::make_shared<common::WAL<KvEntry>>(fs_);
    auto result = wal_->open(GetWALPath(current_wal_sequence_));
    if (!result.ok()) {
        throw std::runtime_error("Failed to open WAL: " + result.error().message());
    }

    // Initialize active memtable
    active_memtable_ = std::make_unique<MemTable>();

    // Initialize metadata state machine
    metadata_state_machine_ = std::make_shared<TableMetadataStateMachine>(fs_, table_name_ + "_metadata");
    auto open_result = metadata_state_machine_->Open();
    if (!open_result.ok()) {
        throw std::runtime_error("Failed to open metadata state machine: " + open_result.error().message());
    }

    // Initialize SSTable manager
    sstable_manager_ = std::make_unique<SSTableManager>(fs_, table_name_, metadata_state_machine_);
}

common::Result<void> KvTable::Put(const std::string& key, const common::DataChunk& value, bool acquire_lock) {
    std::optional<std::lock_guard<std::mutex>> lock;
    if (acquire_lock) {
        lock.emplace(mutex_);
    }

    // Create and write WAL entry first
    KvEntry entry(key, value, common::INVALID_LSN, common::now(), EntryType::Put);
    auto wal_result = WriteToWAL(entry);
    if (!wal_result.ok()) {
        return common::Result<void>::failure(wal_result.error());
    }

    // Check if memtable needs to be flushed
    if (active_memtable_->ShouldFlush()) {
        auto switch_result = SwitchMemTable();
        if (!switch_result.ok()) {
            return switch_result;
        }
    }

    // Add to memtable
    return active_memtable_->Put(key, value);
}

common::Result<common::DataChunk> KvTable::Get(const std::string& key, bool acquire_lock) const {
    std::optional<std::lock_guard<std::mutex>> lock;
    if (acquire_lock) {
        lock.emplace(mutex_);
    }

    // Try memtable first
    auto result = active_memtable_->Get(key);
    if (result.ok()) {
        return result;
    }

    // Try SSTables
    return sstable_manager_->Get(key);
}

common::Result<void> KvTable::Delete(const std::string& key, bool acquire_lock) {
    std::optional<std::lock_guard<std::mutex>> lock;
    if (acquire_lock) {
        lock.emplace(mutex_);
    }

    // Create and write WAL entry
    KvEntry entry(key, common::DataChunk(), common::INVALID_LSN, common::now(), EntryType::Delete);
    auto wal_result = WriteToWAL(entry);
    if (!wal_result.ok()) {
        return common::Result<void>::failure(wal_result.error());
    }

    // Add to memtable
    return active_memtable_->Delete(key);
}

common::Result<void> KvTable::BatchPut(const std::vector<std::pair<std::string, common::DataChunk>>& entries) {
    std::lock_guard<std::mutex> lock(mutex_);

    for (const auto& [key, value] : entries) {
        auto result = Put(key, value, false /* acquire_lock */);
        if (!result.ok()) {
            return result;
        }
    }

    return common::Result<void>::success();
}

common::Result<std::vector<common::Result<common::DataChunk>>> KvTable::BatchGet(
    const std::vector<std::string>& keys) const {
    std::lock_guard<std::mutex> lock(mutex_);

    std::vector<common::Result<common::DataChunk>> results;
    results.reserve(keys.size());

    for (const auto& key : keys) {
        results.push_back(Get(key, false /* acquire_lock */));
    }

    return common::Result<std::vector<common::Result<common::DataChunk>>>::success(std::move(results));
}

common::Result<void> KvTable::BatchDelete(const std::vector<std::string>& keys) {
    std::lock_guard<std::mutex> lock(mutex_);

    for (const auto& key : keys) {
        auto result = Delete(key, false /* acquire_lock */);
        if (!result.ok()) {
            return result;
        }
    }

    return common::Result<void>::success();
}

common::Result<bool> KvTable::Recover() {
    std::lock_guard<std::mutex> lock(mutex_);

    // First recover metadata state
    auto metadata_result = metadata_state_machine_->Open();
    if (!metadata_result.ok()) {
        return common::Result<bool>::failure(metadata_result.error());
    }

    if (!wal_) {
        return common::Result<bool>::success(false);
    }

    // Find all WAL files
    std::vector<std::string> wal_files;
    size_t max_sequence = 0;
    for (size_t i = 0; i < 1000; i++) {  // Limit to prevent infinite loop
        std::string wal_path = GetWALPath(i);
        auto exists = fs_->Exists(wal_path);
        if (!exists) {
            break;
        }
        wal_files.push_back(wal_path);
        max_sequence = i;
    }

    if (wal_files.empty()) {
        return common::Result<bool>::success(false);
    }

    // Set current sequence number to the highest found
    current_wal_sequence_ = max_sequence;

    // Replay WAL files in order
    for (const auto& wal_path : wal_files) {
        // Open WAL file
        auto open_result = wal_->open(wal_path);
        if (!open_result.ok()) {
            return common::Result<bool>::failure(open_result.error());
        }

        // Read all entries
        auto entries = wal_->read(0);
        if (!entries.ok()) {
            return common::Result<bool>::failure(entries.error());
        }

        // Replay entries
        for (const auto& entry : entries.value()) {
            switch (entry.type) {
                case EntryType::Put:
                    active_memtable_->Put(entry.key, entry.value);
                    break;
                case EntryType::Delete:
                    active_memtable_->Delete(entry.key);
                    break;
                default:
                    return common::Result<bool>::failure(common::ErrorCode::InvalidOperation, "Unknown WAL entry type");
            }

            // Check if memtable needs to be flushed
            if (active_memtable_->ShouldFlush()) {
                auto switch_result = SwitchMemTable();
                if (!switch_result.ok()) {
                    return common::Result<bool>::failure(switch_result.error());
                }
            }
        }

        // Close WAL file
        auto close_result = wal_->close();
        if (!close_result.ok()) {
            return common::Result<bool>::failure(close_result.error());
        }
    }

    // Open the latest WAL file for writing
    auto result = wal_->open(GetWALPath(current_wal_sequence_));
    if (!result.ok()) {
        return common::Result<bool>::failure(result.error());
    }

    return common::Result<bool>::success(true);
}

common::Result<void> KvTable::Flush() {
    std::lock_guard<std::mutex> lock(mutex_);
    return SwitchMemTable();
}

common::Result<common::LSN> KvTable::WriteToWAL(KvEntry& entry) {
    // Check if WAL needs rotation
    auto size_result = fs_->Size(wal_->handle());
    if (!size_result.ok()) {
        return common::Result<common::LSN>::failure(size_result.error());
    }

    if (size_result.value() >= max_wal_size_) {
        auto rotate_result = RotateWAL();
        if (!rotate_result.ok()) {
            return common::Result<common::LSN>::failure(rotate_result.error());
        }
    }

    auto result = wal_->append(entry);
    if (!result.ok()) {
        return common::Result<common::LSN>::failure(result.error());
    }

    return common::Result<common::LSN>::success(entry.lsn());
}

common::Result<void> KvTable::SwitchMemTable() {
    // Create a new memtable
    auto new_memtable = std::make_unique<MemTable>();

    // Flush current memtable to SSTable
    auto flush_result = sstable_manager_->CreateSSTableFromMemTable(*active_memtable_);
    if (!flush_result.ok()) {
        return common::Result<void>::failure(flush_result.error());
    }

    // Track the flush operation in metadata
    std::vector<FileInfo> files;
    files.emplace_back(flush_result.value());
    auto track_result = TrackMetadataOp(MetadataOpType::FlushMemTable, files);
    if (!track_result.ok()) {
        return track_result;
    }

    // Switch to new memtable
    active_memtable_ = std::move(new_memtable);

    return common::Result<void>::success();
}

common::Result<void> KvTable::RotateWAL() {
    // Track the WAL rotation in metadata
    auto track_result = TrackMetadataOp(MetadataOpType::RotateWAL);
    if (!track_result.ok()) {
        return track_result;
    }

    // Close current WAL
    auto close_result = wal_->close();
    if (!close_result.ok()) {
        return common::Result<void>::failure(close_result.error());
    }

    // Increment sequence number and open new WAL
    current_wal_sequence_++;
    auto open_result = wal_->open(GetWALPath(current_wal_sequence_));
    if (!open_result.ok()) {
        return common::Result<void>::failure(open_result.error());
    }

    return common::Result<void>::success();
}

std::string KvTable::GetWALPath(size_t sequence_number) const {
    return table_name_ + ".wal." + std::to_string(sequence_number);
}

common::Result<void> KvTable::TrackMetadataOp(MetadataOpType op_type, const std::vector<FileInfo>& files) {
    TableMetadataEntry entry(op_type, files);
    return metadata_state_machine_->Apply(entry.Serialize());
}

}  // namespace pond::kv