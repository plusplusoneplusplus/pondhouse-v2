#include "kv/kv_table.h"

#include <sstream>

#include "common/log.h"
#include "common/time.h"

namespace pond::kv {

KvTable::KvTable(std::shared_ptr<common::IAppendOnlyFileSystem> fs, const std::string& table_name, size_t max_wal_size)
    : fs_(std::move(fs)), table_name_(table_name), max_wal_size_(max_wal_size) {
    // Initialize WAL
    wal_ = std::make_shared<common::WAL<KvEntry>>(fs_);

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

    Recover();
}

common::Result<void> KvTable::Put(const std::string& key, const common::DataChunk& value, bool acquire_lock) {
    using ReturnType = common::Result<void>;
    std::optional<std::lock_guard<std::mutex>> lock;
    if (acquire_lock) {
        lock.emplace(mutex_);
    }

    // Create and write WAL entry first
    KvEntry entry(key, value, common::INVALID_LSN, common::now(), EntryType::Put);
    auto wal_result = WriteToWAL(entry);
    RETURN_IF_ERROR_T(ReturnType, wal_result);

    // Check if memtable needs to be flushed
    if (active_memtable_->ShouldFlush()) {
        auto switch_result = SwitchMemTable();
        RETURN_IF_ERROR_T(ReturnType, switch_result);
    }

    // Add to memtable
    return active_memtable_->Put(key, value, 0 /* txn_id */);
}

common::Result<common::DataChunk> KvTable::Get(const std::string& key, bool acquire_lock) const {
    std::optional<std::lock_guard<std::mutex>> lock;
    if (acquire_lock) {
        lock.emplace(mutex_);
    }

    // Try memtable first
    auto result = active_memtable_->Get(key, common::GetNextHybridTime());
    if (result.ok()) {
        return result;
    }

    // Try SSTables
    return sstable_manager_->Get(key);
}

common::Result<void> KvTable::Delete(const std::string& key, bool acquire_lock) {
    using ReturnType = common::Result<void>;
    std::optional<std::lock_guard<std::mutex>> lock;
    if (acquire_lock) {
        lock.emplace(mutex_);
    }

    // Create and write WAL entry
    KvEntry entry(key, common::DataChunk(), common::INVALID_LSN, common::now(), EntryType::Delete);
    auto wal_result = WriteToWAL(entry);
    RETURN_IF_ERROR_T(ReturnType, wal_result);

    // Add to memtable
    return active_memtable_->Delete(key, 0 /* txn_id */);
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
    using ReturnType = common::Result<void>;
    std::lock_guard<std::mutex> lock(mutex_);

    for (const auto& key : keys) {
        auto result = Delete(key, false /* acquire_lock */);
        RETURN_IF_ERROR_T(ReturnType, result);
    }

    return common::Result<void>::success();
}

common::Result<bool> KvTable::Recover() {
    using ReturnType = common::Result<bool>;
    std::lock_guard<std::mutex> lock(mutex_);

    if (!metadata_state_machine_->HasHistory()) {
        auto rotate_result = RotateWAL();
        RETURN_IF_ERROR_T(ReturnType, rotate_result);
        LOG_STATUS("Initialized the kv table");
        return common::Result<bool>::success(true);
    }

    // Find all WAL files
    std::vector<std::string> wal_files;
    for (size_t sequence : metadata_state_machine_->GetActiveLogSequences()) {
        std::string wal_path = GetWALPath(sequence);
        auto exists = fs_->Exists(wal_path);
        if (!exists) {
            break;
        }
        wal_files.push_back(wal_path);
    }

    if (wal_files.empty()) {
        LOG_STATUS("No active WAL files found.");
        return common::Result<bool>::success(true);
    }

    // Set current sequence number to the highest found
    next_wal_sequence_ = metadata_state_machine_->GetActiveLogSequences().front();

    // Replay WAL files in order
    for (const auto& wal_path : wal_files) {
        // Open WAL file
        auto open_result = wal_->Open(wal_path);
        RETURN_IF_ERROR_T(ReturnType, open_result);

        // Read all entries
        auto entries = wal_->Read(0);
        RETURN_IF_ERROR_T(ReturnType, entries);

        // Replay entries
        for (const auto& entry : entries.value()) {
            switch (entry.type) {
                case EntryType::Put:
                    active_memtable_->Put(entry.key, entry.value, 0 /* txn_id */);
                    break;
                case EntryType::Delete:
                    active_memtable_->Delete(entry.key, 0 /* txn_id */);
                    break;
                default:
                    return common::Result<bool>::failure(common::ErrorCode::InvalidOperation, "Unknown WAL entry type");
            }

            next_wal_sequence_ += 1;

            // Check if memtable needs to be flushed
            if (active_memtable_->ShouldFlush()) {
                auto switch_result = SwitchMemTable();
                RETURN_IF_ERROR_T(ReturnType, switch_result);
            }
        }

        // Close WAL file, except for the last one
        if (&wal_path != &wal_files.back()) {
            auto close_result = wal_->Close();
            RETURN_IF_ERROR_T(ReturnType, close_result);
        }
    }

    return common::Result<bool>::success(true);
}

common::Result<void> KvTable::Flush() {
    std::lock_guard<std::mutex> lock(mutex_);
    return SwitchMemTable();
}

common::Result<common::LSN> KvTable::WriteToWAL(KvEntry& entry) {
    using ReturnType = common::Result<common::LSN>;

    auto result = wal_->Append(entry);
    RETURN_IF_ERROR_T(ReturnType, result);

    LOG_CHECK(result.value() == next_wal_sequence_, "WAL sequence number mismatch");
    next_wal_sequence_++;

    // Check if WAL needs rotation
    auto size_result = fs_->Size(wal_->handle());
    RETURN_IF_ERROR_T(ReturnType, size_result);

    if (size_result.value() >= max_wal_size_) {
        auto rotate_result = RotateWAL();
        RETURN_IF_ERROR_T(ReturnType, rotate_result);
    }

    return common::Result<common::LSN>::success(entry.lsn());
}

common::Result<void> KvTable::SwitchMemTable() {
    using ReturnType = common::Result<void>;
    // Create a new memtable
    auto new_memtable = std::make_unique<MemTable>();

    // Flush current memtable to SSTable
    auto flush_result = sstable_manager_->CreateSSTableFromMemTable(*active_memtable_);
    RETURN_IF_ERROR_T(ReturnType, flush_result);

    // NOTE: sstable_manager_ will track the flush operation in metadata state machine
    // so we don't need to do anything here

    LOG_VERBOSE("Flushed memtable to SSTable, seq number=%llu, table state=%s",
                next_wal_sequence_,
                metadata_state_machine_->ToString().c_str());

    // Switch to new memtable
    active_memtable_ = std::move(new_memtable);

    return common::Result<void>::success();
}

common::Result<void> KvTable::RotateWAL() {
    using ReturnType = common::Result<void>;
    // Track the WAL rotation in metadata
    auto track_result = TrackMetadataOp(MetadataOpType::RotateWAL);
    RETURN_IF_ERROR_T(ReturnType, track_result);

    // Close current WAL
    auto close_result = wal_->Close();
    RETURN_IF_ERROR_T(ReturnType, close_result);

    // open new WAL
    auto open_result = wal_->Open(GetWALPath(next_wal_sequence_));
    RETURN_IF_ERROR_T(ReturnType, open_result);

    LOG_VERBOSE("Rotated WAL, seq number=%llu, table state=%s",
                next_wal_sequence_,
                metadata_state_machine_->ToString().c_str());

    return common::Result<void>::success();
}

std::string KvTable::GetWALPath(size_t sequence_number) const {
    return table_name_ + ".wal." + std::to_string(sequence_number);
}

common::Result<void> KvTable::TrackMetadataOp(MetadataOpType op_type, const std::vector<FileInfo>& files) {
    TableMetadataEntry entry(op_type, files, {}, next_wal_sequence_);
    return metadata_state_machine_->Apply(entry.Serialize());
}

}  // namespace pond::kv