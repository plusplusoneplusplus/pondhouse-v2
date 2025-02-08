#include "kv/table.h"

#include <sstream>

#include "common/time.h"

namespace pond::kv {

Table::Table(std::shared_ptr<Schema> schema,
             std::shared_ptr<common::IAppendOnlyFileSystem> fs,
             const std::string& table_name,
             size_t max_wal_size)
    : schema_(std::move(schema)), fs_(std::move(fs)), table_name_(table_name), max_wal_size_(max_wal_size) {
    // Initialize WAL
    wal_ = std::make_shared<common::WAL<KvEntry>>(fs_);
    auto result = wal_->open(GetWALPath(current_wal_sequence_));
    if (!result.ok()) {
        throw std::runtime_error("Failed to open WAL: " + result.error().message());
    }

    // Initialize active memtable
    active_memtable_ = std::make_unique<MemTable>(schema_);

    // Initialize SSTable manager
    sstable_manager_ = std::make_unique<SSTableManager>(fs_, table_name_);

    // Initialize metadata state machine
    metadata_state_machine_ = std::make_unique<TableMetadataStateMachine>(fs_, table_name_ + "_metadata");
    auto open_result = metadata_state_machine_->Open();
    if (!open_result.ok()) {
        throw std::runtime_error("Failed to open metadata state machine: " + open_result.error().message());
    }
}

common::Result<void> Table::Put(const Key& key, std::unique_ptr<Record> record, bool acquire_lock) {
    std::optional<std::lock_guard<std::mutex>> lock;
    if (acquire_lock) {
        lock.emplace(mutex_);
    }

    // Create and write WAL entry first
    KvEntry entry(key, record->Serialize(), common::INVALID_LSN, common::now(), EntryType::Put);
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

    // Apply to memtable
    return active_memtable_->Put(key, record);
}

common::Result<std::unique_ptr<Record>> Table::Get(const Key& key, bool acquire_lock) const {
    std::optional<std::lock_guard<std::mutex>> lock;
    if (acquire_lock) {
        lock.emplace(mutex_);
    }

    // First try memtable
    auto result = active_memtable_->Get(key);
    if (result.ok()) {
        return result;
    }

    // If not found in memtable, check SSTables
    auto sstable_result = sstable_manager_->Get(key);
    if (!sstable_result.ok()) {
        return common::Result<std::unique_ptr<Record>>::failure(sstable_result.error());
    }

    // Deserialize the record
    return Record::Deserialize(sstable_result.value(), schema_);
}

common::Result<void> Table::Delete(const Key& key) {
    std::lock_guard<std::mutex> lock(mutex_);

    // Create and write WAL entry first
    KvEntry entry(key, common::DataChunk(), common::INVALID_LSN, common::now(), EntryType::Delete);
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

    return active_memtable_->Delete(key);
}

common::Result<void> Table::UpdateColumn(const Key& key,
                                         const std::string& column_name,
                                         const common::DataChunk& value) {
    std::lock_guard<std::mutex> lock(mutex_);

    // First get the current record
    auto get_result = Get(key, false /* acquire_lock */);
    if (!get_result.ok()) {
        return common::Result<void>::failure(get_result.error());
    }

    // Update the record
    auto record = std::move(get_result).value();
    record->Set(schema_->GetColumnIndex(column_name), value);

    // Write as a Put operation
    return Put(key, std::move(record), false /* acquire_lock */);
}

common::Result<bool> Table::Recover() {
    std::lock_guard<std::mutex> lock(mutex_);

    if (!wal_) {
        return common::Result<bool>::success(false);
    }

    // Find all WAL files
    std::vector<std::string> wal_files;
    size_t max_sequence = 0;
    for (size_t i = 0; i < 1000; i++) {  // Limit to prevent infinite loop
        std::string wal_path = GetWALPath(i);
        auto exists = fs_->exists(wal_path);
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
                case EntryType::Put: {
                    auto record_result = Record::Deserialize(entry.value, schema_);
                    if (!record_result.ok()) {
                        return common::Result<bool>::failure(record_result.error());
                    }

                    auto result = active_memtable_->Put(entry.key, record_result.value());
                    if (!result.ok()) {
                        return common::Result<bool>::failure(result.error());
                    }
                    break;
                }
                case EntryType::Delete: {
                    auto result = active_memtable_->Delete(entry.key);
                    if (!result.ok()) {
                        return common::Result<bool>::failure(result.error());
                    }
                    break;
                }
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

common::Result<void> Table::Flush() {
    std::lock_guard<std::mutex> lock(mutex_);
    return SwitchMemTable();
}

common::Result<common::LSN> Table::WriteToWAL(KvEntry& entry) {
    if (!wal_) {
        return common::Result<common::LSN>::failure(common::ErrorCode::InvalidOperation, "WAL not initialized");
    }

    // Check if WAL needs rotation
    auto size_result = fs_->size(wal_->handle());
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

common::Result<void> Table::TrackMetadataOp(MetadataOpType op_type, const std::vector<FileInfo>& files) {
    TableMetadataEntry entry(op_type, files);
    return metadata_state_machine_->Apply(entry.Serialize());
}

common::Result<void> Table::SwitchMemTable() {
    // Create a new memtable
    auto new_memtable = std::make_unique<MemTable>(schema_);

    // Flush current memtable to SSTable
    auto flush_result = sstable_manager_->CreateSSTableFromMemTable(*active_memtable_);
    if (!flush_result.ok()) {
        return common::Result<void>::failure(flush_result.error());
    }

    // Track the flush operation in metadata
    std::vector<FileInfo> files;
    // TODO: Get the actual file name and size from SSTableManager
    files.emplace_back("sstable_" + std::to_string(current_wal_sequence_), active_memtable_->GetEntryCount());
    auto track_result = TrackMetadataOp(MetadataOpType::FlushMemTable, files);
    if (!track_result.ok()) {
        return track_result;
    }

    // Switch to new memtable
    active_memtable_ = std::move(new_memtable);

    return common::Result<void>::success();
}

common::Result<void> Table::RotateWAL() {
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

    // Increment sequence number
    current_wal_sequence_++;

    // Open new WAL file
    auto result = wal_->open(GetWALPath(current_wal_sequence_));
    if (!result.ok()) {
        return common::Result<void>::failure(result.error());
    }

    return common::Result<void>::success();
}

std::string Table::GetWALPath(size_t sequence_number) const {
    std::stringstream ss;
    ss << table_name_ << ".wal." << sequence_number;
    return ss.str();
}

}  // namespace pond::kv