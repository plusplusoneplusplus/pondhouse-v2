#include "kv/table.h"

#include "common/time.h"

namespace pond::kv {

Table::Table(std::shared_ptr<Schema> schema,
             std::shared_ptr<common::IAppendOnlyFileSystem> fs,
             const std::string& table_name)
    : schema_(std::move(schema)), fs_(std::move(fs)), table_name_(table_name) {
    // Initialize WAL
    wal_ = std::make_shared<common::WAL<KvEntry>>(fs_);
    auto result = wal_->open(table_name_ + ".wal");
    if (!result.ok()) {
        throw std::runtime_error("Failed to open WAL: " + result.error().message());
    }

    // Initialize active memtable
    active_memtable_ = std::make_unique<MemTable>(schema_);
}

common::Result<void> Table::Put(const Key& key, std::unique_ptr<Record> record) {
    // Create and write WAL entry first
    KvEntry entry(key, record->Serialize(), common::INVALID_LSN, common::now(), EntryType::Put);
    auto wal_result = WriteToWAL(entry);
    if (!wal_result.ok()) {
        return common::Result<void>::failure(wal_result.error());
    }

    // Apply to memtable
    return active_memtable_->Put(key, record);
}

common::Result<std::unique_ptr<Record>> Table::Get(const Key& key) const {
    return active_memtable_->Get(key);
    // TODO: If not found in memtable, check SSTables
}

common::Result<void> Table::Delete(const Key& key) {
    // Create and write WAL entry first
    KvEntry entry(key, common::DataChunk(), common::INVALID_LSN, common::now(), EntryType::Delete);
    auto wal_result = WriteToWAL(entry);
    if (!wal_result.ok()) {
        return common::Result<void>::failure(wal_result.error());
    }

    return active_memtable_->Delete(key);
}

common::Result<void> Table::UpdateColumn(const Key& key,
                                         const std::string& column_name,
                                         const common::DataChunk& value) {
    // First get the current record
    auto get_result = Get(key);
    if (!get_result.ok()) {
        return common::Result<void>::failure(get_result.error());
    }

    // Update the record
    auto record = std::move(get_result).value();
    record->Set(schema_->GetColumnIndex(column_name), value);

    // Write as a Put operation
    return Put(key, std::move(record));
}

common::Result<bool> Table::Recover() {
    if (!wal_) {
        return common::Result<bool>::success(false);
    }

    // Read all entries from WAL
    auto entries = wal_->read(0);
    if (!entries.ok()) {
        return common::Result<bool>::failure(entries.error());
    }

    if (entries.value().empty()) {
        return common::Result<bool>::success(false);
    }

    // Replay all entries
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
    }

    return common::Result<bool>::success(true);
}

common::Result<void> Table::Flush() {
    // TODO: Implement flushing memtable to SSTable
    return common::Result<void>::success();
}

common::Result<common::LSN> Table::WriteToWAL(KvEntry& entry) {
    if (!wal_) {
        return common::Result<common::LSN>::failure(common::ErrorCode::InvalidOperation, "WAL not initialized");
    }

    auto result = wal_->append(entry);
    if (!result.ok()) {
        return common::Result<common::LSN>::failure(result.error());
    }

    return common::Result<common::LSN>::success(entry.lsn());
}

}  // namespace pond::kv
