#include "kv/memtable.h"

#include <stdexcept>

#include "common/time.h"

using namespace pond::common;

namespace pond::kv {

MemTable::MemTable(std::shared_ptr<Schema> schema, std::shared_ptr<common::WAL<KvEntry>> wal, size_t max_size)
    : schema_(std::move(schema)),
      table_(std::make_unique<common::SkipList<Key, std::unique_ptr<Record>>>()),
      approximate_memory_usage_(0),
      max_size_(max_size),
      wal_(std::move(wal)) {}

common::Result<void> MemTable::WriteToWAL(KvEntry& entry) {
    if (!wal_) {
        return common::Result<void>::success();
    }
    auto result = wal_->append(entry);
    if (!result.ok()) {
        return common::Result<void>::failure(result.error().code(), result.error().message());
    }

    return common::Result<void>::success();
}

common::Result<bool> MemTable::Recover() {
    if (!wal_) {
        return Result<bool>::success(false);
    }

    // Read all entries from WAL
    auto entries = wal_->read(0);
    if (!entries.ok()) {
        return Result<bool>::failure(entries.error().code(), entries.error().message());
    }

    if (entries.value().empty()) {
        return Result<bool>::success(false);
    }

    // Replay all entries
    for (const auto& entry : entries.value()) {
        switch (entry.type) {
            case EntryType::Put: {
                // Create a record from the entry value
                auto record_result = Record::Deserialize(entry.value, schema_);
                if (!record_result.ok()) {
                    return Result<bool>::failure(record_result.error().code(), record_result.error().message());
                }

                auto result = Put(entry.key, record_result.value());
                if (!result.ok()) {
                    return Result<bool>::failure(result.error().code(), result.error().message());
                }
                break;
            }
            case EntryType::Delete: {
                auto result = Delete(entry.key);
                if (!result.ok()) {
                    return Result<bool>::failure(result.error().code(), result.error().message());
                }
                break;
            }
            default:
                return Result<bool>::failure(ErrorCode::InvalidOperation, "Unknown WAL entry type");
        }
    }

    return Result<bool>::success(true);
}

common::Result<void> MemTable::Put(const Key& key, const std::unique_ptr<Record>& record) {
    if (key.size() > MAX_KEY_SIZE) {
        return common::Result<void>::failure(common::ErrorCode::InvalidArgument,
                                             "Key size exceeds maximum allowed size");
    }
    if (record->schema() != schema_) {
        return common::Result<void>::failure(common::ErrorCode::InvalidOperation, "Schema mismatch");
    }

    if (approximate_memory_usage_ > max_size_) {
        return common::Result<void>::failure(common::ErrorCode::InvalidOperation, "MemTable is full");
    }

    // Create and write WAL entry first
    if (wal_) {
        KvEntry entry(key, record->Serialize(), INVALID_LSN, common::now(), EntryType::Put);
        RETURN_IF_ERROR(WriteToWAL(entry));
    }

    size_t entry_size = CalculateEntrySize(key, *record);

    {
        std::lock_guard<std::mutex> lock(mutex_);
        // Check if we're replacing an existing entry
        std::unique_ptr<Record> old_record;
        if (table_->Get(key, old_record)) {
            size_t old_size = CalculateEntrySize(key, *old_record);
            approximate_memory_usage_ -= old_size;
        }

        table_->Insert(key, std::make_unique<Record>(*record));
        approximate_memory_usage_ += entry_size;
    }

    return common::Result<void>::success();
}

common::Result<std::unique_ptr<Record>> MemTable::Get(const Key& key) const {
    std::unique_ptr<Record> record;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!table_->Get(key, record)) {
            return common::Result<std::unique_ptr<Record>>::failure(common::ErrorCode::NotFound, "Key not found");
        }

        // Check if this is a tombstone (all values are null)
        bool is_tombstone = true;
        for (size_t i = 0; i < schema_->num_columns(); i++) {
            if (!record->IsNull(i)) {
                is_tombstone = false;
                break;
            }
        }
        if (is_tombstone) {
            return common::Result<std::unique_ptr<Record>>::failure(common::ErrorCode::NotFound, "Key was deleted");
        }
    }
    return common::Result<std::unique_ptr<Record>>::success(std::move(record));
}

common::Result<void> MemTable::Delete(const Key& key) {
    auto tombstone = std::make_unique<Record>(schema_);  // Empty record represents a tombstone
    size_t entry_size = CalculateEntrySize(key, *tombstone);

    // Create and write WAL entry first
    if (wal_) {
        KvEntry entry(key, common::DataChunk(), INVALID_LSN, common::now(), EntryType::Delete);
        RETURN_IF_ERROR(WriteToWAL(entry));
    }

    {
        std::lock_guard<std::mutex> lock(mutex_);
        // Check if we're replacing an existing entry
        std::unique_ptr<Record> old_record;
        if (table_->Get(key, old_record)) {
            size_t old_size = CalculateEntrySize(key, *old_record);
            approximate_memory_usage_ -= old_size;
        }

        if (approximate_memory_usage_ + entry_size > max_size_) {
            return common::Result<void>::failure(common::ErrorCode::InvalidOperation, "MemTable is full");
        }

        table_->Insert(key, std::move(tombstone));
        approximate_memory_usage_ += entry_size;
    }

    return common::Result<void>::success();
}

common::Result<void> MemTable::UpdateColumn(const Key& key,
                                            const std::string& column_name,
                                            const common::DataChunk& value) {
    // Create and write WAL entry first
    if (wal_) {
        KvEntry entry(key, value, INVALID_LSN, common::now(), EntryType::Put);
        RETURN_IF_ERROR(WriteToWAL(entry));
    }

    std::lock_guard<std::mutex> lock(mutex_);

    int col_idx = schema_->GetColumnIndex(column_name);
    if (col_idx == -1) {
        return common::Result<void>::failure(common::ErrorCode::NotFound, "Column not found");
    }

    std::unique_ptr<Record> record;
    if (!table_->Get(key, record)) {
        record = std::make_unique<Record>(schema_);
    } else {
        // Check if this is a tombstone
        bool is_tombstone = true;
        for (size_t i = 0; i < schema_->num_columns(); i++) {
            if (!record->IsNull(i)) {
                is_tombstone = false;
                break;
            }
        }
        if (is_tombstone) {
            record = std::make_unique<Record>(schema_);
        }
    }

    record->Set(col_idx, value);
    size_t entry_size = CalculateEntrySize(key, *record);

    // If we're updating an existing record, we've already subtracted its size
    // when we retrieved it earlier. If it's a new record, we don't need to subtract anything.

    if (approximate_memory_usage_ + entry_size > max_size_) {
        return common::Result<void>::failure(common::ErrorCode::InvalidOperation, "MemTable is full");
    }

    table_->Insert(key, std::move(record));
    approximate_memory_usage_ += entry_size;

    return common::Result<void>::success();
}

common::Result<common::DataChunk> MemTable::GetColumn(const Key& key, const std::string& column_name) const {
    std::lock_guard<std::mutex> lock(mutex_);

    int col_idx = schema_->GetColumnIndex(column_name);
    if (col_idx == -1) {
        return common::Result<common::DataChunk>::failure(common::ErrorCode::NotFound, "Column not found");
    }

    std::unique_ptr<Record> record;
    if (!table_->Get(key, record)) {
        return common::Result<common::DataChunk>::failure(common::ErrorCode::NotFound, "Key not found");
    }

    // Check if this is a tombstone
    bool is_tombstone = true;
    for (size_t i = 0; i < schema_->num_columns(); i++) {
        if (!record->IsNull(i)) {
            is_tombstone = false;
            break;
        }
    }
    if (is_tombstone) {
        return common::Result<common::DataChunk>::failure(common::ErrorCode::NotFound, "Key was deleted");
    }

    if (record->IsNull(col_idx)) {
        return common::Result<common::DataChunk>::failure(common::ErrorCode::InvalidOperation, "Column is null");
    }

    return common::Result<common::DataChunk>::success(record->Get<common::DataChunk>(col_idx).value());
}

size_t MemTable::ApproximateMemoryUsage() const {
    return approximate_memory_usage_.load(std::memory_order_relaxed);
}

bool MemTable::ShouldFlush() const {
    return approximate_memory_usage_.load(std::memory_order_relaxed) >= max_size_;
}

size_t MemTable::CalculateEntrySize(const Key& key, const Record& record) const {
    // Size calculation includes:
    // 1. Key size (string data + string overhead)
    // 2. Serialized record size
    // 3. Skip list node overhead (pointers + height)
    // 4. Memory allocator overhead
    constexpr size_t kStringOverhead = 24;     // std::string overhead on 64-bit systems
    constexpr size_t kNodeOverhead = 64;       // Skip list node overhead (approximate)
    constexpr size_t kAllocatorOverhead = 16;  // Typical memory allocator overhead

    return key.size() + kStringOverhead + record.Serialize().size() + kNodeOverhead + kAllocatorOverhead;
}

// Iterator implementation

std::unique_ptr<MemTable::Iterator> MemTable::NewIterator() const {
    return std::make_unique<Iterator>(table_->NewIterator(), mutex_);
}

}  // namespace pond::kv
