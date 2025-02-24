#include "kv/memtable.h"

#include <stdexcept>

#include "common/time.h"
#include "kv/kv_entry.h"

using namespace pond::common;

namespace pond::kv {

MemTable::MemTable(size_t max_size)
    : table_(std::make_unique<common::SkipList<Key, Value>>()), approximate_memory_usage_(0), max_size_(max_size) {}

common::Result<void> MemTable::Put(const Key& key,
                                   const RawValue& value,
                                   uint64_t txn_id,
                                   std::optional<common::HybridTime> version) {
    if (key.size() > MAX_KEY_SIZE) {
        return common::Result<void>::failure(common::ErrorCode::InvalidArgument,
                                             "Key size exceeds maximum allowed size");
    }
    if (value.Size() > MAX_VALUE_SIZE) {
        return common::Result<void>::failure(common::ErrorCode::InvalidArgument,
                                             "Value size exceeds maximum allowed size");
    }

    if (key.empty()) {
        return common::Result<void>::failure(common::ErrorCode::InvalidArgument, "Key cannot be empty");
    }

    if (value.Empty()) {
        return common::Result<void>::failure(common::ErrorCode::InvalidArgument, "Value cannot be empty");
    }

    if (ShouldFlush()) {
        return common::Result<void>::failure(common::ErrorCode::InvalidOperation, "MemTable is full");
    }

    std::lock_guard<std::mutex> lock(mutex_);

    // Get current version chain if it exists
    Value current_version;
    table_->Get(key, current_version);

    // Create new version
    auto version_time = version.value_or(common::GetNextHybridTime());
    Value new_version;
    if (current_version) {
        new_version = current_version->CreateNewVersion(value, version_time, txn_id);
    } else {
        new_version = std::make_shared<DataChunkVersionedValue>(value, version_time, txn_id);
    }

    // Calculate size change
    size_t entry_size = CalculateEntrySize(key, new_version);
    if (current_version) {
        size_t old_size = CalculateEntrySize(key, current_version);
        approximate_memory_usage_ -= old_size;
    }

    // Insert new version
    table_->Insert(key, std::move(new_version));
    approximate_memory_usage_ += entry_size;

    return common::Result<void>::success();
}

common::Result<MemTable::RawValue> MemTable::Get(const Key& key, common::HybridTime read_time) const {
    std::lock_guard<std::mutex> lock(mutex_);

    if (key.empty()) {
        return common::Result<RawValue>::failure(common::ErrorCode::InvalidArgument, "Key cannot be empty");
    }

    Value version_chain;
    if (!table_->Get(key, version_chain)) {
        return common::Result<RawValue>::failure(common::ErrorCode::NotFound, "Key not found");
    }

    auto value_ref = version_chain->GetValueAt(read_time);
    if (!value_ref) {
        return common::Result<RawValue>::failure(common::ErrorCode::NotFound, "No visible version at specified time");
    }

    return common::Result<RawValue>::success(value_ref->get());
}

common::Result<MemTable::RawValue> MemTable::GetForTxn(const Key& key, uint64_t txn_id) const {
    std::lock_guard<std::mutex> lock(mutex_);

    if (key.empty()) {
        return common::Result<RawValue>::failure(common::ErrorCode::InvalidArgument, "Key cannot be empty");
    }

    Value version_chain;
    if (!table_->Get(key, version_chain)) {
        return common::Result<RawValue>::failure(common::ErrorCode::NotFound, "Key not found");
    }

    auto value_ref = version_chain->GetValueForTxn(txn_id);
    if (!value_ref) {
        return common::Result<RawValue>::failure(common::ErrorCode::NotFound, "No visible version for transaction");
    }

    return common::Result<RawValue>::success(value_ref->get());
}

common::Result<void> MemTable::Delete(const Key& key, uint64_t txn_id, std::optional<common::HybridTime> version) {
    if (ShouldFlush()) {
        return common::Result<void>::failure(common::ErrorCode::InvalidOperation, "MemTable is full");
    }

    std::lock_guard<std::mutex> lock(mutex_);

    // Get current version chain if it exists
    Value current_version;
    table_->Get(key, current_version);  // Don't check return value since we always write a marker

    // Create deletion marker
    auto version_time = version.value_or(common::GetNextHybridTime());
    auto deletion_version = current_version ? current_version->CreateDeletionMarker(version_time, txn_id)
                                            : std::make_shared<DataChunkVersionedValue>(
                                                  DataChunk(), version_time, txn_id, true /*is_deleted*/);

    // Calculate size change
    size_t old_size = current_version ? CalculateEntrySize(key, current_version) : 0;
    size_t new_size = CalculateEntrySize(key, deletion_version);
    approximate_memory_usage_ = approximate_memory_usage_ - old_size + new_size;

    // Insert deletion marker
    table_->Insert(key, std::move(deletion_version));

    return common::Result<void>::success();
}

size_t MemTable::ApproximateMemoryUsage() const {
    return approximate_memory_usage_.load(std::memory_order_relaxed);
}

size_t MemTable::GetEntryCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return table_->Size();
}

bool MemTable::ShouldFlush() const {
    return approximate_memory_usage_.load(std::memory_order_relaxed) >= max_size_;
}

size_t MemTable::CalculateEntrySize(const Key& key, const Value& value) const {
    // Size calculation includes:
    // 1. Key size (string data + string overhead)
    // 2. VersionedValue overhead (timestamp, txn_id, etc.)
    // 3. Value data size
    // 4. Skip list node overhead (pointers + height)
    // 5. Memory allocator overhead
    constexpr size_t kStringOverhead = 24;     // std::string overhead on 64-bit systems
    constexpr size_t kVersionOverhead = 32;    // VersionedValue overhead (approximate)
    constexpr size_t kNodeOverhead = 64;       // Skip list node overhead (approximate)
    constexpr size_t kAllocatorOverhead = 16;  // Typical memory allocator overhead

    return key.size() + kStringOverhead + kVersionOverhead + value->value().Size() + kNodeOverhead + kAllocatorOverhead;
}

// Iterator implementation

std::unique_ptr<common::Iterator<Key, MemTable::Value>> MemTable::NewIterator(common::IteratorMode mode) const {
    std::unique_ptr<common::Iterator<Key, Value>> iter;
    iter.reset(table_->NewIterator());
    return std::make_unique<Iterator>(std::move(iter), mutex_, mode);
}

}  // namespace pond::kv
