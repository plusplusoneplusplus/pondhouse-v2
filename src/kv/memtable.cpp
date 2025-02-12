#include "kv/memtable.h"

#include <stdexcept>

#include "common/time.h"
#include "kv/kv_entry.h"

using namespace pond::common;

namespace pond::kv {

MemTable::MemTable(size_t max_size)
    : table_(std::make_unique<common::SkipList<Key, Value>>()), approximate_memory_usage_(0), max_size_(max_size) {}

common::Result<void> MemTable::Put(const Key& key, const Value& value) {
    if (key.size() > MAX_KEY_SIZE) {
        return common::Result<void>::failure(common::ErrorCode::InvalidArgument,
                                             "Key size exceeds maximum allowed size");
    }
    if (value.Size() > MAX_VALUE_SIZE) {
        return common::Result<void>::failure(common::ErrorCode::InvalidArgument,
                                             "Value size exceeds maximum allowed size");
    }

    if (ShouldFlush()) {
        return common::Result<void>::failure(common::ErrorCode::InvalidOperation, "MemTable is full");
    }

    size_t entry_size = CalculateEntrySize(key, value);

    {
        std::lock_guard<std::mutex> lock(mutex_);

        // Check if we're replacing an existing entry
        Value old_value;
        if (table_->Get(key, old_value)) {
            size_t old_size = CalculateEntrySize(key, old_value);
            approximate_memory_usage_ -= old_size;
        }

        table_->Insert(key, std::move(value));
        approximate_memory_usage_ += entry_size;
    }

    return common::Result<void>::success();
}

common::Result<MemTable::Value> MemTable::Get(const Key& key) const {
    Value value;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!table_->Get(key, value)) {
            return common::Result<Value>::failure(common::ErrorCode::NotFound, "Key not found");
        }

        // Check if this is a tombstone (empty value)
        if (value.Empty()) {
            return common::Result<Value>::failure(common::ErrorCode::NotFound, "Key was deleted");
        }
    }
    return common::Result<Value>::success(std::move(value));
}

common::Result<void> MemTable::Delete(const Key& key) {
    Value tombstone;  // Empty DataChunk represents a tombstone
    size_t entry_size = CalculateEntrySize(key, tombstone);

    {
        std::lock_guard<std::mutex> lock(mutex_);
        // Check if we're replacing an existing entry
        Value old_value;
        if (table_->Get(key, old_value)) {
            size_t old_size = CalculateEntrySize(key, old_value);
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
    // 2. Serialized value size
    // 3. Skip list node overhead (pointers + height)
    // 4. Memory allocator overhead
    constexpr size_t kStringOverhead = 24;     // std::string overhead on 64-bit systems
    constexpr size_t kNodeOverhead = 64;       // Skip list node overhead (approximate)
    constexpr size_t kAllocatorOverhead = 16;  // Typical memory allocator overhead

    return key.size() + kStringOverhead + value.Size() + kNodeOverhead + kAllocatorOverhead;
}

// Iterator implementation

std::unique_ptr<MemTable::Iterator> MemTable::NewIterator() const {
    return std::make_unique<Iterator>(table_->NewIterator(), mutex_);
}

}  // namespace pond::kv
