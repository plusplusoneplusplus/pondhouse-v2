#pragma once

#include <atomic>
#include <memory>
#include <mutex>

#include "common/data_chunk.h"
#include "common/result.h"
#include "common/skip_list.h"
#include "common/types.h"

namespace pond::kv {

// Constants
static constexpr size_t DEFAULT_MEMTABLE_SIZE = 64 * 1024 * 1024;  // 64MB

class MemTable {
public:
    using Key = std::string;
    using Value = common::DataChunk;

    explicit MemTable(size_t max_size = DEFAULT_MEMTABLE_SIZE);
    ~MemTable() = default;

    // Core operations
    common::Result<void> Put(const Key& key, const Value& value);
    common::Result<Value> Get(const Key& key) const;
    common::Result<void> Delete(const Key& key);

    // Size management
    size_t ApproximateMemoryUsage() const;
    bool ShouldFlush() const;
    size_t GetEntryCount() const;

    // Iterator interface
    class Iterator {
    public:
        explicit Iterator(common::SkipList<Key, Value>::Iterator* it, std::mutex& mutex) : iter_(it), mutex_(mutex) {}
        ~Iterator() = default;

        // Thread-safe operations
        bool Valid() const {
            std::lock_guard<std::mutex> lock(mutex_);
            return iter_->Valid();
        }

        common::Result<void> Next() {
            std::lock_guard<std::mutex> lock(mutex_);
            if (!iter_->Valid()) {
                return common::Result<void>::failure(common::ErrorCode::InvalidOperation, "Iterator is not valid");
            }
            iter_->Next();
            return common::Result<void>::success();
        }

        common::Result<void> Seek(const Key& key) {
            if (key.empty()) {
                return common::Result<void>::failure(common::ErrorCode::InvalidArgument, "Cannot seek to empty key");
            }
            std::lock_guard<std::mutex> lock(mutex_);
            iter_->Seek(key);
            return common::Result<void>::success();
        }

        common::Result<Key> key() const {
            std::lock_guard<std::mutex> lock(mutex_);
            if (!iter_->Valid()) {
                return common::Result<Key>::failure(common::ErrorCode::InvalidOperation, "Iterator is not valid");
            }
            return common::Result<Key>::success(iter_->key());
        }

        common::Result<std::reference_wrapper<const Value>> value() const {
            std::lock_guard<std::mutex> lock(mutex_);
            if (!iter_->Valid()) {
                return common::Result<std::reference_wrapper<const Value>>::failure(common::ErrorCode::InvalidOperation,
                                                                                    "Iterator is not valid");
            }
            return common::Result<std::reference_wrapper<const Value>>::success(std::cref(iter_->value()));
        }

    private:
        std::unique_ptr<common::SkipList<Key, Value>::Iterator> iter_;
        std::mutex& mutex_;  // Reference to the mutex for thread-safe operations
    };

    std::unique_ptr<Iterator> NewIterator() const;

private:
    size_t CalculateEntrySize(const Key& key, const Value& value) const;

    std::unique_ptr<common::SkipList<Key, Value>> table_;
    mutable std::mutex mutex_;
    std::atomic<size_t> approximate_memory_usage_{0};
    size_t max_size_;
};

}  // namespace pond::kv
