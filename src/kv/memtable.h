#pragma once

#include <atomic>
#include <memory>
#include <mutex>

#include "common/data_chunk.h"
#include "common/result.h"
#include "common/skip_list.h"
#include "kv/record.h"

namespace pond::kv {

// Constants
static constexpr size_t DEFAULT_MEMTABLE_SIZE = 64 * 1024 * 1024;  // 64MB
static constexpr size_t MAX_KEY_SIZE = 1024;                       // 1KB

class MemTable {
public:
    using Key = std::string;

    explicit MemTable(std::shared_ptr<Schema> schema, size_t max_size = DEFAULT_MEMTABLE_SIZE);
    ~MemTable() = default;

    // Core operations
    common::Result<void> Put(const Key& key, const std::unique_ptr<Record>& record);
    common::Result<std::unique_ptr<Record>> Get(const Key& key) const;
    common::Result<void> Delete(const Key& key);

    // Column operations
    common::Result<void> UpdateColumn(const Key& key, const std::string& column_name, const common::DataChunk& value);
    common::Result<common::DataChunk> GetColumn(const Key& key, const std::string& column_name) const;

    // Size management
    size_t ApproximateMemoryUsage() const;
    bool ShouldFlush() const;

    // Schema access
    const std::shared_ptr<Schema>& schema() const { return schema_; }

    // Iterator interface
    class Iterator {
    public:
        explicit Iterator(common::SkipList<Key, std::unique_ptr<Record>>::Iterator* it,
                          std::mutex& mutex)
            : iter_(it), mutex_(mutex) {}
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

        common::Result<std::reference_wrapper<const Record>> record() const {
            std::lock_guard<std::mutex> lock(mutex_);
            if (!iter_->Valid()) {
                return common::Result<std::reference_wrapper<const Record>>::failure(
                    common::ErrorCode::InvalidOperation, "Iterator is not valid");
            }
            return common::Result<std::reference_wrapper<const Record>>::success(std::cref(*iter_->value()));
        }

    private:
        std::unique_ptr<common::SkipList<Key, std::unique_ptr<Record>>::Iterator> iter_;
        std::mutex& mutex_;  // Reference to the mutex for thread-safe operations
    };

    std::unique_ptr<Iterator> NewIterator() const;

private:
    std::shared_ptr<Schema> schema_;
    std::unique_ptr<common::SkipList<Key, std::unique_ptr<Record>>> table_;
    std::atomic<size_t> approximate_memory_usage_;
    const size_t max_size_;
    mutable std::mutex mutex_;  // Mutable to allow locking in const member functions

    size_t CalculateEntrySize(const Key& key, const Record& record) const;
};

}  // namespace pond::kv
