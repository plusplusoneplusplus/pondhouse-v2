#pragma once

#include <memory>
#include <mutex>
#include <atomic>
#include "common/skip_list.h"
#include "common/data_chunk.h"
#include "common/result.h"
#include "kv/record.h"

namespace pond::kv {

// Constants
static constexpr size_t DEFAULT_MEMTABLE_SIZE = 64 * 1024 * 1024;  // 64MB
static constexpr size_t MAX_KEY_SIZE = 1024;  // 1KB

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
        explicit Iterator(common::SkipList<Key, std::unique_ptr<Record>>::Iterator* it)
            : iter_(it) {}
        ~Iterator() = default;

        bool Valid() const { return iter_->Valid(); }
        void Next() { iter_->Next(); }
        void Seek(const Key& key) { iter_->Seek(key); }
        Key key() const { return iter_->key(); }
        const Record& record() const { return *iter_->value(); }

    private:
        std::unique_ptr<common::SkipList<Key, std::unique_ptr<Record>>::Iterator> iter_;
    };

    std::unique_ptr<Iterator> NewIterator() const;

private:
    std::shared_ptr<Schema> schema_;
    std::unique_ptr<common::SkipList<Key, std::unique_ptr<Record>>> table_;
    std::atomic<size_t> approximate_memory_usage_;
    const size_t max_size_;
    mutable std::mutex mutex_;

    size_t CalculateEntrySize(const Key& key, const Record& record) const;
};

} // namespace pond::kv
