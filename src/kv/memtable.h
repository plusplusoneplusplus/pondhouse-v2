#pragma once

#include <atomic>
#include <memory>
#include <mutex>

#include "common/data_chunk.h"
#include "common/result.h"
#include "common/skip_list.h"
#include "common/time.h"
#include "common/types.h"
#include "kv/versioned_value.h"

namespace pond::kv {

// Constants
static constexpr size_t DEFAULT_MEMTABLE_SIZE = 64 * 1024 * 1024;  // 64MB

struct MemTableMetadata {
    uint64_t flush_sequence_{0};

    uint64_t GetFlushSequence() const { return flush_sequence_; }
    void SetFlushSequence(uint64_t sequence) { flush_sequence_ = sequence; }
};

class MemTable {
public:
    using Key = std::string;
    using RawValue = common::DataChunk;
    using Value = std::shared_ptr<DataChunkVersionedValue>;

    explicit MemTable(size_t max_size = DEFAULT_MEMTABLE_SIZE);
    ~MemTable() = default;

    // Core operations
    common::Result<void> Put(const Key& key, const RawValue& value, uint64_t txn_id);
    common::Result<RawValue> Get(const Key& key, common::HybridTime read_time) const;
    common::Result<RawValue> GetForTxn(const Key& key, uint64_t txn_id) const;
    common::Result<void> Delete(const Key& key, uint64_t txn_id);

    // Size management
    size_t ApproximateMemoryUsage() const;
    bool ShouldFlush() const;
    size_t GetEntryCount() const;

    // Iterator interface
    class Iterator : public common::Iterator<Key, Value> {
    public:
        explicit Iterator(std::unique_ptr<common::Iterator<Key, Value>> iter, std::mutex& mutex)
            : iter_(std::move(iter)), mutex_(mutex) {}
        ~Iterator() override = default;

        void Seek(const Key& target) override {
            std::lock_guard<std::mutex> lock(mutex_);
            iter_->Seek(target);
        }

        void Next() override {
            std::lock_guard<std::mutex> lock(mutex_);
            iter_->Next();
        }

        bool Valid() const override {
            std::lock_guard<std::mutex> lock(mutex_);
            return iter_->Valid();
        }

        const Key& key() const override {
            std::lock_guard<std::mutex> lock(mutex_);
            return iter_->key();
        }

        const Value& value() const override {
            std::lock_guard<std::mutex> lock(mutex_);
            return iter_->value();
        }

    private:
        std::unique_ptr<common::Iterator<Key, Value>> iter_;
        std::mutex& mutex_;
    };

    std::unique_ptr<common::Iterator<Key, Value>> NewIterator() const;

    const MemTableMetadata& GetMetadata() const { return metadata_; }
    MemTableMetadata& GetMetadata() { return metadata_; }

private:
    size_t CalculateEntrySize(const Key& key, const Value& value) const;

    std::unique_ptr<common::SkipList<Key, Value>> table_;
    mutable std::mutex mutex_;
    std::atomic<size_t> approximate_memory_usage_{0};
    size_t max_size_;
    MemTableMetadata metadata_;
};

}  // namespace pond::kv
