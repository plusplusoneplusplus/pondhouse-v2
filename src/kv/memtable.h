#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <optional>

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
    common::Result<void> Put(const Key& key,
                             const RawValue& value,
                             uint64_t txn_id,
                             std::optional<common::HybridTime> version = std::nullopt);

    common::Result<RawValue> Get(const Key& key, common::HybridTime read_time) const;
    common::Result<RawValue> GetForTxn(const Key& key, uint64_t txn_id) const;
    common::Result<void> Delete(const Key& key,
                                uint64_t txn_id,
                                std::optional<common::HybridTime> version = std::nullopt);

    // Size management
    size_t ApproximateMemoryUsage() const;
    bool ShouldFlush() const;
    size_t GetEntryCount() const;

    // Iterator interface
    class Iterator : public common::Iterator<Key, Value> {
    public:
        explicit Iterator(std::unique_ptr<common::Iterator<Key, Value>> iter,
                          std::mutex& mutex,
                          common::IteratorMode mode)
            : common::Iterator<Key, Value>(mode), iter_(std::move(iter)), mutex_(mutex) {
            AdvanceToValidRecord();
        }

        ~Iterator() override = default;

        void Seek(const Key& target) override {
            std::lock_guard<std::mutex> lock(mutex_);
            iter_->Seek(target);
            AdvanceToValidRecord();
        }

        void Next() override {
            std::lock_guard<std::mutex> lock(mutex_);
            iter_->Next();
            AdvanceToValidRecord();
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

        bool IsTombstone() const override {
            std::lock_guard<std::mutex> lock(mutex_);
            return iter_->value()->IsDeleted();
        }

    private:
        // Advances the iterator until it finds a non-deleted record or reaches the end
        void AdvanceToValidRecord() {
            while (iter_->Valid()) {
                const auto& version_chain = iter_->value();
                // Check if the latest version is a deletion marker
                if (!version_chain->IsDeleted()
                    || common::CheckIteratorMode(mode_, common::IteratorMode::IncludeTombstones)) {
                    break;
                }
                iter_->Next();
            }
        }

        std::unique_ptr<common::Iterator<Key, Value>> iter_;
        std::mutex& mutex_;
    };

    // SnapshotIterator interface
    class SnapshotIterator : public common::SnapshotIterator<Key, RawValue> {
    public:
        explicit SnapshotIterator(std::unique_ptr<common::Iterator<Key, Value>> iter,
                                  std::mutex& mutex,
                                  common::HybridTime read_time,
                                  common::IteratorMode mode)
            : common::SnapshotIterator<Key, RawValue>(read_time, mode), iter_(std::move(iter)), mutex_(mutex) {
            AdvanceToValidRecord();
        }

        ~SnapshotIterator() noexcept override = default;

        void Seek(const Key& target) override {
            std::lock_guard<std::mutex> lock(mutex_);
            iter_->Seek(target);
            AdvanceToValidRecord();
        }

        void Next() override {
            std::lock_guard<std::mutex> lock(mutex_);
            iter_->Next();
            AdvanceToValidRecord();
        }

        bool Valid() const override {
            std::lock_guard<std::mutex> lock(mutex_);
            return iter_->Valid() && current_value_.has_value();
        }

        const Key& key() const override {
            std::lock_guard<std::mutex> lock(mutex_);
            return iter_->key();
        }

        const RawValue& value() const override {
            std::lock_guard<std::mutex> lock(mutex_);
            return current_value_.value();
        }

        bool IsTombstone() const override {
            std::lock_guard<std::mutex> lock(mutex_);
            return current_is_tombstone_;
        }

        common::HybridTime version() const override {
            std::lock_guard<std::mutex> lock(mutex_);
            return current_version_;
        }

    private:
        void AdvanceToValidRecord() {
            while (iter_->Valid()) {
                std::shared_ptr<const DataChunkVersionedValue> current = iter_->value();

                while (current && current->version() > read_time_) {
                    current = current->prev_version();
                }

                if (current) {
                    current_version_ = current->version();
                    current_is_tombstone_ = current->IsDeleted();

                    if (!current_is_tombstone_
                        || common::CheckIteratorMode(mode_, common::IteratorMode::IncludeTombstones)) {
                        current_value_ = current->value();
                        return;
                    }
                }
                iter_->Next();
            }
            current_value_.reset();
        }

        std::shared_ptr<common::Iterator<Key, Value>> iter_;
        std::mutex& mutex_;
        std::optional<RawValue> current_value_;
        common::HybridTime current_version_{common::MinHybridTime()};
        bool current_is_tombstone_{false};
    };

    std::shared_ptr<MemTable::Iterator> NewIterator(common::IteratorMode mode = common::IteratorMode::Default) const;

    std::shared_ptr<MemTable::SnapshotIterator> NewSnapshotIterator(
        common::HybridTime read_time = common::MaxHybridTime(),
        common::IteratorMode mode = common::IteratorMode::Default) const;

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
