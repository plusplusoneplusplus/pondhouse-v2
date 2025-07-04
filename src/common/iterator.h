#pragma once

#include <memory>
#include <queue>
#include <string>
#include <vector>

#include "common/data_chunk.h"
#include "common/result.h"
#include "common/time.h"

namespace pond::common {

enum class IteratorMode : uint64_t {
    Default = 0,
    IncludeTombstones = 1 << 1,
    IncludeAllVersions = 1 << 2,
    PrefixScan = 1 << 3,  // New mode for prefix scanning
};

inline IteratorMode operator|(IteratorMode a, IteratorMode b) {
    return static_cast<IteratorMode>(static_cast<int>(a) | static_cast<int>(b));
}

inline IteratorMode operator&(IteratorMode a, IteratorMode b) {
    return static_cast<IteratorMode>(static_cast<int>(a) & static_cast<int>(b));
}

inline bool CheckIteratorMode(IteratorMode mode, IteratorMode flag) {
    return (mode & flag) == flag;
}

// Helper function to check if a key matches a prefix
template <typename K>
inline bool KeyMatchesPrefix(const K& key, const K& prefix) {
    return key.compare(0, prefix.length(), prefix) == 0;
}

/**
 * Common interface for iterating over key-value pairs in storage.
 * This interface is used by various components like MemTable, SSTable, and KvTable
 * to provide a consistent way to iterate over their contents.
 */
template <typename K, typename V>
class Iterator {
public:
    Iterator(IteratorMode mode) : mode_(mode) {}

    virtual ~Iterator() = default;

    /**
     * Positions the iterator at the first key that is >= target.
     * If no such key exists, positions at end.
     * @param target The key to seek to
     */
    virtual void Seek(const K& target) = 0;

    /**
     * Advances the iterator to the next key.
     * No effect if !Valid().
     */
    virtual void Next() = 0;

    /**
     * Returns whether the iterator is valid (not at end).
     * @return true if the iterator is at a valid entry
     */
    virtual bool Valid() const = 0;

    /**
     * Returns the current key.
     * Must only be called when Valid() returns true.
     * @return The current key
     */
    virtual const K& key() const = 0;

    /**
     * Returns the current value.
     * Must only be called when Valid() returns true.
     * @return The current value
     */
    virtual const V& value() const = 0;

    /**
     * Returns true if the current entry is a tombstone.
     * Must only be called when Valid() returns true.
     * @return true if the entry is a tombstone, false otherwise
     */
    virtual bool IsTombstone() const = 0;

protected:
    Iterator() = default;

    // Prevent copying
    Iterator(const Iterator&) = delete;
    Iterator& operator=(const Iterator&) = delete;

    // Allow moving
    Iterator(Iterator&&) = default;
    Iterator& operator=(Iterator&&) = default;

protected:
    IteratorMode mode_;
};

template <typename K, typename V>
class SnapshotIterator : public Iterator<K, V> {
public:
    SnapshotIterator(HybridTime read_time, IteratorMode mode) : Iterator<K, V>(mode), read_time_(read_time) {}

    virtual common::HybridTime version() const = 0;

protected:
    HybridTime read_time_;
};

// UnionIterator combines multiple SnapshotIterators
// L0 iterators might have overlapping keys and are checked in reverse order (newest first)
// Other level iterators don't have overlapping keys within the same level
template <typename K, typename V>
class UnionIterator : public SnapshotIterator<K, V> {
public:
    UnionIterator(std::vector<std::shared_ptr<SnapshotIterator<K, V>>> l0_iters,
                  std::vector<std::vector<std::shared_ptr<SnapshotIterator<K, V>>>> level_iters,
                  HybridTime read_time,
                  IteratorMode mode,
                  const K& prefix = K())  // Add prefix parameter
        : SnapshotIterator<K, V>(read_time, mode),
          l0_iters_(std::move(l0_iters)),
          level_iters_(std::move(level_iters)),
          prefix_(prefix) {
        RefreshHeap();
    }

    void Seek(const K& target) override {
        // Seek all iterators
        for (auto& iter : l0_iters_) {
            iter->Seek(target);
        }
        for (auto& level : level_iters_) {
            for (auto& iter : level) {
                iter->Seek(target);
            }
        }
        RefreshHeap();
    }

    void Next() override {
        if (!Valid()) {
            return;
        }

        // Modified handling for L0 iterators based on IncludeAllVersions flag
        if (current_is_l0_ && !CheckIteratorMode(this->mode_, IteratorMode::IncludeAllVersions)) {
            // Only advance all L0 iterators when not in IncludeAllVersions mode
            const K& current_key = current_key_;
            for (auto& iter : l0_iters_) {
                if (iter->Valid() && iter->key() == current_key) {
                    iter->Next();
                }
            }
        } else {
            // Default case - just advance current iterator
            current_iter_->Next();
        }

        RefreshHeap();
    }

    bool Valid() const override { return current_iter_ != nullptr; }

    const K& key() const override { return current_key_; }

    const V& value() const override { return current_value_; }

    bool IsTombstone() const override { return current_is_tombstone_; }

    HybridTime version() const override { return current_version_; }

private:
    struct HeapEntry {
        std::shared_ptr<SnapshotIterator<K, V>> iter;
        size_t level;

        bool operator>(const HeapEntry& other) const {
            if (iter->key() != other.iter->key()) {
                return iter->key() > other.iter->key();
            }
            // For same key, lower level has higher priority
            return level > other.level;
        }
    };

    void RefreshHeap() {
        std::priority_queue<HeapEntry, std::vector<HeapEntry>, std::greater<>> heap;

        // Add valid L0 iterators to heap
        for (auto& iter : l0_iters_) {
            if (iter->Valid()) {
                // Skip if we're in prefix scan mode and the key doesn't match the prefix
                if (CheckIteratorMode(this->mode_, IteratorMode::PrefixScan)
                    && !KeyMatchesPrefix(iter->key(), prefix_)) {
                    continue;
                }
                heap.push(HeapEntry{iter, 0});
            }
        }

        // Add valid level iterators to heap
        for (size_t level = 0; level < level_iters_.size(); ++level) {
            for (auto& iter : level_iters_[level]) {
                if (iter->Valid()) {
                    // Skip if we're in prefix scan mode and the key doesn't match the prefix
                    if (CheckIteratorMode(this->mode_, IteratorMode::PrefixScan)
                        && !KeyMatchesPrefix(iter->key(), prefix_)) {
                        continue;
                    }
                    heap.push(HeapEntry{iter, level + 1});
                }
            }
        }

        if (heap.empty()) {
            current_iter_ = nullptr;
            return;
        }

        // Get the smallest key
        const K& smallest_key = heap.top().iter->key();
        current_is_l0_ = (heap.top().level == 0);

        // For L0, we need to find the newest version of this key
        if (current_is_l0_) {
            // Find newest version in L0 for this key
            HybridTime newest_version = MinHybridTime();
            std::shared_ptr<SnapshotIterator<K, V>> newest_iter = nullptr;

            while (!heap.empty() && heap.top().level == 0 && heap.top().iter->key() == smallest_key) {
                auto entry = heap.top();
                heap.pop();

                if (entry.iter->version() > newest_version) {
                    newest_version = entry.iter->version();
                    newest_iter = entry.iter;
                }
            }

            current_iter_ = newest_iter;
            current_key_ = current_iter_->key();
            current_value_ = current_iter_->value();
            current_version_ = current_iter_->version();
            current_is_tombstone_ = current_iter_->IsTombstone();
        } else {
            // For other levels, just take the top entry
            auto entry = heap.top();
            current_iter_ = entry.iter;
            current_key_ = current_iter_->key();
            current_value_ = current_iter_->value();
            current_version_ = current_iter_->version();
            current_is_tombstone_ = current_iter_->IsTombstone();
        }

        // Skip duplicate keys if not in IncludeAllVersions mode
        if (!CheckIteratorMode(this->mode_, IteratorMode::IncludeAllVersions) && current_is_l0_) {
            const K& current_key = current_key_;
            while (!heap.empty() && heap.top().iter->key() == current_key) {
                auto entry = heap.top();
                heap.pop();

                // Advance the iterator to skip the duplicate key
                if (entry.level == 0) {
                    // For L0, advance all iterators with the same key
                    for (auto& iter : l0_iters_) {
                        if (iter->Valid() && iter->key() == current_key) {
                            iter->Next();
                        }
                    }
                } else {
                    // For other levels, just advance the current iterator
                    entry.iter->Next();
                }
            }
        }

        // Skip if it's a tombstone and we're not including tombstones
        if (current_is_tombstone_ && !CheckIteratorMode(this->mode_, IteratorMode::IncludeTombstones)) {
            Next();
            return;
        }

        // If we're in prefix scan mode and the current key doesn't match the prefix,
        // invalidate the iterator
        if (CheckIteratorMode(this->mode_, IteratorMode::PrefixScan) && !KeyMatchesPrefix(current_key_, prefix_)) {
            current_iter_ = nullptr;
        }
    }

    std::vector<std::shared_ptr<SnapshotIterator<K, V>>> l0_iters_;
    std::vector<std::vector<std::shared_ptr<SnapshotIterator<K, V>>>> level_iters_;
    std::shared_ptr<SnapshotIterator<K, V>> current_iter_;
    K current_key_;
    V current_value_;
    HybridTime current_version_;
    bool current_is_tombstone_;
    bool current_is_l0_;
    K prefix_;  // Store the prefix for prefix scanning
};

}  // namespace pond::common