#pragma once

#include <string>

#include "common/data_chunk.h"
#include "common/result.h"
#include "common/time.h"

namespace pond::common {

enum class IteratorMode {
    Default,
    IncludeTombstones,
};

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

}  // namespace pond::common