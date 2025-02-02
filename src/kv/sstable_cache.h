#pragma once

#include <memory>
#include <string>
#include <vector>

#include "common/bplus_tree.h"
#include "common/lru_cache.h"
#include "common/result.h"
#include "kv/kv_entry.h"

namespace pond::kv {

using BlockOffset = uint64_t;
using SSTableId = uint64_t;

// Represents a block within an SSTable's index
struct IndexBlock {
    std::vector<uint8_t> data;
    size_t size() const { return data.size(); }
};

// Composite key for cache lookup
struct BlockKey {
    SSTableId sstable_id;
    BlockOffset block_offset;

    bool operator==(const BlockKey& other) const {
        return sstable_id == other.sstable_id && block_offset == other.block_offset;
    }
};

// Hash function for BlockKey
struct BlockKeyHash {
    size_t operator()(const BlockKey& key) const {
        return std::hash<SSTableId>()(key.sstable_id) ^ 
               std::hash<BlockOffset>()(key.block_offset);
    }
};

class SSTableCache {
public:
    explicit SSTableCache(size_t max_memory_bytes = 100 * 1024 * 1024)  // Default 100MB
        : cache_(max_memory_bytes, [](const IndexBlock& block) { return block.size(); }) {}

    // Get an index block from cache
    common::Result<std::optional<IndexBlock>> GetBlock(const BlockKey& key) {
        return cache_.Get(key);
    }

    // Put an index block into cache
    common::Result<void> PutBlock(const BlockKey& key, IndexBlock block) {
        return cache_.Put(key, std::move(block));
    }

    // Remove a block from cache
    common::Result<void> Remove(const BlockKey& key) {
        return cache_.Remove(key);
    }

    // Clear all entries
    common::Result<void> Clear() {
        return cache_.Clear();
    }

    // Get current cache statistics
    common::LRUCache<BlockKey, IndexBlock, BlockKeyHash>::Stats GetStats() const {
        return cache_.GetStats();
    }

private:
    common::LRUCache<BlockKey, IndexBlock, BlockKeyHash> cache_;
};

}  // namespace pond::kv
