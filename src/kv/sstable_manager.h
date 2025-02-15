#pragma once

#include <atomic>
#include <condition_variable>
#include <map>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <vector>

#include "common/append_only_fs.h"
#include "common/data_chunk.h"
#include "common/lru_cache.h"
#include "common/result.h"
#include "kv/memtable.h"
#include "kv/sstable_cache.h"
#include "kv/sstable_reader.h"
#include "kv/sstable_writer.h"
#include "kv/table_metadata.h"

namespace pond::kv {

/**
 * SSTableManager manages the lifecycle and organization of SSTables.
 * It handles:
 * - SSTable level organization
 * - Read-only access to SSTables
 * - MemTable to SSTable flushing
 * - Block caching
 * - Background compaction
 */
class SSTableManager {
public:
    // Configuration for the manager
    struct Config {
        size_t block_cache_size = 100 * 1024 * 1024;  // 100MB default
        size_t level0_size_limit = 4;                 // Max number of L0 tables
        size_t level_size_multiplier = 10;            // Size multiplier between levels
        size_t target_file_size = 64 * 1024 * 1024;   // 64MB target SSTable size

        static Config Default() {
            return Config{.block_cache_size = 100 * 1024 * 1024,
                          .level0_size_limit = 4,
                          .level_size_multiplier = 10,
                          .target_file_size = 64 * 1024 * 1024};
        }
    };

    // Statistics about the SSTable organization
    struct Stats {
        std::vector<size_t> files_per_level;  // Number of files in each level
        std::vector<size_t> bytes_per_level;  // Total bytes in each level
        size_t total_files{0};                // Total number of SSTable files
        size_t total_bytes{0};                // Total bytes across all SSTables
        common::LRUCacheStats cache_stats;    // Block cache statistics

        // Metadata cache statistics
        size_t metadata_filter_cache_hits{0};    // Number of times metadata cache avoided file read
        size_t metadata_filter_cache_misses{0};  // Number of times metadata cache required file read
        size_t physical_reads{0};                // Number of actual SSTable file reads

        void clear() {
            files_per_level.clear();
            bytes_per_level.clear();
            total_files = 0;
            total_bytes = 0;
            cache_stats = common::LRUCacheStats{};
            metadata_filter_cache_hits = 0;
            metadata_filter_cache_misses = 0;
            physical_reads = 0;
        }
    };

    // Constructor takes filesystem and base directory
    SSTableManager(std::shared_ptr<common::IAppendOnlyFileSystem> fs,
                   const std::string& base_dir,
                   std::shared_ptr<TableMetadataStateMachine> metadata_state_machine,
                   const Config& config = Config::Default());
    ~SSTableManager();

    // Prevent copying
    SSTableManager(const SSTableManager&) = delete;
    SSTableManager& operator=(const SSTableManager&) = delete;

    // Allow moving
    SSTableManager(SSTableManager&&) = default;
    SSTableManager& operator=(SSTableManager&&) = default;

    /**
     * Get a value by key from the SSTables.
     * Searches through levels in order.
     * @param key The key to look up
     * @return Result<DataChunk> containing the value if found
     */
    [[nodiscard]] common::Result<common::DataChunk> Get(const std::string& key);

    /**
     * Create a new SSTable from a MemTable.
     * @param memtable The MemTable to flush
     * @return Result<FileInfo> containing the created SSTable's information
     */
    [[nodiscard]] common::Result<FileInfo> CreateSSTableFromMemTable(const MemTable& memtable);

    /**
     * Get current statistics about the SSTable organization.
     * @return The current statistics
     */
    [[nodiscard]] Stats GetStats() const;

    /**
     * Trigger a manual compaction of the specified level.
     * @param level The level to compact (-1 for all levels)
     * @return Result<bool> indicating success
     */
    [[nodiscard]] common::Result<bool> CompactLevel(int level = -1);

    /**
     * Start background compaction thread.
     * @return Result<bool> indicating if thread started successfully
     */
    [[nodiscard]] common::Result<bool> StartCompaction();

    /**
     * Stop background compaction thread.
     * @return Result<bool> indicating if thread stopped successfully
     */
    [[nodiscard]] common::Result<bool> StopCompaction();

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

// SSTable metadata for quick filtering
struct SSTableMetadata {
    std::string file_path;
    std::string smallest_key;
    std::string largest_key;
    size_t file_size;
    std::unique_ptr<common::BloomFilter> filter;
    size_t entry_count;

    SSTableMetadata() = default;
    SSTableMetadata(const std::string& path,
                    const std::string& min_key,
                    const std::string& max_key,
                    size_t size,
                    std::unique_ptr<common::BloomFilter> f,
                    size_t count)
        : file_path(path),
          smallest_key(min_key),
          largest_key(max_key),
          file_size(size),
          filter(std::move(f)),
          entry_count(count) {}
};

// Cache for SSTable metadata
class MetadataCache {
public:
    MetadataCache() = default;

    // Add metadata for a new SSTable
    void AddTable(size_t level, size_t file_number, SSTableMetadata metadata) {
        std::lock_guard<std::shared_mutex> lock(mutex_);
        metadata_[level][file_number] = std::move(metadata);
    }

    // Remove metadata for a deleted SSTable
    void RemoveTable(size_t level, size_t file_number) {
        std::lock_guard<std::shared_mutex> lock(mutex_);
        auto it = metadata_.find(level);
        if (it != metadata_.end()) {
            it->second.erase(file_number);
        }
    }

    // Get metadata for a specific SSTable
    std::optional<const SSTableMetadata*> GetMetadata(size_t level, size_t file_number) const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        auto level_it = metadata_.find(level);
        if (level_it != metadata_.end()) {
            auto file_it = level_it->second.find(file_number);
            if (file_it != level_it->second.end()) {
                return &file_it->second;
            }
        }
        return std::nullopt;
    }

    // Check if a key might exist in a specific SSTable
    bool MayContainKey(size_t level, size_t file_number, const std::string& key) const {
        auto metadata = GetMetadata(level, file_number);
        if (!metadata) {
            return true;  // Conservative approach: if no metadata, assume it might contain the key
        }

        const auto& meta = *metadata.value();

        // Check key range first (quick rejection)
        if (key < meta.smallest_key || key > meta.largest_key) {
            return false;
        }

        // Check bloom filter if available
        if (meta.filter) {
            return meta.filter->MightContain(common::DataChunk::FromString(key));
        }

        return true;
    }

    // Clear all cached metadata
    void Clear() {
        std::lock_guard<std::shared_mutex> lock(mutex_);
        metadata_.clear();
    }

private:
    mutable std::shared_mutex mutex_;
    std::unordered_map<size_t, std::unordered_map<size_t, SSTableMetadata>> metadata_;
};

}  // namespace pond::kv
