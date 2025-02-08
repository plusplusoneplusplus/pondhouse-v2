#pragma once

#include <memory>
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
    };

    // Constructor takes filesystem and base directory
    SSTableManager(std::shared_ptr<common::IAppendOnlyFileSystem> fs,
                   const std::string& base_dir,
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

}  // namespace pond::kv
