#pragma once

#include <memory>
#include <string>

#include "common/append_only_fs.h"
#include "common/data_chunk.h"
#include "common/result.h"
#include "kv/sstable_format.h"

namespace pond::kv {

/**
 * SSTableWriter is responsible for writing SSTable files to disk.
 * It handles the creation of data blocks, index blocks, and optional bloom filters.
 */
class SSTableWriter {
public:
    // Constructor takes filesystem and path
    SSTableWriter(std::shared_ptr<common::IAppendOnlyFileSystem> fs, const std::string& path);
    ~SSTableWriter();

    // Prevent copying
    SSTableWriter(const SSTableWriter&) = delete;
    SSTableWriter& operator=(const SSTableWriter&) = delete;

    // Allow moving
    SSTableWriter(SSTableWriter&&) = default;
    SSTableWriter& operator=(SSTableWriter&&) = default;

    /**
     * Add a key-value pair to the SSTable.
     * Keys must be added in sorted order.
     * @return Result<bool> indicating success or failure
     */
    [[nodiscard]] common::Result<bool> Add(const std::string& key, const common::DataChunk& value) {
        return Add(key, value, common::MinHybridTime(), false);
    }

    /**
     * Add a key-value pair with version to the SSTable.
     * Keys must be added in sorted order.
     * @param key The key to add
     * @param value The value to add
     * @param version The version of the key
     * @param is_tombstone Whether the key is a tombstone
     * @return Result<bool> indicating success or failure
     */
    [[nodiscard]] common::Result<bool> Add(const std::string& key,
                                           const common::DataChunk& value,
                                           common::HybridTime version,
                                           bool is_tombstone);

    /**
     * Finalize the SSTable and write all remaining data to disk.
     * After calling Finish(), the writer cannot be used anymore.
     * @return Result<bool> indicating success or failure
     */
    [[nodiscard]] common::Result<bool> Finish();

    /**
     * Enable bloom filter for this SSTable.
     * Must be called before adding any keys.
     * @param expected_keys Expected number of keys to be added
     * @param false_positive_rate Desired false positive rate (default: 0.01)
     */
    void EnableFilter(size_t expected_keys, double false_positive_rate = 0.01);

    // Get the current memory usage of the writer
    [[nodiscard]] size_t GetMemoryUsage() const;

    // Get the current file size
    [[nodiscard]] size_t GetFileSize() const;

private:
    static constexpr size_t kEstimatedBufferSize = 64 * 1024;  // 64KB for internal buffers
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace pond::kv