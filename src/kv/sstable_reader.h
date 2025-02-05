#pragma once

#include <memory>
#include <string>

#include "common/append_only_fs.h"
#include "common/data_chunk.h"
#include "common/result.h"
#include "kv/sstable_format.h"

namespace pond::kv {

/**
 * SSTableReader provides read access to an SSTable file.
 * It supports random access to key-value pairs and sequential scanning.
 */
class SSTableReader {
public:
    // Metadata structure to expose to clients
    struct Metadata {
        MetadataStats stats;
        MetadataProperties props;
    };

    // Constructor takes filesystem and path
    SSTableReader(std::shared_ptr<common::IAppendOnlyFileSystem> fs, const std::string& path);
    ~SSTableReader();

    // Prevent copying
    SSTableReader(const SSTableReader&) = delete;
    SSTableReader& operator=(const SSTableReader&) = delete;

    // Allow moving
    SSTableReader(SSTableReader&&) = default;
    SSTableReader& operator=(SSTableReader&&) = default;

    /**
     * Open the SSTable file and read metadata.
     * Must be called before any other operations.
     * @return Result<bool> indicating success or failure
     */
    [[nodiscard]] common::Result<bool> Open();

    /**
     * Get a value by key.
     * @param key The key to look up
     * @return Result<DataChunk> containing the value if found, or empty if not found
     */
    [[nodiscard]] common::Result<common::DataChunk> Get(const std::string& key);

    /**
     * Check if a key might exist in the SSTable.
     * If false, the key definitely does not exist.
     * If true, the key might exist (need to check with Get).
     * @param key The key to check
     * @return Result<bool> indicating if key might exist
     */
    [[nodiscard]] common::Result<bool> MayContain(const std::string& key);

    /**
     * Get the number of entries in the SSTable.
     * @return The number of key-value pairs
     */
    [[nodiscard]] size_t GetEntryCount() const;

    /**
     * Get the total size of the SSTable file in bytes.
     * @return The file size
     */
    [[nodiscard]] size_t GetFileSize() const;

    /**
     * Get the smallest key in the SSTable.
     * @return The smallest key
     */
    [[nodiscard]] const std::string& GetSmallestKey() const;

    /**
     * Get the largest key in the SSTable.
     * @return The largest key
     */
    [[nodiscard]] const std::string& GetLargestKey() const;

    /**
     * Get the metadata information from the SSTable.
     * Must be called after Open().
     * @return Result<Metadata> containing the metadata if present
     */
    [[nodiscard]] common::Result<Metadata> GetMetadata() const;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace pond::kv