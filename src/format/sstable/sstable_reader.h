#pragma once

#include <memory>
#include <string>

#include "common/append_only_fs.h"
#include "common/data_chunk.h"
#include "common/iterator.h"
#include "common/result.h"
#include "format/sstable/sstable_format.h"

namespace pond::format {

/**
 * SSTableReader provides read access to an SSTable file.
 * It supports random access to key-value pairs and sequential scanning.
 */
class SSTableReader {
public:
    // Forward declaration of Iterator
    class Iterator;

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
     * @param version Optional version to look up. If not provided, returns the latest version.
     * @return Result<DataChunk> containing the value if found, or empty if not found
     */
    [[nodiscard]] common::Result<common::DataChunk> Get(const std::string& key,
                                                        common::HybridTime version = common::MaxHybridTime());

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
     * Get the bloom filter from the SSTable if available.
     * @return Result<unique_ptr<BloomFilter>> containing the filter if present
     */
    [[nodiscard]] common::Result<std::unique_ptr<common::BloomFilter>> GetBloomFilter() const;

    /**
     * Get the metadata information from the SSTable.
     * Must be called after Open().
     * @return Result<Metadata> containing the metadata if present
     */
    [[nodiscard]] common::Result<Metadata> GetMetadata() const;

    /**
     * Create a new iterator for scanning the SSTable.
     * The caller takes ownership of the returned iterator.
     * @param read_time Optional timestamp to get a consistent view of data up to this time
     * @param version_behavior Controls whether to return all versions or just the latest
     * @return A new iterator instance
     */
    [[nodiscard]] std::shared_ptr<Iterator> NewIterator(common::HybridTime read_time = common::MaxHybridTime(),
                                                        common::IteratorMode mode = common::IteratorMode::Default);

    /**
     * Get an iterator pointing to the beginning of the SSTable.
     * @param read_time Optional timestamp to get a consistent view of data up to this time
     * @param version_behavior Controls whether to return all versions or just the latest
     * @return An iterator pointing to the first entry
     */
    [[nodiscard]] Iterator begin(common::HybridTime read_time = common::MaxHybridTime(),
                                 common::IteratorMode mode = common::IteratorMode::Default);

    /**
     * Get an iterator representing the end of the SSTable.
     * @return An iterator representing the end
     */
    [[nodiscard]] Iterator end();

    // Get the current memory usage of the reader
    [[nodiscard]] size_t GetMemoryUsage() const;

    /**
     * Get the total number of bytes read from disk.
     * This includes all data read during Get operations and scans.
     * @return The total number of bytes read
     */
    [[nodiscard]] size_t GetBytesRead() const;

    /**
     * Iterator provides sequential access to entries in the SSTable.
     * Supports seeking to specific keys and sequential scanning.
     */
    class Iterator : public common::SnapshotIterator<std::string, common::DataChunk> {
    public:
        // Iterator traits for STL compatibility
        using iterator_category = std::forward_iterator_tag;
        using value_type = std::pair<const std::string&, const common::DataChunk&>;
        using pointer = const value_type*;
        using reference = const value_type&;

        explicit Iterator(SSTableReader* reader,
                          common::HybridTime read_time = common::MaxHybridTime(),
                          common::IteratorMode mode = common::IteratorMode::Default,
                          bool seek_to_first = true);
        ~Iterator();

        Iterator(const Iterator&);
        Iterator& operator=(const Iterator&);

        // Allow moving
        Iterator(Iterator&&) = default;
        Iterator& operator=(Iterator&&) = default;

        /**
         * Check if the iterator is positioned at a valid entry.
         * @return true if valid, false if we've reached the end
         */
        [[nodiscard]] bool Valid() const override;

        /**
         * Get the key at the current position.
         * Must only be called when Valid() returns true.
         * @return The current key
         */
        [[nodiscard]] const std::string& key() const override;

        /**
         * Get the value at the current position.
         * Must only be called when Valid() returns true.
         * @return The current value
         */
        [[nodiscard]] const common::DataChunk& value() const override;

        /**
         * Get the version (timestamp) at the current position.
         * Must only be called when Valid() returns true.
         * @return The current version
         */
        [[nodiscard]] common::HybridTime version() const override;

        /**
         * Check if the current entry is a tombstone.
         * @return true if the entry is a tombstone, false otherwise
         */
        [[nodiscard]] bool IsTombstone() const override;

        /**
         * Advance to the next entry.
         * Must only be called when Valid() returns true.
         * @return true if successful, false if we've reached the end
         */
        void Next() override;

        /**
         * Position at the first entry in the table.
         */
        void SeekToFirst();

        /**
         * Position at the first entry with a key >= target.
         * @param target The target key to seek to
         */
        void Seek(const std::string& target) override;

        // STL iterator support
        Iterator& operator++() {
            Next();
            return *this;
        }

        void operator++(int) {  // Post-increment just advances without returning previous value
            Next();
        }

        value_type operator*() const { return {key(), value()}; }

        bool operator==(const Iterator& other) const {
            return (!Valid() && !other.Valid()) || (Valid() && other.Valid() && key() == other.key());
        }

        bool operator!=(const Iterator& other) const { return !(*this == other); }

    private:
        class Impl;
        std::unique_ptr<Impl> impl_;
    };

private:
    static constexpr size_t kEstimatedBufferSize = 32 * 1024;  // 32KB for internal buffers
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace pond::kv