#pragma once

#include <chrono>
#include <cstdint>
#include <string>
#include <vector>

#include "common/bloom_filter.h"
#include "common/crc.h"
#include "common/data_chunk.h"
#include "common/time.h"

namespace pond::kv {

// Internal key format used in SSTable
// Combines user key with version information
class InternalKey {
public:
    InternalKey() = default;
    InternalKey(std::string user_key, common::HybridTime version);

    // Serialization
    std::vector<uint8_t> Serialize() const;
    bool Deserialize(const uint8_t* data, size_t size);

    // Accessors
    const std::string& user_key() const { return user_key_; }
    common::HybridTime version() const { return version_; }
    size_t size() const { return user_key_.size() + sizeof(uint64_t); }

    // Comparison operators
    bool operator<(const InternalKey& other) const;
    bool operator<=(const InternalKey& other) const;
    bool operator>(const InternalKey& other) const;
    bool operator>=(const InternalKey& other) const;
    bool operator==(const InternalKey& other) const;

private:
    std::string user_key_;
    common::HybridTime version_;
};

// All multi-byte integers are stored in little-endian format
struct FileHeader {
    static constexpr uint64_t kMagicNumber = 0x46544268;  // "STBFH" in little-endian
    static constexpr size_t kHeaderSize = 64;

    // Flags for various features
    enum Flags {
        kHasFilter = 0x1,  // Has bloom filter
    };

    uint64_t magic_number{kMagicNumber};  // 8 bytes
    uint32_t version{1};                  // 4 bytes
    uint32_t flags{0};                    // 4 bytes: compression, filter, etc.
    uint8_t reserved[48]{0};              // 48 bytes for future use

    // Serialization
    std::vector<uint8_t> Serialize() const;
    // Returns true if successful, false if data is invalid
    bool Deserialize(const uint8_t* data, size_t size);

    // Validation
    bool IsValid() const { return magic_number == kMagicNumber; }
    bool HasFilter() const { return (flags & kHasFilter) != 0; }
};

static_assert(sizeof(FileHeader) == FileHeader::kHeaderSize, "FileHeader size mismatch");

struct Footer {
    static constexpr size_t kFooterSize = 128;
    static constexpr uint64_t kMagicNumber = FileHeader::kMagicNumber;

    uint64_t index_block_offset{0};       // 8 bytes
    uint64_t filter_block_offset{0};      // 8 bytes
    uint64_t metadata_block_offset{0};    // 8 bytes
    uint64_t index_block_size{0};         // 8 bytes
    uint64_t filter_block_size{0};        // 8 bytes
    uint64_t metadata_block_size{0};      // 8 bytes
    uint8_t padding[72]{0};               // 72 bytes reserved
    uint64_t magic_number{kMagicNumber};  // 8 bytes, same as header

    // Serialization
    std::vector<uint8_t> Serialize() const;
    // Returns true if successful, false if data is invalid
    bool Deserialize(const uint8_t* data, size_t size);

    // Validation
    bool IsValid() const { return magic_number == kMagicNumber; }
};

static_assert(sizeof(Footer) == Footer::kFooterSize, "Footer size mismatch");

// Common block footer for both data and index blocks
struct BlockFooter {
    static constexpr size_t kFooterSize = 16;

    uint32_t entry_count{0};       // Number of entries in block
    uint32_t block_size{0};        // Total size of block including footer
    uint32_t checksum{0};          // CRC32 of block data
    uint32_t compression_type{0};  // 0=None, 1=Snappy

    std::vector<uint8_t> Serialize() const;
    bool Deserialize(const uint8_t* data, size_t size);
};

static_assert(sizeof(BlockFooter) == BlockFooter::kFooterSize, "BlockFooter size mismatch");

// Entry in a data block
struct DataBlockEntry {
    static constexpr uint8_t FLAG_TOMBSTONE = 0x01;
    uint16_t flags{0};         // Add flags field to indicate tombstone
    uint16_t key_length{0};    // Length of internal key
    uint32_t value_length{0};  // Length of value
    // Followed by key_length bytes of internal key
    // Then value_length bytes of value

    static constexpr size_t kHeaderSize = 8;  // flags + key_length + value_length

    // Serializes just the header (caller must append key and value data)
    std::vector<uint8_t> SerializeHeader() const;

    // Deserializes just the header (caller must read key and value data)
    bool DeserializeHeader(const uint8_t* data, size_t size);

    // Full serialization including key and value
    std::vector<uint8_t> Serialize(const InternalKey& key, const common::DataChunk& value) const;

    bool IsTombstone() const { return (flags & FLAG_TOMBSTONE) != 0; }
};

// Entry in an index block
struct IndexBlockEntry {
    uint32_t key_length{0};    // Length of largest key
    uint64_t block_offset{0};  // Offset of data block
    uint32_t block_size{0};    // Size of data block
    uint32_t entry_count{0};   // Number of entries in data block
    // Followed by key_length bytes of largest key

    static constexpr size_t kHeaderSize = 20;  // All fields except key

    // Serializes just the header (caller must append key data)
    std::vector<uint8_t> SerializeHeader() const;
    // Deserializes just the header (caller must read key data)
    bool DeserializeHeader(const uint8_t* data, size_t size);

    // Full serialization including key
    std::vector<uint8_t> Serialize(const std::string& largest_key) const;
};

// Block builders help construct blocks incrementally
class DataBlockBuilder {
public:
    static constexpr size_t kTargetBlockSize = 4 * 1024 * 1024;  // 4MB target

    // Returns false if block would exceed target size
    bool Add(const std::string& user_key,
             common::HybridTime version,
             const common::DataChunk& value,
             bool is_tombstone = false);

    // Finalize block and get its contents
    std::vector<uint8_t> Finish();

    // Reset for building next block
    void Reset();

    size_t CurrentSize() const { return current_size_; }
    bool Empty() const { return entries_.empty(); }

    // Get all user keys in this block
    std::vector<std::string> GetKeys() const;

    size_t GetMemoryUsage() const {
        size_t usage = 0;
        for (const auto& entry : entries_) {
            usage += entry.key.size() + entry.value.Size();
        }
        return usage;
    }

private:
    struct Entry {
        bool is_tombstone{false};
        InternalKey key;
        common::DataChunk value;
    };

    std::vector<Entry> entries_;
    size_t current_size_{0};
};

class IndexBlockBuilder {
public:
    void AddEntry(const std::string& largest_key, uint64_t block_offset, uint32_t block_size, uint32_t entry_count);
    std::vector<uint8_t> Finish();
    void Reset();
    bool Empty() const { return entries_.empty(); }

    size_t GetMemoryUsage() const {
        size_t usage = 0;
        for (const auto& entry : entries_) {
            usage += entry.largest_key.size() + sizeof(IndexBlockEntry);
        }
        return usage;
    }

private:
    struct Entry {
        std::string largest_key;
        uint64_t block_offset;
        uint32_t block_size;
        uint32_t entry_count;
    };

    std::vector<Entry> entries_;
};

// Footer for filter blocks
struct FilterBlockFooter {
    static constexpr size_t kFooterSize = 16;

    uint32_t filter_size{0};  // Size of filter data
    uint32_t num_keys{0};     // Number of keys in filter
    uint32_t checksum{0};     // CRC32 of filter data
    uint32_t reserved{0};     // Reserved for future use

    // Serialization
    std::vector<uint8_t> Serialize() const;
    bool Deserialize(const uint8_t* data, size_t size);
};

static_assert(sizeof(FilterBlockFooter) == FilterBlockFooter::kFooterSize, "FilterBlockFooter size mismatch");

// Filter block builder for bloom filters
class FilterBlockBuilder {
public:
    explicit FilterBlockBuilder(size_t expected_keys, double false_positive_rate = 0.01)
        : filter_(expected_keys, false_positive_rate) {}

    // Add keys from a data block
    void AddKeys(const std::vector<std::string>& keys) {
        for (const auto& key : keys) {
            filter_.Add(common::DataChunk::FromString(key));
        }
    }

    // Finalize and get filter block with footer
    std::vector<uint8_t> Finish() {
        auto result = filter_.Serialize();
        if (!result.ok()) {
            // Return empty block on error
            return {};
        }

        // Create filter data
        std::vector<uint8_t> filter_data(result.value().Data(), result.value().Data() + result.value().Size());

        // Create and serialize footer
        FilterBlockFooter footer;
        footer.filter_size = filter_data.size();
        footer.num_keys = filter_.GetItemsCount();
        footer.checksum = common::Crc32(filter_data.data(), filter_data.size());
        footer.reserved = 0;

        auto footer_data = footer.Serialize();

        // Combine filter and footer
        filter_data.insert(filter_data.end(), footer_data.begin(), footer_data.end());
        return filter_data;
    }

    void Reset() { filter_.Clear(); }

    double GetFalsePositiveRate() const { return filter_.GetFalsePositiveProbability(); }

    size_t GetMemoryUsage() const { return filter_.GetMemoryUsage(); }

private:
    common::BloomFilter filter_;
};

// Utility functions for endian conversion
namespace util {
inline uint64_t HostToLittleEndian64(uint64_t value) {
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    return value;
#else
    return __builtin_bswap64(value);
#endif
}

inline uint32_t HostToLittleEndian32(uint32_t value) {
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    return value;
#else
    return __builtin_bswap32(value);
#endif
}

inline uint64_t LittleEndianToHost64(uint64_t value) {
    return HostToLittleEndian64(value);
}

inline uint32_t LittleEndianToHost32(uint32_t value) {
    return HostToLittleEndian32(value);
}
}  // namespace util

// Metadata block structures
struct MetadataStats {
    uint64_t key_count{0};         // Total number of keys
    std::string smallest_key;      // Smallest key in the SSTable
    std::string largest_key;       // Largest key in the SSTable
    uint64_t total_key_size{0};    // Total size of all keys
    uint64_t total_value_size{0};  // Total size of all values

    // Serialization
    std::vector<uint8_t> Serialize() const;
    bool Deserialize(const uint8_t* data, size_t size);
};

struct MetadataProperties {
    uint64_t creation_time{0};     // Unix timestamp of creation
    uint32_t compression_type{0};  // 0=None, 1=Snappy
    uint32_t index_type{0};        // 0=Binary search
    uint32_t filter_type{0};       // 0=None, 1=Bloom
    double filter_fp_rate{0.0};    // Bloom filter false positive rate

    // Serialization
    std::vector<uint8_t> Serialize() const;
    bool Deserialize(const uint8_t* data, size_t size);
};

struct MetadataBlockFooter {
    static constexpr size_t kFooterSize = 16;

    uint32_t stats_size{0};  // Size of stats section
    uint32_t props_size{0};  // Size of properties section
    uint32_t checksum{0};    // CRC32 of block data
    uint32_t reserved{0};    // Reserved for future use

    std::vector<uint8_t> Serialize() const;
    bool Deserialize(const uint8_t* data, size_t size);
};

// Builder for metadata blocks
class MetadataBlockBuilder {
public:
    MetadataBlockBuilder() = default;

    void UpdateStats(const std::string& key, const common::DataChunk& value) {
        stats_.key_count++;
        stats_.total_key_size += key.size();
        stats_.total_value_size += value.Size();

        if (stats_.smallest_key.empty() || key < stats_.smallest_key) {
            stats_.smallest_key = key;
        }
        if (stats_.largest_key.empty() || key > stats_.largest_key) {
            stats_.largest_key = key;
        }
    }

    void SetCompressionType(uint32_t type) { props_.compression_type = type; }
    void SetFilterType(uint32_t type, double fp_rate = 0.0) {
        props_.filter_type = type;
        props_.filter_fp_rate = fp_rate;
    }

    std::vector<uint8_t> Finish() {
        // Set creation time if not already set
        if (props_.creation_time == 0) {
            props_.creation_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        }

        // Serialize components
        auto stats_data = stats_.Serialize();
        auto props_data = props_.Serialize();

        // Calculate total size and prepare buffer
        std::vector<uint8_t> buffer;
        buffer.reserve(stats_data.size() + props_data.size() + MetadataBlockFooter::kFooterSize);

        // Add stats and properties data
        buffer.insert(buffer.end(), stats_data.begin(), stats_data.end());
        buffer.insert(buffer.end(), props_data.begin(), props_data.end());

        // Create and add footer
        MetadataBlockFooter footer;
        footer.stats_size = stats_data.size();
        footer.props_size = props_data.size();
        footer.checksum = common::Crc32(buffer.data(), buffer.size());

        auto footer_data = footer.Serialize();
        buffer.insert(buffer.end(), footer_data.begin(), footer_data.end());

        return buffer;
    }

    void Reset() {
        stats_ = MetadataStats();
        props_ = MetadataProperties();
    }

private:
    MetadataStats stats_;
    MetadataProperties props_;
};

}  // namespace pond::kv
