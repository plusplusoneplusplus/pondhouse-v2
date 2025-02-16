#pragma once

#include <array>
#include <bit>
#include <cstdint>
#include <string>
#include <vector>

#include "common/data_chunk.h"
#include "common/result.h"

namespace pond::common {

class BloomFilter {
public:
    // Creates a Bloom filter with the given expected number of items and false positive probability
    BloomFilter(size_t expected_items, double false_positive_prob);

    // Creates a Bloom filter with specific size and number of hash functions
    BloomFilter(size_t size_in_bits, size_t num_hash_functions);

    // Add an item to the filter
    void Add(const common::DataChunk& item);

    // Check if an item might be in the filter
    // False positives are possible, but false negatives are not
    [[nodiscard]] bool MightContain(const common::DataChunk& item) const;

    // Clear the filter
    void Clear();

    // Get the current false positive probability
    [[nodiscard]] double GetFalsePositiveProbability() const;

    // Get number of bits set to 1
    [[nodiscard]] size_t GetPopCount() const;

    // Serialization
    [[nodiscard]] common::Result<common::DataChunk> Serialize() const;
    [[nodiscard]] static common::Result<BloomFilter> Deserialize(const common::DataChunk& data);

    // Get filter parameters
    [[nodiscard]] size_t GetBitSize() const { return bits_.size(); }
    [[nodiscard]] size_t GetHashFunctionCount() const { return num_hash_functions_; }

    [[nodiscard]] size_t GetItemsCount() const { return items_count_; }

    [[nodiscard]] size_t GetMemoryUsage() const {
        // Approximate memory usage:
        // - Filter data (std::vector<bool>)
        // - Internal buffers
        return bits_.size() + sizeof(size_t) + sizeof(size_t) + sizeof(size_t);
    }

private:
    // Calculate optimal size and hash functions
    static std::pair<size_t, size_t> CalculateOptimalParameters(size_t expected_items, double false_positive_prob);

    // Hash functions
    [[nodiscard]] std::array<size_t, 2> GetBaseHashes(const common::DataChunk& item) const;
    [[nodiscard]] size_t GetNthHash(size_t n, size_t hash1, size_t hash2) const;

    std::vector<bool> bits_;
    size_t num_hash_functions_;
    size_t items_count_{0};

    // Constants for serialization
    static constexpr uint32_t MAGIC_NUMBER = 0x424C4D46;  // "BLMF"
    static constexpr uint32_t VERSION = 1;
};

}  // namespace pond::common