#include "bloom_filter.h"

#include <algorithm>
#include <cmath>
#include <xxhash.h>

namespace pond::common {

namespace {
// Helper function to pack bools into bytes for serialization
std::vector<uint8_t> packBits(const std::vector<bool>& bits) {
    std::vector<uint8_t> bytes((bits.size() + 7) / 8);
    for (size_t i = 0; i < bits.size(); ++i) {
        if (bits[i]) {
            bytes[i / 8] |= (1 << (i % 8));
        }
    }
    return bytes;
}

// Helper function to unpack bytes into bools for deserialization
std::vector<bool> unpackBits(const uint8_t* bytes, size_t num_bits) {
    std::vector<bool> bits(num_bits);
    for (size_t i = 0; i < num_bits; ++i) {
        bits[i] = (bytes[i / 8] & (1 << (i % 8))) != 0;
    }
    return bits;
}
}  // namespace

BloomFilter::BloomFilter(size_t expected_items, double false_positive_prob) {
    auto [size, num_hash] = CalculateOptimalParameters(expected_items, false_positive_prob);
    bits_.resize(size);
    num_hash_functions_ = num_hash;
}

BloomFilter::BloomFilter(size_t size_in_bits, size_t num_hash_functions)
    : bits_(size_in_bits), num_hash_functions_(num_hash_functions) {}

void BloomFilter::Add(const DataChunk& item) {
    auto [hash1, hash2] = GetBaseHashes(item);
    for (size_t i = 0; i < num_hash_functions_; ++i) {
        size_t hash = GetNthHash(i, hash1, hash2);
        bits_[hash % bits_.size()] = true;
    }
    ++items_count_;
}

bool BloomFilter::MightContain(const DataChunk& item) const {
    auto [hash1, hash2] = GetBaseHashes(item);
    for (size_t i = 0; i < num_hash_functions_; ++i) {
        size_t hash = GetNthHash(i, hash1, hash2);
        if (!bits_[hash % bits_.size()]) {
            return false;
        }
    }
    return true;
}

void BloomFilter::Clear() {
    std::fill(bits_.begin(), bits_.end(), false);
    items_count_ = 0;
}

double BloomFilter::GetFalsePositiveProbability() const {
    if (items_count_ == 0)
        return 0.0;

    double bit_count = static_cast<double>(bits_.size());
    double k = static_cast<double>(num_hash_functions_);
    double n = static_cast<double>(items_count_);

    // p = (1 - e^(-k*n/m))^k
    double exponent = -k * n / bit_count;
    double inner = 1.0 - std::exp(exponent);
    return std::pow(inner, k);
}

size_t BloomFilter::GetPopCount() const {
    return std::count(bits_.begin(), bits_.end(), true);
}

Result<DataChunk> BloomFilter::Serialize() const {
    // Calculate total size needed
    size_t header_size =
        sizeof(MAGIC_NUMBER) + sizeof(VERSION) + sizeof(size_t) * 3;  // bits size, num hash functions, items count

    // Pack bits into bytes
    auto packed_bits = packBits(bits_);

    // Create data chunk for serialization
    DataChunk chunk(header_size + packed_bits.size());
    uint8_t* ptr = chunk.Data();

    // Write header
    *reinterpret_cast<uint32_t*>(ptr) = MAGIC_NUMBER;
    ptr += sizeof(MAGIC_NUMBER);

    *reinterpret_cast<uint32_t*>(ptr) = VERSION;
    ptr += sizeof(VERSION);

    *reinterpret_cast<size_t*>(ptr) = bits_.size();
    ptr += sizeof(size_t);

    *reinterpret_cast<size_t*>(ptr) = num_hash_functions_;
    ptr += sizeof(size_t);

    *reinterpret_cast<size_t*>(ptr) = items_count_;
    ptr += sizeof(size_t);

    // Write packed bits
    std::memcpy(ptr, packed_bits.data(), packed_bits.size());

    return Result<DataChunk>::success(std::move(chunk));
}

Result<BloomFilter> BloomFilter::Deserialize(const DataChunk& data) {
    if (data.Size() < sizeof(MAGIC_NUMBER) + sizeof(VERSION) + sizeof(size_t) * 3) {
        return Result<BloomFilter>::failure(ErrorCode::BloomFilterInvalidDataSize,
                                            "Data too small for valid bloom filter");
    }

    const uint8_t* ptr = data.Data();

    // Verify magic number and version
    uint32_t magic = *reinterpret_cast<const uint32_t*>(ptr);
    ptr += sizeof(MAGIC_NUMBER);

    if (magic != MAGIC_NUMBER) {
        return Result<BloomFilter>::failure(ErrorCode::BloomFilterInvalidMagicNumber, "Invalid magic number");
    }

    uint32_t version = *reinterpret_cast<const uint32_t*>(ptr);
    ptr += sizeof(VERSION);

    if (version != VERSION) {
        return Result<BloomFilter>::failure(ErrorCode::BloomFilterUnsupportedVersion, "Unsupported version");
    }

    // Read parameters
    size_t bits_size = *reinterpret_cast<const size_t*>(ptr);
    ptr += sizeof(size_t);

    size_t num_hash_functions = *reinterpret_cast<const size_t*>(ptr);
    ptr += sizeof(size_t);

    size_t items_count = *reinterpret_cast<const size_t*>(ptr);
    ptr += sizeof(size_t);

    // Create bloom filter
    BloomFilter filter(bits_size, num_hash_functions);
    filter.items_count_ = items_count;

    // Unpack bits
    size_t packed_size = (bits_size + 7) / 8;
    if (data.Size() != sizeof(MAGIC_NUMBER) + sizeof(VERSION) + sizeof(size_t) * 3 + packed_size) {
        return Result<BloomFilter>::failure(ErrorCode::BloomFilterInvalidDataSize, "Invalid data size");
    }

    filter.bits_ = unpackBits(ptr, bits_size);

    return Result<BloomFilter>::success(std::move(filter));
}

std::pair<size_t, size_t> BloomFilter::CalculateOptimalParameters(size_t expected_items, double false_positive_prob) {
    // m = -n*ln(p)/(ln(2))^2
    size_t optimal_size = static_cast<size_t>(
        std::ceil(-static_cast<double>(expected_items) * std::log(false_positive_prob) / (std::log(2) * std::log(2))));

    // k = m/n * ln(2)
    size_t optimal_hash_functions = static_cast<size_t>(
        std::ceil(static_cast<double>(optimal_size) / static_cast<double>(expected_items) * std::log(2)));

    return {optimal_size, optimal_hash_functions};
}

std::array<size_t, 2> BloomFilter::GetBaseHashes(const DataChunk& item) const {
    // Use xxHash for high-quality, fast hashing
    size_t hash1 = XXH64(item.Data(), item.Size(), 0);
    size_t hash2 = XXH64(item.Data(), item.Size(), hash1);
    return {hash1, hash2};
}

size_t BloomFilter::GetNthHash(size_t n, size_t hash1, size_t hash2) const {
    // Use double hashing technique: h(i) = h1 + i*h2
    return hash1 + n * hash2;
}

}  // namespace pond::common