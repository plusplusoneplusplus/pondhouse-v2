#pragma once

#include <cstdint>
#include <memory>
#include <ranges>
#include <span>
#include <string>
#include <vector>

#include <fmt/format.h>

namespace pond::common {

class DataChunk {
private:
    std::vector<uint8_t> data_;  // Underlying buffer
    size_t size_;                // Current size of valid data

public:
    static constexpr size_t DEFAULT_INITIAL_CAPACITY = 64;  // Default pre-allocation size

    // Create an empty chunk with pre-allocated memory
    DataChunk() : data_(DEFAULT_INITIAL_CAPACITY), size_(0) {}

    // Create a chunk with initial data
    explicit DataChunk(std::vector<uint8_t> data) : data_(std::move(data)), size_(data_.size()) {}

    // Create a chunk with size
    explicit DataChunk(size_t size) : data_(std::max(size, DEFAULT_INITIAL_CAPACITY)), size_(size) {}

    // Create a chunk from raw data
    DataChunk(const uint8_t *data, size_t size) : data_(std::max(size, DEFAULT_INITIAL_CAPACITY)), size_(size) {
        std::memcpy(data_.data(), data, size);
    }

    bool operator==(const DataChunk &other) const {
        if (size_ != other.size_)
            return false;
        return std::memcmp(data_.data(), other.data_.data(), size_) == 0;
    }

    // Access data
    [[nodiscard]] constexpr uint8_t *Data() noexcept { return data_.data(); }
    [[nodiscard]] constexpr const uint8_t *Data() const noexcept { return data_.data(); }
    [[nodiscard]] constexpr size_t Size() const noexcept { return size_; }
    [[nodiscard]] constexpr size_t Capacity() const noexcept { return data_.size(); }

    // Get data as span
    [[nodiscard]] constexpr std::span<uint8_t> Span() noexcept { return std::span<uint8_t>(data_.data(), size_); }
    [[nodiscard]] constexpr std::span<const uint8_t> Span() const noexcept {
        return std::span<const uint8_t>(data_.data(), size_);
    }

    // Modify data
    constexpr void Resize(size_t new_size) {
        if (new_size > data_.size()) {
            data_.resize(new_size);
        }
        size_ = new_size;
    }

    constexpr void Reserve(size_t new_capacity) {
        if (new_capacity > data_.size()) {
            data_.resize(new_capacity);
        }
    }

    void Append(const uint8_t *data, size_t length) {
        if (size_ + length > data_.size()) {
            data_.resize(std::max(size_ + length, data_.size() * 2));
        }
        std::memcpy(data_.data() + size_, data, length);
        size_ += length;
    }

    void Append(std::span<const uint8_t> data) { Append(data.data(), data.size()); }

    void Append(const std::vector<uint8_t> &other) { Append(other.data(), other.size()); }

    void Append(const DataChunk &other) { Append(other.Data(), other.Size()); }

    void AppendSizedDataChunk(const DataChunk &other) {
        size_t size = other.Size();
        Append(reinterpret_cast<const uint8_t *>(&size), sizeof(size));
        Append(other);
    }

    // Clear data
    constexpr void Clear() noexcept {
        size_ = 0;  // Just reset size, keep the allocated memory
    }

    // Check if empty
    [[nodiscard]] constexpr bool Empty() const noexcept { return size_ == 0; }

    // Get underlying vector
    [[nodiscard]] constexpr std::vector<uint8_t> AsVector() const {
        return std::vector<uint8_t>(data_.begin(), data_.begin() + size_);
    }

    // Create from string
    [[nodiscard]] static DataChunk FromString(std::string_view str) {
        return DataChunk(reinterpret_cast<const uint8_t *>(str.data()), str.size());
    }

    // Convert to string
    [[nodiscard]] std::string ToString() const {
        return std::string(reinterpret_cast<const char *>(data_.data()), size_);
    }

    // Write methods for common types
    void WriteUInt8(uint8_t value) { Append(&value, sizeof(value)); }

    void WriteUInt16(uint16_t value) { Append(reinterpret_cast<const uint8_t *>(&value), sizeof(value)); }

    void WriteUInt32(uint32_t value) { Append(reinterpret_cast<const uint8_t *>(&value), sizeof(value)); }

    void WriteUInt64(uint64_t value) { Append(reinterpret_cast<const uint8_t *>(&value), sizeof(value)); }

    void WriteInt8(int8_t value) { Append(reinterpret_cast<const uint8_t *>(&value), sizeof(value)); }

    void WriteInt16(int16_t value) { Append(reinterpret_cast<const uint8_t *>(&value), sizeof(value)); }

    void WriteInt32(int32_t value) { Append(reinterpret_cast<const uint8_t *>(&value), sizeof(value)); }

    void WriteInt64(int64_t value) { Append(reinterpret_cast<const uint8_t *>(&value), sizeof(value)); }

    void WriteBool(bool value) { WriteUInt8(value ? 1 : 0); }

    void WriteString(std::string_view str) {
        WriteUInt32(static_cast<uint32_t>(str.size()));
        Append(reinterpret_cast<const uint8_t *>(str.data()), str.size());
    }

    // Read methods for common types
    [[nodiscard]] uint8_t ReadUInt8(size_t &offset) const {
        if (offset + sizeof(uint8_t) > Size()) {
            throw std::out_of_range("ReadUInt8: not enough data");
        }
        uint8_t value = data_[offset];
        offset += sizeof(uint8_t);
        return value;
    }

    [[nodiscard]] uint16_t ReadUInt16(size_t &offset) const {
        if (offset + sizeof(uint16_t) > Size()) {
            throw std::out_of_range("ReadUInt16: not enough data");
        }
        uint16_t value;
        memcpy(&value, data_.data() + offset, sizeof(value));
        offset += sizeof(uint16_t);
        return value;
    }

    [[nodiscard]] uint32_t ReadUInt32(size_t &offset) const {
        if (offset + sizeof(uint32_t) > Size()) {
            throw std::out_of_range("ReadUInt32: not enough data");
        }
        uint32_t value;
        memcpy(&value, data_.data() + offset, sizeof(value));
        offset += sizeof(uint32_t);
        return value;
    }

    [[nodiscard]] uint64_t ReadUInt64(size_t &offset) const {
        if (offset + sizeof(uint64_t) > Size()) {
            throw std::out_of_range("ReadUInt64: not enough data");
        }
        uint64_t value;
        memcpy(&value, data_.data() + offset, sizeof(value));
        offset += sizeof(uint64_t);
        return value;
    }

    [[nodiscard]] int8_t ReadInt8(size_t &offset) const {
        if (offset + sizeof(int8_t) > Size()) {
            throw std::out_of_range("ReadInt8: not enough data");
        }
        int8_t value;
        memcpy(&value, data_.data() + offset, sizeof(value));
        offset += sizeof(int8_t);
        return value;
    }

    [[nodiscard]] int16_t ReadInt16(size_t &offset) const {
        if (offset + sizeof(int16_t) > Size()) {
            throw std::out_of_range("ReadInt16: not enough data");
        }
        int16_t value;
        memcpy(&value, data_.data() + offset, sizeof(value));
        offset += sizeof(int16_t);
        return value;
    }

    [[nodiscard]] int32_t ReadInt32(size_t &offset) const {
        if (offset + sizeof(int32_t) > Size()) {
            throw std::out_of_range("ReadInt32: not enough data");
        }
        int32_t value;
        memcpy(&value, data_.data() + offset, sizeof(value));
        offset += sizeof(int32_t);
        return value;
    }

    [[nodiscard]] int64_t ReadInt64(size_t &offset) const {
        if (offset + sizeof(int64_t) > Size()) {
            throw std::out_of_range("ReadInt64: not enough data");
        }
        int64_t value;
        memcpy(&value, data_.data() + offset, sizeof(value));
        offset += sizeof(int64_t);
        return value;
    }

    [[nodiscard]] bool ReadBool(size_t &offset) const { return ReadUInt8(offset) != 0; }

    [[nodiscard]] std::string ReadString(size_t &offset) const {
        uint32_t length = ReadUInt32(offset);
        if (offset + length > Size()) {
            throw std::out_of_range("ReadString: not enough data");
        }
        std::string result(reinterpret_cast<const char *>(data_.data() + offset), length);
        offset += length;
        return result;
    }
};

typedef std::shared_ptr<DataChunk> DataChunkPtr;
}  // namespace pond::common

template <>
struct fmt::formatter<pond::common::DataChunk> : formatter<string_view> {
    template <typename FormatContext>
    auto format(const pond::common::DataChunk &chunk, FormatContext &ctx) const {
        std::string hex;
        hex.reserve(chunk.Size() * 2);
        static const char *digits = "0123456789ABCDEF";

        for (size_t i = 0; i < chunk.Size(); ++i) {
            uint8_t byte = chunk.Data()[i];
            hex.push_back(digits[byte >> 4]);
            hex.push_back(digits[byte & 0xF]);
        }

        return formatter<string_view>::format(hex, ctx);
    }
};