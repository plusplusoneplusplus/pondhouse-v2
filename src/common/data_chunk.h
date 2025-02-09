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
public:
    // Create an empty chunk
    constexpr DataChunk() = default;

    // Create a chunk with initial data
    explicit constexpr DataChunk(std::vector<uint8_t> data) : data_(std::move(data)) {}

    // Create a chunk with size
    explicit constexpr DataChunk(size_t size) : data_(size) {}

    DataChunk(const uint8_t *data, size_t size) : data_(data, data + size) {}

    bool operator==(const DataChunk &other) const { return data_ == other.data_; }

    // Access data
    [[nodiscard]] constexpr uint8_t *Data() noexcept { return data_.data(); }
    [[nodiscard]] constexpr const uint8_t *Data() const noexcept { return data_.data(); }
    [[nodiscard]] constexpr size_t Size() const noexcept { return data_.size(); }

    // Get data as span
    [[nodiscard]] constexpr std::span<uint8_t> Span() noexcept { return std::span<uint8_t>(data_); }
    [[nodiscard]] constexpr std::span<const uint8_t> Span() const noexcept { return std::span<const uint8_t>(data_); }

    // Modify data
    constexpr void Resize(size_t new_size) { data_.resize(new_size); }

    constexpr void Reserve(size_t new_size) { data_.reserve(new_size); }

    void Append(const uint8_t *data, size_t length) {
        size_t old_size = data_.size();
        if (old_size + length > data_.capacity()) {
            data_.reserve((old_size + length) * 2);
        }
        data_.resize(old_size + length);
        memcpy(data_.data() + old_size, data, length);
    }

    constexpr void Append(std::span<const uint8_t> data) { data_.insert(data_.end(), data.begin(), data.end()); }

    constexpr void Append(const std::vector<uint8_t> &other) { data_.insert(data_.end(), other.begin(), other.end()); }

    constexpr void Append(const DataChunk &other) { Append(other.Span()); }

    // Clear data
    constexpr void Clear() noexcept { data_.clear(); }

    // Check if empty
    [[nodiscard]] constexpr bool Empty() const noexcept { return data_.empty(); }

    // Get underlying vector
    [[nodiscard]] constexpr const std::vector<uint8_t> &AsVector() const noexcept { return data_; }

    // Create from string
    [[nodiscard]] static DataChunk FromString(std::string_view str) {
        return DataChunk(std::vector<uint8_t>(str.begin(), str.end()));
    }

    // Convert to string
    [[nodiscard]] std::string ToString() const {
        return std::string(reinterpret_cast<const char *>(data_.data()), data_.size());
    }

private:
    std::vector<uint8_t> data_;
};
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