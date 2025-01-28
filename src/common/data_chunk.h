#pragma once

#include <cstdint>
#include <memory>
#include <ranges>
#include <span>
#include <string>
#include <vector>

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
    [[nodiscard]] constexpr uint8_t *data() noexcept { return data_.data(); }
    [[nodiscard]] constexpr const uint8_t *data() const noexcept { return data_.data(); }
    [[nodiscard]] constexpr size_t size() const noexcept { return data_.size(); }

    // Get data as span
    [[nodiscard]] constexpr std::span<uint8_t> span() noexcept { return std::span<uint8_t>(data_); }
    [[nodiscard]] constexpr std::span<const uint8_t> span() const noexcept { return std::span<const uint8_t>(data_); }

    // Modify data
    constexpr void resize(size_t new_size) { data_.resize(new_size); }

    constexpr void reserve(size_t new_size) { data_.reserve(new_size); }

    void append(const uint8_t *data, size_t length) {
        size_t old_size = data_.size();
        if (old_size + length > data_.capacity()) {
            data_.reserve((old_size + length) * 2);
        }
        data_.resize(old_size + length);
        std::memcpy(data_.data() + old_size, data, length);
    }

    constexpr void append(std::span<const uint8_t> data) { data_.insert(data_.end(), data.begin(), data.end()); }

    constexpr void append(const std::vector<uint8_t> &other) { data_.insert(data_.end(), other.begin(), other.end()); }

    constexpr void append(const DataChunk &other) { append(other.span()); }

    // Clear data
    constexpr void clear() noexcept { data_.clear(); }

    // Check if empty
    [[nodiscard]] constexpr bool empty() const noexcept { return data_.empty(); }

    // Get underlying vector
    [[nodiscard]] constexpr const std::vector<uint8_t> &asVector() const noexcept { return data_; }

    // Create from string
    [[nodiscard]] static DataChunk fromString(std::string_view str) {
        return DataChunk(std::vector<uint8_t>(str.begin(), str.end()));
    }

    // Convert to string
    [[nodiscard]] std::string toString() const {
        return std::string(reinterpret_cast<const char *>(data_.data()), data_.size());
    }

private:
    std::vector<uint8_t> data_;
};
}  // namespace pond::common

template <>
struct std::formatter<pond::common::DataChunk> {
    constexpr auto parse(format_parse_context &ctx) { return ctx.begin(); }
    template <typename FormatContext>
    auto format(const pond::common::DataChunk &value, FormatContext &ctx) {
        return format_to(ctx.out(), "DataChunk(size={})", value.size());
    }
};