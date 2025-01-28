#pragma once

#include <cstdint>
#include <string>
#include <type_traits>
#include <variant>

#include "common/uuid.h"

namespace pond::common {

struct PositionRecord {
    UUID id_;
    size_t offset_;
    size_t length_;

    bool operator==(const PositionRecord&) const = default;

    friend std::ostream& operator<<(std::ostream& os, const PositionRecord& val) {
        return os << "PositionRecord(" << val.id_ << ", " << val.offset_ << ", " << val.length_ << ")";
    }
};

struct ContinousBuffer {
    uint8_t* data_;
    size_t size_;

    bool operator==(const ContinousBuffer&) const = default;

    char* as_char() { return reinterpret_cast<char*>(data_); }
};

class DataVariant {
public:
    using VariantType =
        std::variant<std::nullptr_t, bool, int32_t, int64_t, float, double, PositionRecord, ContinousBuffer>;

    // Default constructor creates null value
    DataVariant() : value_(nullptr) {}

    // Constructors for supported types
    template <typename T>
    DataVariant(T&& val) : value_(std::forward<T>(val)) {}

    // Type checking
    bool isNull() const { return std::holds_alternative<std::nullptr_t>(value_); }
    bool isBool() const { return std::holds_alternative<bool>(value_); }
    bool isInt32() const { return std::holds_alternative<int32_t>(value_); }
    bool isInt64() const { return std::holds_alternative<int64_t>(value_); }
    bool isFloat() const { return std::holds_alternative<float>(value_); }
    bool isDouble() const { return std::holds_alternative<double>(value_); }
    bool isRecordId() const { return std::holds_alternative<PositionRecord>(value_); }
    bool isContinousBuffer() const { return std::holds_alternative<ContinousBuffer>(value_); }
    // Value getters with type checking
    template <typename T>
    const T& get() const {
        return std::get<T>(value_);
    }

    template <typename T>
    T& get() {
        return std::get<T>(value_);
    }

    // Safe value getters that return nullptr/default if type mismatch
    template <typename T>
    const T* getIf() const {
        return std::get_if<T>(&value_);
    }

    // Comparison operators
    bool operator==(const DataVariant& other) const { return value_ == other.value_; }

    // Type conversion utilities
    std::string toString() const;

private:
    VariantType value_;
};

// Implementation of toString()
inline std::string DataVariant::toString() const {
    return std::visit(
        [](const auto& val) -> std::string {
            using T = std::decay_t<decltype(val)>;
            if constexpr (std::is_same_v<T, std::nullptr_t>) {
                return "null";
            } else if constexpr (std::is_same_v<T, bool>) {
                return val ? "true" : "false";
            } else if constexpr (std::is_same_v<T, int32_t> || std::is_same_v<T, int64_t> || std::is_same_v<T, float>
                                 || std::is_same_v<T, double>) {
                return std::to_string(val);
            } else if constexpr (std::is_same_v<T, PositionRecord>) {
                return "PositionRecord(" + val.id_.toString() + ", offset=" + std::to_string(val.offset_)
                       + ", length=" + std::to_string(val.length_) + ")";
            } else if constexpr (std::is_same_v<T, ContinousBuffer>) {
                return "ContinousBuffer(" + std::to_string(val.size_) + " bytes)";
            }
        },
        value_);
}

// Helper type traits
template <typename T>
struct is_value_type : std::false_type {};

template <>
struct is_value_type<std::nullptr_t> : std::true_type {};

template <>
struct is_value_type<bool> : std::true_type {};

template <>
struct is_value_type<int32_t> : std::true_type {};

template <>
struct is_value_type<int64_t> : std::true_type {};

template <>
struct is_value_type<float> : std::true_type {};

template <>
struct is_value_type<double> : std::true_type {};

template <>
struct is_value_type<std::string> : std::true_type {};

template <>
struct is_value_type<PositionRecord> : std::true_type {};

template <typename T>
inline constexpr bool is_value_type_v = is_value_type<std::remove_cvref_t<T>>::value;

}  // namespace pond::common