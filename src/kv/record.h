#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/data_chunk.h"
#include "common/result.h"

namespace pond::kv {

enum class ColumnType { INT32, INT64, FLOAT, DOUBLE, STRING, BINARY, BOOLEAN, TIMESTAMP };

struct ColumnSchema {
    std::string name;
    ColumnType type;
    bool nullable;

    ColumnSchema(std::string name_, ColumnType type_, bool nullable_ = true)
        : name(std::move(name_)), type(type_), nullable(nullable_) {}
};

class Schema {
public:
    explicit Schema(std::vector<ColumnSchema> columns) : columns_(std::move(columns)) {
        for (size_t i = 0; i < columns_.size(); i++) {
            column_indices_[columns_[i].name] = i;
        }
    }

    const std::vector<ColumnSchema>& columns() const { return columns_; }
    size_t num_columns() const { return columns_.size(); }

    int GetColumnIndex(const std::string& name) const {
        auto it = column_indices_.find(name);
        return it == column_indices_.end() ? -1 : static_cast<int>(it->second);
    }

private:
    std::vector<ColumnSchema> columns_;
    std::unordered_map<std::string, size_t> column_indices_;
};

class Record {
public:
    explicit Record(std::shared_ptr<Schema> schema) : schema_(std::move(schema)), values_(schema_->num_columns()) {}

    void SetNull(size_t col_idx) {
        if (col_idx >= schema_->num_columns()) {
            throw std::out_of_range("Column index out of range");
        }
        values_[col_idx].reset();
    }

    template <typename T>
    void Set(size_t col_idx, const T& value) {
        if (col_idx >= schema_->num_columns()) {
            throw std::out_of_range("Column index out of range");
        }

        const auto& column = schema_->columns()[col_idx];
        if constexpr (std::is_same_v<T, common::DataChunk>) {
            // Special case for DataChunk to directly update the value
            values_[col_idx] = PackValue(value);
            return;
        }

        if (!IsTypeCompatible<T>(column.type)) {
            throw std::runtime_error("Type mismatch: Attempting to set value with incompatible type");
        }

        values_[col_idx] = PackValue(value);
    }

    bool IsNull(size_t col_idx) const {
        if (col_idx >= schema_->num_columns()) {
            throw std::out_of_range("Column index out of range");
        }
        return !values_[col_idx].has_value();
    }

    template <typename T>
    common::Result<T> Get(size_t col_idx) const {
        if (col_idx >= schema_->num_columns()) {
            throw std::out_of_range("Column index out of range");
        }

        if constexpr (!std::is_same_v<T, common::DataChunk>) {
            const auto& column = schema_->columns()[col_idx];
            if (!IsTypeCompatible<T>(column.type)) {
                throw std::runtime_error("Type mismatch: Attempting to get value with incompatible type");
            }
        }

        if (IsNull(col_idx)) {
            return common::Result<T>::failure(common::ErrorCode::InvalidOperation, "Column is null");
        }

        return UnpackValue<T>(values_[col_idx].value());
    }

    // Serialization
    common::DataChunk Serialize() const;
    static common::Result<std::unique_ptr<Record>> Deserialize(const common::DataChunk& data,
                                                               std::shared_ptr<Schema> schema);

    const std::shared_ptr<Schema>& schema() const { return schema_; }

private:
    template <typename T>
    static bool IsTypeCompatible(ColumnType type) {
        if constexpr (std::is_same_v<T, int32_t>) {
            return type == ColumnType::INT32;
        } else if constexpr (std::is_same_v<T, int64_t>) {
            return type == ColumnType::INT64;
        } else if constexpr (std::is_same_v<T, float>) {
            return type == ColumnType::FLOAT;
        } else if constexpr (std::is_same_v<T, double>) {
            return type == ColumnType::DOUBLE;
        } else if constexpr (std::is_same_v<T, bool>) {
            return type == ColumnType::BOOLEAN;
        } else if constexpr (std::is_same_v<T, std::string> || std::is_same_v<T, const char*>) {
            return type == ColumnType::STRING;
        } else if constexpr (std::is_same_v<T, common::DataChunk>) {
            return type == ColumnType::BINARY;
        }
        return false;
    }

    std::shared_ptr<Schema> schema_;
    std::vector<std::optional<common::DataChunk>> values_;

    static common::DataChunk PackValue(int32_t value);
    static common::DataChunk PackValue(int64_t value);
    static common::DataChunk PackValue(float value);
    static common::DataChunk PackValue(double value);
    static common::DataChunk PackValue(bool value);
    static common::DataChunk PackValue(const std::string& value);
    static common::DataChunk PackValue(const char* value);
    static common::DataChunk PackValue(const common::DataChunk& value);

    template <typename T>
    static common::Result<T> UnpackValue(const common::DataChunk& data);
};

}  // namespace pond::kv
