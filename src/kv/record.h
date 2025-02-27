#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/column_type.h"
#include "common/data_chunk.h"
#include "common/result.h"
#include "common/schema.h"
#include "common/uuid.h"

namespace pond::kv {

class Record {
public:
    explicit Record(std::shared_ptr<common::Schema> schema)
        : schema_(std::move(schema)), values_(schema_->num_columns()) {}

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

        if (col_idx >= values_.size()) {
            throw std::out_of_range("Column index out of data range");
        }

        if constexpr (!std::is_same_v<T, common::DataChunk>) {
            const auto& column = schema_->columns()[col_idx];
            if (!IsTypeCompatible<T>(column.type)) {
                throw std::runtime_error("Type mismatch: Attempting to get value with incompatible type");
            }
        }

        if (IsNull(col_idx)) {
            return common::Result<T>::failure(common::ErrorCode::NullValue, "Column is null");
        }

        return UnpackValue<T>(values_[col_idx].value());
    }

    // Serialization
    common::DataChunk Serialize() const;
    static common::Result<std::unique_ptr<Record>> Deserialize(const common::DataChunk& data,
                                                               std::shared_ptr<common::Schema> schema);

    const std::shared_ptr<common::Schema>& schema() const { return schema_; }

private:
    template <typename T>
    static bool IsTypeCompatible(common::ColumnType type) {
        if constexpr (std::is_same_v<T, int32_t>) {
            return type == common::ColumnType::INT32;
        } else if constexpr (std::is_same_v<T, int64_t>) {
            return type == common::ColumnType::INT64;
        } else if constexpr (std::is_same_v<T, float>) {
            return type == common::ColumnType::FLOAT;
        } else if constexpr (std::is_same_v<T, double>) {
            return type == common::ColumnType::DOUBLE;
        } else if constexpr (std::is_same_v<T, bool>) {
            return type == common::ColumnType::BOOLEAN;
        } else if constexpr (std::is_same_v<T, std::string> || std::is_same_v<T, const char*>) {
            return type == common::ColumnType::STRING;
        } else if constexpr (std::is_same_v<T, common::DataChunk>) {
            return type == common::ColumnType::BINARY;
        } else if constexpr (std::is_same_v<T, common::UUID>) {
            return type == common::ColumnType::UUID;
        }
        return false;
    }

    std::shared_ptr<common::Schema> schema_;
    std::vector<std::optional<common::DataChunk>> values_;

    static common::DataChunk PackValue(int32_t value);
    static common::DataChunk PackValue(int64_t value);
    static common::DataChunk PackValue(float value);
    static common::DataChunk PackValue(double value);
    static common::DataChunk PackValue(bool value);
    static common::DataChunk PackValue(const std::string& value);
    static common::DataChunk PackValue(const char* value);
    static common::DataChunk PackValue(const common::DataChunk& value);
    static common::DataChunk PackValue(const common::UUID& value);

    template <typename T>
    static common::Result<T> UnpackValue(const common::DataChunk& data);
};

}  // namespace pond::kv
