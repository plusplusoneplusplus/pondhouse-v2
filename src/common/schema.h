#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "common/column_type.h"

namespace pond::common {

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

}  // namespace pond::common