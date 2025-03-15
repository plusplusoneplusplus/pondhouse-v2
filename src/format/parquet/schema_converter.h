#pragma once

#include <memory>
#include <string>

#include <arrow/api.h>

#include "common/result.h"
#include "common/schema.h"

namespace pond::format {

class SchemaConverter {
public:
    // Convert Pond column type to Arrow data type
    [[nodiscard]] static std::shared_ptr<arrow::DataType> ToArrowDataType(common::ColumnType type);

    // Convert Pond column schema to Arrow field
    [[nodiscard]] static std::shared_ptr<arrow::Field> ToArrowField(const common::ColumnSchema& column);

    // Convert entire Pond schema to Arrow schema
    [[nodiscard]] static common::Result<std::shared_ptr<arrow::Schema>> ToArrowSchema(const common::Schema& schema);

    [[nodiscard]] static common::Result<std::shared_ptr<arrow::Schema>> ToArrowSchema(
        const std::shared_ptr<common::Schema>& schema);

    // Convert Arrow schema to Pond schema
    // Convert Arrow data type to Pond column type
    [[nodiscard]] static common::Result<common::ColumnType> FromArrowDataType(
        const std::shared_ptr<arrow::DataType>& type);

    // Convert Arrow field to Pond column schema
    [[nodiscard]] static common::Result<common::ColumnSchema> FromArrowField(
        const std::shared_ptr<arrow::Field>& field);

    // Convert Arrow schema to Pond schema
    [[nodiscard]] static common::Result<std::shared_ptr<common::Schema>> FromArrowSchema(
        const std::shared_ptr<arrow::Schema>& schema);

    // Validate Arrow schema against Pond schema
    [[nodiscard]] static common::Result<void> ValidateSchema(const std::shared_ptr<arrow::Schema>& schema,
                                                             const std::shared_ptr<common::Schema>& table_schema);
};

}  // namespace pond::format