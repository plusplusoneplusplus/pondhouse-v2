#include "format/parquet/schema_converter.h"

#include <arrow/type.h>
#include <arrow/type_traits.h>

#include "common/error.h"
#include "common/log.h"
#include "common/result.h"

namespace pond::format {

std::shared_ptr<arrow::DataType> SchemaConverter::ToArrowDataType(common::ColumnType type) {
    switch (type) {
        case common::ColumnType::INT32:
            return arrow::int32();
        case common::ColumnType::INT64:
            return arrow::int64();
        case common::ColumnType::FLOAT:
            return arrow::float32();
        case common::ColumnType::DOUBLE:
            return arrow::float64();
        case common::ColumnType::STRING:
            return arrow::utf8();
        case common::ColumnType::BINARY:
            return arrow::binary();
        case common::ColumnType::BOOLEAN:
            return arrow::boolean();
        case common::ColumnType::TIMESTAMP:
            return arrow::timestamp(arrow::TimeUnit::MICRO);
        case common::ColumnType::UUID:
            return arrow::fixed_size_binary(16);  // UUID is 16 bytes
        default:
            return nullptr;
    }
}

std::shared_ptr<arrow::Field> SchemaConverter::ToArrowField(const common::ColumnSchema& column) {
    auto arrow_type = ToArrowDataType(column.type);
    bool nullable = column.nullability == common::Nullability::NULLABLE;
    return arrow::field(column.name, arrow_type, nullable);
}

common::Result<std::shared_ptr<arrow::Schema>> SchemaConverter::ToArrowSchema(const common::Schema& schema) {
    std::vector<std::shared_ptr<arrow::Field>> fields;
    fields.reserve(schema.num_columns());

    for (const auto& column : schema.columns()) {
        auto arrow_type = ToArrowDataType(column.type);
        if (!arrow_type) {
            return common::Result<std::shared_ptr<arrow::Schema>>::failure(
                common::ErrorCode::InvalidArgument, "Unsupported column type for column: " + column.name);
        }
        fields.push_back(ToArrowField(column));
    }

    return common::Result<std::shared_ptr<arrow::Schema>>::success(arrow::schema(fields));
}

common::Result<common::ColumnType> SchemaConverter::FromArrowDataType(const std::shared_ptr<arrow::DataType>& type) {
    switch (type->id()) {
        case arrow::Type::INT32:
            return common::Result<common::ColumnType>::success(common::ColumnType::INT32);
        case arrow::Type::INT64:
            return common::Result<common::ColumnType>::success(common::ColumnType::INT64);
        case arrow::Type::FLOAT:
            return common::Result<common::ColumnType>::success(common::ColumnType::FLOAT);
        case arrow::Type::DOUBLE:
            return common::Result<common::ColumnType>::success(common::ColumnType::DOUBLE);
        case arrow::Type::STRING:
        case arrow::Type::LARGE_STRING:
            return common::Result<common::ColumnType>::success(common::ColumnType::STRING);
        case arrow::Type::BINARY:
        case arrow::Type::LARGE_BINARY:
            return common::Result<common::ColumnType>::success(common::ColumnType::BINARY);
        case arrow::Type::BOOL:
            return common::Result<common::ColumnType>::success(common::ColumnType::BOOLEAN);
        case arrow::Type::TIMESTAMP:
            return common::Result<common::ColumnType>::success(common::ColumnType::TIMESTAMP);
        case arrow::Type::FIXED_SIZE_BINARY:
            if (static_cast<const arrow::FixedSizeBinaryType*>(type.get())->byte_width() == 16) {
                return common::Result<common::ColumnType>::success(common::ColumnType::UUID);
            }
            [[fallthrough]];
        default:
            return common::Result<common::ColumnType>::failure(common::ErrorCode::InvalidArgument,
                                                               "Unsupported Arrow data type: " + type->ToString());
    }
}

common::Result<common::ColumnSchema> SchemaConverter::FromArrowField(const std::shared_ptr<arrow::Field>& field) {
    using ReturnType = common::Result<common::ColumnSchema>;
    auto type_result = FromArrowDataType(field->type());
    RETURN_IF_ERROR_T(ReturnType, type_result);

    auto nullability = field->nullable() ? common::Nullability::NULLABLE : common::Nullability::NOT_NULL;
    return common::Result<common::ColumnSchema>::success(
        common::ColumnSchema(field->name(), type_result.value(), nullability));
}

common::Result<std::shared_ptr<common::Schema>> SchemaConverter::FromArrowSchema(
    const std::shared_ptr<arrow::Schema>& schema) {
    using ReturnType = common::Result<std::shared_ptr<common::Schema>>;

    std::vector<common::ColumnSchema> columns;
    columns.reserve(schema->num_fields());

    for (const auto& field : schema->fields()) {
        auto column_result = FromArrowField(field);
        RETURN_IF_ERROR_T(ReturnType, column_result);

        columns.push_back(column_result.value());
    }

    return common::Result<std::shared_ptr<common::Schema>>::success(
        std::make_shared<common::Schema>(std::move(columns)));
}

}  // namespace pond::format