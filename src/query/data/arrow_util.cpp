#include "query/data/arrow_util.h"

#include <arrow/api.h>
#include <arrow/builder.h>
#include <arrow/compute/api.h>
#include <arrow/type_traits.h>
#include <rapidjson/document.h>
#include <rapidjson/error/en.h>

#include "common/error.h"
#include "common/log.h"
#include "format/parquet/schema_converter.h"

namespace pond::query {

namespace detail {

// Type mapping between ColumnType and Arrow builder types
template <common::ColumnType T>
struct BuilderTypeMap;

template <>
struct BuilderTypeMap<common::ColumnType::INT32> {
    using type = arrow::Int32Builder;
};

template <>
struct BuilderTypeMap<common::ColumnType::INT64> {
    using type = arrow::Int64Builder;
};

template <>
struct BuilderTypeMap<common::ColumnType::UINT32> {
    using type = arrow::UInt32Builder;
};

template <>
struct BuilderTypeMap<common::ColumnType::UINT64> {
    using type = arrow::UInt64Builder;
};

template <>
struct BuilderTypeMap<common::ColumnType::FLOAT> {
    using type = arrow::FloatBuilder;
};

template <>
struct BuilderTypeMap<common::ColumnType::DOUBLE> {
    using type = arrow::DoubleBuilder;
};

template <>
struct BuilderTypeMap<common::ColumnType::BOOLEAN> {
    using type = arrow::BooleanBuilder;
};

template <>
struct BuilderTypeMap<common::ColumnType::STRING> {
    using type = arrow::StringBuilder;
};

template <>
struct BuilderTypeMap<common::ColumnType::TIMESTAMP> {
    using type = arrow::TimestampBuilder;
};

template <>
struct BuilderTypeMap<common::ColumnType::BINARY> {
    using type = arrow::BinaryBuilder;
};

// Helper function to create empty array for a specific type
template <common::ColumnType T>
arrow::Result<std::shared_ptr<arrow::Array>> CreateEmptyArrayImpl() {
    if constexpr (T == common::ColumnType::TIMESTAMP) {
        arrow::TimestampBuilder builder(arrow::timestamp(arrow::TimeUnit::MILLI), arrow::default_memory_pool());
        ARROW_RETURN_NOT_OK(builder.Finish());
        return builder.Finish();
    } else {
        typename BuilderTypeMap<T>::type builder;
        ARROW_RETURN_NOT_OK(builder.Finish());
        return builder.Finish();
    }
}

// Helper function to create array builder for a specific type
template <common::ColumnType T>
arrow::Result<std::shared_ptr<arrow::ArrayBuilder>> CreateArrayBuilderImpl() {
    if constexpr (T == common::ColumnType::TIMESTAMP) {
        return std::make_shared<arrow::TimestampBuilder>(arrow::timestamp(arrow::TimeUnit::MILLI),
                                                         arrow::default_memory_pool());
    } else {
        return std::make_shared<typename BuilderTypeMap<T>::type>();
    }
}

// Function pointer type for array creation
using CreateArrayFunc = arrow::Result<std::shared_ptr<arrow::Array>> (*)();
using CreateArrayBuilderFunc = arrow::Result<std::shared_ptr<arrow::ArrayBuilder>> (*)();

// Map of ColumnType to creation functions
const std::unordered_map<common::ColumnType, CreateArrayFunc> kCreateArrayFuncs = {
    {common::ColumnType::INT32, &CreateEmptyArrayImpl<common::ColumnType::INT32>},
    {common::ColumnType::INT64, &CreateEmptyArrayImpl<common::ColumnType::INT64>},
    {common::ColumnType::UINT32, &CreateEmptyArrayImpl<common::ColumnType::UINT32>},
    {common::ColumnType::UINT64, &CreateEmptyArrayImpl<common::ColumnType::UINT64>},
    {common::ColumnType::FLOAT, &CreateEmptyArrayImpl<common::ColumnType::FLOAT>},
    {common::ColumnType::DOUBLE, &CreateEmptyArrayImpl<common::ColumnType::DOUBLE>},
    {common::ColumnType::BOOLEAN, &CreateEmptyArrayImpl<common::ColumnType::BOOLEAN>},
    {common::ColumnType::STRING, &CreateEmptyArrayImpl<common::ColumnType::STRING>},
    {common::ColumnType::TIMESTAMP, &CreateEmptyArrayImpl<common::ColumnType::TIMESTAMP>},
    {common::ColumnType::BINARY, &CreateEmptyArrayImpl<common::ColumnType::BINARY>},
};

const std::unordered_map<common::ColumnType, CreateArrayBuilderFunc> kCreateArrayBuilderFuncs = {
    {common::ColumnType::INT32, &CreateArrayBuilderImpl<common::ColumnType::INT32>},
    {common::ColumnType::INT64, &CreateArrayBuilderImpl<common::ColumnType::INT64>},
    {common::ColumnType::UINT32, &CreateArrayBuilderImpl<common::ColumnType::UINT32>},
    {common::ColumnType::UINT64, &CreateArrayBuilderImpl<common::ColumnType::UINT64>},
    {common::ColumnType::FLOAT, &CreateArrayBuilderImpl<common::ColumnType::FLOAT>},
    {common::ColumnType::DOUBLE, &CreateArrayBuilderImpl<common::ColumnType::DOUBLE>},
    {common::ColumnType::BOOLEAN, &CreateArrayBuilderImpl<common::ColumnType::BOOLEAN>},
    {common::ColumnType::STRING, &CreateArrayBuilderImpl<common::ColumnType::STRING>},
    {common::ColumnType::TIMESTAMP, &CreateArrayBuilderImpl<common::ColumnType::TIMESTAMP>},
    {common::ColumnType::BINARY, &CreateArrayBuilderImpl<common::ColumnType::BINARY>},
};

}  // namespace detail

arrow::Result<std::shared_ptr<arrow::Array>> ArrowUtil::CreateEmptyArray(common::ColumnType type) {
    auto it = detail::kCreateArrayFuncs.find(type);
    if (it == detail::kCreateArrayFuncs.end()) {
        return arrow::Status::Invalid("Unsupported column type");
    }
    return it->second();
}

common::Result<std::shared_ptr<arrow::ArrayBuilder>> ArrowUtil::CreateArrayBuilder(common::ColumnType type) {
    auto it = detail::kCreateArrayBuilderFuncs.find(type);
    if (it == detail::kCreateArrayBuilderFuncs.end()) {
        return common::Error(common::ErrorCode::NotImplemented, "Unsupported column type");
    }
    auto builder = it->second();
    if (!builder.ok()) {
        return common::Error(common::ErrorCode::Failure,
                             "Failed to create array builder: " + builder.status().ToString());
    }
    return builder.ValueOrDie();
}

common::Result<ArrowDataBatchSharedPtr> ArrowUtil::CreateEmptyBatch(const common::Schema& schema) {
    using ResultType = common::Result<ArrowDataBatchSharedPtr>;

    // Create empty arrays for each field
    std::vector<std::shared_ptr<arrow::Array>> columns;

    for (const auto& field : schema.Fields()) {
        auto empty_array = CreateEmptyArray(field.type);
        if (!empty_array.ok()) {
            return common::Error(common::ErrorCode::Failure,
                                 "Failed to create empty array: " + empty_array.status().ToString());
        }
        columns.push_back(empty_array.ValueOrDie());
    }

    // Create Arrow schema
    auto arrow_schema_result = format::SchemaConverter::ToArrowSchema(schema);
    RETURN_IF_ERROR_T(ResultType, arrow_schema_result);

    // Create record batch
    return arrow::RecordBatch::Make(arrow_schema_result.value(), 0, columns);
}

ArrowDataBatchSharedPtr ArrowUtil::CreateEmptyBatch() {
    auto empty_schema = arrow::schema({});
    std::vector<std::shared_ptr<arrow::Array>> empty_columns;
    return arrow::RecordBatch::Make(empty_schema, 0, empty_columns);
}

common::Result<ArrowDataBatchSharedPtr> ArrowUtil::ApplyPredicate(
    const ArrowDataBatchSharedPtr& batch, const std::shared_ptr<common::Expression>& predicate) {
    if (!predicate) {
        return common::Result<ArrowDataBatchSharedPtr>::success(batch);
    }

    // For simple implementation, we'll handle only binary operations with comparison operators
    if (predicate->Type() == common::ExprType::BinaryOp) {
        auto binary_expr = std::static_pointer_cast<common::BinaryExpression>(predicate);
        auto op_type = binary_expr->OpType();

        // Handle comparison operations
        if (op_type == common::BinaryOpType::Greater || op_type == common::BinaryOpType::GreaterEqual
            || op_type == common::BinaryOpType::Less || op_type == common::BinaryOpType::LessEqual
            || op_type == common::BinaryOpType::Equal || op_type == common::BinaryOpType::NotEqual) {
            // Basic implementation - only handle column comparison with constant
            if (binary_expr->Left()->Type() == common::ExprType::Column
                && binary_expr->Right()->Type() == common::ExprType::Constant) {
                auto col_expr = std::static_pointer_cast<common::ColumnExpression>(binary_expr->Left());
                auto const_expr = std::static_pointer_cast<common::ConstantExpression>(binary_expr->Right());

                // Find the column index
                int col_idx = -1;
                for (int i = 0; i < batch->schema()->num_fields(); i++) {
                    if (batch->schema()->field(i)->name() == col_expr->ColumnName()) {
                        col_idx = i;
                        break;
                    }
                }

                if (col_idx == -1) {
                    return common::Error(common::ErrorCode::Failure, "Column not found: " + col_expr->ColumnName());
                }

                // Get the column
                auto column = batch->column(col_idx);

                // Build filter mask based on comparison type
                arrow::compute::FilterOptions filter_options(arrow::compute::FilterOptions::DROP);

                // Prepare the result of the comparison
                arrow::Result<arrow::Datum> comparison_result;

                std::string arrow_function_name;
                switch (op_type) {
                    case common::BinaryOpType::Greater:
                        arrow_function_name = "greater";
                        break;
                    case common::BinaryOpType::GreaterEqual:
                        arrow_function_name = "greater_equal";
                        break;
                    case common::BinaryOpType::Less:
                        arrow_function_name = "less";
                        break;
                    case common::BinaryOpType::LessEqual:
                        arrow_function_name = "less_equal";
                        break;
                    case common::BinaryOpType::Equal:
                        arrow_function_name = "equal";
                        break;
                    case common::BinaryOpType::NotEqual:
                        arrow_function_name = "not_equal";
                        break;
                    default:
                        return common::Error(common::ErrorCode::NotImplemented, "Unsupported comparison type");
                }

                // Switch based on column type and comparison type
                switch (column->type_id()) {
                    case arrow::Type::INT32: {
                        // Use the appropriate getter based on constant type
                        int64_t value = (const_expr->GetColumnType() == common::ColumnType::INT32
                                         || const_expr->GetColumnType() == common::ColumnType::INT64)
                                            ? static_cast<int64_t>(const_expr->GetInteger())
                                            : static_cast<int64_t>(const_expr->GetFloat());

                        auto scalar = std::make_shared<arrow::Int64Scalar>(value);
                        comparison_result = arrow::compute::CallFunction(arrow_function_name,
                                                                         {arrow::Datum(column), arrow::Datum(scalar)});

                        break;
                    }
                    case arrow::Type::DOUBLE: {
                        double value = const_expr->GetFloat();
                        auto scalar = std::make_shared<arrow::DoubleScalar>(value);

                        comparison_result = arrow::compute::CallFunction(arrow_function_name,
                                                                         {arrow::Datum(column), arrow::Datum(scalar)});

                        break;
                    }
                    case arrow::Type::STRING: {
                        std::string value = const_expr->Value();
                        auto scalar = std::make_shared<arrow::StringScalar>(value);

                        comparison_result = arrow::compute::CallFunction(arrow_function_name,
                                                                         {arrow::Datum(column), arrow::Datum(scalar)});

                        break;
                    }
                    default:
                        return common::Error(common::ErrorCode::NotImplemented,
                                             "Unsupported column type for filtering");
                }

                if (!comparison_result.ok()) {
                    return common::Error(common::ErrorCode::Failure,
                                         "Failed to perform comparison: " + comparison_result.status().ToString());
                }

                // Apply the filter
                auto filter_result =
                    arrow::compute::Filter(arrow::Datum(batch), comparison_result.ValueOrDie(), filter_options);
                if (!filter_result.ok()) {
                    return common::Error(common::ErrorCode::Failure,
                                         "Failed to apply filter: " + filter_result.status().ToString());
                }

                // Return the filtered record batch
                return common::Result<ArrowDataBatchSharedPtr>::success(filter_result.ValueOrDie().record_batch());
            }
        }
    }

    return common::Error(common::ErrorCode::NotImplemented, "Unsupported predicate expression type");
}

common::Result<ArrowDataBatchSharedPtr> ArrowUtil::ConcatenateBatches(
    const std::vector<ArrowDataBatchSharedPtr>& batches) {
    using ResultType = common::Result<ArrowDataBatchSharedPtr>;

    // Handle empty input case
    if (batches.empty()) {
        return ResultType::success(CreateEmptyBatch());
    }

    // If there's only one batch, return it directly
    if (batches.size() == 1) {
        return ResultType::success(batches[0]);
    }

    // Get total number of rows
    int64_t total_rows = 0;
    for (const auto& batch : batches) {
        total_rows += batch->num_rows();
    }

    // First, ensure all batches have the same schema
    for (size_t i = 1; i < batches.size(); i++) {
        if (!batches[0]->schema()->Equals(*batches[i]->schema())) {
            return ResultType::failure(common::ErrorCode::InvalidArgument,
                                       "Cannot concatenate batches with different schemas");
        }
    }

    // Create arrays to hold the concatenated columns
    std::vector<std::shared_ptr<arrow::Array>> concatenated_arrays;
    concatenated_arrays.resize(batches[0]->num_columns());

    // For each column, concatenate the arrays from all batches
    for (int col = 0; col < batches[0]->num_columns(); col++) {
        // Collect arrays for this column from all batches
        std::vector<std::shared_ptr<arrow::Array>> arrays_to_concat;
        for (const auto& batch : batches) {
            arrays_to_concat.push_back(batch->column(col));
        }

        // Concatenate the arrays
        arrow::Result<std::shared_ptr<arrow::Array>> concat_result = arrow::Concatenate(arrays_to_concat);
        if (!concat_result.ok()) {
            return ResultType::failure(common::ErrorCode::Failure,
                                       "Failed to concatenate arrays: " + concat_result.status().ToString());
        }

        concatenated_arrays[col] = concat_result.ValueOrDie();
    }

    // Create a new record batch with the concatenated arrays
    return ResultType::success(arrow::RecordBatch::Make(batches[0]->schema(), total_rows, concatenated_arrays));
}

common::Result<ArrowDataBatchSharedPtr> ArrowUtil::JsonToRecordBatch(const std::string& json_str,
                                                                     const common::Schema& schema) {
    using ResultType = common::Result<ArrowDataBatchSharedPtr>;

    rapidjson::Document doc;
    rapidjson::ParseResult parse_result = doc.Parse(json_str.c_str());
    if (!parse_result) {
        std::string error_msg = "JSON parse error: ";
        error_msg += rapidjson::GetParseError_En(parse_result.Code());
        error_msg += " at offset " + std::to_string(parse_result.Offset());
        return ResultType::failure(common::ErrorCode::InvalidArgument, error_msg);
    }

    return JsonToRecordBatch(doc, schema);
}

common::Result<ArrowDataBatchSharedPtr> ArrowUtil::JsonToRecordBatch(const rapidjson::Document& json_doc,
                                                                     const common::Schema& schema) {
    using ResultType = common::Result<ArrowDataBatchSharedPtr>;

    if (!json_doc.IsArray()) {
        return ResultType::failure(common::ErrorCode::InvalidArgument, "JSON document must be an array of objects");
    }

    if (json_doc.Empty()) {
        // Handle empty array case by returning an empty batch
        return CreateEmptyBatch(schema);
    }

    const auto& columns = schema.Columns();
    size_t num_rows = json_doc.Size();
    size_t num_cols = columns.size();

    // Create arrow builders for each column
    std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders;
    builders.reserve(num_cols);

    for (const auto& col_schema : columns) {
        arrow::Result<std::shared_ptr<arrow::Array>> empty_array_result = CreateEmptyArray(col_schema.type);

        if (!empty_array_result.ok()) {
            return ResultType::failure(common::ErrorCode::InternalError,
                                       "Failed to create array builder for column " + col_schema.name);
        }

        // Get the builder type for this column
        std::shared_ptr<arrow::DataType> data_type = empty_array_result.ValueOrDie()->type();

        // Use MakeBuilder with the correct signature that returns a Result
        arrow::Result<std::unique_ptr<arrow::ArrayBuilder>> builder_result =
            arrow::MakeBuilder(data_type, arrow::default_memory_pool());

        if (!builder_result.ok()) {
            return ResultType::failure(
                common::ErrorCode::InternalError,
                "Failed to create builder for column " + col_schema.name + ": " + builder_result.status().ToString());
        }

        builders.push_back(std::move(builder_result.ValueOrDie()));
    }

    // Process each row in the JSON array
    for (rapidjson::SizeType row_idx = 0; row_idx < num_rows; row_idx++) {
        const auto& json_row = json_doc[row_idx];

        if (!json_row.IsObject()) {
            return ResultType::failure(common::ErrorCode::InvalidArgument,
                                       "Row " + std::to_string(row_idx) + " is not an object");
        }

        // Process each column in the schema
        for (size_t col_idx = 0; col_idx < num_cols; col_idx++) {
            const auto& col_schema = columns[col_idx];
            const std::string& col_name = col_schema.name;

            // Check if this column exists in the current row
            bool is_null = !json_row.HasMember(col_name.c_str());

            if (is_null) {
                // Column not present, treat as null
                if (col_schema.nullability == common::Nullability::NOT_NULL) {
                    return ResultType::failure(
                        common::ErrorCode::InvalidArgument,
                        "Row " + std::to_string(row_idx) + ": Non-nullable column '" + col_name + "' has null value");
                }

                auto status = builders[col_idx]->AppendNull();
                if (!status.ok()) {
                    return ResultType::failure(common::ErrorCode::InternalError,
                                               "Failed to append null to column " + col_name);
                }
                continue;
            }

            const auto& json_value = json_row[col_name.c_str()];

            // Check if value is null
            if (json_value.IsNull()) {
                if (col_schema.nullability == common::Nullability::NOT_NULL) {
                    return ResultType::failure(
                        common::ErrorCode::InvalidArgument,
                        "Row " + std::to_string(row_idx) + ": Non-nullable column '" + col_name + "' has null value");
                }

                auto status = builders[col_idx]->AppendNull();
                if (!status.ok()) {
                    return ResultType::failure(common::ErrorCode::InternalError,
                                               "Failed to append null to column " + col_name);
                }
                continue;
            }

            // Handle different column types
            arrow::Status status;
            switch (col_schema.type) {
                case common::ColumnType::INT32: {
                    if (!json_value.IsInt()) {
                        return ResultType::failure(common::ErrorCode::InvalidArgument,
                                                   "Row " + std::to_string(row_idx) + ": Value for column '" + col_name
                                                       + "' is not an integer");
                    }
                    status = static_cast<arrow::Int32Builder*>(builders[col_idx].get())->Append(json_value.GetInt());
                    break;
                }
                case common::ColumnType::INT64: {
                    if (!json_value.IsInt64()) {
                        return ResultType::failure(common::ErrorCode::InvalidArgument,
                                                   "Row " + std::to_string(row_idx) + ": Value for column '" + col_name
                                                       + "' is not an integer");
                    }
                    status = static_cast<arrow::Int64Builder*>(builders[col_idx].get())->Append(json_value.GetInt64());
                    break;
                }
                case common::ColumnType::FLOAT: {
                    if (!json_value.IsNumber()) {
                        return ResultType::failure(
                            common::ErrorCode::InvalidArgument,
                            "Row " + std::to_string(row_idx) + ": Value for column '" + col_name + "' is not a number");
                    }
                    status = static_cast<arrow::FloatBuilder*>(builders[col_idx].get())
                                 ->Append(static_cast<float>(json_value.GetDouble()));
                    break;
                }
                case common::ColumnType::DOUBLE: {
                    if (!json_value.IsNumber()) {
                        return ResultType::failure(
                            common::ErrorCode::InvalidArgument,
                            "Row " + std::to_string(row_idx) + ": Value for column '" + col_name + "' is not a number");
                    }
                    status =
                        static_cast<arrow::DoubleBuilder*>(builders[col_idx].get())->Append(json_value.GetDouble());
                    break;
                }
                case common::ColumnType::BOOLEAN: {
                    if (!json_value.IsBool()) {
                        return ResultType::failure(common::ErrorCode::InvalidArgument,
                                                   "Row " + std::to_string(row_idx) + ": Value for column '" + col_name
                                                       + "' is not a boolean");
                    }
                    status = static_cast<arrow::BooleanBuilder*>(builders[col_idx].get())->Append(json_value.GetBool());
                    break;
                }
                case common::ColumnType::STRING: {
                    if (!json_value.IsString()) {
                        return ResultType::failure(
                            common::ErrorCode::InvalidArgument,
                            "Row " + std::to_string(row_idx) + ": Value for column '" + col_name + "' is not a string");
                    }
                    status =
                        static_cast<arrow::StringBuilder*>(builders[col_idx].get())->Append(json_value.GetString());
                    break;
                }
                case common::ColumnType::BINARY: {
                    if (!json_value.IsString()) {
                        return ResultType::failure(
                            common::ErrorCode::InvalidArgument,
                            "Row " + std::to_string(row_idx) + ": Value for column '" + col_name + "' is not a string");
                    }
                    // For binary, we assume the string is a base64-encoded binary value
                    // In a real implementation, you'd decode the base64 string here
                    status = static_cast<arrow::BinaryBuilder*>(builders[col_idx].get())
                                 ->Append(reinterpret_cast<const uint8_t*>(json_value.GetString()),
                                          json_value.GetStringLength());
                    break;
                }
                case common::ColumnType::TIMESTAMP: {
                    // For timestamp, we assume the value is a number representing milliseconds since epoch
                    if (json_value.IsNumber()) {
                        status = static_cast<arrow::TimestampBuilder*>(builders[col_idx].get())
                                     ->Append(json_value.GetInt64());
                    } else if (json_value.IsString()) {
                        // In a real implementation, you'd parse the string to a timestamp here
                        // For simplicity, we'll just reject string timestamps
                        return ResultType::failure(common::ErrorCode::InvalidArgument,
                                                   "Row " + std::to_string(row_idx)
                                                       + ": String timestamp not implemented for column '" + col_name
                                                       + "'");
                    } else {
                        return ResultType::failure(common::ErrorCode::InvalidArgument,
                                                   "Row " + std::to_string(row_idx) + ": Value for column '" + col_name
                                                       + "' is not a valid timestamp");
                    }
                    break;
                }
                default:
                    return ResultType::failure(common::ErrorCode::InvalidArgument,
                                               "Unsupported column type for column " + col_name);
            }

            if (!status.ok()) {
                return ResultType::failure(common::ErrorCode::InternalError,
                                           "Failed to append value to column " + col_name + ": " + status.ToString());
            }
        }
    }

    // Finalize arrays
    std::vector<std::shared_ptr<arrow::Array>> arrays(num_cols);
    std::vector<std::shared_ptr<arrow::Field>> fields(num_cols);

    for (size_t i = 0; i < num_cols; i++) {
        const auto& col_schema = columns[i];

        // Create arrow data type for the column
        std::shared_ptr<arrow::DataType> arrow_type;
        switch (col_schema.type) {
            case common::ColumnType::INT32:
                arrow_type = arrow::int32();
                break;
            case common::ColumnType::INT64:
                arrow_type = arrow::int64();
                break;
            case common::ColumnType::FLOAT:
                arrow_type = arrow::float32();
                break;
            case common::ColumnType::DOUBLE:
                arrow_type = arrow::float64();
                break;
            case common::ColumnType::BOOLEAN:
                arrow_type = arrow::boolean();
                break;
            case common::ColumnType::STRING:
                arrow_type = arrow::utf8();
                break;
            case common::ColumnType::BINARY:
                arrow_type = arrow::binary();
                break;
            case common::ColumnType::TIMESTAMP:
                arrow_type = arrow::timestamp(arrow::TimeUnit::MILLI);
                break;
            default:
                return ResultType::failure(common::ErrorCode::InvalidArgument,
                                           "Unsupported column type for column " + col_schema.name);
        }

        // Create field with nullability
        bool nullable = (col_schema.nullability == common::Nullability::NULLABLE);
        fields[i] = arrow::field(col_schema.name, arrow_type, nullable);

        // Finalize array
        std::shared_ptr<arrow::Array> array;
        arrow::Status status = builders[i]->Finish(&array);
        if (!status.ok()) {
            return ResultType::failure(
                common::ErrorCode::InternalError,
                "Failed to finalize array for column " + col_schema.name + ": " + status.ToString());
        }

        arrays[i] = array;
    }

    // Create arrow schema and record batch
    auto arrow_schema = arrow::schema(fields);
    auto record_batch = arrow::RecordBatch::Make(arrow_schema, num_rows, arrays);

    return ResultType::success(record_batch);
}

common::Result<void> ArrowUtil::AppendGroupValue(const std::shared_ptr<arrow::Array>& input_array,
                                                 std::shared_ptr<arrow::ArrayBuilder> builder,
                                                 int row_idx) {
    switch (input_array->type_id()) {
        case arrow::Type::INT32:
            ArrowUtil::AppendGroupValueInternal<arrow::Int32Array, arrow::Int32Builder>(input_array, builder, row_idx);
            break;
        case arrow::Type::INT64:
            ArrowUtil::AppendGroupValueInternal<arrow::Int64Array, arrow::Int64Builder>(input_array, builder, row_idx);
            break;
        case arrow::Type::UINT32:
            ArrowUtil::AppendGroupValueInternal<arrow::UInt32Array, arrow::UInt32Builder>(
                input_array, builder, row_idx);
            break;
        case arrow::Type::UINT64:
            ArrowUtil::AppendGroupValueInternal<arrow::UInt64Array, arrow::UInt64Builder>(
                input_array, builder, row_idx);
            break;
        case arrow::Type::FLOAT:
            ArrowUtil::AppendGroupValueInternal<arrow::FloatArray, arrow::FloatBuilder>(input_array, builder, row_idx);
            break;
        case arrow::Type::DOUBLE:
            ArrowUtil::AppendGroupValueInternal<arrow::DoubleArray, arrow::DoubleBuilder>(
                input_array, builder, row_idx);
            break;
        case arrow::Type::STRING:
            ArrowUtil::AppendGroupValueInternal<arrow::StringArray, arrow::StringBuilder>(
                input_array, builder, row_idx);
            break;
        case arrow::Type::BOOL:
            ArrowUtil::AppendGroupValueInternal<arrow::BooleanArray, arrow::BooleanBuilder>(
                input_array, builder, row_idx);
            break;
        default: {
            return common::Error(common::ErrorCode::NotImplemented, "Unsupported type in GROUP BY");
        }
    }
    return common::Result<void>::success();
}

common::Result<void> ArrowUtil::AppendGroupKeyValue(const std::shared_ptr<arrow::Array>& array,
                                                    int row_idx,
                                                    std::string& group_key) {
    switch (array->type_id()) {
        case arrow::Type::INT32:
            ArrowUtil::AppendGroupKeyValue<arrow::Int32Array>(array, row_idx, group_key);
            break;
        case arrow::Type::INT64:
            ArrowUtil::AppendGroupKeyValue<arrow::Int64Array>(array, row_idx, group_key);
            break;
        case arrow::Type::UINT32:
            ArrowUtil::AppendGroupKeyValue<arrow::UInt32Array>(array, row_idx, group_key);
            break;
        case arrow::Type::UINT64:
            ArrowUtil::AppendGroupKeyValue<arrow::UInt64Array>(array, row_idx, group_key);
            break;
        case arrow::Type::FLOAT:
            ArrowUtil::AppendGroupKeyValue<arrow::FloatArray>(array, row_idx, group_key);
            break;
        case arrow::Type::DOUBLE:
            ArrowUtil::AppendGroupKeyValue<arrow::DoubleArray>(array, row_idx, group_key);
            break;
        case arrow::Type::STRING:
            ArrowUtil::AppendGroupKeyValue<arrow::StringArray>(array, row_idx, group_key);
            break;
        case arrow::Type::BOOL:
            ArrowUtil::AppendGroupKeyValue<arrow::BooleanArray>(array, row_idx, group_key);
            break;
        default: {
            return common::Error(common::ErrorCode::NotImplemented, "Unsupported type in GROUP BY");
        }
    }

    return common::Result<void>::success();
}

}  // namespace pond::query