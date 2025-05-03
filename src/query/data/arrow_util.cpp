#include "query/data/arrow_util.h"

#include <iomanip>  // Add this include for stream manipulators
#include <set>

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
                case common::ColumnType::UINT32: {
                    if (!json_value.IsUint()) {
                        return ResultType::failure(common::ErrorCode::InvalidArgument,
                                                   "Row " + std::to_string(row_idx) + ": Value for column '" + col_name
                                                       + "' is not an unsigned integer");
                    }
                    status = static_cast<arrow::UInt32Builder*>(builders[col_idx].get())->Append(json_value.GetUint());
                    break;
                }
                case common::ColumnType::UINT64: {
                    if (!json_value.IsUint64()) {
                        return ResultType::failure(common::ErrorCode::InvalidArgument,
                                                   "Row " + std::to_string(row_idx) + ": Value for column '" + col_name
                                                       + "' is not an unsigned integer");
                    }
                    status =
                        static_cast<arrow::UInt64Builder*>(builders[col_idx].get())->Append(json_value.GetUint64());
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
            case common::ColumnType::UINT32:
                arrow_type = arrow::uint32();
                break;
            case common::ColumnType::UINT64:
                arrow_type = arrow::uint64();
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

template <typename T>
void GroupKey::AddValue(const T& value, bool is_null) {
    if (is_null) {
        values_.push_back(T{});  // Default value
        null_flags_.push_back(true);
    } else {
        values_.push_back(value);
        null_flags_.push_back(false);
    }
}

bool GroupKey::operator<(const GroupKey& other) const {
    if (values_.size() != other.values_.size()) {
        return values_.size() < other.values_.size();
    }

    for (size_t i = 0; i < values_.size(); i++) {
        if (null_flags_[i] != other.null_flags_[i]) {
            return null_flags_[i];  // Nulls sort first
        }
        if (null_flags_[i]) {
            continue;  // Both null, check next value
        }
        if (values_[i] != other.values_[i]) {
            return values_[i] < other.values_[i];
        }
    }
    return false;
}

bool GroupKey::operator==(const GroupKey& other) const {
    if (values_.size() != other.values_.size()) {
        return false;
    }

    for (size_t i = 0; i < values_.size(); i++) {
        if (null_flags_[i] != other.null_flags_[i]) {
            return false;
        }
        if (!null_flags_[i] && values_[i] != other.values_[i]) {
            return false;
        }
    }
    return true;
}

std::string GroupKey::ToString() const {
    std::stringstream ss;
    for (size_t i = 0; i < values_.size(); i++) {
        if (null_flags_[i]) {
            ss << "null";
        } else {
            // Use std::visit to handle the variant
            std::visit(
                [&ss](const auto& value) {
                    if constexpr (std::is_same_v<std::decay_t<decltype(value)>, std::string>) {
                        ss << '"' << value << '"';  // Quote string values
                    } else {
                        ss << value;
                    }
                },
                values_[i]);
        }
        if (i < values_.size() - 1) {
            ss << ",";
        }
    }
    return ss.str();
}

common::Result<std::vector<GroupKey>> ArrowUtil::ExtractGroupKeys(const ArrowDataBatchSharedPtr& batch,
                                                                  const std::vector<std::string>& group_by_columns) {
    using ResultType = common::Result<std::vector<GroupKey>>;

    // Validate inputs
    if (!batch) {
        return ResultType::failure(common::ErrorCode::InvalidArgument, "Input batch is null");
    }

    if (group_by_columns.empty()) {
        return ResultType::failure(common::ErrorCode::InvalidArgument, "Group by columns list is empty");
    }

    // Get column indices and validate column names
    std::vector<int> column_indices;
    column_indices.reserve(group_by_columns.size());

    for (const auto& col_name : group_by_columns) {
        int col_idx = -1;
        for (int i = 0; i < batch->schema()->num_fields(); i++) {
            if (batch->schema()->field(i)->name() == col_name) {
                col_idx = i;
                break;
            }
        }

        if (col_idx == -1) {
            return ResultType::failure(common::ErrorCode::InvalidArgument, "Column not found: " + col_name);
        }
        column_indices.push_back(col_idx);
    }

    // Use set for unique keys
    std::set<GroupKey> unique_keys;

    // Process each row
    for (int row_idx = 0; row_idx < batch->num_rows(); row_idx++) {
        GroupKey key;

        // Build composite key from group by columns
        for (int col_idx : column_indices) {
            auto array = batch->column(col_idx);
            bool is_null = array->IsNull(row_idx);

            switch (array->type_id()) {
                case arrow::Type::INT32: {
                    auto typed_array = std::static_pointer_cast<arrow::Int32Array>(array);
                    key.AddValue(is_null ? 0 : typed_array->Value(row_idx), is_null);
                    break;
                }
                case arrow::Type::INT64: {
                    auto typed_array = std::static_pointer_cast<arrow::Int64Array>(array);
                    key.AddValue(is_null ? 0 : typed_array->Value(row_idx), is_null);
                    break;
                }
                case arrow::Type::UINT32: {
                    auto typed_array = std::static_pointer_cast<arrow::UInt32Array>(array);
                    key.AddValue(is_null ? 0 : typed_array->Value(row_idx), is_null);
                    break;
                }
                case arrow::Type::UINT64: {
                    auto typed_array = std::static_pointer_cast<arrow::UInt64Array>(array);
                    key.AddValue(is_null ? 0 : typed_array->Value(row_idx), is_null);
                    break;
                }
                case arrow::Type::FLOAT: {
                    auto typed_array = std::static_pointer_cast<arrow::FloatArray>(array);
                    key.AddValue(is_null ? 0.0f : typed_array->Value(row_idx), is_null);
                    break;
                }
                case arrow::Type::DOUBLE: {
                    auto typed_array = std::static_pointer_cast<arrow::DoubleArray>(array);
                    key.AddValue(is_null ? 0.0 : typed_array->Value(row_idx), is_null);
                    break;
                }
                case arrow::Type::STRING: {
                    auto typed_array = std::static_pointer_cast<arrow::StringArray>(array);
                    key.AddValue(is_null ? "" : typed_array->GetString(row_idx), is_null);
                    break;
                }
                case arrow::Type::BOOL: {
                    auto typed_array = std::static_pointer_cast<arrow::BooleanArray>(array);
                    key.AddValue(is_null ? false : typed_array->Value(row_idx), is_null);
                    break;
                }
                default:
                    return ResultType::failure(common::ErrorCode::NotImplemented, "Unsupported type in GROUP BY");
            }
        }

        unique_keys.insert(key);
    }

    return ResultType::success(std::vector<GroupKey>(unique_keys.begin(), unique_keys.end()));
}

common::Result<ArrowDataBatchSharedPtr> ArrowUtil::HashAggregate(
    const ArrowDataBatchSharedPtr& batch,
    const std::vector<std::string>& group_by_columns,
    const std::vector<std::string>& agg_columns,
    const std::vector<common::AggregateType>& agg_types,
    const std::vector<std::string>& output_columns_override /* = {}*/) {
    using ReturnType = common::Result<ArrowDataBatchSharedPtr>;

    // Validate inputs
    if (!batch) {
        return ReturnType::failure(common::ErrorCode::InvalidArgument, "Input batch is null");
    }
    if (group_by_columns.empty()) {
        return ReturnType::failure(common::ErrorCode::InvalidArgument, "Group by columns cannot be empty");
    }
    if (agg_columns.size() != agg_types.size()) {
        return ReturnType::failure(common::ErrorCode::InvalidArgument,
                                   "Number of aggregate columns does not match number of aggregate types");
    }
    if (!output_columns_override.empty() && output_columns_override.size() != agg_columns.size()) {
        return ReturnType::failure(common::ErrorCode::InvalidArgument,
                                   "Number of output columns does not match number of aggregate columns");
    }
    auto output_columns = output_columns_override.empty() ? agg_columns : output_columns_override;

    // Get mapping from column names to column indices in the input batch
    std::unordered_map<std::string, int> column_indices;
    for (int i = 0; i < batch->schema()->num_fields(); i++) {
        column_indices[batch->schema()->field(i)->name()] = i;
    }

    // Verify all columns exist
    for (const auto& col_name : group_by_columns) {
        if (column_indices.find(col_name) == column_indices.end()) {
            return ReturnType::failure(common::ErrorCode::InvalidArgument, "Group by column not found: " + col_name);
        }
    }
    for (const auto& col_name : agg_columns) {
        if (column_indices.find(col_name) == column_indices.end()) {
            return ReturnType::failure(common::ErrorCode::InvalidArgument, "Aggregate column not found: " + col_name);
        }
    }

    // Extract group keys and create mapping from group key to row indices
    auto group_keys_result = ExtractGroupKeys(batch, group_by_columns);
    RETURN_IF_ERROR_T(ReturnType, group_keys_result);
    const auto& unique_keys = group_keys_result.value();

    // If batch is empty or no groups found, return an empty batch with the appropriate schema
    if (batch->num_rows() == 0 || unique_keys.empty()) {
        // Create a schema for the result
        std::vector<std::shared_ptr<arrow::Field>> fields;

        // Add group by fields
        for (const auto& col_name : group_by_columns) {
            int col_idx = column_indices[col_name];
            fields.push_back(batch->schema()->field(col_idx));
        }

        // Add aggregate fields
        for (size_t i = 0; i < agg_columns.size(); i++) {
            const auto& col_name = agg_columns[i];
            int col_idx = column_indices[col_name];
            auto field = batch->schema()->field(col_idx);

            // Adjust field type for certain aggregates
            if (agg_types[i] == common::AggregateType::Avg) {
                fields.push_back(arrow::field(field->name(), arrow::float64(), true));
            } else if (agg_types[i] == common::AggregateType::Count) {
                fields.push_back(arrow::field(field->name(), arrow::int64(), false));
            } else {
                fields.push_back(field->WithNullable(true));
            }
        }

        auto schema = arrow::schema(fields);
        std::vector<std::shared_ptr<arrow::Array>> empty_arrays(fields.size());

        for (size_t i = 0; i < fields.size(); i++) {
            auto field_type_result = format::SchemaConverter::FromArrowDataType(fields[i]->type());
            RETURN_IF_ERROR_T(ReturnType, field_type_result);
            auto field_type = field_type_result.value();
            auto builder_result = CreateArrayBuilder(field_type);
            RETURN_IF_ERROR_T(ReturnType, builder_result);

            std::shared_ptr<arrow::Array> array;
            auto arrow_result = builder_result.value()->Finish(&array);
            if (!arrow_result.ok()) {
                return ReturnType::failure(common::ErrorCode::Failure, arrow_result.ToString());
            }
            empty_arrays[i] = array;
        }

        return ReturnType::success(arrow::RecordBatch::Make(schema, 0, empty_arrays));
    }

    // Create mapping from group key to row indices
    std::unordered_map<std::string, std::vector<int>> group_map;

    // Populate the group map based on the group keys
    for (int row_idx = 0; row_idx < batch->num_rows(); row_idx++) {
        GroupKey row_key;

        // Create the key for this row
        for (const auto& col_name : group_by_columns) {
            int col_idx = column_indices[col_name];
            auto array = batch->column(col_idx);
            bool is_null = array->IsNull(row_idx);

            // Add the value to the key based on its type
            switch (array->type_id()) {
                case arrow::Type::INT32: {
                    auto typed_array = std::static_pointer_cast<arrow::Int32Array>(array);
                    row_key.AddValue(is_null ? 0 : typed_array->Value(row_idx), is_null);
                    break;
                }
                case arrow::Type::INT64: {
                    auto typed_array = std::static_pointer_cast<arrow::Int64Array>(array);
                    row_key.AddValue(is_null ? 0 : typed_array->Value(row_idx), is_null);
                    break;
                }
                case arrow::Type::UINT32: {
                    auto typed_array = std::static_pointer_cast<arrow::UInt32Array>(array);
                    row_key.AddValue(is_null ? 0 : typed_array->Value(row_idx), is_null);
                    break;
                }
                case arrow::Type::UINT64: {
                    auto typed_array = std::static_pointer_cast<arrow::UInt64Array>(array);
                    row_key.AddValue(is_null ? 0 : typed_array->Value(row_idx), is_null);
                    break;
                }
                case arrow::Type::FLOAT: {
                    auto typed_array = std::static_pointer_cast<arrow::FloatArray>(array);
                    row_key.AddValue(is_null ? 0.0f : typed_array->Value(row_idx), is_null);
                    break;
                }
                case arrow::Type::DOUBLE: {
                    auto typed_array = std::static_pointer_cast<arrow::DoubleArray>(array);
                    row_key.AddValue(is_null ? 0.0 : typed_array->Value(row_idx), is_null);
                    break;
                }
                case arrow::Type::STRING: {
                    auto typed_array = std::static_pointer_cast<arrow::StringArray>(array);
                    row_key.AddValue(is_null ? "" : typed_array->GetString(row_idx), is_null);
                    break;
                }
                case arrow::Type::BOOL: {
                    auto typed_array = std::static_pointer_cast<arrow::BooleanArray>(array);
                    row_key.AddValue(is_null ? false : typed_array->Value(row_idx), is_null);
                    break;
                }
                default:
                    return ReturnType::failure(common::ErrorCode::NotImplemented,
                                               "Unsupported column type for grouping: " + array->type()->ToString());
            }
        }

        // Add this row to the appropriate group using string representation as map key
        group_map[row_key.ToString()].push_back(row_idx);
    }

    // Create builders for the output columns
    std::vector<std::shared_ptr<arrow::ArrayBuilder>> builders;
    std::vector<std::shared_ptr<arrow::Field>> output_fields;

    // Add group by column builders
    for (const auto& col_name : group_by_columns) {
        int col_idx = column_indices[col_name];
        auto field = batch->schema()->field(col_idx);
        output_fields.push_back(field);

        auto column_type_result = format::SchemaConverter::FromArrowDataType(field->type());
        RETURN_IF_ERROR_T(ReturnType, column_type_result);
        auto column_type = column_type_result.value();
        auto builder_result = CreateArrayBuilder(column_type);
        RETURN_IF_ERROR_T(ReturnType, builder_result);
        builders.push_back(builder_result.value());
    }

    // Add aggregate column builders
    for (size_t i = 0; i < agg_columns.size(); i++) {
        const auto& col_name = agg_columns[i];
        int col_idx = column_indices[col_name];
        auto field = batch->schema()->field(col_idx);
        auto output_col_name = output_columns[i];
        auto agg_type = agg_types[i];

        // Determine output field type based on aggregate type
        std::shared_ptr<arrow::Field> output_field;
        if (agg_type == common::AggregateType::Avg) {
            // Average always produces a double
            output_field = arrow::field(output_col_name, arrow::float64(), true);
        } else if (agg_type == common::AggregateType::Count) {
            // Count always produces an int64
            output_field = arrow::field(output_col_name, arrow::int64(), false);
        } else {
            // Other aggregates preserve the input type but are nullable
            output_field = arrow::field(output_col_name, field->type(), true);
        }

        output_fields.push_back(output_field);

        // Create appropriate builder based on aggregate type
        std::shared_ptr<arrow::ArrayBuilder> builder;
        if (agg_type == common::AggregateType::Avg) {
            builder = std::make_shared<arrow::DoubleBuilder>();
        } else if (agg_type == common::AggregateType::Count) {
            builder = std::make_shared<arrow::Int64Builder>();
        } else {
            auto column_type_result = format::SchemaConverter::FromArrowDataType(field->type());
            RETURN_IF_ERROR_T(ReturnType, column_type_result);
            auto column_type = column_type_result.value();
            auto builder_result = CreateArrayBuilder(column_type);
            RETURN_IF_ERROR_T(ReturnType, builder_result);
            builder = builder_result.value();
        }

        builders.push_back(builder);
    }

    // Process each unique group
    for (const auto& key : unique_keys) {
        const auto& row_indices = group_map[key.ToString()];
        if (row_indices.empty()) {
            continue;  // Skip empty groups
        }

        // Add group by column values (take values from the first row in the group)
        for (size_t i = 0; i < group_by_columns.size(); i++) {
            const auto& col_name = group_by_columns[i];
            int col_idx = column_indices[col_name];
            auto array = batch->column(col_idx);

            int first_row = row_indices[0];
            auto append_result = AppendGroupValue(array, builders[i], first_row);
            RETURN_IF_ERROR_T(ReturnType, append_result);
        }

        // Compute aggregates for each aggregate column
        for (size_t i = 0; i < agg_columns.size(); i++) {
            const auto& col_name = agg_columns[i];
            int col_idx = column_indices[col_name];
            auto array = batch->column(col_idx);
            auto builder_idx = group_by_columns.size() + i;

            switch (agg_types[i]) {
                case common::AggregateType::Sum: {
                    auto result = ComputeSum(array, row_indices, builders[builder_idx]);
                    RETURN_IF_ERROR_T(ReturnType, result);
                    break;
                }
                case common::AggregateType::Avg: {
                    auto result = ComputeAverage(array, row_indices, builders[builder_idx]);
                    RETURN_IF_ERROR_T(ReturnType, result);
                    break;
                }
                case common::AggregateType::Min: {
                    auto result = ComputeMin(array, row_indices, builders[builder_idx]);
                    RETURN_IF_ERROR_T(ReturnType, result);
                    break;
                }
                case common::AggregateType::Max: {
                    auto result = ComputeMax(array, row_indices, builders[builder_idx]);
                    RETURN_IF_ERROR_T(ReturnType, result);
                    break;
                }
                case common::AggregateType::Count: {
                    auto typed_builder = std::static_pointer_cast<arrow::Int64Builder>(builders[builder_idx]);
                    auto result = ComputeCount(array, row_indices, typed_builder);
                    RETURN_IF_ERROR_T(ReturnType, result);
                    break;
                }
                default: {
                    return ReturnType::failure(
                        common::ErrorCode::NotImplemented,
                        "Unsupported aggregate function: " + std::to_string(static_cast<int>(agg_types[i])));
                }
            }
        }
    }

    // Finalize all arrays
    std::vector<std::shared_ptr<arrow::Array>> output_arrays;
    for (auto& builder : builders) {
        std::shared_ptr<arrow::Array> array;
        auto arrow_result = builder->Finish(&array);
        if (!arrow_result.ok()) {
            return ReturnType::failure(common::ErrorCode::Failure, arrow_result.ToString());
        }
        output_arrays.push_back(array);
    }

    // Create and return the output batch
    auto output_schema = arrow::schema(output_fields);
    return ReturnType::success(arrow::RecordBatch::Make(output_schema, unique_keys.size(), output_arrays));
}

std::string ArrowUtil::BatchToString(const ArrowDataBatchSharedPtr& batch, size_t max_rows) {
    if (!batch) {
        return "null batch";
    }

    std::stringstream ss;
    size_t num_rows = max_rows == 0 ? batch->num_rows() : std::min(max_rows, static_cast<size_t>(batch->num_rows()));

    // Print header with batch info
    ss << "RecordBatch with " << batch->num_columns() << " columns, " << batch->num_rows() << " rows";
    if (max_rows > 0 && max_rows < batch->num_rows()) {
        ss << " (showing first " << max_rows << ")";
    }
    ss << ":\n";

    if (batch->num_columns() == 0) {
        ss << "<empty schema>";
        return ss.str();
    }

    // Calculate column widths and prepare headers
    std::vector<size_t> widths;
    std::vector<std::string> headers;
    for (int i = 0; i < batch->num_columns(); i++) {
        auto field = batch->schema()->field(i);
        std::string header = field->name() + " (" + field->type()->ToString() + ")";
        headers.push_back(header);
        widths.push_back(GetColumnWidth(batch->column(i), header, num_rows));
    }

    // Print column headers
    for (int i = 0; i < batch->num_columns(); i++) {
        ss << std::left << std::setw(widths[i]) << headers[i];
        if (i < batch->num_columns() - 1) {
            ss << " | ";
        }
    }
    ss << "\n";

    // Print separator line
    for (int i = 0; i < batch->num_columns(); i++) {
        ss << std::string(widths[i], '-');
        if (i < batch->num_columns() - 1) {
            ss << "-|-";
        }
    }
    ss << "\n";

    // Print rows
    for (size_t row = 0; row < num_rows; row++) {
        for (int col = 0; col < batch->num_columns(); col++) {
            std::string value = FormatCellValue(batch->column(col), row);
            ss << std::left << std::setw(widths[col]) << value;
            if (col < batch->num_columns() - 1) {
                ss << " | ";
            }
        }
        ss << "\n";
    }

    return ss.str();
}

std::string ArrowUtil::FormatCellValue(const std::shared_ptr<arrow::Array>& array, int row_idx) {
    if (array->IsNull(row_idx)) {
        return "null";
    }

    switch (array->type_id()) {
        case arrow::Type::INT32:
            return std::to_string(std::static_pointer_cast<arrow::Int32Array>(array)->Value(row_idx));
        case arrow::Type::INT64:
            return std::to_string(std::static_pointer_cast<arrow::Int64Array>(array)->Value(row_idx));
        case arrow::Type::UINT32:
            return std::to_string(std::static_pointer_cast<arrow::UInt32Array>(array)->Value(row_idx));
        case arrow::Type::UINT64:
            return std::to_string(std::static_pointer_cast<arrow::UInt64Array>(array)->Value(row_idx));
        case arrow::Type::FLOAT:
            return std::to_string(std::static_pointer_cast<arrow::FloatArray>(array)->Value(row_idx));
        case arrow::Type::DOUBLE: {
            std::stringstream ss;
            ss << std::fixed << std::setprecision(6)
               << std::static_pointer_cast<arrow::DoubleArray>(array)->Value(row_idx);
            return ss.str();
        }
        case arrow::Type::STRING:
            return "\"" + std::static_pointer_cast<arrow::StringArray>(array)->GetString(row_idx) + "\"";
        case arrow::Type::BOOL:
            return std::static_pointer_cast<arrow::BooleanArray>(array)->Value(row_idx) ? "true" : "false";
        case arrow::Type::TIMESTAMP: {
            auto ts = std::static_pointer_cast<arrow::TimestampArray>(array)->Value(row_idx);
            return std::to_string(ts);  // Basic timestamp formatting - could be enhanced
        }
        default:
            return "<unsupported type>";
    }
}

size_t ArrowUtil::GetColumnWidth(const std::shared_ptr<arrow::Array>& array,
                                 const std::string& header,
                                 size_t max_rows) {
    size_t width = header.length();
    size_t num_rows = max_rows == 0 ? array->length() : std::min(max_rows, static_cast<size_t>(array->length()));

    // Check width needed for each value
    for (size_t i = 0; i < num_rows; i++) {
        std::string value = FormatCellValue(array, i);
        width = std::max(width, value.length());
    }

    return width;
}

common::Result<ArrowDataBatchSharedPtr> ArrowUtil::SortBatch(const ArrowDataBatchSharedPtr& batch,
                                                             const std::vector<std::string>& sort_columns,
                                                             const std::vector<bool>& sort_directions,
                                                             bool nulls_first) {
    using ReturnType = common::Result<ArrowDataBatchSharedPtr>;

    // Validate inputs
    if (!batch) {
        return ReturnType::failure(common::ErrorCode::InvalidArgument, "Input batch is null");
    }

    if (sort_columns.empty()) {
        return ReturnType::success(batch);
    }

    // If batch is empty, return it directly
    if (batch->num_rows() == 0) {
        return ReturnType::success(batch);
    }

    // Create normalized sort directions vector
    std::vector<bool> directions = sort_directions;
    if (directions.size() < sort_columns.size()) {
        // Fill missing directions with true (ascending)
        directions.resize(sort_columns.size(), true);
    }

    // Create a vector of indices for the rows
    std::vector<int64_t> indices(batch->num_rows());
    for (int64_t i = 0; i < batch->num_rows(); i++) {
        indices[i] = i;
    }

    // Sort the indices based on the sort columns and directions
    std::sort(indices.begin(), indices.end(), [&](int64_t a, int64_t b) {
        for (size_t i = 0; i < sort_columns.size(); i++) {
            const std::string& col_name = sort_columns[i];
            bool ascending = directions[i];

            // Get the column index
            int col_idx = batch->schema()->GetFieldIndex(col_name);
            if (col_idx < 0) {
                // Column not found, skip it
                continue;
            }

            auto array = batch->column(col_idx);

            // Handle nulls
            bool a_is_null = array->IsNull(a);
            bool b_is_null = array->IsNull(b);

            if (a_is_null && !b_is_null) {
                return nulls_first ? true : false;
            } else if (!a_is_null && b_is_null) {
                return nulls_first ? false : true;
            } else if (a_is_null && b_is_null) {
                continue;  // Both null, move to next column
            }

            // Compare non-null values based on type
            int comparison = 0;
            switch (array->type_id()) {
                case arrow::Type::INT32: {
                    auto typed_array = std::static_pointer_cast<arrow::Int32Array>(array);
                    int32_t val_a = typed_array->Value(a);
                    int32_t val_b = typed_array->Value(b);
                    comparison = (val_a > val_b) ? 1 : ((val_a < val_b) ? -1 : 0);
                    break;
                }
                case arrow::Type::INT64: {
                    auto typed_array = std::static_pointer_cast<arrow::Int64Array>(array);
                    int64_t val_a = typed_array->Value(a);
                    int64_t val_b = typed_array->Value(b);
                    comparison = (val_a > val_b) ? 1 : ((val_a < val_b) ? -1 : 0);
                    break;
                }
                case arrow::Type::UINT32: {
                    auto typed_array = std::static_pointer_cast<arrow::UInt32Array>(array);
                    uint32_t val_a = typed_array->Value(a);
                    uint32_t val_b = typed_array->Value(b);
                    comparison = (val_a > val_b) ? 1 : ((val_a < val_b) ? -1 : 0);
                    break;
                }
                case arrow::Type::UINT64: {
                    auto typed_array = std::static_pointer_cast<arrow::UInt64Array>(array);
                    uint64_t val_a = typed_array->Value(a);
                    uint64_t val_b = typed_array->Value(b);
                    comparison = (val_a > val_b) ? 1 : ((val_a < val_b) ? -1 : 0);
                    break;
                }
                case arrow::Type::FLOAT: {
                    auto typed_array = std::static_pointer_cast<arrow::FloatArray>(array);
                    float val_a = typed_array->Value(a);
                    float val_b = typed_array->Value(b);
                    comparison = (val_a > val_b) ? 1 : ((val_a < val_b) ? -1 : 0);
                    break;
                }
                case arrow::Type::DOUBLE: {
                    auto typed_array = std::static_pointer_cast<arrow::DoubleArray>(array);
                    double val_a = typed_array->Value(a);
                    double val_b = typed_array->Value(b);
                    comparison = (val_a > val_b) ? 1 : ((val_a < val_b) ? -1 : 0);
                    break;
                }
                case arrow::Type::STRING: {
                    auto typed_array = std::static_pointer_cast<arrow::StringArray>(array);
                    comparison = typed_array->GetString(a).compare(typed_array->GetString(b));
                    break;
                }
                case arrow::Type::BOOL: {
                    auto typed_array = std::static_pointer_cast<arrow::BooleanArray>(array);
                    bool val_a = typed_array->Value(a);
                    bool val_b = typed_array->Value(b);
                    comparison = val_a - val_b;
                    break;
                }
                case arrow::Type::TIMESTAMP: {
                    auto typed_array = std::static_pointer_cast<arrow::TimestampArray>(array);
                    int64_t val_a = typed_array->Value(a);
                    int64_t val_b = typed_array->Value(b);
                    comparison = (val_a > val_b) ? 1 : ((val_a < val_b) ? -1 : 0);
                    break;
                }
                default:
                    // Unsupported type, skip this column
                    continue;
            }

            if (comparison != 0) {
                return ascending ? (comparison < 0) : (comparison > 0);
            }
        }

        // If all columns are equal, maintain original order by using indices
        return a < b;
    });

    // Create a new batch with the sorted rows
    std::vector<std::shared_ptr<arrow::Array>> sorted_arrays;
    for (int col_idx = 0; col_idx < batch->num_columns(); col_idx++) {
        auto input_array = batch->column(col_idx);
        auto array_type = input_array->type();

        auto column_type_result = format::SchemaConverter::FromArrowDataType(array_type);
        if (!column_type_result.ok()) {
            return ReturnType::failure(column_type_result.error());
        }

        // Create a builder for this column
        auto builder_result = ArrowUtil::CreateArrayBuilder(column_type_result.value());
        if (!builder_result.ok()) {
            return ReturnType::failure(builder_result.error());
        }
        auto builder = builder_result.value();

        // Append values in sorted order
        for (int64_t idx : indices) {
            auto result = ArrowUtil::AppendGroupValue(input_array, builder, idx);
            if (!result.ok()) {
                return ReturnType::failure(result.error());
            }
        }

        // Finish the array
        std::shared_ptr<arrow::Array> sorted_array;
        auto status = builder->Finish(&sorted_array);
        if (!status.ok()) {
            return ReturnType::failure(
                common::Error(common::ErrorCode::Failure, "Failed to finish array: " + status.ToString()));
        }

        sorted_arrays.push_back(sorted_array);
    }

    // Create the sorted batch
    auto sorted_batch = arrow::RecordBatch::Make(batch->schema(), batch->num_rows(), sorted_arrays);
    return ReturnType::success(sorted_batch);
}

}  // namespace pond::query