#include "query/data/arrow_util.h"

#include <arrow/api.h>
#include <arrow/builder.h>
#include <arrow/compute/api.h>
#include <arrow/type_traits.h>

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

// Function pointer type for array creation
using CreateArrayFunc = arrow::Result<std::shared_ptr<arrow::Array>> (*)();

// Map of ColumnType to creation functions
const std::unordered_map<common::ColumnType, CreateArrayFunc> kCreateArrayFuncs = {
    {common::ColumnType::INT32, &CreateEmptyArrayImpl<common::ColumnType::INT32>},
    {common::ColumnType::INT64, &CreateEmptyArrayImpl<common::ColumnType::INT64>},
    {common::ColumnType::FLOAT, &CreateEmptyArrayImpl<common::ColumnType::FLOAT>},
    {common::ColumnType::DOUBLE, &CreateEmptyArrayImpl<common::ColumnType::DOUBLE>},
    {common::ColumnType::BOOLEAN, &CreateEmptyArrayImpl<common::ColumnType::BOOLEAN>},
    {common::ColumnType::STRING, &CreateEmptyArrayImpl<common::ColumnType::STRING>},
    {common::ColumnType::TIMESTAMP, &CreateEmptyArrayImpl<common::ColumnType::TIMESTAMP>},
    {common::ColumnType::BINARY, &CreateEmptyArrayImpl<common::ColumnType::BINARY>},
};

}  // namespace detail

arrow::Result<std::shared_ptr<arrow::Array>> ArrowUtil::CreateEmptyArray(common::ColumnType type) {
    auto it = detail::kCreateArrayFuncs.find(type);
    if (it == detail::kCreateArrayFuncs.end()) {
        return arrow::Status::Invalid("Unsupported column type");
    }
    return it->second();
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

}  // namespace pond::query