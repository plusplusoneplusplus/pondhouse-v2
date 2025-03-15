#include "query/executor/executor_util.h"

#include "format/parquet/schema_converter.h"

namespace pond::query {

common::Result<ArrowDataBatchSharedPtr> ExecutorUtil::CreateProjectionBatch(PhysicalProjectionNode& node,
                                                                            ArrowDataBatchSharedPtr input_batch) {
    const auto& projections = node.Projections();
    // Create the output schema based on projection expressions
    std::vector<std::shared_ptr<arrow::Field>> output_fields;
    for (const auto& expr : projections) {
        // For column references, use the original field name
        if (expr->Type() == common::ExprType::Column) {
            auto col_expr = std::static_pointer_cast<common::ColumnExpression>(expr);
            const std::string& col_name = col_expr->ColumnName();

            // Find the field in the input schema
            int col_idx = -1;
            auto input_schema = input_batch->schema();
            for (int i = 0; i < input_schema->num_fields(); ++i) {
                if (input_schema->field(i)->name() == col_name) {
                    col_idx = i;
                    break;
                }
            }

            if (col_idx == -1) {
                return common::Result<ArrowDataBatchSharedPtr>::failure(
                    common::Error(common::ErrorCode::InvalidArgument, "Column not found: " + col_name));
            }

            // Add the field to output schema
            output_fields.push_back(input_schema->field(col_idx));
        } else if (expr->Type() == common::ExprType::Aggregate) {
            auto agg_expr = std::static_pointer_cast<common::AggregateExpression>(expr);
            auto agg_type = agg_expr->AggType();
            auto input_expr = agg_expr->Input();

            // For now, only support column references in aggregate input
            if (input_expr->Type() != common::ExprType::Column && input_expr->Type() != common::ExprType::Star) {
                return common::Result<ArrowDataBatchSharedPtr>::failure(
                    common::Error(common::ErrorCode::NotImplemented,
                                  std::format("Only column references or star are supported in aggregate input: {}",
                                              input_expr->ToString())));
            }

            auto result_name = agg_expr->ResultName();
            auto result_type = agg_expr->ResultType();

            output_fields.push_back(format::SchemaConverter::ToArrowField(
                common::ColumnSchema(result_name, result_type, common::Nullability::NOT_NULL)));
        } else {
            // For now, only support column references and star
            return common::Result<ArrowDataBatchSharedPtr>::failure(common::Error(
                common::ErrorCode::NotImplemented, "Only column references and star are supported in projections"));
        }
    }

    // Create output schema
    auto output_schema = arrow::schema(output_fields);

    // Create arrays for the output batch using output_schema
    std::vector<std::shared_ptr<arrow::Array>> output_arrays;
    auto input_schema = input_batch->schema();

    // For each field in the output schema, find the corresponding column in the input batch
    for (int i = 0; i < output_schema->num_fields(); ++i) {
        const std::string& output_col_name = output_schema->field(i)->name();

        // Find the column in the input batch
        int col_idx = -1;
        for (int j = 0; j < input_schema->num_fields(); ++j) {
            if (input_schema->field(j)->name() == output_col_name) {
                col_idx = j;
                break;
            }
        }

        // If column not found, return error
        if (col_idx == -1) {
            return common::Result<ArrowDataBatchSharedPtr>::failure(
                common::Error(common::ErrorCode::InvalidArgument, "Column not found: " + output_col_name));
        }

        // Add the column to the output arrays
        output_arrays.push_back(input_batch->column(col_idx));
    }

    // Create the output batch
    return common::Result<ArrowDataBatchSharedPtr>::success(
        arrow::RecordBatch::Make(output_schema, input_batch->num_rows(), output_arrays));
}

common::Result<ArrowDataBatchSharedPtr> ExecutorUtil::CreateSortBatch(ArrowDataBatchSharedPtr input_batch,
                                                                      const std::vector<SortSpec>& sort_specs) {
    if (input_batch->num_rows() == 0) {
        return common::Result<ArrowDataBatchSharedPtr>::success(input_batch);
    }

    if (sort_specs.empty()) {
        return common::Result<ArrowDataBatchSharedPtr>::success(input_batch);
    }

    // Extract sort columns and directions from SortSpecs
    std::vector<std::string> sort_columns;
    std::vector<SortDirection> sort_directions;

    for (const auto& spec : sort_specs) {
        if (spec.expr->Type() != common::ExprType::Column) {
            return common::Result<ArrowDataBatchSharedPtr>::failure(
                common::Error(common::ErrorCode::NotImplemented, "Only column references are supported in ORDER BY"));
        }
        auto col_expr = std::static_pointer_cast<common::ColumnExpression>(spec.expr);
        sort_columns.push_back(col_expr->ColumnName());
        sort_directions.push_back(spec.direction);
    }

    // Create a vector of indices for the rows
    std::vector<int64_t> indices(input_batch->num_rows());
    for (int64_t i = 0; i < input_batch->num_rows(); i++) {
        indices[i] = i;
    }

    // Sort the indices based on the sort columns and directions
    std::sort(indices.begin(), indices.end(), [&](int64_t a, int64_t b) {
        for (size_t i = 0; i < sort_columns.size(); i++) {
            const std::string& col_name = sort_columns[i];
            bool ascending = sort_directions[i] == SortDirection::Ascending;

            // Get the column index
            int col_idx = input_batch->schema()->GetFieldIndex(col_name);
            if (col_idx < 0) {
                // Column not found, skip it
                continue;
            }

            auto array = input_batch->column(col_idx);

            // Handle nulls - nulls are considered greater than non-nulls
            bool a_is_null = array->IsNull(a);
            bool b_is_null = array->IsNull(b);

            if (a_is_null && !b_is_null) {
                return ascending;  // If ascending, nulls first (true), else nulls last (false)
            } else if (!a_is_null && b_is_null) {
                return !ascending;  // If ascending, non-nulls last (false), else non-nulls first (true)
            } else if (a_is_null && b_is_null) {
                continue;  // Both null, move to next column
            }

            // Compare non-null values based on type
            int comparison = 0;
            switch (array->type_id()) {
                case arrow::Type::INT32: {
                    auto typed_array = std::static_pointer_cast<arrow::Int32Array>(array);
                    comparison = typed_array->Value(a) - typed_array->Value(b);
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
                    comparison = typed_array->Value(a) - typed_array->Value(b);
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
                    comparison = typed_array->Value(a) - typed_array->Value(b);
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

        // If all columns are equal, maintain original order
        return a < b;
    });

    // Create a new batch with the sorted rows
    std::vector<std::shared_ptr<arrow::Array>> sorted_arrays;
    for (int col_idx = 0; col_idx < input_batch->num_columns(); col_idx++) {
        auto input_array = input_batch->column(col_idx);
        auto array_type = input_array->type();

        auto column_type_result = format::SchemaConverter::FromArrowDataType(array_type);
        if (!column_type_result.ok()) {
            return common::Result<ArrowDataBatchSharedPtr>::failure(column_type_result.error());
        }

        // Create a builder for this column
        auto builder_result = ArrowUtil::CreateArrayBuilder(column_type_result.value());
        if (!builder_result.ok()) {
            return common::Result<ArrowDataBatchSharedPtr>::failure(builder_result.error());
        }
        auto builder = builder_result.value();

        // Append values in sorted order
        for (int64_t idx : indices) {
            auto result = ArrowUtil::AppendGroupValue(input_array, builder, idx);
            if (!result.ok()) {
                return common::Result<ArrowDataBatchSharedPtr>::failure(result.error());
            }
        }

        // Finish the array
        std::shared_ptr<arrow::Array> sorted_array;
        auto status = builder->Finish(&sorted_array);
        if (!status.ok()) {
            return common::Result<ArrowDataBatchSharedPtr>::failure(
                common::Error(common::ErrorCode::Failure, "Failed to finish array: " + status.ToString()));
        }

        sorted_arrays.push_back(sorted_array);
    }

    // Create the sorted batch
    auto sorted_batch = arrow::RecordBatch::Make(input_batch->schema(), input_batch->num_rows(), sorted_arrays);

    return common::Result<ArrowDataBatchSharedPtr>::success(sorted_batch);
}

}  // namespace pond::query