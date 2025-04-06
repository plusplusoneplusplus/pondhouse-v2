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
    std::vector<bool> sort_directions;

    for (const auto& spec : sort_specs) {
        if (spec.expr->Type() != common::ExprType::Column) {
            return common::Result<ArrowDataBatchSharedPtr>::failure(
                common::Error(common::ErrorCode::NotImplemented, "Only column references are supported in ORDER BY"));
        }
        auto col_expr = std::static_pointer_cast<common::ColumnExpression>(spec.expr);
        sort_columns.push_back(col_expr->ColumnName());
        // Convert SortDirection to bool (true for ascending, false for descending)
        sort_directions.push_back(spec.direction == SortDirection::Ascending);
    }

    // Delegate to ArrowUtil::SortBatch with nulls first for ascending sort, nulls last for descending
    return ArrowUtil::SortBatch(input_batch, sort_columns, sort_directions, true);
}

common::Result<ArrowDataBatchSharedPtr> ExecutorUtil::CreateLimitBatch(ArrowDataBatchSharedPtr input_batch,
                                                                       size_t limit,
                                                                       size_t offset) {
    if (!input_batch) {
        return common::Result<ArrowDataBatchSharedPtr>::failure(
            common::Error(common::ErrorCode::InvalidArgument, "Input batch is null"));
    }

    // If offset is beyond the input size, return empty batch
    if (offset >= input_batch->num_rows()) {
        std::vector<std::shared_ptr<arrow::Array>> empty_arrays;
        for (int i = 0; i < input_batch->num_columns(); i++) {
            auto array_type = input_batch->column(i)->type();
            auto column_type_result = format::SchemaConverter::FromArrowDataType(array_type);
            if (!column_type_result.ok()) {
                return common::Result<ArrowDataBatchSharedPtr>::failure(column_type_result.error());
            }
            auto builder_result = ArrowUtil::CreateArrayBuilder(column_type_result.value());
            if (!builder_result.ok()) {
                return common::Result<ArrowDataBatchSharedPtr>::failure(builder_result.error());
            }
            std::shared_ptr<arrow::Array> empty_array;
            if (!builder_result.value()->Finish(&empty_array).ok()) {
                return common::Result<ArrowDataBatchSharedPtr>::failure(
                    common::Error(common::ErrorCode::Failure, "Failed to finish array"));
            }
            empty_arrays.push_back(empty_array);
        }
        return common::Result<ArrowDataBatchSharedPtr>::success(
            arrow::RecordBatch::Make(input_batch->schema(), 0, empty_arrays));
    }

    // Calculate actual number of rows to include
    size_t available_rows = input_batch->num_rows() - offset;
    size_t actual_limit = std::min(limit, available_rows);

    // Create arrays for the output batch
    std::vector<std::shared_ptr<arrow::Array>> output_arrays;
    for (int i = 0; i < input_batch->num_columns(); i++) {
        auto input_array = input_batch->column(i);
        auto array_type = input_array->type();

        // Get column type
        auto column_type_result = format::SchemaConverter::FromArrowDataType(array_type);
        if (!column_type_result.ok()) {
            return common::Result<ArrowDataBatchSharedPtr>::failure(column_type_result.error());
        }

        // Create builder
        auto builder_result = ArrowUtil::CreateArrayBuilder(column_type_result.value());
        if (!builder_result.ok()) {
            return common::Result<ArrowDataBatchSharedPtr>::failure(builder_result.error());
        }
        auto builder = builder_result.value();

        // Append values from offset to offset + actual_limit
        for (size_t j = offset; j < offset + actual_limit; j++) {
            auto result = ArrowUtil::AppendGroupValue(input_array, builder, j);
            if (!result.ok()) {
                return common::Result<ArrowDataBatchSharedPtr>::failure(result.error());
            }
        }

        // Finish array
        std::shared_ptr<arrow::Array> output_array;
        if (!builder->Finish(&output_array).ok()) {
            return common::Result<ArrowDataBatchSharedPtr>::failure(
                common::Error(common::ErrorCode::Failure, "Failed to finish array"));
        }
        output_arrays.push_back(output_array);
    }

    // Create output batch
    return common::Result<ArrowDataBatchSharedPtr>::success(
        arrow::RecordBatch::Make(input_batch->schema(), actual_limit, output_arrays));
}

common::Result<ArrowDataBatchSharedPtr> ExecutorUtil::CreateHashJoinBatch(ArrowDataBatchSharedPtr left_batch,
                                                                          ArrowDataBatchSharedPtr right_batch,
                                                                          const common::Expression& condition,
                                                                          common::JoinType join_type) {
    using ReturnType = common::Result<ArrowDataBatchSharedPtr>;

    auto context = std::make_shared<HashJoinContext>(left_batch, right_batch, condition, join_type);
    RETURN_IF_ERROR_T(ReturnType, context->Build());

    auto probe_result = context->Probe();
    RETURN_IF_ERROR_T(ReturnType, probe_result);

    return ReturnType::success(probe_result.value());
}

}  // namespace pond::query