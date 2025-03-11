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
}  // namespace pond::query