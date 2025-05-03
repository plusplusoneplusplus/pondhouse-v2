#include "query/data/arrow_predicate.h"

#include <arrow/api.h>
#include <arrow/compute/api.h>

#include "common/error.h"
#include "common/log.h"

namespace pond::query {

common::Result<ArrowDataBatchSharedPtr> ArrowPredicate::Apply(const ArrowDataBatchSharedPtr& batch,
                                                              const std::shared_ptr<common::Expression>& predicate) {
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

}  // namespace pond::query