#include "query/executor/materialized_executor.h"

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/record_batch.h>
#include <arrow/table.h>
#include <arrow/type_traits.h>

#include "catalog/metadata.h"
#include "common/error.h"
#include "common/log.h"
#include "format/parquet/parquet_reader.h"
#include "format/parquet/schema_converter.h"
#include "query/data/arrow_util.h"
#include "query/planner/physical_plan_node.h"

namespace pond::query {

MaterializedExecutor::MaterializedExecutor(std::shared_ptr<catalog::Catalog> catalog,
                                           std::shared_ptr<DataAccessor> data_accessor)
    : catalog_(std::move(catalog)),
      data_accessor_(std::move(data_accessor)),
      current_result_(common::Result<ArrowDataBatchSharedPtr>::success(nullptr)) {}

common::Result<std::unique_ptr<BatchIterator>> MaterializedExecutor::Execute(std::shared_ptr<PhysicalPlanNode> plan) {
    using ReturnType = common::Result<std::unique_ptr<BatchIterator>>;

    // Reset the current batch and result
    current_batch_ = nullptr;
    current_result_ = common::Result<ArrowDataBatchSharedPtr>::success(nullptr);

    // If the plan is null, return an error
    if (!plan) {
        return common::Error(common::ErrorCode::InvalidArgument, "Physical plan is null");
    }

    // Accept the plan visitor to execute the plan
    plan->Accept(*this);

    RETURN_IF_ERROR_T(ReturnType, current_result_);

    // Create a MaterializedBatchIterator with the result
    auto iterator = std::make_unique<MaterializedBatchIterator>(current_batch_);
    return common::Result<std::unique_ptr<BatchIterator>>::success(std::move(iterator));
}

common::Result<ArrowDataBatchSharedPtr> MaterializedExecutor::CurrentBatch() const {
    return current_result_;
}

common::Result<ArrowDataBatchSharedPtr> MaterializedExecutor::ExecuteChildren(PhysicalPlanNode& node) {
    // If the node has children, execute them first
    if (!node.Children().empty()) {
        for (const auto& child : node.Children()) {
            child->Accept(*this);
            if (!current_result_.ok()) {
                return current_result_;
            }
        }
    }

    return current_result_;
}

common::Result<bool> MaterializedExecutor::ProduceResults(const common::Schema& schema) {
    // Not needed for this simple implementation
    return common::Result<bool>::success(true);
}

void MaterializedExecutor::Visit(PhysicalSequentialScanNode& node) {
    // Get the table name from the node
    const std::string& table_name = node.TableName();

    // List table files from catalog
    auto files_result = data_accessor_->ListTableFiles(table_name);
    if (!files_result.ok()) {
        current_result_ = common::Result<ArrowDataBatchSharedPtr>::failure(files_result.error());
        return;
    }

    // If the table has no files, return an empty result
    if (files_result.value().empty()) {
        // Create empty batch with the schema using ArrowUtil
        auto empty_batch_result = ArrowUtil::CreateEmptyBatch(node.OutputSchema());
        if (!empty_batch_result.ok()) {
            current_result_ = common::Error(common::ErrorCode::Failure,
                                            "Failed to create empty batch: " + empty_batch_result.error().message());
            return;
        }

        current_batch_ = empty_batch_result.value();
        current_result_ = common::Result<ArrowDataBatchSharedPtr>::success(current_batch_);
        return;
    }

    // Process all files in the table
    const auto& files = files_result.value();

    // Create vectors to collect all arrays and row counts
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    int64_t total_rows = 0;

    // Process each file
    for (const auto& file : files) {
        // Create a reader for the file
        auto reader_result = data_accessor_->GetReader(file);
        if (!reader_result.ok()) {
            current_result_ = common::Result<ArrowDataBatchSharedPtr>::failure(reader_result.error());
            return;
        }

        // Get the reader
        auto reader = std::move(reader_result).value();

        // get projection column names
        auto projection_column_names = node.OutputSchema().GetColumnNames();

        // Use the batch reader interface
        auto batch_reader_result = reader->GetBatchReader(projection_column_names);
        if (!batch_reader_result.ok()) {
            current_result_ = common::Result<ArrowDataBatchSharedPtr>::failure(batch_reader_result.error());
            return;
        }

        auto batch_reader = std::move(batch_reader_result).value();

        // Read all batches from this file
        std::shared_ptr<arrow::RecordBatch> batch;
        auto status = batch_reader->ReadNext(&batch);

        while (status.ok() && batch != nullptr) {
            // Skip empty batches
            if (batch->num_rows() > 0) {
                // Apply predicate if present (predicate pushdown)
                if (node.Predicate()) {
                    auto filter_result = ArrowUtil::ApplyPredicate(batch, node.Predicate());
                    if (!filter_result.ok()) {
                        current_result_ = common::Result<ArrowDataBatchSharedPtr>::failure(filter_result.error());
                        return;
                    }
                    batch = filter_result.value();
                }

                // If the batch still has rows after filtering, add it to our collection
                if (batch->num_rows() > 0) {
                    batches.push_back(batch);
                    total_rows += batch->num_rows();
                }
            }

            // Read the next batch
            status = batch_reader->ReadNext(&batch);
        }

        if (!status.ok()) {
            current_result_ = common::Error(common::ErrorCode::Failure, "Failed to read batch: " + status.ToString());
            return;
        }
    }

    // If we didn't collect any data, return an empty batch
    if (batches.empty() || total_rows == 0) {
        auto empty_batch_result = ArrowUtil::CreateEmptyBatch(node.OutputSchema());
        if (!empty_batch_result.ok()) {
            current_result_ = common::Error(common::ErrorCode::Failure,
                                            "Failed to create empty batch: " + empty_batch_result.error().message());
            return;
        }

        current_batch_ = empty_batch_result.value();
    } else {
        // Use ArrowUtil to concatenate all the batches
        auto concat_result = ArrowUtil::ConcatenateBatches(batches);
        if (!concat_result.ok()) {
            current_result_ = common::Result<ArrowDataBatchSharedPtr>::failure(concat_result.error());
            return;
        }

        current_batch_ = concat_result.value();
        LOG_VERBOSE("Concatenated %d batches with total of %d rows", batches.size(), current_batch_->num_rows());
    }

    current_result_ = common::Result<ArrowDataBatchSharedPtr>::success(current_batch_);
    return;
}

void MaterializedExecutor::Visit(PhysicalFilterNode& node) {
    // Execute the child node first
    auto result = ExecuteChildren(node);
    if (!result.ok()) {
        current_result_ = result;
        return;
    }

    // Check if we have data to filter
    if (!current_batch_) {
        current_result_ = common::Error(common::ErrorCode::Failure, "No data to filter");
        return;
    }

    // Apply the filter predicate using ArrowUtil
    auto filter_result = ArrowUtil::ApplyPredicate(current_batch_, node.Predicate());
    if (!filter_result.ok()) {
        current_result_ = common::Result<ArrowDataBatchSharedPtr>::failure(filter_result.error());
        return;
    }

    // Update the current batch with the filtered result
    current_batch_ = filter_result.value();
    current_result_ = common::Result<ArrowDataBatchSharedPtr>::success(current_batch_);
}

// Minimal implementation for other operators, as we're only supporting simple SELECT *
void MaterializedExecutor::Visit(PhysicalIndexScanNode& node) {
    current_result_ = common::Error(common::ErrorCode::NotImplemented, "Index scan not implemented in simple executor");
}

common::Result<ArrowDataBatchSharedPtr> MaterializedExecutor::CreateProjectionBatch(
    PhysicalProjectionNode& node, ArrowDataBatchSharedPtr input_batch) {
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
            if (input_expr->Type() != common::ExprType::Column) {
                return common::Result<ArrowDataBatchSharedPtr>::failure(common::Error(
                    common::ErrorCode::NotImplemented, "Only column references are supported in aggregate input"));
            }

            auto result_name = agg_expr->ResultName();
            auto result_type = agg_expr->ResultType();

            output_fields.push_back(format::SchemaConverter::ToArrowField(
                common::ColumnSchema(result_name, result_type, common::Nullability::NOT_NULL)));
        } else {
            // For now, only support column references
            return common::Result<ArrowDataBatchSharedPtr>::failure(common::Error(
                common::ErrorCode::NotImplemented, "Only column references are supported in projections"));
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

void MaterializedExecutor::Visit(PhysicalProjectionNode& node) {
    // Execute the child node first
    auto child_result = ExecuteChildren(node);
    if (!child_result.ok()) {
        current_result_ = std::move(child_result);
        return;
    }

    // Get input batch from child
    auto input_batch = child_result.value();
    if (!input_batch) {
        current_result_ = common::Result<ArrowDataBatchSharedPtr>::failure(
            common::Error(common::ErrorCode::InvalidArgument, "Input batch is null"));
        return;
    }

    // Get the projection expressions
    const auto& projections = node.Projections();
    if (projections.empty()) {
        // No projections, just return the input batch
        current_batch_ = input_batch;
        current_result_ = common::Result<ArrowDataBatchSharedPtr>::success(current_batch_);
        return;
    }

    auto projection_result = CreateProjectionBatch(node, input_batch);
    if (!projection_result.ok()) {
        current_result_ = projection_result;
        return;
    }

    current_batch_ = projection_result.value();
    current_result_ = common::Result<ArrowDataBatchSharedPtr>::success(current_batch_);
}

void MaterializedExecutor::Visit(PhysicalHashJoinNode& node) {
    current_result_ = common::Error(common::ErrorCode::NotImplemented, "Hash join not implemented in simple executor");
}

void MaterializedExecutor::Visit(PhysicalNestedLoopJoinNode& node) {
    current_result_ =
        common::Error(common::ErrorCode::NotImplemented, "Nested loop join not implemented in simple executor");
}

void MaterializedExecutor::Visit(PhysicalHashAggregateNode& node) {
    // Execute the child node first to get the input data
    auto result = ExecuteChildren(node);
    if (!result.ok()) {
        current_result_ = result;
        return;
    }

    // Get input batch from child
    auto input_batch = current_batch_;
    if (!input_batch || input_batch->num_rows() == 0) {
        // Create empty batch with the output schema
        auto empty_batch_result = ArrowUtil::CreateEmptyBatch(node.OutputSchema());
        if (!empty_batch_result.ok()) {
            current_result_ = common::Result<ArrowDataBatchSharedPtr>::failure(empty_batch_result.error());
            return;
        }
        current_batch_ = empty_batch_result.value();
        current_result_ = common::Result<ArrowDataBatchSharedPtr>::success(current_batch_);
        return;
    }

    // Get the group by columns and aggregate expressions
    const auto& group_by = node.GroupBy();
    const auto& aggregates = node.Aggregates();

    // If there are no group by columns and no aggregates, just return the input
    if (group_by.empty() && aggregates.empty()) {
        current_result_ = common::Result<ArrowDataBatchSharedPtr>::success(input_batch);
        return;
    }

    // Create a mapping from group keys to row indices
    std::unordered_map<std::string, std::vector<int>> group_map;

    // First pass: Group the rows by the group by columns
    for (int row_idx = 0; row_idx < input_batch->num_rows(); ++row_idx) {
        std::string group_key;

        // Concatenate the group by column values to form a key
        for (const auto& group_expr : group_by) {
            // For now, only support column references in group by
            if (group_expr->Type() != common::ExprType::Column) {
                current_result_ = common::Result<ArrowDataBatchSharedPtr>::failure(common::Error(
                    common::ErrorCode::NotImplemented, "Only column references are supported in GROUP BY"));
                return;
            }

            auto col_expr = std::static_pointer_cast<common::ColumnExpression>(group_expr);
            const std::string& col_name = col_expr->ColumnName();

            // Find column index in the input batch
            int col_idx = -1;
            auto input_schema = input_batch->schema();
            for (int i = 0; i < input_schema->num_fields(); ++i) {
                if (input_schema->field(i)->name() == col_name) {
                    col_idx = i;
                    break;
                }
            }

            if (col_idx == -1) {
                current_result_ = common::Result<ArrowDataBatchSharedPtr>::failure(
                    common::Error(common::ErrorCode::InvalidArgument, "Column not found in input: " + col_name));
                return;
            }

            // Get the column array and value at this row
            auto array = input_batch->column(col_idx);

            // Append the value to the group key based on type
            if (group_key.length() > 0) {
                group_key += "|";  // Separator between values
            }

            switch (array->type_id()) {
                case arrow::Type::INT32: {
                    auto typed_array = std::static_pointer_cast<arrow::Int32Array>(array);
                    if (typed_array->IsNull(row_idx)) {
                        group_key += "NULL";
                    } else {
                        group_key += std::to_string(typed_array->Value(row_idx));
                    }
                    break;
                }
                case arrow::Type::INT64: {
                    auto typed_array = std::static_pointer_cast<arrow::Int64Array>(array);
                    if (typed_array->IsNull(row_idx)) {
                        group_key += "NULL";
                    } else {
                        group_key += std::to_string(typed_array->Value(row_idx));
                    }
                    break;
                }
                case arrow::Type::FLOAT: {
                    auto typed_array = std::static_pointer_cast<arrow::FloatArray>(array);
                    if (typed_array->IsNull(row_idx)) {
                        group_key += "NULL";
                    } else {
                        group_key += std::to_string(typed_array->Value(row_idx));
                    }
                    break;
                }
                case arrow::Type::DOUBLE: {
                    auto typed_array = std::static_pointer_cast<arrow::DoubleArray>(array);
                    if (typed_array->IsNull(row_idx)) {
                        group_key += "NULL";
                    } else {
                        group_key += std::to_string(typed_array->Value(row_idx));
                    }
                    break;
                }
                case arrow::Type::STRING: {
                    auto typed_array = std::static_pointer_cast<arrow::StringArray>(array);
                    if (typed_array->IsNull(row_idx)) {
                        group_key += "NULL";
                    } else {
                        group_key += typed_array->GetString(row_idx);
                    }
                    break;
                }
                case arrow::Type::BOOL: {
                    auto typed_array = std::static_pointer_cast<arrow::BooleanArray>(array);
                    if (typed_array->IsNull(row_idx)) {
                        group_key += "NULL";
                    } else {
                        group_key += typed_array->Value(row_idx) ? "1" : "0";
                    }
                    break;
                }
                default: {
                    current_result_ = common::Result<ArrowDataBatchSharedPtr>::failure(
                        common::Error(common::ErrorCode::NotImplemented, "Unsupported type in GROUP BY"));
                    return;
                }
            }
        }

        // Add this row to the appropriate group
        group_map[group_key].push_back(row_idx);
    }

    // Prepare the output arrays
    auto output_schema = node.OutputSchema();
    const int num_groups = group_map.size();

    // Create builders for each output column
    std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders;
    for (int i = 0; i < output_schema.NumColumns(); ++i) {
        auto type = output_schema.Columns()[i].type;
        switch (type) {
            case common::ColumnType::INT32:
                builders.push_back(std::make_unique<arrow::Int32Builder>());
                break;
            case common::ColumnType::INT64:
                builders.push_back(std::make_unique<arrow::Int64Builder>());
                break;
            case common::ColumnType::FLOAT:
                builders.push_back(std::make_unique<arrow::FloatBuilder>());
                break;
            case common::ColumnType::DOUBLE:
                builders.push_back(std::make_unique<arrow::DoubleBuilder>());
                break;
            case common::ColumnType::STRING:
                builders.push_back(std::make_unique<arrow::StringBuilder>());
                break;
            case common::ColumnType::BOOLEAN:
                builders.push_back(std::make_unique<arrow::BooleanBuilder>());
                break;
            default:
                current_result_ = common::Result<ArrowDataBatchSharedPtr>::failure(
                    common::Error(common::ErrorCode::NotImplemented, "Unsupported type in output schema"));
                return;
        }
    }

    // Second pass: Compute the aggregates for each group
    int output_col_idx = 0;

    // First, add the group by columns to the output
    for (const auto& group_expr : group_by) {
        auto col_expr = std::static_pointer_cast<common::ColumnExpression>(group_expr);
        const std::string& col_name = col_expr->ColumnName();

        // Find column index in the input batch
        int input_col_idx = -1;
        auto input_schema = input_batch->schema();
        for (int i = 0; i < input_schema->num_fields(); ++i) {
            if (input_schema->field(i)->name() == col_name) {
                input_col_idx = i;
                break;
            }
        }

        if (input_col_idx == -1) {
            current_result_ = common::Result<ArrowDataBatchSharedPtr>::failure(
                common::Error(common::ErrorCode::InvalidArgument, "Column not found in input: " + col_name));
            return;
        }

        // Get the column array
        auto input_array = input_batch->column(input_col_idx);

        // For each group, add the group by column value to the output
        for (const auto& [group_key, row_indices] : group_map) {
            // Take the first row in the group for the group by columns
            int row_idx = row_indices[0];

            switch (input_array->type_id()) {
                case arrow::Type::INT32: {
                    auto typed_array = std::static_pointer_cast<arrow::Int32Array>(input_array);
                    auto typed_builder = static_cast<arrow::Int32Builder*>(builders[output_col_idx].get());
                    if (typed_array->IsNull(row_idx)) {
                        typed_builder->AppendNull();
                    } else {
                        typed_builder->Append(typed_array->Value(row_idx));
                    }
                    break;
                }
                case arrow::Type::INT64: {
                    auto typed_array = std::static_pointer_cast<arrow::Int64Array>(input_array);
                    auto typed_builder = static_cast<arrow::Int64Builder*>(builders[output_col_idx].get());
                    if (typed_array->IsNull(row_idx)) {
                        typed_builder->AppendNull();
                    } else {
                        typed_builder->Append(typed_array->Value(row_idx));
                    }
                    break;
                }
                case arrow::Type::FLOAT: {
                    auto typed_array = std::static_pointer_cast<arrow::FloatArray>(input_array);
                    auto typed_builder = static_cast<arrow::FloatBuilder*>(builders[output_col_idx].get());
                    if (typed_array->IsNull(row_idx)) {
                        typed_builder->AppendNull();
                    } else {
                        typed_builder->Append(typed_array->Value(row_idx));
                    }
                    break;
                }
                case arrow::Type::DOUBLE: {
                    auto typed_array = std::static_pointer_cast<arrow::DoubleArray>(input_array);
                    auto typed_builder = static_cast<arrow::DoubleBuilder*>(builders[output_col_idx].get());
                    if (typed_array->IsNull(row_idx)) {
                        typed_builder->AppendNull();
                    } else {
                        typed_builder->Append(typed_array->Value(row_idx));
                    }
                    break;
                }
                case arrow::Type::STRING: {
                    auto typed_array = std::static_pointer_cast<arrow::StringArray>(input_array);
                    auto typed_builder = static_cast<arrow::StringBuilder*>(builders[output_col_idx].get());
                    if (typed_array->IsNull(row_idx)) {
                        typed_builder->AppendNull();
                    } else {
                        typed_builder->Append(typed_array->GetString(row_idx));
                    }
                    break;
                }
                case arrow::Type::BOOL: {
                    auto typed_array = std::static_pointer_cast<arrow::BooleanArray>(input_array);
                    auto typed_builder = static_cast<arrow::BooleanBuilder*>(builders[output_col_idx].get());
                    if (typed_array->IsNull(row_idx)) {
                        typed_builder->AppendNull();
                    } else {
                        typed_builder->Append(typed_array->Value(row_idx));
                    }
                    break;
                }
                default: {
                    current_result_ = common::Result<ArrowDataBatchSharedPtr>::failure(
                        common::Error(common::ErrorCode::NotImplemented, "Unsupported type in GROUP BY"));
                    return;
                }
            }
        }

        output_col_idx++;
    }

    // Now, compute the aggregates
    for (const auto& agg_expr : aggregates) {
        if (agg_expr->Type() != common::ExprType::Aggregate) {
            current_result_ = common::Result<ArrowDataBatchSharedPtr>::failure(common::Error(
                common::ErrorCode::NotImplemented, "Only aggregate expressions are supported in aggregates list"));
            return;
        }

        auto agg = std::static_pointer_cast<common::AggregateExpression>(agg_expr);
        auto agg_type = agg->AggType();
        auto input_expr = agg->Input();

        // For now, only support column references in aggregate input
        if (input_expr->Type() != common::ExprType::Column && input_expr->Type() != common::ExprType::Star) {
            current_result_ = common::Result<ArrowDataBatchSharedPtr>::failure(common::Error(
                common::ErrorCode::NotImplemented, "Only column references or * are supported in aggregate input"));
            return;
        }

        // Special case for COUNT(*)
        if (agg_type == common::AggregateType::Count && input_expr->Type() == common::ExprType::Star) {
            auto typed_builder = static_cast<arrow::Int64Builder*>(builders[output_col_idx].get());

            // Count the number of rows in each group
            for (const auto& [group_key, row_indices] : group_map) {
                typed_builder->Append(row_indices.size());
            }

            output_col_idx++;
            continue;
        }

        // For other aggregates, we need the column
        auto col_expr = std::static_pointer_cast<common::ColumnExpression>(input_expr);
        const std::string& col_name = col_expr->ColumnName();

        // Find column index in the input batch
        int input_col_idx = -1;
        auto input_schema = input_batch->schema();
        for (int i = 0; i < input_schema->num_fields(); ++i) {
            if (input_schema->field(i)->name() == col_name) {
                input_col_idx = i;
                break;
            }
        }

        if (input_col_idx == -1) {
            current_result_ = common::Result<ArrowDataBatchSharedPtr>::failure(
                common::Error(common::ErrorCode::InvalidArgument, "Column not found in input: " + col_name));
            return;
        }

        // Get the column array
        auto input_array = input_batch->column(input_col_idx);

        // Apply the aggregate function to each group
        for (const auto& [group_key, row_indices] : group_map) {
            // Compute the aggregate value based on the type
            switch (agg_type) {
                case common::AggregateType::Count: {
                    auto typed_builder = static_cast<arrow::Int64Builder*>(builders[output_col_idx].get());

                    // Count non-null values
                    int64_t count = 0;
                    for (int row_idx : row_indices) {
                        if (!input_array->IsNull(row_idx)) {
                            count++;
                        }
                    }

                    typed_builder->Append(count);
                    break;
                }
                case common::AggregateType::Sum: {
                    // Handle different numeric types
                    switch (input_array->type_id()) {
                        case arrow::Type::INT32: {
                            auto typed_array = std::static_pointer_cast<arrow::Int32Array>(input_array);
                            auto typed_builder = static_cast<arrow::Int64Builder*>(builders[output_col_idx].get());

                            int64_t sum = 0;
                            bool all_null = true;

                            for (int row_idx : row_indices) {
                                if (!typed_array->IsNull(row_idx)) {
                                    sum += typed_array->Value(row_idx);
                                    all_null = false;
                                }
                            }

                            if (all_null) {
                                typed_builder->AppendNull();
                            } else {
                                typed_builder->Append(sum);
                            }
                            break;
                        }
                        case arrow::Type::INT64: {
                            auto typed_array = std::static_pointer_cast<arrow::Int64Array>(input_array);
                            auto typed_builder = static_cast<arrow::Int64Builder*>(builders[output_col_idx].get());

                            int64_t sum = 0;
                            bool all_null = true;

                            for (int row_idx : row_indices) {
                                if (!typed_array->IsNull(row_idx)) {
                                    sum += typed_array->Value(row_idx);
                                    all_null = false;
                                }
                            }

                            if (all_null) {
                                typed_builder->AppendNull();
                            } else {
                                typed_builder->Append(sum);
                            }
                            break;
                        }
                        case arrow::Type::FLOAT: {
                            auto typed_array = std::static_pointer_cast<arrow::FloatArray>(input_array);
                            auto typed_builder = static_cast<arrow::FloatBuilder*>(builders[output_col_idx].get());

                            float sum = 0.0f;
                            bool all_null = true;

                            for (int row_idx : row_indices) {
                                if (!typed_array->IsNull(row_idx)) {
                                    sum += typed_array->Value(row_idx);
                                    all_null = false;
                                }
                            }

                            if (all_null) {
                                typed_builder->AppendNull();
                            } else {
                                typed_builder->Append(sum);
                            }
                            break;
                        }
                        case arrow::Type::DOUBLE: {
                            auto typed_array = std::static_pointer_cast<arrow::DoubleArray>(input_array);
                            auto typed_builder = static_cast<arrow::DoubleBuilder*>(builders[output_col_idx].get());

                            double sum = 0.0;
                            bool all_null = true;

                            for (int row_idx : row_indices) {
                                if (!typed_array->IsNull(row_idx)) {
                                    sum += typed_array->Value(row_idx);
                                    all_null = false;
                                }
                            }

                            if (all_null) {
                                typed_builder->AppendNull();
                            } else {
                                typed_builder->Append(sum);
                            }
                            break;
                        }
                        default: {
                            current_result_ = common::Result<ArrowDataBatchSharedPtr>::failure(
                                common::Error(common::ErrorCode::NotImplemented, "SUM only supports numeric types"));
                            return;
                        }
                    }
                    break;
                }
                case common::AggregateType::Avg: {
                    // Handle different numeric types
                    switch (input_array->type_id()) {
                        case arrow::Type::INT32: {
                            auto typed_array = std::static_pointer_cast<arrow::Int32Array>(input_array);
                            auto typed_builder = static_cast<arrow::DoubleBuilder*>(builders[output_col_idx].get());

                            double sum = 0.0;
                            int count = 0;

                            for (int row_idx : row_indices) {
                                if (!typed_array->IsNull(row_idx)) {
                                    sum += typed_array->Value(row_idx);
                                    count++;
                                }
                            }

                            if (count == 0) {
                                typed_builder->AppendNull();
                            } else {
                                typed_builder->Append(sum / count);
                            }
                            break;
                        }
                        case arrow::Type::INT64: {
                            auto typed_array = std::static_pointer_cast<arrow::Int64Array>(input_array);
                            auto typed_builder = static_cast<arrow::DoubleBuilder*>(builders[output_col_idx].get());

                            double sum = 0.0;
                            int count = 0;

                            for (int row_idx : row_indices) {
                                if (!typed_array->IsNull(row_idx)) {
                                    sum += typed_array->Value(row_idx);
                                    count++;
                                }
                            }

                            if (count == 0) {
                                typed_builder->AppendNull();
                            } else {
                                typed_builder->Append(sum / count);
                            }
                            break;
                        }
                        case arrow::Type::FLOAT: {
                            auto typed_array = std::static_pointer_cast<arrow::FloatArray>(input_array);
                            auto typed_builder = static_cast<arrow::DoubleBuilder*>(builders[output_col_idx].get());

                            double sum = 0.0;
                            int count = 0;

                            for (int row_idx : row_indices) {
                                if (!typed_array->IsNull(row_idx)) {
                                    sum += typed_array->Value(row_idx);
                                    count++;
                                }
                            }

                            if (count == 0) {
                                typed_builder->AppendNull();
                            } else {
                                typed_builder->Append(sum / count);
                            }
                            break;
                        }
                        case arrow::Type::DOUBLE: {
                            auto typed_array = std::static_pointer_cast<arrow::DoubleArray>(input_array);
                            auto typed_builder = static_cast<arrow::DoubleBuilder*>(builders[output_col_idx].get());

                            double sum = 0.0;
                            int count = 0;

                            for (int row_idx : row_indices) {
                                if (!typed_array->IsNull(row_idx)) {
                                    sum += typed_array->Value(row_idx);
                                    count++;
                                }
                            }

                            if (count == 0) {
                                typed_builder->AppendNull();
                            } else {
                                typed_builder->Append(sum / count);
                            }
                            break;
                        }
                        default: {
                            current_result_ = common::Result<ArrowDataBatchSharedPtr>::failure(
                                common::Error(common::ErrorCode::NotImplemented, "AVG only supports numeric types"));
                            return;
                        }
                    }
                    break;
                }
                case common::AggregateType::Min: {
                    // Handle different types
                    switch (input_array->type_id()) {
                        case arrow::Type::INT32: {
                            auto typed_array = std::static_pointer_cast<arrow::Int32Array>(input_array);
                            auto typed_builder = static_cast<arrow::Int32Builder*>(builders[output_col_idx].get());

                            bool found_value = false;
                            int32_t min_val = std::numeric_limits<int32_t>::max();

                            for (int row_idx : row_indices) {
                                if (!typed_array->IsNull(row_idx)) {
                                    min_val = std::min(min_val, typed_array->Value(row_idx));
                                    found_value = true;
                                }
                            }

                            if (!found_value) {
                                typed_builder->AppendNull();
                            } else {
                                typed_builder->Append(min_val);
                            }
                            break;
                        }
                        case arrow::Type::INT64: {
                            auto typed_array = std::static_pointer_cast<arrow::Int64Array>(input_array);
                            auto typed_builder = static_cast<arrow::Int64Builder*>(builders[output_col_idx].get());

                            bool found_value = false;
                            int64_t min_val = std::numeric_limits<int64_t>::max();

                            for (int row_idx : row_indices) {
                                if (!typed_array->IsNull(row_idx)) {
                                    min_val = std::min(min_val, typed_array->Value(row_idx));
                                    found_value = true;
                                }
                            }

                            if (!found_value) {
                                typed_builder->AppendNull();
                            } else {
                                typed_builder->Append(min_val);
                            }
                            break;
                        }
                        case arrow::Type::FLOAT: {
                            auto typed_array = std::static_pointer_cast<arrow::FloatArray>(input_array);
                            auto typed_builder = static_cast<arrow::FloatBuilder*>(builders[output_col_idx].get());

                            bool found_value = false;
                            float min_val = std::numeric_limits<float>::max();

                            for (int row_idx : row_indices) {
                                if (!typed_array->IsNull(row_idx)) {
                                    min_val = std::min(min_val, typed_array->Value(row_idx));
                                    found_value = true;
                                }
                            }

                            if (!found_value) {
                                typed_builder->AppendNull();
                            } else {
                                typed_builder->Append(min_val);
                            }
                            break;
                        }
                        case arrow::Type::DOUBLE: {
                            auto typed_array = std::static_pointer_cast<arrow::DoubleArray>(input_array);
                            auto typed_builder = static_cast<arrow::DoubleBuilder*>(builders[output_col_idx].get());

                            bool found_value = false;
                            double min_val = std::numeric_limits<double>::max();

                            for (int row_idx : row_indices) {
                                if (!typed_array->IsNull(row_idx)) {
                                    min_val = std::min(min_val, typed_array->Value(row_idx));
                                    found_value = true;
                                }
                            }

                            if (!found_value) {
                                typed_builder->AppendNull();
                            } else {
                                typed_builder->Append(min_val);
                            }
                            break;
                        }
                        case arrow::Type::STRING: {
                            auto typed_array = std::static_pointer_cast<arrow::StringArray>(input_array);
                            auto typed_builder = static_cast<arrow::StringBuilder*>(builders[output_col_idx].get());

                            bool found_value = false;
                            std::string min_val;

                            for (int row_idx : row_indices) {
                                if (!typed_array->IsNull(row_idx)) {
                                    std::string current = typed_array->GetString(row_idx);
                                    if (!found_value || current < min_val) {
                                        min_val = current;
                                        found_value = true;
                                    }
                                }
                            }

                            if (!found_value) {
                                typed_builder->AppendNull();
                            } else {
                                typed_builder->Append(min_val);
                            }
                            break;
                        }
                        default: {
                            current_result_ = common::Result<ArrowDataBatchSharedPtr>::failure(
                                common::Error(common::ErrorCode::NotImplemented, "Unsupported type for MIN"));
                            return;
                        }
                    }
                    break;
                }
                case common::AggregateType::Max: {
                    // Handle different types
                    switch (input_array->type_id()) {
                        case arrow::Type::INT32: {
                            auto typed_array = std::static_pointer_cast<arrow::Int32Array>(input_array);
                            auto typed_builder = static_cast<arrow::Int32Builder*>(builders[output_col_idx].get());

                            bool found_value = false;
                            int32_t max_val = std::numeric_limits<int32_t>::min();

                            for (int row_idx : row_indices) {
                                if (!typed_array->IsNull(row_idx)) {
                                    max_val = std::max(max_val, typed_array->Value(row_idx));
                                    found_value = true;
                                }
                            }

                            if (!found_value) {
                                typed_builder->AppendNull();
                            } else {
                                typed_builder->Append(max_val);
                            }
                            break;
                        }
                        case arrow::Type::INT64: {
                            auto typed_array = std::static_pointer_cast<arrow::Int64Array>(input_array);
                            auto typed_builder = static_cast<arrow::Int64Builder*>(builders[output_col_idx].get());

                            bool found_value = false;
                            int64_t max_val = std::numeric_limits<int64_t>::min();

                            for (int row_idx : row_indices) {
                                if (!typed_array->IsNull(row_idx)) {
                                    max_val = std::max(max_val, typed_array->Value(row_idx));
                                    found_value = true;
                                }
                            }

                            if (!found_value) {
                                typed_builder->AppendNull();
                            } else {
                                typed_builder->Append(max_val);
                            }
                            break;
                        }
                        case arrow::Type::FLOAT: {
                            auto typed_array = std::static_pointer_cast<arrow::FloatArray>(input_array);
                            auto typed_builder = static_cast<arrow::FloatBuilder*>(builders[output_col_idx].get());

                            bool found_value = false;
                            float max_val = std::numeric_limits<float>::lowest();

                            for (int row_idx : row_indices) {
                                if (!typed_array->IsNull(row_idx)) {
                                    max_val = std::max(max_val, typed_array->Value(row_idx));
                                    found_value = true;
                                }
                            }

                            if (!found_value) {
                                typed_builder->AppendNull();
                            } else {
                                typed_builder->Append(max_val);
                            }
                            break;
                        }
                        case arrow::Type::DOUBLE: {
                            auto typed_array = std::static_pointer_cast<arrow::DoubleArray>(input_array);
                            auto typed_builder = static_cast<arrow::DoubleBuilder*>(builders[output_col_idx].get());

                            bool found_value = false;
                            double max_val = std::numeric_limits<double>::lowest();

                            for (int row_idx : row_indices) {
                                if (!typed_array->IsNull(row_idx)) {
                                    max_val = std::max(max_val, typed_array->Value(row_idx));
                                    found_value = true;
                                }
                            }

                            if (!found_value) {
                                typed_builder->AppendNull();
                            } else {
                                typed_builder->Append(max_val);
                            }
                            break;
                        }
                        case arrow::Type::STRING: {
                            auto typed_array = std::static_pointer_cast<arrow::StringArray>(input_array);
                            auto typed_builder = static_cast<arrow::StringBuilder*>(builders[output_col_idx].get());

                            bool found_value = false;
                            std::string max_val;

                            for (int row_idx : row_indices) {
                                if (!typed_array->IsNull(row_idx)) {
                                    std::string current = typed_array->GetString(row_idx);
                                    if (!found_value || current > max_val) {
                                        max_val = current;
                                        found_value = true;
                                    }
                                }
                            }

                            if (!found_value) {
                                typed_builder->AppendNull();
                            } else {
                                typed_builder->Append(max_val);
                            }
                            break;
                        }
                        default: {
                            current_result_ = common::Result<ArrowDataBatchSharedPtr>::failure(
                                common::Error(common::ErrorCode::NotImplemented, "Unsupported type for MAX"));
                            return;
                        }
                    }
                    break;
                }
                default: {
                    current_result_ = common::Result<ArrowDataBatchSharedPtr>::failure(
                        common::Error(common::ErrorCode::NotImplemented, "Unsupported aggregate function"));
                    return;
                }
            }
        }

        output_col_idx++;
    }

    // Finalize the arrays and create the record batch
    std::vector<std::shared_ptr<arrow::Array>> output_arrays;
    for (auto& builder : builders) {
        std::shared_ptr<arrow::Array> array;
        auto status = builder->Finish(&array);
        if (!status.ok()) {
            current_result_ = common::Result<ArrowDataBatchSharedPtr>::failure(
                common::Error(common::ErrorCode::Failure, "Failed to finalize array: " + status.ToString()));
            return;
        }
        output_arrays.push_back(array);
    }

    // Create arrow schema from output schema
    auto arrow_schema_result = format::SchemaConverter::ToArrowSchema(output_schema);
    if (!arrow_schema_result.ok()) {
        current_result_ = common::Result<ArrowDataBatchSharedPtr>::failure(arrow_schema_result.error());
        return;
    }
    auto arrow_schema = arrow_schema_result.value();

    // Create the output batch
    current_batch_ = arrow::RecordBatch::Make(arrow_schema, num_groups, output_arrays);
    current_result_ = common::Result<ArrowDataBatchSharedPtr>::success(current_batch_);
}

void MaterializedExecutor::Visit(PhysicalSortNode& node) {
    current_result_ = common::Error(common::ErrorCode::NotImplemented, "Sort not implemented in simple executor");
}

void MaterializedExecutor::Visit(PhysicalLimitNode& node) {
    current_result_ = common::Error(common::ErrorCode::NotImplemented, "Limit not implemented in simple executor");
}

void MaterializedExecutor::Visit(PhysicalShuffleExchangeNode& node) {
    current_result_ =
        common::Error(common::ErrorCode::NotImplemented, "Shuffle exchange not implemented in simple executor");
}

}  // namespace pond::query