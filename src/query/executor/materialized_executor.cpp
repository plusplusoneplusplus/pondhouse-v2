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
#include "query/executor/executor_util.h"
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

    auto projection_result = ExecutorUtil::CreateProjectionBatch(node, input_batch);
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

common::Result<ArrowDataBatchSharedPtr> MaterializedExecutor::VisitHashAggregate(PhysicalHashAggregateNode& node) {
    using ReturnType = common::Result<ArrowDataBatchSharedPtr>;

    // Execute the child node first to get the input data
    auto result = ExecuteChildren(node);
    RETURN_IF_ERROR_T(ReturnType, result);

    // Get input batch from child
    auto input_batch = current_batch_;
    if (!input_batch || input_batch->num_rows() == 0) {
        // Create empty batch with the output schema
        auto empty_batch_result = ArrowUtil::CreateEmptyBatch(node.OutputSchema());
        RETURN_IF_ERROR_T(ReturnType, empty_batch_result);
        return empty_batch_result;
    }

    // Get the group by columns and aggregate expressions
    const auto& group_by_exprs = node.GroupBy();
    const auto& agg_exprs = node.Aggregates();

    // If there are no group by columns and no aggregates, just return the input
    if (group_by_exprs.empty() && agg_exprs.empty()) {
        return common::Result<ArrowDataBatchSharedPtr>::success(input_batch);
    }

    // Extract column names from group by expressions
    std::vector<std::string> group_by_columns;
    for (const auto& expr : group_by_exprs) {
        if (expr->Type() != common::ExprType::Column) {
            return ReturnType::failure(
                common::Error(common::ErrorCode::NotImplemented, "Only column references are supported in GROUP BY"));
        }
        auto col_expr = std::static_pointer_cast<common::ColumnExpression>(expr);
        group_by_columns.push_back(col_expr->ColumnName());
    }

    // Extract column names and aggregate types from aggregate expressions
    std::vector<std::string> agg_columns;
    std::vector<std::string> output_columns_override;
    std::vector<common::AggregateType> agg_types;

    for (const auto& expr : agg_exprs) {
        if (expr->Type() != common::ExprType::Aggregate) {
            return ReturnType::failure(common::Error(common::ErrorCode::NotImplemented,
                                                     "Only aggregate expressions are supported in aggregates list"));
        }

        auto agg_expr = std::static_pointer_cast<common::AggregateExpression>(expr);
        auto input_expr = agg_expr->Input();
        auto agg_type = agg_expr->AggType();

        // Special case for COUNT(*)
        if (agg_type == common::AggregateType::Count && input_expr->Type() == common::ExprType::Star) {
            // For COUNT(*), we can use any column since we're just counting rows
            if (input_batch->num_columns() > 0) {
                agg_columns.push_back(input_batch->schema()->field(0)->name());
                output_columns_override.push_back(agg_expr->ResultName());
                agg_types.push_back(common::AggregateType::Count);
            } else {
                return ReturnType::failure(
                    common::Error(common::ErrorCode::InvalidArgument, "Cannot perform COUNT(*) on empty schema"));
            }
            continue;
        }

        // For other aggregates, extract the column
        if (input_expr->Type() != common::ExprType::Column) {
            return ReturnType::failure(common::Error(common::ErrorCode::NotImplemented,
                                                     "Only column references are supported in aggregate input"));
        }

        auto col_expr = std::static_pointer_cast<common::ColumnExpression>(input_expr);
        agg_columns.push_back(col_expr->ColumnName());
        output_columns_override.push_back(agg_expr->ResultName());
        agg_types.push_back(agg_type);
    }

    // Special case for aggregates without GROUP BY - create a single group for the entire dataset
    // why we need to do this?
    // Given a query like:
    // SELECT COUNT(*) FROM table;
    // or
    // SELECT SUM(value) FROM table;
    //
    //        Input:           col1    col2
    //                     10      A
    //                     20      B
    //                     30      C

    //    With dummy:      __dummy_group    col1    col2
    //                     1                10      A
    //                     1                20      B
    //                     1                30      C

    //    After grouping:  __dummy_group    sum(col1)
    //                     1                60

    //    Final result:    sum(col1)
    //                     60
    if (group_by_columns.empty() && !agg_columns.empty()) {
        // We need to add a dummy constant column to group by
        // First, create a constant column with the same value for all rows
        arrow::Int32Builder builder;
        if (!builder.Reserve(input_batch->num_rows()).ok()) {
            return ReturnType::failure(
                common::Error(common::ErrorCode::InvalidArgument, "Failed to reserve space for dummy group column"));
        }
        for (int i = 0; i < input_batch->num_rows(); i++) {
            if (!builder.Append(1).ok()) {
                return ReturnType::failure(
                    common::Error(common::ErrorCode::InvalidArgument, "Failed to append value to dummy group column"));
            }
        }
        std::shared_ptr<arrow::Array> dummy_array;
        if (!builder.Finish(&dummy_array).ok()) {
            return ReturnType::failure(
                common::Error(common::ErrorCode::InvalidArgument, "Failed to finish dummy group column"));
        }

        // Create a new batch with the dummy column prepended
        std::vector<std::shared_ptr<arrow::Field>> fields = {arrow::field("__dummy_group", arrow::int32())};
        for (int i = 0; i < input_batch->schema()->num_fields(); i++) {
            fields.push_back(input_batch->schema()->field(i));
        }

        std::vector<std::shared_ptr<arrow::Array>> arrays = {dummy_array};
        for (int i = 0; i < input_batch->num_columns(); i++) {
            arrays.push_back(input_batch->column(i));
        }

        auto new_schema = arrow::schema(fields);
        auto new_batch = arrow::RecordBatch::Make(new_schema, input_batch->num_rows(), arrays);

        // Now use this batch and include the dummy column in group_by
        input_batch = new_batch;
        group_by_columns.push_back("__dummy_group");
    }

    LOG_CHECK(agg_columns.size() == agg_types.size(),
              "Number of aggregate columns does not match number of aggregate types");
    LOG_CHECK(output_columns_override.size() == agg_columns.size(),
              "Number of output columns override does not match number of aggregate columns");

    // Call ArrowUtil::HashAggregate with the extracted parameters
    auto agg_result =
        ArrowUtil::HashAggregate(input_batch, group_by_columns, agg_columns, agg_types, output_columns_override);
    RETURN_IF_ERROR_T(ReturnType, agg_result);

    LOG_STATUS("agg_result: %s", ArrowUtil::BatchToString(agg_result.value()).c_str());

    auto result_batch = agg_result.value();

    // If we added a dummy group column, remove it from the result
    if (group_by_exprs.empty() && !agg_columns.empty() && result_batch->schema()->GetFieldIndex("__dummy_group") >= 0) {
        // Create a new batch without the dummy column
        std::vector<std::shared_ptr<arrow::Field>> fields;
        std::vector<std::shared_ptr<arrow::Array>> arrays;

        for (int i = 0; i < result_batch->schema()->num_fields(); i++) {
            auto field = result_batch->schema()->field(i);
            if (field->name() != "__dummy_group") {
                fields.push_back(field);
                arrays.push_back(result_batch->column(i));
            }
        }

        auto new_schema = arrow::schema(fields);
        result_batch = arrow::RecordBatch::Make(new_schema, result_batch->num_rows(), arrays);
    }

    // Verify the schema matches the expected output schema
    auto output_schema = node.OutputSchema();

    // If schema doesn't match exactly what's expected, we need to create a projection
    if (result_batch->schema()->num_fields() != output_schema.NumColumns()) {
        LOG_VERBOSE("Aggregation result schema doesn't match output schema, creating projection");

        // Create a mapping from result columns to output columns
        std::unordered_map<std::string, int> result_col_indices;
        for (int i = 0; i < result_batch->schema()->num_fields(); i++) {
            result_col_indices[result_batch->schema()->field(i)->name()] = i;
        }

        // Create arrays for the projected output
        std::vector<std::shared_ptr<arrow::Array>> output_arrays;
        std::vector<std::shared_ptr<arrow::Field>> output_fields;

        for (const auto& col : output_schema.Columns()) {
            auto it = result_col_indices.find(col.name);
            if (it == result_col_indices.end()) {
                return ReturnType::failure(
                    common::Error(common::ErrorCode::InvalidArgument,
                                  "Column from output schema not found in aggregation result: " + col.name));
            }

            int col_idx = it->second;
            output_arrays.push_back(result_batch->column(col_idx));
            output_fields.push_back(result_batch->schema()->field(col_idx));
        }

        // Create the projected record batch
        auto arrow_schema = arrow::schema(output_fields);
        result_batch = arrow::RecordBatch::Make(arrow_schema, result_batch->num_rows(), output_arrays);
    }

    return ReturnType::success(result_batch);
}

void MaterializedExecutor::Visit(PhysicalHashAggregateNode& node) {
    current_result_ = VisitHashAggregate(node);
    if (current_result_.ok()) {
        current_batch_ = current_result_.value();
    }

    return;
}

void MaterializedExecutor::Visit(PhysicalSortNode& node) {
    // Execute the child node first
    auto result = ExecuteChildren(node);
    if (!result.ok()) {
        current_result_ = result;
        return;
    }

    // Check if we have data to sort
    if (!current_batch_) {
        current_result_ = common::Error(common::ErrorCode::Failure, "No data to sort");
        return;
    }

    // Get the sort specifications from the node
    const auto& sort_specs = node.SortSpecs();

    auto sort_result = ExecutorUtil::CreateSortBatch(current_batch_, sort_specs);
    if (!sort_result.ok()) {
        current_result_ = sort_result;
        return;
    }

    current_result_ = sort_result;
    current_batch_ = sort_result.value();
    return;
}

void MaterializedExecutor::Visit(PhysicalLimitNode& node) {
    // Execute the child node first
    auto result = ExecuteChildren(node);
    if (!result.ok()) {
        current_result_ = result;
        return;
    }

    auto limit_result = ExecutorUtil::CreateLimitBatch(current_batch_, node.Limit(), node.Offset());
    if (!limit_result.ok()) {
        current_result_ = limit_result;
        return;
    }

    current_result_ = limit_result;
    current_batch_ = limit_result.value();
    return;
}

void MaterializedExecutor::Visit(PhysicalShuffleExchangeNode& node) {
    current_result_ =
        common::Error(common::ErrorCode::NotImplemented, "Shuffle exchange not implemented in simple executor");
}

}  // namespace pond::query