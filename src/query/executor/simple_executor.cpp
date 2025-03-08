#include "query/executor/simple_executor.h"

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/record_batch.h>
#include <arrow/table.h>
#include <arrow/type_traits.h>

#include "catalog/metadata.h"
#include "common/error.h"
#include "common/log.h"
#include "format/parquet/parquet_reader.h"
#include "query/data/arrow_util.h"
#include "query/planner/physical_plan_node.h"

namespace pond::query {

SimpleExecutor::SimpleExecutor(std::shared_ptr<catalog::Catalog> catalog, std::shared_ptr<DataAccessor> data_accessor)
    : catalog_(std::move(catalog)),
      data_accessor_(std::move(data_accessor)),
      current_result_(common::Result<DataBatchSharedPtr>::success(nullptr)) {}

common::Result<DataBatchSharedPtr> SimpleExecutor::execute(std::shared_ptr<PhysicalPlanNode> plan) {
    // Reset the current batch and result
    current_batch_ = nullptr;
    current_result_ = common::Result<DataBatchSharedPtr>::success(nullptr);

    // If the plan is null, return an error
    if (!plan) {
        return common::Error(common::ErrorCode::InvalidArgument, "Physical plan is null");
    }

    // Accept the plan visitor to execute the plan
    plan->Accept(*this);

    // Return the current result
    return current_result_;
}

common::Result<DataBatchSharedPtr> SimpleExecutor::CurrentBatch() const {
    return current_result_;
}

common::Result<DataBatchSharedPtr> SimpleExecutor::ExecuteChildren(PhysicalPlanNode& node) {
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

common::Result<bool> SimpleExecutor::ProduceResults(const common::Schema& schema) {
    // Not needed for this simple implementation
    return common::Result<bool>::success(true);
}

void SimpleExecutor::Visit(PhysicalSequentialScanNode& node) {
    // Get the table name from the node
    const std::string& table_name = node.TableName();

    // List table files from catalog
    auto files_result = data_accessor_->ListTableFiles(table_name);
    if (!files_result.ok()) {
        current_result_ = common::Result<DataBatchSharedPtr>::failure(files_result.error());
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
        current_result_ = common::Result<DataBatchSharedPtr>::success(current_batch_);
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
            current_result_ = common::Result<DataBatchSharedPtr>::failure(reader_result.error());
            return;
        }

        // Get the reader
        auto reader = std::move(reader_result).value();

        // Use the batch reader interface
        auto batch_reader_result = reader->GetBatchReader();
        if (!batch_reader_result.ok()) {
            current_result_ = common::Result<DataBatchSharedPtr>::failure(batch_reader_result.error());
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
                        current_result_ = common::Result<DataBatchSharedPtr>::failure(filter_result.error());
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
            current_result_ = common::Result<DataBatchSharedPtr>::failure(concat_result.error());
            return;
        }

        current_batch_ = concat_result.value();
        LOG_VERBOSE("Concatenated %d batches with total of %d rows", batches.size(), current_batch_->num_rows());
    }

    current_result_ = common::Result<DataBatchSharedPtr>::success(current_batch_);
    return;
}

void SimpleExecutor::Visit(PhysicalFilterNode& node) {
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
        current_result_ = common::Result<DataBatchSharedPtr>::failure(filter_result.error());
        return;
    }

    // Update the current batch with the filtered result
    current_batch_ = filter_result.value();
    current_result_ = common::Result<DataBatchSharedPtr>::success(current_batch_);
}

// Minimal implementation for other operators, as we're only supporting simple SELECT *
void SimpleExecutor::Visit(PhysicalIndexScanNode& node) {
    current_result_ = common::Error(common::ErrorCode::NotImplemented, "Index scan not implemented in simple executor");
}

void SimpleExecutor::Visit(PhysicalProjectionNode& node) {
    // Execute the child node first
    auto child_result = ExecuteChildren(node);
    if (!child_result.ok()) {
        current_result_ = std::move(child_result);
        return;
    }

    // Get input batch from child
    auto input_batch = child_result.value();
    if (!input_batch) {
        current_result_ = common::Result<DataBatchSharedPtr>::failure(
            common::Error(common::ErrorCode::InvalidArgument, "Input batch is null"));
        return;
    }

    // Get the projection expressions
    const auto& projections = node.Projections();
    if (projections.empty()) {
        // No projections, just return the input batch
        current_batch_ = input_batch;
        current_result_ = common::Result<DataBatchSharedPtr>::success(current_batch_);
        return;
    }

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
                current_result_ = common::Result<DataBatchSharedPtr>::failure(
                    common::Error(common::ErrorCode::InvalidArgument, "Column not found: " + col_name));
                return;
            }

            // Add the field to output schema
            output_fields.push_back(input_schema->field(col_idx));
        } else {
            // For now, only support column references
            current_result_ = common::Result<DataBatchSharedPtr>::failure(common::Error(
                common::ErrorCode::NotImplemented, "Only column references are supported in projections"));
            return;
        }
    }

    // Create output schema
    auto output_schema = arrow::schema(output_fields);

    // Create arrays for the output batch
    std::vector<std::shared_ptr<arrow::Array>> output_arrays;
    for (const auto& expr : projections) {
        // Only handle column references for now
        if (expr->Type() == common::ExprType::Column) {
            auto col_expr = std::static_pointer_cast<common::ColumnExpression>(expr);
            const std::string& col_name = col_expr->ColumnName();

            // Find the column in the input batch
            int col_idx = -1;
            auto input_schema = input_batch->schema();
            for (int i = 0; i < input_schema->num_fields(); ++i) {
                if (input_schema->field(i)->name() == col_name) {
                    col_idx = i;
                    break;
                }
            }

            // This should never happen because we already checked above
            if (col_idx == -1) {
                current_result_ = common::Result<DataBatchSharedPtr>::failure(
                    common::Error(common::ErrorCode::InvalidArgument, "Column not found: " + col_name));
                return;
            }

            // Add the column to the output arrays
            output_arrays.push_back(input_batch->column(col_idx));
        } else {
            // For now, only support column references
            current_result_ = common::Result<DataBatchSharedPtr>::failure(common::Error(
                common::ErrorCode::NotImplemented, "Only column references are supported in projections"));
            return;
        }
    }

    // Create the output batch
    current_batch_ = arrow::RecordBatch::Make(output_schema, input_batch->num_rows(), output_arrays);
    current_result_ = common::Result<DataBatchSharedPtr>::success(current_batch_);
}

void SimpleExecutor::Visit(PhysicalHashJoinNode& node) {
    current_result_ = common::Error(common::ErrorCode::NotImplemented, "Hash join not implemented in simple executor");
}

void SimpleExecutor::Visit(PhysicalNestedLoopJoinNode& node) {
    current_result_ =
        common::Error(common::ErrorCode::NotImplemented, "Nested loop join not implemented in simple executor");
}

void SimpleExecutor::Visit(PhysicalHashAggregateNode& node) {
    current_result_ =
        common::Error(common::ErrorCode::NotImplemented, "Hash aggregate not implemented in simple executor");
}

void SimpleExecutor::Visit(PhysicalSortNode& node) {
    current_result_ = common::Error(common::ErrorCode::NotImplemented, "Sort not implemented in simple executor");
}

void SimpleExecutor::Visit(PhysicalLimitNode& node) {
    current_result_ = common::Error(common::ErrorCode::NotImplemented, "Limit not implemented in simple executor");
}

void SimpleExecutor::Visit(PhysicalShuffleExchangeNode& node) {
    current_result_ =
        common::Error(common::ErrorCode::NotImplemented, "Shuffle exchange not implemented in simple executor");
}

}  // namespace pond::query