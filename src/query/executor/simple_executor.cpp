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

    // If there are no files, return an empty result
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

    // Use the first file for this simple implementation
    const auto& file = files_result.value()[0];

    // Create a reader for the file
    auto reader_result = data_accessor_->GetReader(file);
    if (!reader_result.ok()) {
        current_result_ = common::Result<DataBatchSharedPtr>::failure(reader_result.error());
        return;
    }

    // Get the reader
    auto reader = std::move(reader_result).value();

    // Read the first batch by reading the entire table and converting to a record batch
    auto table_result = reader->read();
    if (!table_result.ok()) {
        current_result_ = common::Result<DataBatchSharedPtr>::failure(table_result.error());
        return;
    }

    // Convert the table to a record batch
    std::shared_ptr<arrow::Table> table = table_result.value();
    if (table->num_rows() > 0) {
        arrow::TableBatchReader reader(*table);
        std::shared_ptr<arrow::RecordBatch> batch;
        auto status = reader.ReadNext(&batch);
        if (!status.ok()) {
            current_result_ = common::Error(common::ErrorCode::Failure,
                                            "Failed to convert table to record batch: " + status.ToString());
            return;
        }
        current_batch_ = batch;

        // If the scan node has a predicate, apply it directly here (predicate pushdown)
        if (node.Predicate()) {
            auto filter_result = ArrowUtil::ApplyPredicate(current_batch_, node.Predicate());
            if (!filter_result.ok()) {
                current_result_ = common::Result<DataBatchSharedPtr>::failure(filter_result.error());
                return;
            }
            current_batch_ = filter_result.value();
        }
    } else {
        // Create empty batch with the schema using ArrowUtil
        auto empty_batch_result = ArrowUtil::CreateEmptyBatch(node.OutputSchema());
        if (!empty_batch_result.ok()) {
            current_result_ = common::Error(common::ErrorCode::Failure,
                                            "Failed to create empty batch: " + empty_batch_result.error().message());
            return;
        }

        current_batch_ = empty_batch_result.value();
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
    current_result_ = common::Error(common::ErrorCode::NotImplemented, "Projection not implemented in simple executor");
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