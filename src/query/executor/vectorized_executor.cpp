#include "query/executor/vectorized_executor.h"

#include <arrow/api.h>
#include <arrow/table.h>

#include "common/error.h"
#include "common/log.h"
#include "format/parquet/schema_converter.h"
#include "query/data/arrow_util.h"
#include "query/executor/operator_iterators.h"

namespace pond::query {

VectorizedExecutor::VectorizedExecutor(std::shared_ptr<catalog::Catalog> catalog,
                                       std::shared_ptr<DataAccessor> data_accessor)
    : catalog_(std::move(catalog)), data_accessor_(std::move(data_accessor)) {}

common::Result<std::unique_ptr<BatchIterator>> VectorizedExecutor::Execute(std::shared_ptr<PhysicalPlanNode> plan) {
    if (!plan) {
        return common::Result<std::unique_ptr<BatchIterator>>::failure(common::ErrorCode::InvalidArgument,
                                                                       "Physical plan is null");
    }

    // Create the operator tree for the plan
    auto iterator_result = CreateIterator(plan);
    if (!iterator_result.ok()) {
        return common::Result<std::unique_ptr<BatchIterator>>::failure(iterator_result.error());
    }

    auto iterator = std::move(iterator_result).value();

    // Initialize the iterator
    auto init_result = iterator->Initialize();
    if (!init_result.ok()) {
        return common::Result<std::unique_ptr<BatchIterator>>::failure(init_result.error());
    }

    return common::Result<std::unique_ptr<BatchIterator>>::success(std::move(iterator));
}

common::Result<ArrowDataBatchSharedPtr> VectorizedExecutor::ExecuteToCompletion(
    std::shared_ptr<PhysicalPlanNode> plan) {
    using ReturnType = common::Result<ArrowDataBatchSharedPtr>;

    // Execute the plan and get the iterator
    auto iterator_result = Execute(plan);
    if (!iterator_result.ok()) {
        return common::Result<ArrowDataBatchSharedPtr>::failure(iterator_result.error());
    }

    auto iterator = std::move(iterator_result).value();
    auto output_schema = plan->OutputSchema();

    // If there's no data, return an empty batch with the plan's schema
    if (!iterator->HasNext()) {
        return ArrowUtil::CreateEmptyBatch(output_schema);
    }

    // Collect all batches
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    int64_t total_rows = 0;

    while (iterator->HasNext()) {
        auto batch_result = iterator->Next();
        if (!batch_result.ok()) {
            return common::Result<ArrowDataBatchSharedPtr>::failure(batch_result.error());
        }

        auto batch = batch_result.value();
        if (!batch) {
            break;  // End of data
        }

        batches.push_back(batch);
        total_rows += batch->num_rows();
    }

    // If no batches were collected (shouldn't happen, but just in case)
    if (batches.empty()) {
        return ArrowUtil::CreateEmptyBatch(output_schema);
    }

    // If there's only one batch, return it directly
    if (batches.size() == 1) {
        return common::Result<ArrowDataBatchSharedPtr>::success(batches[0]);
    }

    return ArrowUtil::ConcatenateBatches(batches);
}

common::Result<std::unique_ptr<OperatorIterator>> VectorizedExecutor::CreateIterator(
    std::shared_ptr<PhysicalPlanNode> node) {
    using ReturnType = common::Result<std::unique_ptr<OperatorIterator>>;

    if (!node) {
        return ReturnType::failure(common::ErrorCode::InvalidArgument, "Node is null");
    }

    // Handle different node types
    if (auto scan_node = std::dynamic_pointer_cast<PhysicalSequentialScanNode>(node)) {
        // Create a sequential scan iterator
        std::vector<std::string> column_names;
        // Get projections if the node has them
        if (scan_node->HasProjections()) {
            auto projection_schema = scan_node->ProjectionSchema();
            // Extract column names from projections
            for (const auto& column : projection_schema->Fields()) {
                column_names.push_back(column.name);
            }
        }

        return ReturnType::success(std::make_unique<SequentialScanIterator>(
            data_accessor_, scan_node->TableName(), scan_node->Predicate(), column_names, scan_node->OutputSchema()));
    } else if (auto filter_node = std::dynamic_pointer_cast<PhysicalFilterNode>(node)) {
        // Create iterators for the child nodes first
        if (filter_node->Children().empty()) {
            return ReturnType::failure(common::ErrorCode::InvalidArgument, "Filter node has no children");
        }

        auto child_iterator_result = CreateIterator(filter_node->Children()[0]);
        if (!child_iterator_result.ok()) {
            return child_iterator_result;
        }

        // Create a filter iterator with the child iterator
        return ReturnType::success(std::make_unique<FilterIterator>(
            std::move(child_iterator_result).value(), filter_node->Predicate(), filter_node->OutputSchema()));
    } else if (auto projection_node = std::dynamic_pointer_cast<PhysicalProjectionNode>(node)) {
        // Create iterators for the child nodes first
        if (projection_node->Children().empty()) {
            return ReturnType::failure(common::ErrorCode::InvalidArgument, "Projection node has no children");
        }

        auto child_iterator_result = CreateIterator(projection_node->Children()[0]);
        if (!child_iterator_result.ok()) {
            return child_iterator_result;
        }

        // Create a projection iterator with the child iterator
        return ReturnType::success(std::make_unique<ProjectionIterator>(
            std::move(child_iterator_result).value(), projection_node->Projections(), projection_node->OutputSchema()));
    }

    // Unsupported node type
    return ReturnType::failure(
        common::ErrorCode::NotImplemented,
        "Node type not implemented in vectorized execution model: " + PhysicalNodeTypeToString(node->Type()));
}

}  // namespace pond::query