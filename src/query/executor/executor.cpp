#include "query/executor/executor.h"

namespace pond::query {

common::Result<ArrowDataBatchSharedPtr> Executor::ExecuteToCompletion(std::shared_ptr<PhysicalPlanNode> plan) {
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

}  // namespace pond::query