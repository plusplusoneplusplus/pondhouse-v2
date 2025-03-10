#pragma once

#include <memory>

#include "common/result.h"
#include "common/schema.h"
#include "query/data/arrow_util.h"
#include "query/planner/physical_plan_node.h"

namespace pond::query {

/**
 * @brief Iterator for pulling batches of data from a query execution
 *
 * This interface provides a pull-based vectorized execution model,
 * allowing consumers to request batches of data on demand.
 */
class BatchIterator {
public:
    virtual ~BatchIterator() = default;

    /**
     * @brief Pull the next batch of data
     *
     * @return common::Result<ArrowDataBatchSharedPtr> Next batch or end of data (nullptr) if no more data
     */
    virtual common::Result<ArrowDataBatchSharedPtr> Next() = 0;

    /**
     * @brief Check if more data is available without consuming it
     *
     * @return bool True if more data is available, false otherwise
     */
    virtual bool HasNext() const = 0;
};

/**
 * @brief Interface for query executors
 * Query executors are responsible for executing physical plans and producing results.
 * They implement the visitor pattern to handle different physical operator types.
 */
class Executor {
public:
    virtual ~Executor() = default;

    // Execute a physical plan
    virtual common::Result<std::unique_ptr<BatchIterator>> Execute(std::shared_ptr<PhysicalPlanNode> plan) = 0;

    /**
     * @brief Compatibility method that returns all results in a single batch
     *
     * @param plan The physical plan to execute
     * @return common::Result<ArrowDataBatchSharedPtr> All results in a single batch
     */
    virtual common::Result<ArrowDataBatchSharedPtr> ExecuteToCompletion(std::shared_ptr<PhysicalPlanNode> plan);
};

}  // namespace pond::query