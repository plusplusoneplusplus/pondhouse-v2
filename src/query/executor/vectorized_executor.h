#pragma once

#include <memory>

#include "catalog/catalog.h"
#include "common/result.h"
#include "query/data/arrow_util.h"
#include "query/data/data_accessor.h"
#include "query/executor/executor.h"
#include "query/planner/physical_plan_node.h"

namespace pond::query {

/**
 * @brief Implementation of BatchIterator for different operators
 */
class OperatorIterator : public BatchIterator {
public:
    virtual ~OperatorIterator() = default;

    /**
     * @brief Initialize the iterator before usage
     * This allows for deferred initialization after construction
     *
     * @return common::Result<bool> Success or error if initialization fails
     */
    virtual common::Result<bool> Initialize() = 0;

    /**
     * @brief Get the schema of the data produced by this iterator
     *
     * @return common::Result<common::Schema> Schema of the output data
     */
    virtual common::Result<common::Schema> GetSchema() const = 0;
};

/**
 * @brief Vectorized query executor that implements the physical plan using a pull-based model
 *
 * The VectorizedExecutor processes queries by converting the physical plan tree into a tree of
 * iterators, where each operator pulls data from its children as needed. This allows for
 * lazy evaluation and streaming processing of data.
 */
class VectorizedExecutor : public Executor {
public:
    /**
     * @brief Create a new VectorizedExecutor
     *
     * @param catalog Catalog for metadata access
     * @param data_accessor Data accessor for reading data files
     */
    VectorizedExecutor(std::shared_ptr<catalog::Catalog> catalog, std::shared_ptr<DataAccessor> data_accessor);

    ~VectorizedExecutor() = default;

    /**
     * @brief Execute a physical plan and return an iterator for retrieving results
     *
     * @param plan The physical plan to execute
     * @return common::Result<std::unique_ptr<BatchIterator>> Iterator for retrieving result batches
     */
    common::Result<std::unique_ptr<BatchIterator>> Execute(std::shared_ptr<PhysicalPlanNode> plan) override;

private:
    /**
     * @brief Create an iterator for a specific node in the physical plan
     *
     * @param node The physical plan node
     * @return common::Result<std::unique_ptr<OperatorIterator>> The created iterator
     */
    common::Result<std::unique_ptr<OperatorIterator>> CreateIterator(std::shared_ptr<PhysicalPlanNode> node);

    // State
    std::shared_ptr<catalog::Catalog> catalog_;
    std::shared_ptr<DataAccessor> data_accessor_;
};

}  // namespace pond::query