#pragma once

#include <memory>

#include "common/result.h"
#include "common/schema.h"
#include "query/data/arrow_util.h"
#include "query/planner/physical_plan_node.h"

namespace pond::query {

/**
 * @brief Interface for query executors
 * Query executors are responsible for executing physical plans and producing results.
 * They implement the visitor pattern to handle different physical operator types.
 */
class Executor : public PhysicalPlanVisitor {
public:
    virtual ~Executor() = default;

    // Execute a physical plan
    virtual common::Result<ArrowDataBatchSharedPtr> execute(std::shared_ptr<PhysicalPlanNode> plan) = 0;

    // Visitor methods for different physical operators
    virtual void Visit(PhysicalSequentialScanNode& node) override = 0;
    virtual void Visit(PhysicalIndexScanNode& node) override = 0;
    virtual void Visit(PhysicalFilterNode& node) override = 0;
    virtual void Visit(PhysicalProjectionNode& node) override = 0;
    virtual void Visit(PhysicalHashJoinNode& node) override = 0;
    virtual void Visit(PhysicalNestedLoopJoinNode& node) override = 0;
    virtual void Visit(PhysicalHashAggregateNode& node) override = 0;
    virtual void Visit(PhysicalSortNode& node) override = 0;
    virtual void Visit(PhysicalLimitNode& node) override = 0;
    virtual void Visit(PhysicalShuffleExchangeNode& node) override = 0;

    // Get the current batch
    virtual common::Result<ArrowDataBatchSharedPtr> CurrentBatch() const = 0;

protected:
    // Helper methods for execution
    virtual common::Result<ArrowDataBatchSharedPtr> ExecuteChildren(PhysicalPlanNode& node) = 0;
    virtual common::Result<bool> ProduceResults(const common::Schema& schema) = 0;
};

}  // namespace pond::query