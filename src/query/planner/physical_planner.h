#pragma once

#include <memory>
#include <optional>

#include "catalog/catalog.h"
#include "common/result.h"
#include "logical_plan_node.h"
#include "physical_plan_node.h"

namespace pond::query {

/**
 * @brief Physical planner that converts logical plans to physical plans
 * The physical planner is responsible for:
 * 1. Converting logical operators to physical operators
 * 2. Selecting appropriate implementations based on statistics and cost
 * 3. Determining access methods (e.g., sequential scan vs index scan)
 * 4. Choosing join algorithms (e.g., nested loop vs hash join)
 * 5. Adding exchange operators for distributed execution
 */
class PhysicalPlanner {
public:
    explicit PhysicalPlanner(catalog::Catalog& catalog);

    // Convert a logical plan to a physical plan
    common::Result<std::shared_ptr<PhysicalPlanNode>> Plan(std::shared_ptr<LogicalPlanNode> logical_plan);

protected:
    // Helper methods for planning different node types
    common::Result<std::shared_ptr<PhysicalPlanNode>> PlanScan(LogicalScanNode* node,
                                                               std::shared_ptr<Expression> pushed_predicate,
                                                               std::optional<common::Schema> projection_schema);
    common::Result<std::shared_ptr<PhysicalPlanNode>> PlanFilter(LogicalFilterNode* node);
    common::Result<std::shared_ptr<PhysicalPlanNode>> PlanProjection(LogicalProjectionNode* node);
    common::Result<std::shared_ptr<PhysicalPlanNode>> PlanJoin(LogicalJoinNode* node);
    common::Result<std::shared_ptr<PhysicalPlanNode>> PlanAggregate(LogicalAggregateNode* node);
    common::Result<std::shared_ptr<PhysicalPlanNode>> PlanSort(LogicalSortNode* node);
    common::Result<std::shared_ptr<PhysicalPlanNode>> PlanLimit(LogicalLimitNode* node);

    // Helper methods for distributed execution
    bool NeedsShuffle(LogicalPlanNode* node);
    bool IsDistributed() const;

private:
    catalog::Catalog& catalog_;
};

}  // namespace pond::query
