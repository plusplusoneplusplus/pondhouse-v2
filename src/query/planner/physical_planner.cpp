#include "physical_planner.h"

#include <memory>

#include "catalog/catalog.h"
#include "common/error.h"
#include "physical_plan_node.h"

using namespace pond::common;
using namespace pond::catalog;

namespace pond::query {

PhysicalPlanner::PhysicalPlanner(Catalog& catalog) : catalog_(catalog) {}

Result<std::shared_ptr<PhysicalPlanNode>> PhysicalPlanner::Plan(std::shared_ptr<LogicalPlanNode> logical_plan) {
    switch (logical_plan->Type()) {
        case LogicalNodeType::Scan:
            return PlanScan(logical_plan->as<LogicalScanNode>(), nullptr, std::nullopt);
        case LogicalNodeType::Filter:
            return PlanFilter(logical_plan->as<LogicalFilterNode>());
        case LogicalNodeType::Projection:
            return PlanProjection(logical_plan->as<LogicalProjectionNode>());
        case LogicalNodeType::Join:
            return PlanJoin(logical_plan->as<LogicalJoinNode>());
        case LogicalNodeType::Aggregate:
            return PlanAggregate(logical_plan->as<LogicalAggregateNode>());
        case LogicalNodeType::Sort:
            return PlanSort(logical_plan->as<LogicalSortNode>());
        case LogicalNodeType::Limit:
            return PlanLimit(logical_plan->as<LogicalLimitNode>());
        default:
            return Result<std::shared_ptr<PhysicalPlanNode>>(
                Error(ErrorCode::InvalidArgument, "Unknown logical node type"));
    }
}

Result<std::shared_ptr<PhysicalPlanNode>> PhysicalPlanner::PlanScan(LogicalScanNode* node,
                                                                    std::shared_ptr<Expression> pushed_predicate,
                                                                    std::optional<common::Schema> projection_schema) {
    using ReturnType = Result<std::shared_ptr<PhysicalPlanNode>>;
    // Check if we can use an index scan
    auto table_metadata_result = catalog_.LoadTable(node->TableName());
    RETURN_IF_ERROR_T(ReturnType, table_metadata_result);

    // Create scan node with pushed down operations
    auto scan_node = std::make_shared<PhysicalSequentialScanNode>(
        node->TableName(), node->OutputSchema(), pushed_predicate, projection_schema);

    return Result<std::shared_ptr<PhysicalPlanNode>>::success(scan_node);
}

Result<std::shared_ptr<PhysicalPlanNode>> PhysicalPlanner::PlanFilter(LogicalFilterNode* node) {
    // If child is a projection and its child is a scan, we can push both filter and projection down
    if (node->Children()[0]->Type() == LogicalNodeType::Projection) {
        auto projection = node->Children()[0]->as<LogicalProjectionNode>();
        if (projection->Children()[0]->Type() == LogicalNodeType::Scan) {
            auto scan = projection->Children()[0]->as<LogicalScanNode>();
            return PlanScan(scan, node->GetCondition(), projection->OutputSchema());
        }
    }

    // If child is a scan, push filter down
    if (node->Children()[0]->Type() == LogicalNodeType::Scan) {
        auto scan = node->Children()[0]->as<LogicalScanNode>();
        return PlanScan(scan, node->GetCondition(), std::nullopt);
    }

    // Otherwise plan child first
    auto child_result = Plan(node->Children()[0]);
    if (!child_result.ok()) {
        return child_result.error();
    }

    // Create filter node
    auto filter_node = std::make_shared<PhysicalFilterNode>(node->GetCondition(), node->OutputSchema());
    filter_node->AddChild(child_result.value());
    return Result<std::shared_ptr<PhysicalPlanNode>>::success(filter_node);
}

Result<std::shared_ptr<PhysicalPlanNode>> PhysicalPlanner::PlanProjection(LogicalProjectionNode* node) {
    // If child is a filter and its child is a scan, push both down
    if (node->Children()[0]->Type() == LogicalNodeType::Filter) {
        auto filter = node->Children()[0]->as<LogicalFilterNode>();
        if (filter->Children()[0]->Type() == LogicalNodeType::Scan) {
            auto scan = filter->Children()[0]->as<LogicalScanNode>();
            return PlanScan(scan, filter->GetCondition(), node->OutputSchema());
        }
    }

    // If child is a scan, push projection down
    if (node->Children()[0]->Type() == LogicalNodeType::Scan) {
        auto scan = node->Children()[0]->as<LogicalScanNode>();
        return PlanScan(scan, nullptr, node->OutputSchema());
    }

    // Otherwise plan child first
    auto child_result = Plan(node->Children()[0]);
    if (!child_result.ok()) {
        return child_result.error();
    }

    // Create projection node
    auto projection_node = std::make_shared<PhysicalProjectionNode>(node->GetExpressions(), node->OutputSchema());
    projection_node->AddChild(child_result.value());
    return Result<std::shared_ptr<PhysicalPlanNode>>::success(projection_node);
}

Result<std::shared_ptr<PhysicalPlanNode>> PhysicalPlanner::PlanJoin(LogicalJoinNode* node) {
    // Plan children first
    auto left_result = Plan(node->Children()[0]);
    if (!left_result.ok()) {
        return left_result.error();
    }

    auto right_result = Plan(node->Children()[1]);
    if (!right_result.ok()) {
        return right_result.error();
    }

    // Choose join algorithm based on join condition and table statistics
    // TODO: Implement cost-based join selection
    bool use_hash_join = true;  // For now, always use hash join
    std::shared_ptr<PhysicalPlanNode> join_node;

    if (use_hash_join) {
        join_node = std::make_shared<PhysicalHashJoinNode>(node->GetCondition(), node->OutputSchema());
    } else {
        join_node = std::make_shared<PhysicalNestedLoopJoinNode>(node->GetCondition(), node->OutputSchema());
    }

    join_node->AddChild(left_result.value());
    join_node->AddChild(right_result.value());

    // Add shuffle exchange for distributed execution if needed
    if (NeedsShuffle(node)) {
        auto exchange_node = std::make_shared<PhysicalShuffleExchangeNode>(
            std::vector<std::shared_ptr<Expression>>{node->GetCondition()}, node->OutputSchema());
        exchange_node->AddChild(join_node);
        return Result<std::shared_ptr<PhysicalPlanNode>>::success(exchange_node);
    }

    return join_node;
}

Result<std::shared_ptr<PhysicalPlanNode>> PhysicalPlanner::PlanAggregate(LogicalAggregateNode* node) {
    // Plan child first
    auto child_result = Plan(node->Children()[0]);
    if (!child_result.ok()) {
        return child_result.error();
    }

    // Create hash aggregate node
    auto aggregate_node =
        std::make_shared<PhysicalHashAggregateNode>(node->GetGroupBy(), node->GetAggregates(), node->OutputSchema());
    aggregate_node->AddChild(child_result.value());

    // Add shuffle exchange for distributed execution if needed
    if (NeedsShuffle(node)) {
        auto exchange_node = std::make_shared<PhysicalShuffleExchangeNode>(node->GetGroupBy(), node->OutputSchema());
        exchange_node->AddChild(aggregate_node);
        return Result<std::shared_ptr<PhysicalPlanNode>>::success(exchange_node);
    }

    return Result<std::shared_ptr<PhysicalPlanNode>>::success(aggregate_node);
}

Result<std::shared_ptr<PhysicalPlanNode>> PhysicalPlanner::PlanSort(LogicalSortNode* node) {
    // Plan child first
    auto child_result = Plan(node->Children()[0]);
    if (!child_result.ok()) {
        return child_result.error();
    }

    // Create sort node
    auto sort_node = std::make_shared<PhysicalSortNode>(node->GetSortSpecs(), node->OutputSchema());
    sort_node->AddChild(child_result.value());
    return Result<std::shared_ptr<PhysicalPlanNode>>::success(sort_node);
}

Result<std::shared_ptr<PhysicalPlanNode>> PhysicalPlanner::PlanLimit(LogicalLimitNode* node) {
    // Plan child first
    auto child_result = Plan(node->Children()[0]);
    if (!child_result.ok()) {
        return child_result.error();
    }

    // Create limit node
    auto limit_node = std::make_shared<PhysicalLimitNode>(node->Limit(), node->Offset(), node->OutputSchema());
    limit_node->AddChild(child_result.value());
    return Result<std::shared_ptr<PhysicalPlanNode>>::success(limit_node);
}

bool PhysicalPlanner::NeedsShuffle(LogicalPlanNode* node) {
    // TODO: Implement proper logic to determine if shuffling is needed
    // For now, always shuffle for joins and aggregates in distributed mode
    return (node->Type() == LogicalNodeType::Join || node->Type() == LogicalNodeType::Aggregate) && IsDistributed();
}

bool PhysicalPlanner::IsDistributed() const {
    // TODO: Implement proper check for distributed mode
    return false;  // For now, assume we're always in distributed mode
}

}  // namespace pond::query