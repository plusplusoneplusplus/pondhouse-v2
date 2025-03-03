#include "physical_plan_node.h"

#include "physical_plan_printer.h"
#include "query/executor/executor.h"

using namespace pond::common;

namespace pond::query {

void PhysicalSequentialScanNode::Accept(PhysicalPlanVisitor& visitor) {
    visitor.Visit(*this);
}

void PhysicalIndexScanNode::Accept(PhysicalPlanVisitor& visitor) {
    visitor.Visit(*this);
}

void PhysicalFilterNode::Accept(PhysicalPlanVisitor& visitor) {
    visitor.Visit(*this);
}

void PhysicalProjectionNode::Accept(PhysicalPlanVisitor& visitor) {
    visitor.Visit(*this);
}

void PhysicalHashJoinNode::Accept(PhysicalPlanVisitor& visitor) {
    visitor.Visit(*this);
}

void PhysicalNestedLoopJoinNode::Accept(PhysicalPlanVisitor& visitor) {
    visitor.Visit(*this);
}

void PhysicalHashAggregateNode::Accept(PhysicalPlanVisitor& visitor) {
    visitor.Visit(*this);
}

void PhysicalSortNode::Accept(PhysicalPlanVisitor& visitor) {
    visitor.Visit(*this);
}

void PhysicalLimitNode::Accept(PhysicalPlanVisitor& visitor) {
    visitor.Visit(*this);
}

void PhysicalShuffleExchangeNode::Accept(PhysicalPlanVisitor& visitor) {
    visitor.Visit(*this);
}

std::string GetPhysicalPlanUserFriendlyString(PhysicalPlanNode& root) {
    PhysicalPlanPrinter printer;
    root.Accept(printer);
    return printer.ToString();
}

Result<DataBatchSharedPtr> PhysicalSequentialScanNode::Execute(Executor& executor) {
    // Accept visitor to execute this node
    executor.Visit(*this);
    return executor.CurrentBatch();
}

Result<DataBatchSharedPtr> PhysicalIndexScanNode::Execute(Executor& executor) {
    // Accept visitor to execute this node
    executor.Visit(*this);
    return Result<DataBatchSharedPtr>::failure(ErrorCode::NotImplemented, "Index scan not implemented");
}

Result<DataBatchSharedPtr> PhysicalFilterNode::Execute(Executor& executor) {
    // Execute child first
    auto result = children_[0]->Execute(executor);
    if (!result.ok()) {
        return result;
    }

    // Accept visitor to execute this node
    executor.Visit(*this);
    return Result<DataBatchSharedPtr>::failure(ErrorCode::NotImplemented, "Filter not implemented");
}

Result<DataBatchSharedPtr> PhysicalProjectionNode::Execute(Executor& executor) {
    // Execute child first
    auto result = children_[0]->Execute(executor);
    if (!result.ok()) {
        return result;
    }

    // Accept visitor to execute this node
    executor.Visit(*this);
    return executor.CurrentBatch();
}

Result<DataBatchSharedPtr> PhysicalHashJoinNode::Execute(Executor& executor) {
    // Accept visitor to execute this node
    executor.Visit(*this);
    return executor.CurrentBatch();
}

Result<DataBatchSharedPtr> PhysicalNestedLoopJoinNode::Execute(Executor& executor) {
    // Accept visitor to execute this node
    executor.Visit(*this);
    return Result<DataBatchSharedPtr>::failure(ErrorCode::NotImplemented, "Nested loop join not implemented");
}

Result<DataBatchSharedPtr> PhysicalHashAggregateNode::Execute(Executor& executor) {
    // Execute child first
    auto result = children_[0]->Execute(executor);
    if (!result.ok()) {
        return result;
    }

    // Accept visitor to execute this node
    executor.Visit(*this);
    return executor.CurrentBatch();
}

Result<DataBatchSharedPtr> PhysicalSortNode::Execute(Executor& executor) {
    // Execute child first
    auto result = children_[0]->Execute(executor);
    if (!result.ok()) {
        return result;
    }

    // Accept visitor to execute this node
    executor.Visit(*this);
    return Result<DataBatchSharedPtr>::failure(ErrorCode::NotImplemented, "Sort not implemented");
}

Result<DataBatchSharedPtr> PhysicalLimitNode::Execute(Executor& executor) {
    // Execute child first
    auto result = children_[0]->Execute(executor);
    if (!result.ok()) {
        return result;
    }

    // Accept visitor to execute this node
    executor.Visit(*this);
    return executor.CurrentBatch();
}

Result<DataBatchSharedPtr> PhysicalShuffleExchangeNode::Execute(Executor& executor) {
    // Execute child first
    auto result = children_[0]->Execute(executor);
    if (!result.ok()) {
        return result;
    }

    // Accept visitor to execute this node
    executor.Visit(*this);
    return Result<DataBatchSharedPtr>::failure(ErrorCode::NotImplemented, "Shuffle exchange not implemented");
}

}  // namespace pond::query