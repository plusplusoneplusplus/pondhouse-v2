#include "physical_plan_node.h"

#include "physical_plan_printer.h"
#include "query/executor/executor.h"

using namespace pond::common;

namespace pond::query {

std::string PhysicalNodeTypeToString(PhysicalNodeType type) {
    switch (type) {
        case PhysicalNodeType::SequentialScan:
            return "SequentialScan";
        case PhysicalNodeType::IndexScan:
            return "IndexScan";
        case PhysicalNodeType::Filter:
            return "Filter";
        case PhysicalNodeType::Projection:
            return "Projection";
        case PhysicalNodeType::HashJoin:
            return "HashJoin";
        case PhysicalNodeType::NestedLoopJoin:
            return "NestedLoopJoin";
        case PhysicalNodeType::HashAggregate:
            return "HashAggregate";
        case PhysicalNodeType::SortAggregate:
            return "SortAggregate";
        case PhysicalNodeType::Sort:
            return "Sort";
        case PhysicalNodeType::Limit:
            return "Limit";
        case PhysicalNodeType::ShuffleExchange:
            return "ShuffleExchange";
        default:
            return "Unknown";
    }
}

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

}  // namespace pond::query