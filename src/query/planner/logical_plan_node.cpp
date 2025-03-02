#include "logical_plan_node.h"

#include "logical_plan_printer.h"

namespace pond::query {

void LogicalScanNode::Accept(LogicalPlanVisitor& visitor) {
    visitor.Visit(*this);
}

void LogicalFilterNode::Accept(LogicalPlanVisitor& visitor) {
    visitor.Visit(*this);
}

void LogicalProjectionNode::Accept(LogicalPlanVisitor& visitor) {
    visitor.Visit(*this);
}

void LogicalJoinNode::Accept(LogicalPlanVisitor& visitor) {
    visitor.Visit(*this);
}

void LogicalAggregateNode::Accept(LogicalPlanVisitor& visitor) {
    visitor.Visit(*this);
}

void LogicalSortNode::Accept(LogicalPlanVisitor& visitor) {
    visitor.Visit(*this);
}

void LogicalLimitNode::Accept(LogicalPlanVisitor& visitor) {
    visitor.Visit(*this);
}

std::string GetLogicalPlanUserFriendlyString(LogicalPlanNode& root) {
    LogicalPlanPrinter printer;
    root.Accept(printer);
    return printer.ToString();
}

}  // namespace pond::query
