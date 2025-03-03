#include "physical_plan_printer.h"

namespace pond::query {

void PhysicalPlanPrinter::Visit(PhysicalSequentialScanNode& node) {
    output_ << GetIndent() << "SequentialScan(" << node.TableName() << ", " << node.OutputSchema().ToString() << ")";
    if (node.Predicate()) {
        output_ << GetIndent() << " Predicate: " << node.Predicate()->ToString();
    }
    output_ << std::endl;
}

void PhysicalPlanPrinter::Visit(PhysicalIndexScanNode& node) {
    output_ << GetIndent() << "IndexScan(" << node.TableName() << ", " << node.IndexName() << ", "
            << node.OutputSchema().ToString() << ")" << std::endl;
}

void PhysicalPlanPrinter::Visit(PhysicalFilterNode& node) {
    output_ << GetIndent() << "Filter(" << node.Predicate()->ToString() << ", " << node.OutputSchema().ToString() << ")"
            << std::endl;

    indent_level_++;
    for (const auto& child : node.Children()) {
        child->Accept(*this);
    }
    indent_level_--;
}

void PhysicalPlanPrinter::Visit(PhysicalProjectionNode& node) {
    output_ << GetIndent() << "Projection(" << node.OutputSchema().ToString() << ")" << std::endl;

    indent_level_++;
    for (const auto& child : node.Children()) {
        child->Accept(*this);
    }
    indent_level_--;
}

void PhysicalPlanPrinter::Visit(PhysicalHashJoinNode& node) {
    output_ << GetIndent() << "HashJoin(" << node.Condition()->ToString() << ", " << node.OutputSchema().ToString()
            << ")" << std::endl;

    indent_level_++;
    for (const auto& child : node.Children()) {
        child->Accept(*this);
    }
    indent_level_--;
}

void PhysicalPlanPrinter::Visit(PhysicalNestedLoopJoinNode& node) {
    output_ << GetIndent() << "NestedLoopJoin(" << node.Condition()->ToString() << ", "
            << node.OutputSchema().ToString() << ")" << std::endl;

    indent_level_++;
    for (const auto& child : node.Children()) {
        child->Accept(*this);
    }
    indent_level_--;
}

void PhysicalPlanPrinter::Visit(PhysicalHashAggregateNode& node) {
    output_ << GetIndent() << "HashAggregate(" << node.OutputSchema().ToString() << ")" << std::endl;

    indent_level_++;
    for (const auto& child : node.Children()) {
        child->Accept(*this);
    }
    indent_level_--;
}

void PhysicalPlanPrinter::Visit(PhysicalSortNode& node) {
    output_ << GetIndent() << "Sort(" << node.OutputSchema().ToString() << ")" << std::endl;

    indent_level_++;
    for (const auto& child : node.Children()) {
        child->Accept(*this);
    }
    indent_level_--;
}

void PhysicalPlanPrinter::Visit(PhysicalLimitNode& node) {
    output_ << GetIndent() << "Limit(" << node.Limit() << ", " << node.Offset() << ", "
            << node.OutputSchema().ToString() << ")" << std::endl;

    indent_level_++;
    for (const auto& child : node.Children()) {
        child->Accept(*this);
    }
    indent_level_--;
}

void PhysicalPlanPrinter::Visit(PhysicalShuffleExchangeNode& node) {
    output_ << GetIndent() << "ShuffleExchange(" << node.OutputSchema().ToString() << ")" << std::endl;

    indent_level_++;
    for (const auto& child : node.Children()) {
        child->Accept(*this);
    }
    indent_level_--;
}

}  // namespace pond::query