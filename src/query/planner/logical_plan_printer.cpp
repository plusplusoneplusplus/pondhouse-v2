#include "logical_plan_printer.h"

#include "logical_plan_node.h"

namespace pond::query {

void LogicalPlanPrinter::Visit(LogicalScanNode& node) {
    output_ << GetIndent() << "Scan(" << node.TableName() << ", " << node.OutputSchema().ToString() << ")\n";
}

void LogicalPlanPrinter::Visit(LogicalFilterNode& node) {
    output_ << GetIndent() << "Filter(" << node.GetCondition()->ToString() << ")\n";

    indent_level_++;
    node.Children()[0]->Accept(*this);
    indent_level_--;
}

void LogicalPlanPrinter::Visit(LogicalJoinNode& node) {
    output_ << GetIndent() << "Join("
            << "type=" << JoinTypeToString(node.GetJoinType());
    if (node.GetCondition()) {
        output_ << ", on=" << node.GetCondition()->ToString();
    }
    output_ << ")\n";

    indent_level_++;
    // Visit left child
    node.Children()[0]->Accept(*this);
    // Visit right child
    node.Children()[1]->Accept(*this);
    indent_level_--;
}

void LogicalPlanPrinter::Visit(LogicalProjectionNode& node) {
    output_ << GetIndent() << "Projection(" << node.TableName() << ", ";
    bool first = true;
    for (const auto& expr : node.GetExpressions()) {
        if (!first)
            output_ << ", ";
        output_ << expr->ToString();
        first = false;
    }
    output_ << ") " << node.OutputSchema().ToString() << "\n";

    indent_level_++;
    node.Children()[0]->Accept(*this);
    indent_level_--;
}

void LogicalPlanPrinter::Visit(LogicalAggregateNode& node) {
    output_ << GetIndent() << "Aggregate(";

    // Print group by expressions
    if (!node.GetGroupBy().empty()) {
        output_ << "GROUP BY: ";
        bool first = true;
        for (const auto& expr : node.GetGroupBy()) {
            if (!first)
                output_ << ", ";
            output_ << expr->ToString();
            first = false;
        }
    }

    // Print aggregate expressions
    if (!node.GetAggregates().empty()) {
        if (!node.GetGroupBy().empty())
            output_ << ", ";
        output_ << "AGG: ";
        bool first = true;
        for (const auto& expr : node.GetAggregates()) {
            if (!first)
                output_ << ", ";
            output_ << expr->ToString();
            first = false;
        }
    }
    output_ << ") " << node.OutputSchema().ToString() << "\n";

    indent_level_++;
    node.Children()[0]->Accept(*this);
    indent_level_--;
}

void LogicalPlanPrinter::Visit(LogicalSortNode& node) {
    output_ << GetIndent() << "Sort(";
    bool first = true;
    for (const auto& spec : node.GetSortSpecs()) {
        if (!first)
            output_ << ", ";
        output_ << spec.ToString();
        first = false;
    }
    output_ << ")\n";

    indent_level_++;
    node.Children()[0]->Accept(*this);
    indent_level_--;
}

void LogicalPlanPrinter::Visit(LogicalLimitNode& node) {
    output_ << GetIndent() << "Limit(" << node.Limit();
    if (node.Offset() > 0) {
        output_ << " OFFSET " << node.Offset();
    }
    output_ << ")\n";

    indent_level_++;
    node.Children()[0]->Accept(*this);
    indent_level_--;
}

std::string LogicalPlanPrinter::JoinTypeToString(JoinType type) {
    switch (type) {
        case JoinType::Inner:
            return "INNER";
        case JoinType::Left:
            return "LEFT";
        case JoinType::Right:
            return "RIGHT";
        case JoinType::Full:
            return "FULL";
        case JoinType::Cross:
            return "CROSS";
        default:
            return "UNKNOWN";
    }
}

}  // namespace pond::query