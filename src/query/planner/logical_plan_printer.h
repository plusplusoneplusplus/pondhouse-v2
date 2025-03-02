#pragma once

#include "logical_plan_node.h"

namespace pond::query {

class LogicalPlanPrinter : public LogicalPlanVisitor {
public:
    void Visit(LogicalScanNode& node) override;
    void Visit(LogicalFilterNode& node) override;
    void Visit(LogicalJoinNode& node) override;
    void Visit(LogicalProjectionNode& node) override;
    void Visit(LogicalAggregateNode& node) override;
    void Visit(LogicalSortNode& node) override;
    void Visit(LogicalLimitNode& node) override;

    std::string ToString() const { return output_.str(); }

    std::string GetIndent() const { return std::string(indent_level_ * 2, ' '); }

    std::string JoinTypeToString(JoinType type);

private:
    std::stringstream output_;
    int indent_level_ = 0;
};

}  // namespace pond::query