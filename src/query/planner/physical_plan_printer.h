#pragma once

#include <memory>
#include <sstream>
#include <string>

#include "physical_plan_node.h"

namespace pond::query {

/**
 * @brief Visitor for printing physical plan nodes in a structured format
 */
class PhysicalPlanPrinter : public PhysicalPlanVisitor {
public:
    // Visit methods for different node types
    void Visit(PhysicalSequentialScanNode& node) override;
    void Visit(PhysicalIndexScanNode& node) override;
    void Visit(PhysicalFilterNode& node) override;
    void Visit(PhysicalProjectionNode& node) override;
    void Visit(PhysicalHashJoinNode& node) override;
    void Visit(PhysicalNestedLoopJoinNode& node) override;
    void Visit(PhysicalHashAggregateNode& node) override;
    void Visit(PhysicalSortNode& node) override;
    void Visit(PhysicalLimitNode& node) override;
    void Visit(PhysicalShuffleExchangeNode& node) override;

    std::string ToString() const { return output_.str(); }

    std::string GetIndent() const { return std::string(indent_level_ * 2, ' '); }

private:
    std::stringstream output_;
    int indent_level_ = 0;
};

}  // namespace pond::query