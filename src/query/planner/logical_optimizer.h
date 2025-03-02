#pragma once

#include <memory>
#include <vector>

#include "common/result.h"
#include "logical_plan_node.h"

namespace pond::query {

// Forward declarations
class OptimizationRule;

struct LogicalOptimizerOptions {
    bool enable_predicate_pushdown = true;
    bool enable_column_pruning = true;
    bool enable_constant_folding = true;
    bool enable_join_reordering = true;
    bool enable_projection_pushdown = true;
    bool enable_limit_pushdown = true;

    static const LogicalOptimizerOptions& OnlyPredicatePushdown() {
        static LogicalOptimizerOptions options = {
            .enable_predicate_pushdown = true,
            .enable_column_pruning = false,
            .enable_constant_folding = false,
            .enable_join_reordering = false,
            .enable_projection_pushdown = false,
            .enable_limit_pushdown = false,
        };
        return options;
    }

    static const LogicalOptimizerOptions& OnlyColumnPruning() {
        static LogicalOptimizerOptions options = {
            .enable_predicate_pushdown = false,
            .enable_column_pruning = true,
            .enable_constant_folding = false,
            .enable_join_reordering = false,
            .enable_projection_pushdown = false,
            .enable_limit_pushdown = false,
        };
        return options;
    }

    static const LogicalOptimizerOptions& OnlyProjectionPushdown() {
        static LogicalOptimizerOptions options = {
            .enable_predicate_pushdown = false,
            .enable_column_pruning = false,
            .enable_constant_folding = false,
            .enable_join_reordering = false,
            .enable_projection_pushdown = true,
            .enable_limit_pushdown = false,
        };
        return options;
    }

    static const LogicalOptimizerOptions& OnlyJoinReordering() {
        static LogicalOptimizerOptions options = {
            .enable_predicate_pushdown = false,
            .enable_column_pruning = false,
            .enable_constant_folding = false,
            .enable_join_reordering = true,
            .enable_projection_pushdown = false,
            .enable_limit_pushdown = false,
        };
        return options;
    }

    static const LogicalOptimizerOptions& OnlyConstantFolding() {
        static LogicalOptimizerOptions options = {
            .enable_predicate_pushdown = false,
            .enable_column_pruning = false,
            .enable_constant_folding = true,
            .enable_join_reordering = false,
            .enable_projection_pushdown = false,
            .enable_limit_pushdown = false,
        };
        return options;
    }

    static const LogicalOptimizerOptions& OnlyLimitPushdown() {
        static LogicalOptimizerOptions options = {
            .enable_predicate_pushdown = false,
            .enable_column_pruning = false,
            .enable_constant_folding = false,
            .enable_join_reordering = false,
            .enable_projection_pushdown = false,
            .enable_limit_pushdown = true,
        };
        return options;
    }
};

/**
 * @brief Logical plan optimizer that applies optimization rules to transform the logical plan
 * Common optimizations include:
 * 1. Predicate pushdown - Push filters down closer to scans
 * 2. Column pruning - Only read needed columns
 * 3. Constant folding - Evaluate constant expressions at compile time
 * 4. Join reordering - Reorder joins based on cardinality and filter selectivity
 */
class LogicalOptimizer {
public:
    explicit LogicalOptimizer(const LogicalOptimizerOptions& options = LogicalOptimizerOptions());
    ~LogicalOptimizer();

    // Apply all optimization rules to the logical plan
    common::Result<std::shared_ptr<LogicalPlanNode>> Optimize(std::shared_ptr<LogicalPlanNode> plan);

private:
    // Apply a single optimization rule
    common::Result<std::shared_ptr<LogicalPlanNode>> ApplyRule(OptimizationRule& rule,
                                                               std::shared_ptr<LogicalPlanNode> plan);

private:
    std::vector<std::unique_ptr<OptimizationRule>> rules_;
};

class LogicalOptimizerHelper {
public:
    static void CollectColumnsFromExpression(const std::shared_ptr<Expression>& expr,
                                             std::vector<std::string>& columns);
};

/**
 * @brief Base class for optimization rules
 */
class OptimizationRule {
public:
    virtual ~OptimizationRule() = default;
    virtual common::Result<std::shared_ptr<LogicalPlanNode>> Apply(std::shared_ptr<LogicalPlanNode> plan) = 0;
    virtual std::string Name() const = 0;
};

/**
 * @brief Push filter operations down the plan tree to reduce the amount of data processed
 */
class PredicatePushdown : public OptimizationRule {
public:
    common::Result<std::shared_ptr<LogicalPlanNode>> Apply(std::shared_ptr<LogicalPlanNode> plan) override;
    std::string Name() const override { return "PredicatePushdown"; }

private:
    common::Result<std::shared_ptr<LogicalPlanNode>> PushFilter(std::shared_ptr<LogicalFilterNode> filter,
                                                                std::shared_ptr<LogicalPlanNode> child);
    bool CanPushThroughJoin(const std::shared_ptr<Expression>& condition, const LogicalJoinNode& join, bool to_left);
    void SplitConjunctivePredicatesWithAnd(const std::shared_ptr<Expression>& expr,
                                           std::vector<std::shared_ptr<Expression>>& predicates);
    std::shared_ptr<Expression> CombinePredicatesWithAnd(const std::vector<std::shared_ptr<Expression>>& predicates);
};

/**
 * @brief Prune unnecessary columns from scan operations
 */
class ColumnPruning : public OptimizationRule {
public:
    common::Result<std::shared_ptr<LogicalPlanNode>> Apply(std::shared_ptr<LogicalPlanNode> plan) override;
    std::string Name() const override { return "ColumnPruning"; }

private:
    std::vector<std::string> CollectRequiredColumns(const LogicalPlanNode& node);
    common::Result<std::shared_ptr<LogicalPlanNode>> PruneColumns(std::shared_ptr<LogicalPlanNode> node,
                                                                  const std::vector<std::string>& required_columns);
};

/**
 * @brief Evaluate constant expressions at compile time
 */
class ConstantFolding : public OptimizationRule {
public:
    common::Result<std::shared_ptr<LogicalPlanNode>> Apply(std::shared_ptr<LogicalPlanNode> plan) override;
    std::string Name() const override { return "ConstantFolding"; }

private:
    bool IsConstantExpression(const std::shared_ptr<Expression>& expr);
    std::shared_ptr<Expression> EvaluateConstantExpression(const std::shared_ptr<Expression>& expr);
    std::shared_ptr<Expression> FoldConstants(const std::shared_ptr<Expression>& expr);
    bool MatchLikePattern(const std::string& str, const std::string& pattern);
};

/**
 * @brief Reorder joins based on cardinality and filter selectivity
 */
class JoinReordering : public OptimizationRule {
public:
    common::Result<std::shared_ptr<LogicalPlanNode>> Apply(std::shared_ptr<LogicalPlanNode> plan) override;
    std::string Name() const override { return "JoinReordering"; }

private:
    double EstimateCardinality(const LogicalPlanNode& node);
    double EstimateSelectivity(const std::string& condition);
    common::Result<std::shared_ptr<LogicalPlanNode>> ReorderJoins(std::shared_ptr<LogicalJoinNode> join);
    std::shared_ptr<Expression> SwapJoinCondition(const std::shared_ptr<Expression>& condition);
};

/**
 * @brief Push projection operations down the plan tree to reduce the number of columns processed
 */
class ProjectionPushdown : public OptimizationRule {
public:
    common::Result<std::shared_ptr<LogicalPlanNode>> Apply(std::shared_ptr<LogicalPlanNode> plan) override;
    std::string Name() const override { return "ProjectionPushdown"; }

private:
    common::Result<std::shared_ptr<LogicalPlanNode>> PushProjection(std::shared_ptr<LogicalProjectionNode> projection,
                                                                    std::shared_ptr<LogicalPlanNode> child);
    bool CanPushThroughJoin(const std::vector<std::shared_ptr<Expression>>& exprs,
                            const LogicalJoinNode& join,
                            bool to_left);
    std::vector<std::shared_ptr<Expression>> ExtractProjectionExprs(const std::shared_ptr<LogicalPlanNode>& node);
    std::shared_ptr<LogicalPlanNode> CreateProjection(std::shared_ptr<LogicalPlanNode> input,
                                                      const std::vector<std::shared_ptr<Expression>>& exprs,
                                                      const std::string& table_name);
};

// Limit pushdown optimization
class LimitPushdown : public OptimizationRule {
public:
    common::Result<std::shared_ptr<LogicalPlanNode>> Apply(std::shared_ptr<LogicalPlanNode> plan) override;
    std::string Name() const override { return "LimitPushdown"; }
};

}  // namespace pond::query
