#include "logical_optimizer.h"

#include <regex>
#include <set>
#include <sstream>

using namespace pond::common;

namespace pond::query {

LogicalOptimizer::LogicalOptimizer(const LogicalOptimizerOptions& options) {
    // Add optimization rules in the order they should be applied
    if (options.enable_predicate_pushdown) {
        rules_.push_back(std::make_unique<PredicatePushdown>());
    }
    if (options.enable_column_pruning) {
        rules_.push_back(std::make_unique<ColumnPruning>());
    }
    if (options.enable_constant_folding) {
        rules_.push_back(std::make_unique<ConstantFolding>());
    }
    if (options.enable_join_reordering) {
        rules_.push_back(std::make_unique<JoinReordering>());
    }
    if (options.enable_projection_pushdown) {
        rules_.push_back(std::make_unique<ProjectionPushdown>());
    }
    if (options.enable_limit_pushdown) {
        rules_.push_back(std::make_unique<LimitPushdown>());
    }
}

LogicalOptimizer::~LogicalOptimizer() = default;

Result<std::shared_ptr<LogicalPlanNode>> LogicalOptimizer::Optimize(std::shared_ptr<LogicalPlanNode> plan) {
    auto current = plan;
    for (const auto& rule : rules_) {
        auto result = ApplyRule(*rule, current);
        if (!result.ok()) {
            return result;
        }
        current = result.value();
    }
    return Result<std::shared_ptr<LogicalPlanNode>>::success(current);
}

Result<std::shared_ptr<LogicalPlanNode>> LogicalOptimizer::ApplyRule(OptimizationRule& rule,
                                                                     std::shared_ptr<LogicalPlanNode> plan) {
    return rule.Apply(plan);
}

// PredicatePushdown implementation
Result<std::shared_ptr<LogicalPlanNode>> PredicatePushdown::Apply(std::shared_ptr<LogicalPlanNode> plan) {
    if (!plan) {
        return Result<std::shared_ptr<LogicalPlanNode>>::failure(ErrorCode::InvalidArgument, "Null plan");
    }

    if (plan->Type() == LogicalNodeType::Filter) {
        auto filter = std::static_pointer_cast<LogicalFilterNode>(plan);
        return PushFilter(filter, filter->Children()[0]);
    }

    // Recursively apply to children
    std::vector<std::shared_ptr<LogicalPlanNode>> new_children;
    for (const auto& child : plan->Children()) {
        auto result = Apply(child);
        if (!result.ok()) {
            return result;
        }
        new_children.push_back(result.value());
    }

    // Create new node with optimized children
    switch (plan->Type()) {
        case LogicalNodeType::Projection: {
            auto proj = std::static_pointer_cast<LogicalProjectionNode>(plan);
            return Result<std::shared_ptr<LogicalPlanNode>>::success(std::make_shared<LogicalProjectionNode>(
                new_children[0], proj->OutputSchema(), proj->GetExpressions(), proj->TableName()));
        }
        case LogicalNodeType::Join: {
            auto join = std::static_pointer_cast<LogicalJoinNode>(plan);
            return Result<std::shared_ptr<LogicalPlanNode>>::success(std::make_shared<LogicalJoinNode>(
                new_children[0], new_children[1], join->GetJoinType(), join->GetCondition(), join->OutputSchema()));
        }
        default:
            return Result<std::shared_ptr<LogicalPlanNode>>::success(plan);
    }
}

/**
 * @brief Push filter node above scan node in logical plan
 * @param filter Filter node to push
 * @param child Child node to push filter above
 * @return New logical plan node with filter pushed above child
 */
Result<std::shared_ptr<LogicalPlanNode>> PredicatePushdown::PushFilter(std::shared_ptr<LogicalFilterNode> filter,
                                                                       std::shared_ptr<LogicalPlanNode> child) {
    switch (child->Type()) {
        case LogicalNodeType::Scan:
            // Keep filter above scan since scan is a leaf node in logical plan
            return Result<std::shared_ptr<LogicalPlanNode>>::success(filter);

        case LogicalNodeType::Join: {
            // Push filter to join node
            auto join = std::static_pointer_cast<LogicalJoinNode>(child);

            // Split conjunctive predicates
            std::vector<std::shared_ptr<Expression>> predicates;
            SplitConjunctivePredicatesWithAnd(filter->GetCondition(), predicates);

            std::vector<std::shared_ptr<Expression>> left_predicates;
            std::vector<std::shared_ptr<Expression>> right_predicates;
            std::vector<std::shared_ptr<Expression>> remaining_predicates;

            // Categorize each predicate
            for (const auto& pred : predicates) {
                if (CanPushThroughJoin(pred, *join, true /* to_left */)) {
                    left_predicates.push_back(pred);
                } else if (CanPushThroughJoin(pred, *join, false /* to_left */)) {
                    right_predicates.push_back(pred);
                } else {
                    remaining_predicates.push_back(pred);
                }
            }

            // Create new children with pushed predicates
            auto new_left = join->Children()[0];
            if (!left_predicates.empty()) {
                auto left_condition = CombinePredicatesWithAnd(left_predicates);
                new_left = std::make_shared<LogicalFilterNode>(new_left, left_condition);
            }

            auto new_right = join->Children()[1];
            if (!right_predicates.empty()) {
                auto right_condition = CombinePredicatesWithAnd(right_predicates);
                new_right = std::make_shared<LogicalFilterNode>(new_right, right_condition);
            }

            // Create new join with remaining predicates
            auto new_join = std::make_shared<LogicalJoinNode>(
                new_left, new_right, join->GetJoinType(), join->GetCondition(), join->OutputSchema());

            if (remaining_predicates.empty()) {
                return Result<std::shared_ptr<LogicalPlanNode>>::success(new_join);
            }

            auto remaining_condition = CombinePredicatesWithAnd(remaining_predicates);
            return Result<std::shared_ptr<LogicalPlanNode>>::success(
                std::make_shared<LogicalFilterNode>(new_join, remaining_condition));
        }

        default:
            return Result<std::shared_ptr<LogicalPlanNode>>::success(filter);
    }
}

/**
 * @brief Split a conjunctive predicate into a list of individual predicates
 * @details This function recursively splits a conjunctive predicate into a list of individual predicates.
 * Only AND operations are supported. An example of a conjunctive predicate is:
 * @code
 * age > 25 AND salary < 75000
 * @endcode
 * @param expr Predicate to split
 * @param predicates List to store individual predicates
 */
void PredicatePushdown::SplitConjunctivePredicatesWithAnd(const std::shared_ptr<Expression>& expr,
                                                          std::vector<std::shared_ptr<Expression>>& predicates) {
    if (!expr) {
        return;
    }

    if (expr->Type() == ExprType::BinaryOp) {
        auto bin_expr = std::static_pointer_cast<BinaryExpression>(expr);
        if (bin_expr->OpType() == BinaryOpType::And) {
            // Recursively split AND conditions
            SplitConjunctivePredicatesWithAnd(bin_expr->Left(), predicates);
            SplitConjunctivePredicatesWithAnd(bin_expr->Right(), predicates);
        } else {
            predicates.push_back(expr);
        }
    } else {
        predicates.push_back(expr);
    }
}

/**
 * @brief Combine a list of predicates into a single predicate
 * @param predicates List of predicates to combine
 * @return Combined predicate
 */
std::shared_ptr<Expression> PredicatePushdown::CombinePredicatesWithAnd(
    const std::vector<std::shared_ptr<Expression>>& predicates) {
    if (predicates.empty()) {
        return nullptr;
    }
    if (predicates.size() == 1) {
        return predicates[0];
    }

    // Combine predicates with AND
    auto result = predicates[0];
    for (size_t i = 1; i < predicates.size(); ++i) {
        result = std::make_shared<BinaryExpression>(BinaryOpType::And, result, predicates[i]);
    }
    return result;
}

/**
 * @brief Check if a predicate can be pushed through a join node
 * @details The simple idea is that if a predicate only references columns from one side of the join,
 * then it can be pushed through the join node.
 * @param condition Predicate to check
 * @param join Join node to push predicate through
 * @param to_left Push predicate to left child of join node
 * @return True if predicate can be pushed through join node, false otherwise
 */
bool PredicatePushdown::CanPushThroughJoin(const std::shared_ptr<Expression>& condition,
                                           const LogicalJoinNode& join,
                                           bool to_left) {
    // Helper function to check if an expression only references columns from a specific schema
    std::function<bool(const std::shared_ptr<Expression>& expr, const common::Schema& schema)> referencesOnlySchema =
        [&](const std::shared_ptr<Expression>& expr, const common::Schema& schema) -> bool {
        if (!expr) {
            return true;
        }

        switch (expr->Type()) {
            case ExprType::Column: {
                auto col_expr = std::static_pointer_cast<ColumnExpression>(expr);
                return schema.HasField(col_expr->ColumnName());
            }
            case ExprType::BinaryOp: {
                auto bin_expr = std::static_pointer_cast<BinaryExpression>(expr);
                return referencesOnlySchema(bin_expr->Left(), schema)
                       && referencesOnlySchema(bin_expr->Right(), schema);
            }
            case ExprType::Constant:
                return true;
            default:
                return false;
        }
    };

    const auto& target_schema = to_left ? join.Children()[0]->OutputSchema() : join.Children()[1]->OutputSchema();
    return referencesOnlySchema(condition, target_schema);
}

// ColumnPruning implementation
Result<std::shared_ptr<LogicalPlanNode>> ColumnPruning::Apply(std::shared_ptr<LogicalPlanNode> plan) {
    if (!plan) {
        return Result<std::shared_ptr<LogicalPlanNode>>::failure(ErrorCode::InvalidArgument, "Null plan");
    }

    // Collect required columns from the plan
    auto required_columns = CollectRequiredColumns(*plan);

    // Prune columns based on requirements
    return PruneColumns(plan, required_columns);
}

std::vector<std::string> ColumnPruning::CollectRequiredColumns(const LogicalPlanNode& node) {
    std::vector<std::string> columns;

    switch (node.Type()) {
        case LogicalNodeType::Projection: {
            auto& proj = static_cast<const LogicalProjectionNode&>(node);
            // Extract columns from projection expressions
            for (const auto& expr : proj.GetExpressions()) {
                LogicalOptimizerHelper::CollectColumnsFromExpression(expr, columns);
            }
            break;
        }
        case LogicalNodeType::Filter: {
            auto& filter = static_cast<const LogicalFilterNode&>(node);
            LogicalOptimizerHelper::CollectColumnsFromExpression(filter.GetCondition(), columns);
            break;
        }
        case LogicalNodeType::Join: {
            auto& join = static_cast<const LogicalJoinNode&>(node);
            if (join.GetCondition()) {
                LogicalOptimizerHelper::CollectColumnsFromExpression(join.GetCondition(), columns);
            }
            // Collect columns from children
            for (const auto& child : node.Children()) {
                auto child_columns = CollectRequiredColumns(*child);
                columns.insert(columns.end(), child_columns.begin(), child_columns.end());
            }
            break;
        }
        case LogicalNodeType::Aggregate: {
            auto& agg = static_cast<const LogicalAggregateNode&>(node);
            for (const auto& group_by : agg.GetGroupBy()) {
                LogicalOptimizerHelper::CollectColumnsFromExpression(group_by, columns);
            }
            for (const auto& aggregate : agg.GetAggregates()) {
                LogicalOptimizerHelper::CollectColumnsFromExpression(aggregate, columns);
            }
            break;
        }
        default:
            // For other nodes, collect columns from children
            for (const auto& child : node.Children()) {
                auto child_columns = CollectRequiredColumns(*child);
                columns.insert(columns.end(), child_columns.begin(), child_columns.end());
            }
            break;
    }

    return columns;
}

/*static*/ void LogicalOptimizerHelper::CollectColumnsFromExpression(const std::shared_ptr<Expression>& expr,
                                                                     std::vector<std::string>& columns) {
    if (!expr) {
        return;
    }

    switch (expr->Type()) {
        case ExprType::Column: {
            auto col_expr = std::static_pointer_cast<ColumnExpression>(expr);
            columns.push_back(col_expr->ColumnName());
            break;
        }
        case ExprType::BinaryOp: {
            auto bin_expr = std::static_pointer_cast<BinaryExpression>(expr);
            LogicalOptimizerHelper::CollectColumnsFromExpression(bin_expr->Left(), columns);
            LogicalOptimizerHelper::CollectColumnsFromExpression(bin_expr->Right(), columns);
            break;
        }
        case ExprType::Aggregate: {
            auto agg_expr = std::static_pointer_cast<AggregateExpression>(expr);
            LogicalOptimizerHelper::CollectColumnsFromExpression(agg_expr->Input(), columns);
            break;
        }
        default:
            break;
    }
}

Result<std::shared_ptr<LogicalPlanNode>> ColumnPruning::PruneColumns(std::shared_ptr<LogicalPlanNode> node,
                                                                     const std::vector<std::string>& required_columns) {
    if (!node) {
        return Result<std::shared_ptr<LogicalPlanNode>>::failure(ErrorCode::InvalidArgument, "Null node");
    }

    switch (node->Type()) {
        case LogicalNodeType::Scan: {
            auto scan = std::static_pointer_cast<LogicalScanNode>(node);
            // Create new schema with only required columns
            common::Schema new_schema{};
            std::set<std::string> unique_columns(required_columns.begin(), required_columns.end());

            for (const auto& field : scan->OutputSchema().Columns()) {
                if (unique_columns.find(field.name) != unique_columns.end()) {
                    new_schema.AddField(field.name, field.type, field.nullability);
                }
            }

            auto new_node = std::make_shared<LogicalScanNode>(scan->TableName(), new_schema);
            for (const auto& child : node->Children()) {
                if (child->Type() == LogicalNodeType::Filter) {
                    auto filter = std::static_pointer_cast<LogicalFilterNode>(child);
                    auto new_child = std::make_shared<LogicalFilterNode>(child, filter->GetCondition());
                    new_node->AddChild(new_child);
                } else {
                    new_node->AddChild(child);
                }
            }
            return Result<std::shared_ptr<LogicalPlanNode>>::success(new_node);
        }
        case LogicalNodeType::Join: {
            auto join = std::static_pointer_cast<LogicalJoinNode>(node);

            // Collect columns needed for join condition
            std::vector<std::string> join_columns;
            if (join->GetCondition()) {
                LogicalOptimizerHelper::CollectColumnsFromExpression(join->GetCondition(), join_columns);
            }

            // Add join columns to required columns for both children
            std::vector<std::string> all_required = required_columns;
            all_required.insert(all_required.end(), join_columns.begin(), join_columns.end());

            // Split required columns between left and right children based on their schemas
            std::vector<std::string> left_columns, right_columns;
            for (const auto& col : all_required) {
                if (join->Children()[0]->OutputSchema().HasField(col)) {
                    left_columns.push_back(col);
                }
                if (join->Children()[1]->OutputSchema().HasField(col)) {
                    right_columns.push_back(col);
                }
            }

            // Recursively prune children
            auto left_result = PruneColumns(join->Children()[0], left_columns);
            if (!left_result.ok()) {
                return left_result;
            }

            auto right_result = PruneColumns(join->Children()[1], right_columns);
            if (!right_result.ok()) {
                return right_result;
            }

            // Create new output schema combining pruned children schemas
            common::Schema new_schema{};
            for (const auto& field : left_result.value()->OutputSchema().Columns()) {
                new_schema.AddField(field.name, field.type, field.nullability);
            }
            for (const auto& field : right_result.value()->OutputSchema().Columns()) {
                new_schema.AddField(field.name, field.type, field.nullability);
            }

            return Result<std::shared_ptr<LogicalPlanNode>>::success(std::make_shared<LogicalJoinNode>(
                left_result.value(), right_result.value(), join->GetJoinType(), join->GetCondition(), new_schema));
        }
        case LogicalNodeType::Filter: {
            auto filter = std::static_pointer_cast<LogicalFilterNode>(node);

            // Add columns from filter condition
            std::vector<std::string> filter_columns;
            LogicalOptimizerHelper::CollectColumnsFromExpression(filter->GetCondition(), filter_columns);
            std::vector<std::string> all_required = required_columns;
            all_required.insert(all_required.end(), filter_columns.begin(), filter_columns.end());

            // Recursively prune child
            auto child_result = PruneColumns(filter->Children()[0], all_required);
            if (!child_result.ok()) {
                return child_result;
            }

            return Result<std::shared_ptr<LogicalPlanNode>>::success(
                std::make_shared<LogicalFilterNode>(child_result.value(), filter->GetCondition()));
        }
        case LogicalNodeType::Projection: {
            auto proj = std::static_pointer_cast<LogicalProjectionNode>(node);

            // Collect columns needed for projection expressions
            std::vector<std::string> proj_columns;
            for (const auto& expr : proj->GetExpressions()) {
                LogicalOptimizerHelper::CollectColumnsFromExpression(expr, proj_columns);
            }

            // Recursively prune child
            auto child_result = PruneColumns(proj->Children()[0], proj_columns);
            if (!child_result.ok()) {
                return child_result;
            }

            return Result<std::shared_ptr<LogicalPlanNode>>::success(std::make_shared<LogicalProjectionNode>(
                child_result.value(), proj->OutputSchema(), proj->GetExpressions(), proj->TableName()));
        }
        case LogicalNodeType::Aggregate: {
            auto agg = std::static_pointer_cast<LogicalAggregateNode>(node);

            auto required_columns = CollectRequiredColumns(*node);

            if (required_columns.empty() && !agg->Children().empty()) {
                const auto& child_schema = agg->Children()[0]->OutputSchema();
                if (child_schema.FieldCount() == 0) {
                    return Result<std::shared_ptr<LogicalPlanNode>>::failure(ErrorCode::InvalidArgument,
                                                                             "No columns to prune");
                }

                required_columns.emplace_back(child_schema.Fields()[0].name);
            }

            // Recursively prune child
            auto child_result = PruneColumns(agg->Children()[0], required_columns);
            if (!child_result.ok()) {
                return child_result;
            }

            return Result<std::shared_ptr<LogicalPlanNode>>::success(std::make_shared<LogicalAggregateNode>(
                child_result.value(), agg->GetGroupBy(), agg->GetAggregates(), agg->OutputSchema()));
        }
        default:
            return Result<std::shared_ptr<LogicalPlanNode>>::success(node);
    }
}

// ConstantFolding implementation
Result<std::shared_ptr<LogicalPlanNode>> ConstantFolding::Apply(std::shared_ptr<LogicalPlanNode> plan) {
    if (!plan) {
        return Result<std::shared_ptr<LogicalPlanNode>>::failure(ErrorCode::InvalidArgument, "Null plan");
    }

    // Fold constants in filter conditions
    if (plan->Type() == LogicalNodeType::Filter) {
        auto filter = std::static_pointer_cast<LogicalFilterNode>(plan);
        auto folded_expr = FoldConstants(filter->GetCondition());
        if (!folded_expr) {
            return Result<std::shared_ptr<LogicalPlanNode>>::failure(ErrorCode::InvalidArgument,
                                                                     "Failed to fold constants");
        }
        return Result<std::shared_ptr<LogicalPlanNode>>::success(
            std::make_shared<LogicalFilterNode>(filter->Children()[0], folded_expr));
    }

    // Recursively apply to children
    std::vector<std::shared_ptr<LogicalPlanNode>> new_children;
    for (const auto& child : plan->Children()) {
        auto result = Apply(child);
        if (!result.ok()) {
            return result;
        }
        new_children.push_back(result.value());
    }

    // Create new node with folded children
    switch (plan->Type()) {
        case LogicalNodeType::Projection: {
            auto proj = std::static_pointer_cast<LogicalProjectionNode>(plan);
            return Result<std::shared_ptr<LogicalPlanNode>>::success(std::make_shared<LogicalProjectionNode>(
                new_children[0], proj->OutputSchema(), proj->GetExpressions(), proj->TableName()));
        }
        case LogicalNodeType::Join: {
            auto join = std::static_pointer_cast<LogicalJoinNode>(plan);
            return Result<std::shared_ptr<LogicalPlanNode>>::success(std::make_shared<LogicalJoinNode>(
                new_children[0], new_children[1], join->GetJoinType(), join->GetCondition(), join->OutputSchema()));
        }
        default:
            return Result<std::shared_ptr<LogicalPlanNode>>::success(plan);
    }
}

std::shared_ptr<Expression> ConstantFolding::FoldConstants(const std::shared_ptr<Expression>& expr) {
    if (!expr) {
        return nullptr;
    }

    switch (expr->Type()) {
        case ExprType::BinaryOp: {
            auto binary = std::static_pointer_cast<BinaryExpression>(expr);
            auto left = FoldConstants(binary->Left());
            auto right = FoldConstants(binary->Right());

            // If both operands are constants, evaluate the expression
            if (left->Type() == ExprType::Constant && right->Type() == ExprType::Constant) {
                auto left_const = std::static_pointer_cast<ConstantExpression>(left);
                auto right_const = std::static_pointer_cast<ConstantExpression>(right);

                // special case for LIKE operator
                if (left_const->GetColumnType() == ColumnType::STRING
                    || right_const->GetColumnType() == ColumnType::STRING) {
                    if (binary->OpType() == BinaryOpType::Like) {
                        // Simple pattern matching implementation
                        const std::string& pattern = right_const->Value();
                        const std::string& str = left_const->Value();
                        bool matches = MatchLikePattern(str, pattern);
                        return ConstantExpression::CreateBoolean(matches);
                    }

                    return std::make_shared<BinaryExpression>(binary->OpType(), left, right);
                }

                // If both are integers, use integer arithmetic
                if (left_const->GetColumnType() == ColumnType::INT64
                    && right_const->GetColumnType() == ColumnType::INT64) {
                    auto left_val = left_const->GetInteger();
                    auto right_val = right_const->GetInteger();

                    switch (binary->OpType()) {
                        case BinaryOpType::Add:
                            return ConstantExpression::CreateInteger(left_val + right_val);
                        case BinaryOpType::Subtract:
                            return ConstantExpression::CreateInteger(left_val - right_val);
                        case BinaryOpType::Multiply:
                            return ConstantExpression::CreateInteger(left_val * right_val);
                        case BinaryOpType::Divide:
                            if (right_val == 0) {
                                return std::make_shared<BinaryExpression>(binary->OpType(), left, right);
                            }
                            // If division is exact, keep as integer
                            if (left_val % right_val == 0) {
                                return ConstantExpression::CreateInteger(left_val / right_val);
                            }
                            // Otherwise use float division
                            return ConstantExpression::CreateFloat(static_cast<double>(left_val) / right_val);
                        case BinaryOpType::Equal:
                            return ConstantExpression::CreateBoolean(left_val == right_val);
                        case BinaryOpType::NotEqual:
                            return ConstantExpression::CreateBoolean(left_val != right_val);
                        case BinaryOpType::Less:
                            return ConstantExpression::CreateBoolean(left_val < right_val);
                        case BinaryOpType::LessEqual:
                            return ConstantExpression::CreateBoolean(left_val <= right_val);
                        case BinaryOpType::Greater:
                            return ConstantExpression::CreateBoolean(left_val > right_val);
                        case BinaryOpType::GreaterEqual:
                            return ConstantExpression::CreateBoolean(left_val >= right_val);
                        default:
                            return std::make_shared<BinaryExpression>(binary->OpType(), left, right);
                    }
                }

                // Otherwise use float arithmetic
                double left_val = left_const->GetColumnType() == ColumnType::INT64 ? left_const->GetInteger()
                                                                                   : left_const->GetFloat();
                double right_val = right_const->GetColumnType() == ColumnType::INT64 ? right_const->GetInteger()
                                                                                     : right_const->GetFloat();

                switch (binary->OpType()) {
                    case BinaryOpType::Add:
                        return ConstantExpression::CreateFloat(left_val + right_val);
                    case BinaryOpType::Subtract:
                        return ConstantExpression::CreateFloat(left_val - right_val);
                    case BinaryOpType::Multiply:
                        return ConstantExpression::CreateFloat(left_val * right_val);
                    case BinaryOpType::Divide:
                        if (right_val == 0) {
                            return std::make_shared<BinaryExpression>(binary->OpType(), left, right);
                        }
                        return ConstantExpression::CreateFloat(left_val / right_val);
                    case BinaryOpType::Equal:
                        return ConstantExpression::CreateBoolean(left_val == right_val);
                    case BinaryOpType::NotEqual:
                        return ConstantExpression::CreateBoolean(left_val != right_val);
                    case BinaryOpType::Less:
                        return ConstantExpression::CreateBoolean(left_val < right_val);
                    case BinaryOpType::LessEqual:
                        return ConstantExpression::CreateBoolean(left_val <= right_val);
                    case BinaryOpType::Greater:
                        return ConstantExpression::CreateBoolean(left_val > right_val);
                    case BinaryOpType::GreaterEqual:
                        return ConstantExpression::CreateBoolean(left_val >= right_val);

                    default:
                        return std::make_shared<BinaryExpression>(binary->OpType(), left, right);
                }
            }

            // If not both constants, return a new binary expression with folded operands
            return std::make_shared<BinaryExpression>(binary->OpType(), left, right);
        }

        case ExprType::Column:
        case ExprType::Constant:
            return expr;

        default:
            return expr;
    }
}

// Helper function to match SQL LIKE patterns
bool ConstantFolding::MatchLikePattern(const std::string& str, const std::string& pattern) {
    // Convert SQL LIKE pattern to regex pattern
    std::string regex_pattern;
    regex_pattern.reserve(pattern.size() * 2);

    // Escape regex special characters except % and _
    for (size_t i = 0; i < pattern.size(); i++) {
        char c = pattern[i];
        switch (c) {
            case '%':  // % matches any sequence of characters
                regex_pattern += ".*";
                break;
            case '_':  // _ matches any single character
                regex_pattern += ".";
                break;
            // Escape regex special characters
            case '.':
            case '^':
            case '$':
            case '*':
            case '+':
            case '?':
            case '(':
            case ')':
            case '[':
            case ']':
            case '{':
            case '}':
            case '|':
            case '\\':
                regex_pattern += '\\';
                regex_pattern += c;
                break;
            default:
                regex_pattern += c;
                break;
        }
    }

    try {
        std::regex regex(regex_pattern);
        return std::regex_match(str, regex);
    } catch (const std::regex_error&) {
        // If regex compilation fails, return false
        return false;
    }
}

// JoinReordering implementation
Result<std::shared_ptr<LogicalPlanNode>> JoinReordering::Apply(std::shared_ptr<LogicalPlanNode> plan) {
    if (!plan) {
        return Result<std::shared_ptr<LogicalPlanNode>>::failure(ErrorCode::InvalidArgument, "Null plan");
    }

    if (plan->Type() == LogicalNodeType::Join) {
        return ReorderJoins(std::static_pointer_cast<LogicalJoinNode>(plan));
    }

    // Recursively apply to children
    std::vector<std::shared_ptr<LogicalPlanNode>> new_children;
    for (const auto& child : plan->Children()) {
        auto result = Apply(child);
        if (!result.ok()) {
            return result;
        }
        new_children.push_back(result.value());
    }

    // Create new node with reordered children
    switch (plan->Type()) {
        case LogicalNodeType::Projection: {
            auto proj = std::static_pointer_cast<LogicalProjectionNode>(plan);
            return Result<std::shared_ptr<LogicalPlanNode>>::success(std::make_shared<LogicalProjectionNode>(
                new_children[0], proj->OutputSchema(), proj->GetExpressions(), proj->TableName()));
        }
        case LogicalNodeType::Filter: {
            auto filter = std::static_pointer_cast<LogicalFilterNode>(plan);
            return Result<std::shared_ptr<LogicalPlanNode>>::success(
                std::make_shared<LogicalFilterNode>(new_children[0], filter->GetCondition()));
        }
        default:
            return Result<std::shared_ptr<LogicalPlanNode>>::success(plan);
    }
}

double JoinReordering::EstimateCardinality(const LogicalPlanNode& node) {
    // Simple cardinality estimation
    switch (node.Type()) {
        case LogicalNodeType::Scan:
            return 1000.0;  // Assume 1000 rows for each table
        case LogicalNodeType::Filter:
            return EstimateCardinality(*node.Children()[0]) * 0.1;  // Assume 10% selectivity
        case LogicalNodeType::Join:
            return EstimateCardinality(*node.Children()[0]) * EstimateCardinality(*node.Children()[1]) * 0.1;
        default:
            return EstimateCardinality(*node.Children()[0]);
    }
}

double JoinReordering::EstimateSelectivity(const std::string& condition) {
    // Simple selectivity estimation
    // In a real implementation, this would use statistics
    return 0.1;  // Assume 10% selectivity for all conditions
}

Result<std::shared_ptr<LogicalPlanNode>> JoinReordering::ReorderJoins(std::shared_ptr<LogicalJoinNode> join) {
    // Simple implementation: if right child has lower cardinality, swap children
    double left_card = EstimateCardinality(*join->Children()[0]);
    double right_card = EstimateCardinality(*join->Children()[1]);

    if (right_card < left_card && join->GetJoinType() == JoinType::Inner) {
        // For inner joins, we can swap the sides
        // We need to update the join condition to swap column references
        auto swapped_condition = SwapJoinCondition(join->GetCondition());
        return Result<std::shared_ptr<LogicalPlanNode>>::success(std::make_shared<LogicalJoinNode>(
            join->Children()[1], join->Children()[0], join->GetJoinType(), swapped_condition, join->OutputSchema()));
    }

    return Result<std::shared_ptr<LogicalPlanNode>>::success(join);
}

std::shared_ptr<Expression> JoinReordering::SwapJoinCondition(const std::shared_ptr<Expression>& condition) {
    if (!condition) {
        return nullptr;
    }

    switch (condition->Type()) {
        case ExprType::BinaryOp: {
            auto bin_expr = std::static_pointer_cast<BinaryExpression>(condition);
            // For comparison operators, swap the operands
            switch (bin_expr->OpType()) {
                case BinaryOpType::Equal:
                case BinaryOpType::NotEqual:
                    // These operators are commutative, just swap the operands
                    return std::make_shared<BinaryExpression>(
                        bin_expr->OpType(), SwapJoinCondition(bin_expr->Right()), SwapJoinCondition(bin_expr->Left()));
                case BinaryOpType::Less:
                    // a < b becomes b > a
                    return std::make_shared<BinaryExpression>(BinaryOpType::Greater,
                                                              SwapJoinCondition(bin_expr->Right()),
                                                              SwapJoinCondition(bin_expr->Left()));
                case BinaryOpType::LessEqual:
                    // a <= b becomes b >= a
                    return std::make_shared<BinaryExpression>(BinaryOpType::GreaterEqual,
                                                              SwapJoinCondition(bin_expr->Right()),
                                                              SwapJoinCondition(bin_expr->Left()));
                case BinaryOpType::Greater:
                    // a > b becomes b < a
                    return std::make_shared<BinaryExpression>(
                        BinaryOpType::Less, SwapJoinCondition(bin_expr->Right()), SwapJoinCondition(bin_expr->Left()));
                case BinaryOpType::GreaterEqual:
                    // a >= b becomes b <= a
                    return std::make_shared<BinaryExpression>(BinaryOpType::LessEqual,
                                                              SwapJoinCondition(bin_expr->Right()),
                                                              SwapJoinCondition(bin_expr->Left()));
                default:
                    // For other operators (AND, OR), just recurse on children
                    return std::make_shared<BinaryExpression>(
                        bin_expr->OpType(), SwapJoinCondition(bin_expr->Left()), SwapJoinCondition(bin_expr->Right()));
            }
        }
        case ExprType::Column:
        case ExprType::Constant:
            return condition;
        default:
            return condition;
    }
}

// ProjectionPushdown implementation
Result<std::shared_ptr<LogicalPlanNode>> ProjectionPushdown::Apply(std::shared_ptr<LogicalPlanNode> plan) {
    if (!plan) {
        return Result<std::shared_ptr<LogicalPlanNode>>::failure(ErrorCode::InvalidArgument, "Null plan");
    }

    // First recursively apply to children
    std::vector<std::shared_ptr<LogicalPlanNode>> new_children;
    for (const auto& child : plan->Children()) {
        auto result = Apply(child);
        if (!result.ok()) {
            return result;
        }
        new_children.push_back(result.value());
    }

    // Then handle this node
    switch (plan->Type()) {
        case LogicalNodeType::Projection: {
            auto projection = std::static_pointer_cast<LogicalProjectionNode>(plan);
            if (new_children.empty()) {
                return Result<std::shared_ptr<LogicalPlanNode>>::success(projection);
            }
            return PushProjection(projection, new_children[0]);
        }
        case LogicalNodeType::Filter: {
            auto filter = std::static_pointer_cast<LogicalFilterNode>(plan);
            return Result<std::shared_ptr<LogicalPlanNode>>::success(
                std::make_shared<LogicalFilterNode>(new_children[0], filter->GetCondition()));
        }
        case LogicalNodeType::Join: {
            auto join = std::static_pointer_cast<LogicalJoinNode>(plan);
            return Result<std::shared_ptr<LogicalPlanNode>>::success(std::make_shared<LogicalJoinNode>(
                new_children[0], new_children[1], join->GetJoinType(), join->GetCondition(), join->OutputSchema()));
        }
        default:
            return Result<std::shared_ptr<LogicalPlanNode>>::success(plan);
    }
}

Result<std::shared_ptr<LogicalPlanNode>> ProjectionPushdown::PushProjection(
    std::shared_ptr<LogicalProjectionNode> projection, std::shared_ptr<LogicalPlanNode> child) {
    switch (child->Type()) {
        case LogicalNodeType::Projection: {
            // Combine consecutive projections
            auto child_proj = std::static_pointer_cast<LogicalProjectionNode>(child);
            auto combined_exprs = projection->GetExpressions();
            return Result<std::shared_ptr<LogicalPlanNode>>::success(
                CreateProjection(child_proj->Children()[0], combined_exprs, projection->TableName()));
        }
        case LogicalNodeType::Filter: {
            auto filter = std::static_pointer_cast<LogicalFilterNode>(child);
            // Check if filter uses any columns that would be projected away
            std::vector<std::string> filter_cols;
            LogicalOptimizerHelper::CollectColumnsFromExpression(filter->GetCondition(), filter_cols);

            // Get columns from projection
            std::vector<std::string> proj_cols;
            for (const auto& expr : projection->GetExpressions()) {
                LogicalOptimizerHelper::CollectColumnsFromExpression(expr, proj_cols);
            }

            // If filter doesn't use any columns that would be projected away,
            // we can push the projection below the filter
            bool can_push = true;
            for (const auto& col : filter_cols) {
                if (std::find(proj_cols.begin(), proj_cols.end(), col) == proj_cols.end()) {
                    can_push = false;
                    break;
                }
            }

            if (can_push) {
                auto new_child =
                    CreateProjection(filter->Children()[0], projection->GetExpressions(), projection->TableName());
                return Result<std::shared_ptr<LogicalPlanNode>>::success(
                    std::make_shared<LogicalFilterNode>(new_child, filter->GetCondition()));
            }
            return Result<std::shared_ptr<LogicalPlanNode>>::success(projection);
        }
        case LogicalNodeType::Join: {
            auto join = std::static_pointer_cast<LogicalJoinNode>(child);
            auto exprs = projection->GetExpressions();

            // Check if all expressions can be pushed to left or right child
            bool can_push_left = CanPushThroughJoin(exprs, *join, true);
            bool can_push_right = CanPushThroughJoin(exprs, *join, false);

            if (can_push_left || can_push_right) {
                // Create new projections for each side
                auto left_child = join->Children()[0];
                auto right_child = join->Children()[1];

                if (can_push_left) {
                    left_child = CreateProjection(left_child, exprs, projection->TableName());
                }
                if (can_push_right) {
                    right_child = CreateProjection(right_child, exprs, projection->TableName());
                }

                // Create new join with pushed projections
                auto new_join = std::make_shared<LogicalJoinNode>(
                    left_child, right_child, join->GetJoinType(), join->GetCondition(), join->OutputSchema());

                // If we pushed to both sides, we don't need the top projection
                if (can_push_left && can_push_right) {
                    return Result<std::shared_ptr<LogicalPlanNode>>::success(new_join);
                }
                return Result<std::shared_ptr<LogicalPlanNode>>::success(
                    CreateProjection(new_join, exprs, projection->TableName()));
            }
            return Result<std::shared_ptr<LogicalPlanNode>>::success(projection);
        }
        default:
            return Result<std::shared_ptr<LogicalPlanNode>>::success(projection);
    }
}

bool ProjectionPushdown::CanPushThroughJoin(const std::vector<std::shared_ptr<Expression>>& exprs,
                                            const LogicalJoinNode& join,
                                            bool to_left) {
    // Get the schema for the target side
    const auto& schema = to_left ? join.Children()[0]->OutputSchema() : join.Children()[1]->OutputSchema();

    // Check if all columns referenced in expressions exist in the target schema
    for (const auto& expr : exprs) {
        std::vector<std::string> cols;
        LogicalOptimizerHelper::CollectColumnsFromExpression(expr, cols);
        for (const auto& col : cols) {
            if (!schema.HasField(col)) {
                return false;
            }
        }
    }

    // Also check join condition columns
    if (join.GetCondition()) {
        std::vector<std::string> join_cols;
        LogicalOptimizerHelper::CollectColumnsFromExpression(join.GetCondition(), join_cols);
        for (const auto& col : join_cols) {
            if (!schema.HasField(col)) {
                return false;
            }
        }
    }

    return true;
}

std::vector<std::shared_ptr<Expression>> ProjectionPushdown::ExtractProjectionExprs(
    const std::shared_ptr<LogicalPlanNode>& node) {
    if (node->Type() == LogicalNodeType::Projection) {
        return std::static_pointer_cast<LogicalProjectionNode>(node)->GetExpressions();
    }
    return {};
}

std::shared_ptr<LogicalPlanNode> ProjectionPushdown::CreateProjection(
    std::shared_ptr<LogicalPlanNode> input,
    const std::vector<std::shared_ptr<Expression>>& exprs,
    const std::string& table_name) {
    // Create output schema based on expressions
    common::Schema schema{};
    for (const auto& expr : exprs) {
        if (expr->Type() == ExprType::Column) {
            auto col_expr = std::static_pointer_cast<ColumnExpression>(expr);
            if (input->OutputSchema().HasField(col_expr->ColumnName())) {
                auto column = input->OutputSchema().GetColumn(col_expr->ColumnName());
                schema.AddField(column.name, column.type, column.nullability);
            }
        }
    }

    return std::make_shared<LogicalProjectionNode>(input, schema, exprs, table_name);
}

// Limit pushdown optimization
Result<std::shared_ptr<LogicalPlanNode>> LimitPushdown::Apply(std::shared_ptr<LogicalPlanNode> plan) {
    if (plan->Type() == LogicalNodeType::Limit) {
        auto limit_node = plan->as<LogicalLimitNode>();
        auto child = limit_node->Children()[0];

        if (child->Type() == LogicalNodeType::Projection) {
            // Push limit below projection
            auto proj_node = child->as<LogicalProjectionNode>();
            auto new_limit =
                std::make_shared<LogicalLimitNode>(proj_node->Children()[0], limit_node->Limit(), limit_node->Offset());
            proj_node->ReplaceChild(0, new_limit);

            // return child (which is projection node) after pushing limit to one side
            return child;
        } else if (child->Type() == LogicalNodeType::Sort) {
            // Push limit below sort only if there's no offset
            if (limit_node->Offset() == 0) {
                auto sort_node = child->as<LogicalSortNode>();
                auto new_limit = std::make_shared<LogicalLimitNode>(sort_node->Children()[0], limit_node->Limit(), 0);
                sort_node->ReplaceChild(0, new_limit);

                // return child (which is sort node) after pushing limit to one side
                return child;
            }
        }
    }
    return plan;
}

}  // namespace pond::query