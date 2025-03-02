#pragma once

#include <memory>
#include <SQLParser.h>
#include <string>
#include <vector>

#include "catalog/catalog.h"
#include "common/result.h"
#include "logical_plan_node.h"

namespace pond::query {

class LogicalPlanner {
public:
    explicit LogicalPlanner(catalog::Catalog& catalog) : catalog_(catalog) {}

    // Convert SQL AST to logical plan
    common::Result<std::shared_ptr<LogicalPlanNode>> Plan(const hsql::SQLStatement* statement);

private:
    // Helper methods for different statement types
    common::Result<std::shared_ptr<LogicalPlanNode>> PlanSelect(const hsql::SelectStatement* select);
    common::Result<std::shared_ptr<LogicalPlanNode>> PlanTableRef(const hsql::TableRef* table);
    common::Result<std::shared_ptr<LogicalPlanNode>> PlanWhere(std::shared_ptr<LogicalPlanNode> input,
                                                               const hsql::Expr* where);
    common::Result<std::shared_ptr<LogicalPlanNode>> PlanProjection(const std::string& table_name,
                                                                    std::shared_ptr<LogicalPlanNode> input,
                                                                    const std::vector<hsql::Expr*>* exprs,
                                                                    const std::shared_ptr<LogicalPlanNode>& agg);
    common::Result<std::shared_ptr<LogicalPlanNode>> PlanGroupBy(std::shared_ptr<LogicalPlanNode> input,
                                                                 const hsql::GroupByDescription* group_by,
                                                                 const std::vector<hsql::Expr*>* select_list);
    common::Result<std::shared_ptr<LogicalPlanNode>> PlanSort(std::shared_ptr<LogicalPlanNode> input,
                                                              const std::vector<hsql::OrderDescription*>* order);
    common::Result<std::shared_ptr<LogicalPlanNode>> PlanLimit(std::shared_ptr<LogicalPlanNode> input,
                                                               const hsql::LimitDescription* limit);

    // Join planning methods
    common::Result<std::shared_ptr<LogicalPlanNode>> PlanJoin(const hsql::TableRef* table);
    common::Result<std::shared_ptr<LogicalPlanNode>> PlanJoinCondition(std::shared_ptr<LogicalPlanNode> left,
                                                                       std::shared_ptr<LogicalPlanNode> right,
                                                                       JoinType join_type,
                                                                       const hsql::JoinDefinition* join);
    common::Result<common::Schema> CreateJoinSchema(const common::Schema& left_schema,
                                                    const common::Schema& right_schema);

    // Schema verification helpers
    common::Result<common::Schema> VerifyTableExists(const std::string& table_name);
    bool DoesColumnExist(const common::Schema& schema, const std::string& column_name);
    common::Result<common::Schema> CreateProjectionSchema(const common::Schema& input_schema,
                                                          const std::vector<std::shared_ptr<Expression>>& current_exprs,
                                                          const std::vector<std::shared_ptr<Expression>>& child_exprs);

    // Convert expression to string representation
    std::string ExprToString(const hsql::Expr* expr);

    // New method to build expressions
    common::Result<std::shared_ptr<Expression>> BuildExpression(const hsql::Expr* expr);

private:
    catalog::Catalog& catalog_;
};

}  // namespace pond::query