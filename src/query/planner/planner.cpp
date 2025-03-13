#include "query/planner/planner.h"

namespace pond::query {

Planner::Planner(const std::shared_ptr<catalog::Catalog>& catalog) : catalog_(catalog) {}

Result<std::shared_ptr<LogicalPlanNode>> Planner::PlanLogical(const std::string& query, bool optimize) {
    LogicalPlanner planner(*catalog_);
    hsql::SQLParserResult parse_result;
    hsql::SQLParser::parse(query, &parse_result);
    if (!parse_result.isValid()) {
        return common::Result<std::shared_ptr<LogicalPlanNode>>::failure(common::ErrorCode::InvalidArgument, std::string("Invalid SQL query: ") + parse_result.errorMsg());
    }
    auto logical_plan = planner.Plan(parse_result.getStatement(0));
    if (optimize) {
        return Optimize(logical_plan.value());
    }
    return logical_plan;
}

Result<std::shared_ptr<LogicalPlanNode>> Planner::Optimize(std::shared_ptr<LogicalPlanNode> plan) {
    using ReturnType = Result<std::shared_ptr<LogicalPlanNode>>;
    LogicalOptimizer optimizer;
    auto optimized_plan_result = optimizer.Optimize(plan);
    RETURN_IF_ERROR_T(ReturnType, optimized_plan_result);
    return optimized_plan_result;
}

Result<std::shared_ptr<PhysicalPlanNode>> Planner::PlanPhysical(const std::string& query, bool optimize) {
    using ReturnType = Result<std::shared_ptr<PhysicalPlanNode>>;
    PhysicalPlanner planner(*catalog_);
    auto logical_plan = PlanLogical(query, optimize);
    RETURN_IF_ERROR_T(ReturnType, logical_plan);
    return planner.Plan(logical_plan.value());
}

common::Result<std::shared_ptr<PhysicalPlanNode>> Planner::Plan(const std::string& sql_query) {
    using ReturnType = Result<std::shared_ptr<PhysicalPlanNode>>;
    auto physical_plan = PlanPhysical(sql_query);
    RETURN_IF_ERROR_T(ReturnType, physical_plan);
    return physical_plan;
}

} // namespace pond::query