#pragma once

#include "query/planner/logical_planner.h"
#include "query/planner/physical_planner.h"
#include "query/planner/logical_optimizer.h"
#include "common/result.h"

namespace pond::query {

class Planner {
public:
    Planner(const std::shared_ptr<catalog::Catalog>& catalog);
    common::Result<std::shared_ptr<PhysicalPlanNode>> Plan(const std::string& sql_query);

    common::Result<std::shared_ptr<LogicalPlanNode>> PlanLogical(const std::string& sql_query, bool optimize = true);
    common::Result<std::shared_ptr<LogicalPlanNode>> Optimize(std::shared_ptr<LogicalPlanNode> plan);
    common::Result<std::shared_ptr<PhysicalPlanNode>> PlanPhysical(const std::string& query, bool optimize = true);

private:
    std::shared_ptr<catalog::Catalog> catalog_;
};

} // namespace pond::query