#pragma once

#include <memory>

#include "common/result.h"
#include "common/schema.h"
#include "query/data/arrow_util.h"
#include "query/planner/physical_plan_node.h"

namespace pond::query {

/**
 * @brief Interface for query executors
 * Query executors are responsible for executing physical plans and producing results.
 * They implement the visitor pattern to handle different physical operator types.
 */
class Executor {
public:
    virtual ~Executor() = default;

    // Execute a physical plan
    virtual common::Result<ArrowDataBatchSharedPtr> Execute(std::shared_ptr<PhysicalPlanNode> plan) = 0;
};

}  // namespace pond::query