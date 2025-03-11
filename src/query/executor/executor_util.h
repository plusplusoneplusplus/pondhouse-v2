#pragma once

#include "common/expression.h"
#include "common/result.h"
#include "common/schema.h"
#include "query/data/arrow_util.h"
#include "query/planner/physical_plan_node.h"

namespace pond::query {

class ExecutorUtil {
public:
    static common::Result<ArrowDataBatchSharedPtr> CreateProjectionBatch(PhysicalProjectionNode& node,
                                                                         ArrowDataBatchSharedPtr input_batch);
};

}  // namespace pond::query
