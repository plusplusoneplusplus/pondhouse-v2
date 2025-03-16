#pragma once

#include "common/expression.h"
#include "common/result.h"
#include "common/schema.h"
#include "query/data/arrow_util.h"
#include "query/planner/physical_plan_node.h"

namespace pond::query {

class ExecutorUtil {
public:
    /**
     * @brief Create a projection batch
     *
     * @param node The physical projection node
     * @param input_batch The input batch
     * @return The projected batch
     */
    static common::Result<ArrowDataBatchSharedPtr> CreateProjectionBatch(PhysicalProjectionNode& node,
                                                                         ArrowDataBatchSharedPtr input_batch);

    /**
     * @brief Create a sort batch
     *
     * @param input_batch The input batch to sort
     * @param sort_specs The sort specifications
     * @return The sorted batch
     *
     * Note:
     * - This function assumes the input batch has a schema that matches the sort specifications.
     * - When value is null, it is considered smaller than any non-null value.
     */
    static common::Result<ArrowDataBatchSharedPtr> CreateSortBatch(ArrowDataBatchSharedPtr input_batch,
                                                                   const std::vector<SortSpec>& sort_specs);

    /**
     * @brief Create a limit batch
     *
     * @param input_batch The input batch
     * @param limit The limit
     * @return The limited batch
     */
    static common::Result<ArrowDataBatchSharedPtr> CreateLimitBatch(ArrowDataBatchSharedPtr input_batch,
                                                                    size_t limit,
                                                                    size_t offset);
};

}  // namespace pond::query
