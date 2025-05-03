#pragma once

#include <memory>
#include <string>

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/record_batch.h>

#include "common/expression.h"
#include "common/result.h"

namespace pond::query {

using ArrowDataBatchSharedPtr = std::shared_ptr<arrow::RecordBatch>;

/**
 * @brief Utility class for applying predicates to Arrow record batches
 */
class ArrowPredicate {
public:
    /**
     * @brief Apply a filter expression to a record batch
     * @param batch The record batch to filter
     * @param predicate The filter expression
     * @return The filtered record batch
     */
    static common::Result<ArrowDataBatchSharedPtr> Apply(const ArrowDataBatchSharedPtr& batch,
                                                         const std::shared_ptr<common::Expression>& predicate);
};

}  // namespace pond::query