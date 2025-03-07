#pragma once

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/record_batch.h>

#include "common/expression.h"
#include "common/result.h"
#include "common/schema.h"

namespace pond::query {

using DataBatchSharedPtr = std::shared_ptr<arrow::RecordBatch>;

/**
 * @brief Utility functions for working with Arrow data
 */
class ArrowUtil {
public:
    /**
     * @brief Create an empty array of the appropriate type for a column
     * @param type The column type
     * @return An empty array of the appropriate type
     */
    static arrow::Result<std::shared_ptr<arrow::Array>> CreateEmptyArray(common::ColumnType type);

    /**
     * @brief Create an empty record batch with the given schema
     * @param schema The schema for the record batch
     * @return An empty record batch with the given schema
     */
    static common::Result<std::shared_ptr<arrow::RecordBatch>> CreateEmptyBatch(const common::Schema& schema);

    /**
     * @brief Apply a filter expression to a record batch
     * @param batch The record batch to filter
     * @param predicate The filter expression
     * @return The filtered record batch
     */
    static common::Result<DataBatchSharedPtr> ApplyPredicate(const DataBatchSharedPtr& batch,
                                                             const std::shared_ptr<common::Expression>& predicate);
};

}  // namespace pond::query