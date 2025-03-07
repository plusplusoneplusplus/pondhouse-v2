#pragma once

#include <memory>
#include <vector>

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
    static common::Result<DataBatchSharedPtr> CreateEmptyBatch(const common::Schema& schema);

    /**
     * @brief Create an empty record batch with an empty schema
     * @return An empty record batch with an empty schema
     */
    static DataBatchSharedPtr CreateEmptyBatch();

    /**
     * @brief Apply a filter expression to a record batch
     * @param batch The record batch to filter
     * @param predicate The filter expression
     * @return The filtered record batch
     */
    static common::Result<DataBatchSharedPtr> ApplyPredicate(const DataBatchSharedPtr& batch,
                                                             const std::shared_ptr<common::Expression>& predicate);

    /**
     * @brief Concatenate multiple record batches into a single batch
     * @param batches Vector of record batches to concatenate
     * @return Result containing the concatenated batch or an error
     *
     * All batches must have the same schema. If the vector is empty,
     * an empty record batch will be returned with a null schema.
     * If there's only one batch, it will be returned directly without copying.
     */
    static common::Result<DataBatchSharedPtr> ConcatenateBatches(const std::vector<DataBatchSharedPtr>& batches);
};

}  // namespace pond::query