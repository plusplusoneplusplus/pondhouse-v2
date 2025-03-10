#pragma once

#include <memory>
#include <string>
#include <vector>

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/record_batch.h>

#include "common/expression.h"
#include "common/result.h"
#include "common/schema.h"

namespace pond::query {

using ArrowDataBatchSharedPtr = std::shared_ptr<arrow::RecordBatch>;

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
    static common::Result<ArrowDataBatchSharedPtr> CreateEmptyBatch(const common::Schema& schema);

    /**
     * @brief Create an empty record batch with an empty schema
     * @return An empty record batch with an empty schema
     */
    static ArrowDataBatchSharedPtr CreateEmptyBatch();

    /**
     * @brief Apply a filter expression to a record batch
     * @param batch The record batch to filter
     * @param predicate The filter expression
     * @return The filtered record batch
     */
    static common::Result<ArrowDataBatchSharedPtr> ApplyPredicate(const ArrowDataBatchSharedPtr& batch,
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
    static common::Result<ArrowDataBatchSharedPtr> ConcatenateBatches(
        const std::vector<ArrowDataBatchSharedPtr>& batches);

    /**
     * @brief Convert JSON string to Arrow RecordBatch
     * @param json_str The JSON string to convert
     * @param schema The schema for the record batch
     * @return Result containing the converted record batch or an error
     *
     * The JSON string should be an array of objects, where each object represents a row.
     * Each object should have keys matching the column names in the schema.
     * Example: [{"col1": 1, "col2": "value"}, {"col1": 2, "col2": "another value"}]
     */
    static common::Result<ArrowDataBatchSharedPtr> JsonToRecordBatch(const std::string& json_str,
                                                                     const common::Schema& schema);

    /**
     * @brief Convert JSON document to Arrow RecordBatch
     * @param json_doc The RapidJSON document to convert
     * @param schema The schema for the record batch
     * @return Result containing the converted record batch or an error
     */
    static common::Result<ArrowDataBatchSharedPtr> JsonToRecordBatch(const rapidjson::Document& json_doc,
                                                                     const common::Schema& schema);
};

}  // namespace pond::query