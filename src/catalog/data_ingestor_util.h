#pragma once

#include "arrow/compute/api.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "catalog/metadata.h"
#include "common/result.h"
namespace pond::catalog {

/**
 * Structure to hold a record batch and its associated partition values
 */
struct PartitionedBatch {
    std::shared_ptr<arrow::RecordBatch> batch;
    std::unordered_map<std::string, std::string> partition_values;
};

class DataIngestorUtil {
public:
    /**
     * Extracts partition values from a record batch (only uses the first row).
     * This assumes the record batch is already partitioned with the same partition specs as the table.
     * @param metadata The table metadata containing partition specifications
     * @param batch The record batch to extract values from
     * @return A map of partition field names to their string values
     */
    static common::Result<std::unordered_map<std::string, std::string>> ExtractPartitionValues(
        const TableMetadata& metadata, const std::shared_ptr<arrow::RecordBatch>& batch);

    /**
     * Partitions a record batch into multiple record batches based on partition specifications.
     * @param metadata The table metadata containing partition specifications
     * @param batch The record batch to partition
     * @return A vector of PartitionedBatch structures, each containing a record batch for a specific partition
     *         and its associated partition values
     */
    static common::Result<std::vector<PartitionedBatch>> PartitionRecordBatch(
        const TableMetadata& metadata, const std::shared_ptr<arrow::RecordBatch>& batch);

    /**
     * Extracts partition values for a column in a vectorized manner.
     * @param field The partition field specification
     * @param array The column array to extract partition values from
     * @return A Result containing an array of partition values as strings
     */
    static common::Result<std::shared_ptr<arrow::StringArray>> ExtractPartitionValuesVectorized(
        const PartitionField& field, const std::shared_ptr<arrow::Array>& array);

    /**
     * Updates partition masks based on the partition values extracted from a column.
     * @param partition_values_array The array of partition values for the current field
     * @param partition_masks Map of partition keys to selection masks
     * @param partition_values_lookup Map of partition keys to partition value maps
     * @param field_name The name of the current partition field
     */
    static void UpdatePartitionMasks(
        const std::shared_ptr<arrow::StringArray>& partition_values_array,
        std::unordered_map<std::string, arrow::Datum>& partition_masks,
        std::unordered_map<std::string, std::unordered_map<std::string, std::string>>& partition_values_lookup,
        const std::string& field_name);
};

}  // namespace pond::catalog
