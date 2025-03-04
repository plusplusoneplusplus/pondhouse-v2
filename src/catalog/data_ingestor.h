#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/metadata.h"
#include "common/append_only_fs.h"
#include "common/result.h"
#include "common/schema.h"
#include "format/parquet/parquet_writer.h"

namespace pond::catalog {

/**
 * DataIngestor handles the ingestion of data into tables managed by the catalog.
 * It manages writing data to Parquet files and updating the catalog metadata.
 */
class DataIngestor {
public:
    /**
     * Creates a new DataIngestor instance.
     * @param catalog The catalog managing the table
     * @param fs The filesystem to write data to
     * @param table_name The name of the table to ingest data into
     */
    static common::Result<std::unique_ptr<DataIngestor>> Create(std::shared_ptr<Catalog> catalog,
                                                                std::shared_ptr<common::IAppendOnlyFileSystem> fs,
                                                                const std::string& table_name);

    /**
     * Ingest a batch of data into the table.
     * @param batch The record batch to ingest
     * @param commit_after_write Whether to commit the changes immediately after writing
     */
    common::Result<DataFile> IngestBatch(const std::shared_ptr<arrow::RecordBatch>& batch,
                                         bool commit_after_write = true);

    /**
     * Ingest multiple batches of data into the table.
     * @param batches The record batches to ingest
     * @param commit_after_write Whether to commit the changes immediately after writing
     */
    common::Result<std::vector<DataFile>> IngestBatches(const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
                                                        bool commit_after_write = true);

    /**
     * Ingest an Arrow table into the table.
     * @param table The Arrow table to ingest
     * @param commit_after_write Whether to commit the changes immediately after writing
     */
    common::Result<DataFile> IngestTable(const std::shared_ptr<arrow::Table>& table, bool commit_after_write = true);

    /**
     * Commit any pending changes to the catalog.
     */
    common::Result<bool> Commit();

public:
    /**
     * Extracts partition values from a record batch (only uses the first row).
     * This is assume the record batch is already partitioned with the same partition specs as the table.
     * @param metadata The table metadata containing partition specifications
     * @param batch The record batch to extract values from
     * @return A map of partition field names to their string values
     */
    static common::Result<std::unordered_map<std::string, std::string>> ExtractPartitionValues(
        const TableMetadata& metadata, const std::shared_ptr<arrow::RecordBatch>& batch);

private:
    DataIngestor(std::shared_ptr<Catalog> catalog,
                 std::shared_ptr<common::IAppendOnlyFileSystem> fs,
                 const std::string& table_name,
                 TableMetadata metadata);

    common::Result<std::string> GenerateDataFilePath(
        const std::unordered_map<std::string, std::string>& partition_values);
    common::Result<std::unique_ptr<format::ParquetWriter>> CreateWriter(const std::string& file_path);
    common::Result<DataFile> FinalizeDataFile(const std::string& file_path,
                                              int64_t num_records,
                                              const std::unordered_map<std::string, std::string>& partition_values);

    // Add new helper method for schema validation
    common::Result<void> ValidateSchema(const std::shared_ptr<arrow::Schema>& input_schema) const;

    std::shared_ptr<Catalog> catalog_;
    std::shared_ptr<common::IAppendOnlyFileSystem> fs_;
    std::string table_name_;
    TableMetadata current_metadata_;
    std::vector<DataFile> pending_files_;
};

}  // namespace pond::catalog