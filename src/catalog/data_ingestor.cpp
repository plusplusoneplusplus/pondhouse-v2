#include "catalog/data_ingestor.h"

#include <iostream>

#include <arrow/array.h>
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/compute/api.h>
#include <arrow/record_batch.h>
#include <arrow/table.h>
#include <arrow/type.h>
#include <arrow/util/bit_util.h>

#include "catalog/data_ingestor_util.h"
#include "common/result.h"
#include "format/parquet/schema_converter.h"

namespace pond::catalog {

common::Result<std::unique_ptr<DataIngestor>> DataIngestor::Create(std::shared_ptr<Catalog> catalog,
                                                                   std::shared_ptr<common::IAppendOnlyFileSystem> fs,
                                                                   const std::string& table_name) {
    using ReturnType = common::Result<std::unique_ptr<DataIngestor>>;

    auto metadata_result = catalog->LoadTable(table_name);
    RETURN_IF_ERROR_T(ReturnType, metadata_result);

    return ReturnType::success(
        std::unique_ptr<DataIngestor>(new DataIngestor(catalog, fs, table_name, metadata_result.value())));
}

DataIngestor::DataIngestor(std::shared_ptr<Catalog> catalog,
                           std::shared_ptr<common::IAppendOnlyFileSystem> fs,
                           const std::string& table_name,
                           TableMetadata metadata)
    : catalog_(std::move(catalog)),
      fs_(std::move(fs)),
      table_name_(table_name),
      current_metadata_(std::move(metadata)) {}

common::Result<DataFile> DataIngestor::IngestBatch(const std::shared_ptr<arrow::RecordBatch>& batch,
                                                   bool commit_after_write) {
    using ReturnType = common::Result<DataFile>;

    auto result = IngestBatches({batch}, commit_after_write);
    RETURN_IF_ERROR_T(ReturnType, result);
    if (result.value().empty()) {
        return common::Error(common::ErrorCode::InvalidOperation, "No data files were created");
    }

    // Return the first file for backward compatibility
    return ReturnType::success(result.value()[0]);
}

common::Result<std::vector<DataFile>> DataIngestor::IngestBatches(
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches, bool commit_after_write) {
    using ReturnType = common::Result<std::vector<DataFile>>;

    if (batches.empty()) {
        return ReturnType::success({});
    }

    // Validate schema for each batch
    for (const auto& batch : batches) {
        auto schema_validation = ValidateSchema(batch->schema());
        RETURN_IF_ERROR_T(ReturnType, schema_validation);
    }

    std::vector<DataFile> result_files;

    // Process each batch
    for (const auto& batch : batches) {
        // Skip empty batches
        if (batch->num_rows() == 0) {
            continue;
        }

        // If we have partition specs, split the batch by partition
        if (!current_metadata_.partition_specs.empty()) {
            // Partition the batch
            auto partitioned_result = DataIngestorUtil::PartitionRecordBatch(current_metadata_, batch);
            RETURN_IF_ERROR_T(ReturnType, partitioned_result);
            std::vector<PartitionedBatch> partitioned_batches = partitioned_result.value();

            // Skip if no partitions were created (could happen if all rows are filtered out)
            if (partitioned_batches.empty()) {
                LOG_STATUS("No partitions were created for batch, falling back to unpartitioned file");

                // Fall back to using the original batch with empty partition values
                std::unordered_map<std::string, std::string> empty_partition_values;

                // Use the new method
                auto data_file_result = WriteBatchToFile(batch, empty_partition_values);
                RETURN_IF_ERROR_T(ReturnType, data_file_result);
                result_files.push_back(data_file_result.value());

                continue;
            }

            // Process each partitioned batch separately
            for (const auto& p_batch : partitioned_batches) {
                // Use the new method
                auto data_file_result = WriteBatchToFile(p_batch.batch, p_batch.partition_values);
                RETURN_IF_ERROR_T(ReturnType, data_file_result);
                result_files.push_back(data_file_result.value());
            }
        } else {
            // No partitioning, process the whole batch
            auto partition_values_result = DataIngestorUtil::ExtractPartitionValues(current_metadata_, batch);
            RETURN_IF_ERROR_T(ReturnType, partition_values_result);
            std::unordered_map<std::string, std::string> partition_values = partition_values_result.value();

            // Use the common method
            auto data_file_result = WriteBatchToFile(batch, partition_values);
            RETURN_IF_ERROR_T(ReturnType, data_file_result);
            result_files.push_back(data_file_result.value());
        }
    }

    // If we didn't create any files, return an error
    if (result_files.empty()) {
        return common::Error(common::ErrorCode::InvalidOperation, "No data files were created");
    }

    if (commit_after_write) {
        RETURN_IF_ERROR_T(ReturnType, Commit());
    }

    return ReturnType::success(result_files);
}

common::Result<DataFile> DataIngestor::IngestTable(const std::shared_ptr<arrow::Table>& table,
                                                   bool commit_after_write) {
    using ReturnType = common::Result<DataFile>;

    // Validate schema
    auto schema_validation = ValidateSchema(table->schema());
    RETURN_IF_ERROR_T(ReturnType, schema_validation);

    // Check if table has rows
    if (table->num_rows() == 0) {
        return common::Error(common::ErrorCode::InvalidOperation, "Cannot ingest empty table");
    }

    // If we have partition specs, we need to convert the table to record batches and use IngestBatches
    if (!current_metadata_.partition_specs.empty()) {
        // Convert table to record batches
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches;

        // Create a record batch reader from the table
        auto reader = std::make_shared<arrow::TableBatchReader>(*table);
        std::shared_ptr<arrow::RecordBatch> batch;

        // Read all batches
        while (reader->ReadNext(&batch).ok() && batch != nullptr) {
            batches.push_back(batch);
        }

        // Use IngestBatches to handle partitioning
        auto result = IngestBatches(batches, commit_after_write);
        RETURN_IF_ERROR_T(ReturnType, result);
        if (result.value().empty()) {
            return common::Error(common::ErrorCode::InvalidOperation, "No data files were created");
        }

        // Return the first file for backward compatibility
        return ReturnType::success(result.value()[0]);
    } else {
        // No partitioning, process the whole table directly
        // Create a record batch from the first row of the table to extract partition values
        std::vector<std::shared_ptr<arrow::Array>> columns;
        for (int i = 0; i < table->num_columns(); i++) {
            auto chunked_array = table->column(i);
            if (chunked_array->num_chunks() == 0) {
                return common::Error(common::ErrorCode::InvalidOperation,
                                     "Column " + std::to_string(i) + " has no chunks");
            }
            // Get the first chunk and slice to get just the first row
            columns.push_back(chunked_array->chunk(0)->Slice(0, 1));
        }
        std::shared_ptr<arrow::RecordBatch> batch = arrow::RecordBatch::Make(table->schema(), 1, columns);

        auto partition_values_result = DataIngestorUtil::ExtractPartitionValues(current_metadata_, batch);
        RETURN_IF_ERROR_T(ReturnType, partition_values_result);
        std::unordered_map<std::string, std::string> partition_values = partition_values_result.value();

        // Convert table to a single record batch for writing
        auto reader = std::make_shared<arrow::TableBatchReader>(*table);
        std::shared_ptr<arrow::RecordBatch> full_batch;
        auto read_result = reader->ReadNext(&full_batch);
        if (!read_result.ok()) {
            return common::Error(common::ErrorCode::InvalidOperation, "Failed to read table");
        }

        // Use the common method
        auto data_file_result = WriteBatchToFile(full_batch, partition_values);
        RETURN_IF_ERROR_T(ReturnType, data_file_result);
        auto data_file = data_file_result.value();

        if (commit_after_write) {
            RETURN_IF_ERROR_T(ReturnType, Commit());
        }

        return ReturnType::success(data_file);
    }
}

common::Result<bool> DataIngestor::Commit() {
    using ReturnType = common::Result<bool>;

    if (pending_files_.empty()) {
        return ReturnType::success(true);
    }

    auto snapshot_result = catalog_->CreateSnapshot(table_name_,
                                                    pending_files_,
                                                    {},  // No deleted files
                                                    Operation::APPEND);

    RETURN_IF_ERROR_T(ReturnType, snapshot_result);
    current_metadata_ = snapshot_result.value();

    LOG_VERBOSE("Committed ingested files for table %s, snapshot id: %d, first file: %s, pending files: %d",
                table_name_.c_str(),
                current_metadata_.current_snapshot_id,
                pending_files_.empty() ? "None" : pending_files_.front().file_path.c_str(),
                pending_files_.size());

    pending_files_.clear();

    return ReturnType::success(true);
}

common::Result<std::string> DataIngestor::GenerateDataFilePath(
    const std::unordered_map<std::string, std::string>& partition_values) {
    // Start with the base path from metadata
    std::string base_path = current_metadata_.location;

    // If base path doesn't start with /, add it
    if (!base_path.empty() && base_path[0] != '/') {
        base_path = "/" + base_path;
    }

    // Ensure base path ends with /
    if (!base_path.empty() && base_path.back() != '/') {
        base_path += '/';
    }

    // Create partition path
    std::string partition_path = "data";
    for (const auto& [key, value] : partition_values) {
        partition_path += "/" + key + "=" + value;
    }

    // Generate unique file name using snapshot ID and pending files count
    std::string file_name = "part_" + std::to_string(current_metadata_.current_snapshot_id) + "_"
                            + std::to_string(pending_files_.size()) + ".parquet";

    // Combine all parts
    return common::Result<std::string>::success(base_path + partition_path + "/" + file_name);
}

common::Result<std::unique_ptr<format::ParquetWriter>> DataIngestor::CreateWriter(const std::string& file_path) {
    auto arrow_schema_result = format::SchemaConverter::ToArrowSchema(*current_metadata_.schema);
    RETURN_IF_ERROR_T(common::Result<std::unique_ptr<format::ParquetWriter>>, arrow_schema_result);
    return format::ParquetWriter::Create(fs_, file_path, arrow_schema_result.value());
}

common::Result<DataFile> DataIngestor::FinalizeDataFile(
    const std::string& file_path,
    int64_t num_records,
    const std::unordered_map<std::string, std::string>& partition_values) {
    using ReturnType = common::Result<DataFile>;

    DataFile file;
    file.file_path = file_path;
    file.format = FileFormat::PARQUET;
    file.record_count = num_records;
    file.partition_values = partition_values;

    // Get file size from filesystem
    auto handle_result = fs_->OpenFile(file_path);
    RETURN_IF_ERROR_T(ReturnType, handle_result);
    auto handle = handle_result.value();

    auto size_result = fs_->Size(handle);
    RETURN_IF_ERROR_T(ReturnType, size_result);
    file.file_size_bytes = size_result.value();

    auto close_result = fs_->CloseFile(handle);
    RETURN_IF_ERROR_T(ReturnType, close_result);
    return ReturnType::success(std::move(file));
}

common::Result<void> DataIngestor::ValidateSchema(const std::shared_ptr<arrow::Schema>& input_schema) const {
    return format::SchemaConverter::ValidateSchema(input_schema, current_metadata_.schema);
}

common::Result<DataFile> DataIngestor::WriteBatchToFile(
    const std::shared_ptr<arrow::RecordBatch>& batch,
    const std::unordered_map<std::string, std::string>& partition_values) {
    using ReturnType = common::Result<DataFile>;

    // Validate nullability constraints first
    auto schema = current_metadata_.schema;
    for (int i = 0; i < schema->FieldCount(); i++) {
        auto field = schema->Fields()[i];
        if (field.nullability == common::Nullability::NOT_NULL) {
            auto array = batch->column(i);
            if (array->null_count() > 0) {
                return common::Error(common::ErrorCode::ParquetInvalidNullability,
                                     "Column '" + field.name + "' is declared non-nullable but contains "
                                         + std::to_string(array->null_count()) + " null values");
            }
        }
    }

    // Generate file path
    auto file_path_result = GenerateDataFilePath(partition_values);
    RETURN_IF_ERROR_T(ReturnType, file_path_result);
    auto file_path = file_path_result.value();

    // Create writer
    auto writer_result = CreateWriter(file_path);
    RETURN_IF_ERROR_T(ReturnType, writer_result);
    auto writer = std::move(writer_result).value();

    // Write the batch
    auto write_result = writer->Write({batch});
    RETURN_IF_ERROR_T(ReturnType, write_result);
    auto num_records = writer->NumRows();

    // Close the writer
    auto close_result = writer->Close();
    RETURN_IF_ERROR_T(ReturnType, close_result);

    // Finalize the data file
    auto data_file_result = FinalizeDataFile(file_path, num_records, partition_values);
    RETURN_IF_ERROR_T(ReturnType, data_file_result);
    auto data_file = std::move(data_file_result).value();

    // Add to pending files
    pending_files_.push_back(data_file);

    LOG_STATUS("Wrote %d records to %s", num_records, file_path.c_str());

    return ReturnType::success(data_file);
}

}  // namespace pond::catalog