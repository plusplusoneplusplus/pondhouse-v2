#include "parquet_reader.h"

#include "append_only_input_stream.h"
#include "common/log.h"

namespace pond::format {

using namespace pond::common;

ParquetReader::ParquetReader(std::shared_ptr<arrow::io::RandomAccessFile> input,
                             std::unique_ptr<parquet::arrow::FileReader> reader)
    : input_(std::move(input)), reader_(std::move(reader)) {}

ParquetReader::~ParquetReader() {
    reader_.reset();
    input_.reset();
}

Result<std::unique_ptr<ParquetReader>> ParquetReader::Create(std::shared_ptr<IAppendOnlyFileSystem> fs,
                                                             const std::string& path) {
    // Open the file for reading
    auto file_result = fs->OpenFile(path, false);
    if (!file_result.ok()) {
        return Result<std::unique_ptr<ParquetReader>>::failure(file_result.error());
    }

    // Create input stream
    auto input = std::make_shared<AppendOnlyInputStream>(fs, file_result.value());

    // Create Parquet reader
    std::unique_ptr<parquet::arrow::FileReader> reader;
    auto status = parquet::arrow::OpenFile(input, arrow::default_memory_pool(), &reader);
    if (!status.ok()) {
        return Result<std::unique_ptr<ParquetReader>>::failure(ErrorCode::ParquetReaderNotInitialized,
                                                               "Failed to open Parquet file: " + status.ToString());
    }

    return Result<std::unique_ptr<ParquetReader>>::success(
        std::unique_ptr<ParquetReader>(new ParquetReader(input, std::move(reader))));
}

Result<std::shared_ptr<arrow::Schema>> ParquetReader::Schema() const {
    std::shared_ptr<arrow::Schema> out_schema;
    auto status = reader_->GetSchema(&out_schema);
    if (!status.ok()) {
        return Result<std::shared_ptr<arrow::Schema>>::failure(ErrorCode::ParquetSchemaError,
                                                               "Failed to get schema: " + status.ToString());
    }
    return Result<std::shared_ptr<arrow::Schema>>::success(out_schema);
}

Result<int> ParquetReader::NumRowGroups() const {
    return Result<int>::success(reader_->num_row_groups());
}

Result<std::shared_ptr<arrow::Table>> ParquetReader::ReadRowGroup(int row_group) {
    std::shared_ptr<arrow::Table> table;
    auto status = reader_->ReadRowGroup(row_group, &table);
    if (!status.ok()) {
        return Result<std::shared_ptr<arrow::Table>>::failure(ErrorCode::ParquetReadFailed,
                                                              "Failed to read row group: " + status.ToString());
    }
    return Result<std::shared_ptr<arrow::Table>>::success(table);
}

Result<std::shared_ptr<arrow::Table>> ParquetReader::ReadRowGroup(int row_group,
                                                                  const std::vector<int>& column_indices) {
    std::shared_ptr<arrow::Table> table;
    auto status = reader_->ReadRowGroup(row_group, column_indices, &table);
    if (!status.ok()) {
        return Result<std::shared_ptr<arrow::Table>>::failure(ErrorCode::ParquetReadFailed,
                                                              "Failed to read row group columns: " + status.ToString());
    }
    return Result<std::shared_ptr<arrow::Table>>::success(table);
}

Result<std::shared_ptr<arrow::Table>> ParquetReader::Read() {
    std::shared_ptr<arrow::Table> table;
    auto status = reader_->ReadTable(&table);
    if (!status.ok()) {
        return Result<std::shared_ptr<arrow::Table>>::failure(ErrorCode::ParquetReadFailed,
                                                              "Failed to read table: " + status.ToString());
    }
    return Result<std::shared_ptr<arrow::Table>>::success(table);
}

Result<std::shared_ptr<arrow::Table>> ParquetReader::Read(const std::vector<int>& column_indices) {
    std::shared_ptr<arrow::Table> table;
    auto status = reader_->ReadTable(column_indices, &table);
    if (!status.ok()) {
        return Result<std::shared_ptr<arrow::Table>>::failure(ErrorCode::ParquetReadFailed,
                                                              "Failed to read table columns: " + status.ToString());
    }
    return Result<std::shared_ptr<arrow::Table>>::success(table);
}

Result<std::shared_ptr<arrow::RecordBatchReader>> ParquetReader::GetBatchReader() {
    // Get the number of row groups
    int num_row_groups = reader_->num_row_groups();
    if (num_row_groups <= 0) {
        return Result<std::shared_ptr<arrow::RecordBatchReader>>::failure(ErrorCode::ParquetReadFailed,
                                                                          "No row groups found in the Parquet file");
    }

    // Create a vector of row group indices
    std::vector<int> row_groups;
    for (int i = 0; i < num_row_groups; i++) {
        row_groups.push_back(i);
    }

    // Get a batch reader for all row groups
    std::shared_ptr<arrow::RecordBatchReader> reader;
    auto status = reader_->GetRecordBatchReader(row_groups, &reader);
    if (!status.ok()) {
        return Result<std::shared_ptr<arrow::RecordBatchReader>>::failure(
            ErrorCode::ParquetReadFailed, "Failed to create RecordBatchReader: " + status.ToString());
    }
    return Result<std::shared_ptr<arrow::RecordBatchReader>>::success(reader);
}

Result<std::shared_ptr<arrow::RecordBatchReader>> ParquetReader::GetBatchReader(
    const std::vector<int>& column_indices) {
    // Get the number of row groups
    int num_row_groups = reader_->num_row_groups();
    if (num_row_groups <= 0) {
        return Result<std::shared_ptr<arrow::RecordBatchReader>>::failure(ErrorCode::ParquetReadFailed,
                                                                          "No row groups found in the Parquet file");
    }

    // Create a vector of row group indices
    std::vector<int> row_groups;
    for (int i = 0; i < num_row_groups; i++) {
        row_groups.push_back(i);
    }

    // Get a batch reader for all row groups with selected columns
    std::shared_ptr<arrow::RecordBatchReader> reader;
    auto status = reader_->GetRecordBatchReader(row_groups, column_indices, &reader);
    if (!status.ok()) {
        return Result<std::shared_ptr<arrow::RecordBatchReader>>::failure(
            ErrorCode::ParquetReadFailed, "Failed to create RecordBatchReader for columns: " + status.ToString());
    }
    return Result<std::shared_ptr<arrow::RecordBatchReader>>::success(reader);
}

Result<std::shared_ptr<arrow::RecordBatch>> ParquetReader::ReadRowGroupAsBatch(int row_group) {
    std::shared_ptr<arrow::Table> table;
    auto status = reader_->ReadRowGroup(row_group, &table);
    if (!status.ok()) {
        return Result<std::shared_ptr<arrow::RecordBatch>>::failure(
            ErrorCode::ParquetReadFailed, "Failed to read row group as batch: " + status.ToString());
    }

    // Convert table to record batch
    auto batch_result = arrow::TableBatchReader(*table).Next();
    if (!batch_result.ok()) {
        return Result<std::shared_ptr<arrow::RecordBatch>>::failure(
            ErrorCode::ParquetReadFailed,
            "Failed to convert table to record batch: " + batch_result.status().ToString());
    }

    auto batch = batch_result.ValueOrDie();
    if (batch == nullptr) {
        return Result<std::shared_ptr<arrow::RecordBatch>>::failure(ErrorCode::ParquetReadFailed,
                                                                    "Row group converted to empty batch");
    }

    return Result<std::shared_ptr<arrow::RecordBatch>>::success(batch);
}

Result<std::shared_ptr<arrow::RecordBatch>> ParquetReader::ReadRowGroupAsBatch(int row_group,
                                                                               const std::vector<int>& column_indices) {
    std::shared_ptr<arrow::Table> table;
    auto status = reader_->ReadRowGroup(row_group, column_indices, &table);
    if (!status.ok()) {
        return Result<std::shared_ptr<arrow::RecordBatch>>::failure(
            ErrorCode::ParquetReadFailed, "Failed to read row group columns as batch: " + status.ToString());
    }

    // Convert table to record batch
    auto batch_result = arrow::TableBatchReader(*table).Next();
    if (!batch_result.ok()) {
        return Result<std::shared_ptr<arrow::RecordBatch>>::failure(
            ErrorCode::ParquetReadFailed,
            "Failed to convert table to record batch: " + batch_result.status().ToString());
    }

    auto batch = batch_result.ValueOrDie();
    if (batch == nullptr) {
        return Result<std::shared_ptr<arrow::RecordBatch>>::failure(ErrorCode::ParquetReadFailed,
                                                                    "Row group converted to empty batch");
    }

    return Result<std::shared_ptr<arrow::RecordBatch>>::success(batch);
}

}  // namespace pond::format
