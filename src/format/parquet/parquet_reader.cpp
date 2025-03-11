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

Result<std::vector<int>> ParquetReader::ConvertColumnNamesToIndices(const std::vector<std::string>& column_names) {
    // If empty list provided, return empty vector (means all columns)
    if (column_names.empty()) {
        return Result<std::vector<int>>::success(std::vector<int>());
    }

    std::string column_names_str;
    for (const auto& name : column_names) {
        column_names_str += name + ", ";
    }
    LOG_VERBOSE("Converting column names to indices: %s", column_names_str.c_str());

    // Get the schema first
    auto schema_result = Schema();
    if (!schema_result.ok()) {
        return Result<std::vector<int>>::failure(
            schema_result.error().code(),
            "Failed to get schema for column name mapping: " + schema_result.error().message());
    }

    auto schema = schema_result.value();

    // Convert column names to indices
    std::vector<int> column_indices;
    for (const auto& name : column_names) {
        int idx = schema->GetFieldIndex(name);
        if (idx != -1) {  // -1 means column not found
            column_indices.push_back(idx);
        } else {
            LOG_WARNING("Column not found in schema: %s", name.c_str());
        }
    }

    // If no valid columns were found, return an error
    if (column_indices.empty()) {
        return Result<std::vector<int>>::failure(
            ErrorCode::InvalidArgument,
            "None of the specified column names were found in the schema: " + column_names_str);
    }

    return Result<std::vector<int>>::success(column_indices);
}

Result<std::shared_ptr<arrow::Table>> ParquetReader::ReadRowGroup(int row_group,
                                                                  const std::vector<std::string>& column_names) {
    // Convert column names to indices
    auto indices_result = ConvertColumnNamesToIndices(column_names);
    if (!indices_result.ok()) {
        return Result<std::shared_ptr<arrow::Table>>::failure(indices_result.error());
    }

    // Call the existing method with the indices
    return ReadRowGroup(row_group, indices_result.value());
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

Result<std::shared_ptr<arrow::Table>> ParquetReader::Read(const std::vector<std::string>& column_names) {
    // Convert column names to indices
    auto indices_result = ConvertColumnNamesToIndices(column_names);
    if (!indices_result.ok()) {
        return Result<std::shared_ptr<arrow::Table>>::failure(indices_result.error());
    }

    // Call the existing method with the indices
    return Read(indices_result.value());
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

Result<std::shared_ptr<arrow::RecordBatchReader>> ParquetReader::GetBatchReader(
    const std::vector<std::string>& column_names) {
    // Convert column names to indices
    auto indices_result = ConvertColumnNamesToIndices(column_names);
    if (!indices_result.ok()) {
        return Result<std::shared_ptr<arrow::RecordBatchReader>>::failure(indices_result.error());
    }

    // Call the existing method with the indices
    return GetBatchReader(indices_result.value());
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

Result<std::shared_ptr<arrow::RecordBatch>> ParquetReader::ReadRowGroupAsBatch(
    int row_group, const std::vector<std::string>& column_names) {
    // Convert column names to indices
    auto indices_result = ConvertColumnNamesToIndices(column_names);
    if (!indices_result.ok()) {
        return Result<std::shared_ptr<arrow::RecordBatch>>::failure(indices_result.error());
    }

    // Call the existing method with the indices
    return ReadRowGroupAsBatch(row_group, indices_result.value());
}

}  // namespace pond::format
