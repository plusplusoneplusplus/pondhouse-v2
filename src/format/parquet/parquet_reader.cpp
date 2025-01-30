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

Result<std::unique_ptr<ParquetReader>> ParquetReader::create(
    std::shared_ptr<IAppendOnlyFileSystem> fs,
    const std::string& path) {
    // Open the file
    auto result = fs->openFile(path);
    if (!result.ok()) {
        return Result<std::unique_ptr<ParquetReader>>::failure(result.error());
    }

    // Create input stream
    auto input = std::make_shared<AppendOnlyInputStream>(fs, result.value());

    // Create Parquet reader
    std::unique_ptr<parquet::arrow::FileReader> reader;
    auto status = parquet::arrow::OpenFile(input, arrow::default_memory_pool(), &reader);
    if (!status.ok()) {
        return Result<std::unique_ptr<ParquetReader>>::failure(
            ErrorCode::ParquetReaderNotInitialized,
            "Failed to open Parquet file: " + status.ToString());
    }

    return Result<std::unique_ptr<ParquetReader>>::success(
        std::unique_ptr<ParquetReader>(new ParquetReader(input, std::move(reader))));
}

Result<std::shared_ptr<arrow::Schema>> ParquetReader::schema() const {
    std::shared_ptr<arrow::Schema> out_schema;
    auto status = reader_->GetSchema(&out_schema);
    if (!status.ok()) {
        return Result<std::shared_ptr<arrow::Schema>>::failure(
            ErrorCode::ParquetSchemaError,
            "Failed to get schema: " + status.ToString());
    }
    return Result<std::shared_ptr<arrow::Schema>>::success(out_schema);
}

Result<int> ParquetReader::num_row_groups() const {
    return Result<int>::success(reader_->num_row_groups());
}

Result<std::shared_ptr<arrow::Table>> ParquetReader::read_row_group(int row_group) {
    std::shared_ptr<arrow::Table> table;
    auto status = reader_->ReadRowGroup(row_group, &table);
    if (!status.ok()) {
        return Result<std::shared_ptr<arrow::Table>>::failure(
            ErrorCode::ParquetReadFailed,
            "Failed to read row group: " + status.ToString());
    }
    return Result<std::shared_ptr<arrow::Table>>::success(table);
}

Result<std::shared_ptr<arrow::Table>> ParquetReader::read_row_group(
    int row_group,
    const std::vector<int>& column_indices) {
    std::shared_ptr<arrow::Table> table;
    auto status = reader_->ReadRowGroup(row_group, column_indices, &table);
    if (!status.ok()) {
        return Result<std::shared_ptr<arrow::Table>>::failure(
            ErrorCode::ParquetReadFailed,
            "Failed to read row group columns: " + status.ToString());
    }
    return Result<std::shared_ptr<arrow::Table>>::success(table);
}

Result<std::shared_ptr<arrow::Table>> ParquetReader::read() {
    std::shared_ptr<arrow::Table> table;
    auto status = reader_->ReadTable(&table);
    if (!status.ok()) {
        return Result<std::shared_ptr<arrow::Table>>::failure(
            ErrorCode::ParquetReadFailed,
            "Failed to read table: " + status.ToString());
    }
    return Result<std::shared_ptr<arrow::Table>>::success(table);
}

Result<std::shared_ptr<arrow::Table>> ParquetReader::read(
    const std::vector<int>& column_indices) {
    std::shared_ptr<arrow::Table> table;
    auto status = reader_->ReadTable(column_indices, &table);
    if (!status.ok()) {
        return Result<std::shared_ptr<arrow::Table>>::failure(
            ErrorCode::ParquetReadFailed,
            "Failed to read table columns: " + status.ToString());
    }
    return Result<std::shared_ptr<arrow::Table>>::success(table);
}

} // namespace pond::format
