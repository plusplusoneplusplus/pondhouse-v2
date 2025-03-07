#pragma once

#include <memory>
#include <string>
#include <vector>

#include <arrow/api.h>
#include <parquet/arrow/reader.h>

#include "common/append_only_fs.h"
#include "common/result.h"

namespace pond::format {

class ParquetReader {
public:
    // Create a new ParquetReader instance
    static common::Result<std::unique_ptr<ParquetReader>> Create(std::shared_ptr<common::IAppendOnlyFileSystem> fs,
                                                                 const std::string& path);

    ~ParquetReader();

    // Get the schema of the Parquet file
    common::Result<std::shared_ptr<arrow::Schema>> Schema() const;

    // Get the number of row groups in the file
    common::Result<int> NumRowGroups() const;

    // Read a specific row group into an Arrow table
    common::Result<std::shared_ptr<arrow::Table>> ReadRowGroup(int row_group);

    // Read specific columns from a row group
    common::Result<std::shared_ptr<arrow::Table>> ReadRowGroup(int row_group, const std::vector<int>& column_indices);

    // Read the entire file into an Arrow table
    common::Result<std::shared_ptr<arrow::Table>> Read();

    // Read specific columns from the entire file
    common::Result<std::shared_ptr<arrow::Table>> Read(const std::vector<int>& column_indices);

    // Get a RecordBatchReader that can iterate through the entire file
    common::Result<std::shared_ptr<arrow::RecordBatchReader>> GetBatchReader();

    // Get a RecordBatchReader for specific columns
    common::Result<std::shared_ptr<arrow::RecordBatchReader>> GetBatchReader(const std::vector<int>& column_indices);

    // Read a specific row group as a RecordBatch
    common::Result<std::shared_ptr<arrow::RecordBatch>> ReadRowGroupAsBatch(int row_group);

    // Read specific columns from a row group as a RecordBatch
    common::Result<std::shared_ptr<arrow::RecordBatch>> ReadRowGroupAsBatch(int row_group,
                                                                            const std::vector<int>& column_indices);

private:
    ParquetReader(std::shared_ptr<arrow::io::RandomAccessFile> input,
                  std::unique_ptr<parquet::arrow::FileReader> reader);

    std::shared_ptr<arrow::io::RandomAccessFile> input_;
    std::unique_ptr<parquet::arrow::FileReader> reader_;
};

}  // namespace pond::format
