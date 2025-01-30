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
    static common::Result<std::unique_ptr<ParquetReader>> create(
        std::shared_ptr<common::IAppendOnlyFileSystem> fs,
        const std::string& path);

    ~ParquetReader();

    // Get the schema of the Parquet file
    common::Result<std::shared_ptr<arrow::Schema>> schema() const;

    // Get the number of row groups in the file
    common::Result<int> num_row_groups() const;

    // Read a specific row group into an Arrow table
    common::Result<std::shared_ptr<arrow::Table>> read_row_group(int row_group);

    // Read specific columns from a row group
    common::Result<std::shared_ptr<arrow::Table>> read_row_group(
        int row_group,
        const std::vector<int>& column_indices);

    // Read the entire file into an Arrow table
    common::Result<std::shared_ptr<arrow::Table>> read();

    // Read specific columns from the entire file
    common::Result<std::shared_ptr<arrow::Table>> read(
        const std::vector<int>& column_indices);

private:
    ParquetReader(std::shared_ptr<arrow::io::RandomAccessFile> input,
                 std::unique_ptr<parquet::arrow::FileReader> reader);

    std::shared_ptr<arrow::io::RandomAccessFile> input_;
    std::unique_ptr<parquet::arrow::FileReader> reader_;
};

} // namespace pond::format
