#pragma once

#include <memory>
#include <string>

#include <arrow/api.h>
#include <parquet/arrow/writer.h>

#include "common/append_only_fs.h"
#include "common/result.h"

namespace pond::format {

class ParquetWriter {
public:
    // Create a new ParquetWriter instance
    static common::Result<std::unique_ptr<ParquetWriter>> create(
        std::shared_ptr<common::IAppendOnlyFileSystem> fs,
        const std::string& path,
        const std::shared_ptr<arrow::Schema>& schema,
        parquet::WriterProperties::Builder properties = parquet::WriterProperties::Builder());

    ~ParquetWriter();

    // Write a table to the Parquet file
    common::Result<bool> write(const std::shared_ptr<arrow::Table>& table);

    // Write a record batch to the Parquet file
    common::Result<bool> write(const std::shared_ptr<arrow::RecordBatch>& batch);

    // Write multiple record batches to the Parquet file
    common::Result<bool> write(const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches);

    // Close the writer and finalize the file
    common::Result<bool> close();

    // Get the current number of rows written
    int64_t num_rows() const { return total_rows_; }

    // Get the schema of the writer
    std::shared_ptr<arrow::Schema> schema() const;

private:
    ParquetWriter(std::shared_ptr<arrow::io::OutputStream> output,
                 std::unique_ptr<parquet::arrow::FileWriter> writer);

    std::shared_ptr<arrow::io::OutputStream> output_;
    std::unique_ptr<parquet::arrow::FileWriter> writer_;
    bool closed_;
    int64_t total_rows_;
};

} // namespace pond::format
