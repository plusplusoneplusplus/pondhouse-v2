#include "parquet_writer.h"

#include "append_only_output_stream.h"
#include "common/log.h"

namespace pond::format {

using namespace pond::common;

ParquetWriter::ParquetWriter(std::shared_ptr<arrow::io::OutputStream> output,
                           std::unique_ptr<parquet::arrow::FileWriter> writer)
    : output_(std::move(output)), writer_(std::move(writer)), closed_(false), total_rows_(0) {}

ParquetWriter::~ParquetWriter() {
    if (!closed_) {
        auto result = close();
        if (!result.ok()) {
            LOG_ERROR("Failed to close Parquet writer: %s", result.error().message().c_str());
        }
    }
}

Result<std::unique_ptr<ParquetWriter>> ParquetWriter::create(
    std::shared_ptr<IAppendOnlyFileSystem> fs,
    const std::string& path,
    const std::shared_ptr<arrow::Schema>& schema,
    parquet::WriterProperties::Builder properties) {
    // Open the file for writing
    auto file_result = fs->openFile(path, true);
    if (!file_result.ok()) {
        return Result<std::unique_ptr<ParquetWriter>>::failure(file_result.error());
    }

    // verify schema
    if (schema->num_fields() == 0) {
        return Result<std::unique_ptr<ParquetWriter>>::failure(
            ErrorCode::InvalidArgument, "Schema must not be empty");
    }

    // Create output stream
    auto output = std::make_shared<AppendOnlyOutputStream>(fs, file_result.value());

    // Create Parquet writer
    auto writer_props = properties.build();
    auto arrow_props = parquet::ArrowWriterProperties::Builder().build();
    
    auto writer_result = parquet::arrow::FileWriter::Open(
        *schema,
        arrow::default_memory_pool(),
        output,
        writer_props,
        arrow_props);

    if (!writer_result.ok()) {
        return Result<std::unique_ptr<ParquetWriter>>::failure(
            ErrorCode::ParquetWriterNotInitialized,
            "Failed to create Parquet writer: " + writer_result.status().ToString());
    }

    return Result<std::unique_ptr<ParquetWriter>>::success(
        std::unique_ptr<ParquetWriter>(new ParquetWriter(output, std::move(writer_result).ValueOrDie())));
}

Result<bool> ParquetWriter::write(const std::shared_ptr<arrow::Table>& table) {
    if (closed_) {
        return Result<bool>::failure(ErrorCode::InvalidOperation, "Writer is closed");
    }

    auto status = writer_->WriteTable(*table, table->num_rows());
    if (!status.ok()) {
        return Result<bool>::failure(
            ErrorCode::ParquetWriteFailed,
            "Failed to write table: " + status.ToString());
    }

    total_rows_ += table->num_rows();
    return Result<bool>::success(true);
}

Result<bool> ParquetWriter::write(const std::shared_ptr<arrow::RecordBatch>& batch) {
    if (closed_) {
        return Result<bool>::failure(ErrorCode::InvalidOperation, "Writer is closed");
    }

    auto status = writer_->WriteRecordBatch(*batch);
    if (!status.ok()) {
        return Result<bool>::failure(
            ErrorCode::ParquetWriteFailed,
            "Failed to write record batch: " + status.ToString());
    }

    total_rows_ += batch->num_rows();
    return Result<bool>::success(true);
}

Result<bool> ParquetWriter::write(
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches) {
    if (closed_) {
        return Result<bool>::failure(ErrorCode::InvalidOperation, "Writer is closed");
    }

    for (const auto& batch : batches) {
        auto result = write(batch);
        if (!result.ok()) {
            return result;
        }
    }

    return Result<bool>::success(true);
}

Result<bool> ParquetWriter::close() {
    if (closed_) {
        return Result<bool>::success(true);
    }

    auto status = writer_->Close();
    if (!status.ok()) {
        return Result<bool>::failure(
            ErrorCode::ParquetWriteFailed,
            "Failed to close writer: " + status.ToString());
    }

    closed_ = true;
    return Result<bool>::success(true);
}

std::shared_ptr<arrow::Schema> ParquetWriter::schema() const {
    return writer_->schema();
}

} // namespace pond::format
