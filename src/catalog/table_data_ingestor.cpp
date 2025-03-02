#include "catalog/table_data_ingestor.h"

#include "common/result.h"
#include "format/parquet/schema_converter.h"

namespace pond::catalog {

common::Result<std::unique_ptr<TableDataIngestor>> TableDataIngestor::Create(
    std::shared_ptr<Catalog> catalog,
    std::shared_ptr<common::IAppendOnlyFileSystem> fs,
    const std::string& table_name) {
    using ReturnType = common::Result<std::unique_ptr<TableDataIngestor>>;

    auto metadata_result = catalog->LoadTable(table_name);
    RETURN_IF_ERROR_T(ReturnType, metadata_result);

    return ReturnType::success(
        std::unique_ptr<TableDataIngestor>(new TableDataIngestor(catalog, fs, table_name, metadata_result.value())));
}

TableDataIngestor::TableDataIngestor(std::shared_ptr<Catalog> catalog,
                                     std::shared_ptr<common::IAppendOnlyFileSystem> fs,
                                     const std::string& table_name,
                                     TableMetadata metadata)
    : catalog_(std::move(catalog)),
      fs_(std::move(fs)),
      table_name_(table_name),
      current_metadata_(std::move(metadata)) {}

common::Result<bool> TableDataIngestor::IngestBatch(const std::shared_ptr<arrow::RecordBatch>& batch,
                                                    bool commit_after_write) {
    using ReturnType = common::Result<bool>;

    // Validate schema before proceeding
    auto schema_validation = ValidateSchema(batch->schema());
    RETURN_IF_ERROR_T(ReturnType, schema_validation);

    auto file_path_result = GenerateDataFilePath();
    RETURN_IF_ERROR_T(ReturnType, file_path_result);
    auto file_path = file_path_result.value();

    auto writer_result = CreateWriter(file_path);
    RETURN_IF_ERROR_T(ReturnType, writer_result);
    auto writer = std::move(writer_result).value();

    auto write_result = writer->write(batch);
    RETURN_IF_ERROR_T(ReturnType, write_result);
    auto num_records = writer->num_rows();

    auto close_result = writer->close();
    RETURN_IF_ERROR_T(ReturnType, close_result);

    auto data_file_result = FinalizeDataFile(file_path, num_records);
    RETURN_IF_ERROR_T(ReturnType, data_file_result);
    pending_files_.push_back(std::move(data_file_result).value());

    if (commit_after_write) {
        return Commit();
    }
    return ReturnType::success(true);
}

common::Result<bool> TableDataIngestor::IngestBatches(const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
                                                      bool commit_after_write) {
    using ReturnType = common::Result<bool>;

    if (batches.empty()) {
        return ReturnType::success(true);
    }

    // Validate schema using the first batch's schema
    auto schema_validation = ValidateSchema(batches[0]->schema());
    RETURN_IF_ERROR_T(ReturnType, schema_validation);

    auto file_path_result = GenerateDataFilePath();
    RETURN_IF_ERROR_T(ReturnType, file_path_result);
    auto file_path = file_path_result.value();

    auto writer_result = CreateWriter(file_path);
    RETURN_IF_ERROR_T(ReturnType, writer_result);
    auto writer = std::move(writer_result).value();

    auto write_result = writer->write(batches);
    RETURN_IF_ERROR_T(ReturnType, write_result);
    auto num_records = writer->num_rows();

    auto close_result = writer->close();
    RETURN_IF_ERROR_T(ReturnType, close_result);

    auto data_file_result = FinalizeDataFile(file_path, num_records);
    RETURN_IF_ERROR_T(ReturnType, data_file_result);
    pending_files_.push_back(std::move(data_file_result).value());

    if (commit_after_write) {
        return Commit();
    }
    return ReturnType::success(true);
}

common::Result<bool> TableDataIngestor::IngestTable(const std::shared_ptr<arrow::Table>& table,
                                                    bool commit_after_write) {
    using ReturnType = common::Result<bool>;

    // Validate schema before proceeding
    auto schema_validation = ValidateSchema(table->schema());
    RETURN_IF_ERROR_T(ReturnType, schema_validation);

    auto file_path_result = GenerateDataFilePath();
    RETURN_IF_ERROR_T(ReturnType, file_path_result);
    auto file_path = file_path_result.value();

    auto writer_result = CreateWriter(file_path);
    RETURN_IF_ERROR_T(ReturnType, writer_result);
    auto writer = std::move(writer_result).value();

    auto write_result = writer->write(table);
    RETURN_IF_ERROR_T(ReturnType, write_result);
    auto num_records = writer->num_rows();

    auto close_result = writer->close();
    RETURN_IF_ERROR_T(ReturnType, close_result);

    auto data_file_result = FinalizeDataFile(file_path, num_records);
    RETURN_IF_ERROR_T(ReturnType, data_file_result);
    pending_files_.push_back(std::move(data_file_result).value());

    if (commit_after_write) {
        return Commit();
    }
    return ReturnType::success(true);
}

common::Result<bool> TableDataIngestor::Commit() {
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
    pending_files_.clear();
    return ReturnType::success(true);
}

common::Result<std::string> TableDataIngestor::GenerateDataFilePath() {
    std::string base_path = current_metadata_.location;
    if (!base_path.empty() && base_path.back() != '/') {
        base_path += '/';
    }
    std::string file_name = "data_" + std::to_string(current_metadata_.current_snapshot_id) + "_"
                            + std::to_string(pending_files_.size()) + ".parquet";
    return common::Result<std::string>::success(base_path + "data/" + file_name);
}

common::Result<std::unique_ptr<format::ParquetWriter>> TableDataIngestor::CreateWriter(const std::string& file_path) {
    auto arrow_schema_result = format::SchemaConverter::ToArrowSchema(*current_metadata_.schema);
    RETURN_IF_ERROR_T(common::Result<std::unique_ptr<format::ParquetWriter>>, arrow_schema_result);
    return format::ParquetWriter::create(fs_, file_path, arrow_schema_result.value());
}

common::Result<DataFile> TableDataIngestor::FinalizeDataFile(const std::string& file_path, int64_t num_records) {
    using ReturnType = common::Result<DataFile>;

    DataFile file;
    file.file_path = file_path;
    file.format = FileFormat::PARQUET;
    file.record_count = num_records;

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

common::Result<void> TableDataIngestor::ValidateSchema(const std::shared_ptr<arrow::Schema>& input_schema) const {
    return format::SchemaConverter::ValidateSchema(input_schema, current_metadata_.schema);
}

}  // namespace pond::catalog