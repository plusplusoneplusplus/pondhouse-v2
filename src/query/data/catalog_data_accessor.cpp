#include "query/data/catalog_data_accessor.h"

#include "common/error.h"

namespace pond::query {

common::Result<std::unique_ptr<format::ParquetReader>> CatalogDataAccessor::GetReader(const catalog::DataFile& file) {
    // Create a ParquetReader for the file using the filesystem
    auto reader_result = format::ParquetReader::Create(fs_, file.file_path);
    if (!reader_result.ok()) {
        return common::Error(common::ErrorCode::Failure,
                             "Failed to create ParquetReader for file: " + file.file_path
                                 + ", error: " + reader_result.error().message());
    }
    return reader_result;
}

common::Result<std::shared_ptr<common::Schema>> CatalogDataAccessor::GetTableSchema(const std::string& table_name) {
    // Load table metadata from catalog
    auto table_result = catalog_->LoadTable(table_name);
    if (!table_result.ok()) {
        return common::Error(common::ErrorCode::TableNotFound,
                             "Failed to load table: " + table_name + ", error: " + table_result.error().message());
    }

    // Return the schema from table metadata
    return common::Result<std::shared_ptr<common::Schema>>::success(table_result.value().schema);
}

common::Result<std::vector<catalog::DataFile>> CatalogDataAccessor::ListTableFiles(
    const std::string& table_name, const std::optional<catalog::SnapshotId>& snapshot_id) {
    // List data files from catalog
    auto files_result = catalog_->ListDataFiles(table_name, snapshot_id);
    if (!files_result.ok()) {
        return common::Error(
            common::ErrorCode::Failure,
            "Failed to list data files for table: " + table_name + ", error: " + files_result.error().message());
    }
    return files_result;
}

}  // namespace pond::query