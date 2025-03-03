#pragma once

#include <memory>

#include "catalog/catalog.h"
#include "common/append_only_fs.h"
#include "common/result.h"
#include "query/data/data_accessor.h"

namespace pond::query {

/**
 * @brief Implementation of DataAccessor that uses Catalog for metadata and file access
 *
 * This implementation uses the catalog to locate and access data files, and creates
 * appropriate readers based on the file format (currently only Parquet is supported).
 */
class CatalogDataAccessor : public DataAccessor {
public:
    /**
     * @brief Create a new CatalogDataAccessor
     * @param catalog The catalog instance to use for metadata
     * @param fs The filesystem to use for file access
     */
    CatalogDataAccessor(std::shared_ptr<catalog::Catalog> catalog, std::shared_ptr<common::IAppendOnlyFileSystem> fs)
        : catalog_(std::move(catalog)), fs_(std::move(fs)) {}

    ~CatalogDataAccessor() override = default;

    common::Result<std::unique_ptr<format::ParquetReader>> GetReader(const catalog::DataFile& file) override;

    common::Result<std::shared_ptr<common::Schema>> GetTableSchema(const std::string& table_name) override;

    common::Result<std::vector<catalog::DataFile>> ListTableFiles(
        const std::string& table_name, const std::optional<catalog::SnapshotId>& snapshot_id = std::nullopt) override;

private:
    std::shared_ptr<catalog::Catalog> catalog_;
    std::shared_ptr<common::IAppendOnlyFileSystem> fs_;
};

}  // namespace pond::query