#pragma once

#include <memory>
#include <string>

#include "catalog/metadata.h"
#include "common/result.h"
#include "common/schema.h"
#include "format/parquet/parquet_reader.h"

namespace pond::query {

/**
 * @brief Interface for accessing table data from various storage formats
 *
 * DataAccessor provides an abstraction layer between the query executor and
 * the underlying storage system. It works with the catalog to locate data files
 * and creates appropriate readers for accessing the data.
 */
class DataAccessor {
public:
    virtual ~DataAccessor() = default;

    /**
     * @brief Get a reader for a specific data file
     * @param file The data file metadata from the catalog
     * @return Result containing a ParquetReader if successful
     */
    virtual common::Result<std::unique_ptr<format::ParquetReader>> GetReader(const catalog::DataFile& file) = 0;

    /**
     * @brief Get the schema for a table
     * @param table_name Name of the table
     * @return Result containing the table's schema if successful
     */
    virtual common::Result<std::shared_ptr<common::Schema>> GetTableSchema(const std::string& table_name) = 0;

    /**
     * @brief List all data files for a table at a specific snapshot
     * @param table_name Name of the table
     * @param snapshot_id Optional snapshot ID (uses latest if not specified)
     * @return Result containing vector of data files if successful
     */
    virtual common::Result<std::vector<catalog::DataFile>> ListTableFiles(
        const std::string& table_name, const std::optional<catalog::SnapshotId>& snapshot_id = std::nullopt) = 0;
};

}  // namespace pond::query