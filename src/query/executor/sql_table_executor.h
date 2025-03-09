#pragma once

#include <memory>
#include <string>

#include "catalog/catalog.h"
#include "common/result.h"
#include "common/schema.h"
#include "SQLParser.h"

namespace pond::query {

/**
 * @brief An executor for SQL DDL statements that create tables
 *
 * This class translates SQL CREATE TABLE statements into catalog operations,
 * converting SQL column definitions to Pond schemas and creating tables
 * in the catalog.
 */
class SQLTableExecutor {
public:
    /**
     * @brief Create a new SQLTableExecutor
     *
     * @param catalog The catalog to use for table operations
     */
    explicit SQLTableExecutor(std::shared_ptr<catalog::Catalog> catalog);

    /**
     * @brief Execute a CREATE TABLE SQL statement
     *
     * Parses the SQL statement, extracts the table schema, and creates a table
     * in the catalog with the appropriate schema and default partition spec.
     *
     * @param sql_statement The SQL CREATE TABLE statement to execute
     * @param location Optional location for the table data (default is empty which lets catalog decide)
     * @param properties Optional key-value properties for the table
     * @return common::Result<catalog::TableMetadata> Result containing table metadata or error
     */
    common::Result<catalog::TableMetadata> ExecuteCreateTable(
        const std::string& sql_statement,
        const std::string& location = "",
        const std::unordered_map<std::string, std::string>& properties = {});

private:
    /**
     * @brief Convert a SQL CREATE statement to a Pond schema
     *
     * @param create_stmt The parsed CREATE TABLE statement
     * @return common::Result<std::shared_ptr<common::Schema>> The converted schema or error
     */
    common::Result<std::shared_ptr<common::Schema>> ConvertToSchema(const hsql::CreateStatement* create_stmt);

    /**
     * @brief Map SQL data types to Pond column types
     *
     * @param sql_type The SQL data type
     * @param pond_type The corresponding Pond column type
     * @return bool True if the mapping was successful, false otherwise
     */
    bool MapColumnType(hsql::DataType sql_type, common::ColumnType& pond_type);

    /**
     * @brief Determine nullability from SQL column definition
     *
     * @param column The SQL column definition
     * @return common::Nullability The corresponding nullability constraint
     */
    common::Nullability DetermineNullability(const hsql::ColumnDefinition* column);

    /**
     * @brief Create a default partition spec for a table
     *
     * @return catalog::PartitionSpec A default partition specification
     */
    catalog::PartitionSpec CreateDefaultPartitionSpec();

    // The catalog instance used for table operations
    std::shared_ptr<catalog::Catalog> catalog_;
};

}  // namespace pond::query