#include "query/executor/sql_table_executor.h"

#include <sstream>

#include "common/error.h"
#include "common/log.h"

using namespace pond::common;

namespace pond::query {

SQLTableExecutor::SQLTableExecutor(std::shared_ptr<catalog::Catalog> catalog) : catalog_(std::move(catalog)) {}

Result<catalog::TableMetadata> SQLTableExecutor::ExecuteCreateTable(
    const std::string& sql_statement,
    const std::string& location,
    const std::unordered_map<std::string, std::string>& properties) {
    using ReturnType = Result<catalog::TableMetadata>;

    // Parse the SQL statement
    hsql::SQLParserResult parse_result;
    hsql::SQLParser::parse(sql_statement, &parse_result);

    // Check if parsing was successful
    if (!parse_result.isValid()) {
        std::stringstream error_msg;
        error_msg << "SQL parsing error: " << parse_result.errorMsg() << " at line " << parse_result.errorLine()
                  << ", column " << parse_result.errorColumn();
        return ReturnType::failure(common::ErrorCode::InvalidArgument, error_msg.str());
    }

    // Check if we have exactly one statement
    if (parse_result.size() != 1) {
        return ReturnType::failure(common::ErrorCode::InvalidArgument,
                                   "SQL must contain exactly one CREATE TABLE statement");
    }

    // Check if it's a CREATE statement
    const hsql::SQLStatement* stmt = parse_result.getStatement(0);
    if (stmt->type() != hsql::kStmtCreate) {
        return ReturnType::failure(common::ErrorCode::InvalidArgument, "SQL must be a CREATE TABLE statement");
    }

    // Cast to CREATE statement
    const hsql::CreateStatement* create_stmt = static_cast<const hsql::CreateStatement*>(stmt);

    // Check if it's a CREATE TABLE statement
    if (create_stmt->type != hsql::kCreateTable) {
        return ReturnType::failure(common::ErrorCode::InvalidArgument, "Only CREATE TABLE statements are supported");
    }

    // Convert the SQL schema to a Pond schema
    auto schema_result = ConvertToSchema(create_stmt);
    RETURN_IF_ERROR_T(ReturnType, schema_result);

    std::string table_name = create_stmt->tableName;
    auto schema = schema_result.value();
    auto partition_spec = CreateDefaultPartitionSpec();

    // Create the table in the catalog
    auto create_result = catalog_->CreateTable(table_name, schema, partition_spec, location, properties);
    RETURN_IF_ERROR_T(ReturnType, create_result);

    LOG_STATUS("Created table '%s' with %zu columns", table_name.c_str(), schema->NumColumns());
    return create_result;
}

Result<std::shared_ptr<Schema>> SQLTableExecutor::ConvertToSchema(const hsql::CreateStatement* create_stmt) {
    using ReturnType = Result<std::shared_ptr<Schema>>;

    if (!create_stmt->columns || create_stmt->columns->empty()) {
        return ReturnType::failure(common::ErrorCode::InvalidArgument,
                                   "CREATE TABLE statement must have at least one column");
    }

    auto schema = std::make_shared<common::Schema>();

    // Process each column in the CREATE TABLE statement
    for (const hsql::ColumnDefinition* column : *create_stmt->columns) {
        common::ColumnType pond_type;
        if (!MapColumnType(column->type.data_type, pond_type)) {
            return ReturnType::failure(
                common::ErrorCode::InvalidArgument,
                "Unsupported SQL type " + std::to_string(static_cast<int>(column->type.data_type)));
        }

        common::Nullability nullability = DetermineNullability(column);

        // Add the field to the schema
        schema->AddField(column->name, pond_type, nullability);
    }

    if (schema->Empty()) {
        return ReturnType::failure(common::ErrorCode::InvalidArgument, "Failed to create schema from SQL statement");
    }

    return ReturnType::success(schema);
}

bool SQLTableExecutor::MapColumnType(hsql::DataType sql_type, common::ColumnType& pond_type) {
    switch (sql_type) {
        case hsql::DataType::INT:
            pond_type = common::ColumnType::INT32;
            break;
        case hsql::DataType::BIGINT:
            pond_type = common::ColumnType::INT64;
            break;
        case hsql::DataType::FLOAT:
        case hsql::DataType::REAL:
            pond_type = common::ColumnType::FLOAT;
            break;
        case hsql::DataType::DOUBLE:
            pond_type = common::ColumnType::DOUBLE;
            break;
        case hsql::DataType::VARCHAR:
        case hsql::DataType::CHAR:
        case hsql::DataType::TEXT:
            pond_type = common::ColumnType::STRING;
            break;
        case hsql::DataType::BOOLEAN:
            pond_type = common::ColumnType::BOOLEAN;
            break;
        case hsql::DataType::DATETIME:
        case hsql::DataType::DATE:
        case hsql::DataType::TIME:
            pond_type = common::ColumnType::TIMESTAMP;
            break;
        default:
            return false;
    }

    return true;
}

common::Nullability SQLTableExecutor::DetermineNullability(const hsql::ColumnDefinition* column) {
    if (!column->nullable) {
        return common::Nullability::NOT_NULL;
    }
    return common::Nullability::NULLABLE;
}

catalog::PartitionSpec SQLTableExecutor::CreateDefaultPartitionSpec() {
    // Create a simple default partition spec (identity transform)
    return catalog::PartitionSpec(1);
}

}  // namespace pond::query