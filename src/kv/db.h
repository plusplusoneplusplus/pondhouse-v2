#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "common/append_only_fs.h"
#include "common/result.h"
#include "common/schema.h"
#include "kv/table.h"

namespace pond::kv {

/**
 * DB provides a collection of tables, each with its own schema.
 * It manages table creation, deletion, and access.
 * Table metadata is stored in a special system table.
 */
class DB {
public:
    static constexpr const char* SYSTEM_TABLE = "__system";
    static constexpr const char* TABLES_KEY = "tables";

    explicit DB(std::shared_ptr<common::IAppendOnlyFileSystem> fs, const std::string& db_name);
    ~DB() = default;

    // Table operations
    common::Result<void> CreateTable(const std::string& table_name, std::shared_ptr<common::Schema> schema);
    common::Result<void> DropTable(const std::string& table_name);
    common::Result<std::shared_ptr<Table>> GetTable(const std::string& table_name);
    common::Result<std::vector<std::string>> ListTables() const;

    // DB-wide operations
    common::Result<void> Flush();
    common::Result<void> Recover();

private:
    // System table operations
    common::Result<void> InitSystemTable();
    common::Result<void> SaveTableMetadata(const std::string& table_name,
                                           const std::shared_ptr<common::Schema>& schema);
    common::Result<void> DeleteTableMetadata(const std::string& table_name);
    common::Result<std::shared_ptr<common::Schema>> LoadTableSchema(const std::string& table_name);

    // Helper methods
    std::string GetTablePath(const std::string& table_name) const;
    static std::shared_ptr<common::Schema> CreateSystemTableSchema();

    std::shared_ptr<common::IAppendOnlyFileSystem> fs_;
    std::string db_name_;
    mutable std::mutex mutex_;
    std::unordered_map<std::string, std::shared_ptr<Table>> tables_;
    std::shared_ptr<Table> system_table_;  // Special table for storing metadata
};

}  // namespace pond::kv