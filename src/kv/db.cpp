#include "kv/db.h"

#include <filesystem>

#include "common/data_chunk.h"
#include "common/log.h"
#include "common/validation.h"
#include "kv/record.h"

namespace pond::kv {

common::Result<std::shared_ptr<DB>> DB::Create(std::shared_ptr<common::IAppendOnlyFileSystem> fs,
                                               const std::string& db_name) {
    using ReturnType = common::Result<std::shared_ptr<DB>>;

    if (!fs) {
        return common::Result<std::shared_ptr<DB>>::failure(common::ErrorCode::InvalidArgument,
                                                            "Filesystem cannot be null");
    }

    if (!common::ValidateName(db_name)) {
        return common::Result<std::shared_ptr<DB>>::failure(common::ErrorCode::InvalidArgument,
                                                            "Database name is invalid");
    }

    auto dbPtr = new DB(fs, db_name);
    auto init_result = dbPtr->Initialize();
    RETURN_IF_ERROR_T(ReturnType, init_result);
    return common::Result<std::shared_ptr<DB>>::success(std::shared_ptr<DB>(dbPtr));
}

std::shared_ptr<common::Schema> DB::CreateSystemTableSchema() {
    std::vector<common::ColumnSchema> columns = {
        {"key", common::ColumnType::STRING},     // Table name
        {"schema", common::ColumnType::BINARY},  // Serialized schema
    };
    return std::make_shared<common::Schema>(std::move(columns));
}

DB::DB(std::shared_ptr<common::IAppendOnlyFileSystem> fs, const std::string& db_name)
    : fs_(std::move(fs)), db_name_(db_name) {}

common::Result<void> DB::Initialize() {
    using ReturnType = common::Result<void>;

    // Create DB directory if it doesn't exist
    if (!fs_->Exists(db_name_)) {
        auto result = fs_->CreateDirectory(db_name_);
        RETURN_IF_ERROR_T(ReturnType, result);
    }

    // Initialize system table
    auto init_result = InitSystemTable();
    RETURN_IF_ERROR_T(ReturnType, init_result);
    return common::Result<void>::success();
}

common::Result<void> DB::InitSystemTable() {
    std::string system_path = GetTablePath(SYSTEM_TABLE);
    auto schema = CreateSystemTableSchema();

    try {
        system_table_ = std::make_shared<Table>(schema, fs_, system_path);
        return common::Result<void>::success();
    } catch (const std::exception& e) {
        return common::Result<void>::failure(common::ErrorCode::CreateTableFailed, e.what());
    }
}

common::Result<void> DB::SaveTableMetadata(const std::string& table_name,
                                           const std::shared_ptr<common::Schema>& schema) {
    // Create a record for the table metadata
    auto record = std::make_unique<Record>(system_table_->schema());
    record->Set(0, common::DataChunk::FromString(table_name));

    // Serialize the schema
    auto schema_data = schema->Serialize();
    record->Set(1, schema_data);

    // Save to system table
    return system_table_->Put(table_name, std::move(record));
}

common::Result<void> DB::DeleteTableMetadata(const std::string& table_name) {
    return system_table_->Delete(table_name);
}

common::Result<std::shared_ptr<common::Schema>> DB::LoadTableSchema(const std::string& table_name) {
    using ReturnType = common::Result<std::shared_ptr<common::Schema>>;
    auto record_result = system_table_->Get(table_name);
    if (!record_result.ok()) {
        if (record_result.error().code() == common::ErrorCode::NotFound) {
            return common::Result<std::shared_ptr<common::Schema>>::failure(common::ErrorCode::TableNotFound,
                                                                            "Table not found");
        }

        return common::Result<std::shared_ptr<common::Schema>>::failure(record_result.error());
    }

    auto record = std::move(record_result).value();
    auto schema_data = record->Get<common::DataChunk>(1);
    RETURN_IF_ERROR_T(ReturnType, schema_data);

    // Deserialize the schema
    common::Schema schema;
    if (!schema.Deserialize(schema_data.value())) {
        return common::Result<std::shared_ptr<common::Schema>>::failure(common::ErrorCode::InvalidArgument,
                                                                        "Failed to deserialize schema");
    }

    return common::Result<std::shared_ptr<common::Schema>>::success(std::make_shared<common::Schema>(schema));
}

common::Result<void> DB::CreateTable(const std::string& table_name, std::shared_ptr<common::Schema> schema) {
    using ReturnType = common::Result<void>;

    if (!common::ValidateName(table_name)) {
        return common::Result<void>::failure(common::ErrorCode::InvalidArgument, "Table name is invalid");
    }

    if (!schema) {
        return common::Result<void>::failure(common::ErrorCode::InvalidArgument, "Schema cannot be null");
    }

    if (table_name == SYSTEM_TABLE) {
        return common::Result<void>::failure(common::ErrorCode::InvalidArgument, "Cannot create system table");
    }

    std::lock_guard<std::mutex> lock(mutex_);

    // Check if table already exists
    if (tables_.find(table_name) != tables_.end()) {
        return common::Result<void>::failure(common::ErrorCode::TableAlreadyOpen, "Table already exists");
    }

    // Create table directory
    std::string table_path = GetTablePath(table_name);
    auto dir_result = fs_->CreateDirectory(table_path);
    if (!dir_result.ok()) {
        return common::Result<void>::failure(dir_result.error());
    }

    // Save table metadata
    auto save_result = SaveTableMetadata(table_name, schema);
    if (!save_result.ok()) {
        // Cleanup directory on failure
        RETURN_IF_ERROR_T(ReturnType, fs_->DeleteDirectory(table_path));
        return save_result;
    }

    // Create table instance
    try {
        auto table = std::make_shared<Table>(schema, fs_, table_path);
        tables_[table_name] = table;
        return common::Result<void>::success();
    } catch (const std::exception& e) {
        // Cleanup on failure
        RETURN_IF_ERROR_T(ReturnType, fs_->DeleteDirectory(table_path));
        DeleteTableMetadata(table_name);
        return common::Result<void>::failure(common::ErrorCode::CreateTableFailed, e.what());
    }
}

common::Result<void> DB::DropTable(const std::string& table_name) {
    if (table_name.empty()) {
        return common::Result<void>::failure(common::ErrorCode::InvalidArgument, "Table name cannot be empty");
    }

    if (table_name == SYSTEM_TABLE) {
        return common::Result<void>::failure(common::ErrorCode::InvalidArgument, "Cannot drop system table");
    }

    std::lock_guard<std::mutex> lock(mutex_);

    auto it = tables_.find(table_name);
    if (it == tables_.end()) {
        return common::Result<void>::failure(common::ErrorCode::TableNotFound, "Table not found");
    }

    // Remove table from memory
    tables_.erase(it);

    // Remove table metadata
    auto delete_result = DeleteTableMetadata(table_name);
    if (!delete_result.ok()) {
        return delete_result;
    }

    // Remove table directory
    std::string table_path = GetTablePath(table_name);
    auto remove_result = fs_->DeleteDirectory(table_path, true /*recursive to delete non-empty directory*/);
    if (!remove_result.ok()) {
        return common::Result<void>::failure(remove_result.error());
    }

    return common::Result<void>::success();
}

common::Result<std::shared_ptr<Table>> DB::GetTable(const std::string& table_name) {
    if (!common::ValidateName(table_name)) {
        return common::Result<std::shared_ptr<Table>>::failure(common::ErrorCode::InvalidArgument,
                                                               "Table name is invalid");
    }

    std::lock_guard<std::mutex> lock(mutex_);

    auto it = tables_.find(table_name);
    if (it != tables_.end()) {
        return common::Result<std::shared_ptr<Table>>::success(it->second);
    }

    // Table not in memory, try to load it
    auto schema_result = LoadTableSchema(table_name);
    if (!schema_result.ok()) {
        return common::Result<std::shared_ptr<Table>>::failure(schema_result.error());
    }

    try {
        std::string table_path = GetTablePath(table_name);
        auto table = std::make_shared<Table>(schema_result.value(), fs_, table_path);
        tables_[table_name] = table;
        return common::Result<std::shared_ptr<Table>>::success(table);
    } catch (const std::exception& e) {
        return common::Result<std::shared_ptr<Table>>::failure(common::ErrorCode::CreateTableFailed, e.what());
    }
}

common::Result<std::vector<std::string>> DB::ListTables() const {
    std::lock_guard<std::mutex> lock(mutex_);

    std::vector<std::string> table_names;
    table_names.reserve(tables_.size());
    for (const auto& [name, _] : tables_) {
        if (name != SYSTEM_TABLE) {
            table_names.push_back(name);
        }
    }

    return common::Result<std::vector<std::string>>::success(std::move(table_names));
}

common::Result<void> DB::Flush() {
    std::lock_guard<std::mutex> lock(mutex_);

    // Flush system table first
    auto system_result = system_table_->Flush();
    if (!system_result.ok()) {
        return system_result;
    }

    // Flush user tables
    for (const auto& [name, table] : tables_) {
        if (name != SYSTEM_TABLE) {
            auto result = table->Flush();
            if (!result.ok()) {
                return result;
            }
        }
    }

    return common::Result<void>::success();
}

common::Result<void> DB::Recover() {
    using ReturnType = common::Result<void>;
    std::lock_guard<std::mutex> lock(mutex_);

    // Clear existing tables
    tables_.clear();

    // Recover system table first
    auto system_result = system_table_->Recover();
    RETURN_IF_ERROR_T(ReturnType, system_result);

    // Get an iterator over the system table
    auto iter_result = system_table_->NewIterator();
    RETURN_IF_ERROR_T(ReturnType, iter_result);
    auto iter = std::move(iter_result).value();

    // Iterate over all tables in the system table
    while (iter->Valid()) {
        const std::string& table_name = iter->key();
        if (table_name == SYSTEM_TABLE) {
            iter->Next();
            continue;
        }

        auto schema_result = LoadTableSchema(table_name);
        RETURN_IF_ERROR_T(ReturnType, schema_result);

        try {
            std::string table_path = GetTablePath(table_name);
            auto table = std::make_shared<Table>(schema_result.value(), fs_, table_path);
            auto recover_result = table->Recover();
            RETURN_IF_ERROR_T(ReturnType, recover_result);

            tables_[table_name] = table;
        } catch (const std::exception& e) {
            return common::Result<void>::failure(common::ErrorCode::RecoveryFailed, e.what());
        }

        iter->Next();
    }

    return common::Result<void>::success();
}

std::string DB::GetTablePath(const std::string& table_name) const {
    return db_name_ + "/" + table_name;
}

}  // namespace pond::kv