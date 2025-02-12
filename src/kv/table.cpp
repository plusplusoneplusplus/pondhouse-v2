#include "kv/table.h"

namespace pond::kv {

Table::Table(std::shared_ptr<Schema> schema,
             std::shared_ptr<common::IAppendOnlyFileSystem> fs,
             const std::string& table_name,
             size_t max_wal_size)
    : KvTable(fs, table_name, max_wal_size), schema_(std::move(schema)) {}

common::Result<void> Table::Put(const Key& key, std::unique_ptr<Record> record) {
    // Validate record schema
    if (record->schema() != schema_) {
        return common::Result<void>::failure(common::ErrorCode::InvalidArgument, "Schema mismatch");
    }

    // Serialize record to DataChunk
    auto data_result = SerializeRecord(*record);
    if (!data_result.ok()) {
        return common::Result<void>::failure(data_result.error());
    }

    // Use base class Put
    return KvTable::Put(key, data_result.value());
}

common::Result<std::unique_ptr<Record>> Table::Get(const Key& key) const {
    // Use base class Get
    auto result = KvTable::Get(key);
    if (!result.ok()) {
        return common::Result<std::unique_ptr<Record>>::failure(result.error());
    }

    // Deserialize DataChunk to Record
    return DeserializeRecord(result.value());
}

common::Result<void> Table::Delete(const Key& key) {
    // Use base class Delete
    return KvTable::Delete(key);
}

common::Result<void> Table::UpdateColumn(const Key& key,
                                         const std::string& column_name,
                                         const common::DataChunk& value) {
    // Get current record
    auto record_result = Get(key);
    if (!record_result.ok()) {
        return common::Result<void>::failure(record_result.error());
    }

    // Update column
    auto record = std::move(record_result).value();
    int col_idx = schema_->GetColumnIndex(column_name);
    if (col_idx < 0) {
        return common::Result<void>::failure(common::ErrorCode::InvalidArgument, "Column not found");
    }

    record->Set(col_idx, value);

    // Put updated record
    return Put(key, std::move(record));
}

common::Result<common::DataChunk> Table::SerializeRecord(const Record& record) const {
    return common::Result<common::DataChunk>::success(record.Serialize());
}

common::Result<std::unique_ptr<Record>> Table::DeserializeRecord(const common::DataChunk& data) const {
    return Record::Deserialize(data, schema_);
}

}  // namespace pond::kv