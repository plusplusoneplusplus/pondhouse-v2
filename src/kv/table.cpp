#include "kv/table.h"

namespace pond::kv {

Table::Table(std::shared_ptr<common::Schema> schema,
             std::shared_ptr<common::IAppendOnlyFileSystem> fs,
             const std::string& table_name,
             size_t max_wal_size)
    : KvTable(fs, table_name, max_wal_size), schema_(std::move(schema)) {}

common::Result<void> Table::Put(const Key& key, std::unique_ptr<Record> record) {
    // Validate record schema
    if (record->schema() != schema_) {
        return common::Result<void>::failure(common::ErrorCode::InvalidArgument,
                                             "Schema mismatch. Record schema: " + record->schema()->ToString() +
                                                 " != Table schema: " + schema_->ToString());
    }

    if (key.empty()) {
        return common::Result<void>::failure(common::ErrorCode::InvalidArgument, "Key cannot be empty");
    }

    // Serialize record to DataChunk
    auto data_result = SerializeRecord(*record);
    if (!data_result.ok()) {
        return common::Result<void>::failure(data_result.error());
    }

    // Use base class Put
    return KvTable::Put(key, data_result.value());
}

common::Result<bool> Table::PutIfNotExists(const Key& key, std::unique_ptr<Record> record) {
    using ReturnType = common::Result<bool>;

    // Check if the key exists
    auto result = Get(key);
    if (result.ok()) {
        return ReturnType::success(false);
    }

    // Use base class Put
    auto put_result = Put(key, std::move(record));
    RETURN_IF_ERROR_T(ReturnType, put_result);
    return ReturnType::success(true);
}

common::Result<std::unique_ptr<Record>> Table::Get(const Key& key) const {
    if (key.empty()) {
        return common::Result<std::unique_ptr<Record>>::failure(common::ErrorCode::InvalidArgument,
                                                                "Key cannot be empty");
    }

    // Use base class Get
    auto result = KvTable::Get(key);
    if (!result.ok()) {
        return common::Result<std::unique_ptr<Record>>::failure(result.error());
    }

    // Deserialize DataChunk to Record
    return DeserializeRecord(result.value());
}

common::Result<void> Table::Delete(const Key& key) {
    if (key.empty()) {
        return common::Result<void>::failure(common::ErrorCode::InvalidArgument, "Key cannot be empty");
    }

    // Use base class Delete
    return KvTable::Delete(key);
}

common::Result<void> Table::UpdateColumn(const Key& key,
                                         const std::string& column_name,
                                         const common::DataChunk& value) {
    if (key.empty()) {
        return common::Result<void>::failure(common::ErrorCode::InvalidArgument, "Key cannot be empty");
    }

    if (column_name.empty()) {
        return common::Result<void>::failure(common::ErrorCode::InvalidArgument, "Column name cannot be empty");
    }

    if (value.Empty()) {
        return common::Result<void>::failure(common::ErrorCode::InvalidArgument, "Value cannot be empty");
    }

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

common::Result<std::shared_ptr<Table::Iterator>> Table::NewIterator(common::HybridTime read_time,
                                                                    common::IteratorMode mode) const {
    // Get the base KvTable iterator
    auto base_iter_result = KvTable::NewIterator(read_time, mode);
    if (!base_iter_result.ok()) {
        return common::Result<std::shared_ptr<Iterator>>::failure(base_iter_result.error());
    }

    // Create and return a RecordIterator that wraps the base iterator
    auto record_iter = std::make_shared<RecordIterator>(base_iter_result.value(), schema_, read_time, mode);

    return common::Result<std::shared_ptr<Iterator>>::success(record_iter);
}

common::Result<std::shared_ptr<Table::Iterator>> Table::ScanPrefix(const std::string& prefix,
                                                                   common::IteratorMode mode) const {
    // Get the base KvTable iterator for prefix scan
    auto base_iter_result = KvTable::ScanPrefix(prefix, mode);
    if (!base_iter_result.ok()) {
        return common::Result<std::shared_ptr<Iterator>>::failure(base_iter_result.error());
    }

    // Create and return a RecordIterator that wraps the base iterator
    auto record_iter =
        std::make_shared<RecordIterator>(base_iter_result.value(), schema_, common::MaxHybridTime(), mode);

    return common::Result<std::shared_ptr<Iterator>>::success(record_iter);
}

common::Result<common::DataChunk> Table::SerializeRecord(const Record& record) const {
    return common::Result<common::DataChunk>::success(record.Serialize());
}

common::Result<std::unique_ptr<Record>> Table::DeserializeRecord(const common::DataChunk& data) const {
    return Record::Deserialize(data, schema_);
}

}  // namespace pond::kv