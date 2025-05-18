#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "common/append_only_fs.h"
#include "common/result.h"
#include "kv/kv_table.h"
#include "kv/record.h"
#include "kv/record_iterator.h"

namespace pond::kv {

/**
 * Table provides a schema-aware interface on top of KvTable.
 * It adds schema validation and type-safe column operations.
 */
class Table : public KvTable {
public:
    // Define RecordIterator as the Iterator type for Table
    using Iterator = RecordIterator;

    explicit Table(std::shared_ptr<common::Schema> schema,
                   std::shared_ptr<common::IAppendOnlyFileSystem> fs,
                   const std::string& table_name,
                   size_t max_wal_size = DEFAULT_WAL_SIZE);
    ~Table() = default;

    // Schema-aware operations
    common::Result<void> Put(const Key& key, std::unique_ptr<Record> record);
    common::Result<bool> PutIfNotExists(const Key& key, std::unique_ptr<Record> record);
    common::Result<std::unique_ptr<Record>> Get(const Key& key) const;
    common::Result<void> Delete(const Key& key);
    common::Result<void> UpdateColumn(const Key& key, const std::string& column_name, const common::DataChunk& value);

    // Schema access
    const std::shared_ptr<common::Schema>& schema() const { return schema_; }

    // Iterator creation
    common::Result<std::shared_ptr<Iterator>> NewIterator(
        common::HybridTime read_time = common::MaxHybridTime(),
        common::IteratorMode mode = common::IteratorMode::Default) const;

    // Prefix scan operation
    common::Result<std::shared_ptr<Iterator>> ScanPrefix(
        const std::string& prefix, common::IteratorMode mode = common::IteratorMode::Default) const;

private:
    // Convert between Record and DataChunk
    common::Result<common::DataChunk> SerializeRecord(const Record& record) const;
    common::Result<std::unique_ptr<Record>> DeserializeRecord(const common::DataChunk& data) const;

    std::shared_ptr<common::Schema> schema_;
};

}  // namespace pond::kv
