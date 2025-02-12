#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "common/append_only_fs.h"
#include "common/result.h"
#include "kv/kv_table.h"
#include "kv/record.h"

namespace pond::kv {

/**
 * Table provides a schema-aware interface on top of KvTable.
 * It adds schema validation and type-safe column operations.
 */
class Table : public KvTable {
public:
    explicit Table(std::shared_ptr<Schema> schema,
                   std::shared_ptr<common::IAppendOnlyFileSystem> fs,
                   const std::string& table_name,
                   size_t max_wal_size = DEFAULT_WAL_SIZE);
    ~Table() = default;

    // Schema-aware operations
    common::Result<void> Put(const Key& key, std::unique_ptr<Record> record);
    common::Result<std::unique_ptr<Record>> Get(const Key& key) const;
    common::Result<void> Delete(const Key& key);
    common::Result<void> UpdateColumn(const Key& key, const std::string& column_name, const common::DataChunk& value);

    // Schema access
    const std::shared_ptr<Schema>& schema() const { return schema_; }

private:
    // Convert between Record and DataChunk
    common::Result<common::DataChunk> SerializeRecord(const Record& record) const;
    common::Result<std::unique_ptr<Record>> DeserializeRecord(const common::DataChunk& data) const;

    std::shared_ptr<Schema> schema_;
};

}  // namespace pond::kv
