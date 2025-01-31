#pragma once

#include <memory>
#include <string>
#include <vector>

#include "common/append_only_fs.h"
#include "common/result.h"
#include "common/wal.h"
#include "kv/kv_entry.h"
#include "kv/memtable.h"
#include "kv/record.h"

namespace pond::kv {

class Table {
public:
    explicit Table(std::shared_ptr<Schema> schema,
                   std::shared_ptr<common::IAppendOnlyFileSystem> fs,
                   const std::string& table_name);
    ~Table() = default;

    // Core operations
    common::Result<void> Put(const Key& key, std::unique_ptr<Record> record);
    common::Result<std::unique_ptr<Record>> Get(const Key& key) const;
    common::Result<void> Delete(const Key& key);
    common::Result<void> UpdateColumn(const Key& key,
                                    const std::string& column_name,
                                    const common::DataChunk& value);

    // Recovery
    common::Result<bool> Recover();

    // Flush operations (to be implemented)
    common::Result<void> Flush();

private:
    // Write entry to WAL and return LSN
    common::Result<common::LSN> WriteToWAL(KvEntry& entry);

    std::shared_ptr<Schema> schema_;
    std::shared_ptr<common::IAppendOnlyFileSystem> fs_;
    std::string table_name_;
    std::unique_ptr<MemTable> active_memtable_;
    std::shared_ptr<common::WAL<KvEntry>> wal_;
    // TODO: Add SSTable management
};

}  // namespace pond::kv
