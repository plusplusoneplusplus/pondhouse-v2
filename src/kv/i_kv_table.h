#pragma once

#include <memory>
#include <string>
#include <vector>

#include "common/data_chunk.h"
#include "common/iterator.h"
#include "common/result.h"
#include "common/time.h"
#include "kv/kv_table_iterator.h"

namespace pond::kv {

/**
 * IKvTable defines the interface for a schema-agnostic key-value store.
 * It uses string keys and DataChunk values, providing raw byte storage
 * without schema validation or column awareness.
 */
class IKvTable {
public:
    // Define key type
    using Key = std::string;
    // Define value type
    using Value = common::DataChunk;
    // Define iterator type
    using Iterator = KvTableIterator;

    virtual ~IKvTable() = default;

    // Core operations
    virtual common::Result<void> Put(const Key& key, const Value& value, bool acquire_lock = true) = 0;
    virtual common::Result<Value> Get(const Key& key, bool acquire_lock = true) const = 0;
    virtual common::Result<void> Delete(const Key& key, bool acquire_lock = true) = 0;

    // Prefix scan operation
    virtual common::Result<std::shared_ptr<Iterator>> ScanPrefix(
        const std::string& prefix, common::IteratorMode mode = common::IteratorMode::Default) const = 0;

    // Batch operations
    virtual common::Result<void> BatchPut(const std::vector<std::pair<Key, Value>>& entries) = 0;
    virtual common::Result<std::vector<common::Result<Value>>> BatchGet(const std::vector<Key>& keys) const = 0;
    virtual common::Result<void> BatchDelete(const std::vector<Key>& keys) = 0;

    // Recovery and maintenance
    virtual common::Result<bool> Recover() = 0;
    virtual common::Result<void> Flush() = 0;
    virtual common::Result<void> RotateWAL() = 0;

    // Iterator creation
    virtual common::Result<std::shared_ptr<Iterator>> NewIterator(
        common::HybridTime read_time = common::MaxHybridTime(),
        common::IteratorMode mode = common::IteratorMode::Default,
        const std::string& prefix = "") const = 0;
};

}  // namespace pond::kv