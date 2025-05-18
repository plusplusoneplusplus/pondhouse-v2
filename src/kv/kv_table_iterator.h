#pragma once

#include <memory>
#include <string>
#include <vector>

#include "common/data_chunk.h"
#include "common/iterator.h"
#include "common/time.h"
#include "kv/memtable.h"
#include "kv/sstable_manager.h"

namespace pond::kv {

/**
 * KvTableIterator combines the current memtable iterator with the SSTableManager's
 * union iterator to provide a unified view of all data in the KV table.
 * It follows the same versioning and tombstone rules as the underlying iterators.
 */
class KvTableIterator : public common::SnapshotIterator<std::string, common::DataChunk> {
public:
    KvTableIterator(std::shared_ptr<MemTable::SnapshotIterator> memtable_iter,
                    std::shared_ptr<SSTableManager::Iterator> sstable_iter,
                    common::HybridTime read_time,
                    common::IteratorMode mode,
                    const std::string& prefix = "");

    // Iterator methods
    void Seek(const std::string& target) override;
    void Next() override;
    bool Valid() const override;
    const std::string& key() const override;
    const common::DataChunk& value() const override;
    bool IsTombstone() const override;
    common::HybridTime version() const override;

private:
    std::unique_ptr<common::UnionIterator<std::string, common::DataChunk>> union_iter_;
};

}  // namespace pond::kv