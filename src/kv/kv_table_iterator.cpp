#include "kv/kv_table_iterator.h"

namespace pond::kv {

KvTableIterator::KvTableIterator(std::shared_ptr<MemTable::SnapshotIterator> memtable_iter,
                                 std::shared_ptr<SSTableManager::Iterator> sstable_iter,
                                 common::HybridTime read_time,
                                 common::IteratorMode mode,
                                 const std::string& prefix)
    : SnapshotIterator<std::string, common::DataChunk>(read_time, mode) {
    // Set up L0 iterators (just the memtable)
    std::vector<std::shared_ptr<common::SnapshotIterator<std::string, common::DataChunk>>> l0_iters;
    l0_iters.push_back(memtable_iter);

    // Set up L1+ iterators (the sstable iterator is already a union of all SSTables)
    std::vector<std::vector<std::shared_ptr<common::SnapshotIterator<std::string, common::DataChunk>>>> level_iters;
    std::vector<std::shared_ptr<common::SnapshotIterator<std::string, common::DataChunk>>> l1_iters;
    l1_iters.push_back(sstable_iter);
    level_iters.push_back(l1_iters);

    // Create the union iterator with prefix support
    union_iter_ = std::make_unique<common::UnionIterator<std::string, common::DataChunk>>(
        l0_iters, level_iters, read_time, mode, prefix);
}

void KvTableIterator::Seek(const std::string& target) {
    union_iter_->Seek(target);
}

void KvTableIterator::Next() {
    union_iter_->Next();
}

bool KvTableIterator::Valid() const {
    return union_iter_->Valid();
}

const std::string& KvTableIterator::key() const {
    return union_iter_->key();
}

const common::DataChunk& KvTableIterator::value() const {
    return union_iter_->value();
}

bool KvTableIterator::IsTombstone() const {
    return union_iter_->IsTombstone();
}

common::HybridTime KvTableIterator::version() const {
    return union_iter_->version();
}

}  // namespace pond::kv