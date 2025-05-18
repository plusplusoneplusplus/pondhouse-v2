#include "kv/record_iterator.h"

namespace pond::kv {

RecordIterator::RecordIterator(std::shared_ptr<KvTableIterator> base_iterator,
                               std::shared_ptr<common::Schema> schema,
                               common::HybridTime read_time,
                               common::IteratorMode mode)
    : common::SnapshotIterator<std::string, std::unique_ptr<Record>>(read_time, mode),
      base_iterator_(std::move(base_iterator)),
      schema_(std::move(schema)),
      current_record_(nullptr) {
    UpdateCurrentRecord();
}

void RecordIterator::Seek(const std::string& target) {
    base_iterator_->Seek(target);
    UpdateCurrentRecord();
}

void RecordIterator::Next() {
    base_iterator_->Next();
    UpdateCurrentRecord();
}

bool RecordIterator::Valid() const {
    return base_iterator_->Valid();
}

const std::string& RecordIterator::key() const {
    return base_iterator_->key();
}

const std::unique_ptr<Record>& RecordIterator::value() const {
    return current_record_;
}

bool RecordIterator::IsTombstone() const {
    return base_iterator_->IsTombstone();
}

common::HybridTime RecordIterator::version() const {
    return base_iterator_->version();
}

void RecordIterator::UpdateCurrentRecord() {
    if (Valid()) {
        auto result = Record::Deserialize(base_iterator_->value(), schema_);
        if (result.ok()) {
            current_record_ = std::move(result).value();
        } else {
            // If deserialization fails, set current_record_ to nullptr
            current_record_ = nullptr;
        }
    } else {
        current_record_ = nullptr;
    }
}

}  // namespace pond::kv