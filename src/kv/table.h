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
 * RecordIterator wraps a KvTable::Iterator to provide schema-aware iteration
 * with Record objects instead of raw DataChunks.
 */
class RecordIterator : public common::SnapshotIterator<std::string, std::unique_ptr<Record>> {
public:
    RecordIterator(std::shared_ptr<KvTable::Iterator> base_iterator,
                   std::shared_ptr<common::Schema> schema,
                   common::HybridTime read_time,
                   common::IteratorMode mode)
        : common::SnapshotIterator<std::string, std::unique_ptr<Record>>(read_time, mode),
          base_iterator_(std::move(base_iterator)),
          schema_(std::move(schema)),
          current_record_(nullptr) {
        UpdateCurrentRecord();
    }

    void Seek(const std::string& target) override {
        base_iterator_->Seek(target);
        UpdateCurrentRecord();
    }

    void Next() override {
        base_iterator_->Next();
        UpdateCurrentRecord();
    }

    bool Valid() const override { return base_iterator_->Valid(); }

    const std::string& key() const override { return base_iterator_->key(); }

    const std::unique_ptr<Record>& value() const override { return current_record_; }

    bool IsTombstone() const override { return base_iterator_->IsTombstone(); }

    common::HybridTime version() const override { return base_iterator_->version(); }

private:
    void UpdateCurrentRecord() {
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

    std::shared_ptr<KvTable::Iterator> base_iterator_;
    std::shared_ptr<common::Schema> schema_;
    std::unique_ptr<Record> current_record_;
};

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
