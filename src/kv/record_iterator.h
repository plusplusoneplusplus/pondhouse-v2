#pragma once

#include <memory>
#include <string>

#include "common/iterator.h"
#include "common/schema.h"
#include "kv/kv_table_iterator.h"
#include "kv/record.h"

namespace pond::kv {

/**
 * RecordIterator wraps a KvTableIterator to provide schema-aware iteration
 * with Record objects instead of raw DataChunks.
 */
class RecordIterator : public common::SnapshotIterator<std::string, std::unique_ptr<Record>> {
public:
    RecordIterator(std::shared_ptr<KvTableIterator> base_iterator,
                   std::shared_ptr<common::Schema> schema,
                   common::HybridTime read_time,
                   common::IteratorMode mode);

    void Seek(const std::string& target) override;
    void Next() override;
    bool Valid() const override;
    const std::string& key() const override;
    const std::unique_ptr<Record>& value() const override;
    bool IsTombstone() const override;
    common::HybridTime version() const override;

private:
    void UpdateCurrentRecord();

    std::shared_ptr<KvTableIterator> base_iterator_;
    std::shared_ptr<common::Schema> schema_;
    std::unique_ptr<Record> current_record_;
};

}  // namespace pond::kv