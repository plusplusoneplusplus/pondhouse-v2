#pragma once

#include <cstdint>
#include <string>

#include "common/types.h"
#include "common/data_chunk.h"
#include "common/result.h"
#include "common/wal_entry.h"

namespace pond::kv {

using Key = std::string;

// Size limits
constexpr size_t MAX_KEY_SIZE = 4 * 1024;                   // 4KB
constexpr size_t MAX_VALUE_SIZE = 4 * 1024 * 1024;          // 4MB
constexpr size_t DEFAULT_MEMTABLE_SIZE = 64 * 1024 * 1024;  // 64MB

enum class EntryType : uint32_t { Unknown, Put, Delete };

class KvEntry : public common::WalEntry {
public:
    using key_type = std::string;
    using value_type = common::DataChunk;

    key_type key;
    value_type value;
    common::Timestamp ts;
    EntryType type;

    KvEntry() = default;

    KvEntry(key_type k, value_type v, common::LSN seq, common::Timestamp timestamp, EntryType t)
        : WalEntry(seq), key(std::move(k)), value(std::move(v)), ts(timestamp), type(t) {}

    common::DataChunk serialize() const override;
    void serialize(common::DataChunk& data) const override;
    bool deserialize(const common::DataChunk& data) override;
    common::Result<std::unique_ptr<common::ISerializable>> deserializeAsUniquePtr(
        const common::DataChunk& data) const override;
};

}  // namespace pond::kv