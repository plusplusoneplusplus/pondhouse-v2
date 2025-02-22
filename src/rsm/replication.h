#pragma once

#include <atomic>

#include "common/data_chunk.h"
#include "common/error.h"
#include "common/log.h"
#include "common/result.h"
#include "common/wal_entry.h"

namespace pond::rsm {

using common::DataChunk;
using common::Result;
using common::WalEntry;

// Forward declarations
class IAppendOnlyFileSystem;

/**
 * Base configuration class for replication mechanisms
 */
struct ReplicationConfig {
    std::string directory;                   // Path to store replication data
    size_t max_log_size = 64 * 1024 * 1024;  // 64MB default
    size_t checkpoint_interval = 1000;       // Number of entries between checkpoints
    size_t max_checkpoints = 3;              // Maximum number of checkpoint files to keep

    static ReplicationConfig Default() {
        static ReplicationConfig config;
        return config;
    }
};

/**
 * Interface for replication entries that can be replicated across nodes
 */
class ReplicationEntry : public WalEntry {
public:
    ReplicationEntry() = default;
    virtual ~ReplicationEntry() = default;

    // Getters
    uint64_t index() const { return index_; }
    uint64_t term() const { return term_; }
    uint64_t timestamp() const { return timestamp_; }
    const DataChunk& data() const { return data_; }

    // Setters
    void SetIndex(uint64_t index) {
        index_ = index;
        set_lsn(index);
    }

    void SetTerm(uint64_t term) { term_ = term; }

    void SetTimestamp(uint64_t timestamp) { timestamp_ = timestamp; }

    void SetData(const DataChunk& data) { data_ = data; }

    // Implement ISerializable interface
    DataChunk Serialize() const override {
        DataChunk chunk;
        Serialize(chunk);
        return chunk;
    }

    void Serialize(DataChunk& chunk) const override {
        chunk.WriteUInt64(lsn());
        chunk.WriteUInt64(index_);
        chunk.WriteUInt64(term_);
        chunk.WriteUInt64(timestamp_);
        chunk.WriteUInt64(data_.Size());
        chunk.Append(data_);
    }

    bool Deserialize(const DataChunk& chunk) override {
        size_t offset = 0;
        if (chunk.Size() < sizeof(uint64_t) * 5) {
            return false;
        }
        set_lsn(chunk.ReadUInt64(offset));
        index_ = chunk.ReadUInt64(offset);
        term_ = chunk.ReadUInt64(offset);
        timestamp_ = chunk.ReadUInt64(offset);
        auto size = chunk.ReadUInt64(offset);
        if (offset + size > chunk.Size()) {
            return false;
        }
        data_ = DataChunk(chunk.Data() + offset, size);
        offset += size;
        return true;
    }

protected:
    uint64_t index_{0};      // Log index (WAL LSN or Raft log index)
    uint64_t term_{0};       // Term number (always 0 for WAL, used by Raft)
    uint64_t timestamp_{0};  // Timestamp of the entry, wall clock (only for debugging)
    DataChunk data_;         // Data of the entry
};

/**
 * Interface for replication mechanisms (WAL or Raft)
 * This provides a common abstraction for different replication strategies
 */
class IReplication {
public:
    virtual ~IReplication() = default;

    // Initialize the replication state
    // There is no log recovery at this point.
    virtual Result<bool> Initialize(const ReplicationConfig& config) = 0;

    // Bootstrap the replication state
    virtual Result<bool> Bootstrap() = 0;

    // Close and cleanup
    virtual Result<bool> Close() = 0;

    // Append a new entry
    virtual Result<uint64_t> Append(const DataChunk& data) = 0;

    // Read entries starting from given index
    virtual Result<std::vector<DataChunk>> Read(uint64_t start_index) = 0;

    // Get current log index
    virtual uint64_t GetCurrentIndex() const = 0;

    // Reset index (for testing/recovery)
    virtual void ResetIndex() = 0;
};

}  // namespace pond::rsm