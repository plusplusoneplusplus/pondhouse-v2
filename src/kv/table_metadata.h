#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "common/data_chunk.h"
#include "common/serializable.h"
#include "common/wal.h"
#include "common/wal_state_machine.h"
#include "proto/kv.pb.h"

namespace pond::kv {

// Metadata operation types
// Make sure this is in sync with the MetadataOpType enum in proto/kv.proto
enum class MetadataOpType {
    CreateSSTable,  // New SSTable created
    DeleteSSTable,  // SSTable deleted (after compaction)
    UpdateStats,    // Update table statistics
    FlushMemTable,  // MemTable flushed to SSTable
    RotateWAL,      // WAL rotation
    CompactFiles,   // Compact files
    Unknown,        // Placeholder for unknown type
};

// File information for metadata operations
struct FileInfo : public common::ISerializable {
    std::string name;
    uint64_t size;
    size_t level;
    std::string smallest_key;
    std::string largest_key;

    FileInfo() = default;
    FileInfo(const std::string& n,
             uint64_t s,
             size_t l = 0,
             const std::string& min_key = "",
             const std::string& max_key = "")
        : name(n), size(s), level(l), smallest_key(min_key), largest_key(max_key) {}

    // Convert to/from protobuf message
    proto::FileInfo ToProto() const;
    static FileInfo FromProto(const proto::FileInfo& pb);

    // ISerializable interface
    common::DataChunk Serialize() const override;
    void Serialize(common::DataChunk& chunk) const override;
    bool Deserialize(const common::DataChunk& chunk) override;
};

// Entry for tracking table metadata operations
class TableMetadataEntry : public common::WalEntry {
public:
    TableMetadataEntry() = default;
    TableMetadataEntry(MetadataOpType op_type,
                       const std::vector<FileInfo>& added = {},
                       const std::vector<FileInfo>& deleted = {})
        : op_type_(op_type), added_files_(added), deleted_files_(deleted) {}

    MetadataOpType op_type() const { return op_type_; }
    const std::vector<FileInfo>& added_files() const { return added_files_; }
    const std::vector<FileInfo>& deleted_files() const { return deleted_files_; }

    // Convert to/from protobuf message
    proto::TableMetadataEntry ToProto() const;
    static TableMetadataEntry FromProto(const proto::TableMetadataEntry& pb);

    // ISerializable interface
    common::DataChunk Serialize() const override;
    void Serialize(common::DataChunk& chunk) const override;
    bool Deserialize(const common::DataChunk& chunk) override;

private:
    MetadataOpType op_type_{MetadataOpType::Unknown};
    std::vector<FileInfo> added_files_;
    std::vector<FileInfo> deleted_files_;
};

class TableMetadataStateMachineState : public common::ISerializable {
public:
    TableMetadataStateMachineState() = default;

    // Get current state information
    const std::vector<FileInfo>& GetSSTableFiles(size_t level) const {
        auto it = sstable_files_.find(level);
        if (it != sstable_files_.end()) {
            return it->second;
        }
        static const std::vector<FileInfo> empty;
        return empty;
    }

    size_t GetLevelCount() const { return sstable_files_.size(); }
    uint64_t GetTotalSize() const { return total_size_; }
    uint64_t GetLevelSize(size_t level) const {
        auto it = level_sizes_.find(level);
        return it != level_sizes_.end() ? it->second : 0;
    }

    // Convert to/from protobuf message
    proto::TableMetadataStateMachineState ToProto() const;
    static TableMetadataStateMachineState FromProto(const proto::TableMetadataStateMachineState& pb);

    // ISerializable interface
    common::DataChunk Serialize() const override;
    void Serialize(common::DataChunk& chunk) const override;
    bool Deserialize(const common::DataChunk& chunk) override;

protected:
    // Helper methods
    void AddFiles(const std::vector<FileInfo>& files);
    void RemoveFiles(const std::vector<FileInfo>& files);

protected:
    std::unordered_map<size_t, std::vector<FileInfo>> sstable_files_;  // Level -> Files mapping
    std::unordered_map<size_t, uint64_t> level_sizes_;                 // Level -> Total size mapping
    uint64_t total_size_{0};
};

// Table metadata state machine
class TableMetadataStateMachine : public common::WalStateMachine, public TableMetadataStateMachineState {
public:
    TableMetadataStateMachine(std::shared_ptr<common::IAppendOnlyFileSystem> fs,
                              const std::string& dir_path,
                              const Config& config = Config::Default())
        : common::WalStateMachine(std::move(fs), dir_path, config) {}

    common::Result<common::DataChunk> GetCurrentState() override;
    common::Result<void> RestoreState(const common::DataChunk& state_data) override;

private:
    // State machine methods
    common::Result<void> ApplyEntry(const common::DataChunk& entry_data) override;
};

}  // namespace pond::kv