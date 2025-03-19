#pragma once

#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/data_chunk.h"
#include "common/serializable.h"
#include "proto/kv.pb.h"
#include "rsm/api.h"

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
                       const std::vector<FileInfo>& deleted = {},
                       uint64_t sequence = 0)
        : op_type_(op_type), added_files_(added), deleted_files_(deleted), sequence_(sequence) {}

    MetadataOpType op_type() const { return op_type_; }
    const std::vector<FileInfo>& added_files() const { return added_files_; }
    const std::vector<FileInfo>& deleted_files() const { return deleted_files_; }
    uint64_t sequence() const { return sequence_; }
    void set_sequence(uint64_t sequence) { sequence_ = sequence; }

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
    uint64_t sequence_{0};  // WAL sequence number associated with this operation
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

    // Log file management
    const std::vector<uint64_t>& GetActiveLogSequences() const { return active_log_sequences_; }
    const std::vector<uint64_t>& GetPendingGCSequences() const { return pending_gc_sequences_; }
    void AddActiveLogSequence(uint64_t sequence) { active_log_sequences_.push_back(sequence); }
    void RemoveActiveLogSequence(uint64_t sequence);
    void AddPendingGCSequence(uint64_t sequence) { pending_gc_sequences_.push_back(sequence); }
    void ClearPendingGCSequences() { pending_gc_sequences_.clear(); }

    // Get the minimum sequence number of active log files
    std::optional<uint64_t> GetMinActiveLogSequence() const {
        if (active_log_sequences_.empty()) {
            return std::nullopt;
        }
        return *std::min_element(active_log_sequences_.begin(), active_log_sequences_.end());
    }

    bool HasHistory() const { return !active_log_sequences_.empty() || total_size_ > 0; }

    std::string ToString(bool verbose = false) const;

protected:
    // Helper methods
    void AddFiles(const std::vector<FileInfo>& files);
    void RemoveFiles(const std::vector<FileInfo>& files);

protected:
    std::unordered_map<size_t, std::vector<FileInfo>> sstable_files_;  // Level -> Files mapping
    std::unordered_map<size_t, uint64_t> level_sizes_;                 // Level -> Total size mapping
    uint64_t total_size_{0};
    uint64_t sstable_flush_wal_sequence_{common::INVALID_LSN};
    std::vector<uint64_t>
        active_log_sequences_;  // Sequence numbers of log files that must be kept (memtable not flushed)
    std::vector<uint64_t>
        pending_gc_sequences_;  // Sequence numbers of log files that can be deleted after next checkpoint
};

// Table metadata state machine
class TableMetadataStateMachine : public rsm::ReplicatedStateMachine, public TableMetadataStateMachineState {
public:
    TableMetadataStateMachine(std::shared_ptr<common::IAppendOnlyFileSystem> fs,
                              const std::string& dir_path,
                              const rsm::ReplicationConfig& config = rsm::ReplicationConfig::Default(),
                              const rsm::SnapshotConfig& snapshot_config = rsm::SnapshotConfig::Default("snapshots"));

    common::Result<bool> Open() {
        auto result =
            Initialize(rsm::ReplicationConfig{.directory = dir_path_}, rsm::SnapshotConfig{.snapshot_dir = dir_path_});
        if (!result.ok()) {
            return result;
        }
        return Recover();
    }

    ~TableMetadataStateMachine() { Close(); }

protected:
    // ReplicatedStateMachine interface
    void ExecuteReplicatedLog(uint64_t lsn, const common::DataChunk& data) override;
    void SaveState(common::OutputStream* writer) override;
    void LoadState(common::InputStream* reader) override;

    // private methods
    void TruncateLogFiles(uint64_t sequence);

private:
    std::shared_ptr<common::IAppendOnlyFileSystem> fs_;
    std::string dir_path_;
};

}  // namespace pond::kv