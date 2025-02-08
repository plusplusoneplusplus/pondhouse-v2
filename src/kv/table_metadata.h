#pragma once

#include <string>
#include <vector>

#include "common/data_chunk.h"
#include "common/wal.h"
#include "common/wal_state_machine.h"

namespace pond::kv {

// Metadata operation types
enum class MetadataOpType {
    CreateSSTable,  // New SSTable created
    DeleteSSTable,  // SSTable deleted (after compaction)
    UpdateStats,    // Update table statistics
    FlushMemTable,  // MemTable flushed to SSTable
    RotateWAL,      // WAL rotation
};

// File information for metadata operations
struct FileInfo {
    std::string name;
    uint64_t size;

    FileInfo() = default;
    FileInfo(const std::string& n, uint64_t s) : name(n), size(s) {}
};

// Entry for tracking table metadata operations
class TableMetadataEntry : public common::WalEntry {
public:
    TableMetadataEntry() = default;
    TableMetadataEntry(MetadataOpType op_type, const std::vector<FileInfo>& files = {})
        : op_type_(op_type), files_(files) {}

    common::DataChunk Serialize() const override;
    void Serialize(common::DataChunk& chunk) const override;
    bool Deserialize(const common::DataChunk& chunk) override;
    common::Result<std::unique_ptr<common::ISerializable>> DeserializeAsUniquePtr(
        const common::DataChunk& chunk) const override;

    MetadataOpType op_type() const { return op_type_; }
    const std::vector<FileInfo>& files() const { return files_; }

private:
    MetadataOpType op_type_{MetadataOpType::CreateSSTable};
    std::vector<FileInfo> files_;
};

// Table metadata state machine
class TableMetadataStateMachine : public common::WalStateMachine {
public:
    TableMetadataStateMachine(std::shared_ptr<common::IAppendOnlyFileSystem> fs,
                              const std::string& dir_path,
                              const Config& config = Config::Default())
        : common::WalStateMachine(std::move(fs), dir_path, config) {}

    // Get current state information
    const std::vector<std::string>& GetSSTableFiles() const { return sstable_files_; }
    uint64_t GetTotalSize() const { return total_size_; }

protected:
    common::Result<void> ApplyEntry(const common::DataChunk& entry_data) override;
    common::Result<common::DataChunk> GetCurrentState() override;
    common::Result<void> RestoreState(const common::DataChunk& state_data) override;

private:
    std::vector<std::string> sstable_files_;
    uint64_t total_size_{0};
};

}  // namespace pond::kv