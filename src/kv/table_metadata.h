#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "common/data_chunk.h"
#include "common/serializable.h"
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
    CompactFiles,   // Compact files
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

    common::DataChunk Serialize() const override {
        common::DataChunk chunk;
        Serialize(chunk);
        return chunk;
    }

    void Serialize(common::DataChunk& chunk) const override {
        // Serialize name
        uint32_t name_size = name.size();
        chunk.Append(reinterpret_cast<const uint8_t*>(&name_size), sizeof(name_size));
        chunk.Append(reinterpret_cast<const uint8_t*>(name.data()), name_size);

        // Serialize size and level
        chunk.Append(reinterpret_cast<const uint8_t*>(&size), sizeof(size));
        chunk.Append(reinterpret_cast<const uint8_t*>(&level), sizeof(level));

        // Serialize smallest key
        uint32_t min_key_size = smallest_key.size();
        chunk.Append(reinterpret_cast<const uint8_t*>(&min_key_size), sizeof(min_key_size));
        chunk.Append(reinterpret_cast<const uint8_t*>(smallest_key.data()), min_key_size);

        // Serialize largest key
        uint32_t max_key_size = largest_key.size();
        chunk.Append(reinterpret_cast<const uint8_t*>(&max_key_size), sizeof(max_key_size));
        chunk.Append(reinterpret_cast<const uint8_t*>(largest_key.data()), max_key_size);
    }

    bool Deserialize(const common::DataChunk& chunk) override {
        if (chunk.Size() < sizeof(uint32_t)) {
            return false;
        }

        const uint8_t* ptr = chunk.Data();
        const uint8_t* end = ptr + chunk.Size();

        // Deserialize name
        uint32_t name_size;
        std::memcpy(&name_size, ptr, sizeof(name_size));
        ptr += sizeof(name_size);

        name = std::string(reinterpret_cast<const char*>(ptr), name_size);
        ptr += name_size;

        std::memcpy(&size, ptr, sizeof(size));
        ptr += sizeof(size);
        std::memcpy(&level, ptr, sizeof(level));
        ptr += sizeof(level);

        uint32_t min_key_size;
        std::memcpy(&min_key_size, ptr, sizeof(min_key_size));
        ptr += sizeof(min_key_size);

        if (ptr + min_key_size > end) {
            return false;
        }
        smallest_key = std::string(reinterpret_cast<const char*>(ptr), min_key_size);
        ptr += min_key_size;

        uint32_t max_key_size;
        std::memcpy(&max_key_size, ptr, sizeof(max_key_size));
        ptr += sizeof(max_key_size);

        largest_key = std::string(reinterpret_cast<const char*>(ptr), max_key_size);
        ptr += max_key_size;

        return true;
    }
};

// Entry for tracking table metadata operations
class TableMetadataEntry : public common::WalEntry {
public:
    TableMetadataEntry() = default;
    TableMetadataEntry(MetadataOpType op_type,
                       const std::vector<FileInfo>& added_files = {},
                       const std::vector<FileInfo>& deleted_files = {})
        : op_type_(op_type), added_files_(added_files), deleted_files_(deleted_files) {}

    common::DataChunk Serialize() const override;
    void Serialize(common::DataChunk& chunk) const override;
    bool Deserialize(const common::DataChunk& chunk) override;
    void SerializeFiles(const std::vector<FileInfo>& files, common::DataChunk& chunk) const;
    bool DeserializeFiles(const uint8_t*& ptr, std::vector<FileInfo>& files);

    MetadataOpType op_type() const { return op_type_; }
    const std::vector<FileInfo>& added_files() const { return added_files_; }
    const std::vector<FileInfo>& deleted_files() const { return deleted_files_; }

private:
    MetadataOpType op_type_{MetadataOpType::CreateSSTable};
    std::vector<FileInfo> added_files_;
    std::vector<FileInfo> deleted_files_;
};

// Table metadata state machine
class TableMetadataStateMachine : public common::WalStateMachine {
public:
    TableMetadataStateMachine(std::shared_ptr<common::IAppendOnlyFileSystem> fs,
                              const std::string& dir_path,
                              const Config& config = Config::Default())
        : common::WalStateMachine(std::move(fs), dir_path, config) {}

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

public:
    // state machine methods
    common::Result<void> ApplyEntry(const common::DataChunk& entry_data) override;
    common::Result<common::DataChunk> GetCurrentState() override;
    common::Result<void> RestoreState(const common::DataChunk& state_data) override;

private:
    void AddFiles(const std::vector<FileInfo>& files);
    void RemoveFiles(const std::vector<FileInfo>& files);

private:
    std::unordered_map<size_t, std::vector<FileInfo>> sstable_files_;  // Level -> Files mapping
    std::unordered_map<size_t, uint64_t> level_sizes_;                 // Level -> Total size mapping
    uint64_t total_size_{0};
};

}  // namespace pond::kv