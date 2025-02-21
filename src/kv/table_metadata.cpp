#include "table_metadata.h"

#include <cstring>

#include "common/error.h"
#include "common/log.h"

namespace pond::kv {

common::DataChunk TableMetadataEntry::Serialize() const {
    common::DataChunk chunk;
    Serialize(chunk);
    return chunk;
}

void TableMetadataEntry::SerializeFiles(const std::vector<FileInfo>& files, common::DataChunk& chunk) const {
    uint32_t num_files = files.size();
    chunk.Append(reinterpret_cast<const uint8_t*>(&num_files), sizeof(num_files));

    for (const auto& file : files) {
        common::DataChunk file_chunk;
        file.Serialize(file_chunk);
        chunk.AppendSizedDataChunk(file_chunk);
    }
}

void TableMetadataEntry::Serialize(common::DataChunk& chunk) const {
    // Serialize operation type
    chunk.Append(reinterpret_cast<const uint8_t*>(&op_type_), sizeof(op_type_));

    // Serialize added files
    SerializeFiles(added_files_, chunk);

    // Serialize deleted files
    SerializeFiles(deleted_files_, chunk);
}

bool TableMetadataEntry::DeserializeFiles(const uint8_t*& ptr, std::vector<FileInfo>& files) {
    uint32_t num_files;
    std::memcpy(&num_files, ptr, sizeof(num_files));
    ptr += sizeof(num_files);

    // Read each file info
    files.clear();
    for (uint32_t i = 0; i < num_files; i++) {
        FileInfo file;
        size_t file_size;
        std::memcpy(&file_size, ptr, sizeof(file_size));
        ptr += sizeof(file_size);

        common::DataChunk file_chunk(ptr, file_size);
        if (!file.Deserialize(file_chunk)) {
            return false;
        }
        files.push_back(file);

        ptr += file_size;
    }

    return true;
}

bool TableMetadataEntry::Deserialize(const common::DataChunk& chunk) {
    if (chunk.Size() < sizeof(op_type_) + sizeof(uint32_t)) {
        LOG_ERROR("Invalid metadata entry size: {}", chunk.Size());
        return false;
    }

    const uint8_t* ptr = chunk.Data();

    // Read operation type
    std::memcpy(&op_type_, ptr, sizeof(op_type_));
    ptr += sizeof(op_type_);

    // Read number of files
    if (!DeserializeFiles(ptr, added_files_)) {
        return false;
    }

    if (!DeserializeFiles(ptr, deleted_files_)) {
        return false;
    }

    return true;
}

void TableMetadataStateMachine::AddFiles(const std::vector<FileInfo>& files) {
    for (const auto& file : files) {
        if (sstable_files_.find(file.level) == sstable_files_.end()) {
            sstable_files_[file.level] = {};
        }

        sstable_files_[file.level].push_back(file);
        level_sizes_[file.level] += file.size;
        total_size_ += file.size;
    }
}

void TableMetadataStateMachine::RemoveFiles(const std::vector<FileInfo>& files) {
    for (const auto& file : files) {
        auto& level_files = sstable_files_[file.level];
        auto it = std::find_if(
            level_files.begin(), level_files.end(), [&](const FileInfo& f) { return f.name == file.name; });
        if (it != level_files.end()) {
            level_sizes_[file.level] -= it->size;
            total_size_ -= it->size;
            level_files.erase(it);
        }
        if (level_files.empty()) {
            sstable_files_.erase(file.level);
            level_sizes_.erase(file.level);
        }
    }
}

common::Result<void> TableMetadataStateMachine::ApplyEntry(const common::DataChunk& entry_data) {
    TableMetadataEntry entry;
    if (!entry.Deserialize(entry_data)) {
        return common::Result<void>::failure(common::ErrorCode::InvalidArgument,
                                             "Failed to deserialize metadata entry");
    }

    switch (entry.op_type()) {
        case MetadataOpType::CreateSSTable:
            AddFiles(entry.added_files());
            break;
        case MetadataOpType::DeleteSSTable:
            RemoveFiles(entry.deleted_files());
            break;
        case MetadataOpType::CompactFiles:
            AddFiles(entry.added_files());
            RemoveFiles(entry.deleted_files());
            break;

        case MetadataOpType::UpdateStats:
            // For UpdateStats, we expect files to contain per-level size information
            total_size_ = 0;
            level_sizes_.clear();
            for (const auto& file : entry.added_files()) {
                level_sizes_[file.level] = file.size;
                total_size_ += file.size;
            }
            break;
        case MetadataOpType::FlushMemTable:
        case MetadataOpType::RotateWAL:
            // These operations don't modify the state directly
            break;
    }

    return common::Result<void>::success();
}

common::Result<common::DataChunk> TableMetadataStateMachine::GetCurrentState() {
    common::DataChunk state;

    // Serialize number of levels
    uint32_t num_levels = sstable_files_.size();
    state.Append(reinterpret_cast<const uint8_t*>(&num_levels), sizeof(num_levels));

    // Serialize each level's files
    for (const auto& [level, files] : sstable_files_) {
        // Serialize level number
        uint32_t level_num = level;
        state.Append(reinterpret_cast<const uint8_t*>(&level_num), sizeof(level_num));

        // Serialize number of files in this level
        uint32_t num_files = files.size();
        state.Append(reinterpret_cast<const uint8_t*>(&num_files), sizeof(num_files));

        // Serialize each file's information using FileInfo's serialization
        for (const auto& file : files) {
            state.AppendSizedDataChunk(file.Serialize());
        }
    }

    return common::Result<common::DataChunk>::success(std::move(state));
}

common::Result<void> TableMetadataStateMachine::RestoreState(const common::DataChunk& state_data) {
    if (state_data.Size() < sizeof(uint32_t)) {
        return common::Result<void>::failure(common::ErrorCode::InvalidArgument, "Invalid state data size");
    }

    const uint8_t* ptr = state_data.Data();
    const uint8_t* end = ptr + state_data.Size();

    // Clear existing state
    sstable_files_.clear();
    level_sizes_.clear();
    total_size_ = 0;

    // Read number of levels
    uint32_t num_levels;
    std::memcpy(&num_levels, ptr, sizeof(num_levels));
    ptr += sizeof(num_levels);

    // Read each level's files
    for (uint32_t i = 0; i < num_levels; i++) {
        if (ptr + sizeof(uint32_t) * 2 > end) {
            return common::Result<void>::failure(common::ErrorCode::InvalidArgument, "Invalid state data");
        }

        // Read level number
        uint32_t level;
        std::memcpy(&level, ptr, sizeof(level));
        ptr += sizeof(level);

        // Read number of files
        uint32_t num_files;
        std::memcpy(&num_files, ptr, sizeof(num_files));
        ptr += sizeof(num_files);

        // Read each file's information
        for (uint32_t j = 0; j < num_files; j++) {
            FileInfo file;
            size_t file_size;
            std::memcpy(&file_size, ptr, sizeof(file_size));
            ptr += sizeof(file_size);

            common::DataChunk file_chunk(ptr, file_size);
            if (!file.Deserialize(file_chunk)) {
                return common::Result<void>::failure(common::ErrorCode::InvalidArgument,
                                                     "Failed to deserialize file info");
            }

            // Update pointer position based on serialized size
            ptr += file_chunk.Size();

            // Add file to state
            sstable_files_[level].push_back(file);
            level_sizes_[level] += file.size;
            total_size_ += file.size;
        }
    }

    return common::Result<void>::success();
}

}  // namespace pond::kv