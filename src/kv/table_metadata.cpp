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

void TableMetadataEntry::Serialize(common::DataChunk& chunk) const {
    // Serialize operation type
    chunk.Append(reinterpret_cast<const uint8_t*>(&op_type_), sizeof(op_type_));

    // Serialize number of files
    uint32_t num_files = files_.size();
    chunk.Append(reinterpret_cast<const uint8_t*>(&num_files), sizeof(num_files));

    // Serialize each file info
    for (const auto& file : files_) {
        uint32_t name_size = file.name.size();
        chunk.Append(reinterpret_cast<const uint8_t*>(&name_size), sizeof(name_size));
        chunk.Append(reinterpret_cast<const uint8_t*>(file.name.data()), name_size);
        chunk.Append(reinterpret_cast<const uint8_t*>(&file.size), sizeof(file.size));
    }
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
    uint32_t num_files;
    std::memcpy(&num_files, ptr, sizeof(num_files));
    ptr += sizeof(num_files);

    // Read each file info
    files_.clear();
    for (uint32_t i = 0; i < num_files; i++) {
        if (ptr + sizeof(uint32_t) > chunk.Data() + chunk.Size()) {
            LOG_ERROR("Invalid metadata entry size: {}", chunk.Size());
            return false;
        }

        uint32_t name_size;
        std::memcpy(&name_size, ptr, sizeof(name_size));
        ptr += sizeof(name_size);

        if (ptr + name_size + sizeof(uint64_t) > chunk.Data() + chunk.Size()) {
            LOG_ERROR("Invalid metadata entry size: {}", chunk.Size());
            return false;
        }

        std::string name(reinterpret_cast<const char*>(ptr), name_size);
        ptr += name_size;

        uint64_t size;
        std::memcpy(&size, ptr, sizeof(size));
        ptr += sizeof(size);

        files_.emplace_back(name, size);
    }

    return true;
}

common::Result<std::unique_ptr<common::ISerializable>> TableMetadataEntry::DeserializeAsUniquePtr(
    const common::DataChunk& chunk) const {
    TableMetadataEntry entry;
    if (!entry.Deserialize(chunk)) {
        return common::Result<std::unique_ptr<common::ISerializable>>::failure(common::ErrorCode::InvalidArgument,
                                                                               "Failed to deserialize metadata entry");
    }

    return common::Result<std::unique_ptr<common::ISerializable>>::success(std::make_unique<TableMetadataEntry>(entry));
}

common::Result<void> TableMetadataStateMachine::ApplyEntry(const common::DataChunk& entry_data) {
    TableMetadataEntry entry;
    if (!entry.Deserialize(entry_data)) {
        return common::Result<void>::failure(common::ErrorCode::InvalidArgument,
                                             "Failed to deserialize metadata entry");
    }

    switch (entry.op_type()) {
        case MetadataOpType::CreateSSTable:
            for (const auto& file : entry.files()) {
                sstable_files_[file.level].push_back(file);
                level_sizes_[file.level] += file.size;
                total_size_ += file.size;
            }
            break;
        case MetadataOpType::DeleteSSTable:
            for (const auto& file : entry.files()) {
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
            break;
        case MetadataOpType::UpdateStats:
            // For UpdateStats, we expect files to contain per-level size information
            total_size_ = 0;
            level_sizes_.clear();
            for (const auto& file : entry.files()) {
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
            file.Serialize(state);
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
            common::DataChunk file_chunk(ptr, end - ptr);
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