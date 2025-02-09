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
                sstable_files_.push_back(file.name);
                total_size_ += file.size;
            }
            break;
        case MetadataOpType::DeleteSSTable:
            for (const auto& file : entry.files()) {
                auto it = std::find(sstable_files_.begin(), sstable_files_.end(), file.name);
                if (it != sstable_files_.end()) {
                    sstable_files_.erase(it);
                    total_size_ -= file.size;
                }
            }
            break;
        case MetadataOpType::UpdateStats:
            // For UpdateStats, we expect only one entry with the total size
            if (!entry.files().empty()) {
                total_size_ = entry.files()[0].size;
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

    // Serialize number of files
    uint32_t num_files = sstable_files_.size();
    state.Append(reinterpret_cast<const uint8_t*>(&num_files), sizeof(num_files));

    // Serialize file names
    for (const auto& file : sstable_files_) {
        uint32_t name_size = file.size();
        state.Append(reinterpret_cast<const uint8_t*>(&name_size), sizeof(name_size));
        state.Append(reinterpret_cast<const uint8_t*>(file.data()), name_size);
    }

    // Serialize total size
    state.Append(reinterpret_cast<const uint8_t*>(&total_size_), sizeof(total_size_));

    return common::Result<common::DataChunk>::success(std::move(state));
}

common::Result<void> TableMetadataStateMachine::RestoreState(const common::DataChunk& state_data) {
    if (state_data.Size() < sizeof(uint32_t)) {
        return common::Result<void>::failure(common::ErrorCode::InvalidArgument, "Invalid state data size");
    }

    const uint8_t* ptr = state_data.Data();

    // Read number of files
    uint32_t num_files;
    std::memcpy(&num_files, ptr, sizeof(num_files));
    ptr += sizeof(num_files);

    // Read file names
    sstable_files_.clear();
    for (uint32_t i = 0; i < num_files; i++) {
        if (ptr + sizeof(uint32_t) > state_data.Data() + state_data.Size()) {
            return common::Result<void>::failure(common::ErrorCode::InvalidArgument, "Invalid state data");
        }

        uint32_t name_size;
        std::memcpy(&name_size, ptr, sizeof(name_size));
        ptr += sizeof(name_size);

        if (ptr + name_size > state_data.Data() + state_data.Size()) {
            return common::Result<void>::failure(common::ErrorCode::InvalidArgument, "Invalid state data");
        }

        std::string name(reinterpret_cast<const char*>(ptr), name_size);
        ptr += name_size;

        sstable_files_.push_back(std::move(name));
    }

    // Read total size
    if (ptr + sizeof(total_size_) > state_data.Data() + state_data.Size()) {
        return common::Result<void>::failure(common::ErrorCode::InvalidArgument, "Invalid state data");
    }
    std::memcpy(&total_size_, ptr, sizeof(total_size_));

    return common::Result<void>::success();
}

}  // namespace pond::kv