#include "kv/table_metadata.h"

#include "common/data_chunk_writer.h"

namespace pond::kv {

// FileInfo implementations
proto::FileInfo FileInfo::ToProto() const {
    proto::FileInfo pb;
    pb.set_name(name);
    pb.set_size(size);
    pb.set_level(level);
    pb.set_smallest_key(smallest_key.data(), smallest_key.size());
    pb.set_largest_key(largest_key.data(), largest_key.size());
    return pb;
}

FileInfo FileInfo::FromProto(const proto::FileInfo& pb) {
    return FileInfo(pb.name(),
                    pb.size(),
                    pb.level(),
                    std::string(pb.smallest_key().data(), pb.smallest_key().size()),
                    std::string(pb.largest_key().data(), pb.largest_key().size()));
}

common::DataChunk FileInfo::Serialize() const {
    common::DataChunk chunk;
    Serialize(chunk);
    return chunk;
}

void FileInfo::Serialize(common::DataChunk& chunk) const {
    common::DataChunkOutputStream output_stream(chunk);
    LOG_CHECK(ToProto().SerializeToZeroCopyStream(&output_stream), "Failed to serialize FileInfo");
}

bool FileInfo::Deserialize(const common::DataChunk& chunk) {
    proto::FileInfo pb;
    if (!pb.ParseFromArray(chunk.Data(), chunk.Size())) {
        return false;
    }
    *this = FromProto(pb);
    return true;
}

// TableMetadataEntry implementations
proto::TableMetadataEntry TableMetadataEntry::ToProto() const {
    proto::TableMetadataEntry pb;
    pb.set_op_type(static_cast<proto::MetadataOpType>(op_type_));
    pb.set_wal_sequence(sequence_);

    // Convert added files
    for (const auto& file : added_files_) {
        *pb.add_added_files() = file.ToProto();
    }

    // Convert deleted files
    for (const auto& file : deleted_files_) {
        *pb.add_deleted_files() = file.ToProto();
    }

    return pb;
}

TableMetadataEntry TableMetadataEntry::FromProto(const proto::TableMetadataEntry& pb) {
    std::vector<FileInfo> added;
    for (const auto& file_pb : pb.added_files()) {
        added.push_back(FileInfo::FromProto(file_pb));
    }

    std::vector<FileInfo> deleted;
    for (const auto& file_pb : pb.deleted_files()) {
        deleted.push_back(FileInfo::FromProto(file_pb));
    }

    return TableMetadataEntry(static_cast<MetadataOpType>(pb.op_type()), added, deleted, pb.wal_sequence());
}

common::DataChunk TableMetadataEntry::Serialize() const {
    common::DataChunk chunk;
    Serialize(chunk);
    return chunk;
}

void TableMetadataEntry::Serialize(common::DataChunk& chunk) const {
    common::DataChunkOutputStream output_stream(chunk);
    LOG_CHECK(ToProto().SerializeToZeroCopyStream(&output_stream), "Failed to serialize TableMetadataEntry");
}

bool TableMetadataEntry::Deserialize(const common::DataChunk& chunk) {
    proto::TableMetadataEntry pb;
    if (!pb.ParseFromArray(chunk.Data(), chunk.Size())) {
        return false;
    }
    *this = FromProto(pb);
    return true;
}

// TableMetadataStateMachineState implementations
void TableMetadataStateMachineState::RemoveActiveLogSequence(uint64_t sequence) {
    auto it = std::find(active_log_sequences_.begin(), active_log_sequences_.end(), sequence);
    if (it != active_log_sequences_.end()) {
        active_log_sequences_.erase(it);
    }
}

proto::TableMetadataStateMachineState TableMetadataStateMachineState::ToProto() const {
    proto::TableMetadataStateMachineState state;
    state.set_total_size(total_size_);
    state.set_sstable_flush_wal_sequence(sstable_flush_wal_sequence_);

    // Serialize each level's files
    for (const auto& [level, files] : sstable_files_) {
        auto* level_state = state.add_levels();
        level_state->set_level(level);
        level_state->set_total_size(level_sizes_.at(level));

        for (const auto& file : files) {
            *level_state->add_files() = file.ToProto();
        }
    }

    // Add log sequences
    for (const auto sequence : active_log_sequences_) {
        state.add_active_log_sequences(sequence);
    }
    for (const auto sequence : pending_gc_sequences_) {
        state.add_pending_gc_sequences(sequence);
    }

    return state;
}

TableMetadataStateMachineState TableMetadataStateMachineState::FromProto(
    const proto::TableMetadataStateMachineState& pb) {
    TableMetadataStateMachineState state;
    state.total_size_ = pb.total_size();
    state.sstable_flush_wal_sequence_ = pb.sstable_flush_wal_sequence();

    // Restore state from protobuf
    for (const auto& level_state : pb.levels()) {
        size_t level = level_state.level();
        state.level_sizes_[level] = level_state.total_size();

        for (const auto& file_pb : level_state.files()) {
            state.sstable_files_[level].push_back(FileInfo::FromProto(file_pb));
        }
    }

    // Restore log sequences
    for (const auto sequence : pb.active_log_sequences()) {
        state.active_log_sequences_.push_back(sequence);
    }
    for (const auto sequence : pb.pending_gc_sequences()) {
        state.pending_gc_sequences_.push_back(sequence);
    }

    return state;
}

common::DataChunk TableMetadataStateMachineState::Serialize() const {
    common::DataChunk chunk;
    Serialize(chunk);
    return chunk;
}

void TableMetadataStateMachineState::Serialize(common::DataChunk& chunk) const {
    common::DataChunkOutputStream output_stream(chunk);
    LOG_CHECK(ToProto().SerializeToZeroCopyStream(&output_stream),
              "Failed to serialize TableMetadataStateMachineState");
}

bool TableMetadataStateMachineState::Deserialize(const common::DataChunk& chunk) {
    proto::TableMetadataStateMachineState pb;
    if (!pb.ParseFromArray(chunk.Data(), chunk.Size())) {
        return false;
    }
    *this = FromProto(pb);
    return true;
}

void TableMetadataStateMachineState::AddFiles(const std::vector<FileInfo>& files) {
    for (const auto& file : files) {
        if (sstable_files_.find(file.level) == sstable_files_.end()) {
            sstable_files_[file.level] = {};
        }

        sstable_files_[file.level].push_back(file);
        level_sizes_[file.level] += file.size;
        total_size_ += file.size;
    }
}

void TableMetadataStateMachineState::RemoveFiles(const std::vector<FileInfo>& files) {
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

std::string TableMetadataStateMachineState::ToString(bool verbose) const {
    std::stringstream ss;
    ss << "TableMetadataStateMachineState{" << std::endl;
    ss << "  total_size: " << total_size_ << std::endl;
    ss << "  sstable_flush_wal_sequence: " << sstable_flush_wal_sequence_ << std::endl;
    ss << "  sstable_files: " << sstable_files_.size() << std::endl;
    ss << "  level_sizes: " << level_sizes_.size() << std::endl;
    ss << "  active_log_sequences: " << active_log_sequences_.size() << std::endl;
    ss << "  pending_gc_sequences: " << pending_gc_sequences_.size() << std::endl;
    if (verbose) {
        ss << "  active_log_sequences: ";
        for (const auto& sequence : active_log_sequences_) {
            ss << sequence << " ";
        }
        ss << std::endl;
    }
    ss << "}" << std::endl;
    return ss.str();
}

// TableMetadataStateMachine implementations

TableMetadataStateMachine::TableMetadataStateMachine(std::shared_ptr<common::IAppendOnlyFileSystem> fs,
                                                     const std::string& dir_path,
                                                     const rsm::ReplicationConfig& config,
                                                     const rsm::SnapshotConfig& snapshot_config)
    : rsm::ReplicatedStateMachine(std::make_shared<rsm::WalReplication>(fs),
                                  rsm::FileSystemSnapshotManager::Create(fs, snapshot_config).value()),
      fs_(fs),
      dir_path_(dir_path) {}

void TableMetadataStateMachine::ExecuteReplicatedLog(uint64_t lsn, const common::DataChunk& entry_data) {
    TableMetadataEntry entry;
    if (!entry.Deserialize(entry_data)) {
        LOG_ERROR("Failed to deserialize metadata entry");
        return;
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
        case MetadataOpType::FlushMemTable: {
            LOG_CHECK(
                sstable_flush_wal_sequence_ == common::INVALID_LSN || entry.sequence() >= sstable_flush_wal_sequence_,
                "FlushMemTable called multiple times");
            sstable_flush_wal_sequence_ = entry.sequence();

            TruncateLogFiles(sstable_flush_wal_sequence_);
            break;
        }
        case MetadataOpType::RotateWAL:
            // When WAL is rotated, add the new log sequence to active list
            AddActiveLogSequence(entry.sequence());
            TruncateLogFiles(sstable_flush_wal_sequence_);
            break;

        case MetadataOpType::Unknown:
            LOG_ERROR("Unknown metadata operation type");
            break;
    }
}

void TableMetadataStateMachine::TruncateLogFiles(uint64_t sequence) {
    if (sequence == common::INVALID_LSN) {
        return;
    }

    // Move all log sequences less than current sequence to pending GC
    // Keep the last sequence untouched
    if (active_log_sequences_.size() > 1) {
        for (auto i = 0; i < active_log_sequences_.size() - 1; ++i) {
            auto last = active_log_sequences_.back();
            for (auto it = active_log_sequences_.begin(); it != active_log_sequences_.end();) {
                if (*it <= sequence && *it != last) {
                    AddPendingGCSequence(*it);
                    it = active_log_sequences_.erase(it);
                } else {
                    ++it;
                }
            }
        }
    }
}

void TableMetadataStateMachine::SaveState(common::OutputStream* writer) {
    auto data = Serialize();
    writer->Write(data.Data(), data.Size());
}

void TableMetadataStateMachine::LoadState(common::InputStream* reader) {
    auto result = reader->Read(reader->Size().value());

    LOG_CHECK(result.ok(), "Failed to read state data");
    LOG_CHECK(Deserialize(*result.value()), "Failed to parse state data");
}

}  // namespace pond::kv