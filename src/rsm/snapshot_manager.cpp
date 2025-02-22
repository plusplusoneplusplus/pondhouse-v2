#include "rsm/snapshot_manager.h"

#include <filesystem>
#include <sstream>

#include "common/error.h"
#include "common/filesystem_stream.h"
#include "common/log.h"

using namespace pond::common;

namespace pond::rsm {

// +------------------------+
// |     Snapshot Data      |
// |         ...            |
// +------------------------+
// |       Metadata         |
// +------------------------+
// |   Metadata Offset      |
// +------------------------+
// |    Magic Number        |
// +------------------------+

namespace {
constexpr char kSnapshotFileExtension[] = ".snapshot";
constexpr uint32_t kMagicNumber = 0x534E4150;  // "SNAP" in hex
constexpr size_t kFooterSize = sizeof(uint32_t) + sizeof(uint64_t) + sizeof(SnapshotMetadata);

// Footer format:
// | Metadata (sizeof(SnapshotMetadata)) | Metadata Offset (8B) | Magic Number (4B) |
struct SnapshotFooter {
    SnapshotMetadata metadata;
    uint64_t metadata_offset;  // Offset where metadata starts in the file
    uint32_t magic;            // Magic number for validation

    void Serialize(uint8_t* buffer) const {
        size_t offset = 0;
        std::memcpy(buffer + offset, &metadata, sizeof(metadata));
        offset += sizeof(metadata);
        std::memcpy(buffer + offset, &metadata_offset, sizeof(metadata_offset));
        offset += sizeof(metadata_offset);
        std::memcpy(buffer + offset, &magic, sizeof(magic));
    }

    static Result<SnapshotFooter> Deserialize(const uint8_t* buffer) {
        SnapshotFooter footer;
        size_t offset = 0;

        // Read in reverse order to validate magic number first
        offset = kFooterSize - sizeof(uint32_t);
        std::memcpy(&footer.magic, buffer + offset, sizeof(footer.magic));
        if (footer.magic != kMagicNumber) {
            return Result<SnapshotFooter>::failure(ErrorCode::FileCorrupted, "Invalid magic number in snapshot footer");
        }

        offset -= sizeof(uint64_t);
        std::memcpy(&footer.metadata_offset, buffer + offset, sizeof(footer.metadata_offset));

        offset = 0;
        std::memcpy(&footer.metadata, buffer + offset, sizeof(footer.metadata));

        return Result<SnapshotFooter>::success(footer);
    }
};

}  // namespace

FileSystemSnapshotManager::FileSystemSnapshotManager(std::shared_ptr<common::IAppendOnlyFileSystem> fs,
                                                     const SnapshotConfig& config)
    : fs_(std::move(fs)), config_(config) {}

Result<std::shared_ptr<ISnapshotManager>> FileSystemSnapshotManager::Create(
    std::shared_ptr<common::IAppendOnlyFileSystem> fs, const SnapshotConfig& config) {
    if (!fs) {
        return Result<std::shared_ptr<ISnapshotManager>>::failure(ErrorCode::InvalidArgument, "Filesystem is null");
    }

    if (config.snapshot_dir.empty()) {
        return Result<std::shared_ptr<ISnapshotManager>>::failure(ErrorCode::InvalidArgument,
                                                                  "Snapshot directory is empty");
    }

    return Result<std::shared_ptr<ISnapshotManager>>::success(
        std::shared_ptr<FileSystemSnapshotManager>(new FileSystemSnapshotManager(fs, config)));
}

Result<SnapshotMetadata> FileSystemSnapshotManager::CreateSnapshot(ISnapshotable* state) {
    using ReturnType = Result<SnapshotMetadata>;
    if (!state) {
        return ReturnType::failure(ErrorCode::InvalidArgument, "State is null");
    }

    std::lock_guard<std::recursive_mutex> lock(mutex_);

    // Create a temporary file for the snapshot
    std::string temp_path = config_.snapshot_dir + "/temp_snapshot";

    // Check if temp file exists and delete it if it does
    if (fs_->Exists(temp_path)) {
        RETURN_IF_ERROR_T(ReturnType, fs_->DeleteFiles({temp_path}));
    }

    SnapshotFooter footer;
    std::string final_path;

    {
        // Create new temp file
        auto stream_result = common::FileSystemOutputStream::Create(fs_, temp_path);
        if (!stream_result.ok()) {
            return ReturnType::failure(stream_result.error());
        }
        auto& stream = stream_result.value();

        // Create snapshot using the state machine
        auto snapshot_result = state->CreateSnapshot(stream.get());
        if (!snapshot_result.ok()) {
            auto _ = fs_->DeleteFiles({temp_path});  // Clean up temp file
            return snapshot_result;
        }

        // Generate final snapshot path using the new metadata
        std::string snapshot_id = GenerateSnapshotId(snapshot_result.value());
        final_path = GetSnapshotFilePath(snapshot_id);

        // Write footer
        footer.metadata = snapshot_result.value();
        footer.metadata_offset = stream->Position();
        footer.magic = kMagicNumber;

        auto footer_chunk = std::make_shared<common::DataChunk>(kFooterSize);
        footer.Serialize(footer_chunk->Data());
        auto write_result = stream->Write(footer_chunk);
        if (!write_result.ok()) {
            auto _ = fs_->DeleteFiles({temp_path});  // Clean up temp file
            return ReturnType::failure(write_result.error());
        }

        // close the stream ...
    }

    // Rename temp file to final path
    auto rename_result = fs_->RenameFiles({{temp_path, final_path}});
    if (!rename_result.ok()) {
        auto _ = fs_->DeleteFiles({temp_path});  // Clean up temp file
        return ReturnType::failure(rename_result.error());
    }

    // Prune old snapshots if needed
    if (config_.keep_snapshots > 0) {
        PruneSnapshots(config_.keep_snapshots);
    }

    return ReturnType::success(footer.metadata);
}

Result<bool> FileSystemSnapshotManager::RestoreSnapshot(ISnapshotable* state, const std::string& snapshot_id) {
    if (!state) {
        return Result<bool>::failure(ErrorCode::InvalidArgument, "State is null");
    }

    std::lock_guard<std::recursive_mutex> lock(mutex_);

    // Open snapshot file
    std::string snapshot_path = GetSnapshotFilePath(snapshot_id);
    auto stream_result = common::FileSystemInputStream::Create(fs_, snapshot_path);
    if (!stream_result.ok()) {
        return Result<bool>::failure(stream_result.error());
    }
    auto& stream = stream_result.value();

    // Read footer
    auto size_result = stream->Size();
    if (!size_result.ok()) {
        return Result<bool>::failure(size_result.error());
    }

    if (size_result.value() < kFooterSize) {
        return Result<bool>::failure(ErrorCode::FileCorrupted, "Snapshot file too small");
    }

    auto seek_result = stream->Seek(size_result.value() - kFooterSize);
    if (!seek_result.ok()) {
        return Result<bool>::failure(seek_result.error());
    }

    auto footer_chunk_result = stream->Read(kFooterSize);
    if (!footer_chunk_result.ok()) {
        return Result<bool>::failure(footer_chunk_result.error());
    }

    auto footer_result = SnapshotFooter::Deserialize(footer_chunk_result.value()->Data());
    if (!footer_result.ok()) {
        return Result<bool>::failure(footer_result.error());
    }

    // Seek back to start for snapshot data
    seek_result = stream->Seek(0);
    if (!seek_result.ok()) {
        return Result<bool>::failure(seek_result.error());
    }

    // Apply snapshot using the state machine
    return state->ApplySnapshot(stream.get(), footer_result.value().metadata);
}

Result<std::vector<SnapshotMetadata>> FileSystemSnapshotManager::ListSnapshots() const {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    std::vector<SnapshotMetadata> snapshots;

    // List all files in snapshot directory
    auto files = fs_->List(config_.snapshot_dir, false /* recursive */);
    if (!files.ok()) {
        return Result<std::vector<SnapshotMetadata>>::failure(files.error());
    }

    // Read metadata from each snapshot file
    for (const auto& file : files.value()) {
        if (file.find(kSnapshotFileExtension) != std::string::npos) {
            std::string snapshot_id = file.substr(0, file.length() - strlen(kSnapshotFileExtension));
            auto metadata_result = ReadMetadata(snapshot_id);
            if (metadata_result.ok()) {
                snapshots.push_back(metadata_result.value());
            }
        }
    }

    // Sort by LSN in descending order
    std::sort(snapshots.begin(), snapshots.end(), [](const SnapshotMetadata& a, const SnapshotMetadata& b) {
        return a.lsn > b.lsn;
    });

    return Result<std::vector<SnapshotMetadata>>::success(std::move(snapshots));
}

Result<bool> FileSystemSnapshotManager::PruneSnapshots(size_t keep_count) {
    std::lock_guard<std::recursive_mutex> lock(mutex_);
    auto snapshots_result = ListSnapshots();
    if (!snapshots_result.ok()) {
        return Result<bool>::failure(snapshots_result.error());
    }

    auto& snapshots = snapshots_result.value();
    if (snapshots.size() <= keep_count) {
        return Result<bool>::success(true);
    }

    // Delete older snapshots
    for (size_t i = keep_count; i < snapshots.size(); i++) {
        std::string snapshot_id = GenerateSnapshotId(snapshots[i]);
        std::string snapshot_path = GetSnapshotFilePath(snapshot_id);
        auto delete_result = fs_->DeleteFiles({snapshot_path});
        if (!delete_result.ok()) {
            LOG_WARNING("Failed to delete snapshot file %s: %s",
                        snapshot_path.c_str(),
                        delete_result.error().message().c_str());
        }
    }

    return Result<bool>::success(true);
}

std::string FileSystemSnapshotManager::GetSnapshotFilePath(const std::string& snapshot_id) const {
    return config_.snapshot_dir + "/" + snapshot_id + kSnapshotFileExtension;
}

Result<SnapshotMetadata> FileSystemSnapshotManager::ReadMetadata(const std::string& snapshot_id) const {
    std::string snapshot_path = GetSnapshotFilePath(snapshot_id);
    auto stream_result = common::FileSystemInputStream::Create(fs_, snapshot_path);
    if (!stream_result.ok()) {
        return Result<SnapshotMetadata>::failure(stream_result.error());
    }
    auto& stream = stream_result.value();

    // Read footer
    auto size_result = stream->Size();
    if (!size_result.ok()) {
        return Result<SnapshotMetadata>::failure(size_result.error());
    }

    if (size_result.value() < kFooterSize) {
        return Result<SnapshotMetadata>::failure(ErrorCode::FileCorrupted, "Snapshot file too small");
    }

    auto seek_result = stream->Seek(size_result.value() - kFooterSize);
    if (!seek_result.ok()) {
        return Result<SnapshotMetadata>::failure(seek_result.error());
    }

    auto footer_chunk_result = stream->Read(kFooterSize);
    if (!footer_chunk_result.ok()) {
        return Result<SnapshotMetadata>::failure(footer_chunk_result.error());
    }

    auto footer_result = SnapshotFooter::Deserialize(footer_chunk_result.value()->Data());
    if (!footer_result.ok()) {
        return Result<SnapshotMetadata>::failure(footer_result.error());
    }

    return Result<SnapshotMetadata>::success(footer_result.value().metadata);
}

std::string FileSystemSnapshotManager::GenerateSnapshotId(const SnapshotMetadata& metadata) const {
    return std::to_string(metadata.lsn);
}

}  // namespace pond::rsm