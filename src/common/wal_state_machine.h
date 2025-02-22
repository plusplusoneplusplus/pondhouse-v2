#pragma once

#include <algorithm>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "common/append_only_fs.h"
#include "common/data_chunk.h"
#include "common/error.h"
#include "common/log.h"
#include "common/result.h"
#include "common/serializable.h"
#include "common/wal.h"

namespace pond::common {

// WAL entry for state machine operations
class StateMachineWalEntry final : public WalEntry {
public:
    DataChunk data;

    // Implement ISerializable interface
    DataChunk Serialize() const override { return data; }

    void Serialize(DataChunk& chunk) const override { chunk = data; }

    bool Deserialize(const DataChunk& chunk) override {
        data = chunk;
        return true;
    }
};

/**
 * Base class for WAL-based state machines.
 * Maintains an in-memory state that can be updated through WAL entries.
 * Supports checkpointing and recovery.
 * At any time, the state machine has one active WAL file and one or more checkpoint files.
 * The latest checkpoint file contains the state of the state machine at the time of the checkpoint.
 */
class WalStateMachine {
public:
    struct Config {
        size_t max_wal_size = 64 * 1024 * 1024;  // 64MB default
        size_t checkpoint_interval = 1000;       // Number of entries between checkpoints
        size_t max_checkpoints = 3;              // Maximum number of checkpoint files to keep

        static Config Default() {
            static Config config;
            return config;
        }
    };

    static constexpr const char* WAL_FILE_NAME = "state.wal";
    static constexpr const char* CHECKPOINT_FILE_PREFIX = "state.checkpoint.";

    WalStateMachine(std::shared_ptr<IAppendOnlyFileSystem> fs,
                    const std::string& dir_path,
                    const Config& config = Config::Default())
        : fs_(std::move(fs)), dir_path_(dir_path), config_(config), wal_(fs_) {
        // Create directory if it doesn't exist
        if (!fs_->Exists(dir_path_)) {
            auto result = fs_->CreateDirectory(dir_path_);
            if (!result.ok()) {
                throw std::runtime_error("Failed to create directory: " + result.error().message());
            }
        }
    }

    virtual ~WalStateMachine() = default;

    // Initialize or recover the state machine
    Result<bool> Open() {
        std::lock_guard<std::mutex> lock(mutex_);

        // First try to recover from the latest checkpoint
        auto checkpoint_result = RecoverFromCheckpoint();
        if (!checkpoint_result.ok()) {
            return Result<bool>::failure(checkpoint_result.error());
        }

        // Open WAL file
        auto result = wal_.Open(GetWALPath(), false /* recover */);
        if (!result.ok()) {
            return Result<bool>::failure(result.error());
        }

        // Replay WAL entries since checkpoint
        auto entries = wal_.Read(checkpoint_lsn_);
        if (!entries.ok()) {
            return Result<bool>::failure(entries.error());
        }

        // Apply each entry to recover state
        for (const auto& entry : entries.value()) {
            auto apply_result = ApplyEntry(entry.data);
            if (!apply_result.ok()) {
                return Result<bool>::failure(apply_result.error());
            }
        }

        return Result<bool>::success(true);
    }

    Result<void> Apply(const DataChunk& entry_data) { return ApplyInternal(entry_data, true /* acquire_lock */); }

    Result<void> ApplyNoLock(const DataChunk& entry_data) {
        return ApplyInternal(entry_data, false /* acquire_lock */);
    }

    Result<void> CreateCheckpoint() { return CreateCheckpointInternal(true /* acquire_lock */); }

    Result<void> CreateCheckpointNoLock() { return CreateCheckpointInternal(false /* acquire_lock */); }

protected:
    // Apply an entry to update the state
    virtual Result<void> ApplyEntry(const DataChunk& entry_data) = 0;

    // Get the current state for checkpointing
    virtual Result<DataChunkPtr> GetCurrentState() = 0;

    // Restore state from a checkpoint
    virtual Result<void> RestoreState(const DataChunkPtr& state_data) = 0;

private:
    // Apply a new entry to the state machine
    Result<void> ApplyInternal(const DataChunk& entry_data, bool acquire_lock = true) {
        std::optional<std::lock_guard<std::mutex>> lock;
        if (acquire_lock) {
            lock.emplace(mutex_);
        }

        // Create WAL entry
        StateMachineWalEntry entry;
        entry.data = entry_data;
        entry.set_lsn(INVALID_LSN);

        // Write to WAL first
        auto wal_result = wal_.Append(entry);
        if (!wal_result.ok()) {
            LOG_ERROR("Failed to write to WAL: %s", wal_result.error().message().c_str());
            return Result<void>::failure(wal_result.error());
        }

        // Apply the entry to update state
        auto apply_result = ApplyEntry(entry_data);
        if (!apply_result.ok()) {
            return apply_result;
        }

        entries_since_checkpoint_++;

        // Check if we need to create a checkpoint
        if (entries_since_checkpoint_ >= config_.checkpoint_interval) {
            auto checkpoint_result = CreateCheckpointInternal(!acquire_lock /* acquire_lock */);
            if (!checkpoint_result.ok()) {
                return checkpoint_result;
            }
        }

        return Result<void>::success();
    }

    // Force create a checkpoint
    Result<void> CreateCheckpointInternal(bool acquire_lock = true) {
        std::optional<std::lock_guard<std::mutex>> lock;
        if (acquire_lock) {
            lock.emplace(mutex_);
        }

        // Get current state
        auto state_result = GetCurrentState();
        if (!state_result.ok()) {
            return Result<void>::failure(state_result.error());
        }

        LSN current_lsn = wal_.current_lsn();
        std::string checkpoint_path = GetCheckpointPath(current_lsn);

        // Write checkpoint to file
        auto file_result = fs_->OpenFile(checkpoint_path, true);
        if (!file_result.ok()) {
            return Result<void>::failure(file_result.error());
        }

        // Write state data
        auto write_result = fs_->Append(file_result.value(), *(state_result.value()));
        fs_->CloseFile(file_result.value());

        if (!write_result.ok()) {
            return Result<void>::failure(write_result.error());
        }

        // Update checkpoint LSN and reset counter
        checkpoint_lsn_ = current_lsn;
        entries_since_checkpoint_ = 0;

        // Clean up old checkpoints if needed
        auto cleanup_result = CleanupOldCheckpoints();
        if (!cleanup_result.ok()) {
            LOG_WARNING("Failed to cleanup old checkpoints: %s", cleanup_result.error().message().c_str());
            // Continue despite cleanup failure
        }

        // Rotate WAL
        auto rotate_result = RotateWAL();
        if (!rotate_result.ok()) {
            return rotate_result;
        }

        return Result<void>::success();
    }

    Result<void> RecoverFromCheckpoint() {
        // List all files in the directory
        auto list_result = fs_->List(dir_path_);
        if (!list_result.ok()) {
            return Result<void>::failure(list_result.error());
        }

        // Find latest checkpoint file
        LSN latest_lsn = 0;
        std::string latest_checkpoint;

        for (const auto& file : list_result.value()) {
            // Check if file is a checkpoint file
            if (file.find("checkpoint.") == std::string::npos) {
                continue;
            }

            // Extract LSN from filename
            try {
                size_t pos = file.rfind('.');
                if (pos == std::string::npos)
                    continue;
                LSN lsn = std::stoull(file.substr(pos + 1));
                if (lsn > latest_lsn) {
                    latest_lsn = lsn;
                    latest_checkpoint = file;
                }
            } catch (...) {
                continue;  // Skip files with invalid LSN
            }
        }

        if (latest_checkpoint.empty()) {
            checkpoint_lsn_ = 0;
            return Result<void>::success();  // No checkpoint found
        }

        // Read checkpoint file
        auto file_result = fs_->OpenFile(dir_path_ + "/" + latest_checkpoint, false);
        if (!file_result.ok()) {
            return Result<void>::failure(file_result.error());
        }

        auto size_result = fs_->Size(file_result.value());
        if (!size_result.ok()) {
            fs_->CloseFile(file_result.value());
            return Result<void>::failure(size_result.error());
        }

        auto read_result = fs_->Read(file_result.value(), 0, size_result.value());
        fs_->CloseFile(file_result.value());

        if (!read_result.ok()) {
            return Result<void>::failure(read_result.error());
        }

        // Restore state from checkpoint
        auto restore_result = RestoreState(std::make_shared<DataChunk>(std::move(read_result).value()));
        if (!restore_result.ok()) {
            return restore_result;
        }

        checkpoint_lsn_ = latest_lsn;
        return Result<void>::success();
    }

    Result<void> RotateWAL() {
        auto close_result = wal_.Close();
        if (!close_result.ok()) {
            return Result<void>::failure(close_result.error());
        }

        // Delete the old WAL file since we're rotating to a new one
        auto delete_result = fs_->DeleteFiles({GetWALPath()});
        if (!delete_result.ok()) {
            return Result<void>::failure(delete_result.error());
        }

        auto open_result = wal_.Open(GetWALPath(), false);
        if (!open_result.ok()) {
            return Result<void>::failure(open_result.error());
        }

        return Result<void>::success();
    }

    std::string GetWALPath() const { return dir_path_ + "/" + WAL_FILE_NAME; }

    std::string GetCheckpointPath(LSN lsn) const {
        return dir_path_ + "/" + CHECKPOINT_FILE_PREFIX + std::to_string(lsn);
    }

    Result<void> CleanupOldCheckpoints() {
        // List all files in the directory
        auto list_result = fs_->List(dir_path_);
        if (!list_result.ok()) {
            return Result<void>::failure(list_result.error());
        }

        // Find all checkpoint files and their LSNs
        std::vector<std::pair<LSN, std::string>> checkpoints;
        for (const auto& file : list_result.value()) {
            // Check if file is a checkpoint file
            if (file.find("checkpoint.") == std::string::npos) {
                continue;
            }

            // Extract LSN from filename
            try {
                size_t pos = file.rfind('.');
                if (pos == std::string::npos)
                    continue;
                LSN lsn = std::stoull(file.substr(pos + 1));
                checkpoints.emplace_back(lsn, file);
            } catch (...) {
                continue;  // Skip files with invalid LSN
            }
        }

        // Sort checkpoints by LSN in descending order
        std::sort(
            checkpoints.begin(), checkpoints.end(), [](const auto& a, const auto& b) { return a.first > b.first; });

        // Remove old checkpoints exceeding the limit
        if (checkpoints.size() > config_.max_checkpoints) {
            std::vector<std::string> files_to_delete;
            for (size_t i = config_.max_checkpoints; i < checkpoints.size(); i++) {
                files_to_delete.push_back(dir_path_ + "/" + checkpoints[i].second);
            }

            auto delete_result = fs_->DeleteFiles(files_to_delete);
            if (!delete_result.ok()) {
                LOG_WARNING("Failed to delete old checkpoints: %s", delete_result.error().message().c_str());
            }
        }

        return Result<void>::success();
    }

    std::shared_ptr<IAppendOnlyFileSystem> fs_;
    std::string dir_path_;  // Directory path for all state machine files
    Config config_;
    WAL<StateMachineWalEntry> wal_;  // Store WAL directly
    mutable std::mutex mutex_;
    size_t entries_since_checkpoint_{0};
    LSN checkpoint_lsn_{0};
};

}  // namespace pond::common