#pragma once

#include <memory>
#include <mutex>
#include <string>

#include "common/append_only_fs.h"
#include "rsm/snapshot.h"

namespace pond::rsm {

/**
 * Filesystem-based implementation of ISnapshotManager.
 * Stores snapshots as files in a configured directory with metadata.
 */
class FileSystemSnapshotManager : public ISnapshotManager {
public:
    /**
     * Creates a new FileSystemSnapshotManager.
     * @param fs The filesystem to use for storage
     * @param config Configuration for snapshot management
     * @return Result containing the manager or an error
     */
    static common::Result<std::shared_ptr<ISnapshotManager>> Create(std::shared_ptr<common::IAppendOnlyFileSystem> fs,
                                                                    const SnapshotConfig& config);

    ~FileSystemSnapshotManager() override = default;

    /**
     * Creates a new snapshot.
     * @param state The state machine to snapshot
     * @return Result containing the snapshot metadata or an error
     */
    common::Result<SnapshotMetadata> CreateSnapshot(ISnapshotable* state) override;

    /**
     * Restores state from a snapshot.
     * @param state The state machine to restore
     * @param snapshot_id Identifier of the snapshot to restore
     * @return Result indicating success or failure
     */
    common::Result<bool> RestoreSnapshot(ISnapshotable* state, const std::string& snapshot_id) override;

    /**
     * Lists available snapshots.
     * @return Result containing list of snapshot metadata or an error
     */
    common::Result<std::vector<SnapshotMetadata>> ListSnapshots() const override;

    /**
     * Deletes old snapshots keeping only the most recent ones.
     * @param keep_count Number of recent snapshots to keep
     * @return Result indicating success or failure
     */
    common::Result<bool> PruneSnapshots(size_t keep_count) override;

    /**
     * Gets the path where snapshots are stored.
     * @return The snapshot directory path
     */
    std::string GetSnapshotPath() const override { return config_.snapshot_dir; }

private:
    FileSystemSnapshotManager(std::shared_ptr<common::IAppendOnlyFileSystem> fs, const SnapshotConfig& config);

    // Helper methods
    std::string GetSnapshotFilePath(const std::string& snapshot_id) const;
    std::string GetMetadataFilePath(const std::string& snapshot_id) const;
    common::Result<bool> WriteMetadata(const std::string& snapshot_id, const SnapshotMetadata& metadata);
    common::Result<SnapshotMetadata> ReadMetadata(const std::string& snapshot_id) const;
    std::string GenerateSnapshotId(const SnapshotMetadata& metadata) const;

    std::shared_ptr<common::IAppendOnlyFileSystem> fs_;
    SnapshotConfig config_;
    mutable std::recursive_mutex mutex_;  // Protects concurrent operations
};

}  // namespace pond::rsm