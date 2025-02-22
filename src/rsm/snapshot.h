#pragma once

#include <memory>
#include <string>

#include "common/data_chunk.h"
#include "common/result.h"
#include "common/stream.h"

namespace pond::rsm {

/**
 * Metadata for a snapshot, including version and state information.
 */
struct SnapshotMetadata {
    uint64_t lsn;                // LSN at which the snapshot was taken
    uint64_t term;               // Term number when snapshot was taken
    uint64_t version;            // Version of the snapshot format
    std::string cluster_config;  // Cluster configuration at snapshot time
};

/**
 * Interface for objects that can be snapshotted.
 * Any state machine that needs snapshot support should implement this interface.
 */
class ISnapshotable {
public:
    virtual ~ISnapshotable() = default;

    /**
     * Creates a snapshot of the current state.
     * @param writer The output stream to write the snapshot to
     * @return Result containing the snapshot metadata or an error
     */
    virtual Result<SnapshotMetadata> CreateSnapshot(common::OutputStream* writer) = 0;

    /**
     * Applies a snapshot to restore state.
     * @param reader The input stream to read the snapshot from
     * @param metadata The metadata of the snapshot being restored
     * @return Result indicating success or failure
     */
    virtual Result<bool> ApplySnapshot(common::InputStream* reader, const SnapshotMetadata& metadata) = 0;

    /**
     * Gets the last snapshot metadata.
     * @return Result containing the last snapshot metadata or an error
     */
    virtual Result<SnapshotMetadata> GetLastSnapshotMetadata() const = 0;
};

/**
 * Manager for handling snapshots, including creation, storage, and cleanup.
 */
class ISnapshotManager {
public:
    virtual ~ISnapshotManager() = default;

    /**
     * Creates a new snapshot.
     * @param state The state machine to snapshot
     * @return Result containing the snapshot metadata or an error
     */
    virtual Result<SnapshotMetadata> CreateSnapshot(ISnapshotable* state) = 0;

    /**
     * Restores state from a snapshot.
     * @param state The state machine to restore
     * @param snapshot_id Identifier of the snapshot to restore
     * @return Result indicating success or failure
     */
    virtual Result<bool> RestoreSnapshot(ISnapshotable* state, const std::string& snapshot_id) = 0;

    /**
     * Lists available snapshots.
     * @return Result containing list of snapshot metadata or an error
     */
    virtual Result<std::vector<SnapshotMetadata>> ListSnapshots() const = 0;

    /**
     * Deletes old snapshots keeping only the most recent ones.
     * @param keep_count Number of recent snapshots to keep
     * @return Result indicating success or failure
     */
    virtual Result<bool> PruneSnapshots(size_t keep_count) = 0;

    /**
     * Gets the path where snapshots are stored.
     * @return The snapshot directory path
     */
    virtual std::string GetSnapshotPath() const = 0;
};

/**
 * Configuration for snapshot management.
 */
struct SnapshotConfig {
    size_t snapshot_interval = 10000;  // Number of entries between snapshots
    size_t keep_snapshots = 3;         // Number of snapshots to retain
    std::string snapshot_dir;          // Directory to store snapshots
    size_t chunk_size = 1024 * 1024;   // Size of chunks for streaming snapshots (1MB)

    static SnapshotConfig Default(const std::string& dir) {
        SnapshotConfig config;
        config.snapshot_dir = dir;
        return config;
    }
};

}  // namespace pond::rsm