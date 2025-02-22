#pragma once

#include <memory>
#include <string>

#include "common/data_chunk.h"
#include "common/result.h"
#include "common/stream.h"

namespace pond::rsm {

/**
 * Fixed-size cluster configuration for Raft consensus.
 * This structure is designed to be easily serializable and have a fixed size.
 */
struct ClusterConfig {
    static constexpr size_t kMaxNodes = 15;      // Maximum number of nodes in the cluster
    static constexpr size_t kMaxNodeIdLen = 64;  // Maximum length of a node ID string

    struct NodeInfo {
        char node_id[kMaxNodeIdLen]{};  // Fixed-size node ID
        bool is_voter{};                // Whether this node is a voter
        bool is_learner{};              // Whether this node is a learner
        char not_used[62]{};            // Padding to ensure 8-byte alignment
    };

    static_assert(sizeof(NodeInfo) == 128, "NodeInfo size is not as expected");

    uint32_t version{};                // Configuration version
    uint32_t node_count{};             // Number of active nodes
    NodeInfo nodes[kMaxNodes]{};       // Fixed-size array of nodes
    char cluster_id[kMaxNodeIdLen]{};  // Cluster identifier
    bool enable_single_node{};         // Whether single-node operation is allowed
    char padding[55]{};                // Padding to ensure 8-byte alignment

    ClusterConfig() : version(0), node_count(0), enable_single_node(false) {}

    // Helper to set node info
    bool SetNode(size_t index, const std::string& id, bool voter, bool learner) {
        if (index >= kMaxNodes || id.length() >= kMaxNodeIdLen) {
            return false;
        }
        std::strncpy(nodes[index].node_id, id.c_str(), kMaxNodeIdLen - 1);
        nodes[index].is_voter = voter;
        nodes[index].is_learner = learner;
        return true;
    }

    // Helper to set cluster ID
    bool SetClusterId(const std::string& id) {
        if (id.length() >= kMaxNodeIdLen) {
            return false;
        }
        std::strncpy(cluster_id, id.c_str(), kMaxNodeIdLen - 1);
        return true;
    }
};

static_assert(sizeof(ClusterConfig) == 2048, "ClusterConfig size is not as expected");

/**
 * Metadata for a snapshot, including version and state information.
 */
struct SnapshotMetadata {
    uint64_t lsn;                  // LSN at which the snapshot was taken
    uint64_t term;                 // Term number when snapshot was taken
    uint64_t version;              // Version of the snapshot format
    uint64_t not_used[5];          // Padding to ensure 8-byte alignment
    ClusterConfig cluster_config;  // Fixed-size cluster configuration
};

static_assert(sizeof(SnapshotMetadata) == sizeof(uint64_t) * 8 + sizeof(ClusterConfig),
              "SnapshotMetadata size is not as expected");

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
    virtual common::Result<SnapshotMetadata> CreateSnapshot(common::OutputStream* writer) = 0;

    /**
     * Applies a snapshot to restore state.
     * @param reader The input stream to read the snapshot from
     * @param metadata The metadata of the snapshot being restored
     * @return Result indicating success or failure
     */
    virtual common::Result<bool> ApplySnapshot(common::InputStream* reader, const SnapshotMetadata& metadata) = 0;

    /**
     * Gets the last snapshot metadata.
     * @return Result containing the last snapshot metadata or an error
     */
    virtual common::Result<SnapshotMetadata> GetLastSnapshotMetadata() const = 0;
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
    virtual common::Result<SnapshotMetadata> CreateSnapshot(ISnapshotable* state) = 0;

    /**
     * Restores state from a snapshot.
     * @param state The state machine to restore
     * @param snapshot_id Identifier of the snapshot to restore
     * @return Result indicating success or failure
     */
    virtual common::Result<bool> RestoreSnapshot(ISnapshotable* state, const std::string& snapshot_id) = 0;

    /**
     * Lists available snapshots.
     * @return Result containing list of snapshot metadata or an error
     */
    virtual common::Result<std::vector<SnapshotMetadata>> ListSnapshots() const = 0;

    /**
     * Deletes old snapshots keeping only the most recent ones.
     * @param keep_count Number of recent snapshots to keep
     * @return Result indicating success or failure
     */
    virtual common::Result<bool> PruneSnapshots(size_t keep_count) = 0;

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