#pragma once

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>

#include "common/data_chunk.h"
#include "common/result.h"
#include "rsm/replication.h"
#include "rsm/snapshot.h"

namespace pond::rsm {

/**
 * Interface for a replicated state machine that combines replication, execution.
 * This provides a complete abstraction for a state machine that can:
 * 1. Replicate entries across nodes (via IReplication)
 * 2. Execute committed entries (via IReplicatedLogExecutor)
 */
class IReplicatedStateMachine {
public:
    virtual ~IReplicatedStateMachine() = default;

    // Execute a replicated log entry
    // @param lsn The log sequence number of the entry
    // @param data The data to execute
    // @return Result<void> Success if execution was successful, error otherwise
    virtual void ExecuteReplicatedLog(uint64_t lsn, const DataChunk& data) = 0;

    // Get the last executed LSN
    virtual uint64_t GetLastExecutedLSN() const = 0;

    // Get the last passed LSN
    virtual uint64_t GetLastPassedLSN() const = 0;

    // Called before executing a replicated log entry
    virtual void BeforeExecuteReplicatedLog(uint64_t lsn) {}

    // Called after executing a replicated log entry
    virtual void AfterExecuteReplicatedLog(uint64_t lsn) {}

    // Save the state of the state machine to a stream, used for snapshots
    virtual void SaveState(common::OutputStream* writer) = 0;

    // Load the state of the state machine from a stream, used for restoring from snapshots
    virtual void LoadState(common::InputStream* reader) = 0;
};

// Entry for background execution
struct PendingEntry {
    uint64_t lsn;
    DataChunk data;

    PendingEntry(uint64_t l, const DataChunk& d) : lsn(l), data(d) {}
};

/**
 * Replicated state machine that combines replication, execution, and snapshotting.
 */
class ReplicatedStateMachine : public IReplicatedStateMachine, public ISnapshotable {
public:
    ReplicatedStateMachine(std::shared_ptr<IReplication> replication,
                           std::shared_ptr<ISnapshotManager> snapshot_manager);
    ~ReplicatedStateMachine();

    Result<bool> Initialize(const ReplicationConfig& config, const SnapshotConfig& snapshot_config);
    Result<bool> Close();
    Result<void> StopAndDrain();

    // Replicate a log entry
    Result<bool> Replicate(const DataChunk& data);

    // IReplicatedStateMachine interface
    uint64_t GetLastExecutedLSN() const override;
    uint64_t GetLastPassedLSN() const override;

    /**
     * Creates a snapshot of the current state.
     * This is a public wrapper around the ISnapshotable::CreateSnapshot method.
     * @return Result containing the snapshot metadata or an error
     */
    Result<SnapshotMetadata> TriggerSnapshot();

    /**
     * Recovers state to the latest committed state.
     * This will:
     * 1. Find and restore from the latest snapshot (if any)
     * 2. Replay all logs after the snapshot to reach the latest committed state
     * @return Result indicating success or failure
     */
    Result<bool> Recover();

private:
    void Start();
    void Stop();
    void ExecuteLoop();

    // ISnapshotable implementation
    Result<SnapshotMetadata> CreateSnapshot(common::OutputStream* writer) final override;
    Result<bool> ApplySnapshot(common::InputStream* reader, const SnapshotMetadata& metadata) final override;
    Result<SnapshotMetadata> GetLastSnapshotMetadata() const final override;

    // Internal helper for snapshot restoration
    Result<bool> RestoreSnapshot(const std::string& snapshot_id);

private:
    bool initialized_{false};
    std::shared_ptr<IReplication> replication_;
    std::atomic<uint64_t> last_executed_lsn_{common::INVALID_LSN};
    std::atomic<uint64_t> last_passed_lsn_{common::INVALID_LSN};
    std::atomic<uint64_t> last_snapshot_lsn_{common::INVALID_LSN};

    // Background execution thread
    std::thread execute_thread_;
    std::atomic<bool> running_;
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::queue<PendingEntry> pending_entries_;

    // Snapshot support
    std::shared_ptr<ISnapshotManager> snapshot_manager_;
    SnapshotConfig snapshot_config_;
    SnapshotMetadata last_snapshot_metadata_;
};

}  // namespace pond::rsm
