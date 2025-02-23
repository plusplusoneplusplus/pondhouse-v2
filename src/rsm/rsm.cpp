#include "rsm/rsm.h"

#include <mutex>
#include <thread>

#include "common/error.h"
#include "common/log.h"
#include "common/memory_stream.h"
#include "rsm/snapshot.h"
#include "rsm/snapshot_manager.h"

using namespace pond::common;

namespace pond::rsm {

ReplicatedStateMachine::ReplicatedStateMachine(std::shared_ptr<IReplication> replication,
                                               std::shared_ptr<ISnapshotManager> snapshot_manager)
    : replication_(std::move(replication)), snapshot_manager_(std::move(snapshot_manager)), running_(false) {}

ReplicatedStateMachine::~ReplicatedStateMachine() {
    Close();
}

Result<bool> ReplicatedStateMachine::Initialize(const ReplicationConfig& config,
                                                const SnapshotConfig& snapshot_config) {
    auto result = replication_->Initialize(config);
    if (!result.ok()) {
        return result;
    }

    result = snapshot_manager_->Initialize(snapshot_config);
    if (!result.ok()) {
        return result;
    }

    {
        auto lock = std::lock_guard(mutex_);
        snapshot_config_ = snapshot_config;
    }

    // Start background execution thread
    Start();

    initialized_ = true;
    return Result<bool>::success(true);
}

Result<bool> ReplicatedStateMachine::Close() {
    if (!initialized_) {
        return Result<bool>::failure(ErrorCode::InvalidOperation, "State machine not initialized");
    }

    Stop();
    return replication_->Close();
}

Result<SnapshotMetadata> ReplicatedStateMachine::TriggerSnapshot() {
    if (!initialized_) {
        return Result<SnapshotMetadata>::failure(ErrorCode::InvalidOperation, "State machine not initialized");
    }

    std::lock_guard<std::mutex> lock(mutex_);

    if (!snapshot_manager_) {
        return Result<SnapshotMetadata>::failure(ErrorCode::InvalidOperation, "Snapshot manager not configured");
    }
    return snapshot_manager_->CreateSnapshot(this);
}

Result<bool> ReplicatedStateMachine::RestoreSnapshot(const std::string& snapshot_id) {
    if (!initialized_) {
        return Result<bool>::failure(ErrorCode::InvalidOperation, "State machine not initialized");
    }

    if (!snapshot_manager_) {
        return Result<bool>::failure(ErrorCode::InvalidOperation, "Snapshot manager not configured");
    }
    return snapshot_manager_->RestoreSnapshot(this, snapshot_id);
}

Result<bool> ReplicatedStateMachine::Recover() {
    using ResultType = Result<bool>;

    if (!initialized_) {
        return ResultType::failure(ErrorCode::InvalidOperation, "State machine not initialized");
    }

    std::lock_guard<std::mutex> lock(mutex_);
    if (!snapshot_manager_ || !replication_) {
        return ResultType::failure(ErrorCode::InvalidOperation, "Snapshot manager or replication not configured");
    }

    // Restore from latest snapshot if found
    uint64_t start_lsn = 0;

    // List available snapshots
    auto snapshots_result = snapshot_manager_->ListSnapshots();
    if (!snapshots_result.ok()) {
        if (snapshots_result.error().code() == ErrorCode::DirectoryNotFound) {
            LOG_STATUS("No snapshots found, recovering from empty state");
            // continue on log replay
        } else {
            return ResultType::failure(snapshots_result.error());
        }
    } else {
        // Find the latest snapshot
        const SnapshotMetadata* latest_snapshot = nullptr;
        for (const auto& snapshot : snapshots_result.value()) {
            if (!latest_snapshot || snapshot.lsn > latest_snapshot->lsn) {
                latest_snapshot = &snapshot;
            }
        }
        if (latest_snapshot) {
            auto restore_result = RestoreSnapshot(std::to_string(latest_snapshot->lsn));
            RETURN_IF_ERROR_T(ResultType, restore_result);

            last_executed_lsn_.store(latest_snapshot->lsn);
            start_lsn = latest_snapshot->lsn + 1;
        }
    }

    // Replay all logs after the snapshot
    auto read_result = replication_->Read(start_lsn);
    RETURN_IF_ERROR_T(ResultType, read_result);

    for (const auto& log : read_result.value()) {
        // Execute the log entry
        ExecuteReplicatedLog(start_lsn, log);
        last_executed_lsn_.store(start_lsn);
        start_lsn++;
    }

    return Result<bool>::success(true);
}

Result<void> ReplicatedStateMachine::StopAndDrain() {
    if (!running_) {
        return Result<void>::success();
    }

    // Wait for queue to be empty
    {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this] { return pending_entries_.empty(); });
    }

    // Now stop the thread
    Stop();
    return Result<void>::success();
}

Result<bool> ReplicatedStateMachine::Replicate(const DataChunk& data) {
    if (!initialized_) {
        return Result<bool>::failure(ErrorCode::InvalidOperation, "State machine not initialized");
    }

    auto result = replication_->Append(data);
    if (!result.ok()) {
        LOG_ERROR("Failed to replicate data: %s", result.error().message().c_str());
        return Result<bool>::failure(result.error());
    }

    // Push to execution queue
    {
        auto lsn = result.value();
        std::lock_guard<std::mutex> lock(mutex_);
        pending_entries_.emplace(lsn, data);
        last_passed_lsn_.store(lsn);
    }
    cv_.notify_one();

    return Result<bool>::success(true);
}

uint64_t ReplicatedStateMachine::GetLastExecutedLSN() const {
    return last_executed_lsn_.load();
}

uint64_t ReplicatedStateMachine::GetLastPassedLSN() const {
    return last_passed_lsn_.load();
}

void ReplicatedStateMachine::Start() {
    running_ = true;
    execute_thread_ = std::thread(&ReplicatedStateMachine::ExecuteLoop, this);
}

void ReplicatedStateMachine::Stop() {
    if (running_) {
        running_ = false;
        cv_.notify_all();
        if (execute_thread_.joinable()) {
            execute_thread_.join();
        }
    }
}

void ReplicatedStateMachine::ExecuteLoop() {
    while (running_) {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this] { return !running_ || !pending_entries_.empty(); });

        if (!running_) {
            break;
        }

        // Get next entry to execute
        auto entry = std::move(pending_entries_.front());
        pending_entries_.pop();
        lock.unlock();

        BeforeExecuteReplicatedLog(entry.lsn);

        // Execute entry
        ExecuteReplicatedLog(entry.lsn, entry.data);
        last_executed_lsn_.store(entry.lsn);

        // Check if we need to create a snapshot
        if (snapshot_manager_ && (entry.lsn - last_snapshot_lsn_ >= snapshot_config_.snapshot_interval)) {
            if (auto result = TriggerSnapshot(); result.ok()) {
                last_snapshot_lsn_ = entry.lsn;
            }
        }

        AfterExecuteReplicatedLog(entry.lsn);

        cv_.notify_all();
    }
}

Result<SnapshotMetadata> ReplicatedStateMachine::CreateSnapshot(common::OutputStream* writer) {
    if (!initialized_) {
        return Result<SnapshotMetadata>::failure(ErrorCode::InvalidOperation, "State machine not initialized");
    }

    // Create metadata for the snapshot
    SnapshotMetadata metadata;
    metadata.lsn = last_executed_lsn_.load();
    metadata.term = 0;  // TODO: Add term support when implementing Raft
    metadata.version = 1;
    metadata.cluster_config = ClusterConfig();  // TODO: Add cluster config when implementing Raft

    // Write state machine data
    SaveState(writer);

    last_snapshot_metadata_ = metadata;
    return Result<SnapshotMetadata>::success(metadata);
}

Result<bool> ReplicatedStateMachine::ApplySnapshot(common::InputStream* reader, const SnapshotMetadata& metadata) {
    if (!initialized_) {
        return Result<bool>::failure(ErrorCode::InvalidOperation, "State machine not initialized");
    }

    // Verify metadata
    if (metadata.version != 1) {
        return Result<bool>::failure(ErrorCode::InvalidArgument, "Unsupported snapshot version");
    }

    // Apply state machine data
    LoadState(reader);

    // Update state machine
    last_executed_lsn_.store(metadata.lsn);
    last_snapshot_metadata_ = metadata;
    last_snapshot_lsn_ = metadata.lsn;

    return Result<bool>::success(true);
}

Result<SnapshotMetadata> ReplicatedStateMachine::GetLastSnapshotMetadata() const {
    if (!initialized_) {
        return Result<SnapshotMetadata>::failure(ErrorCode::InvalidOperation, "State machine not initialized");
    }

    std::unique_lock<std::mutex> lock(mutex_);
    return Result<SnapshotMetadata>::success(last_snapshot_metadata_);
}

}  // namespace pond::rsm