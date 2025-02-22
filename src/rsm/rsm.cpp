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

Result<bool> ReplicatedStateMachine::Initialize(const ReplicationConfig& config) {
    auto result = replication_->Initialize(config);
    if (!result.ok()) {
        return result;
    }

    // Start background execution thread
    Start();
    return Result<bool>::success(true);
}

Result<bool> ReplicatedStateMachine::Close() {
    Stop();
    return replication_->Close();
}

Result<bool> ReplicatedStateMachine::ConfigureSnapshots(const SnapshotConfig& config) {
    std::lock_guard<std::mutex> lock(mutex_);
    snapshot_config_ = config;
    return Result<bool>::success(true);
}

Result<SnapshotMetadata> ReplicatedStateMachine::TriggerSnapshot() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!snapshot_manager_) {
        return Result<SnapshotMetadata>::failure(ErrorCode::InvalidOperation, "Snapshot manager not configured");
    }
    return snapshot_manager_->CreateSnapshot(this);
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

        // Execute entry
        ExecuteReplicatedLog(entry.lsn, entry.data);
        last_executed_lsn_.store(entry.lsn);

        // Check if we need to create a snapshot
        if (snapshot_manager_ && (entry.lsn - last_snapshot_lsn_ >= snapshot_config_.snapshot_interval)) {
            if (auto result = TriggerSnapshot(); result.ok()) {
                last_snapshot_lsn_ = entry.lsn;
            }
        }

        cv_.notify_all();
    }
}

Result<SnapshotMetadata> ReplicatedStateMachine::CreateSnapshot(common::OutputStream* writer) {
    std::lock_guard<std::mutex> lock(mutex_);

    // Create metadata for the snapshot
    SnapshotMetadata metadata;
    metadata.lsn = last_executed_lsn_.load();
    metadata.term = 0;  // TODO: Add term support when implementing Raft
    metadata.version = 1;
    metadata.cluster_config = "";  // TODO: Add cluster config when implementing Raft

    // Write state machine data
    // TODO: Implement state serialization in derived classes

    last_snapshot_metadata_ = metadata;
    return Result<SnapshotMetadata>::success(metadata);
}

Result<bool> ReplicatedStateMachine::ApplySnapshot(common::InputStream* reader, const SnapshotMetadata& metadata) {
    std::lock_guard<std::mutex> lock(mutex_);

    // Verify metadata
    if (metadata.version != 1) {
        return Result<bool>::failure(ErrorCode::InvalidArgument, "Unsupported snapshot version");
    }

    // Read and verify metadata from stream
    auto metadata_chunk_result = reader->Read(sizeof(SnapshotMetadata));
    if (!metadata_chunk_result.ok()) {
        return Result<bool>::failure(metadata_chunk_result.error());
    }

    SnapshotMetadata stored_metadata;
    std::memcpy(&stored_metadata, metadata_chunk_result.value()->Data(), sizeof(SnapshotMetadata));

    if (stored_metadata.lsn != metadata.lsn || stored_metadata.term != metadata.term
        || stored_metadata.version != metadata.version) {
        return Result<bool>::failure(ErrorCode::InvalidArgument, "Snapshot metadata mismatch");
    }

    // Apply state machine data
    // TODO: Implement state deserialization in derived classes

    // Update state machine
    last_executed_lsn_.store(metadata.lsn);
    last_snapshot_metadata_ = metadata;
    last_snapshot_lsn_ = metadata.lsn;

    return Result<bool>::success(true);
}

Result<SnapshotMetadata> ReplicatedStateMachine::GetLastSnapshotMetadata() const {
    std::unique_lock<std::mutex> lock(mutex_);
    return Result<SnapshotMetadata>::success(last_snapshot_metadata_);
}

}  // namespace pond::rsm