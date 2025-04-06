#pragma once

#include <atomic>
#include <condition_variable>
#include <mutex>

#include "common/data_chunk.h"
#include "common/log.h"
#include "rsm/rsm.h"
#include "rsm/snapshot_manager.h"

namespace pond::test {

// Simple command to update an integer value
struct IntegerCommand {
    enum class Op { Set, Add, Subtract };

    Op op;
    int value;

    static pond::common::DataChunk Serialize(Op op, int value) {
        pond::common::DataChunk chunk;
        chunk.WriteUInt32(static_cast<uint32_t>(op));
        chunk.WriteInt32(value);
        return chunk;
    }

    static IntegerCommand Deserialize(const pond::common::DataChunk& chunk) {
        size_t offset = 0;
        IntegerCommand cmd;
        cmd.op = static_cast<Op>(chunk.ReadUInt32(offset));
        cmd.value = chunk.ReadInt32(offset);
        return cmd;
    }
};

// Simple state machine that maintains a single integer value
class IntegerStateMachine : public pond::rsm::ReplicatedStateMachine {
public:
    IntegerStateMachine(std::shared_ptr<pond::rsm::IReplication> replication,
                        std::shared_ptr<pond::rsm::ISnapshotManager> snapshot_manager)
        : ReplicatedStateMachine(std::move(replication), std::move(snapshot_manager)) {}

    int GetValue() const { return value_.load(); }

    // Add synchronization support
    void WaitForLSN(uint64_t target_lsn) {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this, target_lsn] {
            return GetLastExecutedLSN() != pond::common::INVALID_LSN && GetLastExecutedLSN() >= target_lsn;
        });

        LOG_VERBOSE("WaitForLSN: %llu, GetLastExecutedLSN: %llu", target_lsn, GetLastExecutedLSN());
    }

protected:
    void ExecuteReplicatedLog(uint64_t lsn, const pond::common::DataChunk& data) override {
        auto cmd = IntegerCommand::Deserialize(data);
        switch (cmd.op) {
            case IntegerCommand::Op::Set:
                value_.store(cmd.value);
                break;
            case IntegerCommand::Op::Add:
                value_.fetch_add(cmd.value);
                break;
            case IntegerCommand::Op::Subtract:
                value_.fetch_sub(cmd.value);
                break;
        }
    }

    void AfterExecuteReplicatedLog(uint64_t lsn) override {
        std::lock_guard<std::mutex> lock(mutex_);
        cv_.notify_all();
    }

    void SaveState(pond::common::OutputStream* writer) override { writer->Write(&value_, sizeof(value_)); }

    void LoadState(pond::common::InputStream* reader) override { reader->Read(&value_, sizeof(value_)); }

private:
    std::atomic<int> value_{0};
    std::mutex mutex_;
    std::condition_variable cv_;
};

// State machine that can block execution for testing
class BlockingStateMachine : public pond::rsm::ReplicatedStateMachine {
public:
    BlockingStateMachine(std::shared_ptr<pond::rsm::IReplication> replication,
                         std::shared_ptr<pond::rsm::ISnapshotManager> snapshot_manager)
        : ReplicatedStateMachine(std::move(replication), std::move(snapshot_manager)) {}

    int GetValue() const { return value_.load(); }

    // Block until operation is allowed to proceed
    void WaitForOperation() {
        std::unique_lock<std::mutex> lock(blocking_mutex_);
        blocking_cv_.wait(lock, [this] { return can_proceed_; });
    }

    // Allow one operation to proceed
    void AllowOperation() {
        std::lock_guard<std::mutex> lock(blocking_mutex_);
        can_proceed_ = true;
        blocking_cv_.notify_one();
    }

    // Reset the blocking state
    void Reset() {
        std::lock_guard<std::mutex> lock(blocking_mutex_);
        can_proceed_ = false;
    }

    // Wait for an operation to start executing
    void WaitForExecutionStart(uint64_t expected_lsn) {
        std::unique_lock<std::mutex> lock(blocking_mutex_);
        execution_start_cv_.wait(lock, [this, expected_lsn] { return current_executing_lsn_ == expected_lsn; });
    }

    void SaveState(pond::common::OutputStream* writer) override { writer->Write(&value_, sizeof(value_)); }

    void LoadState(pond::common::InputStream* reader) override { reader->Read(&value_, sizeof(value_)); }

    void SetNoneBlocking() {
        std::lock_guard<std::mutex> lock(blocking_mutex_);
        none_blocking_ = true;
    }

protected:
    void ExecuteReplicatedLog(uint64_t lsn, const pond::common::DataChunk& data) override {
        {
            // Notify that execution is starting
            std::lock_guard<std::mutex> lock(blocking_mutex_);
            current_executing_lsn_ = lsn;
            execution_start_cv_.notify_all();
        }

        if (!none_blocking_) {
            // Wait for test to allow execution
            WaitForOperation();
            Reset();  // Reset for next operation
        }

        auto cmd = IntegerCommand::Deserialize(data);
        value_.fetch_add(cmd.value);
    }

private:
    std::atomic<int> value_{0};
    std::mutex blocking_mutex_;
    std::condition_variable blocking_cv_;
    std::condition_variable execution_start_cv_;
    bool can_proceed_{false};
    bool none_blocking_{false};
    uint64_t current_executing_lsn_{pond::common::INVALID_LSN};
};

}  // namespace pond::test