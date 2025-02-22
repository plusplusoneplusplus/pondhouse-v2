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

namespace pond::rsm {

/**
 * Interface for a replicated state machine that combines both replication and execution
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
};

// Entry for background execution
struct PendingEntry {
    uint64_t lsn;
    DataChunk data;

    PendingEntry(uint64_t l, const DataChunk& d) : lsn(l), data(d) {}
};

class ReplicatedStateMachine : public IReplicatedStateMachine {
public:
    ReplicatedStateMachine(std::shared_ptr<IReplication> replication)
        : replication_(std::move(replication)), running_(false) {}

    ~ReplicatedStateMachine() { Close(); }

    Result<bool> Initialize(const ReplicationConfig& config) {
        auto result = replication_->Initialize(config);
        if (!result.ok()) {
            return result;
        }

        // Start background execution thread
        Start();
        return Result<bool>::success(true);
    }

    Result<bool> Close() {
        Stop();
        return replication_->Close();
    }

    // Stop the execution thread after draining all pending entries
    Result<void> StopAndDrain() {
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

    Result<bool> Replicate(const DataChunk& data) {
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

    uint64_t GetLastExecutedLSN() const override { return last_executed_lsn_.load(); }

    uint64_t GetLastPassedLSN() const override { return last_passed_lsn_.load(); }

private:
    void Start() {
        running_ = true;
        execute_thread_ = std::thread(&ReplicatedStateMachine::ExecuteLoop, this);
    }

    void Stop() {
        if (running_) {
            running_ = false;
            cv_.notify_all();
            if (execute_thread_.joinable()) {
                execute_thread_.join();
            }
        }
    }

    void ExecuteLoop() {
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
            cv_.notify_all();
        }
    }

private:
    std::shared_ptr<IReplication> replication_;
    std::atomic<uint64_t> last_executed_lsn_{0};
    std::atomic<uint64_t> last_passed_lsn_{0};

    // Background execution thread
    std::thread execute_thread_;
    std::atomic<bool> running_;
    std::mutex mutex_;
    std::condition_variable cv_;
    std::queue<PendingEntry> pending_entries_;
};

}  // namespace pond::rsm
