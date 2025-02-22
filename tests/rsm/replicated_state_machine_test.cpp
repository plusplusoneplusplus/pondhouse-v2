#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>

#include <gtest/gtest.h>

#include "common/memory_append_only_fs.h"
#include "rsm/replication.h"
#include "rsm/rsm.h"
#include "rsm/snapshot_manager.h"
#include "rsm/wal_replication.h"
#include "test_helper.h"

namespace pond::test {

using namespace pond::rsm;
using namespace pond::common;

// Simple command to update an integer value
struct IntegerCommand {
    enum class Op { Set, Add, Subtract };

    Op op;
    int value;

    static DataChunk Serialize(Op op, int value) {
        DataChunk chunk;
        chunk.WriteUInt32(static_cast<uint32_t>(op));
        chunk.WriteInt32(value);
        return chunk;
    }

    static IntegerCommand Deserialize(const DataChunk& chunk) {
        size_t offset = 0;
        IntegerCommand cmd;
        cmd.op = static_cast<Op>(chunk.ReadUInt32(offset));
        cmd.value = chunk.ReadInt32(offset);
        return cmd;
    }
};

// Simple state machine that maintains a single integer value
class IntegerStateMachine : public ReplicatedStateMachine {
public:
    IntegerStateMachine(std::shared_ptr<IReplication> replication, std::shared_ptr<ISnapshotManager> snapshot_manager)
        : ReplicatedStateMachine(std::move(replication), std::move(snapshot_manager)) {}

    int GetValue() const { return value_.load(); }

    // Add synchronization support
    void WaitForLSN(uint64_t target_lsn) {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this, target_lsn] {
            return GetLastExecutedLSN() != common::INVALID_LSN && GetLastExecutedLSN() >= target_lsn;
        });

        LOG_VERBOSE("WaitForLSN: %llu, GetLastExecutedLSN: %llu", target_lsn, GetLastExecutedLSN());
    }

protected:
    void ExecuteReplicatedLog(uint64_t lsn, const DataChunk& data) override {
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

    void SaveState(common::OutputStream* writer) override { writer->Write(&value_, sizeof(value_)); }

    void LoadState(common::InputStream* reader) override { reader->Read(&value_, sizeof(value_)); }

private:
    std::atomic<int> value_{0};
    std::mutex mutex_;
    std::condition_variable cv_;
};

// State machine that can block execution for testing
class BlockingStateMachine : public ReplicatedStateMachine {
public:
    BlockingStateMachine(std::shared_ptr<IReplication> replication, std::shared_ptr<ISnapshotManager> snapshot_manager)
        : ReplicatedStateMachine(std::move(replication), std::move(snapshot_manager)) {}

    int GetValue() const { return value_.load(); }

    // Block until operation is allowed to proceed
    void WaitForOperation() {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this] { return can_proceed_; });
    }

    // Allow one operation to proceed
    void AllowOperation() {
        std::lock_guard<std::mutex> lock(mutex_);
        can_proceed_ = true;
        cv_.notify_one();
    }

    // Reset the blocking state
    void Reset() {
        std::lock_guard<std::mutex> lock(mutex_);
        can_proceed_ = false;
    }

    // Wait for an operation to start executing
    void WaitForExecutionStart(uint64_t expected_lsn) {
        std::unique_lock<std::mutex> lock(mutex_);
        execution_start_cv_.wait(lock, [this, expected_lsn] { return current_executing_lsn_ == expected_lsn; });
    }

    void SaveState(common::OutputStream* writer) override { writer->Write(&value_, sizeof(value_)); }

    void LoadState(common::InputStream* reader) override { reader->Read(&value_, sizeof(value_)); }

    void SetNoneBlocking() {
        std::lock_guard<std::mutex> lock(mutex_);
        none_blocking_ = true;
    }

protected:
    void ExecuteReplicatedLog(uint64_t lsn, const DataChunk& data) override {
        {
            // Notify that execution is starting
            std::lock_guard<std::mutex> lock(mutex_);
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
    std::mutex mutex_;
    std::condition_variable cv_;
    std::condition_variable execution_start_cv_;
    bool can_proceed_{false};
    bool none_blocking_{false};
    uint64_t current_executing_lsn_{INVALID_LSN};
};

class ReplicatedStateMachineTest : public ::testing::Test {
protected:
    void SetUp() override {
        snapshot_config_.snapshot_dir = "test_snapshots";

        fs_ = std::make_shared<MemoryAppendOnlyFileSystem>();

        snapshot_manager_ = FileSystemSnapshotManager::Create(fs_, snapshot_config_).value();
        replication_ = std::make_shared<WalReplication>(fs_);
    }

    std::shared_ptr<MemoryAppendOnlyFileSystem> fs_;
    std::shared_ptr<WalReplication> replication_;
    std::shared_ptr<ISnapshotManager> snapshot_manager_;
    SnapshotConfig snapshot_config_;
};

//
// Test Setup:
//      Create an IntegerStateMachine and test basic operations with background execution
// Test Result:
//      All operations should be executed in order and the final value should be correct
//
TEST_F(ReplicatedStateMachineTest, BasicOperations) {
    IntegerStateMachine state_machine(replication_, snapshot_manager_);

    // Initialize state machine
    ReplicationConfig config;
    config.path = "test.log";
    auto result = state_machine.Initialize(config, snapshot_config_);
    VERIFY_RESULT_MSG(result, "Should initialize state machine");

    // Test Set operation
    auto set_cmd = IntegerCommand::Serialize(IntegerCommand::Op::Set, 42);
    result = state_machine.Replicate(set_cmd);
    VERIFY_RESULT_MSG(result, "Should replicate Set command");

    // Wait for execution
    state_machine.WaitForLSN(0);
    EXPECT_EQ(42, state_machine.GetValue()) << "Value should be set to 42";
    EXPECT_EQ(0, state_machine.GetLastExecutedLSN()) << "First entry should have LSN 0";

    // Test Add operation
    auto add_cmd = IntegerCommand::Serialize(IntegerCommand::Op::Add, 8);
    result = state_machine.Replicate(add_cmd);
    VERIFY_RESULT_MSG(result, "Should replicate Add command");

    // Wait for execution
    state_machine.WaitForLSN(1);
    EXPECT_EQ(50, state_machine.GetValue()) << "Value should be incremented to 50";
    EXPECT_EQ(1, state_machine.GetLastExecutedLSN()) << "Second entry should have LSN 1";

    // Test Subtract operation
    auto sub_cmd = IntegerCommand::Serialize(IntegerCommand::Op::Subtract, 10);
    result = state_machine.Replicate(sub_cmd);
    VERIFY_RESULT_MSG(result, "Should replicate Subtract command");

    // Wait for execution
    state_machine.WaitForLSN(2);
    EXPECT_EQ(40, state_machine.GetValue()) << "Value should be decremented to 40";
    EXPECT_EQ(2, state_machine.GetLastExecutedLSN()) << "Third entry should have LSN 2";
}

//
// Test Setup:
//      Test concurrent operations on the state machine
// Test Result:
//      All operations should be executed in order despite concurrent replication
//
TEST_F(ReplicatedStateMachineTest, ConcurrentOperations) {
    IntegerStateMachine state_machine(replication_, snapshot_manager_);

    // Initialize state machine
    ReplicationConfig config;
    config.path = "concurrent.log";
    auto result = state_machine.Initialize(config, snapshot_config_);
    VERIFY_RESULT_MSG(result, "Should initialize state machine");

    // Set initial value
    auto set_cmd = IntegerCommand::Serialize(IntegerCommand::Op::Set, 0);
    result = state_machine.Replicate(set_cmd);
    VERIFY_RESULT_MSG(result, "Should replicate initial Set command");

    // Launch multiple threads to increment the value
    constexpr int kNumThreads = 4;
    constexpr int kOpsPerThread = 100;
    std::vector<std::thread> threads;

    for (int i = 0; i < kNumThreads; i++) {
        threads.emplace_back([&state_machine]() {
            for (int j = 0; j < kOpsPerThread; j++) {
                auto add_cmd = IntegerCommand::Serialize(IntegerCommand::Op::Add, 1);
                auto result = state_machine.Replicate(add_cmd);
                EXPECT_TRUE(result.ok()) << "Should replicate Add command";
            }
        });
    }

    // Wait for all threads to finish
    for (auto& thread : threads) {
        thread.join();
    }

    // Wait for all operations to be executed
    auto drain_result = state_machine.StopAndDrain();
    VERIFY_RESULT_MSG(drain_result, "Should drain all pending operations");

    // Verify final value
    EXPECT_EQ(kNumThreads * kOpsPerThread, state_machine.GetValue())
        << "Final value should match total number of increments";
    EXPECT_EQ(kNumThreads * kOpsPerThread, state_machine.GetLastExecutedLSN())
        << "Last executed LSN should match total number of operations";
}

//
// Test Setup:
//      Test StopAndDrain behavior with empty queue
// Test Result:
//      Should return success immediately when queue is empty
//
TEST_F(ReplicatedStateMachineTest, StopAndDrainEmpty) {
    IntegerStateMachine state_machine(replication_, snapshot_manager_);

    // Initialize state machine
    ReplicationConfig config;
    config.path = "empty.log";
    auto result = state_machine.Initialize(config, snapshot_config_);
    VERIFY_RESULT_MSG(result, "Should initialize state machine");

    // Drain empty queue
    auto drain_result = state_machine.StopAndDrain();
    VERIFY_RESULT_MSG(drain_result, "Should drain empty queue successfully");
    EXPECT_EQ(0, state_machine.GetValue()) << "Value should remain unchanged";
}

//
// Test Setup:
//      Test StopAndDrain behavior when already stopped
// Test Result:
//      Should return success immediately when already stopped
//
TEST_F(ReplicatedStateMachineTest, StopAndDrainAlreadyStopped) {
    IntegerStateMachine state_machine(replication_, snapshot_manager_);

    // Initialize and immediately close
    ReplicationConfig config;
    config.path = "stopped.log";
    auto result = state_machine.Initialize(config, snapshot_config_);
    VERIFY_RESULT_MSG(result, "Should initialize state machine");

    result = state_machine.Close();
    VERIFY_RESULT_MSG(result, "Should close state machine");

    // Try to drain after stopping
    auto drain_result = state_machine.StopAndDrain();
    VERIFY_RESULT_MSG(drain_result, "Should handle StopAndDrain when already stopped");
}

//
// Test Setup:
//      Test that last_passed_lsn is correctly updated when operations are blocked
// Test Result:
//      last_passed_lsn should reflect the LSN of operations that have started execution
//
TEST_F(ReplicatedStateMachineTest, LastPassedLSN) {
    BlockingStateMachine state_machine(replication_, snapshot_manager_);

    // Initialize state machine
    ReplicationConfig config;
    config.path = "blocking.log";
    auto result = state_machine.Initialize(config, snapshot_config_);
    VERIFY_RESULT_MSG(result, "Should initialize state machine");

    // Queue up first operation
    auto add_cmd = IntegerCommand::Serialize(IntegerCommand::Op::Add, 1);
    result = state_machine.Replicate(add_cmd);
    VERIFY_RESULT_MSG(result, "Should replicate first Add command");

    // Wait for first operation to start executing
    state_machine.WaitForExecutionStart(0);
    EXPECT_EQ(common::INVALID_LSN, state_machine.GetLastExecutedLSN()) << "No operations should be completed";
    EXPECT_EQ(0, state_machine.GetLastPassedLSN()) << "First operation should be passed to execution";

    // Queue up second operation
    result = state_machine.Replicate(add_cmd);
    VERIFY_RESULT_MSG(result, "Should replicate second Add command");

    EXPECT_EQ(1, state_machine.GetLastPassedLSN()) << "Second operation should be passed to execution";

    // Allow first operation to complete
    state_machine.AllowOperation();

    // Wait for second operation to start executing
    state_machine.WaitForExecutionStart(1);
    EXPECT_EQ(0, state_machine.GetLastExecutedLSN()) << "First operation should be completed";
    EXPECT_EQ(1, state_machine.GetLastPassedLSN()) << "Second operation should be passed to execution";
    EXPECT_EQ(1, state_machine.GetValue()) << "First operation should update value";

    // Allow second operation to complete
    state_machine.AllowOperation();

    // Drain and verify final state
    auto drain_result = state_machine.StopAndDrain();
    VERIFY_RESULT_MSG(drain_result, "Should drain all operations");
    EXPECT_EQ(1, state_machine.GetLastExecutedLSN()) << "Both operations should be completed";
    EXPECT_EQ(1, state_machine.GetLastPassedLSN()) << "Both operations should be passed to execution";
    EXPECT_EQ(2, state_machine.GetValue()) << "Both operations should update value";
}

//
// Test Setup:
//      Test basic snapshot creation and restoration
// Test Result:
//      State should be correctly saved and restored from snapshot
//
TEST_F(ReplicatedStateMachineTest, BasicSnapshot) {
    // Initialize state machine
    ReplicationConfig config;
    config.path = "snapshot_test.log";

    {
        IntegerStateMachine state_machine(replication_, snapshot_manager_);

        auto result = state_machine.Initialize(config, snapshot_config_);
        VERIFY_RESULT_MSG(result, "Should initialize state machine");

        // Set initial value and create snapshot
        auto set_cmd = IntegerCommand::Serialize(IntegerCommand::Op::Set, 42);
        result = state_machine.Replicate(set_cmd);
        VERIFY_RESULT_MSG(result, "Should replicate Set command");
        state_machine.WaitForLSN(0);

        // Create snapshot
        auto snapshot_result = state_machine.TriggerSnapshot();
        VERIFY_RESULT_MSG(snapshot_result, "Should create snapshot");
        auto snapshot_metadata = snapshot_result.value();
        EXPECT_EQ(0, snapshot_metadata.lsn) << "Snapshot LSN should match last executed LSN";
    }

    {
        // Create new state machine and recover
        IntegerStateMachine new_state_machine(replication_, snapshot_manager_);
        auto result = new_state_machine.Initialize(config, snapshot_config_);
        VERIFY_RESULT_MSG(result, "Should initialize new state machine");
        result = new_state_machine.Recover();
        VERIFY_RESULT_MSG(result, "Should recover from snapshot");
        EXPECT_EQ(42, new_state_machine.GetValue()) << "Restored value should match original";
    }
}

//
// Test Setup:
//      Test snapshot with concurrent operations
// Test Result:
//      Snapshot should capture consistent state despite concurrent operations
//
TEST_F(ReplicatedStateMachineTest, ConcurrentSnapshot) {
    ReplicationConfig config;
    config.path = "concurrent_snapshot.log";

    {
        // Initialize state machine
        BlockingStateMachine state_machine(replication_, snapshot_manager_);

        auto result = state_machine.Initialize(config, snapshot_config_);
        VERIFY_RESULT_MSG(result, "Should initialize state machine");

        // Set initial value
        auto set_cmd = IntegerCommand::Serialize(IntegerCommand::Op::Set, 100);
        result = state_machine.Replicate(set_cmd);
        VERIFY_RESULT_MSG(result, "Should replicate Set command");
        state_machine.AllowOperation();
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        // Start a thread that continuously updates the value
        std::atomic<bool> stop_thread{false};
        std::thread update_thread([&]() {
            while (!stop_thread) {
                auto add_cmd = IntegerCommand::Serialize(IntegerCommand::Op::Add, 1);
                auto result = state_machine.Replicate(add_cmd);
                EXPECT_TRUE(result.ok()) << "Should replicate Add command";
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
                state_machine.AllowOperation();  // Allow each operation to complete
            }
        });

        // Create snapshot while updates are happening
        auto snapshot_result = state_machine.TriggerSnapshot();
        VERIFY_RESULT_MSG(snapshot_result, "Should create snapshot");

        // Stop update thread
        stop_thread = true;
        update_thread.join();
    }

    {
        // Create new state machine and recover
        BlockingStateMachine new_state_machine(replication_, snapshot_manager_);
        auto result = new_state_machine.Initialize(config, snapshot_config_);
        VERIFY_RESULT_MSG(result, "Should initialize new state machine");

        new_state_machine.SetNoneBlocking();

        result = new_state_machine.Recover();
        VERIFY_RESULT_MSG(result, "Should recover state");
        EXPECT_GE(new_state_machine.GetValue(), 100) << "Recovered value should be at least initial value";
    }
}

//
// Test Setup:
//      Test recovery failure cases
// Test Result:
//      Should handle various error conditions gracefully
//
TEST_F(ReplicatedStateMachineTest, RecoveryFailures) {
    IntegerStateMachine state_machine(replication_, snapshot_manager_);

    // Initialize state machine
    ReplicationConfig config;
    config.path = "failure_test.log";
    auto result = state_machine.Initialize(config, snapshot_config_);
    VERIFY_RESULT_MSG(result, "Should initialize state machine");

    // Test recovery with no snapshots or logs
    result = state_machine.Recover();
    VERIFY_RESULT_MSG(result, "Should handle recovery with no state");
    EXPECT_EQ(0, state_machine.GetValue()) << "Value should remain at initial state";

    // Test recovery with uninitialized state machine
    IntegerStateMachine uninitialized_machine(replication_, snapshot_manager_);
    result = uninitialized_machine.Recover();
    EXPECT_FALSE(result.ok()) << "Should fail to recover uninitialized state machine";
}

//
// Test Setup:
//      Test recovery with multiple snapshots and logs
// Test Result:
//      Should recover to the latest state using the most recent snapshot and logs
//
TEST_F(ReplicatedStateMachineTest, CompleteRecovery) {
    IntegerStateMachine state_machine(replication_, snapshot_manager_);

    // Initialize state machine
    ReplicationConfig config;
    config.path = "complete_recovery.log";
    auto result = state_machine.Initialize(config, snapshot_config_);
    VERIFY_RESULT_MSG(result, "Should initialize state machine");

    // Create initial state
    auto set_cmd = IntegerCommand::Serialize(IntegerCommand::Op::Set, 100);
    result = state_machine.Replicate(set_cmd);
    VERIFY_RESULT_MSG(result, "Should replicate Set command");
    state_machine.WaitForLSN(0);

    // Create first snapshot
    auto snapshot_result = state_machine.TriggerSnapshot();
    VERIFY_RESULT_MSG(snapshot_result, "Should create first snapshot");

    // Add more operations and create another snapshot
    for (int i = 1; i <= 5; i++) {
        auto add_cmd = IntegerCommand::Serialize(IntegerCommand::Op::Add, 10);
        result = state_machine.Replicate(add_cmd);
        VERIFY_RESULT_MSG(result, "Should replicate Add command");
        state_machine.WaitForLSN(i);
    }

    snapshot_result = state_machine.TriggerSnapshot();
    VERIFY_RESULT_MSG(snapshot_result, "Should create second snapshot");

    // Add final operations
    for (int i = 6; i <= 8; i++) {
        auto add_cmd = IntegerCommand::Serialize(IntegerCommand::Op::Add, 5);
        result = state_machine.Replicate(add_cmd);
        VERIFY_RESULT_MSG(result, "Should replicate final Add commands");
        state_machine.WaitForLSN(i);
    }

    // Create new state machine and recover
    IntegerStateMachine recovery_machine(replication_, snapshot_manager_);
    result = recovery_machine.Initialize(config, snapshot_config_);
    VERIFY_RESULT_MSG(result, "Should initialize recovery machine");

    result = recovery_machine.Recover();
    VERIFY_RESULT_MSG(result, "Should recover state");
    EXPECT_EQ(165, recovery_machine.GetValue()) << "Should recover to final state (100 + 5*10 + 3*5)";
    EXPECT_EQ(8, recovery_machine.GetLastExecutedLSN()) << "Should execute all log entries";
}

}  // namespace pond::test