#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>

#include <gtest/gtest.h>

#include "common/memory_append_only_fs.h"
#include "integer_state_machine.h"
#include "rsm/replication.h"
#include "rsm/rsm.h"
#include "rsm/snapshot_manager.h"
#include "rsm/wal_replication.h"
#include "test_helper.h"

namespace pond::test {

using namespace pond::rsm;
using namespace pond::common;

// IntegerCommand and IntegerStateMachine are now included from integer_state_machine.h

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
    config.directory = "test";
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
//      Test replication with callback functionality
// Test Result:
//      Callback should be executed after the operation is committed and executed
//
TEST_F(ReplicatedStateMachineTest, ReplicateWithCallback) {
    IntegerStateMachine state_machine(replication_, snapshot_manager_);

    // Initialize state machine
    ReplicationConfig config;
    config.directory = "callback_test";
    auto result = state_machine.Initialize(config, snapshot_config_);
    VERIFY_RESULT_MSG(result, "Should initialize state machine");

    // Test Set operation with callback
    std::mutex mtx;
    std::condition_variable cv;
    bool set_completed = false;
    bool set_callback_executed = false;

    auto set_cmd = IntegerCommand::Serialize(IntegerCommand::Op::Set, 100);
    result = state_machine.Replicate(set_cmd, [&]() {
        std::unique_lock<std::mutex> lock(mtx);
        set_callback_executed = true;
        set_completed = true;
        cv.notify_one();
    });
    VERIFY_RESULT_MSG(result, "Should replicate Set command with callback");

    // Wait for callback to complete
    {
        std::unique_lock<std::mutex> lock(mtx);
        cv.wait(lock, [&] { return set_completed; });
    }
    EXPECT_EQ(100, state_machine.GetValue()) << "Value should be set to 100";
    EXPECT_TRUE(set_callback_executed) << "Callback should have been executed";
    EXPECT_EQ(0, state_machine.GetLastExecutedLSN()) << "First entry should have LSN 0";
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
    config.directory = "concurrent";
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
    config.directory = "empty";
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
    config.directory = "stopped";
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
    config.directory = "blocking";
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
    config.directory = "snapshot_test";

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
    config.directory = "concurrent_snapshot";

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

        auto drain_result = state_machine.StopAndDrain();
        VERIFY_RESULT_MSG(drain_result, "Should drain all operations");
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
    config.directory = "failure_test";
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
    config.directory = "complete_recovery";
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