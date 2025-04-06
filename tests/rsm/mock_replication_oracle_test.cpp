#include "rsm/mock_replication_oracle.h"

#include <gtest/gtest.h>

#include "common/memory_append_only_fs.h"
#include "integer_state_machine.h"
#include "rsm/snapshot_manager.h"
#include "rsm/wal_replication.h"
#include "test_helper.h"

namespace pond::test {

using namespace pond::rsm;
using namespace pond::common;

// Use IntegerStateMachine from integer_state_machine.h

class MockReplicationOracleTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Initialize snapshot configurations
        primary_snapshot_config_.snapshot_dir = "primary/snapshots";
        secondary1_snapshot_config_.snapshot_dir = "secondary1/snapshots";
        secondary2_snapshot_config_.snapshot_dir = "secondary2/snapshots";

        // Create file systems for primary and secondaries
        primary_fs_ = std::make_shared<MemoryAppendOnlyFileSystem>();
        secondary1_fs_ = std::make_shared<MemoryAppendOnlyFileSystem>();
        secondary2_fs_ = std::make_shared<MemoryAppendOnlyFileSystem>();

        // Create snapshot managers
        primary_snapshot_manager_ = FileSystemSnapshotManager::Create(primary_fs_, primary_snapshot_config_).value();
        secondary1_snapshot_manager_ =
            FileSystemSnapshotManager::Create(secondary1_fs_, secondary1_snapshot_config_).value();
        secondary2_snapshot_manager_ =
            FileSystemSnapshotManager::Create(secondary2_fs_, secondary2_snapshot_config_).value();

        // Create replication instances
        primary_replication_ = std::make_shared<WalReplication>(primary_fs_);
        secondary1_replication_ = std::make_shared<WalReplication>(secondary1_fs_);
        secondary2_replication_ = std::make_shared<WalReplication>(secondary2_fs_);

        // Create state machines
        primary_sm_ = std::make_shared<IntegerStateMachine>(primary_replication_, primary_snapshot_manager_);
        secondary1_sm_ = std::make_shared<IntegerStateMachine>(secondary1_replication_, secondary1_snapshot_manager_);
        secondary2_sm_ = std::make_shared<IntegerStateMachine>(secondary2_replication_, secondary2_snapshot_manager_);

        // Initialize state machines
        primary_sm_->Initialize(primary_config_, primary_snapshot_config_);
        secondary1_sm_->Initialize(secondary1_config_, secondary1_snapshot_config_);
        secondary2_sm_->Initialize(secondary2_config_, secondary2_snapshot_config_);

        // Create and initialize oracle
        oracle_ = std::make_shared<MockReplicationOracle>();
        oracle_->Initialize();
    }

    void TearDown() override {
        // Close state machines
        primary_sm_->Close();
        secondary1_sm_->Close();
        secondary2_sm_->Close();
    }

    // Primary resources
    std::shared_ptr<MemoryAppendOnlyFileSystem> primary_fs_;
    std::shared_ptr<WalReplication> primary_replication_;
    std::shared_ptr<ISnapshotManager> primary_snapshot_manager_;
    std::shared_ptr<IntegerStateMachine> primary_sm_;
    ReplicationConfig primary_config_{"primary"};
    SnapshotConfig primary_snapshot_config_;

    // Secondary 1 resources
    std::shared_ptr<MemoryAppendOnlyFileSystem> secondary1_fs_;
    std::shared_ptr<WalReplication> secondary1_replication_;
    std::shared_ptr<ISnapshotManager> secondary1_snapshot_manager_;
    std::shared_ptr<IntegerStateMachine> secondary1_sm_;
    ReplicationConfig secondary1_config_{"secondary1"};
    SnapshotConfig secondary1_snapshot_config_;

    // Secondary 2 resources
    std::shared_ptr<MemoryAppendOnlyFileSystem> secondary2_fs_;
    std::shared_ptr<WalReplication> secondary2_replication_;
    std::shared_ptr<ISnapshotManager> secondary2_snapshot_manager_;
    std::shared_ptr<IntegerStateMachine> secondary2_sm_;
    ReplicationConfig secondary2_config_{"secondary2"};
    SnapshotConfig secondary2_snapshot_config_;

    // Oracle
    std::shared_ptr<MockReplicationOracle> oracle_;
};

//
// Test Setup:
//      Test basic primary-secondary replication with Oracle
// Test Result:
//      All state machines should have the same state after replication
//
TEST_F(MockReplicationOracleTest, BasicReplication) {
    // Register primary and secondaries
    auto result = oracle_->RegisterPrimary(primary_sm_);
    VERIFY_RESULT_MSG(result, "Should register primary");

    result = oracle_->RegisterSecondary(secondary1_sm_);
    VERIFY_RESULT_MSG(result, "Should register secondary1");

    result = oracle_->RegisterSecondary(secondary2_sm_);
    VERIFY_RESULT_MSG(result, "Should register secondary2");

    EXPECT_EQ(2, oracle_->GetSecondaryCount()) << "Oracle should have two secondaries";

    // Replicate a SET command to set value to 42
    auto set_cmd = IntegerCommand::Serialize(IntegerCommand::Op::Set, 42);

    std::mutex mutex;
    std::condition_variable cv;
    bool completed = false;

    // Call Replicate directly on primary instead of going through the oracle
    auto replicate_result = primary_sm_->Replicate(set_cmd, [&mutex, &cv, &completed]() {
        std::lock_guard<std::mutex> lock(mutex);
        completed = true;
        cv.notify_one();
    });

    VERIFY_RESULT_MSG(replicate_result, "Should replicate SET command");

    // Wait for operation to complete across all state machines
    {
        std::unique_lock<std::mutex> lock(mutex);
        cv.wait_for(lock, std::chrono::seconds(1), [&]() { return completed; });
    }

    // Verify all state machines have the same value
    EXPECT_EQ(42, primary_sm_->GetValue()) << "Primary should have value 42";
    EXPECT_EQ(42, secondary1_sm_->GetValue()) << "Secondary1 should have value 42";
    EXPECT_EQ(42, secondary2_sm_->GetValue()) << "Secondary2 should have value 42";

    // Replicate ADD command to add 10
    auto add_cmd = IntegerCommand::Serialize(IntegerCommand::Op::Add, 10);
    completed = false;

    // Call Replicate directly on primary
    replicate_result = primary_sm_->Replicate(add_cmd, [&]() {
        std::lock_guard<std::mutex> lock(mutex);
        completed = true;
        cv.notify_one();
    });

    VERIFY_RESULT_MSG(replicate_result, "Should replicate ADD command");

    // Wait for operation to complete
    {
        std::unique_lock<std::mutex> lock(mutex);
        cv.wait_for(lock, std::chrono::seconds(1), [&]() { return completed; });
    }

    // Verify all state machines have updated
    EXPECT_EQ(52, primary_sm_->GetValue()) << "Primary should have value 52";
    EXPECT_EQ(52, secondary1_sm_->GetValue()) << "Secondary1 should have value 52";
    EXPECT_EQ(52, secondary2_sm_->GetValue()) << "Secondary2 should have value 52";

    // Unregister a secondary
    result = oracle_->UnregisterStateMachine(secondary2_sm_);
    VERIFY_RESULT_MSG(result, "Should unregister secondary2");
    EXPECT_EQ(1, oracle_->GetSecondaryCount()) << "Oracle should have one secondary";

    // Replicate another command
    auto subtract_cmd = IntegerCommand::Serialize(IntegerCommand::Op::Subtract, 2);
    completed = false;

    // Call Replicate directly on primary
    replicate_result = primary_sm_->Replicate(subtract_cmd, [&]() {
        std::lock_guard<std::mutex> lock(mutex);
        completed = true;
        cv.notify_one();
    });

    VERIFY_RESULT_MSG(replicate_result, "Should replicate SUBTRACT command");

    // Wait for operation to complete
    {
        std::unique_lock<std::mutex> lock(mutex);
        cv.wait_for(lock, std::chrono::seconds(1), [&]() { return completed; });
    }

    // Verify unregistered secondary doesn't get the update
    EXPECT_EQ(50, primary_sm_->GetValue()) << "Primary should have value 50";
    EXPECT_EQ(50, secondary1_sm_->GetValue()) << "Secondary1 should have value 50";
    EXPECT_EQ(52, secondary2_sm_->GetValue()) << "Secondary2 should still have value 52";
}

//
// Test Setup:
//      Test error handling when operations fail
// Test Result:
//      Oracle should handle failures gracefully
//
TEST_F(MockReplicationOracleTest, ErrorHandling) {
    // 1. Test uninitialized oracle
    MockReplicationOracle uninitialized_oracle;
    auto result = uninitialized_oracle.RegisterPrimary(primary_sm_);
    EXPECT_FALSE(result.ok()) << "Should fail to register primary with uninitialized oracle";

    // 2. Test registering same state machine as both primary and secondary
    result = oracle_->RegisterPrimary(primary_sm_);
    VERIFY_RESULT_MSG(result, "Should register primary");

    result = oracle_->RegisterSecondary(primary_sm_);
    EXPECT_FALSE(result.ok()) << "Should fail to register primary as secondary";

    // 3. Test registering secondary as primary
    result = oracle_->RegisterSecondary(secondary1_sm_);
    VERIFY_RESULT_MSG(result, "Should register secondary");

    result = oracle_->RegisterPrimary(secondary1_sm_);
    EXPECT_FALSE(result.ok()) << "Should fail to register secondary as primary";

    // 4. Test double primary registration
    SnapshotConfig primary2_snapshot_config;
    primary2_snapshot_config.snapshot_dir = "primary2/snapshots";

    auto primary2_sm = std::make_shared<IntegerStateMachine>(
        std::make_shared<WalReplication>(std::make_shared<MemoryAppendOnlyFileSystem>()),
        FileSystemSnapshotManager::Create(std::make_shared<MemoryAppendOnlyFileSystem>(), primary2_snapshot_config)
            .value());

    ReplicationConfig primary2_config{"primary2"};
    primary2_sm->Initialize(primary2_config, primary2_snapshot_config);

    result = oracle_->RegisterPrimary(primary2_sm);
    EXPECT_FALSE(result.ok()) << "Should fail to register second primary";

    // 5. Test double secondary registration (should succeed but return false)
    result = oracle_->RegisterSecondary(secondary1_sm_);
    VERIFY_RESULT_MSG(result, "Should handle double secondary registration");
    EXPECT_FALSE(result.value()) << "Should return false for already registered secondary";

    // 6. Test unregister non-existent state machine
    result = oracle_->UnregisterStateMachine(primary2_sm);
    VERIFY_RESULT_MSG(result, "Should handle unregistering non-existent state machine");
    EXPECT_FALSE(result.value()) << "Should return false for non-existent state machine";

    // 7. Test direct replication through primary propagates to secondaries
    auto direct_cmd = IntegerCommand::Serialize(IntegerCommand::Op::Set, 123);

    // Call Replicate directly on primary, which should trigger the interceptor
    auto direct_result = primary_sm_->Replicate(direct_cmd);
    VERIFY_RESULT_MSG(direct_result, "Should replicate directly through primary");

    // Wait for execution to complete on all nodes
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Verify both primary and secondary received the update
    EXPECT_EQ(123, primary_sm_->GetValue()) << "Primary should have value 123";
    EXPECT_EQ(123, secondary1_sm_->GetValue()) << "Secondary should have value 123";
}

}  // namespace pond::test