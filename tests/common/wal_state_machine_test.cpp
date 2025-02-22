#include "common/wal_state_machine.h"

#include <thread>

#include <gtest/gtest.h>

#include "common/memory_append_only_fs.h"
#include "test_helper.h"

namespace pond::common {

// Test implementation of WalStateMachine
class TestStateMachine : public WalStateMachine {
public:
    TestStateMachine(std::shared_ptr<IAppendOnlyFileSystem> fs,
                     const std::string& dir_path,
                     const Config& config = Config::Default())
        : WalStateMachine(std::move(fs), dir_path, config) {}

    // Expose internal state for testing
    int64_t value_{0};

protected:
    Result<void> ApplyEntry(const DataChunk& entry_data) override {
        // Simple state machine that just adds integers
        if (entry_data.Size() != sizeof(int64_t)) {
            return Result<void>::failure(Error(ErrorCode::InvalidArgument, "Invalid entry size"));
        }
        int64_t delta;
        std::memcpy(&delta, entry_data.Data(), sizeof(int64_t));
        value_ += delta;
        return Result<void>::success();
    }

    Result<DataChunkPtr> GetCurrentState() override {
        auto state = std::make_shared<DataChunk>(sizeof(int64_t));
        std::memcpy(state->Data(), &value_, sizeof(int64_t));
        return Result<DataChunkPtr>::success(state);
    }

    Result<void> RestoreState(const DataChunkPtr& state_data) override {
        if (state_data->Size() != sizeof(int64_t)) {
            return Result<void>::failure(Error(ErrorCode::InvalidArgument, "Invalid state size"));
        }
        std::memcpy(&value_, state_data->Data(), sizeof(int64_t));
        return Result<void>::success();
    }
};

class WalStateMachineTest : public ::testing::Test {
protected:
    void SetUp() override {
        fs_ = std::make_shared<MemoryAppendOnlyFileSystem>();
        state_machine_ = std::make_unique<TestStateMachine>(fs_, "test_state_machine");
    }

    // Helper to create entry data
    DataChunk CreateEntry(int64_t delta) {
        DataChunk entry(sizeof(int64_t));
        std::memcpy(entry.Data(), &delta, sizeof(int64_t));
        return entry;
    }

    // Helper to count checkpoint files
    size_t CountCheckpointFiles(const std::string& dir) {
        auto files = fs_->List(dir).value();
        size_t count = 0;
        for (const auto& file : files) {
            if (file.find("checkpoint.") != std::string::npos) {
                count++;
            }
        }
        return count;
    }

    std::shared_ptr<MemoryAppendOnlyFileSystem> fs_;
    std::unique_ptr<TestStateMachine> state_machine_;
};

TEST_F(WalStateMachineTest, BasicOperations) {
    // Initialize state machine
    VERIFY_RESULT(state_machine_->Open());
    EXPECT_EQ(state_machine_->value_, 0);

    // Apply some entries
    VERIFY_RESULT(state_machine_->Apply(CreateEntry(5)));
    EXPECT_EQ(state_machine_->value_, 5);

    VERIFY_RESULT(state_machine_->Apply(CreateEntry(3)));
    EXPECT_EQ(state_machine_->value_, 8);

    VERIFY_RESULT(state_machine_->Apply(CreateEntry(-2)));
    EXPECT_EQ(state_machine_->value_, 6);
}

TEST_F(WalStateMachineTest, AutomaticCheckpointing) {
    // Configure checkpoint interval
    WalStateMachine::Config config;
    config.checkpoint_interval = 100;  // Checkpoint every 100 entries
    state_machine_ = std::make_unique<TestStateMachine>(fs_, "test_state_machine", config);

    VERIFY_RESULT(state_machine_->Open());

    // Apply entries until we expect two checkpoints
    for (int i = 0; i < 250; i++) {
        VERIFY_RESULT(state_machine_->Apply(CreateEntry(1)));

        // Verify checkpoint creation at intervals
        if (i == 100) {
            EXPECT_EQ(CountCheckpointFiles("test_state_machine"), 1);
        } else if (i == 200) {
            EXPECT_EQ(CountCheckpointFiles("test_state_machine"), 2);
        }
    }
    EXPECT_EQ(state_machine_->value_, 250);

    // Create new state machine and recover
    auto new_machine = std::make_unique<TestStateMachine>(fs_, "test_state_machine", config);
    VERIFY_RESULT(new_machine->Open());
    EXPECT_EQ(new_machine->value_, 250);
}

TEST_F(WalStateMachineTest, ManualCheckpointing) {
    // Configure a large checkpoint interval to prevent automatic checkpoints
    WalStateMachine::Config config;
    config.checkpoint_interval = 10000;
    state_machine_ = std::make_unique<TestStateMachine>(fs_, "test_state_machine", config);

    VERIFY_RESULT(state_machine_->Open());

    // Apply some entries
    for (int i = 0; i < 100; i++) {
        VERIFY_RESULT(state_machine_->Apply(CreateEntry(1)));
    }
    EXPECT_EQ(CountCheckpointFiles("test_state_machine"), 0);

    // Force a checkpoint
    VERIFY_RESULT(state_machine_->CreateCheckpoint());
    EXPECT_EQ(CountCheckpointFiles("test_state_machine"), 1);

    // Create new state machine and recover
    auto new_machine = std::make_unique<TestStateMachine>(fs_, "test_state_machine", config);
    VERIFY_RESULT(new_machine->Open());
    EXPECT_EQ(new_machine->value_, 100);
}

TEST_F(WalStateMachineTest, CheckpointRetention) {
    // Configure frequent checkpoints but limited retention
    WalStateMachine::Config config;
    config.max_checkpoints = 2;
    config.checkpoint_interval = 10;
    state_machine_ = std::make_unique<TestStateMachine>(fs_, "test_state_machine", config);

    VERIFY_RESULT(state_machine_->Open());

    // Create multiple checkpoints
    for (int i = 0; i < 50; i++) {
        VERIFY_RESULT(state_machine_->Apply(CreateEntry(1)));

        // Verify checkpoint count never exceeds max_checkpoints
        EXPECT_LE(CountCheckpointFiles("test_state_machine"), config.max_checkpoints);
    }

    // Final verification
    EXPECT_EQ(CountCheckpointFiles("test_state_machine"), config.max_checkpoints);
}

TEST_F(WalStateMachineTest, Recovery) {
    VERIFY_RESULT(state_machine_->Open());

    // Apply some entries and create checkpoint
    for (int i = 0; i < 100; i++) {
        VERIFY_RESULT(state_machine_->Apply(CreateEntry(1)));
    }
    VERIFY_RESULT(state_machine_->CreateCheckpoint());

    // Apply more entries after checkpoint
    for (int i = 0; i < 50; i++) {
        VERIFY_RESULT(state_machine_->Apply(CreateEntry(1)));
    }
    EXPECT_EQ(state_machine_->value_, 150);

    // Create new state machine and recover
    auto new_machine = std::make_unique<TestStateMachine>(fs_, "test_state_machine");
    VERIFY_RESULT(new_machine->Open());
    EXPECT_EQ(new_machine->value_, 150);
}

TEST_F(WalStateMachineTest, InvalidEntries) {
    VERIFY_RESULT(state_machine_->Open());

    // Try to apply invalid entry
    DataChunk invalid_entry(1);  // Wrong size
    auto result = state_machine_->Apply(invalid_entry);
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.error().code(), ErrorCode::InvalidArgument);
}

TEST_F(WalStateMachineTest, ConcurrentAccess) {
    VERIFY_RESULT(state_machine_->Open());

    // Create multiple threads to apply entries
    std::vector<std::thread> threads;
    std::atomic<int> success_count{0};

    for (int i = 0; i < 10; i++) {
        threads.emplace_back([this, &success_count]() {
            for (int j = 0; j < 100; j++) {
                if (state_machine_->Apply(CreateEntry(1)).ok()) {
                    success_count++;
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    EXPECT_EQ(success_count, 1000);
    EXPECT_EQ(state_machine_->value_, 1000);
}

TEST_F(WalStateMachineTest, DirectoryCreation) {
    // Test with non-existent directory
    auto new_machine = std::make_unique<TestStateMachine>(fs_, "new_dir/state_machine");
    auto result = new_machine->Open();
    VERIFY_RESULT_MSG(result, "Failed to open state machine");

    // Verify directory was created
    EXPECT_TRUE(fs_->Exists("new_dir/state_machine"));
}

TEST_F(WalStateMachineTest, CheckpointCorruption) {
    VERIFY_RESULT(state_machine_->Open());

    // Apply entries and create checkpoint
    VERIFY_RESULT(state_machine_->Apply(CreateEntry(100)));
    VERIFY_RESULT(state_machine_->CreateCheckpoint());

    {
        auto new_machine = std::make_unique<TestStateMachine>(fs_, "test_state_machine");
        VERIFY_RESULT(new_machine->Open());
        EXPECT_EQ(new_machine->value_, 100);
    }

    // Corrupt the checkpoint file
    auto files = fs_->List("test_state_machine").value();
    for (const auto& file : files) {
        if (file.find("checkpoint.") != std::string::npos) {
            auto handle = fs_->OpenFile("test_state_machine/" + file, true).value();
            DataChunk corrupt_data(1);  // Wrong size
            auto result = fs_->Append(handle, corrupt_data);
            VERIFY_RESULT(result);
            fs_->CloseFile(handle);
            break;
        }
    }

    {
        // Try to recover with corrupted checkpoint
        auto new_machine = std::make_unique<TestStateMachine>(fs_, "test_state_machine");
        auto result = new_machine->Open();
        ASSERT_FALSE(result.ok());
        EXPECT_EQ(result.error().code(), ErrorCode::InvalidArgument);
    }
}

TEST_F(WalStateMachineTest, CheckpointCleanup) {
    // Configure frequent checkpoints with limited retention
    WalStateMachine::Config config;
    config.max_checkpoints = 2;
    config.checkpoint_interval = 50;  // Create checkpoint every 50 entries
    state_machine_ = std::make_unique<TestStateMachine>(fs_, "test_state_machine", config);

    VERIFY_RESULT(state_machine_->Open());

    // Create first checkpoint
    for (int i = 0; i < 50; i++) {
        VERIFY_RESULT(state_machine_->Apply(CreateEntry(1)));
    }
    EXPECT_EQ(CountCheckpointFiles("test_state_machine"), 1);

    // Get the first checkpoint file name
    std::string first_checkpoint;
    {
        auto files = fs_->List("test_state_machine").value();
        for (const auto& file : files) {
            if (file.find("checkpoint.") != std::string::npos) {
                first_checkpoint = file;
                break;
            }
        }
    }
    ASSERT_FALSE(first_checkpoint.empty());

    // Create second checkpoint
    for (int i = 0; i < 50; i++) {
        VERIFY_RESULT(state_machine_->Apply(CreateEntry(1)));
    }
    EXPECT_EQ(CountCheckpointFiles("test_state_machine"), 2);

    // Create third checkpoint, which should trigger cleanup of first checkpoint
    for (int i = 0; i < 50; i++) {
        VERIFY_RESULT(state_machine_->Apply(CreateEntry(1)));
    }
    EXPECT_EQ(CountCheckpointFiles("test_state_machine"), 2);  // Still 2 due to cleanup

    // Verify first checkpoint was deleted
    auto files = fs_->List("test_state_machine").value();
    bool first_checkpoint_exists = false;
    for (const auto& file : files) {
        if (file == first_checkpoint) {
            first_checkpoint_exists = true;
            break;
        }
    }
    EXPECT_FALSE(first_checkpoint_exists) << "First checkpoint should have been cleaned up";

    // Verify we can still recover correctly
    auto new_machine = std::make_unique<TestStateMachine>(fs_, "test_state_machine", config);
    VERIFY_RESULT(new_machine->Open());
    EXPECT_EQ(new_machine->value_, 150);  // All entries were applied
}

TEST_F(WalStateMachineTest, WalRotation) {
    // Configure small WAL size
    WalStateMachine::Config config;
    config.max_wal_size = 1024;  // 1KB
    state_machine_ = std::make_unique<TestStateMachine>(fs_, "test_state_machine");

    VERIFY_RESULT(state_machine_->Open());

    // Apply enough entries to trigger WAL rotation
    for (int i = 0; i < 1000; i++) {
        VERIFY_RESULT(state_machine_->Apply(CreateEntry(1)));
    }

    EXPECT_EQ(state_machine_->value_, 1000);
}

}  // namespace pond::common