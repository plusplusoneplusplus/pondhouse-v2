#include "rsm/snapshot_manager.h"

#include <future>

#include <gtest/gtest.h>

#include "common/filesystem_stream.h"
#include "common/memory_append_only_fs.h"
#include "test_helper.h"

using namespace pond::common;

namespace pond::rsm {

// Mock state machine for testing
class MockSnapshotableState : public ISnapshotable {
public:
    MockSnapshotableState(uint64_t lsn = 1, uint64_t term = 0) : lsn_(lsn), term_(term) {}

    Result<SnapshotMetadata> CreateSnapshot(common::OutputStream* writer) override {
        SnapshotMetadata metadata;
        metadata.lsn = lsn_;
        metadata.term = term_;
        metadata.version = 1;
        metadata.cluster_config = "test_config";

        // Write some test data
        auto data = std::make_shared<common::DataChunk>(test_data_.size());
        std::memcpy(data->Data(), test_data_.data(), test_data_.size());
        auto result = writer->Write(data);
        if (!result.ok()) {
            return Result<SnapshotMetadata>::failure(result.error());
        }

        last_metadata_ = metadata;
        return Result<SnapshotMetadata>::success(metadata);
    }

    Result<bool> ApplySnapshot(common::InputStream* reader, const SnapshotMetadata& metadata) override {
        auto result = reader->Read(test_data_.size());
        if (!result.ok()) {
            return Result<bool>::failure(result.error());
        }

        std::string restored_data(reinterpret_cast<const char*>(result.value()->Data()), result.value()->Size());
        if (restored_data != test_data_) {
            return Result<bool>::failure(ErrorCode::InvalidArgument, "Data mismatch");
        }

        lsn_ = metadata.lsn;
        term_ = metadata.term;
        last_metadata_ = metadata;
        return Result<bool>::success(true);
    }

    Result<SnapshotMetadata> GetLastSnapshotMetadata() const override {
        return Result<SnapshotMetadata>::success(last_metadata_);
    }

    void SetTestData(const std::string& data) { test_data_ = data; }
    const std::string& GetTestData() const { return test_data_; }

private:
    uint64_t lsn_;
    uint64_t term_;
    SnapshotMetadata last_metadata_;
    std::string test_data_{"test data content"};
};

class SnapshotManagerTest : public ::testing::Test {
protected:
    void SetUp() override {
        fs_ = std::make_shared<common::MemoryAppendOnlyFileSystem>();
        config_.snapshot_dir = "/snapshots";
        config_.keep_snapshots = 3;
        auto result = FileSystemSnapshotManager::Create(fs_, config_);
        ASSERT_TRUE(result.ok());
        manager_ = std::dynamic_pointer_cast<FileSystemSnapshotManager>(result.value());
    }

    std::shared_ptr<common::MemoryAppendOnlyFileSystem> fs_;
    std::shared_ptr<FileSystemSnapshotManager> manager_;
    SnapshotConfig config_;
};

//
// Test Setup:
//      Creates a snapshot with valid state and verifies basic functionality
// Test Result:
//      Snapshot is created successfully with correct metadata and content
//
TEST_F(SnapshotManagerTest, BasicCreateAndRestore) {
    MockSnapshotableState state(100, 1);
    state.SetTestData("test data 1");

    // Create snapshot
    auto create_result = manager_->CreateSnapshot(&state);
    VERIFY_RESULT(create_result);
    EXPECT_EQ(create_result.value().lsn, 100);
    EXPECT_EQ(create_result.value().term, 1);

    // Restore snapshot
    MockSnapshotableState new_state;
    new_state.SetTestData("test data 1");  // Set expected data
    auto restore_result = manager_->RestoreSnapshot(&new_state, "100");
    VERIFY_RESULT(restore_result);
}

//
// Test Setup:
//      Attempts to create snapshots with invalid states
// Test Result:
//      Operations fail gracefully with appropriate errors
//
TEST_F(SnapshotManagerTest, InvalidStateHandling) {
    // Null state
    auto result = manager_->CreateSnapshot(nullptr);
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.error().code(), ErrorCode::InvalidArgument);

    // Restore with null state
    auto restore_result = manager_->RestoreSnapshot(nullptr, "1");
    ASSERT_FALSE(restore_result.ok());
    EXPECT_EQ(restore_result.error().code(), ErrorCode::InvalidArgument);
}

//
// Test Setup:
//      Creates multiple snapshots and tests retention policy
// Test Result:
//      Only the specified number of recent snapshots are kept
//
TEST_F(SnapshotManagerTest, SnapshotRetention) {
    MockSnapshotableState state;

    // Create 5 snapshots
    for (uint64_t i = 1; i <= 5; i++) {
        state = MockSnapshotableState(i, 0);
        auto result = manager_->CreateSnapshot(&state);
        VERIFY_RESULT(result);
    }

    // List snapshots
    auto list_result = manager_->ListSnapshots();
    VERIFY_RESULT(list_result);

    // Should only keep 3 snapshots (config_.keep_snapshots)
    ASSERT_EQ(list_result.value().size(), 3);

    // Should keep the most recent ones
    EXPECT_EQ(list_result.value()[0].lsn, 5);
    EXPECT_EQ(list_result.value()[1].lsn, 4);
    EXPECT_EQ(list_result.value()[2].lsn, 3);
}

//
// Test Setup:
//      Attempts to restore non-existent or corrupted snapshots
// Test Result:
//      Operations fail gracefully with appropriate errors
//
TEST_F(SnapshotManagerTest, ErrorHandling) {
    MockSnapshotableState state;

    // Non-existent snapshot
    auto result = manager_->RestoreSnapshot(&state, "999");
    ASSERT_FALSE(result.ok());

    // Create a corrupted snapshot file
    std::string corrupt_path = config_.snapshot_dir + "/1.snapshot";
    auto stream_result = FileSystemOutputStream::Create(fs_, corrupt_path);
    VERIFY_RESULT(stream_result);
    auto& stream = stream_result.value();

    std::string corrupt_data = "corrupted data";
    auto data = std::make_shared<common::DataChunk>(corrupt_data.size());
    std::memcpy(data->Data(), corrupt_data.data(), corrupt_data.size());
    stream->Write(data);

    // Try to restore corrupted snapshot
    result = manager_->RestoreSnapshot(&state, "1");
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.error().code(), ErrorCode::FileCorrupted);
}

//
// Test Setup:
//      Creates snapshots with various data sizes
// Test Result:
//      Handles different data sizes correctly
//
TEST_F(SnapshotManagerTest, VariousDataSizes) {
    MockSnapshotableState state(1, 0);

    // Empty data
    state.SetTestData("");
    auto result = manager_->CreateSnapshot(&state);
    VERIFY_RESULT(result);

    // Large data
    std::string large_data(1024 * 1024, 'x');  // 1MB
    state.SetTestData(large_data);
    result = manager_->CreateSnapshot(&state);
    VERIFY_RESULT(result);

    // Restore and verify large data
    MockSnapshotableState new_state;
    new_state.SetTestData(large_data);
    auto restore_result = manager_->RestoreSnapshot(&new_state, "1");
    VERIFY_RESULT(restore_result);
}

//
// Test Setup:
//      Tests concurrent snapshot operations
// Test Result:
//      Handles concurrent operations correctly
//
TEST_F(SnapshotManagerTest, ConcurrentOperations) {
    MockSnapshotableState state1(1, 0);
    MockSnapshotableState state2(2, 0);

    // Create two snapshots concurrently
    auto create1 = std::async(std::launch::async, [&]() { return manager_->CreateSnapshot(&state1); });
    auto create2 = std::async(std::launch::async, [&]() { return manager_->CreateSnapshot(&state2); });

    auto result1 = create1.get();
    auto result2 = create2.get();
    VERIFY_RESULT(result1);
    VERIFY_RESULT(result2);

    // List snapshots
    auto list_result = manager_->ListSnapshots();
    VERIFY_RESULT(list_result);
    ASSERT_EQ(list_result.value().size(), 2);
}

//
// Test Setup:
//      Tests snapshot operations during state changes
// Test Result:
//      Handles state changes correctly
//
TEST_F(SnapshotManagerTest, StateChanges) {
    MockSnapshotableState state(1, 0);

    // Create initial snapshot
    auto result = manager_->CreateSnapshot(&state);
    VERIFY_RESULT(result);

    // Change state
    state = MockSnapshotableState(2, 1);
    state.SetTestData("new data");

    // Create new snapshot
    result = manager_->CreateSnapshot(&state);
    VERIFY_RESULT(result);
    EXPECT_EQ(result.value().lsn, 2);
    EXPECT_EQ(result.value().term, 1);

    // Verify both snapshots exist
    auto list_result = manager_->ListSnapshots();
    VERIFY_RESULT(list_result);
    ASSERT_EQ(list_result.value().size(), 2);
}

}  // namespace pond::rsm