#include "kv/table_metadata.h"

#include <gtest/gtest.h>

#include "common/memory_append_only_fs.h"
#include "test_helper.h"

namespace pond::kv {

class TableMetadataTest : public ::testing::Test {
protected:
    void SetUp() override { fs_ = std::make_shared<common::MemoryAppendOnlyFileSystem>(); }

    void WaitForExecution(TableMetadataStateMachine& state_machine, uint64_t target_lsn) {
        while (true) {
            if (state_machine.GetLastExecutedLSN() != common::INVALID_LSN
                && state_machine.GetLastExecutedLSN() >= target_lsn) {
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }

    std::shared_ptr<common::MemoryAppendOnlyFileSystem> fs_;
};

TEST_F(TableMetadataTest, MetadataEntrySerialization) {
    // Create a metadata entry with multiple files
    std::vector<FileInfo> files = {
        {"file1.sst", 1000},
        {"file2.sst", 2000},
        {"file3.sst", 3000},
    };
    TableMetadataEntry entry(MetadataOpType::CreateSSTable, files, {});

    // Serialize
    auto data = entry.Serialize();
    ASSERT_GT(data.Size(), 0);

    // Deserialize
    TableMetadataEntry deserialized;
    ASSERT_TRUE(deserialized.Deserialize(data));

    // Verify contents
    EXPECT_EQ(deserialized.op_type(), MetadataOpType::CreateSSTable);
    ASSERT_EQ(deserialized.added_files().size(), 3);
    EXPECT_EQ(deserialized.added_files()[0].name, "file1.sst");
    EXPECT_EQ(deserialized.added_files()[0].size, 1000);
    EXPECT_EQ(deserialized.added_files()[1].name, "file2.sst");
    EXPECT_EQ(deserialized.added_files()[1].size, 2000);
    EXPECT_EQ(deserialized.added_files()[2].name, "file3.sst");
    EXPECT_EQ(deserialized.added_files()[2].size, 3000);
}

TEST_F(TableMetadataTest, MetadataEntryEmptyFiles) {
    // Create entry with no files
    TableMetadataEntry entry(MetadataOpType::RotateWAL);

    // Serialize and deserialize
    auto data = entry.Serialize();
    ASSERT_GT(data.Size(), 0);

    TableMetadataEntry deserialized;
    ASSERT_TRUE(deserialized.Deserialize(data));

    // Verify contents
    EXPECT_EQ(deserialized.op_type(), MetadataOpType::RotateWAL);
    EXPECT_TRUE(deserialized.added_files().empty());
    EXPECT_TRUE(deserialized.deleted_files().empty());
}

TEST_F(TableMetadataTest, MetadataEntryInvalidData) {
    // Try to deserialize invalid data
    common::DataChunk invalid_data(1);  // Too small
    TableMetadataEntry entry;
    EXPECT_FALSE(entry.Deserialize(invalid_data));
}

TEST_F(TableMetadataTest, StateMachineBasicOperations) {
    TableMetadataStateMachine state_machine(fs_, "test_metadata");
    VERIFY_RESULT(state_machine.Open());

    // Create some SSTable files
    std::vector<FileInfo> files1 = {{"file1.sst", 1000}};
    TableMetadataEntry entry1(MetadataOpType::CreateSSTable, files1);
    VERIFY_RESULT(state_machine.Replicate(entry1.Serialize()));
    WaitForExecution(state_machine, 0);

    std::vector<FileInfo> files2 = {{"file2.sst", 2000}};
    TableMetadataEntry entry2(MetadataOpType::CreateSSTable, files2);
    VERIFY_RESULT(state_machine.Replicate(entry2.Serialize()));
    WaitForExecution(state_machine, 1);

    // Verify state
    EXPECT_EQ(state_machine.GetSSTableFiles(0 /*level*/).size(), 2);
    EXPECT_EQ(state_machine.GetTotalSize(), 3000);
}

TEST_F(TableMetadataTest, StateMachineDeleteOperations) {
    TableMetadataStateMachine state_machine(fs_, "test_metadata");
    VERIFY_RESULT(state_machine.Open());

    // Create some files
    std::vector<FileInfo> files = {
        {"file1.sst", 1000},
        {"file2.sst", 2000},
        {"file3.sst", 3000},
    };
    TableMetadataEntry create_entry(MetadataOpType::CreateSSTable, files, {});
    VERIFY_RESULT(state_machine.Replicate(create_entry.Serialize()));
    WaitForExecution(state_machine, 0);

    // Delete one file
    std::vector<FileInfo> delete_files = {{"file2.sst", 2000}};
    TableMetadataEntry delete_entry(MetadataOpType::DeleteSSTable, {}, delete_files);
    VERIFY_RESULT(state_machine.Replicate(delete_entry.Serialize()));
    WaitForExecution(state_machine, 1);

    // Verify state
    EXPECT_EQ(state_machine.GetSSTableFiles(0 /*level*/).size(), 2);
    EXPECT_EQ(state_machine.GetTotalSize(), 4000);
}

TEST_F(TableMetadataTest, StateMachineRecovery) {
    {
        // First instance: create some state
        TableMetadataStateMachine state_machine(fs_, "test_metadata");
        VERIFY_RESULT(state_machine.Open());

        std::vector<FileInfo> files = {
            {"file1.sst", 1000},
            {"file2.sst", 2000},
        };
        TableMetadataEntry entry(MetadataOpType::CreateSSTable, files);
        VERIFY_RESULT(state_machine.Replicate(entry.Serialize()));
        WaitForExecution(state_machine, 0);
    }

    {
        // Second instance: recover state
        TableMetadataStateMachine recovered(fs_, "test_metadata");
        VERIFY_RESULT(recovered.Open());

        // Verify recovered state
        EXPECT_EQ(recovered.GetSSTableFiles(0 /*level*/).size(), 2);
        EXPECT_EQ(recovered.GetTotalSize(), 3000);
    }
}

TEST_F(TableMetadataTest, StateMachineUpdateStats) {
    TableMetadataStateMachine state_machine(fs_, "test_metadata");
    VERIFY_RESULT(state_machine.Open());

    // Create initial state
    std::vector<FileInfo> files = {{"file1.sst", 1000}};
    TableMetadataEntry create_entry(MetadataOpType::CreateSSTable, files);
    VERIFY_RESULT(state_machine.Replicate(create_entry.Serialize()));
    WaitForExecution(state_machine, 0);

    // Update stats
    std::vector<FileInfo> stats = {{"", 5000}};  // Only size matters for UpdateStats
    TableMetadataEntry stats_entry(MetadataOpType::UpdateStats, stats);
    VERIFY_RESULT(state_machine.Replicate(stats_entry.Serialize()));
    WaitForExecution(state_machine, 1);

    // Verify state
    EXPECT_EQ(state_machine.GetTotalSize(), 5000);
}

TEST_F(TableMetadataTest, StateMachineSnapshotAndRecovery) {
    {
        TableMetadataStateMachine state_machine(fs_, "test_metadata");
        VERIFY_RESULT(state_machine.Open());

        // Create some state
        std::vector<FileInfo> files = {{"file1.sst", 1000}};
        TableMetadataEntry entry(MetadataOpType::CreateSSTable, files);
        VERIFY_RESULT(state_machine.Replicate(entry.Serialize()));
        WaitForExecution(state_machine, 0);

        // Create snapshot
        VERIFY_RESULT(state_machine.TriggerSnapshot());
    }

    {
        // Create new instance and verify it recovers from snapshot
        TableMetadataStateMachine recovered(fs_, "test_metadata");
        VERIFY_RESULT(recovered.Open());
        EXPECT_EQ(recovered.GetSSTableFiles(0 /*level*/).size(), 1);
        EXPECT_EQ(recovered.GetTotalSize(), 1000);
    }
}

TEST_F(TableMetadataTest, StateMachineNoOpOperations) {
    TableMetadataStateMachine state_machine(fs_, "test_metadata");
    VERIFY_RESULT(state_machine.Open());

    // Create initial state
    std::vector<FileInfo> files = {{"file1.sst", 1000}};
    TableMetadataEntry create_entry(MetadataOpType::CreateSSTable, files);
    VERIFY_RESULT(state_machine.Replicate(create_entry.Serialize()));
    WaitForExecution(state_machine, 0);

    // Apply no-op operations (FlushMemTable and RotateWAL)
    TableMetadataEntry flush_entry(MetadataOpType::FlushMemTable);
    VERIFY_RESULT(state_machine.Replicate(flush_entry.Serialize()));
    WaitForExecution(state_machine, 1);

    TableMetadataEntry rotate_entry(MetadataOpType::RotateWAL);
    VERIFY_RESULT(state_machine.Replicate(rotate_entry.Serialize()));
    WaitForExecution(state_machine, 2);

    // Verify state hasn't changed
    EXPECT_EQ(state_machine.GetSSTableFiles(0 /*level*/).size(), 1);
    EXPECT_EQ(state_machine.GetTotalSize(), 1000);
}

TEST_F(TableMetadataTest, FileInfoSerialization) {
    // Create a FileInfo with all fields populated
    FileInfo file("test.sst", 1000, 2, "key1", "key100");

    // Serialize
    auto data = file.Serialize();
    ASSERT_GT(data.Size(), 0);

    // Deserialize
    FileInfo deserialized;
    ASSERT_TRUE(deserialized.Deserialize(data));

    // Verify all fields were correctly serialized/deserialized
    EXPECT_EQ(deserialized.name, "test.sst");
    EXPECT_EQ(deserialized.size, 1000);
    EXPECT_EQ(deserialized.level, 2);
    EXPECT_EQ(deserialized.smallest_key, "key1");
    EXPECT_EQ(deserialized.largest_key, "key100");

    // Test empty keys
    FileInfo empty_keys("test2.sst", 2000, 3);
    auto empty_data = empty_keys.Serialize();
    FileInfo deserialized_empty;
    ASSERT_TRUE(deserialized_empty.Deserialize(empty_data));
    EXPECT_EQ(deserialized_empty.name, "test2.sst");
    EXPECT_EQ(deserialized_empty.size, 2000);
    EXPECT_EQ(deserialized_empty.level, 3);
    EXPECT_TRUE(deserialized_empty.smallest_key.empty());
    EXPECT_TRUE(deserialized_empty.largest_key.empty());
}

TEST_F(TableMetadataTest, StateMachineStateSerialization) {
    TableMetadataStateMachine state_machine(fs_, "test_metadata");
    VERIFY_RESULT(state_machine.Open());

    // Create state with multiple levels and files
    std::vector<FileInfo> level0_files = {FileInfo("L0_1.sst", 1000, 0, "a", "m"),
                                          FileInfo("L0_2.sst", 2000, 0, "n", "z")};
    TableMetadataEntry entry0(MetadataOpType::CreateSSTable, level0_files);
    VERIFY_RESULT(state_machine.Replicate(entry0.Serialize()));
    WaitForExecution(state_machine, 0);

    std::vector<FileInfo> level1_files = {FileInfo("L1_1.sst", 3000, 1, "a", "z")};
    TableMetadataEntry entry1(MetadataOpType::CreateSSTable, level1_files);
    VERIFY_RESULT(state_machine.Replicate(entry1.Serialize()));
    WaitForExecution(state_machine, 1);

    // Get current state
    auto state_data = state_machine.Serialize();
    ASSERT_GT(state_data.Size(), 0);

    // Create new state machine and restore state
    TableMetadataStateMachine recovered(fs_, "test_metadata2");
    VERIFY_RESULT(recovered.Open());
    ASSERT_TRUE(recovered.Deserialize(state_data));

    // Verify recovered state
    EXPECT_EQ(recovered.GetLevelCount(), 2);
    EXPECT_EQ(recovered.GetTotalSize(), 6000);

    // Verify level 0
    const auto& recovered_l0 = recovered.GetSSTableFiles(0);
    ASSERT_EQ(recovered_l0.size(), 2);
    EXPECT_EQ(recovered_l0[0].name, "L0_1.sst");
    EXPECT_EQ(recovered_l0[0].size, 1000);
    EXPECT_EQ(recovered_l0[0].level, 0);
    EXPECT_EQ(recovered_l0[0].smallest_key, "a");
    EXPECT_EQ(recovered_l0[0].largest_key, "m");
    EXPECT_EQ(recovered_l0[1].name, "L0_2.sst");
    EXPECT_EQ(recovered_l0[1].size, 2000);
    EXPECT_EQ(recovered_l0[1].smallest_key, "n");
    EXPECT_EQ(recovered_l0[1].largest_key, "z");

    // Verify level 1
    const auto& recovered_l1 = recovered.GetSSTableFiles(1);
    ASSERT_EQ(recovered_l1.size(), 1);
    EXPECT_EQ(recovered_l1[0].name, "L1_1.sst");
    EXPECT_EQ(recovered_l1[0].size, 3000);
    EXPECT_EQ(recovered_l1[0].level, 1);
    EXPECT_EQ(recovered_l1[0].smallest_key, "a");
    EXPECT_EQ(recovered_l1[0].largest_key, "z");

    // Verify level sizes
    EXPECT_EQ(recovered.GetLevelSize(0), 3000);
    EXPECT_EQ(recovered.GetLevelSize(1), 3000);
}

TEST_F(TableMetadataTest, StateMachineCompactionOperations) {
    TableMetadataStateMachine state_machine(fs_, "test_metadata");
    VERIFY_RESULT(state_machine.Open());

    // Create initial files in L0
    std::vector<FileInfo> l0_files = {FileInfo("L0_1.sst", 1000, 0, "a", "m"), FileInfo("L0_2.sst", 2000, 0, "n", "z")};
    TableMetadataEntry create_entry(MetadataOpType::CreateSSTable, l0_files);
    VERIFY_RESULT(state_machine.Replicate(create_entry.Serialize()));
    WaitForExecution(state_machine, 0);

    // Verify initial state
    EXPECT_EQ(state_machine.GetSSTableFiles(0).size(), 2);
    EXPECT_EQ(state_machine.GetTotalSize(), 3000);
    EXPECT_EQ(state_machine.GetLevelSize(0), 3000);

    // Simulate compaction: L0 files are compacted into a single L1 file
    std::vector<FileInfo> compacted_file = {
        FileInfo("L1_1.sst", 2800, 1, "a", "z")  // Slightly smaller due to compaction
    };
    TableMetadataEntry compact_entry(MetadataOpType::CompactFiles, compacted_file, l0_files);
    VERIFY_RESULT(state_machine.Replicate(compact_entry.Serialize()));
    WaitForExecution(state_machine, 1);

    // Verify state after compaction
    EXPECT_EQ(state_machine.GetSSTableFiles(0).size(), 0);  // L0 files should be gone
    EXPECT_EQ(state_machine.GetSSTableFiles(1).size(), 1);  // One L1 file
    EXPECT_EQ(state_machine.GetTotalSize(), 2800);          // New total size
    EXPECT_EQ(state_machine.GetLevelSize(0), 0);            // L0 is empty
    EXPECT_EQ(state_machine.GetLevelSize(1), 2800);         // L1 has the compacted file

    // Verify the compacted file details
    const auto& l1_files = state_machine.GetSSTableFiles(1);
    ASSERT_EQ(l1_files.size(), 1);
    EXPECT_EQ(l1_files[0].name, "L1_1.sst");
    EXPECT_EQ(l1_files[0].size, 2800);
    EXPECT_EQ(l1_files[0].level, 1);
    EXPECT_EQ(l1_files[0].smallest_key, "a");
    EXPECT_EQ(l1_files[0].largest_key, "z");

    // Create new state machine and verify state is recovered correctly
    TableMetadataStateMachine recovered(fs_, "test_metadata");
    VERIFY_RESULT(recovered.Open());

    // Verify recovered state matches
    EXPECT_EQ(recovered.GetSSTableFiles(0).size(), 0);
    EXPECT_EQ(recovered.GetSSTableFiles(1).size(), 1);
    EXPECT_EQ(recovered.GetTotalSize(), 2800);
    EXPECT_EQ(recovered.GetLevelSize(0), 0);
    EXPECT_EQ(recovered.GetLevelSize(1), 2800);
}

}  // namespace pond::kv