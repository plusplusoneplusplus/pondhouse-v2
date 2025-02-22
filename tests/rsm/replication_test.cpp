#include "rsm/replication.h"

#include <gtest/gtest.h>

#include "common/memory_append_only_fs.h"
#include "rsm/wal_replication.h"
#include "test_helper.h"

namespace pond::test {

using namespace pond::rsm;
using namespace pond::common;

class ReplicationTest : public ::testing::Test {
protected:
    void SetUp() override { fs_ = std::make_shared<MemoryAppendOnlyFileSystem>(); }

    void TearDown() override { fs_.reset(); }

    std::shared_ptr<MemoryAppendOnlyFileSystem> fs_;
};

//
// Test Setup:
//      Create a WalReplication instance and test basic operations
// Test Result:
//      All basic operations should work correctly with proper error handling
//
TEST_F(ReplicationTest, BasicOperations) {
    WalReplication replication(fs_);

    // Test Initialize with config
    ReplicationConfig config;
    config.directory = "test";
    auto result = replication.Initialize(config);
    VERIFY_RESULT_MSG(result, "Should initialize replication log");

    // Test initial state
    EXPECT_EQ(0, replication.GetCurrentIndex()) << "Initial index should be 0";

    // Create and append test entries
    DataChunk data1 = DataChunk::FromString("entry1");
    auto append_result = replication.Append(data1);
    VERIFY_RESULT_MSG(append_result, "Should append first entry");
    EXPECT_EQ(0, append_result.value()) << "First entry should get index 0";
    EXPECT_EQ(1, replication.GetCurrentIndex()) << "Current index should be 1";

    DataChunk data2 = DataChunk::FromString("entry2");
    append_result = replication.Append(data2);
    VERIFY_RESULT_MSG(append_result, "Should append second entry");
    EXPECT_EQ(1, append_result.value()) << "Second entry should get index 1";
    EXPECT_EQ(2, replication.GetCurrentIndex()) << "Current index should be 2";

    // Test reading entries
    auto read_result = replication.Read(0);
    VERIFY_RESULT_MSG(read_result, "Should read entries");
    auto entries = read_result.value();
    EXPECT_EQ(2, entries.size()) << "Should have two entries";

    // Verify entry contents
    EXPECT_EQ("entry1", std::string(reinterpret_cast<const char*>(entries[0].Data()), entries[0].Size()))
        << "First entry data should match";
    EXPECT_EQ("entry2", std::string(reinterpret_cast<const char*>(entries[1].Data()), entries[1].Size()))
        << "Second entry data should match";

    // Test index reset
    replication.ResetIndex();
    EXPECT_EQ(0, replication.GetCurrentIndex()) << "Index should be reset to 0";

    // Test close
    auto close_result = replication.Close();
    VERIFY_RESULT_MSG(close_result, "Should close replication log");
}

//
// Test Setup:
//      Test recovery by writing entries, closing, and reopening
// Test Result:
//      All entries should be recovered correctly after reopening
//
TEST_F(ReplicationTest, Recovery) {
    ReplicationConfig config;
    config.directory = "recovery";

    // Write some entries
    {
        WalReplication replication(fs_);
        auto result = replication.Initialize(config);
        VERIFY_RESULT_MSG(result, "Should initialize replication log");

        for (int i = 0; i < 5; ++i) {
            DataChunk data = DataChunk::FromString("entry" + std::to_string(i));
            auto append_result = replication.Append(data);
            VERIFY_RESULT_MSG(append_result, "Should append entry " + std::to_string(i));
            EXPECT_EQ(i, append_result.value()) << "Entry should get correct index";
            EXPECT_EQ(i + 1, replication.GetCurrentIndex()) << "Current index should be incremented";
        }
    }

    // Recover and verify
    {
        WalReplication replication(fs_);
        auto result = replication.Initialize(config);
        VERIFY_RESULT_MSG(result, "Should initialize replication log for recovery");
        EXPECT_EQ(0, replication.GetCurrentIndex()) << "Current index should be 0 after open before read";

        auto read_result = replication.Read(0);
        VERIFY_RESULT_MSG(read_result, "Should read all entries");
        auto entries = read_result.value();
        EXPECT_EQ(5, entries.size()) << "Should have five entries";

        EXPECT_EQ(5, replication.GetCurrentIndex()) << "Current index should be 5 after recovery";

        for (int i = 0; i < 5; ++i) {
            EXPECT_EQ("entry" + std::to_string(i),
                      std::string(reinterpret_cast<const char*>(entries[i].Data()), entries[i].Size()))
                << "Entry " << i << " data should match";
        }

        // Verify we can append after recovery
        DataChunk new_data = DataChunk::FromString("new_entry");
        auto append_result = replication.Append(new_data);
        VERIFY_RESULT_MSG(append_result, "Should append entry after recovery");
        EXPECT_EQ(5, append_result.value()) << "New entry should get correct index";
        EXPECT_EQ(6, replication.GetCurrentIndex()) << "Current index should be incremented";
    }
}

//
// Test Setup:
//      Test partial reads starting from different indices
// Test Result:
//      Should correctly return entries from the specified start index
//
TEST_F(ReplicationTest, PartialRead) {
    ReplicationConfig config;
    config.directory = "partial";

    // Write entries
    WalReplication replication(fs_);
    auto result = replication.Initialize(config);
    VERIFY_RESULT_MSG(result, "Should initialize replication log");

    for (int i = 0; i < 10; ++i) {
        DataChunk data = DataChunk::FromString("entry" + std::to_string(i));
        auto append_result = replication.Append(data);
        VERIFY_RESULT_MSG(append_result, "Should append entry " + std::to_string(i));
    }

    // Read from middle
    auto read_result = replication.Read(5);
    VERIFY_RESULT_MSG(read_result, "Should read entries from index 5");
    auto entries = read_result.value();
    EXPECT_EQ(5, entries.size()) << "Should have five entries";

    for (size_t i = 0; i < entries.size(); ++i) {
        size_t expected_index = i + 5;
        EXPECT_EQ("entry" + std::to_string(expected_index),
                  std::string(reinterpret_cast<const char*>(entries[i].Data()), entries[i].Size()))
            << "Entry data should match at index " << i;
    }
}

//
// Test Setup:
//      Test error handling for invalid operations
// Test Result:
//      Should handle errors gracefully and return appropriate error codes
//
TEST_F(ReplicationTest, ErrorHandling) {
    WalReplication replication(fs_);

    // Test operations before open
    DataChunk data = DataChunk::FromString("test");
    auto append_result = replication.Append(data);
    EXPECT_FALSE(append_result.ok()) << "Should fail to append before open";

    auto read_result = replication.Read(0);
    EXPECT_FALSE(read_result.ok()) << "Should fail to read before open";

    // Test invalid config
    ReplicationConfig empty_config;
    auto result = replication.Initialize(empty_config);
    EXPECT_FALSE(result.ok()) << "Should fail with empty path";

    // Test double open
    result = replication.Initialize(empty_config);
    EXPECT_FALSE(result.ok()) << "Should fail on double open";

    // Test close and reopen
    auto close_result = replication.Close();
    VERIFY_RESULT_MSG(close_result, "Should close replication log");

    result = replication.Initialize(empty_config);
    EXPECT_FALSE(result.ok()) << "Should fail to reopen with empty path";
}

}  // namespace pond::test