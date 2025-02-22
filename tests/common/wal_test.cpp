#include "common/wal.h"

#include <cassert>
#include <filesystem>

#include <gtest/gtest.h>

#include "common/memory_append_only_fs.h"
#include "kv/kv_entry.h"
#include "test_helper.h"

namespace pond::test {

using namespace pond::common;
using namespace pond::kv;

// Helper function to create test entries
KvEntry createTestEntry(const std::string& key, const std::string& value, LSN lsn, Timestamp ts, EntryType type) {
    return KvEntry{key, DataChunk(reinterpret_cast<const uint8_t*>(value.c_str()), value.length()), lsn, ts, type};
}

class WALTest : public ::testing::Test {
protected:
    void SetUp() override { fs_ = std::make_shared<MemoryAppendOnlyFileSystem>(); }

    void TearDown() override { fs_.reset(); }

    std::shared_ptr<IAppendOnlyFileSystem> fs_;
};

TEST_F(WALTest, LSNValidation) {
    auto _ = fs_->DeleteFiles({"test_lsn.wal"});

    WAL<KvEntry> wal(fs_);
    auto result = wal.Open("test_lsn.wal");
    VERIFY_RESULT_MSG(result, "Should open WAL file");

    // Test case 1: Entry with LSN=INVALID_LSN should get assigned next sequential LSN
    auto entry1 = createTestEntry("key1", "value1", INVALID_LSN, 100, EntryType::Put);
    auto append_result = wal.Append(entry1);
    VERIFY_RESULT_MSG(append_result, "Should append entry with INVALID_LSN");
    EXPECT_EQ(0, append_result.value()) << "First entry should get LSN 0";
    EXPECT_EQ(1, wal.current_lsn()) << "Current LSN should be 1";

    // Test case 2: Entry with valid LSN should be accepted
    auto entry2 = createTestEntry("key2", "value2", 1, 200, EntryType::Put);
    append_result = wal.Append(entry2);
    VERIFY_RESULT_MSG(append_result, "Should append entry with valid LSN");
    EXPECT_EQ(1, append_result.value()) << "Second entry should get LSN 1";
    EXPECT_EQ(2, wal.current_lsn()) << "Current LSN should be 2";

    // Test case 3: Entry with LSN < current_lsn should be rejected
    auto entry3 = createTestEntry("key3", "value3", 1, 300, EntryType::Put);
    append_result = wal.Append(entry3);
    EXPECT_FALSE(append_result.ok()) << "Should reject entry with duplicate LSN";
    VERIFY_ERROR_CODE(append_result, ErrorCode::InvalidArgument);

    // Test case 4: Entry with LSN=INVALID_LSN after some entries should get assigned next sequential LSN
    auto entry4 = createTestEntry("key4", "value4", INVALID_LSN, 400, EntryType::Put);
    append_result = wal.Append(entry4);
    VERIFY_RESULT_MSG(append_result, "Should append entry with INVALID_LSN after other entries");
    EXPECT_EQ(2, append_result.value()) << "Fourth entry should get LSN 2";
    EXPECT_EQ(3, wal.current_lsn()) << "Current LSN should be 3";
}

TEST_F(WALTest, BasicOperations) {
    auto _ = fs_->DeleteFiles({"test.wal"});

    WAL<KvEntry> wal(fs_);

    // Open WAL
    auto result = wal.Open("test.wal");
    VERIFY_RESULT_MSG(result, "Should open WAL file");

    // Write entries with LSN=INVALID_LSN, should get assigned sequential LSNs
    auto entry1 = createTestEntry("key1", "value1", INVALID_LSN, 100, EntryType::Put);
    auto append_result = wal.Append(entry1);
    VERIFY_RESULT_MSG(append_result, "Should append first entry");
    EXPECT_EQ(0, append_result.value()) << "First entry should get LSN 0";
    EXPECT_EQ(1, wal.current_lsn()) << "Current LSN should be 1";

    auto entry2 = createTestEntry("key2", "value2", INVALID_LSN, 200, EntryType::Put);
    append_result = wal.Append(entry2);
    VERIFY_RESULT_MSG(append_result, "Should append second entry");
    EXPECT_EQ(1, append_result.value()) << "Second entry should get LSN 1";
    EXPECT_EQ(2, wal.current_lsn()) << "Current LSN should be 2";

    wal.reset_lsn();
    EXPECT_EQ(0, wal.current_lsn()) << "LSN should be reset to 0";

    // Read entries
    auto read_result = wal.Read(0);
    VERIFY_RESULT_MSG(read_result, "Should read entries");
    auto entries = read_result.value();
    EXPECT_EQ(2, entries.size()) << "Should have two entries";
    EXPECT_EQ("key1", entries[0].key) << "First entry key should match";
    EXPECT_EQ(0, entries[0].lsn()) << "First entry LSN should match";
    EXPECT_EQ("key2", entries[1].key) << "Second entry key should match";
    EXPECT_EQ(1, entries[1].lsn()) << "Second entry LSN should match";
}

TEST_F(WALTest, Recovery) {
    auto _ = fs_->DeleteFiles({"recovery.wal"});

    // Write some entries
    {
        WAL<KvEntry> wal(fs_);
        auto result = wal.Open("recovery.wal");
        VERIFY_RESULT_MSG(result, "Should open WAL file");

        for (int i = 0; i < 5; ++i) {
            std::string key = "key" + std::to_string(i);
            std::string value = "value" + std::to_string(i);
            auto entry = createTestEntry(key, value, i, i * 100, EntryType::Put);
            auto append_result = wal.Append(entry);
            VERIFY_RESULT_MSG(append_result, "Should append entry " + std::to_string(i));
        }
    }

    // Recover and verify
    {
        WAL<KvEntry> wal(fs_);
        auto result = wal.Open("recovery.wal", true);  // Open with recovery
        VERIFY_RESULT_MSG(result, "Should open WAL file for recovery");
        EXPECT_EQ(5, wal.current_lsn()) << "Current LSN should be 5 after recovery";

        auto read_result = wal.Read(0);
        VERIFY_RESULT_MSG(read_result, "Should read all entries");
        auto entries = read_result.value();
        EXPECT_EQ(5, entries.size()) << "Should have five entries";

        for (int i = 0; i < 5; ++i) {
            EXPECT_EQ("key" + std::to_string(i), entries[i].key) << "Entry " << i << " key should match";
            EXPECT_EQ(static_cast<uint64_t>(i), entries[i].lsn()) << "Entry " << i << " LSN should match";
            EXPECT_EQ(static_cast<uint64_t>(i * 100), entries[i].ts) << "Entry " << i << " timestamp should match";
        }

        // Verify LSN continuity
        auto new_entry = createTestEntry("key5", "value5", 5, 500, EntryType::Put);
        auto append_result = wal.Append(new_entry);
        VERIFY_RESULT_MSG(append_result, "Should append entry after recovery");
        EXPECT_EQ(6, wal.current_lsn()) << "LSN should be incremented after append";
    }
}

TEST_F(WALTest, PartialRead) {
    auto _ = fs_->DeleteFiles({"partial.wal"});

    // Write entries
    {
        WAL<KvEntry> wal(fs_);
        auto result = wal.Open("partial.wal");
        VERIFY_RESULT_MSG(result, "Should open WAL file");

        for (int i = 0; i < 10; ++i) {
            auto entry =
                createTestEntry("key" + std::to_string(i), "value" + std::to_string(i), i, i * 100, EntryType::Put);
            auto append_result = wal.Append(entry);
            VERIFY_RESULT_MSG(append_result, "Should append entry " + std::to_string(i));
        }
    }

    // Read partial entries
    {
        WAL<KvEntry> wal(fs_);
        auto result = wal.Open("partial.wal");
        VERIFY_RESULT_MSG(result, "Should open WAL file");

        // Read from middle
        auto read_result = wal.Read(5);
        VERIFY_RESULT_MSG(read_result, "Should read entries from LSN 5");
        auto entries = read_result.value();
        EXPECT_EQ(5, entries.size()) << "Should have five entries";

        for (size_t i = 0; i < entries.size(); ++i) {
            size_t expected_lsn = i + 5;
            EXPECT_EQ("key" + std::to_string(expected_lsn), entries[i].key) << "Entry key should match at index " << i;
            EXPECT_EQ(expected_lsn, entries[i].lsn()) << "Entry LSN should match at index " << i;
        }
    }
}

TEST_F(WALTest, CorruptedEntry) {
    auto _ = fs_->DeleteFiles({"corrupted.wal"});

    WAL<KvEntry> wal(fs_);
    auto result = wal.Open("corrupted.wal");
    VERIFY_RESULT_MSG(result, "Should open WAL file");

    // Write a valid entry
    auto entry = createTestEntry("key1", "value1", 0, 100, EntryType::Put);
    auto append_result = wal.Append(entry);
    VERIFY_RESULT_MSG(append_result, "Should append valid entry");

    // Corrupt the file by writing invalid data
    auto corrupt_result = fs_->Append(wal.handle(), DataChunk(10));  // Write zeros
    VERIFY_RESULT_MSG(corrupt_result, "Should append corrupted data");

    // Try to read
    auto read_result = wal.Read(0);
    EXPECT_FALSE(read_result.ok()) << "Should not read valid entry";
}

}  // namespace pond::test