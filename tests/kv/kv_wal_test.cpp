#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "common/memory_append_only_fs.h"
#include "common/time.h"
#include "common/wal.h"
#include "kv/kv_entry.h"
#include "test_helper.h"

namespace pond::kv {

class KVWALTest : public ::testing::Test {
protected:
    void SetUp() override {
        fs_ = std::make_shared<common::MemoryAppendOnlyFileSystem>();
        wal_ = std::make_shared<common::WAL<KvEntry>>(fs_);
        ASSERT_TRUE(wal_->Open("test.wal").ok());
    }

    std::shared_ptr<common::MemoryAppendOnlyFileSystem> fs_;
    std::shared_ptr<common::WAL<KvEntry>> wal_;
};

TEST_F(KVWALTest, BasicOperations) {
    // Write some entries
    KvEntry entry1("key1",
                   common::DataChunk(reinterpret_cast<const uint8_t*>("value1"), 6),
                   common::INVALID_LSN,
                   common::now(),
                   EntryType::Put);
    KvEntry entry2("key2",
                   common::DataChunk(reinterpret_cast<const uint8_t*>("value2"), 6),
                   common::INVALID_LSN,
                   common::now(),
                   EntryType::Put);
    KvEntry entry3("key1", common::DataChunk(), common::INVALID_LSN, common::now(), EntryType::Delete);

    VERIFY_RESULT(wal_->Append(entry1));
    VERIFY_RESULT(wal_->Append(entry2));
    VERIFY_RESULT(wal_->Append(entry3));

    // Read back entries
    auto entries = wal_->Read(0);
    VERIFY_RESULT(entries);
    ASSERT_EQ(entries.value().size(), 3);

    // Verify entries
    const auto& read_entry1 = entries.value()[0];
    EXPECT_EQ(read_entry1.key, "key1");
    EXPECT_EQ(read_entry1.type, EntryType::Put);
    EXPECT_EQ(read_entry1.value, common::DataChunk(reinterpret_cast<const uint8_t*>("value1"), 6));

    const auto& read_entry2 = entries.value()[1];
    EXPECT_EQ(read_entry2.key, "key2");
    EXPECT_EQ(read_entry2.type, EntryType::Put);
    EXPECT_EQ(read_entry2.value, common::DataChunk(reinterpret_cast<const uint8_t*>("value2"), 6));

    const auto& read_entry3 = entries.value()[2];
    EXPECT_EQ(read_entry3.key, "key1");
    EXPECT_EQ(read_entry3.type, EntryType::Delete);
    EXPECT_EQ(read_entry3.value.Size(), 0);
}

TEST_F(KVWALTest, ReadFromOffset) {
    // Write some entries
    KvEntry entry1("key1",
                   common::DataChunk(reinterpret_cast<const uint8_t*>("value1"), 6),
                   common::INVALID_LSN,
                   common::now(),
                   EntryType::Put);
    KvEntry entry2("key2",
                   common::DataChunk(reinterpret_cast<const uint8_t*>("value2"), 6),
                   common::INVALID_LSN,
                   common::now(),
                   EntryType::Put);

    auto result1 = wal_->Append(entry1);
    VERIFY_RESULT(result1);

    auto result2 = wal_->Append(entry2);
    VERIFY_RESULT(result2);

    auto start_lsn = result2.value();

    // Read from first entry's LSN
    auto entries = wal_->Read(start_lsn);
    VERIFY_RESULT(entries);
    ASSERT_EQ(entries.value().size(), 1);  // Should only get the second entry

    const auto& read_entry = entries.value()[0];
    EXPECT_EQ(read_entry.key, "key2");
    EXPECT_EQ(read_entry.type, EntryType::Put);
    EXPECT_EQ(read_entry.value, common::DataChunk(reinterpret_cast<const uint8_t*>("value2"), 6));
}

TEST_F(KVWALTest, ErrorHandling) {
    // Try to read from non-existent WAL
    auto invalid_wal = std::make_shared<common::WAL<KvEntry>>(fs_);
    auto result = invalid_wal->Read(0);
    EXPECT_FALSE(result.ok());

    // Try to append to non-existent WAL
    KvEntry entry("key1", common::DataChunk(), common::INVALID_LSN, common::now(), EntryType::Put);
    auto append_result = invalid_wal->Append(entry);
    EXPECT_FALSE(append_result.ok());
}

}  // namespace pond::kv
