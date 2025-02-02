// #include "kv/sstable.h"

// #include <memory>
// #include <string>
// #include <vector>

// #include <gtest/gtest.h>

// #include "common/memory_append_only_fs.h"
// #include "common/time.h"
// #include "test_helper.h"

// namespace pond::kv {

// class SSTableTest : public ::testing::Test {
// protected:
//     void SetUp() override {
//         fs_ = std::make_shared<common::MemoryAppendOnlyFileSystem>();
//         sstable_ = std::make_unique<SSTable>(fs_, "test.sst");
//     }

//     KvEntry CreateEntry(const std::string& key, const std::string& value, EntryType type = EntryType::Put) {
//         return KvEntry(key,
//                       common::DataChunk(reinterpret_cast<const uint8_t*>(value.data()), value.size()),
//                       common::INVALID_LSN,
//                       common::now(),
//                       type);
//     }

//     std::shared_ptr<common::MemoryAppendOnlyFileSystem> fs_;
//     std::unique_ptr<SSTable> sstable_;
// };

// TEST_F(SSTableTest, WriteAndReadSingleEntry) {
//     std::vector<KvEntry> entries = {CreateEntry("key1", "value1")};
//     VERIFY_RESULT(sstable_->Write(entries));

//     auto result = sstable_->Get("key1");
//     VERIFY_RESULT(result);
//     EXPECT_EQ(result.value()->key, "key1");
//     EXPECT_EQ(result.value()->type, EntryType::Put);
//     EXPECT_EQ(result.value()->value,
//               common::DataChunk(reinterpret_cast<const uint8_t*>("value1"), 6));
// }

// TEST_F(SSTableTest, WriteAndReadMultipleEntries) {
//     std::vector<KvEntry> entries = {
//         CreateEntry("key1", "value1"),
//         CreateEntry("key2", "value2"),
//         CreateEntry("key3", "value3")
//     };
//     VERIFY_RESULT(sstable_->Write(entries));

//     // Verify all entries can be read
//     for (int i = 1; i <= 3; i++) {
//         std::string key = "key" + std::to_string(i);
//         std::string value = "value" + std::to_string(i);

//         auto result = sstable_->Get(key);
//         VERIFY_RESULT(result);
//         EXPECT_EQ(result.value()->key, key);
//         EXPECT_EQ(result.value()->type, EntryType::Put);
//         EXPECT_EQ(result.value()->value,
//                   common::DataChunk(reinterpret_cast<const uint8_t*>(value.data()), value.size()));
//     }
// }

// TEST_F(SSTableTest, WriteAndReadDeletedEntry) {
//     std::vector<KvEntry> entries = {
//         CreateEntry("key1", "value1"),
//         CreateEntry("key1", "", EntryType::Delete)
//     };
//     VERIFY_RESULT(sstable_->Write(entries));

//     auto result = sstable_->Get("key1");
//     VERIFY_RESULT(result);
//     EXPECT_EQ(result.value()->type, EntryType::Delete);
// }

// TEST_F(SSTableTest, ReadNonExistentKey) {
//     std::vector<KvEntry> entries = {CreateEntry("key1", "value1")};
//     VERIFY_RESULT(sstable_->Write(entries));

//     auto result = sstable_->Get("non_existent_key");
//     EXPECT_FALSE(result.ok());
// }

// TEST_F(SSTableTest, WriteEmptyEntries) {
//     std::vector<KvEntry> entries;
//     VERIFY_RESULT(sstable_->Write(entries));
// }

// TEST_F(SSTableTest, WriteLargeNumberOfEntries) {
//     std::vector<KvEntry> entries;
//     for (int i = 0; i < 1000; i++) {
//         std::string key = "key" + std::to_string(i);
//         std::string value = "value" + std::to_string(i);
//         entries.push_back(CreateEntry(key, value));
//     }
//     VERIFY_RESULT(sstable_->Write(entries));

//     // Verify random entries can be read
//     for (int i = 0; i < 10; i++) {
//         int idx = rand() % 1000;
//         std::string key = "key" + std::to_string(idx);
//         std::string value = "value" + std::to_string(idx);

//         auto result = sstable_->Get(key);
//         VERIFY_RESULT(result);
//         EXPECT_EQ(result.value()->key, key);
//         EXPECT_EQ(result.value()->type, EntryType::Put);
//         EXPECT_EQ(result.value()->value,
//                   common::DataChunk(reinterpret_cast<const uint8_t*>(value.data()), value.size()));
//     }
// }

// }  // namespace pond::kv
