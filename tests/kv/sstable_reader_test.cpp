#include "kv/sstable_reader.h"

#include <gtest/gtest.h>

#include "common/memory_append_only_fs.h"
#include "kv/sstable_writer.h"
#include "test_helper.h"

using namespace pond::common;
using namespace pond::kv;

namespace {

class SSTableReaderTest : public ::testing::Test {
protected:
    void SetUp() override { fs_ = std::make_shared<MemoryAppendOnlyFileSystem>(); }

    // Helper to create DataChunk from string
    DataChunk stringToChunk(const std::string& str) {
        return DataChunk(reinterpret_cast<const uint8_t*>(str.data()), str.size());
    }

    // Helper to create a test SSTable file
    void createTestSSTable(const std::string& path,
                           const std::vector<std::pair<std::string, std::string>>& entries,
                           bool use_filter = false) {
        SSTableWriter writer(fs_, path);
        if (use_filter) {
            writer.EnableFilter(entries.size());
        }
        for (const auto& [key, value] : entries) {
            VERIFY_RESULT(writer.Add(key, stringToChunk(value)));
        }
        VERIFY_RESULT(writer.Finish());
    }

    std::shared_ptr<MemoryAppendOnlyFileSystem> fs_;
};

TEST_F(SSTableReaderTest, BasicOperations) {
    // Create test data
    std::vector<std::pair<std::string, std::string>> entries = {
        {"key1", "value1"},
        {"key2", "value2"},
        {"key3", "value3"},
    };
    createTestSSTable("test.sst", entries);

    // Open reader
    SSTableReader reader(fs_, "test.sst");
    ASSERT_TRUE(reader.Open().ok());

    // Verify metadata
    EXPECT_EQ(reader.GetEntryCount(), 3);
    EXPECT_GT(reader.GetFileSize(), 0);
    EXPECT_EQ(reader.GetSmallestKey(), "key1");
    EXPECT_EQ(reader.GetLargestKey(), "key3");

    // Read existing keys
    for (const auto& [key, value] : entries) {
        auto result = reader.Get(key);
        ASSERT_TRUE(result.ok());
        ASSERT_TRUE(result.value().size() > 0);
        EXPECT_EQ(result.value().toString(), value);
    }

    // Try non-existent key
    auto result = reader.Get("key4");
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.error().code(), ErrorCode::NotFound);
}

TEST_F(SSTableReaderTest, WithBloomFilter) {
    // Create test data with bloom filter
    std::vector<std::pair<std::string, std::string>> entries = {
        {"key1", "value1"},
        {"key2", "value2"},
        {"key3", "value3"},
    };
    createTestSSTable("test.sst", entries, true);

    // Open reader
    SSTableReader reader(fs_, "test.sst");
    ASSERT_TRUE(reader.Open().ok());

    // Check existing keys
    for (const auto& [key, value] : entries) {
        ASSERT_TRUE(reader.MayContain(key).ok());
        EXPECT_TRUE(reader.MayContain(key).value());
    }

    // Check non-existent key
    auto result = reader.MayContain("key4");
    ASSERT_TRUE(result.ok());
    EXPECT_FALSE(result.value());
}

TEST_F(SSTableReaderTest, LargeValues) {
    // Create test data with large values
    std::string large_value(1024 * 1024, 'x');  // 1MB value
    std::vector<std::pair<std::string, std::string>> entries = {
        {"key1", large_value},
        {"key2", large_value},
    };
    createTestSSTable("test.sst", entries);

    // Open reader
    SSTableReader reader(fs_, "test.sst");
    ASSERT_TRUE(reader.Open().ok());

    // Read and verify large values
    for (const auto& [key, value] : entries) {
        auto result = reader.Get(key);
        ASSERT_TRUE(result.ok());
        ASSERT_EQ(result.value().size(), large_value.size());
        EXPECT_EQ(std::string(reinterpret_cast<const char*>(result.value().data()), result.value().size()),
                  large_value);
    }
}

TEST_F(SSTableReaderTest, KeyRange) {
    std::vector<std::pair<std::string, std::string>> entries = {
        {"key1", "value1"},
        {"key2", "value2"},
        {"key3", "value3"},
    };
    createTestSSTable("test.sst", entries);

    SSTableReader reader(fs_, "test.sst");
    ASSERT_TRUE(reader.Open().ok());

    // Try keys outside range
    auto result1 = reader.Get("key0");  // Before smallest
    ASSERT_FALSE(result1.ok());
    EXPECT_EQ(result1.error().code(), ErrorCode::NotFound);

    auto result2 = reader.Get("key4");  // After largest
    ASSERT_FALSE(result2.ok());
    EXPECT_EQ(result2.error().code(), ErrorCode::NotFound);
}

TEST_F(SSTableReaderTest, InvalidFile) {
    // Try to open non-existent file
    SSTableReader reader(fs_, "nonexistent.sst");
    ASSERT_FALSE(reader.Open().ok());

    // Try to read without opening
    auto result = reader.Get("key1");
    ASSERT_FALSE(result.ok());
}

TEST_F(SSTableReaderTest, EmptyKeyValue) {
    // Create test data with empty key/value
    std::vector<std::pair<std::string, std::string>> entries = {
        {"", "empty_key"},
        {"key1", ""},
    };
    createTestSSTable("test.sst", entries);

    SSTableReader reader(fs_, "test.sst");
    ASSERT_TRUE(reader.Open().ok());

    // Read empty key
    auto result1 = reader.Get("");
    ASSERT_TRUE(result1.ok());
    ASSERT_TRUE(result1.value().size() > 0);
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(result1.value().data()), result1.value().size()), "empty_key");

    // Read empty value
    auto result2 = reader.Get("key1");
    ASSERT_TRUE(result2.ok());
    EXPECT_EQ(result2.value().size(), 0);
}

TEST_F(SSTableReaderTest, MultipleBlocks) {
    // Create enough entries to span multiple blocks
    std::vector<std::pair<std::string, std::string>> entries;
    std::string large_value(50 * 1024, 'x');  // 50KB value
    for (int i = 0; i < 100; i++) {           // 100 entries * 50KB = ~5MB total, split across multiple blocks
        entries.push_back({pond::test::GenerateKey(i), large_value + std::to_string(i)});
    }
    createTestSSTable("test.sst", entries);

    SSTableReader reader(fs_, "test.sst");
    ASSERT_TRUE(reader.Open().ok());

    // Read random keys
    for (int i = 0; i < 10; i++) {
        int idx = rand() % entries.size();
        auto result = reader.Get(entries[idx].first);
        ASSERT_TRUE(result.ok());
        ASSERT_TRUE(result.value().size() > 0);
        EXPECT_EQ(std::string(reinterpret_cast<const char*>(result.value().data()), result.value().size()),
                  entries[idx].second);
    }
}

}  // namespace