#include "kv/sstable_reader.h"

#include <random>

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
    DataChunk stringToChunk(const std::string& str) { return DataChunk::fromString(str); }

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
    VERIFY_RESULT(reader.Open());

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
    VERIFY_RESULT(reader.Open());

    // Check existing keys
    for (const auto& [key, value] : entries) {
        VERIFY_RESULT(reader.MayContain(key));
        EXPECT_TRUE(reader.MayContain(key).value());
    }

    // Check non-existent key
    auto result = reader.MayContain("key4");
    VERIFY_RESULT(result);
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
    VERIFY_RESULT(reader.Open());

    // Read and verify large values
    for (const auto& [key, value] : entries) {
        auto result = reader.Get(key);
        VERIFY_RESULT(result);
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
    VERIFY_RESULT(reader.Open());

    // Try keys outside range
    auto result1 = reader.Get("key0");  // Before smallest
    EXPECT_FALSE(result1.ok());
    EXPECT_EQ(result1.error().code(), ErrorCode::NotFound);

    auto result2 = reader.Get("key4");  // After largest
    EXPECT_FALSE(result2.ok());
    EXPECT_EQ(result2.error().code(), ErrorCode::NotFound);
}

TEST_F(SSTableReaderTest, InvalidFile) {
    // Try to open non-existent file
    SSTableReader reader(fs_, "nonexistent.sst");
    EXPECT_FALSE(reader.Open().ok());

    // Try to read without opening
    auto result = reader.Get("key1");
    EXPECT_FALSE(result.ok());
    EXPECT_EQ(result.error().code(), ErrorCode::InvalidOperation);
}

TEST_F(SSTableReaderTest, EmptyKeyValue) {
    // Create test data with empty key/value
    std::vector<std::pair<std::string, std::string>> entries = {
        {"", "empty_key"},
        {"key1", ""},
    };
    createTestSSTable("test.sst", entries);

    SSTableReader reader(fs_, "test.sst");
    VERIFY_RESULT(reader.Open());

    // Read empty key
    auto result1 = reader.Get("");
    VERIFY_RESULT(result1);
    ASSERT_TRUE(result1.value().size() > 0);
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(result1.value().data()), result1.value().size()), "empty_key");

    // Read empty value
    auto result2 = reader.Get("key1");
    VERIFY_RESULT(result2);
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
    VERIFY_RESULT(reader.Open());

    // Read random keys
    for (int i = 0; i < 10; i++) {
        int idx = rand() % entries.size();
        auto result = reader.Get(entries[idx].first);
        VERIFY_RESULT(result);
        ASSERT_TRUE(result.value().size() > 0);
        EXPECT_EQ(std::string(reinterpret_cast<const char*>(result.value().data()), result.value().size()),
                  entries[idx].second);
    }
}

TEST_F(SSTableReaderTest, WriterReaderInteraction) {
    // Create a writer
    SSTableWriter writer(fs_, "test.sst");
    writer.EnableFilter(100);  // Enable bloom filter with expected keys

    // Write data in chunks to test incremental writing
    for (int i = 0; i < 10; i++) {
        std::string key = pond::test::GenerateKey(i);
        std::string value = "value" + std::to_string(i);
        VERIFY_RESULT(writer.Add(key, stringToChunk(value)));
    }
    VERIFY_RESULT(writer.Finish());

    // Create multiple readers to test concurrent access
    std::vector<std::unique_ptr<SSTableReader>> readers;
    for (int i = 0; i < 3; i++) {
        auto reader = std::make_unique<SSTableReader>(fs_, "test.sst");
        VERIFY_RESULT(reader->Open());
        readers.push_back(std::move(reader));
    }

    // Test concurrent reads from different readers
    for (int i = 0; i < 10; i++) {
        std::string key = pond::test::GenerateKey(i);
        std::string expected_value = "value" + std::to_string(i);

        for (const auto& reader : readers) {
            auto result = reader->Get(key);
            VERIFY_RESULT(result);
            EXPECT_EQ(result.value().toString(), expected_value);
        }
    }
}

TEST_F(SSTableReaderTest, WriterReaderStress) {
    const int kNumEntries = 1000;
    const int kValueSize = 1024;  // 1KB values

    // Create test data
    std::vector<std::pair<std::string, std::string>> entries;
    std::string value_template(kValueSize, 'x');
    for (int i = 0; i < kNumEntries; i++) {
        std::string key = pond::test::GenerateKey(i);
        std::string value = value_template + std::to_string(i);
        entries.push_back({key, value});
    }

    // Write data
    SSTableWriter writer(fs_, "test.sst");
    writer.EnableFilter(kNumEntries);
    for (const auto& [key, value] : entries) {
        VERIFY_RESULT(writer.Add(key, stringToChunk(value)));
    }
    VERIFY_RESULT(writer.Finish());

    // Create reader
    SSTableReader reader(fs_, "test.sst");
    VERIFY_RESULT(reader.Open());

    // Verify metadata
    EXPECT_EQ(reader.GetEntryCount(), kNumEntries);
    EXPECT_EQ(reader.GetSmallestKey(), entries.front().first);
    EXPECT_EQ(reader.GetLargestKey(), entries.back().first);

    // Random access pattern
    std::vector<int> indices(kNumEntries);
    std::iota(indices.begin(), indices.end(), 0);
    std::random_device rd;
    std::mt19937 gen(rd());
    std::shuffle(indices.begin(), indices.end(), gen);

    for (int idx : indices) {
        const auto& [key, expected_value] = entries[idx];

        // Test bloom filter first
        auto contains = reader.MayContain(key);
        VERIFY_RESULT(contains);
        EXPECT_TRUE(contains.value());

        // Then read the actual value
        auto result = reader.Get(key);
        VERIFY_RESULT(result);
        EXPECT_EQ(result.value().toString(), expected_value);
    }
}

TEST_F(SSTableReaderTest, WriterReaderEdgeCases) {
    SSTableWriter writer(fs_, "test.sst");

    // Test writing special characters in keys
    std::vector<std::pair<std::string, std::string>> special_entries = {
        {"\0key1", "value1"},                              // Null byte in key
        {"key\n2", "value2"},                              // Newline in key
        {std::string("key") + char(255) + "3", "value3"},  // High byte in key
        {std::string(1000, 'k'), "value4"},                // Very long key
    };

    for (const auto& [key, value] : special_entries) {
        VERIFY_RESULT(writer.Add(key, stringToChunk(value)));
    }
    VERIFY_RESULT(writer.Finish());

    // Read and verify
    SSTableReader reader(fs_, "test.sst");
    VERIFY_RESULT(reader.Open());

    for (const auto& [key, expected_value] : special_entries) {
        auto result = reader.Get(key);
        VERIFY_RESULT(result);
        EXPECT_EQ(result.value().toString(), expected_value);
    }
}

TEST_F(SSTableReaderTest, WriterReaderCompression) {
    // This test is a placeholder for when compression is implemented
    // It should test writing and reading compressed blocks
    // For now, verify uncompressed operation works correctly
    std::vector<std::pair<std::string, std::string>> entries = {
        {"key1", std::string(1000, 'a')},  // Highly compressible data
        {"key2", std::string(1000, 'b')},
        {"key3", std::string(1000, 'c')},
    };

    SSTableWriter writer(fs_, "test.sst");
    for (const auto& [key, value] : entries) {
        VERIFY_RESULT(writer.Add(key, stringToChunk(value)));
    }
    VERIFY_RESULT(writer.Finish());

    SSTableReader reader(fs_, "test.sst");
    VERIFY_RESULT(reader.Open());

    // Verify metadata block
    auto metadata_result = reader.GetMetadata();
    VERIFY_RESULT(metadata_result);
    const auto& metadata = metadata_result.value();

    // Verify stats
    EXPECT_EQ(metadata.stats.key_count, 3);
    EXPECT_EQ(metadata.stats.smallest_key, "key1");
    EXPECT_EQ(metadata.stats.largest_key, "key3");
    EXPECT_EQ(metadata.stats.total_key_size, 12);      // 4 bytes per key (key1, key2, key3)
    EXPECT_EQ(metadata.stats.total_value_size, 3000);  // 1000 bytes per value

    // Verify properties
    EXPECT_EQ(metadata.props.compression_type, 0);  // No compression yet
    EXPECT_EQ(metadata.props.index_type, 0);        // Binary search index
    EXPECT_GT(metadata.props.creation_time, 0);     // Should have valid timestamp

    // Verify data is readable and correct
    for (const auto& [key, expected_value] : entries) {
        auto result = reader.Get(key);
        VERIFY_RESULT(result);
        EXPECT_EQ(result.value().toString(), expected_value);
    }
}

}  // namespace