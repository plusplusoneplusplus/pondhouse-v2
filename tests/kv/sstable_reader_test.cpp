#include "kv/sstable_reader.h"

#include <random>
#include <thread>

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
    DataChunk stringToChunk(const std::string& str) { return DataChunk::FromString(str); }

    // Helper to create a test SSTable file
    void createTestSSTable(const std::string& path,
                           const std::vector<std::pair<std::string, std::string>>& entries,
                           bool use_filter = false) {
        SSTableWriter writer(fs_, path);
        if (use_filter) {
            writer.EnableFilter(entries.size());
        }

        for (const auto& [key, value] : entries) {
            VERIFY_RESULT(writer.Add(key, stringToChunk(value), GetNextHybridTime()));
        }
        VERIFY_RESULT(writer.Finish());
    }

    std::shared_ptr<MemoryAppendOnlyFileSystem> fs_;
};

//
// Test Setup:
//      Create a simple SSTable with basic key-value pairs
//      Perform basic read operations and metadata checks
// Test Result:
//      Verify basic read operations work correctly
//      Confirm metadata values are accurate
//      Check error handling for non-existent keys
//
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
        ASSERT_TRUE(result.value().Size() > 0);
        EXPECT_EQ(result.value().ToString(), value);
    }

    // Try non-existent key
    auto result = reader.Get("key4");
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.error().code(), ErrorCode::NotFound);
}

//
// Test Setup:
//      Create an SSTable with bloom filter enabled
//      Test key existence checks using the filter
// Test Result:
//      Verify bloom filter correctly identifies existing keys
//      Confirm bloom filter correctly filters non-existent keys
//
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

//
// Test Setup:
//      Create an SSTable with large values (1MB each)
//      Test reading and verifying large value integrity
// Test Result:
//      Verify large values are read correctly
//      Confirm data integrity for large values
//
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
        ASSERT_EQ(result.value().Size(), large_value.size());
        EXPECT_EQ(std::string(reinterpret_cast<const char*>(result.value().Data()), result.value().Size()),
                  large_value);
    }
}

//
// Test Setup:
//      Create an SSTable and test key range boundaries
//      Attempt reads outside the key range
// Test Result:
//      Verify keys outside range return NotFound
//      Confirm proper error handling at range boundaries
//
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

//
// Test Setup:
//      Test error handling for invalid file operations
//      Attempt operations on non-existent files
// Test Result:
//      Verify proper error handling for missing files
//      Confirm operations on unopened readers fail appropriately
//
TEST_F(SSTableReaderTest, InvalidFile) {
    // Try to open non-existent file
    SSTableReader reader(fs_, "nonexistent.sst");
    EXPECT_FALSE(reader.Open().ok());

    // Try to read without opening
    auto result = reader.Get("key1");
    EXPECT_FALSE(result.ok());
    EXPECT_EQ(result.error().code(), ErrorCode::InvalidOperation);
}

//
// Test Setup:
//      Create an SSTable with empty keys and values
//      Test handling of edge cases with empty data
// Test Result:
//      Verify empty keys can be stored and retrieved
//      Confirm empty values are handled correctly
//
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
    ASSERT_TRUE(result1.value().Size() > 0);
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(result1.value().Data()), result1.value().Size()), "empty_key");

    // Read empty value
    auto result2 = reader.Get("key1");
    VERIFY_RESULT(result2);
    EXPECT_EQ(result2.value().Size(), 0);
}

//
// Test Setup:
//      Create an SSTable with multiple data blocks
//      Test random access across block boundaries
// Test Result:
//      Verify reads across multiple blocks work correctly
//      Confirm data integrity for cross-block reads
//
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
        ASSERT_TRUE(result.value().Size() > 0);
        EXPECT_EQ(std::string(reinterpret_cast<const char*>(result.value().Data()), result.value().Size()),
                  entries[idx].second);
    }
}

//
// Test Setup:
//      Create multiple readers for the same SSTable
//      Test concurrent read operations
// Test Result:
//      Verify concurrent reads work correctly
//      Confirm data consistency across readers
//
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
            EXPECT_EQ(result.value().ToString(), expected_value);
        }
    }
}

//
// Test Setup:
//      Create an SSTable with many entries
//      Test concurrent read operations under stress
// Test Result:
//      Verify data consistency under stress
//      Confirm no data corruption during concurrent access
//
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
        EXPECT_EQ(result.value().ToString(), expected_value);
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
        EXPECT_EQ(result.value().ToString(), expected_value);
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
        EXPECT_EQ(result.value().ToString(), expected_value);
    }
}

TEST_F(SSTableReaderTest, IteratorBasicOperations) {
    // Create test data with sorted keys
    std::vector<std::pair<std::string, std::string>> entries = {
        {"key1", "value1"},
        {"key2", "value2"},
        {"key3", "value3"},
        {"key4", "value4"},
        {"key5", "value5"},
    };
    createTestSSTable("test.sst", entries);

    // Open reader and create iterator
    SSTableReader reader(fs_, "test.sst");
    VERIFY_RESULT(reader.Open());
    auto iter = reader.NewIterator();

    // Test SeekToFirst and sequential scan
    iter->SeekToFirst();
    size_t count = 0;
    while (iter->Valid()) {
        ASSERT_LT(count, entries.size());
        EXPECT_EQ(iter->key(), entries[count].first);
        EXPECT_EQ(iter->value().ToString(), entries[count].second);
        iter->Next();
        count++;
    }
    EXPECT_EQ(count, entries.size());
}

TEST_F(SSTableReaderTest, IteratorSeek) {
    // Create test data
    std::vector<std::pair<std::string, std::string>> entries;
    for (int i = 0; i < 100; i++) {
        std::string key = pond::test::GenerateKey(i * 2);  // Even numbered keys
        entries.push_back({key, "value" + std::to_string(i)});
    }
    createTestSSTable("test.sst", entries);

    SSTableReader reader(fs_, "test.sst");
    VERIFY_RESULT(reader.Open());
    auto iter = reader.NewIterator();

    // Test seeking to exact keys
    auto key = pond::test::GenerateKey(50);
    iter->Seek(key);
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(iter->key(), key);

    // Test seeking between keys (should land on next existing key)
    key = pond::test::GenerateKey(51);
    iter->Seek(key);
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(iter->key(), pond::test::GenerateKey(52));

    // Test seeking past all keys
    key = pond::test::GenerateKey(999);
    iter->Seek(key);
    EXPECT_FALSE(iter->Valid());

    // Test seeking to first key
    iter->Seek("");
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(iter->key(), entries.front().first);
}

TEST_F(SSTableReaderTest, IteratorMultipleBlocks) {
    // Create enough entries to span multiple blocks
    std::vector<std::pair<std::string, std::string>> entries;
    std::string large_value(50 * 1024, 'x');  // 50KB value to force block splits
    for (int i = 0; i < 100; i++) {
        entries.push_back({pond::test::GenerateKey(i), large_value + std::to_string(i)});
    }
    createTestSSTable("test.sst", entries);

    SSTableReader reader(fs_, "test.sst");
    VERIFY_RESULT(reader.Open());
    auto iter = reader.NewIterator();

    // Verify sequential scan across blocks
    iter->SeekToFirst();
    size_t count = 0;
    while (iter->Valid()) {
        ASSERT_LT(count, entries.size());
        EXPECT_EQ(iter->key(), entries[count].first);
        EXPECT_EQ(iter->value().ToString(), entries[count].second);
        iter->Next();
        count++;
    }
    EXPECT_EQ(count, entries.size());

    // Test seeking within different blocks
    std::vector<size_t> test_positions = {0, 25, 50, 75, 99};  // Test across different blocks
    for (size_t pos : test_positions) {
        iter->Seek(entries[pos].first);
        ASSERT_TRUE(iter->Valid());
        EXPECT_EQ(iter->key(), entries[pos].first);
        EXPECT_EQ(iter->value().ToString(), entries[pos].second);
    }
}

TEST_F(SSTableReaderTest, IteratorEdgeCases) {
    // Test single entry
    {
        std::vector<std::pair<std::string, std::string>> single_entry = {{"key1", "value1"}};
        createTestSSTable("single.sst", single_entry);

        SSTableReader reader(fs_, "single.sst");
        VERIFY_RESULT(reader.Open());
        auto iter = reader.NewIterator();

        iter->SeekToFirst();
        ASSERT_TRUE(iter->Valid());
        EXPECT_EQ(iter->key(), "key1");
        EXPECT_EQ(iter->value().ToString(), "value1");

        iter->Next();
        EXPECT_FALSE(iter->Valid());
    }

    // Test special keys
    {
        std::vector<std::pair<std::string, std::string>> special_entries = {
            {"", "empty_key"},                              // Empty key
            {"key\0with\0nulls", "value1"},                 // Key with null bytes
            {"key\nwith\nnewlines", "value2"},              // Key with newlines
            {std::string("key") + char(255), "high_byte"},  // Key with high bytes
            {std::string(1000, 'k'), "long_key_value"},     // Very long key
        };
        createTestSSTable("special.sst", special_entries);

        SSTableReader reader(fs_, "special.sst");
        VERIFY_RESULT(reader.Open());
        auto iter = reader.NewIterator();

        iter->SeekToFirst();
        size_t count = 0;
        while (iter->Valid()) {
            ASSERT_LT(count, special_entries.size());
            EXPECT_EQ(iter->key(), special_entries[count].first);
            EXPECT_EQ(iter->value().ToString(), special_entries[count].second);
            iter->Next();
            count++;
        }
        EXPECT_EQ(count, special_entries.size());
    }
}

TEST_F(SSTableReaderTest, IteratorInvalidOperations) {
    std::vector<std::pair<std::string, std::string>> entries = {
        {"key1", "value1"},
        {"key2", "value2"},
    };
    createTestSSTable("test.sst", entries);

    SSTableReader reader(fs_, "test.sst");
    VERIFY_RESULT(reader.Open());
    auto iter = reader.NewIterator();

    // Test Next() on invalid iterator
    iter->Seek("key3");  // Seek past all entries
    EXPECT_FALSE(iter->Valid());

    // Test seeking on closed reader
    SSTableReader closed_reader(fs_, "nonexistent.sst");
    auto closed_iter = closed_reader.NewIterator();
    closed_iter->SeekToFirst();
    EXPECT_FALSE(closed_iter->Valid());
}

TEST_F(SSTableReaderTest, IteratorConcurrentAccess) {
    // Create test data
    std::vector<std::pair<std::string, std::string>> entries;
    for (int i = 0; i < 1000; i++) {
        entries.push_back({pond::test::GenerateKey(i), "value" + std::to_string(i)});
    }
    createTestSSTable("test.sst", entries);

    SSTableReader reader(fs_, "test.sst");
    VERIFY_RESULT(reader.Open());

    // Create multiple iterators and use them concurrently
    constexpr int kNumThreads = 4;
    std::vector<std::thread> threads;
    std::atomic<size_t> success_count{0};

    for (int i = 0; i < kNumThreads; i++) {
        threads.emplace_back([&reader, &entries, &success_count, i]() {
            auto iter = reader.NewIterator();

            // Each thread starts at a different position
            size_t start_pos = (entries.size() / kNumThreads) * i;
            iter->Seek(entries[start_pos].first);

            size_t count = 0;
            while (iter->Valid() && count < entries.size() / kNumThreads) {
                size_t expected_pos = (start_pos + count) % entries.size();
                if (iter->key() == entries[expected_pos].first
                    && iter->value().ToString() == entries[expected_pos].second) {
                    success_count++;
                }
                iter->Next();
                count++;
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    EXPECT_EQ(success_count, entries.size());
}

TEST_F(SSTableReaderTest, RangeBasedForLoop) {
    // Create test data with sorted keys
    std::vector<std::pair<std::string, std::string>> entries = {
        {"key1", "value1"},
        {"key2", "value2"},
        {"key3", "value3"},
        {"key4", "value4"},
        {"key5", "value5"},
    };
    createTestSSTable("test.sst", entries);

    // Open reader
    SSTableReader reader(fs_, "test.sst");
    VERIFY_RESULT(reader.Open());

    // Test range-based for loop
    size_t count = 0;
    for (const auto& [key, value] : reader) {
        ASSERT_LT(count, entries.size());
        EXPECT_EQ(key, entries[count].first);
        EXPECT_EQ(value.ToString(), entries[count].second);
        count++;
    }
    EXPECT_EQ(count, entries.size());
}

TEST_F(SSTableReaderTest, IteratorSTLCompatibility) {
    // Create test data
    std::vector<std::pair<std::string, std::string>> entries = {
        {"key1", "value1"},
        {"key2", "value2"},
    };

    createTestSSTable("test.sst", entries);

    // Open reader
    SSTableReader reader(fs_, "test.sst");
    VERIFY_RESULT(reader.Open());

    // Test STL iterator interface
    {
        auto iter = reader.begin();
        auto end = reader.end();
        for (auto it = iter; it != end; ++it) {
            EXPECT_NE((*it).first, "");
            EXPECT_NE((*it).second.Size(), 0);
        }
    }
}

//
// Test Setup:
//      Create an SSTable with multiple versions of keys
//      Test iteration with different timestamps
// Test Result:
//      Verify correct version visibility at different timestamps
//      Confirm proper version ordering during iteration
//
TEST_F(SSTableReaderTest, IteratorWithTimestamp) {
    // Create test data with multiple versions of keys
    SSTableWriter writer(fs_, "test.sst");

    HybridTime t1(100);
    HybridTime t2(200);
    HybridTime t3(300);

    // Add entries with different versions
    VERIFY_RESULT(writer.Add("key1", stringToChunk("value1_v3"), t3));
    VERIFY_RESULT(writer.Add("key1", stringToChunk("value1_v2"), t2));
    VERIFY_RESULT(writer.Add("key1", stringToChunk("value1_v1"), t1));
    VERIFY_RESULT(writer.Add("key2", stringToChunk("value2_v2"), t2));
    VERIFY_RESULT(writer.Add("key2", stringToChunk("value2_v1"), t1));
    VERIFY_RESULT(writer.Add("key3", stringToChunk("value3_v1"), t1));
    VERIFY_RESULT(writer.Finish());

    SSTableReader reader(fs_, "test.sst");
    VERIFY_RESULT(reader.Open());

    // Test reading at different timestamps
    {
        // Read at t1 should see first versions
        auto iter = reader.NewIterator(t1);
        ASSERT_TRUE(iter->Valid());
        EXPECT_EQ(iter->key(), "key1");
        EXPECT_EQ(iter->value().ToString(), "value1_v1");
        EXPECT_EQ(iter->version(), t1);

        iter->Next();
        ASSERT_TRUE(iter->Valid());
        EXPECT_EQ(iter->key(), "key2");
        EXPECT_EQ(iter->value().ToString(), "value2_v1");
        EXPECT_EQ(iter->version(), t1);

        iter->Next();
        ASSERT_TRUE(iter->Valid());
        EXPECT_EQ(iter->key(), "key3");
        EXPECT_EQ(iter->value().ToString(), "value3_v1");
        EXPECT_EQ(iter->version(), t1);

        iter->Next();
        EXPECT_FALSE(iter->Valid());
    }

    {
        // Read at t2 should see second versions where available
        auto iter = reader.NewIterator(t2);
        ASSERT_TRUE(iter->Valid());
        EXPECT_EQ(iter->key(), "key1");
        EXPECT_EQ(iter->value().ToString(), "value1_v2");
        EXPECT_EQ(iter->version(), t2);

        iter->Next();
        ASSERT_TRUE(iter->Valid());
        EXPECT_EQ(iter->key(), "key2");
        EXPECT_EQ(iter->value().ToString(), "value2_v2");
        EXPECT_EQ(iter->version(), t2);

        iter->Next();
        ASSERT_TRUE(iter->Valid());
        EXPECT_EQ(iter->key(), "key3");
        EXPECT_EQ(iter->value().ToString(), "value3_v1");  // Only has version at t1
        EXPECT_EQ(iter->version(), t1);

        iter->Next();
        EXPECT_FALSE(iter->Valid());
    }

    {
        // Read at t3 should see latest versions
        auto iter = reader.NewIterator(t3);
        ASSERT_TRUE(iter->Valid());
        EXPECT_EQ(iter->key(), "key1");
        EXPECT_EQ(iter->value().ToString(), "value1_v3");
        EXPECT_EQ(iter->version(), t3);

        iter->Next();
        ASSERT_TRUE(iter->Valid());
        EXPECT_EQ(iter->key(), "key2");
        EXPECT_EQ(iter->value().ToString(), "value2_v2");  // Latest version at t2
        EXPECT_EQ(iter->version(), t2);

        iter->Next();
        ASSERT_TRUE(iter->Valid());
        EXPECT_EQ(iter->key(), "key3");
        EXPECT_EQ(iter->value().ToString(), "value3_v1");  // Only has version at t1
        EXPECT_EQ(iter->version(), t1);

        iter->Next();
        EXPECT_FALSE(iter->Valid());
    }

    {
        // Read with MaxHybridTime should see latest versions
        auto iter = reader.NewIterator();
        ASSERT_TRUE(iter->Valid());
        EXPECT_EQ(iter->key(), "key1");
        EXPECT_EQ(iter->value().ToString(), "value1_v3");
        EXPECT_EQ(iter->version(), t3);

        iter->Next();
        ASSERT_TRUE(iter->Valid());
        EXPECT_EQ(iter->key(), "key2");
        EXPECT_EQ(iter->value().ToString(), "value2_v2");
        EXPECT_EQ(iter->version(), t2);

        iter->Next();
        ASSERT_TRUE(iter->Valid());
        EXPECT_EQ(iter->key(), "key3");
        EXPECT_EQ(iter->value().ToString(), "value3_v1");
        EXPECT_EQ(iter->version(), t1);

        iter->Next();
        EXPECT_FALSE(iter->Valid());
    }
}

//
// Test Setup:
//      Create an SSTable with multiple versions of keys at different timestamps
//      Test seeking behavior with different timestamps
// Test Result:
//      Verify correct version visibility at different timestamps
//      Confirm proper seek behavior for existing and non-existent keys
//
TEST_F(SSTableReaderTest, IteratorSeekWithTimestamp) {
    // Create test data with multiple versions
    SSTableWriter writer(fs_, "test.sst");

    HybridTime t1(100);
    HybridTime t2(200);

    // Add entries with different versions
    VERIFY_RESULT(writer.Add("key1", stringToChunk("value1_v2"), t2));
    VERIFY_RESULT(writer.Add("key1", stringToChunk("value1_v1"), t1));
    VERIFY_RESULT(writer.Add("key2", stringToChunk("value2_v1"), t1));
    VERIFY_RESULT(writer.Add("key3", stringToChunk("value3_v2"), t2));
    VERIFY_RESULT(writer.Add("key3", stringToChunk("value3_v1"), t1));
    VERIFY_RESULT(writer.Finish());

    SSTableReader reader(fs_, "test.sst");
    VERIFY_RESULT(reader.Open());

    // Test seeking at different timestamps
    {
        // Seek at t1
        auto iter = reader.NewIterator(t1);

        iter->Seek("key2");
        ASSERT_TRUE(iter->Valid());
        EXPECT_EQ(iter->key(), "key2");
        EXPECT_EQ(iter->value().ToString(), "value2_v1");
        EXPECT_EQ(iter->version(), t1);

        iter->Seek("key3");
        ASSERT_TRUE(iter->Valid());
        EXPECT_EQ(iter->key(), "key3");
        EXPECT_EQ(iter->value().ToString(), "value3_v1");
        EXPECT_EQ(iter->version(), t1);

        // Seek to non-existent key should land on next key
        iter->Seek("key1.5");
        ASSERT_TRUE(iter->Valid());
        EXPECT_EQ(iter->key(), "key2");
        EXPECT_EQ(iter->value().ToString(), "value2_v1");
        EXPECT_EQ(iter->version(), t1);
    }

    {
        // Seek at t2
        auto iter = reader.NewIterator(t2);

        iter->Seek("key1");
        ASSERT_TRUE(iter->Valid());
        EXPECT_EQ(iter->key(), "key1");
        EXPECT_EQ(iter->value().ToString(), "value1_v2");
        EXPECT_EQ(iter->version(), t2);

        iter->Seek("key3");
        ASSERT_TRUE(iter->Valid());
        EXPECT_EQ(iter->key(), "key3");
        EXPECT_EQ(iter->value().ToString(), "value3_v2");
        EXPECT_EQ(iter->version(), t2);

        // Seek past all keys
        iter->Seek("key4");
        EXPECT_FALSE(iter->Valid());
    }
}

//
// Test Setup:
//      Create an SSTable with multiple versions of keys
//      Test range-based for loop iteration
// Test Result:
//      Verify correct iteration over latest versions
//      Confirm proper handling of multiple versions in for-loop
//
TEST_F(SSTableReaderTest, IteratorRangeBasedForWithTimestamp) {
    // Create test data with multiple versions
    SSTableWriter writer(fs_, "test.sst");

    HybridTime t1(100);
    HybridTime t2(200);

    VERIFY_RESULT(writer.Add("key1", stringToChunk("value1_v2"), t2));
    VERIFY_RESULT(writer.Add("key1", stringToChunk("value1_v1"), t1));
    VERIFY_RESULT(writer.Add("key2", stringToChunk("value2_v2"), t2));
    VERIFY_RESULT(writer.Add("key2", stringToChunk("value2_v1"), t1));
    VERIFY_RESULT(writer.Finish());

    SSTableReader reader(fs_, "test.sst");
    VERIFY_RESULT(reader.Open());

    // Test range-based for loop at t1
    {
        std::vector<std::tuple<std::string, std::string, HybridTime>> expected = {
            {"key1", "value1_v2", t2},
            {"key2", "value2_v2", t2},
        };

        size_t count = 0;
        for (const auto& [key, value] : reader) {  // Uses MaxHybridTime by default
            ASSERT_LT(count, expected.size());
            EXPECT_EQ(key, std::get<0>(expected[count]));
            EXPECT_EQ(value.ToString(), std::get<1>(expected[count]));
            count++;
        }
        EXPECT_EQ(count, expected.size());
    }
}

//
// Test Setup:
//      Create an SSTable with multiple entries and a bloom filter
//      Read data in various patterns to track bytes read
// Test Result:
//      Verify bytes read increases appropriately for different operations
//      Confirm bloom filter prevents unnecessary disk reads
//
TEST_F(SSTableReaderTest, BytesReadTracking) {
    // Create test data with bloom filter
    std::vector<std::pair<std::string, std::string>> entries;

    std::string value(1024 * 10,
                      'x');  // 10KB value, to make sure we have multiple blocks (default data block size is 4MB)

    for (int i = 0; i < 500; i++) {
        entries.push_back({pond::test::GenerateKey(i), value + std::to_string(i)});
    }

    createTestSSTable("test.sst", entries, true);

    // Test bytes read during Open()
    SSTableReader reader(fs_, "test.sst");
    VERIFY_RESULT(reader.Open());
    size_t bytes_after_open = reader.GetBytesRead();
    EXPECT_GT(bytes_after_open, 0) << "Should have read some bytes during Open()";

    // Test bytes read during Get()
    auto result1 = reader.Get(entries[0].first);
    VERIFY_RESULT(result1);
    size_t bytes_after_first_get = reader.GetBytesRead();
    EXPECT_GT(bytes_after_first_get, bytes_after_open) << "Should have read more bytes after Get()";

    // Test bytes read during MayContain() (should not increase since we have filter data in memory)
    VERIFY_RESULT(reader.MayContain(entries[1].first));
    size_t bytes_after_may_contain = reader.GetBytesRead();
    EXPECT_EQ(bytes_after_may_contain, bytes_after_first_get)
        << "MayContain should not read more bytes with loaded filter";

    // Test bytes read during iteration
    {
        auto iter = reader.NewIterator();
        iter->SeekToFirst();
        size_t bytes_after_seek = reader.GetBytesRead();
        EXPECT_GT(bytes_after_seek, bytes_after_may_contain) << "Should have read more bytes after seek";

        while (iter->Valid()) {
            iter->Next();
        }
        size_t bytes_after_iteration = reader.GetBytesRead();
        EXPECT_GT(bytes_after_iteration, bytes_after_seek) << "Should have read more bytes after full iteration";
    }
}

//
// Test Setup:
//      Create an SSTable without a bloom filter
//      Perform read operations and track bytes read
// Test Result:
//      Verify bytes read increases for disk operations
//      Confirm MayContain doesn't affect bytes read without filter
//
TEST_F(SSTableReaderTest, BytesReadTrackingWithoutFilter) {
    // Create test data without bloom filter
    std::vector<std::pair<std::string, std::string>> entries = {
        {"key1", "value1"},
        {"key2", "value2"},
        {"key3", "value3"},
    };
    createTestSSTable("test.sst", entries, false);

    // Test bytes read during Open()
    SSTableReader reader(fs_, "test.sst");
    VERIFY_RESULT(reader.Open());
    size_t bytes_after_open = reader.GetBytesRead();
    EXPECT_GT(bytes_after_open, 0) << "Should have read some bytes during Open()";

    // Test bytes read during Get()
    auto result1 = reader.Get("key1");
    VERIFY_RESULT(result1);
    size_t bytes_after_first_get = reader.GetBytesRead();
    EXPECT_GT(bytes_after_first_get, bytes_after_open) << "Should have read more bytes after Get()";

    // Test bytes read during MayContain() (should not increase since we don't have a filter)
    VERIFY_RESULT(reader.MayContain("key2"));
    size_t bytes_after_may_contain = reader.GetBytesRead();
    EXPECT_EQ(bytes_after_may_contain, bytes_after_first_get) << "MayContain should not read more bytes without filter";
}

//
// Test Setup:
//      Create an SSTable with large values (64KB each) across multiple blocks
//      Compare sequential vs random access patterns
// Test Result:
//      Verify random access reads more bytes than sequential
//      Confirm block-level read granularity
//
TEST_F(SSTableReaderTest, BytesReadTrackingLargeValues) {
    // Create test data with large values to ensure multiple blocks
    std::string large_value(64 * 1024, 'x');  // 64KB value to ensure multiple blocks
    std::vector<std::pair<std::string, std::string>> entries;
    for (int i = 0; i < 100; i++) {  // 100 entries * 64KB = ~6.4MB total
        entries.push_back({pond::test::GenerateKey(i), large_value + std::to_string(i)});
    }
    createTestSSTable("test.sst", entries, true);

    // Test bytes read during Open()
    SSTableReader reader(fs_, "test.sst");
    VERIFY_RESULT(reader.Open());
    size_t bytes_after_open = reader.GetBytesRead();
    EXPECT_GT(bytes_after_open, 0) << "Should have read some bytes during Open()";

    // Test bytes read for sequential vs random access
    size_t sequential_bytes = 0;
    {
        auto iter = reader.NewIterator();
        for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
            // Just iterate to read all blocks sequentially
        }
        sequential_bytes = reader.GetBytesRead() - bytes_after_open;
    }

    // Create new reader for random access test
    SSTableReader random_reader(fs_, "test.sst");
    VERIFY_RESULT(random_reader.Open());
    size_t random_bytes = 0;
    {
        // Read all entries in random order
        std::vector<std::string> keys;
        for (const auto& entry : entries) {
            keys.push_back(entry.first);
        }
        std::random_device rd;
        std::mt19937 gen(rd());
        std::shuffle(keys.begin(), keys.end(), gen);
        for (const auto& key : keys) {
            VERIFY_RESULT(random_reader.Get(key));
        }
        random_bytes = random_reader.GetBytesRead() - bytes_after_open;
    }

    EXPECT_GT(random_bytes, sequential_bytes)
        << "Random access should read more bytes than sequential access due to block reads";
}

//
// Test Setup:
//      Create an SSTable without a bloom filter
//      Test iterator behavior at minimum possible read time
// Test Result:
//      Verify iterator returns no entries at minimum time
//
TEST_F(SSTableReaderTest, MinReadTimeBoundary) {
    // Create test data without bloom filter
    std::vector<std::pair<std::string, std::string>> entries = {
        {"key1", "value1"},
        {"key2", "value2"},
        {"key3", "value3"},
    };
    createTestSSTable("test.sst", entries, false);

    // Test with minimum possible read time
    SSTableReader reader(fs_, "test.sst");
    VERIFY_RESULT(reader.Open());
    auto iter = reader.NewIterator(MinHybridTime());
    iter->SeekToFirst();

    // Verify no entries are visible at minimum time
    ASSERT_FALSE(iter->Valid()) << "Should find no entries at min read time";

    // Test boundary conditions
    iter->Next();
    ASSERT_FALSE(iter->Valid()) << "Next() should remain invalid";

    // Verify seeking to specific keys also fails
    iter->Seek("key1");
    ASSERT_FALSE(iter->Valid()) << "Seek(key1) should not find anything";

    iter->Seek("key2");
    ASSERT_FALSE(iter->Valid()) << "Seek(key2) should not find anything";

    // Verify iterator doesn't get stuck in infinite loop
    int safety_counter = 0;
    for (iter->SeekToFirst(); iter->Valid() && safety_counter < 1000; iter->Next()) {
        safety_counter++;
    }
    ASSERT_LT(safety_counter, 1000) << "Iterator entered possible infinite loop";
    ASSERT_FALSE(iter->Valid()) << "Iterator should remain invalid after loop";
}

//
// Test Setup:
//      Create an SSTable with multiple versions of keys
//      Test iteration with AllVersions behavior
// Test Result:
//      Verify all versions are returned in descending order
//      Confirm proper version visibility and ordering
//
TEST_F(SSTableReaderTest, AllVersionsIterator) {
    // Create test data with multiple versions of keys
    SSTableWriter writer(fs_, "test.sst");

    HybridTime t1(100);
    HybridTime t2(200);
    HybridTime t3(300);

    // Add entries with different versions
    VERIFY_RESULT(writer.Add("key1", stringToChunk("value1_v3"), t3));
    VERIFY_RESULT(writer.Add("key1", stringToChunk("value1_v2"), t2));
    VERIFY_RESULT(writer.Add("key1", stringToChunk("value1_v1"), t1));
    VERIFY_RESULT(writer.Add("key2", stringToChunk("value2_v2"), t2));
    VERIFY_RESULT(writer.Add("key2", stringToChunk("value2_v1"), t1));
    VERIFY_RESULT(writer.Add("key3", stringToChunk("value3_v1"), t1));
    VERIFY_RESULT(writer.Finish());

    SSTableReader reader(fs_, "test.sst");
    VERIFY_RESULT(reader.Open());

    // Test reading with AllVersions behavior
    auto iter = reader.NewIterator(MaxHybridTime(), SSTableReader::VersionBehavior::AllVersions);

    // Check key1's versions (should see all three versions)
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(iter->key(), "key1");
    EXPECT_EQ(iter->value().ToString(), "value1_v3");
    EXPECT_EQ(iter->version(), t3);

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(iter->key(), "key1");
    EXPECT_EQ(iter->value().ToString(), "value1_v2");
    EXPECT_EQ(iter->version(), t2);

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(iter->key(), "key1");
    EXPECT_EQ(iter->value().ToString(), "value1_v1");
    EXPECT_EQ(iter->version(), t1);

    // Check key2's versions (should see both versions)
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(iter->key(), "key2");
    EXPECT_EQ(iter->value().ToString(), "value2_v2");
    EXPECT_EQ(iter->version(), t2);

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(iter->key(), "key2");
    EXPECT_EQ(iter->value().ToString(), "value2_v1");
    EXPECT_EQ(iter->version(), t1);

    // Check key3's version (only one version)
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(iter->key(), "key3");
    EXPECT_EQ(iter->value().ToString(), "value3_v1");
    EXPECT_EQ(iter->version(), t1);

    iter->Next();
    EXPECT_FALSE(iter->Valid());

    // Test seeking behavior with AllVersions
    iter->Seek("key2");
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(iter->key(), "key2");
    EXPECT_EQ(iter->value().ToString(), "value2_v2");
    EXPECT_EQ(iter->version(), t2);

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(iter->key(), "key2");
    EXPECT_EQ(iter->value().ToString(), "value2_v1");
    EXPECT_EQ(iter->version(), t1);

    // Test seeking to a key with only one version
    iter->Seek("key3");
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(iter->key(), "key3");
    EXPECT_EQ(iter->value().ToString(), "value3_v1");
    EXPECT_EQ(iter->version(), t1);

    // Test seeking to a non-existent key
    iter->Seek("key4");
    EXPECT_FALSE(iter->Valid());
}

}  // namespace