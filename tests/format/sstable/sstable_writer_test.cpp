
#include "format/sstable/sstable_writer.h"

#include <gtest/gtest.h>

#include "common/memory_append_only_fs.h"
#include "format/sstable/sstable_format.h"
#include "format/sstable/sstable_reader.h"
#include "test_helper.h"

using namespace pond::common;
using namespace pond::format;
using namespace pond::test;

namespace {

class SSTableWriterTest : public ::testing::Test {
protected:
    void SetUp() override { fs_ = std::make_shared<MemoryAppendOnlyFileSystem>(); }

    std::shared_ptr<MemoryAppendOnlyFileSystem> fs_;

    // Helper to read entire file
    Result<DataChunk> readEntireFile(const std::string& path) {
        auto handle_result = fs_->OpenFile(path, false);
        if (!handle_result.ok()) {
            return Result<DataChunk>::failure(handle_result.error());
        }
        auto handle = handle_result.value();

        auto size_result = fs_->Size(handle);
        if (!size_result.ok()) {
            fs_->CloseFile(handle);
            return Result<DataChunk>::failure(size_result.error());
        }

        auto read_result = fs_->Read(handle, 0, size_result.value());
        fs_->CloseFile(handle);
        return read_result;
    }

    // Helper to create DataChunk from string
    DataChunk stringToChunk(const std::string& str) {
        return DataChunk(reinterpret_cast<const uint8_t*>(str.data()), str.size());
    }
};

TEST_F(SSTableWriterTest, BasicOperations) {
    SSTableWriter writer(fs_, "test.sst");

    // Add some key-value pairs
    ASSERT_TRUE(writer.Add("key1", stringToChunk("value1")).ok());
    ASSERT_TRUE(writer.Add("key2", stringToChunk("value2")).ok());
    ASSERT_TRUE(writer.Add("key3", stringToChunk("value3")).ok());

    // Finish writing
    ASSERT_TRUE(writer.Finish().ok());

    // Verify file exists
    ASSERT_TRUE(fs_->Exists("test.sst"));

    // Read file and verify basic structure
    auto read_result = readEntireFile("test.sst");
    ASSERT_TRUE(read_result.ok());
    auto data = read_result.value();

    // Verify file header
    FileHeader header;
    ASSERT_TRUE(header.Deserialize(data.Data(), FileHeader::kHeaderSize));
    EXPECT_EQ(header.magic_number, FileHeader::kMagicNumber);
    EXPECT_EQ(header.version, 1);

    // Verify footer is present (at end of file)
    Footer footer;
    ASSERT_TRUE(footer.Deserialize(data.Data() + data.Size() - Footer::kFooterSize, Footer::kFooterSize));
    EXPECT_EQ(footer.magic_number, Footer::kMagicNumber);
    EXPECT_GT(footer.index_block_offset, FileHeader::kHeaderSize);
}

TEST_F(SSTableWriterTest, UnsortedKeys) {
    SSTableWriter writer(fs_, "test.sst");

    // First key should succeed
    ASSERT_TRUE(writer.Add("key2", stringToChunk("value2")).ok());

    // Adding a smaller key should fail
    auto result = writer.Add("key1", stringToChunk("value1"));
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.error().code(), ErrorCode::InvalidArgument);
}

TEST_F(SSTableWriterTest, EmptyWriter) {
    SSTableWriter writer(fs_, "test.sst");

    // Finishing without adding any data should fail
    auto result = writer.Finish();
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.error().code(), ErrorCode::InvalidOperation);
}

TEST_F(SSTableWriterTest, DoubleFinish) {
    SSTableWriter writer(fs_, "test.sst");

    // Add some data
    ASSERT_TRUE(writer.Add("key1", stringToChunk("value1")).ok());

    // First finish should succeed
    ASSERT_TRUE(writer.Finish().ok());

    // Second finish should fail
    auto result = writer.Finish();
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.error().code(), ErrorCode::InvalidOperation);
}

TEST_F(SSTableWriterTest, AddAfterFinish) {
    SSTableWriter writer(fs_, "test.sst");

    // Add initial data and finish
    ASSERT_TRUE(writer.Add("key1", stringToChunk("value1")).ok());
    ASSERT_TRUE(writer.Finish().ok());

    // Adding after finish should fail
    auto result = writer.Add("key2", stringToChunk("value2"));
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.error().code(), ErrorCode::InvalidOperation);
}

TEST_F(SSTableWriterTest, WithBloomFilter) {
    SSTableWriter writer(fs_, "test.sst");

    // Enable bloom filter before adding data
    writer.EnableFilter(100);  // Expect ~100 keys

    // Add some data
    ASSERT_TRUE(writer.Add("key1", stringToChunk("value1")).ok());
    ASSERT_TRUE(writer.Add("key2", stringToChunk("value2")).ok());
    ASSERT_TRUE(writer.Finish().ok());

    // Read file and verify filter block is present
    auto read_result = readEntireFile("test.sst");
    ASSERT_TRUE(read_result.ok());
    auto data = read_result.value();

    // Verify header has filter flag
    FileHeader header;
    ASSERT_TRUE(header.Deserialize(data.Data(), FileHeader::kHeaderSize));
    EXPECT_TRUE(header.HasFilter());

    // Verify footer has valid filter block offset
    Footer footer;
    ASSERT_TRUE(footer.Deserialize(data.Data() + data.Size() - Footer::kFooterSize, Footer::kFooterSize));
    EXPECT_GT(footer.filter_block_offset, FileHeader::kHeaderSize);
}

TEST_F(SSTableWriterTest, LateFilterEnable) {
    SSTableWriter writer(fs_, "test.sst");

    // Add some data first
    ASSERT_TRUE(writer.Add("key1", stringToChunk("value1")).ok());

    // Try to enable filter after adding data (should be ignored)
    writer.EnableFilter(100);

    // Add more data and finish
    ASSERT_TRUE(writer.Add("key2", stringToChunk("value2")).ok());
    ASSERT_TRUE(writer.Finish().ok());

    // Read file and verify filter block is NOT present
    auto read_result = readEntireFile("test.sst");
    ASSERT_TRUE(read_result.ok());
    auto data = read_result.value();

    // Verify header does not have filter flag
    FileHeader header;
    ASSERT_TRUE(header.Deserialize(data.Data(), FileHeader::kHeaderSize));
    EXPECT_FALSE(header.HasFilter());
}

TEST_F(SSTableWriterTest, BlockBoundaries) {
    SSTableWriter writer(fs_, "test.sst");

    // Add enough entries to force multiple blocks
    // Each value is ~1MB to ensure we get multiple blocks with just a few entries
    std::string large_value(1024 * 1024, 'x');  // 1MB value
    for (int i = 0; i < 5; i++) {
        std::string key = "key" + std::to_string(i);
        ASSERT_TRUE(writer.Add(key, stringToChunk(large_value)).ok());
    }

    ASSERT_TRUE(writer.Finish().ok());

    // Read file and verify structure
    auto read_result = readEntireFile("test.sst");
    ASSERT_TRUE(read_result.ok());
    auto data = read_result.value();

    // Verify footer
    Footer footer;
    ASSERT_TRUE(footer.Deserialize(data.Data() + data.Size() - Footer::kFooterSize, Footer::kFooterSize));

    // Read index block
    DataChunk index_block(data.Data() + footer.index_block_offset,
                          data.Size() - footer.index_block_offset - Footer::kFooterSize);

    // Verify index block has multiple entries (multiple blocks were created)
    BlockFooter index_footer;
    ASSERT_TRUE(index_footer.Deserialize(index_block.Data() + index_block.Size() - BlockFooter::kFooterSize,
                                         BlockFooter::kFooterSize));
    EXPECT_GT(index_footer.entry_count, 1);
}

TEST_F(SSTableWriterTest, MultiVersionBasic) {
    SSTableWriter writer(fs_, "test.sst");

    // Add multiple versions of the same key
    HybridTime t1(100);
    HybridTime t2(200);
    HybridTime t3(300);

    std::vector<TestKvEntry> entries = {
        GenerateTestKvEntry("key1", t3, "value1_v3"),
        GenerateTestKvEntry("key1", t2, "value1_v2"),
        GenerateTestKvEntry("key1", t1, "value1_v1"),
        GenerateTestKvEntry("key2", t2, "value2_v2"),
        GenerateTestKvEntry("key2", t1, "value2_v1"),
    };

    for (const auto& entry : entries) {
        VERIFY_RESULT_MSG(writer.Add(entry.key, entry.value, entry.version, false),
                          "Failed to add entry " + std::string(entry.key));
    }

    VERIFY_RESULT(writer.Finish());

    // Read file and verify structure
    auto read_result = readEntireFile("test.sst");
    VERIFY_RESULT(read_result);
    auto data = read_result.value();

    // Verify file header
    FileHeader header;
    ASSERT_TRUE(header.Deserialize(data.Data(), FileHeader::kHeaderSize));
    EXPECT_EQ(header.version, 1);

    // Verify footer
    Footer footer;
    ASSERT_TRUE(footer.Deserialize(data.Data() + data.Size() - Footer::kFooterSize, Footer::kFooterSize));
    EXPECT_GT(footer.index_block_offset, FileHeader::kHeaderSize);
}

TEST_F(SSTableWriterTest, InvalidVersion) {
    SSTableWriter writer(fs_, "test.sst");

    auto result = writer.Add("key1", stringToChunk("value1"), InvalidHybridTime(), false);
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.error().code(), ErrorCode::InvalidArgument);
}

TEST_F(SSTableWriterTest, MultiVersionOrderDescending) {
    SSTableWriter writer(fs_, "test.sst");

    // Test that versions of the same key can be added in any order
    HybridTime t1(100);
    HybridTime t2(200);
    HybridTime t3(300);

    std::vector<TestKvEntry> entries = {
        GenerateTestKvEntry("key1", t2, "value1_v2"),
        GenerateTestKvEntry("key1", t1, "value1_v1"),
        GenerateTestKvEntry("key1", t3, "value1_v3"),
        GenerateTestKvEntry("key2", t2, "value2_v2"),
        GenerateTestKvEntry("key2", t1, "value2_v1"),
    };

    auto i = 0;
    // Add versions out of order
    for (const auto& entry : entries) {
        if (i == 2) {  // expect failure since the order is not descending
            EXPECT_FALSE(writer.Add(entry.key, entry.value, entry.version, false).ok());
        } else {
            VERIFY_RESULT_MSG(writer.Add(entry.key, entry.value, entry.version, false),
                              "Failed to add entry " + std::string(entry.key));
        }
        i++;
    }

    VERIFY_RESULT(writer.Finish());
}

TEST_F(SSTableWriterTest, MultiVersionWithFilter) {
    SSTableWriter writer(fs_, "test.sst");

    // Enable bloom filter
    writer.EnableFilter(10);

    HybridTime t1(100);
    HybridTime t2(200);

    // Add multiple versions of keys
    VERIFY_RESULT(writer.Add("key1", stringToChunk("value1_v1"), t2, false));
    VERIFY_RESULT(writer.Add("key1", stringToChunk("value1_v2"), t1, false));
    VERIFY_RESULT(writer.Add("key2", stringToChunk("value2_v1"), t2, false));
    VERIFY_RESULT(writer.Add("key2", stringToChunk("value2_v2"), t1, false));

    VERIFY_RESULT(writer.Finish());

    // Verify filter block is present
    auto read_result = readEntireFile("test.sst");
    VERIFY_RESULT(read_result);
    auto data = read_result.value();

    FileHeader header;
    ASSERT_TRUE(header.Deserialize(data.Data(), FileHeader::kHeaderSize));
    EXPECT_TRUE(header.HasFilter());

    Footer footer;
    ASSERT_TRUE(footer.Deserialize(data.Data() + data.Size() - Footer::kFooterSize, Footer::kFooterSize));
    EXPECT_GT(footer.filter_block_offset, FileHeader::kHeaderSize);
}

TEST_F(SSTableWriterTest, MultiVersionLargeValues) {
    SSTableWriter writer(fs_, "test.sst");

    // Create large values to force multiple blocks
    std::string large_value_base(512 * 1024, 'x');  // 512KB base value

    HybridTime t1(100);
    HybridTime t2(200);

    // Add multiple versions with large values
    for (int i = 0; i < 3; i++) {
        std::string key = "key" + std::to_string(i);
        std::string value1 = large_value_base + "_v1_" + std::to_string(i);
        std::string value2 = large_value_base + "_v2_" + std::to_string(i);

        VERIFY_RESULT(writer.Add(key, stringToChunk(value1), t2, false));
        VERIFY_RESULT(writer.Add(key, stringToChunk(value2), t1, false));
    }

    VERIFY_RESULT(writer.Finish());

    // Verify multiple blocks were created
    auto read_result = readEntireFile("test.sst");
    VERIFY_RESULT(read_result);
    auto data = read_result.value();

    Footer footer;
    ASSERT_TRUE(footer.Deserialize(data.Data() + data.Size() - Footer::kFooterSize, Footer::kFooterSize));

    // Read index block
    DataChunk index_block(data.Data() + footer.index_block_offset,
                          data.Size() - footer.index_block_offset - Footer::kFooterSize);

    BlockFooter index_footer;
    ASSERT_TRUE(index_footer.Deserialize(index_block.Data() + index_block.Size() - BlockFooter::kFooterSize,
                                         BlockFooter::kFooterSize));
    EXPECT_GT(index_footer.entry_count, 1);  // Should have multiple blocks
}

TEST_F(SSTableWriterTest, TombstoneOperations) {
    SSTableWriter writer(fs_, "test.sst");

    HybridTime t1(100);
    HybridTime t2(200);

    // Add tombstone for the same key with newer version
    VERIFY_RESULT(writer.Add("key1", stringToChunk(""), t2, true));

    // Add regular key-value pair
    VERIFY_RESULT(writer.Add("key1", stringToChunk("value1"), t1, false));

    // Add another key with tombstone
    VERIFY_RESULT(writer.Add("key2", stringToChunk(""), t1, true));

    VERIFY_RESULT(writer.Finish());

    // Verify file exists
    ASSERT_TRUE(fs_->Exists("test.sst"));

    // Read file and verify structure
    auto read_result = readEntireFile("test.sst");
    VERIFY_RESULT(read_result);
    auto data = read_result.value();

    // Verify footer
    Footer footer;
    ASSERT_TRUE(footer.Deserialize(data.Data() + data.Size() - Footer::kFooterSize, Footer::kFooterSize));
    EXPECT_EQ(footer.magic_number, Footer::kMagicNumber);

    // Create reader to verify key1
    SSTableReader reader(fs_, "test.sst");
    VERIFY_RESULT(reader.Open());

    // Read key1 at different timestamps
    {
        // At t2, should return tombstone
        auto result = reader.Get("key1", t2);
        ASSERT_FALSE(result.ok());
        EXPECT_EQ(result.error().code(), ErrorCode::NotFound);
    }
    {
        // At t1, should return regular value
        auto result = reader.Get("key1", t1);
        VERIFY_RESULT(result);
        EXPECT_EQ(result.value().ToString(), "value1");
    }
}

}  // namespace