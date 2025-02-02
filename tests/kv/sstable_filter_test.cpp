#include <gtest/gtest.h>

#include "kv/sstable_format.h"

using namespace pond::kv;
using namespace pond::common;

namespace {

class SSTableFilterTest : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(SSTableFilterTest, FilterBlockFooterSerializeDeserialize) {
    FilterBlockFooter footer;
    footer.filter_size = 1024;
    footer.num_keys = 100;
    footer.checksum = 0x12345678;
    footer.reserved = 0;

    // Serialize
    auto serialized = footer.Serialize();
    ASSERT_EQ(serialized.size(), FilterBlockFooter::kFooterSize);

    // Deserialize
    FilterBlockFooter deserialized;
    ASSERT_TRUE(deserialized.Deserialize(serialized.data(), serialized.size()));

    // Verify fields
    EXPECT_EQ(deserialized.filter_size, footer.filter_size);
    EXPECT_EQ(deserialized.num_keys, footer.num_keys);
    EXPECT_EQ(deserialized.checksum, footer.checksum);
    EXPECT_EQ(deserialized.reserved, footer.reserved);
}

TEST_F(SSTableFilterTest, FilterBlockWithFooter) {
    FilterBlockBuilder builder(100);  // Expect ~100 keys

    // Add some keys
    std::vector<std::string> keys = {"key1", "key2", "key3", "key4", "key5"};
    builder.AddKeys(keys);

    // Get the filter block with footer
    auto block_data = builder.Finish();
    ASSERT_FALSE(block_data.empty());

    // The block should be at least as large as the footer
    ASSERT_GT(block_data.size(), FilterBlockFooter::kFooterSize);

    // Extract and verify the footer
    FilterBlockFooter footer;
    const uint8_t* footer_data = block_data.data() + block_data.size() - FilterBlockFooter::kFooterSize;
    ASSERT_TRUE(footer.Deserialize(footer_data, FilterBlockFooter::kFooterSize));

    // Verify filter size
    EXPECT_EQ(footer.filter_size, block_data.size() - FilterBlockFooter::kFooterSize);
    EXPECT_GT(footer.num_keys, 0);

    // Verify checksum
    uint32_t computed_checksum = Crc32(block_data.data(), footer.filter_size);
    EXPECT_EQ(footer.checksum, computed_checksum);

    // Verify the filter data works
    auto filter_result = BloomFilter::deserialize(DataChunk(block_data.data(), footer.filter_size));
    ASSERT_TRUE(filter_result.ok());
    auto filter = std::move(filter_result.value());

    // Check existing keys
    for (const auto& key : keys) {
        EXPECT_TRUE(filter.mightContain(DataChunk::fromString(key)));
    }

    // Check non-existent key
    EXPECT_FALSE(filter.mightContain(DataChunk::fromString("nonexistent")));
}

TEST_F(SSTableFilterTest, FilterBlockBasic) {
    FilterBlockBuilder builder(100);  // Expect ~100 keys

    // Add some keys
    std::vector<std::string> keys = {"key1", "key2", "key3", "key4", "key5"};
    builder.AddKeys(keys);

    // Get the filter block
    auto filter_data = builder.Finish();
    ASSERT_FALSE(filter_data.empty());

    // Extract footer
    FilterBlockFooter footer;
    const uint8_t* footer_data = filter_data.data() + filter_data.size() - FilterBlockFooter::kFooterSize;
    ASSERT_TRUE(footer.Deserialize(footer_data, FilterBlockFooter::kFooterSize));

    // Deserialize and verify filter data (without footer)
    auto result = BloomFilter::deserialize(DataChunk(filter_data.data(), footer.filter_size));
    ASSERT_TRUE(result.ok());
    auto filter = std::move(result.value());

    // Check existing keys
    for (const auto& key : keys) {
        EXPECT_TRUE(filter.mightContain(DataChunk::fromString(key)));
    }

    // Check non-existent key
    EXPECT_FALSE(filter.mightContain(DataChunk::fromString("nonexistent")));
}

TEST_F(SSTableFilterTest, FilterBlockMultipleBlocks) {
    FilterBlockBuilder builder(1000);  // Expect ~1000 keys

    // Add keys from multiple blocks
    std::vector<std::string> block1_keys = {"a1", "a2", "a3"};
    std::vector<std::string> block2_keys = {"b1", "b2", "b3"};
    std::vector<std::string> block3_keys = {"c1", "c2", "c3"};

    builder.AddKeys(block1_keys);
    builder.AddKeys(block2_keys);
    builder.AddKeys(block3_keys);

    // Get the filter block
    auto filter_data = builder.Finish();
    ASSERT_FALSE(filter_data.empty());

    // Extract footer
    FilterBlockFooter footer;
    const uint8_t* footer_data = filter_data.data() + filter_data.size() - FilterBlockFooter::kFooterSize;
    ASSERT_TRUE(footer.Deserialize(footer_data, FilterBlockFooter::kFooterSize));

    // Deserialize and verify filter data (without footer)
    auto result = BloomFilter::deserialize(DataChunk(filter_data.data(), footer.filter_size));
    ASSERT_TRUE(result.ok());
    auto filter = std::move(result.value());

    // Check all keys from all blocks
    for (const auto& key : block1_keys) {
        EXPECT_TRUE(filter.mightContain(DataChunk::fromString(key)));
    }
    for (const auto& key : block2_keys) {
        EXPECT_TRUE(filter.mightContain(DataChunk::fromString(key)));
    }
    for (const auto& key : block3_keys) {
        EXPECT_TRUE(filter.mightContain(DataChunk::fromString(key)));
    }

    // Check false positive rate
    int false_positives = 0;
    const int num_tests = 1000;
    for (int i = 0; i < num_tests; i++) {
        std::string test_key = "test" + std::to_string(i);
        if (filter.mightContain(DataChunk::fromString(test_key))) {
            false_positives++;
        }
    }
    double false_positive_rate = static_cast<double>(false_positives) / num_tests;
    EXPECT_LT(false_positive_rate, 0.1);  // Should be well under 10%
}

TEST_F(SSTableFilterTest, FilterBlockReset) {
    FilterBlockBuilder builder(100);

    // Add initial keys
    std::vector<std::string> keys1 = {"key1", "key2", "key3"};
    builder.AddKeys(keys1);

    // Reset the builder
    builder.Reset();

    // Add new keys
    std::vector<std::string> keys2 = {"new1", "new2", "new3"};
    builder.AddKeys(keys2);

    // Get the filter block
    auto filter_data = builder.Finish();
    ASSERT_FALSE(filter_data.empty());

    // Extract footer
    FilterBlockFooter footer;
    const uint8_t* footer_data = filter_data.data() + filter_data.size() - FilterBlockFooter::kFooterSize;
    ASSERT_TRUE(footer.Deserialize(footer_data, FilterBlockFooter::kFooterSize));

    // Deserialize and verify filter data (without footer)
    auto result = BloomFilter::deserialize(DataChunk(filter_data.data(), footer.filter_size));
    ASSERT_TRUE(result.ok());
    auto filter = std::move(result.value());

    // Original keys should not be present
    for (const auto& key : keys1) {
        EXPECT_FALSE(filter.mightContain(DataChunk::fromString(key)));
    }

    // New keys should be present
    for (const auto& key : keys2) {
        EXPECT_TRUE(filter.mightContain(DataChunk::fromString(key)));
    }
}

}  // namespace
