#include "format/sstable/sstable_format.h"

#include <gtest/gtest.h>

using namespace pond::format;
using namespace pond::common;

namespace {

class SSTableFormatTest : public ::testing::Test {
protected:
    void SetUp() override {}
};

TEST_F(SSTableFormatTest, FileHeaderSize) {
    EXPECT_EQ(FileHeader::kHeaderSize, 64);
}

TEST_F(SSTableFormatTest, FooterSize) {
    EXPECT_EQ(Footer::kFooterSize, 128);
}

TEST_F(SSTableFormatTest, FileHeaderSerialization) {
    FileHeader header;
    header.version = 2;
    header.flags = 0x1234;
    header.reserved[0] = 0xFF;  // Set first byte of reserved

    // Serialize
    auto serialized = header.Serialize();
    ASSERT_EQ(serialized.size(), FileHeader::kHeaderSize);

    // Deserialize into new header
    FileHeader new_header;
    ASSERT_TRUE(new_header.Deserialize(serialized.data(), serialized.size()));

    // Verify fields match
    EXPECT_EQ(new_header.magic_number, FileHeader::kMagicNumber);
    EXPECT_EQ(new_header.version, 2);
    EXPECT_EQ(new_header.flags, 0x1234);
    EXPECT_EQ(new_header.reserved[0], 0xFF);

    // Verify other reserved bytes are 0
    for (size_t i = 1; i < sizeof(new_header.reserved); i++) {
        EXPECT_EQ(new_header.reserved[i], 0);
    }
}

TEST_F(SSTableFormatTest, FileHeaderInvalidMagic) {
    FileHeader header;
    auto serialized = header.Serialize();

    // Corrupt magic number
    serialized[0] = ~serialized[0];

    FileHeader new_header;
    ASSERT_FALSE(new_header.Deserialize(serialized.data(), serialized.size()));
    ASSERT_FALSE(new_header.IsValid());
}

TEST_F(SSTableFormatTest, FileHeaderInvalidSize) {
    FileHeader header;
    auto serialized = header.Serialize();

    FileHeader new_header;
    ASSERT_FALSE(new_header.Deserialize(serialized.data(), serialized.size() - 1));
}

TEST_F(SSTableFormatTest, FooterSerialization) {
    Footer footer;
    footer.index_block_offset = 1000;
    footer.filter_block_offset = 2000;
    footer.metadata_block_offset = 3000;
    footer.padding[0] = 0xFF;  // Set first byte of padding

    // Serialize
    auto serialized = footer.Serialize();
    ASSERT_EQ(serialized.size(), Footer::kFooterSize);

    // Deserialize into new footer
    Footer new_footer;
    ASSERT_TRUE(new_footer.Deserialize(serialized.data(), serialized.size()));

    // Verify fields match
    EXPECT_EQ(new_footer.magic_number, Footer::kMagicNumber);
    EXPECT_EQ(new_footer.index_block_offset, 1000);
    EXPECT_EQ(new_footer.filter_block_offset, 2000);
    EXPECT_EQ(new_footer.metadata_block_offset, 3000);
    EXPECT_EQ(new_footer.padding[0], 0xFF);

    // Verify other padding bytes are 0
    for (size_t i = 1; i < sizeof(new_footer.padding); i++) {
        EXPECT_EQ(new_footer.padding[i], 0);
    }
}

TEST_F(SSTableFormatTest, FooterInvalidMagic) {
    Footer footer;
    auto serialized = footer.Serialize();

    // Corrupt magic number (last 8 bytes)
    serialized[Footer::kFooterSize - 1] = ~serialized[Footer::kFooterSize - 1];

    Footer new_footer;
    ASSERT_FALSE(new_footer.Deserialize(serialized.data(), serialized.size()));
    ASSERT_FALSE(new_footer.IsValid());
}

TEST_F(SSTableFormatTest, FooterInvalidSize) {
    Footer footer;
    auto serialized = footer.Serialize();

    Footer new_footer;
    ASSERT_FALSE(new_footer.Deserialize(serialized.data(), serialized.size() - 1));
}

TEST_F(SSTableFormatTest, EndianConversion) {
    uint64_t original64 = 0x1234567890ABCDEF;
    uint64_t converted64 = util::LittleEndianToHost64(util::HostToLittleEndian64(original64));
    EXPECT_EQ(original64, converted64);

    uint32_t original32 = 0x12345678;
    uint32_t converted32 = util::LittleEndianToHost32(util::HostToLittleEndian32(original32));
    EXPECT_EQ(original32, converted32);
}

TEST_F(SSTableFormatTest, BlockFooterSize) {
    EXPECT_EQ(BlockFooter::kFooterSize, 16);
}

TEST_F(SSTableFormatTest, BlockFooterSerialization) {
    BlockFooter footer;
    footer.entry_count = 42;
    footer.block_size = 1024;
    footer.checksum = 0x12345678;
    footer.compression_type = 1;  // Snappy

    auto serialized = footer.Serialize();
    ASSERT_EQ(serialized.size(), BlockFooter::kFooterSize);

    BlockFooter new_footer;
    ASSERT_TRUE(new_footer.Deserialize(serialized.data(), serialized.size()));

    EXPECT_EQ(new_footer.entry_count, 42);
    EXPECT_EQ(new_footer.block_size, 1024);
    EXPECT_EQ(new_footer.checksum, 0x12345678);
    EXPECT_EQ(new_footer.compression_type, 1);
}

TEST_F(SSTableFormatTest, DataBlockEntrySerialization) {
    DataBlockEntry entry;
    std::string user_key = "hello";
    HybridTime version(1000);
    InternalKey key(user_key, version);
    entry.key_length = key.size();
    entry.value_length = 10;

    DataChunk value = DataChunk::FromString("world12345");

    // Test header serialization
    auto header = entry.SerializeHeader();
    ASSERT_EQ(header.size(), DataBlockEntry::kHeaderSize);

    DataBlockEntry new_entry;
    ASSERT_TRUE(new_entry.DeserializeHeader(header.data(), header.size()));
    EXPECT_EQ(new_entry.key_length, key.size());
    EXPECT_EQ(new_entry.value_length, 10);

    // Test full serialization
    auto full = entry.Serialize(key, value);
    ASSERT_EQ(full.size(), DataBlockEntry::kHeaderSize + key.size() + value.Size());

    // Verify the serialized data
    InternalKey decoded_key;
    ASSERT_TRUE(decoded_key.Deserialize(full.data() + DataBlockEntry::kHeaderSize, new_entry.key_length));
    EXPECT_EQ(decoded_key.user_key(), user_key);
    EXPECT_EQ(decoded_key.version(), version);

    std::vector<uint8_t> serialized_value(full.begin() + DataBlockEntry::kHeaderSize + key.size(), full.end());
    EXPECT_EQ(std::vector<uint8_t>(value.Data(), value.Data() + value.Size()), serialized_value);
}

TEST_F(SSTableFormatTest, IndexBlockEntrySerialization) {
    IndexBlockEntry entry;
    entry.key_length = 5;
    entry.block_offset = 1000;
    entry.block_size = 2000;
    entry.entry_count = 42;

    std::string largest_key = "hello";

    // Test header serialization
    auto header = entry.SerializeHeader();
    ASSERT_EQ(header.size(), IndexBlockEntry::kHeaderSize);

    IndexBlockEntry new_entry;
    ASSERT_TRUE(new_entry.DeserializeHeader(header.data(), header.size()));
    EXPECT_EQ(new_entry.key_length, 5);
    EXPECT_EQ(new_entry.block_offset, 1000);
    EXPECT_EQ(new_entry.block_size, 2000);
    EXPECT_EQ(new_entry.entry_count, 42);

    // Test full serialization
    auto full = entry.Serialize(largest_key);
    ASSERT_EQ(full.size(), IndexBlockEntry::kHeaderSize + largest_key.size());

    // Verify the serialized key
    std::string serialized_key(full.begin() + IndexBlockEntry::kHeaderSize, full.end());
    EXPECT_EQ(serialized_key, largest_key);
}

TEST_F(SSTableFormatTest, DataBlockBuilder) {
    DataBlockBuilder builder;

    // Test empty builder
    EXPECT_TRUE(builder.Empty());
    EXPECT_EQ(builder.CurrentSize(), 0);
    auto empty_block = builder.Finish();
    EXPECT_TRUE(empty_block.empty());

    // Add entries
    DataChunk value1 = DataChunk::FromString("val1");
    DataChunk value2 = DataChunk::FromString("val2");
    EXPECT_TRUE(builder.Add("key1", MinHybridTime(), value1));
    EXPECT_TRUE(builder.Add("key2", MinHybridTime(), value2));
    EXPECT_FALSE(builder.Empty());

    // Build block
    auto block = builder.Finish();
    ASSERT_FALSE(block.empty());

    // Verify block footer
    BlockFooter footer;
    size_t footer_offset = block.size() - BlockFooter::kFooterSize;
    ASSERT_TRUE(footer.Deserialize(block.data() + footer_offset, BlockFooter::kFooterSize));
    EXPECT_EQ(footer.entry_count, 2);
    EXPECT_EQ(footer.block_size, block.size());
    EXPECT_EQ(footer.compression_type, 0);  // No compression

    // Reset and verify
    builder.Reset();
    EXPECT_TRUE(builder.Empty());
    EXPECT_EQ(builder.CurrentSize(), 0);
}

TEST_F(SSTableFormatTest, IndexBlockBuilder) {
    IndexBlockBuilder builder;

    // Test empty builder
    EXPECT_TRUE(builder.Empty());
    auto empty_block = builder.Finish();
    EXPECT_TRUE(empty_block.empty());

    // Add entries
    builder.AddEntry("key1", 1000, 100, 5);
    builder.AddEntry("key2", 2000, 200, 10);
    EXPECT_FALSE(builder.Empty());

    // Build block
    auto block = builder.Finish();
    ASSERT_FALSE(block.empty());

    // Verify block footer
    BlockFooter footer;
    size_t footer_offset = block.size() - BlockFooter::kFooterSize;
    ASSERT_TRUE(footer.Deserialize(block.data() + footer_offset, BlockFooter::kFooterSize));
    EXPECT_EQ(footer.entry_count, 2);
    EXPECT_EQ(footer.block_size, block.size());
    EXPECT_EQ(footer.compression_type, 0);  // No compression

    // Reset and verify
    builder.Reset();
    EXPECT_TRUE(builder.Empty());
}

TEST_F(SSTableFormatTest, DataBlockBuilderSizeLimit) {
    DataBlockBuilder builder;
    DataChunk large_value = DataChunk::FromString(std::string(DataBlockBuilder::kTargetBlockSize, 'x'));
    DataChunk small_value = DataChunk::FromString("val1");

    // First entry should succeed
    EXPECT_TRUE(builder.Add("key1", MinHybridTime(), small_value));

    // Adding an entry that would exceed the target size should fail
    EXPECT_FALSE(builder.Add("key2", MinHybridTime(), large_value));

    // Builder should still contain the first entry
    auto block = builder.Finish();
    BlockFooter footer;
    size_t footer_offset = block.size() - BlockFooter::kFooterSize;
    ASSERT_TRUE(footer.Deserialize(block.data() + footer_offset, BlockFooter::kFooterSize));
    EXPECT_EQ(footer.entry_count, 1);
}

TEST_F(SSTableFormatTest, DataBlockBuilderTargetSize) {
    DataBlockBuilder builder;

    // Calculate a value size that will allow 4 entries with headers
    // Each entry has: DataBlockEntry::kHeaderSize + key.size() + value.size()
    // Block also has BlockFooter::kFooterSize at the end
    const size_t header_size = DataBlockEntry::kHeaderSize;
    const std::string key = "key";
    const size_t key_overhead =
        key.size() + 1 /*actual key is with a number suffix*/ + sizeof(HybridTime) /*hybrid time version overhead*/;
    const size_t total_overhead_per_entry = header_size + key_overhead;

    // Target about 900KB per entry to fit 4 entries (including headers) in 4MB
    const size_t value_size = 900 * 1024;
    const size_t entry_size = total_overhead_per_entry + value_size;
    ASSERT_LT(entry_size * 4 + BlockFooter::kFooterSize, DataBlockBuilder::kTargetBlockSize)
        << "Test setup error: entries won't fit in target size";

    DataChunk value = DataChunk::FromString(std::string(value_size, 'x'));

    // Should be able to add 4 entries of ~900KB each
    EXPECT_TRUE(builder.Add(key + "1", MinHybridTime(), value));
    EXPECT_TRUE(builder.Add(key + "2", MinHybridTime(), value));
    EXPECT_TRUE(builder.Add(key + "3", MinHybridTime(), value));
    EXPECT_TRUE(builder.Add(key + "4", MinHybridTime(), value));

    // Fifth entry should fail as it would exceed 4MB
    EXPECT_FALSE(builder.Add(key + "5", MinHybridTime(), value));

    // Verify the block contains exactly 4 entries
    auto block = builder.Finish();
    BlockFooter footer;
    size_t footer_offset = block.size() - BlockFooter::kFooterSize;
    ASSERT_TRUE(footer.Deserialize(block.data() + footer_offset, BlockFooter::kFooterSize));
    EXPECT_EQ(footer.entry_count, 4);

    // Verify total block size is within expected range
    const size_t expected_data_size = entry_size * 4;
    const size_t total_expected_size = expected_data_size + BlockFooter::kFooterSize;
    EXPECT_EQ(block.size(), total_expected_size);
    EXPECT_LT(block.size(), DataBlockBuilder::kTargetBlockSize);
}

TEST_F(SSTableFormatTest, MetadataStatsSerialization) {
    MetadataStats stats;
    stats.key_count = 42;
    stats.smallest_key = "aaa";
    stats.largest_key = "zzz";
    stats.total_key_size = 1000;
    stats.total_value_size = 5000;

    auto serialized = stats.Serialize();
    MetadataStats new_stats;
    ASSERT_TRUE(new_stats.Deserialize(serialized.data(), serialized.size()));

    EXPECT_EQ(new_stats.key_count, 42);
    EXPECT_EQ(new_stats.smallest_key, "aaa");
    EXPECT_EQ(new_stats.largest_key, "zzz");
    EXPECT_EQ(new_stats.total_key_size, 1000);
    EXPECT_EQ(new_stats.total_value_size, 5000);
}

TEST_F(SSTableFormatTest, MetadataPropertiesSerialization) {
    MetadataProperties props;
    props.creation_time = 123456789;
    props.compression_type = 1;
    props.index_type = 0;
    props.filter_type = 1;
    props.filter_fp_rate = 0.01;

    auto serialized = props.Serialize();
    MetadataProperties new_props;
    ASSERT_TRUE(new_props.Deserialize(serialized.data(), serialized.size()));

    EXPECT_EQ(new_props.creation_time, 123456789);
    EXPECT_EQ(new_props.compression_type, 1);
    EXPECT_EQ(new_props.index_type, 0);
    EXPECT_EQ(new_props.filter_type, 1);
    EXPECT_DOUBLE_EQ(new_props.filter_fp_rate, 0.01);
}

TEST_F(SSTableFormatTest, MetadataBlockFooterSerialization) {
    MetadataBlockFooter footer;
    footer.stats_size = 100;
    footer.props_size = 50;
    footer.checksum = 0x12345678;
    footer.reserved = 0;

    auto serialized = footer.Serialize();
    ASSERT_EQ(serialized.size(), MetadataBlockFooter::kFooterSize);

    MetadataBlockFooter new_footer;
    ASSERT_TRUE(new_footer.Deserialize(serialized.data(), serialized.size()));

    EXPECT_EQ(new_footer.stats_size, 100);
    EXPECT_EQ(new_footer.props_size, 50);
    EXPECT_EQ(new_footer.checksum, 0x12345678);
    EXPECT_EQ(new_footer.reserved, 0);
}

TEST_F(SSTableFormatTest, MetadataBlockBuilder) {
    MetadataBlockBuilder builder;

    // Add some entries
    DataChunk value1(reinterpret_cast<const uint8_t*>("value1"), 6);
    DataChunk value2(reinterpret_cast<const uint8_t*>("value2"), 6);
    builder.UpdateStats("key1", value1);
    builder.UpdateStats("key2", value2);

    // Set properties
    builder.SetCompressionType(1);
    builder.SetFilterType(1, 0.01);

    // Build block
    auto block = builder.Finish();
    ASSERT_FALSE(block.empty());

    // Parse the block
    MetadataBlockFooter footer;
    ASSERT_TRUE(footer.Deserialize(block.data() + block.size() - MetadataBlockFooter::kFooterSize,
                                   MetadataBlockFooter::kFooterSize));

    // Verify stats section
    MetadataStats stats;
    ASSERT_TRUE(stats.Deserialize(block.data(), footer.stats_size));
    EXPECT_EQ(stats.key_count, 2);
    EXPECT_EQ(stats.smallest_key, "key1");
    EXPECT_EQ(stats.largest_key, "key2");
    EXPECT_EQ(stats.total_key_size, 8);
    EXPECT_EQ(stats.total_value_size, 12);

    // Verify properties section
    MetadataProperties props;
    ASSERT_TRUE(props.Deserialize(block.data() + footer.stats_size, footer.props_size));
    EXPECT_GT(props.creation_time, 0);
    EXPECT_EQ(props.compression_type, 1);
    EXPECT_EQ(props.filter_type, 1);
    EXPECT_DOUBLE_EQ(props.filter_fp_rate, 0.01);

    // Reset and verify
    builder.Reset();
    auto empty_block = builder.Finish();
    ASSERT_FALSE(empty_block.empty());  // Should still contain empty stats and props
}

TEST_F(SSTableFormatTest, InternalKeySerialization) {
    std::string user_key = "test_key";
    HybridTime version(1000);  // timestamp 1000
    InternalKey key(user_key, version);

    // Test serialization
    auto serialized = key.Serialize();
    ASSERT_EQ(serialized.size(), user_key.size() + sizeof(uint64_t));

    // Test deserialization
    InternalKey new_key;
    ASSERT_TRUE(new_key.Deserialize(serialized.data(), serialized.size()));
    EXPECT_EQ(new_key.user_key(), user_key);
    EXPECT_EQ(new_key.version(), version);
}

TEST_F(SSTableFormatTest, InternalKeyComparison) {
    std::string key1 = "key1";
    std::string key2 = "key2";
    HybridTime version1(1000);
    HybridTime version2(2000);

    // Different user keys
    InternalKey a(key1, version1);
    InternalKey b(key2, version1);
    EXPECT_TRUE(a < b);  // key1 < key2
    EXPECT_FALSE(b < a);

    // Same user key, different versions (newer version should come first)
    InternalKey c(key1, version1);
    InternalKey d(key1, version2);
    EXPECT_TRUE(c > d);  // version1 < version2, but we want newer versions first
    EXPECT_FALSE(d > c);

    // Equal keys
    InternalKey e(key1, version1);
    InternalKey f(key1, version1);
    EXPECT_FALSE(e < f);
    EXPECT_FALSE(f < e);
    EXPECT_TRUE(e == f);
}

TEST_F(SSTableFormatTest, DataBlockEntryWithInternalKey) {
    std::string user_key = "test_key";
    HybridTime version(1000);
    InternalKey key(user_key, version);
    DataChunk value = DataChunk::FromString("test_value");

    DataBlockEntry entry{0 /*flags*/, static_cast<uint16_t>(key.size()), static_cast<uint32_t>(value.Size())};
    auto serialized = entry.Serialize(key, value);

    // Verify size
    EXPECT_EQ(serialized.size(), DataBlockEntry::kHeaderSize + key.size() + value.Size());

    // Verify header
    DataBlockEntry new_entry;
    ASSERT_TRUE(new_entry.DeserializeHeader(serialized.data(), DataBlockEntry::kHeaderSize));
    EXPECT_EQ(new_entry.key_length, key.size());
    EXPECT_EQ(new_entry.value_length, value.Size());

    // Verify key
    InternalKey decoded_key;
    ASSERT_TRUE(decoded_key.Deserialize(serialized.data() + DataBlockEntry::kHeaderSize, new_entry.key_length));
    EXPECT_EQ(decoded_key.user_key(), user_key);
    EXPECT_EQ(decoded_key.version(), version);

    // Verify value
    std::string decoded_value(
        reinterpret_cast<const char*>(serialized.data() + DataBlockEntry::kHeaderSize + new_entry.key_length),
        new_entry.value_length);
    EXPECT_EQ(decoded_value, "test_value");
}

TEST_F(SSTableFormatTest, DataBlockBuilderWithVersioning) {
    DataBlockBuilder builder;

    // Add entries with different versions of the same key
    std::string key = "test_key";
    HybridTime version1(1000);
    HybridTime version2(2000);
    DataChunk value1 = DataChunk::FromString("value1");
    DataChunk value2 = DataChunk::FromString("value2");

    EXPECT_TRUE(builder.Add(key, version2, value2));  // Newer version
    EXPECT_TRUE(builder.Add(key, version1, value1));  // Older version

    auto block = builder.Finish();
    ASSERT_FALSE(block.empty());

    // Verify block footer
    BlockFooter footer;
    size_t footer_offset = block.size() - BlockFooter::kFooterSize;
    ASSERT_TRUE(footer.Deserialize(block.data() + footer_offset, BlockFooter::kFooterSize));
    EXPECT_EQ(footer.entry_count, 2);

    // Verify entries are stored in version order
    size_t offset = 0;
    for (int i = 0; i < 2; i++) {
        DataBlockEntry entry;
        ASSERT_TRUE(entry.DeserializeHeader(block.data() + offset, DataBlockEntry::kHeaderSize));
        offset += DataBlockEntry::kHeaderSize;

        InternalKey decoded_key;
        ASSERT_TRUE(decoded_key.Deserialize(block.data() + offset, entry.key_length));
        offset += entry.key_length;

        std::string decoded_value(reinterpret_cast<const char*>(block.data() + offset), entry.value_length);
        offset += entry.value_length;

        if (i == 0) {
            // First entry should be newer version
            EXPECT_EQ(decoded_key.version(), version2);
            EXPECT_EQ(decoded_value, "value2");
        } else {
            // Second entry should be older version
            EXPECT_EQ(decoded_key.version(), version1);
            EXPECT_EQ(decoded_value, "value1");
        }
    }
}

TEST_F(SSTableFormatTest, DataBlockEntryTombstone) {
    DataBlockEntry entry;

    // Verify default entry is not a tombstone
    EXPECT_FALSE(entry.IsTombstone());

    // Set tombstone flag and verify
    entry.flags = DataBlockEntry::FLAG_TOMBSTONE;
    EXPECT_TRUE(entry.IsTombstone());

    // Set multiple flags including tombstone
    entry.flags = DataBlockEntry::FLAG_TOMBSTONE | 0x02;
    EXPECT_TRUE(entry.IsTombstone());

    // Clear tombstone flag and verify
    entry.flags = 0x02;
    EXPECT_FALSE(entry.IsTombstone());
}

TEST_F(SSTableFormatTest, DataBlockBuilderWithTombstone) {
    DataBlockBuilder builder;
    HybridTime version1(100);
    HybridTime version2(200);

    // Add regular key-value pair
    ASSERT_TRUE(builder.Add("key1", version1, DataChunk::FromString("value1")));

    // Add tombstone for the same key with newer version
    ASSERT_TRUE(builder.Add("key1", version2, DataChunk::FromString(""), true /* is_tombstone */));

    // Add another key with tombstone
    ASSERT_TRUE(builder.Add("key2", version1, DataChunk::FromString(""), true /* is_tombstone */));

    // Finish block
    auto block = builder.Finish();
    ASSERT_FALSE(block.empty());

    // Verify block footer
    BlockFooter footer;
    size_t footer_offset = block.size() - BlockFooter::kFooterSize;
    ASSERT_TRUE(footer.Deserialize(block.data() + footer_offset, BlockFooter::kFooterSize));
    EXPECT_EQ(footer.entry_count, 3);

    // Verify entries
    size_t offset = 0;
    for (int i = 0; i < 3; i++) {
        DataBlockEntry entry;
        ASSERT_TRUE(entry.DeserializeHeader(block.data() + offset, DataBlockEntry::kHeaderSize));
        offset += DataBlockEntry::kHeaderSize;

        InternalKey decoded_key;
        ASSERT_TRUE(decoded_key.Deserialize(block.data() + offset, entry.key_length));
        offset += entry.key_length;

        std::string decoded_value(reinterpret_cast<const char*>(block.data() + offset), entry.value_length);
        offset += entry.value_length;

        if (i == 0) {
            // First entry should be key1 with version1
            EXPECT_EQ(decoded_key.user_key(), "key1");
            EXPECT_EQ(decoded_key.version(), version1);
            EXPECT_EQ(decoded_value, "value1");
            EXPECT_FALSE(entry.IsTombstone());
        } else if (i == 1) {
            // Second entry should be key1 with version2 (tombstone)
            EXPECT_EQ(decoded_key.user_key(), "key1");
            EXPECT_EQ(decoded_key.version(), version2);
            EXPECT_TRUE(entry.IsTombstone());
        } else {
            // Third entry should be key2 with version1 (tombstone)
            EXPECT_EQ(decoded_key.user_key(), "key2");
            EXPECT_EQ(decoded_key.version(), version1);
            EXPECT_TRUE(entry.IsTombstone());
        }
    }
}

}  // namespace
