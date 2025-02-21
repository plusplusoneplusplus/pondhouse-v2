#include <gtest/gtest.h>

#include "common/data_chunk.h"
#include "common/data_chunk_writer.h"
#include "proto/kv.pb.h"  // Generated protobuf header

namespace pond::proto {
class ProtoMetadataTest : public ::testing::Test {
protected:
    void SetUp() override {}
};

TEST_F(ProtoMetadataTest, TestMetadataEntry) {
    // Create a test entry
    TestMetadataEntry entry;
    entry.set_test_field("test_value");
    entry.set_test_number(42);
    entry.set_test_flag(true);

    // Serialize to string
    std::string serialized;
    ASSERT_TRUE(entry.SerializeToString(&serialized));

    // Parse from string
    TestMetadataEntry parsed;
    ASSERT_TRUE(parsed.ParseFromString(serialized));

    // Verify fields
    EXPECT_EQ(parsed.test_field(), "test_value");
    EXPECT_EQ(parsed.test_number(), 42);
    EXPECT_TRUE(parsed.test_flag());
}

TEST_F(ProtoMetadataTest, TestMetadataEntryDataChunkConversion) {
    // Create a test entry
    TestMetadataEntry entry;
    entry.set_test_field("test_value");
    entry.set_test_number(42);
    entry.set_test_flag(true);

    // Test direct serialization to DataChunk
    common::DataChunk direct_chunk;
    {
        common::DataChunkOutputStream output_stream(direct_chunk);
        ASSERT_TRUE(entry.SerializeToZeroCopyStream(&output_stream));
    }

    // Test parsing from DataChunk using ZeroCopyStream
    TestMetadataEntry parsed;
    ASSERT_TRUE(parsed.ParseFromArray(direct_chunk.Data(), direct_chunk.Size()));

    // Verify fields
    EXPECT_EQ(parsed.test_field(), "test_value");
    EXPECT_EQ(parsed.test_number(), 42);
    EXPECT_TRUE(parsed.test_flag());

    // Test empty entry with custom stream
    TestMetadataEntry empty_entry;
    common::DataChunk empty_chunk;
    {
        common::DataChunkOutputStream empty_stream(empty_chunk);
        ASSERT_TRUE(empty_entry.SerializeToZeroCopyStream(&empty_stream));
    }

    TestMetadataEntry parsed_empty;
    ASSERT_TRUE(parsed_empty.ParseFromArray(empty_chunk.Data(), empty_chunk.Size()));
    EXPECT_TRUE(parsed_empty.test_field().empty());
    EXPECT_EQ(parsed_empty.test_number(), 0);
    EXPECT_FALSE(parsed_empty.test_flag());

    // Test partial entry with custom stream
    TestMetadataEntry partial_entry;
    partial_entry.set_test_field("partial");
    // Leave other fields at default values
    common::DataChunk partial_chunk;
    {
        common::DataChunkOutputStream partial_stream(partial_chunk);
        ASSERT_TRUE(partial_entry.SerializeToZeroCopyStream(&partial_stream));
    }

    TestMetadataEntry parsed_partial;
    ASSERT_TRUE(parsed_partial.ParseFromArray(partial_chunk.Data(), partial_chunk.Size()));
    EXPECT_EQ(parsed_partial.test_field(), "partial");
    EXPECT_EQ(parsed_partial.test_number(), 0);
    EXPECT_FALSE(parsed_partial.test_flag());
}

TEST_F(ProtoMetadataTest, TableMetadataEntry) {
    // Create a metadata entry
    TableMetadataEntry entry;
    entry.set_op_type(METADATA_OP_TYPE_CREATE_SSTABLE);
    entry.set_wal_sequence(123);
    entry.add_wal_files("wal.1");
    entry.add_wal_files("wal.2");

    // Add a file info
    FileInfo* file = entry.add_added_files();
    file->set_name("test.sst");
    file->set_size(1024);
    file->set_level(0);
    file->set_smallest_key("a");
    file->set_largest_key("z");

    // Serialize to string
    std::string serialized;
    ASSERT_TRUE(entry.SerializeToString(&serialized));

    // Parse from string
    TableMetadataEntry parsed;
    ASSERT_TRUE(parsed.ParseFromString(serialized));

    // Verify fields
    EXPECT_EQ(parsed.op_type(), METADATA_OP_TYPE_CREATE_SSTABLE);
    EXPECT_EQ(parsed.wal_sequence(), 123);
    ASSERT_EQ(parsed.wal_files_size(), 2);
    EXPECT_EQ(parsed.wal_files(0), "wal.1");
    EXPECT_EQ(parsed.wal_files(1), "wal.2");

    // Verify file info
    ASSERT_EQ(parsed.added_files_size(), 1);
    const FileInfo& parsed_file = parsed.added_files(0);
    EXPECT_EQ(parsed_file.name(), "test.sst");
    EXPECT_EQ(parsed_file.size(), 1024);
    EXPECT_EQ(parsed_file.level(), 0);
    EXPECT_EQ(parsed_file.smallest_key(), "a");
    EXPECT_EQ(parsed_file.largest_key(), "z");
}

}  // namespace pond::proto