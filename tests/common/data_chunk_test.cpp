#include "common/data_chunk.h"

#include <gtest/gtest.h>

using namespace pond::common;

TEST(DataChunkTest, DefaultConstructor) {
    DataChunk chunk;
    EXPECT_TRUE(chunk.Empty());
    EXPECT_EQ(chunk.Size(), 0);
}

TEST(DataChunkTest, VectorConstructor) {
    std::vector<uint8_t> data = {1, 2, 3, 4, 5};
    DataChunk chunk(data);
    EXPECT_EQ(chunk.Size(), 5);
    EXPECT_FALSE(chunk.Empty());
    EXPECT_EQ(chunk.AsVector(), data);
}

TEST(DataChunkTest, SizeConstructor) {
    size_t size = 10;
    DataChunk chunk(size);
    EXPECT_EQ(chunk.Size(), size);
    EXPECT_FALSE(chunk.Empty());
}

TEST(DataChunkTest, RawDataConstructor) {
    uint8_t data[] = {1, 2, 3, 4, 5};
    DataChunk chunk(data, 5);
    EXPECT_EQ(chunk.Size(), 5);
    EXPECT_FALSE(chunk.Empty());
    EXPECT_EQ(std::memcmp(chunk.Data(), data, 5), 0);
}

TEST(DataChunkTest, AppendOperations) {
    DataChunk chunk;

    // Test append raw data
    uint8_t data1[] = {1, 2, 3};
    chunk.Append(data1, 3);
    EXPECT_EQ(chunk.Size(), 3);

    // Test append vector
    std::vector<uint8_t> data2 = {4, 5, 6};
    chunk.Append(data2);
    EXPECT_EQ(chunk.Size(), 6);

    // Test append another chunk
    DataChunk other({7, 8, 9});
    chunk.Append(other);
    EXPECT_EQ(chunk.Size(), 9);

    // Verify final content
    std::vector<uint8_t> expected = {1, 2, 3, 4, 5, 6, 7, 8, 9};
    EXPECT_EQ(chunk.AsVector(), expected);
}

TEST(DataChunkTest, StringConversion) {
    std::string test_str = "Hello, World!";
    DataChunk chunk = DataChunk::FromString(test_str);
    EXPECT_EQ(chunk.Size(), test_str.size());
    EXPECT_EQ(chunk.ToString(), test_str);
}

TEST(DataChunkTest, ResizeOperations) {
    DataChunk chunk({1, 2, 3});

    // Test resize larger
    chunk.Resize(5);
    EXPECT_EQ(chunk.Size(), 5);

    // Test resize smaller
    chunk.Resize(2);
    EXPECT_EQ(chunk.Size(), 2);
    EXPECT_EQ(chunk.Data()[0], 1);
    EXPECT_EQ(chunk.Data()[1], 2);
}

TEST(DataChunkTest, ClearOperation) {
    DataChunk chunk({1, 2, 3});
    EXPECT_FALSE(chunk.Empty());

    chunk.Clear();
    EXPECT_TRUE(chunk.Empty());
    EXPECT_EQ(chunk.Size(), 0);
}

TEST(DataChunkTest, SpanAccess) {
    std::vector<uint8_t> data = {1, 2, 3, 4, 5};
    DataChunk chunk(data);

    auto span = chunk.Span();
    EXPECT_EQ(span.size(), data.size());
    EXPECT_EQ(std::memcmp(span.data(), data.data(), data.size()), 0);
}

TEST(DataChunkTest, Equality) {
    DataChunk chunk1({1, 2, 3});
    DataChunk chunk2({1, 2, 3});
    DataChunk chunk3({1, 2, 3, 4});

    EXPECT_EQ(chunk1, chunk2);
    EXPECT_NE(chunk1, chunk3);
}

TEST(DataChunkTest, PreAllocationAndCapacity) {
    // Test default constructor pre-allocation
    DataChunk chunk;
    EXPECT_EQ(chunk.Size(), 0);
    EXPECT_EQ(chunk.Capacity(), DataChunk::DEFAULT_INITIAL_CAPACITY);
    EXPECT_TRUE(chunk.Empty());

    // Test that Clear preserves capacity
    chunk.WriteString("test data");
    size_t capacityBeforeClear = chunk.Capacity();
    chunk.Clear();
    EXPECT_EQ(chunk.Size(), 0);
    EXPECT_EQ(chunk.Capacity(), capacityBeforeClear);

    // Test capacity growth
    std::string largeString(100, 'x');  // 100 bytes string
    chunk.WriteString(largeString);
    EXPECT_GE(chunk.Capacity(), largeString.size() + sizeof(uint32_t));  // +4 for string length
    EXPECT_EQ(chunk.Size(), largeString.size() + sizeof(uint32_t));
}

TEST(DataChunkTest, SerializationPrimitives) {
    DataChunk chunk;

    // Write various primitives
    chunk.WriteUInt8(0xFF);
    chunk.WriteUInt16(0xFFFF);
    chunk.WriteUInt32(0xFFFFFFFF);
    chunk.WriteUInt64(0xFFFFFFFFFFFFFFFF);
    chunk.WriteInt8(-128);
    chunk.WriteInt16(-32768);
    chunk.WriteInt32(-2147483648);
    chunk.WriteInt64(-9223372036854775807);
    chunk.WriteBool(true);
    chunk.WriteBool(false);

    size_t offset = 0;

    // Read and verify
    EXPECT_EQ(chunk.ReadUInt8(offset), 0xFF);
    EXPECT_EQ(chunk.ReadUInt16(offset), 0xFFFF);
    EXPECT_EQ(chunk.ReadUInt32(offset), 0xFFFFFFFF);
    EXPECT_EQ(chunk.ReadUInt64(offset), 0xFFFFFFFFFFFFFFFF);
    EXPECT_EQ(chunk.ReadInt8(offset), -128);
    EXPECT_EQ(chunk.ReadInt16(offset), -32768);
    EXPECT_EQ(chunk.ReadInt32(offset), -2147483648);
    EXPECT_EQ(chunk.ReadInt64(offset), -9223372036854775807);
    EXPECT_TRUE(chunk.ReadBool(offset));
    EXPECT_FALSE(chunk.ReadBool(offset));
}

TEST(DataChunkTest, StringSerialization) {
    DataChunk chunk;

    // Test empty string
    std::string empty;
    chunk.WriteString(empty);

    // Test normal string
    std::string normal = "Hello, World!";
    chunk.WriteString(normal);

    // Test string with special characters
    std::string special = "Special\0Characters\nand\tTabs";
    chunk.WriteString(std::string_view(special.data(), special.size()));

    // Test long string
    std::string long_str(1000, 'x');
    chunk.WriteString(long_str);

    size_t offset = 0;

    // Verify all strings
    EXPECT_EQ(chunk.ReadString(offset), empty);
    EXPECT_EQ(chunk.ReadString(offset), normal);
    EXPECT_EQ(chunk.ReadString(offset), special);
    EXPECT_EQ(chunk.ReadString(offset), long_str);
}

TEST(DataChunkTest, BoundaryChecks) {
    DataChunk chunk;
    chunk.WriteUInt32(123);

    size_t offset = 0;
    chunk.ReadUInt32(offset);  // Should succeed

    // Reading past the end should throw
    EXPECT_THROW(chunk.ReadUInt8(offset), std::out_of_range);
    EXPECT_THROW(chunk.ReadUInt16(offset), std::out_of_range);
    EXPECT_THROW(chunk.ReadUInt32(offset), std::out_of_range);
    EXPECT_THROW(chunk.ReadUInt64(offset), std::out_of_range);
    EXPECT_THROW(chunk.ReadString(offset), std::out_of_range);
}

TEST(DataChunkTest, AppendAndGrowth) {
    DataChunk chunk;
    size_t initial_capacity = chunk.Capacity();

    // Test that small appends don't cause reallocation
    std::string small_data = "small";
    chunk.WriteString(small_data);
    EXPECT_EQ(chunk.Capacity(), initial_capacity);

    // Test that large appends grow capacity
    std::string large_data(initial_capacity * 2, 'x');
    chunk.WriteString(large_data);
    EXPECT_GT(chunk.Capacity(), initial_capacity);

    // Verify all data is intact
    size_t offset = 0;
    EXPECT_EQ(chunk.ReadString(offset), small_data);
    EXPECT_EQ(chunk.ReadString(offset), large_data);
}

TEST(DataChunkTest, MixedSerialization) {
    DataChunk chunk;

    // Write mixed data types
    chunk.WriteUInt32(0xDEADBEEF);
    chunk.WriteString("test");
    chunk.WriteBool(true);
    chunk.WriteInt64(-42);
    chunk.WriteString("end");

    // Read and verify in same order
    size_t offset = 0;
    EXPECT_EQ(chunk.ReadUInt32(offset), 0xDEADBEEF);
    EXPECT_EQ(chunk.ReadString(offset), "test");
    EXPECT_TRUE(chunk.ReadBool(offset));
    EXPECT_EQ(chunk.ReadInt64(offset), -42);
    EXPECT_EQ(chunk.ReadString(offset), "end");
}

TEST(DataChunkTest, CopyAndMove) {
    DataChunk original;
    original.WriteString("test data");
    original.WriteUInt32(42);

    // Test copy constructor
    DataChunk copied = original;
    EXPECT_EQ(copied.Size(), original.Size());
    EXPECT_EQ(copied.Capacity(), original.Capacity());

    size_t offset1 = 0, offset2 = 0;
    EXPECT_EQ(copied.ReadString(offset1), original.ReadString(offset2));
    EXPECT_EQ(copied.ReadUInt32(offset1), original.ReadUInt32(offset2));

    // Test move constructor
    DataChunk moved = std::move(copied);
    offset1 = 0;
    EXPECT_EQ(moved.Size(), original.Size());
    EXPECT_EQ(moved.ReadString(offset1), "test data");
    EXPECT_EQ(moved.ReadUInt32(offset1), 42);
}
