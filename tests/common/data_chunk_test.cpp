#include <gtest/gtest.h>
#include "common/data_chunk.h"

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
