#include <gtest/gtest.h>
#include "common/data_chunk.h"

using namespace pond::common;

TEST(DataChunkTest, DefaultConstructor) {
    DataChunk chunk;
    EXPECT_TRUE(chunk.empty());
    EXPECT_EQ(chunk.size(), 0);
}

TEST(DataChunkTest, VectorConstructor) {
    std::vector<uint8_t> data = {1, 2, 3, 4, 5};
    DataChunk chunk(data);
    EXPECT_EQ(chunk.size(), 5);
    EXPECT_FALSE(chunk.empty());
    EXPECT_EQ(chunk.asVector(), data);
}

TEST(DataChunkTest, SizeConstructor) {
    size_t size = 10;
    DataChunk chunk(size);
    EXPECT_EQ(chunk.size(), size);
    EXPECT_FALSE(chunk.empty());
}

TEST(DataChunkTest, RawDataConstructor) {
    uint8_t data[] = {1, 2, 3, 4, 5};
    DataChunk chunk(data, 5);
    EXPECT_EQ(chunk.size(), 5);
    EXPECT_FALSE(chunk.empty());
    EXPECT_EQ(std::memcmp(chunk.data(), data, 5), 0);
}

TEST(DataChunkTest, AppendOperations) {
    DataChunk chunk;
    
    // Test append raw data
    uint8_t data1[] = {1, 2, 3};
    chunk.append(data1, 3);
    EXPECT_EQ(chunk.size(), 3);
    
    // Test append vector
    std::vector<uint8_t> data2 = {4, 5, 6};
    chunk.append(data2);
    EXPECT_EQ(chunk.size(), 6);
    
    // Test append another chunk
    DataChunk other({7, 8, 9});
    chunk.append(other);
    EXPECT_EQ(chunk.size(), 9);
    
    // Verify final content
    std::vector<uint8_t> expected = {1, 2, 3, 4, 5, 6, 7, 8, 9};
    EXPECT_EQ(chunk.asVector(), expected);
}

TEST(DataChunkTest, StringConversion) {
    std::string test_str = "Hello, World!";
    DataChunk chunk = DataChunk::fromString(test_str);
    EXPECT_EQ(chunk.size(), test_str.size());
    EXPECT_EQ(chunk.toString(), test_str);
}

TEST(DataChunkTest, ResizeOperations) {
    DataChunk chunk({1, 2, 3});
    
    // Test resize larger
    chunk.resize(5);
    EXPECT_EQ(chunk.size(), 5);
    
    // Test resize smaller
    chunk.resize(2);
    EXPECT_EQ(chunk.size(), 2);
    EXPECT_EQ(chunk.data()[0], 1);
    EXPECT_EQ(chunk.data()[1], 2);
}

TEST(DataChunkTest, ClearOperation) {
    DataChunk chunk({1, 2, 3});
    EXPECT_FALSE(chunk.empty());
    
    chunk.clear();
    EXPECT_TRUE(chunk.empty());
    EXPECT_EQ(chunk.size(), 0);
}

TEST(DataChunkTest, SpanAccess) {
    std::vector<uint8_t> data = {1, 2, 3, 4, 5};
    DataChunk chunk(data);
    
    auto span = chunk.span();
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
