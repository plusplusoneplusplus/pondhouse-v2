#include "common/bloom_filter.h"

#include <random>
#include <string>
#include <unordered_set>

#include <gtest/gtest.h>

#include "common/data_chunk.h"

using namespace pond::common;

class BloomFilterTest : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(BloomFilterTest, BasicOperations) {
    // Create a Bloom filter with 1000 expected items and 1% false positive rate
    BloomFilter filter(1000, 0.01);

    // Test empty filter
    DataChunk item1 = DataChunk::fromString("test1");
    EXPECT_FALSE(filter.mightContain(item1)) << "Empty filter should not contain any items";

    // Test adding and checking items
    filter.add(item1);
    EXPECT_TRUE(filter.mightContain(item1)) << "Filter should contain added item";

    DataChunk item2 = DataChunk::fromString("test2");
    EXPECT_FALSE(filter.mightContain(item2)) << "Filter should not contain non-added item";

    // Test clear operation
    filter.clear();
    EXPECT_FALSE(filter.mightContain(item1)) << "Cleared filter should not contain previously added items";
}

TEST_F(BloomFilterTest, FalsePositiveRate) {
    const size_t num_items = 1000;
    const double target_fp_rate = 0.01;

    BloomFilter filter(num_items, target_fp_rate);

    // Generate random items
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, 1000000);

    std::unordered_set<int> added_items;
    std::vector<DataChunk> test_items;

    // Add items to both filter and set
    for (size_t i = 0; i < num_items; ++i) {
        int value = dis(gen);
        added_items.insert(value);
        filter.add(DataChunk::fromString(std::to_string(value)));
    }

    // Test false positive rate
    size_t false_positives = 0;
    const size_t num_tests = 10000;

    for (size_t i = 0; i < num_tests; ++i) {
        int value = dis(gen);
        if (added_items.find(value) == added_items.end()) {
            // This is a value we didn't add
            if (filter.mightContain(DataChunk::fromString(std::to_string(value)))) {
                ++false_positives;
            }
        }
    }

    double actual_fp_rate = static_cast<double>(false_positives) / static_cast<double>(num_tests);

    EXPECT_LT(actual_fp_rate, target_fp_rate * 2)
        << "False positive rate should be reasonably close to target";
}

TEST_F(BloomFilterTest, Serialization) {
    // Create and populate a filter
    BloomFilter original(100, 0.01);
    std::vector<DataChunk> items = {
        DataChunk::fromString("item1"), DataChunk::fromString("item2"), DataChunk::fromString("item3")};

    for (const auto& item : items) {
        original.add(item);
    }

    // Serialize
    auto serialized_result = original.serialize();
    EXPECT_TRUE(serialized_result.ok()) << "Serialization should succeed";

    // Deserialize
    auto deserialized_result = BloomFilter::deserialize(serialized_result.value());
    EXPECT_TRUE(deserialized_result.ok()) << "Deserialization should succeed";

    auto& deserialized = deserialized_result.value();

    // Verify parameters
    EXPECT_EQ(deserialized.getBitSize(), original.getBitSize()) << "Bit sizes should match";
    EXPECT_EQ(deserialized.getHashFunctionCount(), original.getHashFunctionCount())
        << "Hash function counts should match";

    // Verify contents
    for (const auto& item : items) {
        EXPECT_TRUE(deserialized.mightContain(item))
            << "Deserialized filter should contain all original items";
    }

    // Test invalid deserialization
    DataChunk invalid_data(10);  // Too small
    auto invalid_result = BloomFilter::deserialize(invalid_data);
    EXPECT_FALSE(invalid_result.ok()) << "Should fail to deserialize invalid data";
}

TEST_F(BloomFilterTest, OptimalParameters) {
    // Test various combinations of items and false positive rates
    struct TestCase {
        size_t items;
        double fp_rate;
    };

    std::vector<TestCase> test_cases = {{100, 0.01}, {1000, 0.001}, {10000, 0.0001}};

    for (const auto& test_case : test_cases) {
        BloomFilter filter(test_case.items, test_case.fp_rate);

        // Add the expected number of items
        for (size_t i = 0; i < test_case.items; ++i) {
            filter.add(DataChunk::fromString(std::to_string(i)));
        }

        double actual_fp_rate = filter.getFalsePositiveProbability();
        EXPECT_LE(actual_fp_rate, test_case.fp_rate * 1.5)
            << "Actual false positive rate should be close to target";
    }
}