#include "common/crc.h"

#include <string>

#include <gtest/gtest.h>

namespace pond::common {

TEST(CRCTest, EmptyString) {
    std::string empty;
    EXPECT_EQ(CRC32::Calculate(empty), 0x00000000);
}

TEST(CRCTest, SimpleString) {
    std::string data = "123456789";
    // Known CRC32 value for "123456789" using IEEE polynomial
    EXPECT_EQ(CRC32::Calculate(data), 0xCBF43926);
}

TEST(CRCTest, BinaryData) {
    uint8_t data[] = {0x00, 0xFF, 0x55, 0xAA};
    EXPECT_NE(CRC32::Calculate(data, sizeof(data)), 0);
}

TEST(CRCTest, Consistency) {
    std::string data1 = "test data";
    std::string data2 = "test data";
    EXPECT_EQ(CRC32::Calculate(data1), CRC32::Calculate(data2));
}

TEST(CRCTest, DifferentData) {
    std::string data1 = "test data 1";
    std::string data2 = "test data 2";
    EXPECT_NE(CRC32::Calculate(data1), CRC32::Calculate(data2));
}

TEST(CRCTest, LongString) {
    std::string long_data(1000000, 'a');  // 1MB of 'a' characters
    EXPECT_NE(CRC32::Calculate(long_data), 0);
}

}  // namespace pond::common
