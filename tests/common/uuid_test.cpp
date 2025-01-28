#include <gtest/gtest.h>
#include <regex>
#include "common/uuid.h"

using namespace pond::common;

TEST(UUIDTest, DefaultConstructor) {
    UUID uuid;
    EXPECT_EQ(uuid.size(), 16);
}

TEST(UUIDTest, HighLowConstructor) {
    uint64_t high = 0x0123456789ABCDEF;
    uint64_t low = 0xFEDCBA9876543210;
    UUID uuid(high, low);
    
    const uint8_t* data = uuid.data();
    uint64_t stored_high = *reinterpret_cast<const uint64_t*>(data);
    uint64_t stored_low = *reinterpret_cast<const uint64_t*>(data + 8);
    
    EXPECT_EQ(stored_high, high);
    EXPECT_EQ(stored_low, low);
}

TEST(UUIDTest, CopyConstructor) {
    UUID original(0x0123456789ABCDEF, 0xFEDCBA9876543210);
    UUID copy(original);
    EXPECT_EQ(original, copy);
}

TEST(UUIDTest, MoveConstructor) {
    UUID original(0x0123456789ABCDEF, 0xFEDCBA9876543210);
    UUID moved(std::move(original));
    EXPECT_EQ(moved, UUID(0x0123456789ABCDEF, 0xFEDCBA9876543210));
}

TEST(UUIDTest, Generate) {
    UUID uuid1, uuid2;
    uuid1.generate();
    uuid2.generate();
    
    // UUIDs should be different
    EXPECT_NE(uuid1, uuid2);
    
    // Check UUID version (should be 4)
    const uint8_t* data1 = uuid1.data();
    EXPECT_EQ(data1[6] & 0xF0, 0x40);
    
    // Check UUID variant (should be RFC4122)
    EXPECT_EQ(data1[8] & 0xC0, 0x80);
}

TEST(UUIDTest, ToString) {
    UUID uuid;
    uuid.generate();
    std::string str = uuid.toString();
    
    // Check UUID string format (8-4-4-4-12 = 36 chars)
    EXPECT_EQ(str.length(), 36);
    
    // Verify UUID format using regex
    std::regex uuid_regex("^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$");
    EXPECT_TRUE(std::regex_match(str, uuid_regex));
}

TEST(UUIDTest, NewUUID) {
    UUID uuid1 = UUID::newUUID();
    UUID uuid2 = UUID::newUUID();
    
    // Generated UUIDs should be different
    EXPECT_NE(uuid1, uuid2);
    
    // Check version and variant
    const uint8_t* data1 = uuid1.data();
    const uint8_t* data2 = uuid2.data();
    
    EXPECT_EQ(data1[6] & 0xF0, 0x40); // Version 4
    EXPECT_EQ(data1[8] & 0xC0, 0x80); // RFC4122 variant
    EXPECT_EQ(data2[6] & 0xF0, 0x40); // Version 4
    EXPECT_EQ(data2[8] & 0xC0, 0x80); // RFC4122 variant
}

TEST(UUIDTest, Equality) {
    UUID uuid1(0x0123456789ABCDEF, 0xFEDCBA9876543210);
    UUID uuid2(0x0123456789ABCDEF, 0xFEDCBA9876543210);
    UUID uuid3(0x0123456789ABCDEF, 0xFEDCBA9876543211);
    
    EXPECT_EQ(uuid1, uuid2);
    EXPECT_NE(uuid1, uuid3);
}

TEST(UUIDTest, Size) {
    UUID uuid;
    EXPECT_EQ(uuid.size(), 16); // UUID should always be 16 bytes
}
