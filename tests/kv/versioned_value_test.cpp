#include "kv/versioned_value.h"

#include <string>

#include <gtest/gtest.h>

namespace pond::kv {

class VersionedValueTest : public ::testing::Test {
protected:
    common::DataChunk CreateTestValue(const std::string& str) {
        return common::DataChunk(reinterpret_cast<const uint8_t*>(str.data()), str.size());
    }
};

TEST_F(VersionedValueTest, BasicOperationsWithDataChunk) {
    // Create initial version
    auto value1 = CreateTestValue("value1");
    auto version1 = common::HybridTime(1000);
    uint64_t txn1 = 1;

    auto v1 = std::make_shared<DataChunkVersionedValue>(value1, version1, txn1);

    // Check basic accessors
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(v1->value().Data()), v1->value().Size()), "value1");
    EXPECT_EQ(v1->version(), version1);
    EXPECT_EQ(v1->txn_id(), txn1);
    EXPECT_FALSE(v1->is_deleted());
    EXPECT_EQ(v1->prev_version(), nullptr);
}

TEST_F(VersionedValueTest, BasicOperationsWithString) {
    // Create initial version with string type
    auto version1 = common::HybridTime(1000);
    uint64_t txn1 = 1;

    auto v1 = std::make_shared<VersionedValue<std::string>>("value1", version1, txn1);

    // Check basic accessors
    EXPECT_EQ(v1->value(), "value1");
    EXPECT_EQ(v1->version(), version1);
    EXPECT_EQ(v1->txn_id(), txn1);
    EXPECT_FALSE(v1->is_deleted());
    EXPECT_EQ(v1->prev_version(), nullptr);
}

TEST_F(VersionedValueTest, VersionChainWithDataChunk) {
    // Create version chain: v1 -> v2 -> v3
    auto value1 = CreateTestValue("value1");
    auto version1 = common::HybridTime(1000);
    auto v1 = std::make_shared<DataChunkVersionedValue>(value1, version1, 1);

    auto value2 = CreateTestValue("value2");
    auto version2 = common::HybridTime(1001);
    auto v2 = v1->CreateNewVersion(value2, version2, 2);

    auto value3 = CreateTestValue("value3");
    auto version3 = common::HybridTime(1002);
    auto v3 = v2->CreateNewVersion(value3, version3, 3);

    // Check version chain links
    EXPECT_EQ(v3->prev_version(), v2);
    EXPECT_EQ(v2->prev_version(), v1);
    EXPECT_EQ(v1->prev_version(), nullptr);

    // Check values
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(v1->value().Data()), v1->value().Size()), "value1");
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(v2->value().Data()), v2->value().Size()), "value2");
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(v3->value().Data()), v3->value().Size()), "value3");
}

TEST_F(VersionedValueTest, VersionChainWithString) {
    // Create version chain with string type: v1 -> v2 -> v3
    auto version1 = common::HybridTime(1000);
    auto v1 = std::make_shared<VersionedValue<std::string>>("value1", version1, 1);

    auto version2 = common::HybridTime(1001);
    auto v2 = v1->CreateNewVersion("value2", version2, 2);

    auto version3 = common::HybridTime(1002);
    auto v3 = v2->CreateNewVersion("value3", version3, 3);

    // Check version chain links
    EXPECT_EQ(v3->prev_version(), v2);
    EXPECT_EQ(v2->prev_version(), v1);
    EXPECT_EQ(v1->prev_version(), nullptr);

    // Check values
    EXPECT_EQ(v1->value(), "value1");
    EXPECT_EQ(v2->value(), "value2");
    EXPECT_EQ(v3->value(), "value3");
}

TEST_F(VersionedValueTest, TimeBasedVisibility) {
    // Create version chain with different timestamps
    auto v1 = std::make_shared<DataChunkVersionedValue>(CreateTestValue("value1"), common::HybridTime(1000), 1);
    auto v2 = v1->CreateNewVersion(CreateTestValue("value2"), common::HybridTime(2000), 2);
    auto v3 = v2->CreateNewVersion(CreateTestValue("value3"), common::HybridTime(3000), 3);

    // Test visibility at different timestamps
    auto result1 = v3->GetValueAt(common::HybridTime(1000));
    ASSERT_TRUE(result1.has_value());
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(result1->get().Data()), result1->get().Size()), "value1");

    auto result2 = v3->GetValueAt(common::HybridTime(2000));
    ASSERT_TRUE(result2.has_value());
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(result2->get().Data()), result2->get().Size()), "value2");

    auto result3 = v3->GetValueAt(common::HybridTime(3000));
    ASSERT_TRUE(result3.has_value());
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(result3->get().Data()), result3->get().Size()), "value3");

    // Test timestamp before first version
    auto result_early = v3->GetValueAt(common::HybridTime(999));
    EXPECT_FALSE(result_early.has_value());
}

TEST_F(VersionedValueTest, TransactionVisibility) {
    // Create version chain with different transaction IDs using string type
    auto v1 = std::make_shared<VersionedValue<std::string>>("value1", common::HybridTime(1000), 1);
    auto v2 = v1->CreateNewVersion("value2", common::HybridTime(2000), 2);
    auto v3 = v2->CreateNewVersion("value3", common::HybridTime(3000), 3);

    // Test visibility for different transactions
    auto result1 = v3->GetValueForTxn(1);
    ASSERT_TRUE(result1.has_value());
    EXPECT_EQ(result1->get(), "value1");

    auto result2 = v3->GetValueForTxn(2);
    ASSERT_TRUE(result2.has_value());
    EXPECT_EQ(result2->get(), "value2");

    // Test non-existent transaction
    auto result_missing = v3->GetValueForTxn(999);
    EXPECT_FALSE(result_missing.has_value());
}

TEST_F(VersionedValueTest, DeletionMarkers) {
    // Create version chain with a deletion using string type
    auto v1 = std::make_shared<VersionedValue<std::string>>("value1", common::HybridTime(1000), 1);
    auto v2 = v1->CreateDeletionMarker(common::HybridTime(2000), 2);
    auto v3 = v2->CreateNewVersion("value3", common::HybridTime(3000), 3);

    // Test visibility at different timestamps
    auto result1 = v3->GetValueAt(common::HybridTime(1000));
    ASSERT_TRUE(result1.has_value());
    EXPECT_EQ(result1->get(), "value1");

    // Value should be invisible at deletion time
    auto result2 = v3->GetValueAt(common::HybridTime(2000));
    EXPECT_FALSE(result2.has_value());

    // New value should be visible after deletion
    auto result3 = v3->GetValueAt(common::HybridTime(3000));
    ASSERT_TRUE(result3.has_value());
    EXPECT_EQ(result3->get(), "value3");
}

}  // namespace pond::kv