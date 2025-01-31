#include "kv/record.h"

#include <memory>

#include <gtest/gtest.h>

namespace pond::kv {

class RecordTest : public ::testing::Test {
protected:
    void SetUp() override {
        std::vector<ColumnSchema> columns = {{"id", ColumnType::INT32, false},
                                             {"name", ColumnType::STRING, true},
                                             {"age", ColumnType::INT32, true},
                                             {"salary", ColumnType::DOUBLE, true},
                                             {"is_active", ColumnType::BOOLEAN, true},
                                             {"data", ColumnType::BINARY, true}};
        schema = std::make_shared<Schema>(columns);
    }

    std::shared_ptr<Schema> schema;
};

TEST_F(RecordTest, BasicOperations) {
    Record record(schema);

    // Test setting values
    record.Set(0, int32_t(1));
    record.Set(1, std::string("John Doe"));
    record.Set(2, int32_t(30));
    record.Set(3, 75000.50);
    record.Set(4, true);
    record.Set(5, common::DataChunk(reinterpret_cast<const uint8_t*>("binary data"), 10));

    // Test getting values
    EXPECT_EQ(record.Get<int32_t>(0).value(), 1);
    EXPECT_EQ(record.Get<std::string>(1).value(), "John Doe");
    EXPECT_EQ(record.Get<int32_t>(2).value(), 30);
    EXPECT_EQ(record.Get<double>(3).value(), 75000.50);
    EXPECT_EQ(record.Get<bool>(4).value(), true);

    auto binary_result = record.Get<common::DataChunk>(5);
    EXPECT_TRUE(binary_result.ok());
    EXPECT_EQ(binary_result.value().size(), 10);
    EXPECT_EQ(std::memcmp(binary_result.value().data(), "binary data", 10), 0);
}

TEST_F(RecordTest, NullValues) {
    Record record(schema);

    // Only set some values, leaving others as null
    record.Set(0, int32_t(1));  // id is non-nullable
    record.Set(1, std::string("John Doe"));

    // Check null status
    EXPECT_FALSE(record.IsNull(0));  // id is non-nullable
    EXPECT_FALSE(record.IsNull(1));  // we set this
    EXPECT_TRUE(record.IsNull(2));   // age is null
    EXPECT_TRUE(record.IsNull(3));   // salary is null
    EXPECT_TRUE(record.IsNull(4));   // is_active is null

    // Try to get null values
    EXPECT_FALSE(record.Get<int32_t>(2).ok());
    EXPECT_FALSE(record.Get<double>(3).ok());
    EXPECT_FALSE(record.Get<bool>(4).ok());
}

TEST_F(RecordTest, SerializationDeserialization) {
    Record original(schema);
    original.Set(0, int32_t(1));
    original.Set(1, std::string("John Doe"));
    original.Set(2, int32_t(30));
    original.Set(3, 75000.50);
    original.Set(4, true);

    // Serialize
    common::DataChunk serialized = original.Serialize();
    EXPECT_GT(serialized.size(), 0);

    // Deserialize
    auto result = Record::Deserialize(serialized, schema);
    EXPECT_TRUE(result.ok());
    auto deserialized = std::move(result).value();

    // Verify all values match
    EXPECT_EQ(deserialized->Get<int32_t>(0).value(), 1);
    EXPECT_EQ(deserialized->Get<std::string>(1).value(), "John Doe");
    EXPECT_EQ(deserialized->Get<int32_t>(2).value(), 30);
    EXPECT_EQ(deserialized->Get<double>(3).value(), 75000.50);
    EXPECT_EQ(deserialized->Get<bool>(4).value(), true);

    // Verify null status matches
    for (size_t i = 0; i < schema->num_columns(); i++) {
        EXPECT_EQ(original.IsNull(i), deserialized->IsNull(i));
    }
}

TEST_F(RecordTest, InvalidOperations) {
    Record record(schema);

    // Try to set wrong type
    EXPECT_THROW(record.Set(0, std::string("wrong type")), std::runtime_error);
    EXPECT_THROW(record.Set(1, 42), std::runtime_error);

    // Try to get wrong type
    record.Set(0, int32_t(1));
    EXPECT_THROW(record.Get<std::string>(0), std::runtime_error);
    EXPECT_THROW(record.Get<double>(0), std::runtime_error);

    // Try to access invalid column index
    EXPECT_THROW(record.Set(10, int32_t(1)), std::out_of_range);
    EXPECT_THROW(record.Get<int32_t>(10), std::out_of_range);
    EXPECT_THROW(record.IsNull(10), std::out_of_range);
}

TEST_F(RecordTest, SchemaOperations) {
    // Test schema column lookup
    EXPECT_EQ(schema->GetColumnIndex("id"), 0);
    EXPECT_EQ(schema->GetColumnIndex("name"), 1);
    EXPECT_EQ(schema->GetColumnIndex("nonexistent"), -1);

    // Test schema column access
    EXPECT_EQ(schema->columns()[0].name, "id");
    EXPECT_EQ(schema->columns()[0].type, ColumnType::INT32);
    EXPECT_FALSE(schema->columns()[0].nullable);

    EXPECT_EQ(schema->columns()[1].name, "name");
    EXPECT_EQ(schema->columns()[1].type, ColumnType::STRING);
    EXPECT_TRUE(schema->columns()[1].nullable);
}

TEST_F(RecordTest, LargeValues) {
    Record record(schema);

    // Test with large string
    std::string large_string(1024 * 1024, 'x');  // 1MB string
    record.Set(1, large_string);
    EXPECT_EQ(record.Get<std::string>(1).value(), large_string);

    // Test with large binary data
    std::vector<uint8_t> large_binary(1024 * 1024, 0xFF);  // 1MB binary
    record.Set(5, common::DataChunk(large_binary.data(), large_binary.size()));
    auto binary_result = record.Get<common::DataChunk>(5);
    EXPECT_TRUE(binary_result.ok());
    EXPECT_EQ(binary_result.value().size(), large_binary.size());
    EXPECT_EQ(std::memcmp(binary_result.value().data(), large_binary.data(), large_binary.size()), 0);
}

}  // namespace pond::kv
