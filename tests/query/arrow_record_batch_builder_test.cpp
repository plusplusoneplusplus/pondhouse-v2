#include "query/data/arrow_record_batch_builder.h"

#include <random>

#include <arrow/testing/gtest_util.h>
#include <gtest/gtest.h>

#include "test_helper.h"

namespace pond::query {

class ArrowRecordBatchBuilderTest : public ::testing::Test {
protected:
    void SetUp() override {}

    // Helper function to generate random strings
    std::vector<std::string> GenerateRandomStrings(size_t count, size_t avg_length = 10) {
        std::vector<std::string> result;
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> len_dist(1, avg_length * 2);
        std::uniform_int_distribution<> char_dist(32, 126);  // Printable ASCII characters

        for (size_t i = 0; i < count; i++) {
            std::string str;
            size_t len = len_dist(gen);
            for (size_t j = 0; j < len; j++) {
                str += static_cast<char>(char_dist(gen));
            }
            result.push_back(str);
        }
        return result;
    }

    // Helper function to generate random validity vectors
    std::vector<bool> GenerateRandomValidity(size_t count, float null_probability = 0.1) {
        std::vector<bool> result;
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_real_distribution<> dist(0.0, 1.0);

        for (size_t i = 0; i < count; i++) {
            result.push_back(dist(gen) >= null_probability);
        }
        return result;
    }
};

//
// Test Setup:
//      Create a simple record batch with multiple columns of different types
// Test Result:
//      Verify the batch has correct schema and values
//
TEST_F(ArrowRecordBatchBuilderTest, BasicBuildTest) {
    auto result = ArrowRecordBatchBuilder()
                      .AddInt32Column("id", {1, 2, 3})
                      .AddStringColumn("name", {"Alice", "Bob", "Charlie"})
                      .AddDoubleColumn("salary", {50000.0, 60000.0, 70000.0})
                      .Build();

    VERIFY_RESULT(result);
    auto batch = result.value();

    // Verify schema
    ASSERT_EQ(batch->num_columns(), 3);
    ASSERT_EQ(batch->num_rows(), 3);
    ASSERT_EQ(batch->schema()->field(0)->name(), "id");
    ASSERT_EQ(batch->schema()->field(1)->name(), "name");
    ASSERT_EQ(batch->schema()->field(2)->name(), "salary");

    // Verify types
    ASSERT_EQ(batch->schema()->field(0)->type()->id(), arrow::Type::INT32);
    ASSERT_EQ(batch->schema()->field(1)->type()->id(), arrow::Type::STRING);
    ASSERT_EQ(batch->schema()->field(2)->type()->id(), arrow::Type::DOUBLE);

    // Verify values
    auto id_array = std::static_pointer_cast<arrow::Int32Array>(batch->column(0));
    auto name_array = std::static_pointer_cast<arrow::StringArray>(batch->column(1));
    auto salary_array = std::static_pointer_cast<arrow::DoubleArray>(batch->column(2));

    EXPECT_EQ(id_array->Value(0), 1);
    EXPECT_EQ(name_array->GetString(0), "Alice");
    EXPECT_EQ(salary_array->Value(0), 50000.0);

    EXPECT_EQ(id_array->Value(2), 3);
    EXPECT_EQ(name_array->GetString(2), "Charlie");
    EXPECT_EQ(salary_array->Value(2), 70000.0);
}

//
// Test Setup:
//      Create a record batch with null values
// Test Result:
//      Verify null values are handled correctly
//
TEST_F(ArrowRecordBatchBuilderTest, NullValuesTest) {
    auto result = ArrowRecordBatchBuilder()
                      .AddInt32Column("id", {1, 2, 3}, {true, false, true})
                      .AddStringColumn("name", {"Alice", "Bob", "Charlie"}, {true, true, false})
                      .Build();

    VERIFY_RESULT(result);
    auto batch = result.value();

    // Verify nulls in id column
    auto id_array = std::static_pointer_cast<arrow::Int32Array>(batch->column(0));
    EXPECT_FALSE(id_array->IsNull(0));
    EXPECT_TRUE(id_array->IsNull(1));
    EXPECT_FALSE(id_array->IsNull(2));

    // Verify nulls in name column
    auto name_array = std::static_pointer_cast<arrow::StringArray>(batch->column(1));
    EXPECT_FALSE(name_array->IsNull(0));
    EXPECT_FALSE(name_array->IsNull(1));
    EXPECT_TRUE(name_array->IsNull(2));
}

//
// Test Setup:
//      Create a record batch with non-nullable columns
// Test Result:
//      Verify schema reflects non-nullable status
//
TEST_F(ArrowRecordBatchBuilderTest, NonNullableColumnsTest) {
    auto result = ArrowRecordBatchBuilder()
                      .AddInt32Column("id", {1, 2, 3}, {}, common::Nullability::NOT_NULL)
                      .AddStringColumn("name", {"Alice", "Bob", "Charlie"}, {}, common::Nullability::NOT_NULL)
                      .Build();

    VERIFY_RESULT(result);
    auto batch = result.value();

    // Verify schema nullability
    EXPECT_FALSE(batch->schema()->field(0)->nullable());
    EXPECT_FALSE(batch->schema()->field(1)->nullable());
}

//
// Test Setup:
//      Create a record batch with all supported types
// Test Result:
//      Verify all types are handled correctly
//
TEST_F(ArrowRecordBatchBuilderTest, AllTypesTest) {
    auto result = ArrowRecordBatchBuilder()
                      .AddInt32Column("int32_col", {1, 2})
                      .AddInt64Column("int64_col", {100L, 200L})
                      .AddUInt32Column("uint32_col", {1U, 2U})
                      .AddUInt64Column("uint64_col", {100UL, 200UL})
                      .AddFloatColumn("float_col", {1.1f, 2.2f})
                      .AddDoubleColumn("double_col", {1.1, 2.2})
                      .AddBooleanColumn("bool_col", {true, false})
                      .AddStringColumn("string_col", {"one", "two"})
                      .AddTimestampColumn("ts_col", {1000, 2000})
                      .Build();

    VERIFY_RESULT(result);
    auto batch = result.value();

    // Verify number of columns and rows
    ASSERT_EQ(batch->num_columns(), 9);
    ASSERT_EQ(batch->num_rows(), 2);

    // Verify types
    EXPECT_EQ(batch->schema()->field(0)->type()->id(), arrow::Type::INT32);
    EXPECT_EQ(batch->schema()->field(1)->type()->id(), arrow::Type::INT64);
    EXPECT_EQ(batch->schema()->field(2)->type()->id(), arrow::Type::UINT32);
    EXPECT_EQ(batch->schema()->field(3)->type()->id(), arrow::Type::UINT64);
    EXPECT_EQ(batch->schema()->field(4)->type()->id(), arrow::Type::FLOAT);
    EXPECT_EQ(batch->schema()->field(5)->type()->id(), arrow::Type::DOUBLE);
    EXPECT_EQ(batch->schema()->field(6)->type()->id(), arrow::Type::BOOL);
    EXPECT_EQ(batch->schema()->field(7)->type()->id(), arrow::Type::STRING);
    EXPECT_EQ(batch->schema()->field(8)->type()->id(), arrow::Type::TIMESTAMP);
}

//
// Test Setup:
//      Create an empty record batch
// Test Result:
//      Verify empty batch is created correctly
//
TEST_F(ArrowRecordBatchBuilderTest, EmptyBatchTest) {
    auto result = ArrowRecordBatchBuilder().Build();

    VERIFY_RESULT(result);
    auto batch = result.value();

    EXPECT_EQ(batch->num_columns(), 0);
    EXPECT_EQ(batch->num_rows(), 0);
}

//
// Test Setup:
//      Attempt to create a batch with mismatched column lengths
// Test Result:
//      Verify appropriate error is returned
//
TEST_F(ArrowRecordBatchBuilderTest, MismatchedLengthsTest) {
    auto result = ArrowRecordBatchBuilder()
                      .AddInt32Column("id", {1, 2, 3})
                      .AddStringColumn("name", {"Alice", "Bob"})  // One fewer element
                      .Build();

    EXPECT_FALSE(result.ok());
    EXPECT_EQ(result.error().code(), common::ErrorCode::InvalidArgument);
    EXPECT_TRUE(result.error().message().find("different length") != std::string::npos);
}

//
// Test Setup:
//      Create a batch using Arrow builder directly with validity vector of different length than values
// Test Result:
//      Verify behavior when validity vector length doesn't match values length
//
TEST_F(ArrowRecordBatchBuilderTest, DirectBuilderValidityLengthMismatchTest) {
    // Create builder and array directly
    arrow::Int32Builder builder;

    // Values vector has 3 elements
    std::vector<int32_t> values = {1, 2, 3};

    // Validity vector has only 2 elements
    std::vector<bool> validity = {true, false};

    // Append values with validity
    ASSERT_OK(builder.AppendValues(values, validity));

    // Finish building the array
    std::shared_ptr<arrow::Array> array;
    ASSERT_OK(builder.Finish(&array));

    // Add to record batch builder
    auto result = ArrowRecordBatchBuilder().AddColumn("direct_array", array).Build();

    VERIFY_RESULT(result);
    auto batch = result.value();

    // Verify the array
    auto int_array = std::static_pointer_cast<arrow::Int32Array>(batch->column(0));
    EXPECT_EQ(int_array->length(), 2);

    // First value should be valid
    EXPECT_FALSE(int_array->IsNull(0));
    EXPECT_EQ(int_array->Value(0), 1);

    // Second value should be null
    EXPECT_TRUE(int_array->IsNull(1));
}

//
// Test Setup:
//      Create a batch with invalid validity vectors
// Test Result:
//      Verify appropriate error handling
//
TEST_F(ArrowRecordBatchBuilderTest, InvalidValidityVectorTest) {
    auto result =
        ArrowRecordBatchBuilder()
            .AddInt32Column(
                "id", {1, 2, 3}, {true, false})  // Validity vector too short, missing 3rd element considered null
            .Build();

    VERIFY_RESULT(result);
    auto batch = result.value();

    ASSERT_EQ(batch->num_columns(), 1);
    ASSERT_EQ(batch->num_rows(), 2);

    // Verify the column is null
    auto id_array = std::static_pointer_cast<arrow::Int32Array>(batch->column(0));
    EXPECT_FALSE(id_array->IsNull(0));
    EXPECT_TRUE(id_array->IsNull(1));

    result = ArrowRecordBatchBuilder()
                 .AddInt32Column("id", {1, 2, 3}, {true, false, true, false})  // Validity vector too long
                 .Build();

    EXPECT_FALSE(result.ok());
    EXPECT_EQ(result.error().code(), common::ErrorCode::InvalidArgument);
    EXPECT_TRUE(result.error().message().find("Column 'id' has null array") != std::string::npos);
}

//
// Test Setup:
//      Add existing Arrow arrays to the builder
// Test Result:
//      Verify arrays are incorporated correctly
//
TEST_F(ArrowRecordBatchBuilderTest, AddExistingArrayTest) {
    // Create an array directly
    arrow::Int32Builder builder;
    ASSERT_OK(builder.AppendValues({1, 2, 3}));
    std::shared_ptr<arrow::Array> array;
    ASSERT_OK(builder.Finish(&array));

    // Add it to the builder
    auto result =
        ArrowRecordBatchBuilder().AddColumn("direct_array", array).AddInt32Column("normal_column", {4, 5, 6}).Build();

    VERIFY_RESULT(result);
    auto batch = result.value();

    ASSERT_EQ(batch->num_columns(), 2);
    ASSERT_EQ(batch->num_rows(), 3);

    // Verify the directly added array
    auto direct_array = std::static_pointer_cast<arrow::Int32Array>(batch->column(0));
    EXPECT_EQ(direct_array->Value(0), 1);
    EXPECT_EQ(direct_array->Value(1), 2);
    EXPECT_EQ(direct_array->Value(2), 3);
}

//
// Test Setup:
//      Test the NumRows() method during batch construction
// Test Result:
//      Verify correct row count is maintained
//
TEST_F(ArrowRecordBatchBuilderTest, NumRowsTest) {
    ArrowRecordBatchBuilder builder;

    // Initially should be -1
    EXPECT_EQ(builder.NumRows(), -1);

    // Add first column
    builder.AddInt32Column("col1", {1, 2, 3});
    EXPECT_EQ(builder.NumRows(), 3);

    // Add second column
    builder.AddStringColumn("col2", {"a", "b", "c"});
    EXPECT_EQ(builder.NumRows(), 3);

    // Build and verify
    auto result = builder.Build();
    VERIFY_RESULT(result);
    EXPECT_EQ(result.value()->num_rows(), 3);
}

//
// Test Setup:
//      Create a batch with empty vectors
// Test Result:
//      Verify empty vectors are handled correctly
//
TEST_F(ArrowRecordBatchBuilderTest, EmptyVectorsTest) {
    auto result = ArrowRecordBatchBuilder().AddInt32Column("empty_int", {}).AddStringColumn("empty_str", {}).Build();

    VERIFY_RESULT(result);
    auto batch = result.value();

    EXPECT_EQ(batch->num_columns(), 2);
    EXPECT_EQ(batch->num_rows(), 0);
    EXPECT_EQ(batch->schema()->field(0)->name(), "empty_int");
    EXPECT_EQ(batch->schema()->field(1)->name(), "empty_str");
}

//
// Test Setup:
//      Create a batch with duplicate column names
// Test Result:
//      Verify the batch is created successfully (Arrow allows duplicate names)
//
TEST_F(ArrowRecordBatchBuilderTest, DuplicateColumnNamesTest) {
    auto result = ArrowRecordBatchBuilder().AddInt32Column("id", {1, 2}).AddStringColumn("id", {"a", "b"}).Build();

    VERIFY_RESULT(result);
    auto batch = result.value();

    EXPECT_EQ(batch->num_columns(), 2);
    EXPECT_EQ(batch->schema()->field(0)->name(), "id");
    EXPECT_EQ(batch->schema()->field(1)->name(), "id");
}

//
// Test Setup:
//      Create a batch with special characters in column names
// Test Result:
//      Verify special characters are handled correctly
//
TEST_F(ArrowRecordBatchBuilderTest, SpecialCharacterColumnNamesTest) {
    std::vector<std::string> special_names = {
        "column!@#$%", "空白", "column with spaces", "column\twith\ttabs", "column\nwith\nnewlines"};

    ArrowRecordBatchBuilder builder;
    for (const auto& name : special_names) {
        builder.AddInt32Column(name, {1, 2, 3});
    }

    auto result = builder.Build();
    VERIFY_RESULT(result);
    auto batch = result.value();

    EXPECT_EQ(batch->num_columns(), special_names.size());
    for (size_t i = 0; i < special_names.size(); i++) {
        EXPECT_EQ(batch->schema()->field(i)->name(), special_names[i]);
    }
}

//
// Test Setup:
//      Create a batch with extreme values
// Test Result:
//      Verify extreme values are handled correctly
//
TEST_F(ArrowRecordBatchBuilderTest, ExtremeValuesTest) {
    auto result =
        ArrowRecordBatchBuilder()
            .AddInt32Column("int32",
                            {0, 0, 0, std::numeric_limits<int32_t>::min(), std::numeric_limits<int32_t>::max()})
            .AddInt64Column("int64",
                            {0, 0, 0, std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::max()})
            .AddUInt32Column("uint32",
                             {0, 0, 0, std::numeric_limits<uint32_t>::min(), std::numeric_limits<uint32_t>::max()})
            .AddUInt64Column("uint64",
                             {0, 0, 0, std::numeric_limits<uint64_t>::min(), std::numeric_limits<uint64_t>::max()})
            .AddFloatColumn("float",
                            {std::numeric_limits<float>::lowest(),
                             std::numeric_limits<float>::max(),
                             std::numeric_limits<float>::infinity(),
                             -std::numeric_limits<float>::infinity(),
                             std::numeric_limits<float>::quiet_NaN()})
            .AddDoubleColumn("double",
                             {std::numeric_limits<double>::lowest(),
                              std::numeric_limits<double>::max(),
                              std::numeric_limits<double>::infinity(),
                              -std::numeric_limits<double>::infinity(),
                              std::numeric_limits<double>::quiet_NaN()})
            .Build();

    VERIFY_RESULT(result);
    auto batch = result.value();

    // Verify int32 extremes
    auto int32_array = std::static_pointer_cast<arrow::Int32Array>(batch->column(0));
    EXPECT_EQ(int32_array->Value(3), std::numeric_limits<int32_t>::min());
    EXPECT_EQ(int32_array->Value(4), std::numeric_limits<int32_t>::max());

    // Verify int64 extremes
    auto int64_array = std::static_pointer_cast<arrow::Int64Array>(batch->column(1));
    EXPECT_EQ(int64_array->Value(3), std::numeric_limits<int64_t>::min());
    EXPECT_EQ(int64_array->Value(4), std::numeric_limits<int64_t>::max());

    // Verify uint32 extremes
    auto uint32_array = std::static_pointer_cast<arrow::UInt32Array>(batch->column(2));
    EXPECT_EQ(uint32_array->Value(3), std::numeric_limits<uint32_t>::min());
    EXPECT_EQ(uint32_array->Value(4), std::numeric_limits<uint32_t>::max());

    // Verify uint64 extremes
    auto uint64_array = std::static_pointer_cast<arrow::UInt64Array>(batch->column(3));
    EXPECT_EQ(uint64_array->Value(3), std::numeric_limits<uint64_t>::min());
    EXPECT_EQ(uint64_array->Value(4), std::numeric_limits<uint64_t>::max());

    // Verify float special values
    auto float_array = std::static_pointer_cast<arrow::FloatArray>(batch->column(4));
    EXPECT_EQ(float_array->Value(0), std::numeric_limits<float>::lowest());
    EXPECT_EQ(float_array->Value(1), std::numeric_limits<float>::max());
    EXPECT_EQ(float_array->Value(2), std::numeric_limits<float>::infinity());
    EXPECT_EQ(float_array->Value(3), -std::numeric_limits<float>::infinity());
    EXPECT_TRUE(std::isnan(float_array->Value(4)));

    // Verify double special values
    auto double_array = std::static_pointer_cast<arrow::DoubleArray>(batch->column(5));
    EXPECT_EQ(double_array->Value(0), std::numeric_limits<double>::lowest());
    EXPECT_EQ(double_array->Value(1), std::numeric_limits<double>::max());
    EXPECT_EQ(double_array->Value(2), std::numeric_limits<double>::infinity());
    EXPECT_EQ(double_array->Value(3), -std::numeric_limits<double>::infinity());
    EXPECT_TRUE(std::isnan(double_array->Value(4)));
}

//
// Test Setup:
//      Create a large batch (~128MB) with mixed types
// Test Result:
//      Verify large batch is handled correctly
//
TEST_F(ArrowRecordBatchBuilderTest, LargeBatchTest) {
    const size_t num_rows = 1000000;  // 1M rows

    // Generate test data
    std::vector<int32_t> int_data(num_rows);
    std::vector<double> double_data(num_rows);
    auto string_data = GenerateRandomStrings(num_rows, 100);  // 100 chars avg length
    auto validity = GenerateRandomValidity(num_rows, 0.1);    // 10% nulls

    // Fill numeric data
    for (size_t i = 0; i < num_rows; i++) {
        int_data[i] = static_cast<int32_t>(i);
        double_data[i] = static_cast<double>(i) * 1.1;
    }

    // Build the batch
    auto result = ArrowRecordBatchBuilder()
                      .AddInt32Column("id", int_data, validity)
                      .AddDoubleColumn("value", double_data, validity)
                      .AddStringColumn("description", string_data, validity)
                      .Build();

    VERIFY_RESULT(result);
    auto batch = result.value();

    // Verify basic properties
    EXPECT_EQ(batch->num_rows(), num_rows);
    EXPECT_EQ(batch->num_columns(), 3);

    // Verify some random positions
    auto id_array = std::static_pointer_cast<arrow::Int32Array>(batch->column(0));
    auto value_array = std::static_pointer_cast<arrow::DoubleArray>(batch->column(1));
    auto desc_array = std::static_pointer_cast<arrow::StringArray>(batch->column(2));

    for (size_t i = 0; i < 10; i++) {
        size_t pos = (num_rows / 10) * i;
        if (!validity[pos]) {
            EXPECT_TRUE(id_array->IsNull(pos));
            EXPECT_TRUE(value_array->IsNull(pos));
            EXPECT_TRUE(desc_array->IsNull(pos));
        } else {
            EXPECT_EQ(id_array->Value(pos), static_cast<int32_t>(pos));
            EXPECT_DOUBLE_EQ(value_array->Value(pos), static_cast<double>(pos) * 1.1);
            EXPECT_EQ(desc_array->GetString(pos), string_data[pos]);
        }
    }
}

//
// Test Setup:
//      Create a batch with all null values
// Test Result:
//      Verify all-null batch is handled correctly
//
TEST_F(ArrowRecordBatchBuilderTest, AllNullBatchTest) {
    const size_t num_rows = 100;
    std::vector<bool> all_null(num_rows, false);  // all false = all null

    auto result = ArrowRecordBatchBuilder()
                      .AddInt32Column("int_col", std::vector<int32_t>(num_rows), all_null)
                      .AddStringColumn("str_col", std::vector<std::string>(num_rows), all_null)
                      .AddDoubleColumn("double_col", std::vector<double>(num_rows), all_null)
                      .Build();

    VERIFY_RESULT(result);
    auto batch = result.value();

    EXPECT_EQ(batch->num_rows(), num_rows);

    // Verify all values are null
    for (int col = 0; col < batch->num_columns(); col++) {
        for (int row = 0; row < num_rows; row++) {
            EXPECT_TRUE(batch->column(col)->IsNull(row));
        }
    }
}

//
// Test Setup:
//      Test various invalid inputs
// Test Result:
//      Verify appropriate error handling
//
TEST_F(ArrowRecordBatchBuilderTest, InvalidInputTest) {
    // Test with nullptr array
    {
        auto result = ArrowRecordBatchBuilder().AddColumn("null_col", nullptr).Build();
        VERIFY_ERROR_CODE(result, common::ErrorCode::InvalidArgument);
    }

    // Test with empty column name
    {
        auto result = ArrowRecordBatchBuilder().AddInt32Column("", {1, 2, 3}).Build();
        VERIFY_RESULT(result);
        EXPECT_EQ(result.value()->schema()->field(0)->name(), "");
    }

    // Test with extremely long column name (64KB)
    {
        std::string long_name(65536, 'a');
        auto result = ArrowRecordBatchBuilder().AddInt32Column(long_name, {1, 2, 3}).Build();
        VERIFY_RESULT(result);
        EXPECT_EQ(result.value()->schema()->field(0)->name(), long_name);
    }
}

//
// Test Setup:
//      Test memory limits with very large strings
// Test Result:
//      Verify large strings are handled correctly or fail gracefully
//
TEST_F(ArrowRecordBatchBuilderTest, LargeStringTest) {
    const size_t num_rows = 1000;
    const size_t string_size = 1024 * 1024;  // 1MB per string

    std::vector<std::string> large_strings(num_rows);
    std::string base_string(string_size, 'x');
    for (size_t i = 0; i < num_rows; i++) {
        large_strings[i] = base_string + std::to_string(i);
    }

    auto result = ArrowRecordBatchBuilder().AddStringColumn("large_strings", large_strings).Build();

    VERIFY_RESULT(result);
    auto batch = result.value();

    // Verify strings were stored correctly
    auto string_array = std::static_pointer_cast<arrow::StringArray>(batch->column(0));
    for (size_t i = 0; i < 10; i++) {  // Check first 10 strings
        EXPECT_EQ(string_array->GetString(i), large_strings[i]);
    }
}

//
// Test Setup:
//      Create a batch with timestamp values using raw milliseconds
// Test Result:
//      Verify timestamp values are stored correctly
//
TEST_F(ArrowRecordBatchBuilderTest, TimestampMillisTest) {
    // Create some timestamp values (milliseconds since epoch)
    std::vector<int64_t> timestamps = {
        0,                                   // 1970-01-01 00:00:00.000
        1000,                                // 1970-01-01 00:00:01.000
        1609459200000,                       // 2021-01-01 00:00:00.000
        1609459200123,                       // 2021-01-01 00:00:00.123
        std::numeric_limits<int64_t>::max()  // Far future
    };

    auto result = ArrowRecordBatchBuilder().AddTimestampColumn("ts", timestamps).Build();

    VERIFY_RESULT(result);
    auto batch = result.value();

    // Verify schema
    ASSERT_EQ(batch->num_columns(), 1);
    ASSERT_EQ(batch->schema()->field(0)->name(), "ts");
    ASSERT_EQ(batch->schema()->field(0)->type()->id(), arrow::Type::TIMESTAMP);
    ASSERT_EQ(std::static_pointer_cast<arrow::TimestampType>(batch->schema()->field(0)->type())->unit(),
              arrow::TimeUnit::MILLI);

    // Verify values
    auto ts_array = std::static_pointer_cast<arrow::TimestampArray>(batch->column(0));
    for (size_t i = 0; i < timestamps.size(); i++) {
        EXPECT_EQ(ts_array->Value(i), timestamps[i]);
    }
}

//
// Test Setup:
//      Create a batch with timestamp values using std::chrono::time_point
// Test Result:
//      Verify timestamp values are stored correctly
//
TEST_F(ArrowRecordBatchBuilderTest, TimestampChronoTest) {
    using clock = std::chrono::system_clock;
    using time_point = std::chrono::time_point<clock>;

    // Create some timestamp values using chrono
    std::vector<time_point> timestamps = {
        clock::from_time_t(0),                                           // 1970-01-01 00:00:00
        clock::from_time_t(1609459200),                                  // 2021-01-01 00:00:00
        clock::now(),                                                    // Current time
        clock::from_time_t(1609459200) + std::chrono::milliseconds(123)  // With milliseconds
    };

    auto result = ArrowRecordBatchBuilder().AddTimestampColumn("ts", timestamps).Build();

    VERIFY_RESULT(result);
    auto batch = result.value();

    // Verify schema
    ASSERT_EQ(batch->schema()->field(0)->type()->id(), arrow::Type::TIMESTAMP);

    // Verify values
    auto ts_array = std::static_pointer_cast<arrow::TimestampArray>(batch->column(0));
    for (size_t i = 0; i < timestamps.size(); i++) {
        auto expected_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(timestamps[i].time_since_epoch()).count();
        EXPECT_EQ(ts_array->Value(i), expected_ms);
    }
}

//
// Test Setup:
//      Create a batch with null timestamp values
// Test Result:
//      Verify null handling for timestamps
//
TEST_F(ArrowRecordBatchBuilderTest, TimestampNullTest) {
    std::vector<int64_t> timestamps = {0, 1000, 2000, 3000};
    std::vector<bool> validity = {true, false, true, false};

    auto result = ArrowRecordBatchBuilder().AddTimestampColumn("ts", timestamps, validity).Build();

    VERIFY_RESULT(result);
    auto batch = result.value();

    auto ts_array = std::static_pointer_cast<arrow::TimestampArray>(batch->column(0));
    EXPECT_FALSE(ts_array->IsNull(0));
    EXPECT_TRUE(ts_array->IsNull(1));
    EXPECT_FALSE(ts_array->IsNull(2));
    EXPECT_TRUE(ts_array->IsNull(3));

    EXPECT_EQ(ts_array->Value(0), 0);
    EXPECT_EQ(ts_array->Value(2), 2000);
}

//
// Test Setup:
//      Create a batch with non-nullable timestamp column
// Test Result:
//      Verify non-nullable timestamp handling
//
TEST_F(ArrowRecordBatchBuilderTest, TimestampNotNullableTest) {
    std::vector<int64_t> timestamps = {0, 1000, 2000};

    auto result =
        ArrowRecordBatchBuilder().AddTimestampColumn("ts", timestamps, {}, common::Nullability::NOT_NULL).Build();

    VERIFY_RESULT(result);
    auto batch = result.value();

    EXPECT_FALSE(batch->schema()->field(0)->nullable());
}

//
// Test Setup:
//      Create a large batch with timestamp values
// Test Result:
//      Verify large timestamp batch handling
//
TEST_F(ArrowRecordBatchBuilderTest, LargeTimestampBatchTest) {
    const size_t num_rows = 1000000;  // 1M rows

    // Generate timestamps with 1-second intervals
    std::vector<int64_t> timestamps;
    timestamps.reserve(num_rows);
    for (size_t i = 0; i < num_rows; i++) {
        timestamps.push_back(i * 1000);  // Convert seconds to milliseconds
    }

    auto validity = GenerateRandomValidity(num_rows, 0.1);  // 10% nulls

    auto result = ArrowRecordBatchBuilder().AddTimestampColumn("ts", timestamps, validity).Build();

    VERIFY_RESULT(result);
    auto batch = result.value();

    EXPECT_EQ(batch->num_rows(), num_rows);

    // Verify some random positions
    auto ts_array = std::static_pointer_cast<arrow::TimestampArray>(batch->column(0));
    for (size_t i = 0; i < 10; i++) {
        size_t pos = (num_rows / 10) * i;
        if (!validity[pos]) {
            EXPECT_TRUE(ts_array->IsNull(pos));
        } else {
            EXPECT_EQ(ts_array->Value(pos), pos * 1000);
        }
    }
}

//
// Test Setup:
//      Test timestamp column with invalid inputs
// Test Result:
//      Verify appropriate error handling
//
TEST_F(ArrowRecordBatchBuilderTest, TimestampInvalidInputTest) {
    // Test with mismatched validity vector
    {
        std::vector<int64_t> timestamps = {0, 1000, 2000};
        std::vector<bool> validity = {true, false};  // Too short

        auto result = ArrowRecordBatchBuilder().AddTimestampColumn("ts", timestamps, validity).Build();

        VERIFY_RESULT(result);
        auto batch = result.value();
        EXPECT_EQ(batch->num_rows(), 2);
        auto ts_array = std::static_pointer_cast<arrow::TimestampArray>(batch->column(0));
        EXPECT_TRUE(ts_array->IsNull(1));
        EXPECT_EQ(ts_array->Value(0), 0);
        EXPECT_EQ(ts_array->Value(1), 1000);
    }

    // Test with empty timestamp vector
    {
        auto result = ArrowRecordBatchBuilder().AddTimestampColumn("ts", std::vector<int64_t>{}).Build();

        VERIFY_RESULT(result);
        auto batch = result.value();
        EXPECT_EQ(batch->num_rows(), 0);
    }

    // Test with negative timestamps (should be valid)
    {
        std::vector<int64_t> timestamps = {-1000, 0, 1000};  // 1 second before epoch
        auto result = ArrowRecordBatchBuilder().AddTimestampColumn("ts", timestamps).Build();

        VERIFY_RESULT(result);
        auto batch = result.value();
        auto ts_array = std::static_pointer_cast<arrow::TimestampArray>(batch->column(0));
        EXPECT_EQ(ts_array->Value(0), -1000);
    }
}

//
// Test Setup:
//      Test mixed timestamp precision handling
// Test Result:
//      Verify consistent millisecond precision
//
TEST_F(ArrowRecordBatchBuilderTest, TimestampPrecisionTest) {
    using namespace std::chrono;
    system_clock::time_point now = system_clock::now();

    // Create timestamps with different precisions
    auto ms = time_point_cast<milliseconds>(now);
    auto sec = time_point_cast<seconds>(now);
    auto min = time_point_cast<minutes>(now);

    std::vector<system_clock::time_point> timestamps = {ms, sec, min};

    auto result = ArrowRecordBatchBuilder().AddTimestampColumn("ts", timestamps).Build();

    VERIFY_RESULT(result);
    auto batch = result.value();

    // Verify all values are stored in millisecond precision
    auto ts_array = std::static_pointer_cast<arrow::TimestampArray>(batch->column(0));
    EXPECT_EQ(ts_array->Value(0), duration_cast<milliseconds>(ms.time_since_epoch()).count());
    EXPECT_EQ(ts_array->Value(1), duration_cast<milliseconds>(sec.time_since_epoch()).count());
    EXPECT_EQ(ts_array->Value(2), duration_cast<milliseconds>(min.time_since_epoch()).count());
}

}  // namespace pond::query