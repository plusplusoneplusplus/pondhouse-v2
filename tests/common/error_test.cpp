#include <gtest/gtest.h>
#include "common/error.h"

using namespace pond::common;

TEST(ErrorTest, ConstructorWithCodeAndMessage) {
    Error error(ErrorCode::InvalidArgument, "test message");
    EXPECT_EQ(error.code(), ErrorCode::InvalidArgument);
    EXPECT_EQ(error.message(), "test message");
}

TEST(ErrorTest, ConstructorWithCodeOnly) {
    Error error(ErrorCode::Success);
    EXPECT_EQ(error.code(), ErrorCode::Success);
    EXPECT_EQ(error.message(), "");
}

TEST(ErrorTest, MessageAccessors) {
    Error error(ErrorCode::FileNotFound, "file.txt not found");
    EXPECT_EQ(error.message(), "file.txt not found");
    EXPECT_STREQ(error.c_str(), "file.txt not found");
}

TEST(ErrorTest, ErrorCodeValues) {
    // Test some key error codes to ensure they have expected values
    EXPECT_EQ(static_cast<int>(ErrorCode::Success), 0);
    EXPECT_EQ(static_cast<int>(ErrorCode::Failure), 1);
    EXPECT_EQ(static_cast<int>(ErrorCode::FileNotFound), 100);
    EXPECT_EQ(static_cast<int>(ErrorCode::ParquetReadFailed), 200);
}
