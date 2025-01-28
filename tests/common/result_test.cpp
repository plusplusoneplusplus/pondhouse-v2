#include <gtest/gtest.h>
#include <memory>
#include "common/result.h"

using namespace pond::common;

TEST(ResultTest, SuccessCase) {
    Result<int> result = 42;
    EXPECT_TRUE(result.ok());
    EXPECT_FALSE(result.hasError());
    EXPECT_EQ(result.value(), 42);
}

TEST(ResultTest, ErrorCase) {
    Result<int> result(Error(ErrorCode::InvalidArgument, "test error"));
    EXPECT_FALSE(result.ok());
    EXPECT_TRUE(result.hasError());
    EXPECT_EQ(result.error().code(), ErrorCode::InvalidArgument);
    EXPECT_EQ(result.error().message(), "test error");
}

TEST(ResultTest, MoveConstructible) {
    std::string str = "test string";
    Result<std::string> result(std::move(str));
    EXPECT_TRUE(result.ok());
    EXPECT_EQ(result.value(), "test string");
}

TEST(ResultTest, UniquePtr) {
    auto ptr = std::make_unique<int>(42);
    Result<std::unique_ptr<int>> result(std::move(ptr));
    EXPECT_TRUE(result.ok());
    EXPECT_EQ(*result.value(), 42);
}

TEST(ResultTest, ValueAccessThrow) {
    Result<int> result(Error(ErrorCode::InvalidArgument, "test error"));
    EXPECT_THROW(result.value(), std::runtime_error);
}

TEST(ResultTest, ErrorAccessThrow) {
    Result<int> result = 42;
    EXPECT_THROW(result.error(), std::runtime_error);
}

TEST(ResultTest, VoidResult) {
    Result<void> result;
    EXPECT_TRUE(result.ok());
    EXPECT_FALSE(result.hasError());
}