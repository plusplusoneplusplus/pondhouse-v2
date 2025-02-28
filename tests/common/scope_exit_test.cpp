#include "common/scope_exit.h"

#include <stdexcept>

#include <gtest/gtest.h>

using namespace pond::common;

TEST(ScopeExitTest, BasicFunctionality) {
    bool cleanup_called = false;
    {
        auto cleanup = ScopeExit([&]() { cleanup_called = true; });
    }
    EXPECT_TRUE(cleanup_called);
}

TEST(ScopeExitTest, ExceptionCase) {
    bool cleanup_called = false;
    try {
        auto cleanup = ScopeExit([&]() { cleanup_called = true; });
        throw std::runtime_error("test error");
    } catch (const std::runtime_error&) {
        // Exception caught
    }
    EXPECT_TRUE(cleanup_called);
}

TEST(ScopeExitTest, MoveConstruction) {
    bool cleanup_called = false;
    {
        auto cleanup1 = ScopeExit([&]() { cleanup_called = true; });
        auto cleanup2 = std::move(cleanup1);
    }
    EXPECT_TRUE(cleanup_called);
}

TEST(ScopeExitTest, MultipleCleanups) {
    int cleanup_count = 0;
    {
        auto cleanup1 = ScopeExit([&]() { cleanup_count++; });
        auto cleanup2 = ScopeExit([&]() { cleanup_count++; });
    }
    EXPECT_EQ(cleanup_count, 2);
}

TEST(ScopeExitTest, NestedScopes) {
    int cleanup_order = 0;
    int first_cleanup = 0;
    int second_cleanup = 0;
    {
        auto cleanup1 = ScopeExit([&]() { first_cleanup = ++cleanup_order; });
        {
            auto cleanup2 = ScopeExit([&]() { second_cleanup = ++cleanup_order; });
        }
        EXPECT_EQ(second_cleanup, 1);
    }
    EXPECT_EQ(first_cleanup, 2);
}

TEST(ScopeExitTest, MoveAssignmentDisabled) {
    auto cleanup1 = ScopeExit([]() {});
    auto cleanup2 = ScopeExit([]() {});

    // The following line should not compile:
    // cleanup2 = std::move(cleanup1);

    // Test that the type is not move assignable
    EXPECT_FALSE(std::is_move_assignable_v<ScopeExit>);
}

TEST(ScopeExitTest, CopyConstructionDisabled) {
    // Test that the type is not copy constructible
    EXPECT_FALSE(std::is_copy_constructible_v<ScopeExit>);
}

TEST(ScopeExitTest, CopyAssignmentDisabled) {
    // Test that the type is not copy assignable
    EXPECT_FALSE(std::is_copy_assignable_v<ScopeExit>);
}