#include "common/bplus_tree.h"

#include <random>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "test_helper.h"

namespace pond::common {

class BPlusTreeTest : public ::testing::Test {
protected:
    using TestTree = BPlusTree<std::string, uint64_t>;
    std::unique_ptr<TestTree> tree_;

    void SetUp() override { tree_ = std::make_unique<TestTree>(); }

    std::string MakeKey(int i) {
        // Pad with zeros to ensure proper string ordering
        return "key" + std::string(5 - std::to_string(i).length(), '0') + std::to_string(i);
    }

    void InsertRange(int start, int end) {
        for (int i = start; i < end; i++) {
            VERIFY_RESULT(tree_->Insert(MakeKey(i), i));
        }
    }
};

TEST_F(BPlusTreeTest, EmptyTreeOperations) {
    // Get from empty tree
    auto result = tree_->Get("any_key");
    VERIFY_RESULT(result);
    EXPECT_FALSE(result.value().has_value());

    // Range query on empty tree
    auto range_result = tree_->Range("start", "end", 10);
    VERIFY_RESULT(range_result);
    EXPECT_TRUE(range_result.value().empty());
}

TEST_F(BPlusTreeTest, SingleKeyOperations) {
    // Insert single key
    VERIFY_RESULT(tree_->Insert(MakeKey(1), 1));

    // Get existing key
    auto result = tree_->Get(MakeKey(1));
    VERIFY_RESULT(result);
    ASSERT_TRUE(result.value().has_value());
    EXPECT_EQ(result.value().value(), 1);

    // Get non-existent key
    result = tree_->Get(MakeKey(2));
    VERIFY_RESULT(result);
    EXPECT_FALSE(result.value().has_value());

    // Update existing key
    VERIFY_RESULT(tree_->Insert(MakeKey(1), 2));
    result = tree_->Get(MakeKey(1));
    VERIFY_RESULT(result);
    ASSERT_TRUE(result.value().has_value());
    EXPECT_EQ(result.value().value(), 2);
}

TEST_F(BPlusTreeTest, MultipleKeyOperations) {
    // Insert multiple keys
    InsertRange(0, 10);

    // Verify all keys exist
    for (int i = 0; i < 10; i++) {
        auto result = tree_->Get(MakeKey(i));
        VERIFY_RESULT(result);
        ASSERT_TRUE(result.value().has_value());
        EXPECT_EQ(result.value().value(), i);
    }
}

TEST_F(BPlusTreeTest, RangeQueries) {
    // Insert keys
    InsertRange(0, 100);

    // Full range query
    auto result = tree_->Range(MakeKey(0), MakeKey(99), 100);
    VERIFY_RESULT(result);
    ASSERT_EQ(result.value().size(), 100);
    for (size_t i = 0; i < result.value().size(); i++) {
        EXPECT_EQ(result.value()[i].first, MakeKey(i));
        EXPECT_EQ(result.value()[i].second, i);
    }

    // Limited range query
    result = tree_->Range(MakeKey(0), MakeKey(99), 10);
    VERIFY_RESULT(result);
    ASSERT_EQ(result.value().size(), 10);
    for (size_t i = 0; i < result.value().size(); i++) {
        EXPECT_EQ(result.value()[i].first, MakeKey(i));
        EXPECT_EQ(result.value()[i].second, i);
    }

    // Partial range query
    result = tree_->Range(MakeKey(25), MakeKey(35), 100);
    VERIFY_RESULT(result);
    ASSERT_EQ(result.value().size(), 11);
    for (size_t i = 0; i < result.value().size(); i++) {
        EXPECT_EQ(result.value()[i].first, MakeKey(i + 25));
        EXPECT_EQ(result.value()[i].second, i + 25);
    }

    // Empty range query (start > end)
    result = tree_->Range(MakeKey(99), MakeKey(0), 100);
    VERIFY_RESULT(result);
    EXPECT_TRUE(result.value().empty());
}

TEST_F(BPlusTreeTest, NodeSplitting) {
    // Insert enough keys to force node splitting
    InsertRange(0, 1000);

    // Verify all keys are still accessible
    for (int i = 0; i < 1000; i++) {
        auto result = tree_->Get(MakeKey(i));
        VERIFY_RESULT(result);
        ASSERT_TRUE(result.value().has_value());
        EXPECT_EQ(result.value().value(), i);
    }
}

TEST_F(BPlusTreeTest, RandomOperations) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, 999);

    // Insert random keys
    std::set<int> inserted;
    for (int i = 0; i < 100; i++) {
        int val = dis(gen);
        VERIFY_RESULT(tree_->Insert(MakeKey(val), val));
        inserted.insert(val);
    }

    // Verify all inserted keys exist
    for (int val : inserted) {
        auto result = tree_->Get(MakeKey(val));
        VERIFY_RESULT(result);
        ASSERT_TRUE(result.value().has_value());
        EXPECT_EQ(result.value().value(), val);
    }
}

TEST_F(BPlusTreeTest, StressTest) {
    // Insert a large number of keys
    InsertRange(0, 10000);

    // Verify random access
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, 9999);

    for (int i = 0; i < 1000; i++) {
        int val = dis(gen);
        auto result = tree_->Get(MakeKey(val));
        VERIFY_RESULT(result);
        ASSERT_TRUE(result.value().has_value());
        EXPECT_EQ(result.value().value(), val);
    }

    // Verify range queries still work
    auto result = tree_->Range(MakeKey(1000), MakeKey(1099), 100);
    VERIFY_RESULT(result);
    ASSERT_EQ(result.value().size(), 100);
    for (size_t i = 0; i < result.value().size(); i++) {
        EXPECT_EQ(result.value()[i].first, MakeKey(i + 1000));
        EXPECT_EQ(result.value()[i].second, i + 1000);
    }
}

}  // namespace pond::common
