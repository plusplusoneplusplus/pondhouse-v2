#include "common/skip_list.h"

#include <algorithm>
#include <random>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

namespace pond::common {

class SkipListTest : public ::testing::Test {
protected:
    SkipList<int, std::string> list_;
};

TEST_F(SkipListTest, BasicOperations) {
    // Test initial size
    EXPECT_EQ(list_.Size(), 0);

    // Test insertion
    list_.Insert(1, "one");
    EXPECT_EQ(list_.Size(), 1);
    list_.Insert(2, "two");
    EXPECT_EQ(list_.Size(), 2);
    list_.Insert(3, "three");
    EXPECT_EQ(list_.Size(), 3);

    // Test contains
    EXPECT_TRUE(list_.Contains(1));
    EXPECT_TRUE(list_.Contains(2));
    EXPECT_TRUE(list_.Contains(3));
    EXPECT_FALSE(list_.Contains(4));

    // Test get
    std::string value;
    EXPECT_TRUE(list_.Get(1, value));
    EXPECT_EQ("one", value);
    EXPECT_TRUE(list_.Get(2, value));
    EXPECT_EQ("two", value);
    EXPECT_TRUE(list_.Get(3, value));
    EXPECT_EQ("three", value);
    EXPECT_FALSE(list_.Get(4, value));
}

TEST_F(SkipListTest, DuplicateInsertion) {
    EXPECT_EQ(list_.Size(), 0);

    list_.Insert(1, "one");
    EXPECT_EQ(list_.Size(), 1);

    list_.Insert(1, "ONE");      // Duplicate key
    EXPECT_EQ(list_.Size(), 1);  // Size should not change for duplicate keys

    std::string value;
    EXPECT_TRUE(list_.Get(1, value));
    EXPECT_EQ("ONE", value);
}

TEST_F(SkipListTest, Iterator) {
    std::vector<int> keys = {1, 3, 5, 7, 9};
    for (int key : keys) {
        list_.Insert(key, std::to_string(key));
    }

    auto it = std::unique_ptr<SkipList<int, std::string>::Iterator>(list_.NewIterator());

    // Test sequential iteration
    int index = 0;
    for (; it->Valid(); it->Next(), index++) {
        EXPECT_EQ(keys[index], it->key());
        EXPECT_EQ(std::to_string(keys[index]), it->value());
    }
    EXPECT_EQ(keys.size(), index);

    // Test seek
    it->Seek(3);
    EXPECT_TRUE(it->Valid());
    EXPECT_EQ(3, it->key());
    EXPECT_EQ("3", it->value());

    it->Seek(4);
    EXPECT_TRUE(it->Valid());
    EXPECT_EQ(5, it->key());
}

TEST_F(SkipListTest, ConcurrentOperations) {
    const int num_threads = 4;
    const int num_ops = 1000;
    std::vector<std::thread> threads;

    // Concurrent insertions
    for (int i = 0; i < num_threads; i++) {
        threads.emplace_back([&, i]() {
            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_int_distribution<> dis(i * num_ops, (i + 1) * num_ops - 1);

            for (int j = 0; j < num_ops; j++) {
                int key = dis(gen);
                list_.Insert(key, std::to_string(key));
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    // Verify all insertions
    std::string value;
    for (int i = 0; i < num_threads; i++) {
        for (int j = 0; j < num_ops; j++) {
            int key = i * num_ops + j;
            EXPECT_TRUE(list_.Get(key, value));
            EXPECT_EQ(std::to_string(key), value);
        }
    }
}

TEST_F(SkipListTest, StressTest) {
    const int num_ops = 10000;
    std::vector<int> keys;
    keys.reserve(num_ops);

    // Generate random keys
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(1, num_ops * 10);

    for (int i = 0; i < num_ops; i++) {
        keys.push_back(dis(gen));
    }

    // Insert all keys
    for (int key : keys) {
        list_.Insert(key, std::to_string(key));
    }

    // Verify all keys are present
    std::string value;
    for (int key : keys) {
        EXPECT_TRUE(list_.Get(key, value));
        EXPECT_EQ(std::to_string(key), value);
    }

    // Test iterator with random seeks
    auto it = std::unique_ptr<SkipList<int, std::string>::Iterator>(list_.NewIterator());
    std::shuffle(keys.begin(), keys.end(), gen);

    for (int key : keys) {
        it->Seek(key);
        EXPECT_TRUE(it->Valid());
        EXPECT_LE(it->key(), key);
    }
}

TEST_F(SkipListTest, SizeTracking) {
    EXPECT_EQ(list_.Size(), 0) << "Initial size should be 0";

    // Test size increases with insertions
    for (int i = 0; i < 100; i++) {
        list_.Insert(i, std::to_string(i));
        EXPECT_EQ(list_.Size(), i + 1) << "Size should increase after insertion";
    }

    // Test size doesn't change with duplicate keys
    for (int i = 0; i < 100; i++) {
        list_.Insert(i, std::to_string(i * 2));
        EXPECT_EQ(list_.Size(), 100) << "Size should not change for duplicate keys";
    }

    // Verify all elements are still accessible
    std::string value;
    for (int i = 0; i < 100; i++) {
        EXPECT_TRUE(list_.Get(i, value));
        EXPECT_EQ(value, std::to_string(i * 2));
    }
}

}  // namespace pond::common
