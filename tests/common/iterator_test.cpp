#include "common/iterator.h"

#include <map>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

using namespace pond::common;

namespace {

//
// Test Setup:
//      Test IteratorMode enum operations including bitwise operations
//      Verify flag checking functionality
// Test Result:
//      Verify correct behavior of mode combinations
//      Confirm flag checking works as expected
//
TEST(IteratorModeTest, BitwiseOperations) {
    // Test bitwise OR operations
    EXPECT_EQ(static_cast<uint64_t>(IteratorMode::Default | IteratorMode::IncludeTombstones),
              static_cast<uint64_t>(IteratorMode::IncludeTombstones));

    EXPECT_EQ(static_cast<uint64_t>(IteratorMode::IncludeTombstones | IteratorMode::IncludeAllVersions),
              static_cast<uint64_t>(IteratorMode::IncludeTombstones)
                  | static_cast<uint64_t>(IteratorMode::IncludeAllVersions));

    // Test bitwise AND operations
    auto mode = IteratorMode::IncludeTombstones | IteratorMode::IncludeAllVersions;
    EXPECT_EQ(static_cast<uint64_t>(mode & IteratorMode::IncludeTombstones),
              static_cast<uint64_t>(IteratorMode::IncludeTombstones));
}

//
// Test Setup:
//      Test CheckIteratorMode function with various mode combinations
// Test Result:
//      Verify correct flag detection in combined modes
//      Confirm default mode behavior
//
TEST(IteratorModeTest, FlagChecking) {
    // Test single flag
    auto mode = IteratorMode::IncludeTombstones;
    EXPECT_TRUE(CheckIteratorMode(mode, IteratorMode::IncludeTombstones));
    EXPECT_FALSE(CheckIteratorMode(mode, IteratorMode::IncludeAllVersions));

    // Test combined flags
    mode = IteratorMode::IncludeTombstones | IteratorMode::IncludeAllVersions;
    EXPECT_TRUE(CheckIteratorMode(mode, IteratorMode::IncludeTombstones));
    EXPECT_TRUE(CheckIteratorMode(mode, IteratorMode::IncludeAllVersions));

    // Test default mode
    mode = IteratorMode::Default;
    EXPECT_FALSE(CheckIteratorMode(mode, IteratorMode::IncludeTombstones));
    EXPECT_FALSE(CheckIteratorMode(mode, IteratorMode::IncludeAllVersions));
}

//
// Test Setup:
//      Test all possible combinations of iterator modes
// Test Result:
//      Verify all mode combinations work correctly
//      Confirm no unexpected interactions between flags
//
TEST(IteratorModeTest, ModeCombinations) {
    // Test all possible combinations
    std::vector<IteratorMode> modes = {IteratorMode::Default,
                                       IteratorMode::IncludeTombstones,
                                       IteratorMode::IncludeAllVersions,
                                       IteratorMode::IncludeTombstones | IteratorMode::IncludeAllVersions};

    for (const auto& mode1 : modes) {
        for (const auto& mode2 : modes) {
            // Test OR operation
            auto combined = mode1 | mode2;
            EXPECT_TRUE(CheckIteratorMode(combined, mode1) || mode1 == IteratorMode::Default);
            EXPECT_TRUE(CheckIteratorMode(combined, mode2) || mode2 == IteratorMode::Default);

            // Test AND operation
            auto intersect = mode1 & mode2;
            if (mode1 != IteratorMode::Default && mode2 != IteratorMode::Default) {
                EXPECT_EQ(CheckIteratorMode(intersect, mode1), mode1 == mode2 || mode1 == (mode1 & mode2));
            }
        }
    }
}

// Mock implementation of Iterator for testing
class MockIterator : public Iterator<std::string, std::string> {
public:
    MockIterator(const std::map<std::string, std::pair<std::string, bool>>& data, IteratorMode mode)
        : Iterator(mode), data_(data) {
        it_ = data_.begin();
        SkipTombstones();  // Skip tombstones at initialization if needed
    }

    void Seek(const std::string& target) override {
        it_ = data_.lower_bound(target);
        SkipTombstones();  // Skip tombstones after seeking
    }

    void Next() override {
        if (Valid()) {
            ++it_;
            SkipTombstones();  // Skip tombstones after moving
        }
    }

    bool Valid() const override { return it_ != data_.end(); }

    const std::string& key() const override { return it_->first; }

    const std::string& value() const override { return it_->second.first; }

    bool IsTombstone() const override { return it_->second.second; }

private:
    // Helper method to skip tombstones when not in IncludeTombstones mode
    void SkipTombstones() {
        if (!CheckIteratorMode(mode_, IteratorMode::IncludeTombstones)) {
            while (Valid() && IsTombstone()) {
                ++it_;
            }
        }
    }

    const std::map<std::string, std::pair<std::string, bool>>& data_;
    std::map<std::string, std::pair<std::string, bool>>::const_iterator it_;
};

// Mock implementation of SnapshotIterator for testing
class MockSnapshotIterator : public SnapshotIterator<std::string, std::string> {
public:
    struct VersionedValue {
        std::string value;
        HybridTime version;
        bool is_tombstone;
    };

    MockSnapshotIterator(const std::map<std::string, std::vector<VersionedValue>>& data,
                         HybridTime read_time,
                         IteratorMode mode)
        : SnapshotIterator(read_time, mode), data_(data) {
        it_ = data_.begin();
        UpdateValueIterator();
    }

    void Seek(const std::string& target) override {
        it_ = data_.lower_bound(target);
        UpdateValueIterator();
    }

    void Next() override {
        if (Valid()) {
            ++it_;
            UpdateValueIterator();
        }
    }

    bool Valid() const override { return it_ != data_.end() && value_it_ != it_->second.end(); }

    const std::string& key() const override { return it_->first; }

    const std::string& value() const override { return value_it_->value; }

    bool IsTombstone() const override { return value_it_->is_tombstone; }

    HybridTime version() const override { return value_it_->version; }

private:
    void UpdateValueIterator() {
        if (it_ == data_.end()) {
            return;
        }

        // Find the first version that's <= read_time_
        const auto& versions = it_->second;
        value_it_ = std::find_if(
            versions.begin(), versions.end(), [this](const VersionedValue& v) { return v.version <= read_time_; });

        // If IncludeAllVersions is not set and we found a tombstone, skip this key
        if (!CheckIteratorMode(mode_, IteratorMode::IncludeAllVersions) && value_it_ != versions.end()
            && value_it_->is_tombstone && !CheckIteratorMode(mode_, IteratorMode::IncludeTombstones)) {
            ++it_;
            UpdateValueIterator();
        }
    }

    const std::map<std::string, std::vector<VersionedValue>>& data_;
    std::map<std::string, std::vector<VersionedValue>>::const_iterator it_;
    std::vector<VersionedValue>::const_iterator value_it_;
};

//
// Test Setup:
//      Create a mock iterator with simple key-value pairs
//      Test basic iterator operations
// Test Result:
//      Verify iterator correctly traverses data
//      Confirm seek and tombstone handling work as expected
//
TEST(IteratorTest, BasicOperations) {
    std::map<std::string, std::pair<std::string, bool>> data = {
        {"key1", {"value1", false}},
        {"key2", {"value2", true}},  // tombstone
        {"key3", {"value3", false}},
    };

    // Test default mode (should skip tombstones)
    {
        MockIterator iter(data, IteratorMode::Default);

        // Test initial position
        iter.Seek("key1");
        ASSERT_TRUE(iter.Valid());
        EXPECT_EQ(iter.key(), "key1");
        EXPECT_EQ(iter.value(), "value1");
        EXPECT_FALSE(iter.IsTombstone());

        // Test Next()
        iter.Next();
        ASSERT_TRUE(iter.Valid());
        EXPECT_EQ(iter.key(), "key3");  // Should skip key2 (tombstone)
        EXPECT_EQ(iter.value(), "value3");
        EXPECT_FALSE(iter.IsTombstone());

        // Test seeking to middle
        iter.Seek("key2");
        ASSERT_TRUE(iter.Valid());
        EXPECT_EQ(iter.key(), "key3");  // Should skip key2 (tombstone)
    }

    // Test with IncludeTombstones mode
    {
        MockIterator iter(data, IteratorMode::IncludeTombstones);

        iter.Seek("key1");
        ASSERT_TRUE(iter.Valid());
        EXPECT_EQ(iter.key(), "key1");

        iter.Next();
        ASSERT_TRUE(iter.Valid());
        EXPECT_EQ(iter.key(), "key2");
        EXPECT_TRUE(iter.IsTombstone());

        iter.Next();
        ASSERT_TRUE(iter.Valid());
        EXPECT_EQ(iter.key(), "key3");
    }
}

//
// Test Setup:
//      Create a mock snapshot iterator with versioned key-value pairs
//      Test version visibility and iteration behavior
// Test Result:
//      Verify correct version visibility at different read times
//      Confirm proper handling of tombstones and version ordering
//
TEST(SnapshotIteratorTest, VersionVisibility) {
    using VV = MockSnapshotIterator::VersionedValue;
    std::map<std::string, std::vector<VV>> data = {
        {"key1",
         {
             {.value = "value1_v3", .version = HybridTime(300), .is_tombstone = false},
             {.value = "value1_v2", .version = HybridTime(200), .is_tombstone = false},
             {.value = "value1_v1", .version = HybridTime(100), .is_tombstone = false},
         }},
        {"key2",
         {
             {.value = "", .version = HybridTime(200), .is_tombstone = true},
             {.value = "value2_v1", .version = HybridTime(100), .is_tombstone = false},
         }},
        {"key3",
         {
             {.value = "value3_v2", .version = HybridTime(200), .is_tombstone = false},
             {.value = "value3_v1", .version = HybridTime(100), .is_tombstone = false},
         }},
    };

    // Test at different timestamps
    {
        // At t=150, should see v1 versions
        MockSnapshotIterator iter(data, HybridTime(150), IteratorMode::Default);

        iter.Seek("key1");
        ASSERT_TRUE(iter.Valid());
        EXPECT_EQ(iter.key(), "key1");
        EXPECT_EQ(iter.value(), "value1_v1");
        EXPECT_EQ(iter.version(), HybridTime(100));

        iter.Next();
        ASSERT_TRUE(iter.Valid());
        EXPECT_EQ(iter.key(), "key2");
        EXPECT_EQ(iter.value(), "value2_v1");
        EXPECT_EQ(iter.version(), HybridTime(100));
    }

    // Test with IncludeAllVersions mode
    {
        MockSnapshotIterator iter(data, HybridTime(300), IteratorMode::IncludeAllVersions);

        iter.Seek("key1");
        ASSERT_TRUE(iter.Valid());
        EXPECT_EQ(iter.key(), "key1");
        EXPECT_EQ(iter.value(), "value1_v3");
        EXPECT_EQ(iter.version(), HybridTime(300));

        iter.Next();
        ASSERT_TRUE(iter.Valid());
        EXPECT_EQ(iter.key(), "key2");
        EXPECT_EQ(iter.value(), "");
        EXPECT_TRUE(iter.IsTombstone());
        EXPECT_EQ(iter.version(), HybridTime(200));
    }

    // Test with both IncludeAllVersions and IncludeTombstones
    {
        MockSnapshotIterator iter(
            data, HybridTime(300), IteratorMode::IncludeAllVersions | IteratorMode::IncludeTombstones);

        iter.Seek("key2");
        ASSERT_TRUE(iter.Valid());
        EXPECT_EQ(iter.key(), "key2");
        EXPECT_TRUE(iter.IsTombstone());
        EXPECT_EQ(iter.version(), HybridTime(200));

        iter.Next();
        ASSERT_TRUE(iter.Valid());
        EXPECT_EQ(iter.key(), "key3");
        EXPECT_EQ(iter.value(), "value3_v2");
        EXPECT_EQ(iter.version(), HybridTime(200));
    }
}

}  // namespace