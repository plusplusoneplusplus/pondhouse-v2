#include "common/iterator.h"

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

}  // namespace