#include <gtest/gtest.h>

#include "common/crc.h"

int main(int argc, char **argv) {
    pond::common::CRC32::Initialize();

    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
