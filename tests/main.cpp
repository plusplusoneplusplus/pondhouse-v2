#include <gtest/gtest.h>

#include "common/crc.h"
#include "common/log.h"

int main(int argc, char **argv) {
    pond::common::CRC32::Initialize();
    pond::common::Logger::instance().init("test.log");
    pond::common::Logger::instance().setLogLevel(pond::common::LogLevel::Verbose);

    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
