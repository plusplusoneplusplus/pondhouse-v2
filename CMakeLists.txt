cmake_minimum_required(VERSION 3.14)
project(pondhouse-v2)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find Arrow package
find_package(Arrow REQUIRED)

# Add common library
add_library(pondhouse_common
    src/common/append_only_fs.cpp
    src/common/memory_append_only_fs.cpp
    src/common/log.cpp
    src/kv/kv_entry.cpp
)

# Add include directories for common library
target_include_directories(pondhouse_common PUBLIC ${CMAKE_SOURCE_DIR}/src)

# Link common library with Arrow
target_link_libraries(pondhouse_common PUBLIC Arrow::arrow_shared)

# Add main executable
add_executable(${PROJECT_NAME} src/main.cpp)

# Link main executable with common library
target_link_libraries(${PROJECT_NAME} PRIVATE pondhouse_common)

# Enable testing
enable_testing()

# Include FetchContent for downloading GoogleTest
include(FetchContent)
FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG release-1.12.1
)
FetchContent_MakeAvailable(googletest)

# Add test executable
add_executable(
    unit_tests
    tests/main.cpp
    tests/common/error_test.cpp
    tests/common/result_test.cpp
    tests/common/data_chunk_test.cpp
    tests/common/uuid_test.cpp
    tests/common/append_only_fs_test.cpp
    tests/common/wal_test.cpp
)

# Add include directories for tests
target_include_directories(unit_tests PRIVATE ${CMAKE_SOURCE_DIR}/src)

# Link test executable against gtest & your project libraries
target_link_libraries(
    unit_tests
    PRIVATE
    GTest::gtest
    GTest::gtest_main
    pondhouse_common
)

# Add tests to CTest
add_test(NAME unit_tests COMMAND unit_tests)
