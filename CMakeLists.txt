cmake_minimum_required(VERSION 3.14)
project(pondhouse-v2)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find required packages
find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)

# Find xxHash library
find_library(XXHASH_LIBRARY xxhash REQUIRED)

# Add common library
add_library(pondhouse_common
    src/common/append_only_fs.cpp
    src/common/memory_append_only_fs.cpp
    src/common/log.cpp
    src/common/bloom_filter.cpp
    src/common/crc.cpp
    src/kv/kv_entry.cpp
    src/kv/record.cpp
    src/kv/memtable.cpp
    src/kv/table.cpp
    src/kv/sstable.cpp
    src/kv/sstable_cache.cpp
    src/kv/sstable_format.cpp
    src/kv/sstable_writer.cpp
    src/format/parquet/append_only_input_stream.cpp
    src/format/parquet/append_only_output_stream.cpp
    src/format/parquet/parquet_reader.cpp
    src/format/parquet/parquet_writer.cpp
)

# Add include directories for common library
target_include_directories(pondhouse_common PUBLIC ${CMAKE_SOURCE_DIR}/src)

# Link common library with dependencies
target_link_libraries(pondhouse_common PUBLIC Arrow::arrow_shared ${XXHASH_LIBRARY})

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
    tests/common/bloom_filter_test.cpp
    tests/common/result_test.cpp
    tests/common/crc_test.cpp
    tests/common/data_chunk_test.cpp
    tests/common/uuid_test.cpp
    tests/common/append_only_fs_test.cpp
    tests/common/wal_test.cpp
    tests/common/bplus_tree_test.cpp
    tests/format/parquet/parquet_test.cpp
    tests/kv/record_test.cpp
    tests/kv/memtable_test.cpp
    tests/kv/kv_wal_test.cpp
    tests/kv/table_test.cpp
    tests/kv/sstable_test.cpp
    tests/kv/sstable_cache_test.cpp
    tests/kv/sstable_format_test.cpp
    tests/kv/sstable_filter_test.cpp
    tests/kv/sstable_writer_test.cpp
    tests/common/lru_cache_test.cpp
)

# Add include directories for tests
target_include_directories(unit_tests PRIVATE
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_SOURCE_DIR}/tests
)

# Link test executable against gtest & your project libraries
target_link_libraries(unit_tests
    gtest
    gtest_main
    pondhouse_common
    Arrow::arrow_shared
    Parquet::parquet_shared
)

# Add tests to CTest
add_test(NAME unit_tests COMMAND unit_tests)
