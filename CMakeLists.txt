cmake_minimum_required(VERSION 3.14)
project(pondhouse-v2)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED YES)

# Set policy to handle duplicate targets
cmake_policy(SET CMP0074 NEW)

# Find gRPC first, which will bring in its own Protobuf
find_package(gRPC CONFIG REQUIRED)

# Find other required packages
find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)
# Protobuf is already loaded by gRPC, so we don't need to find it again
# find_package(Protobuf REQUIRED)

# A dependency for the protobuf
find_package(absl REQUIRED)

# Define Abseil libraries needed
set(ABSL_LIBS
    absl::base
    absl::synchronization
    absl::strings
    absl::status
    absl::statusor
    absl::cord
    absl::hash
    absl::flat_hash_map
    absl::flat_hash_set
    absl::btree
    absl::time
)

# Find xxHash library
find_library(XXHASH_LIBRARY xxhash REQUIRED)

# Set protobuf output directory
set(PROTO_OUTPUT_DIR ${CMAKE_CURRENT_SOURCE_DIR}/src/proto/build)

# Create proto build directory if it doesn't exist
file(MAKE_DIRECTORY ${PROTO_OUTPUT_DIR})
file(MAKE_DIRECTORY ${PROTO_OUTPUT_DIR}/proto)

# Make sure we have the protoc executable
if(NOT DEFINED Protobuf_PROTOC_EXECUTABLE)
    get_target_property(Protobuf_PROTOC_EXECUTABLE protobuf::protoc LOCATION)
endif()

# Protobuf files
set(PROTO_FILES
    src/proto/kv.proto
    src/proto/pond_service.proto
)

# Custom command to generate protobuf files
set(PROTO_SRCS "")
set(PROTO_HDRS "")
set(GRPC_SRCS "")
set(GRPC_HDRS "")

foreach(PROTO_FILE ${PROTO_FILES})
    get_filename_component(PROTO_NAME ${PROTO_FILE} NAME_WE)
    set(PROTO_HEADER "${PROTO_OUTPUT_DIR}/proto/${PROTO_NAME}.pb.h")
    set(PROTO_SRC "${PROTO_OUTPUT_DIR}/proto/${PROTO_NAME}.pb.cc")
    set(GRPC_HEADER "${PROTO_OUTPUT_DIR}/proto/${PROTO_NAME}.grpc.pb.h")
    set(GRPC_SRC "${PROTO_OUTPUT_DIR}/proto/${PROTO_NAME}.grpc.pb.cc")
    
    add_custom_command(
        OUTPUT 
            "${PROTO_HEADER}"
            "${PROTO_SRC}"
            "${GRPC_HEADER}"
            "${GRPC_SRC}"
        COMMAND ${Protobuf_PROTOC_EXECUTABLE}
        ARGS 
             "--cpp_out=${PROTO_OUTPUT_DIR}"
             "--grpc_out=${PROTO_OUTPUT_DIR}"
             "--plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>"
             "--proto_path=${CMAKE_CURRENT_SOURCE_DIR}/src"
             "${CMAKE_CURRENT_SOURCE_DIR}/${PROTO_FILE}"
        DEPENDS ${PROTO_FILE}
        COMMENT "Generating protobuf and gRPC files for ${PROTO_FILE}"
        VERBATIM
    )
    
    list(APPEND PROTO_SRCS "${PROTO_SRC}")
    list(APPEND PROTO_HDRS "${PROTO_HEADER}")
    list(APPEND GRPC_SRCS "${GRPC_SRC}")
    list(APPEND GRPC_HDRS "${GRPC_HEADER}")
endforeach()

# Create a custom target for protobuf generation
add_custom_target(generate_protos ALL DEPENDS ${PROTO_SRCS} ${PROTO_HDRS} ${GRPC_SRCS} ${GRPC_HDRS})

# Include FetchContent for downloading dependencies
include(FetchContent)

# Fetch fmt library
FetchContent_Declare(
    fmt
    GIT_REPOSITORY https://github.com/fmtlib/fmt.git
    GIT_TAG 9.1.0
)

FetchContent_MakeAvailable(fmt)

# Add sql-parser build commands
add_custom_command(
    OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/third_party/sql-parser/libsqlparser.so
    COMMAND make clean
    COMMAND make
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/third_party/sql-parser
    COMMENT "Building sql-parser library"
)

# Add command to copy the library to build directory
add_custom_command(
    OUTPUT ${CMAKE_BINARY_DIR}/libsqlparser.so
    COMMAND ${CMAKE_COMMAND} -E copy
        ${CMAKE_CURRENT_SOURCE_DIR}/third_party/sql-parser/libsqlparser.so
        ${CMAKE_BINARY_DIR}/libsqlparser.so
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/third_party/sql-parser/libsqlparser.so
    COMMENT "Copying sql-parser library to build directory"
)

add_custom_target(build_sql_parser
    DEPENDS ${CMAKE_BINARY_DIR}/libsqlparser.so
)

# Add common library
add_library(pondhouse_common
    src/catalog/kv_catalog.cpp
    src/catalog/kv_catalog_util.cpp
    src/catalog/types.cpp
    src/catalog/table_data_ingestor.cpp
    src/common/append_only_fs.cpp
    src/common/memory_append_only_fs.cpp
    src/common/log.cpp
    src/common/bloom_filter.cpp
    src/common/crc.cpp
    src/common/error.cpp
    src/common/filesystem_stream.cpp
    src/common/memory_stream.cpp
    src/kv/kv_entry.cpp
    src/kv/kv_table.cpp
    src/kv/record.cpp
    src/kv/memtable.cpp
    src/kv/table.cpp
    src/kv/table_metadata.cpp
    src/kv/sstable_manager.cpp
    src/kv/sstable_cache.cpp
    src/kv/db.cpp
    src/format/parquet/append_only_input_stream.cpp
    src/format/parquet/append_only_output_stream.cpp
    src/format/parquet/parquet_reader.cpp
    src/format/parquet/parquet_writer.cpp
    src/format/sstable/sstable_writer.cpp
    src/format/sstable/sstable_reader.cpp
    src/format/sstable/sstable_format.cpp
    src/format/parquet/schema_converter.cpp
    src/query/planner/logical_optimizer.cpp
    src/query/planner/logical_plan_node.cpp
    src/query/planner/logical_plan_printer.cpp
    src/query/planner/logical_planner.cpp
    src/rsm/rsm.cpp
    src/rsm/snapshot_manager.cpp
    ${PROTO_SRCS}
    ${GRPC_SRCS}
)

# Add include directories for common library
target_include_directories(pondhouse_common PUBLIC 
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_SOURCE_DIR}/third_party/rapidjson/include
    ${CMAKE_SOURCE_DIR}/third_party/sql-parser/src
    ${Protobuf_INCLUDE_DIRS}
    ${PROTO_OUTPUT_DIR}
)

# Link common library with dependencies
target_link_libraries(pondhouse_common PUBLIC 
    Arrow::arrow_shared
    Parquet::parquet_shared
    ${XXHASH_LIBRARY}
    fmt::fmt
    protobuf::libprotobuf
    gRPC::grpc++
    gRPC::grpc++_reflection
    ${ABSL_LIBS}
    ${CMAKE_BINARY_DIR}/libsqlparser.so
)

# Add dependency on sql-parser build
add_dependencies(pondhouse_common build_sql_parser)

# Add dependency on protobuf generation
add_dependencies(pondhouse_common generate_protos)

# Add main executable
add_executable(${PROJECT_NAME} src/main.cpp)

# Link main executable with common library
target_link_libraries(${PROJECT_NAME} PRIVATE pondhouse_common)

# Add server executable
add_executable(pond_server 
    src/server/gRPC/server_main.cpp
    src/server/gRPC/pond_service_impl.cpp
)

# Link server executable with common library
target_link_libraries(pond_server PRIVATE pondhouse_common)

# Add dependency on protobuf generation for server
add_dependencies(pond_server generate_protos)

# Enable testing
enable_testing()

# Add Google Test from submodule
add_subdirectory(third_party/googletest)

# Add test executable
add_executable(
    unit_tests
    tests/catalog/kv_catalog_test.cpp
    tests/catalog/kv_catalog_util_test.cpp
    tests/catalog/table_data_ingestor_test.cpp
    tests/common/append_only_fs_test.cpp
    tests/common/bloom_filter_test.cpp
    tests/common/bplus_tree_test.cpp
    tests/common/crc_test.cpp
    tests/common/data_chunk_test.cpp
    tests/common/error_test.cpp
    tests/common/expression_test.cpp
    tests/common/iterator_test.cpp
    tests/common/lru_cache_test.cpp
    tests/common/result_test.cpp
    tests/common/scope_exit_test.cpp
    tests/common/stream_test.cpp
    tests/common/time_test.cpp
    tests/common/uuid_test.cpp
    tests/common/wal_state_machine_test.cpp
    tests/common/wal_test.cpp
    tests/rsm/replication_test.cpp
    tests/rsm/replicated_state_machine_test.cpp
    tests/rsm/snapshot_manager_test.cpp
    tests/format/parquet/parquet_test.cpp
    tests/format/parquet/schema_converter_test.cpp
    tests/format/sstable/sstable_reader_test.cpp
    tests/format/sstable/sstable_writer_test.cpp
    tests/format/sstable/sstable_format_test.cpp
    tests/format/sstable/sstable_filter_test.cpp
    tests/kv/compaction_metrics_test.cpp
    tests/kv/db_test.cpp
    tests/kv/kv_table_test.cpp
    tests/kv/kv_wal_test.cpp
    tests/kv/memtable_test.cpp
    tests/kv/record_test.cpp
    tests/kv/sstable_cache_test.cpp
    tests/kv/sstable_manager_test.cpp
    tests/kv/table_metadata_test.cpp
    tests/kv/table_test.cpp
    tests/kv/versioned_value_test.cpp
    tests/main.cpp
    tests/query/logical_planner_test.cpp
    tests/query/logical_optimizer_test.cpp
    tests/sql/hyrise_parser_test.cpp
    tests/test_helper.cpp
    tests/proto/metadata_test.cpp
)

# Add include directories for tests
target_include_directories(unit_tests PRIVATE
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_SOURCE_DIR}/tests
    ${CMAKE_SOURCE_DIR}/third_party/rapidjson/include
    ${CMAKE_SOURCE_DIR}/third_party/sql-parser/src
    ${Protobuf_INCLUDE_DIRS}
    ${PROTO_OUTPUT_DIR}
)

# Link test executable against gtest & your project libraries
target_link_libraries(unit_tests
    GTest::gtest
    GTest::gtest_main
    pondhouse_common
    Arrow::arrow_shared
    Parquet::parquet_shared
    protobuf::libprotobuf
    ${CMAKE_BINARY_DIR}/libsqlparser.so
    ${ABSL_LIBS}
)

# Add dependency on protobuf generation for tests
add_dependencies(unit_tests generate_protos)

# Add tests to CTest
add_test(NAME unit_tests COMMAND unit_tests)

# Add custom clean target to remove generated protobuf files
add_custom_target(clean_all
    COMMAND ${CMAKE_COMMAND} -E remove_directory ${PROTO_OUTPUT_DIR}
    COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR} --target clean
    COMMENT "Removing all generated files and build artifacts"
)
