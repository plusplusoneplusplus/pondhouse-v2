cmake_minimum_required(VERSION 3.14)
project(pondhouse-v2)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find Arrow package
find_package(Arrow REQUIRED)

# Add the executable
add_executable(${PROJECT_NAME} src/main.cpp)

# Link against Arrow
target_link_libraries(${PROJECT_NAME} PRIVATE Arrow::arrow_shared)
