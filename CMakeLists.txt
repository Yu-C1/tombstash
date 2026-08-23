cmake_minimum_required(VERSION 3.16)
project(lsmkv CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Debug)
endif()

# Optional: build everything with ThreadSanitizer for the concurrency tests
# (spec section 4). Enable with -DLSMKV_TSAN=ON.
option(LSMKV_TSAN "Build with ThreadSanitizer" OFF)
if(LSMKV_TSAN)
  add_compile_options(-fsanitize=thread -g -O1)
  add_link_options(-fsanitize=thread)
endif()

# --- core library -----------------------------------------------------------
add_library(lsmkv
  src/memtable.cpp
  src/wal.cpp
  src/bloom_filter.cpp
  src/sstable.cpp
  src/db.cpp)
target_include_directories(lsmkv PUBLIC include)
target_compile_options(lsmkv PRIVATE -Wall -Wextra -Wpedantic)

# --- tests ------------------------------------------------------------------
enable_testing()
find_package(GTest REQUIRED)
include(GoogleTest)

add_executable(unit_tests
  tests/memtable_test.cpp
  tests/wal_recovery_test.cpp
  tests/bloom_filter_test.cpp
  tests/sstable_test.cpp
  tests/db_recovery_test.cpp
  tests/db_sstable_test.cpp)
target_link_libraries(unit_tests PRIVATE lsmkv GTest::gtest GTest::gtest_main)
gtest_discover_tests(unit_tests)
