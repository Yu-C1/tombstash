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
find_package(Threads REQUIRED)

add_library(lsmkv
  src/memtable.cpp
  src/wal.cpp
  src/bloom_filter.cpp
  src/sstable.cpp
  src/vlog.cpp
  src/compaction.cpp
  src/db.cpp)
target_include_directories(lsmkv PUBLIC include)
target_compile_options(lsmkv PRIVATE -Wall -Wextra -Wpedantic)
# DB runs a background compaction thread.
target_link_libraries(lsmkv PUBLIC Threads::Threads)

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
  tests/db_sstable_test.cpp
  tests/compaction_test.cpp
  tests/db_stats_test.cpp
  tests/scan_test.cpp
  tests/kv_separation_test.cpp
  tests/gc_test.cpp
  tests/background_flush_test.cpp
  tests/manifest_test.cpp)
target_link_libraries(unit_tests PRIVATE lsmkv GTest::gtest GTest::gtest_main)
# PRE_TEST defers enumeration to test time, so a sanitizer build does not run the
# instrumented binary during the build (ThreadSanitizer needs ASLR disabled --
# run it via: setarch -R ./build-tsan/unit_tests).
gtest_discover_tests(unit_tests DISCOVERY_MODE PRE_TEST)

# --- benchmark + CLI --------------------------------------------------------
add_executable(benchmark bench/benchmark.cpp)
target_link_libraries(benchmark PRIVATE lsmkv)

add_executable(lsmkv_cli cli/main.cpp)
set_target_properties(lsmkv_cli PROPERTIES OUTPUT_NAME lsmkv)
target_link_libraries(lsmkv_cli PRIVATE lsmkv)

# --- observability dashboard (optional layer) -------------------------------
add_executable(stats_server dashboard/stats_server.cpp)
target_link_libraries(stats_server PRIVATE lsmkv)
