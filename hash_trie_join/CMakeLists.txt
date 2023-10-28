cmake_minimum_required(VERSION 3.0)

project(hash_trie_join)

find_package(PostgreSQL REQUIRED)

# Add the include directories to the project
include_directories(include/hash_trie_join)
include_directories(include/timing_utils)
include_directories(${JANSSON_INCLUDE_DIRS})

# Create a list of all source files
set(SOURCES
  src/hash_trie.c
  src/hash_trie_join_build.c
  src/hash_trie_join_probe.c
  src/hash_trie_join.c
  src/main.c
  src/MurmurHash3.c
  src/benchmark_setup.c
  src/timing.c
  src/validate_results.c
)

add_compile_options(-O3 -fno-tree-vectorize -ffast-math -march=native)

# Create an executable target
add_executable(hash_trie_join_program ${SOURCES})

# Link the executable with the math library
target_link_libraries(hash_trie_join_program PUBLIC m PRIVATE PostgreSQL::PostgreSQL)
target_link_libraries(hash_trie_join_program PUBLIC jansson)

