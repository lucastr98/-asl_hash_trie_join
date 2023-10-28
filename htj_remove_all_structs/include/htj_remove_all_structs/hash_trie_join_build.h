#ifndef HASH_TRIE_JOIN_BUILD_H_
#define HASH_TRIE_JOIN_BUILD_H_

#include "hash_trie.h"
#include "MurmurHash3.h"
#include "tsc_x86.h"

#define SEED (3721346)

size_t get_list_length(size_t *next, size_t head_index);
uint64_t hash(void *key, int len);
uint64_t lookupBucket(int lvl, size_t node_idx, hash_trie_t *trie, uint64_t hash);
size_t *build(size_t head_index, hash_trie_t *trie);

// void trie_destructor(trie_node_t *M, const int *N);

#ifdef BENCHMARK_SUBFUNCS
  typedef struct 
  {
    uint64_t get_list_length;
    uint64_t allocate_hash_table;
    uint64_t hash;
    uint64_t lookup_bucket;
  } build_subfunc_times_t;

  void init_build_subfunc_times();
  void print_build_subfunc_times(int query_nr, int num_iterations);
  void write_build_subfunc_times(int query_nr, int num_iterations);
#endif // BENCHMARK_SUBFUNCS

#endif // HASH_TRIE_JOIN_BUILD_H_
