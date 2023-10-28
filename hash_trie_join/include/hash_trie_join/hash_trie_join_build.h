#ifndef HASH_TRIE_JOIN_BUILD_H_
#define HASH_TRIE_JOIN_BUILD_H_

#include "hash_trie.h"
#include "MurmurHash3.h"
#include "tsc_x86.h"

#define SEED (3721346)

int get_list_length(tuple_t *node);
trie_node_t *allocateHashTable(size_t pow2_p);
uint64_t hash(void *key, int len);
trie_bucket_t *lookupBucket(trie_node_t *M, uint64_t hash);
bucket_child_t build(int i, tuple_t *L, const int *N, const int *attributes);

void destroy_list(tuple_t *head, size_t size);
void destroy_result_list(tuple_t *head);

// void destroy_rec(bucket_child_t M, int level, const int *N);
void trie_destructor(trie_node_t *M, const int *N);

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